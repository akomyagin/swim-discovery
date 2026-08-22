# План Этапа 1 — Модель узла + membership-список + direct ping/ack (UDP, localhost)

Ветка: `stage/1-direct-ping` (уже создана от `master`, уже выбрана).
Опорные требования: `docs/TECHNICAL_PLAN.md` (раздел «Этап 1»),
`docs/PLAN.md`, конвенции `.claude/skills/go-swim-discovery-dev/SKILL.md`.

Модуль: `github.com/akomyagin/swim-discovery`, Go 1.23.4, тулчейн `~/sdk/go/bin/go`.
**Только стандартная библиотека.** Внешних зависимостей и Docker нет.

---

## 0. Цель и границы Этапа

**Цель.** Один узел умеет: держать локальный membership-список с merge по
precedence `(Incarnation, State)`; кодировать/декодировать `Ping`/`Ack` в JSON;
слать датаграммы поверх реального UDP на localhost; в probe-цикле раз в интервал
пинговать случайного члена и по `Ack` подтверждать его `alive`. Плюс минимальный
in-memory fake transport, чтобы тестировать ping/ack без реальной сети.

**Критерий готовности (весь набор обязателен).**
- `~/sdk/go/bin/go build ./...` — чисто.
- `~/sdk/go/bin/go vet ./...` — чисто.
- `~/sdk/go/bin/go test -race ./...` — все тесты зелёные под `-race`.
- `~/sdk/go/bin/gofmt -l .` — пустой вывод (всё отформатировано).
- Ни одного оставшегося `panic("TODO(Этап 1): ...")` в затронутых телах.
- Compile-time assertions (`var _ transport.Transport = (*UDP)(nil)` и новый
  аналог для fake) на месте и компилируются.

### Что НЕ делать в Этапе 1 (жёсткая граница — не забегать вперёд)

- **Gossip / piggyback `Updates` — Этап 2.** Поле `Message.Updates` уже
  существует в структуре, но в Этапе 1 его **не заполнять и не обрабатывать**.
  `Encode`/`Decode` сериализуют его как есть (пустой срез → благодаря
  `omitempty` в wire не попадает). Не писать код прогонки `Updates` через
  `List.Merge` на приёме. Не добавлять re-gossip.
- **PingReq / indirect probing — Этап 3.** `KindPingReq` — константа уже есть;
  **не** реализовывать обработку, не выбирать K посредников, не релеить ack.
  При неполучении `Ack` в Этапе 1 узел просто ничего не делает (логирует и
  продолжает) — **не** переводить в `Suspect`.
- **Suspicion timeouts / `Clock` — Этап 4.** **Не** вводить абстракцию `Clock`,
  **не** заводить suspicion-таймеры, **не** реализовывать переход
  `suspect→dead`, **не** реализовывать refute. `Member.StateChangedAt` можно
  проставлять при merge (поле уже есть), но таймауты по нему не крутить.
  ВАЖНО: `time.Now()` в probe-логике в Этапе 1 допустим только в двух местах —
  RTT-дедлайн через `context.WithTimeout` (это не suspicion) и штамп
  `StateChangedAt`. Абстракцию `Clock` вводить **в Этапе 4**, не сейчас.
- **Управляемый drop/delay/partition в fake transport — Этап 5.** Fake этого
  этапа — **без потерь и без задержек**, простой in-memory роутер. Никаких
  per-link drop-rate. См. раздел 4.

Порог односторонний: если по ходу всплывёт необходимость трогать контракт
`Transport`, схему `Message` или вводить `Clock` — **остановиться и вернуть
вопрос**, а не расширять скоуп.

---

## 1. `internal/member/member.go` — List + Merge (заменить содержимое типа/функций)

Файл уже содержит `State`, `String()`, `ID`, `Member` — **их не трогать**.
Заменить только заглушки `List`, `NewList`, `Merge`, `Members`.

### 1.1. Тип `List`

```go
import (
	"sort"
	"sync"
	"time"
)

type List struct {
	self    ID
	mu      sync.RWMutex
	members map[ID]*Member
}
```

### 1.2. `NewList(self Member) *List`

- Создаёт `List` с `members: map[ID]*Member{}`, `self: self.ID`.
- Кладёт копию `self` в map (значение по ID `self.ID`). Хранить именно
  `*Member`, указывающий на выделенную копию, не на аргумент.
- Если `self.State` нулевое — это `StateAlive` (iota=0), корректно.
- Не паниковать при пустом `self.ID` — просто положить как есть (валидацию адреса
  делает `cmd`, не `List`).

### 1.3. `Merge(m Member) (changed bool)` — несущий инвариант

Правило precedence (из SKILL.md §4 и TECHNICAL_PLAN «Этап 1»):

- Под `l.mu.Lock()` (полный write-lock; читаем-и-пишем).
- `cur, ok := l.members[m.ID]`.
- **Если записи нет** (`!ok`): вставить копию `m`, вернуть `changed=true`.
- **Если запись есть**, сравнить `(Incarnation, State)`:
  - `m.Incarnation > cur.Incarnation` → входящее побеждает: обновить, `changed=true`.
  - `m.Incarnation < cur.Incarnation` → входящее устарело: игнор, `changed=false`.
  - `m.Incarnation == cur.Incarnation`:
    - сравнить `stateRank(m.State)` vs `stateRank(cur.State)`, где
      `stateRank`: `StateAlive→0, StateSuspect→1, StateDead→2` (`Dead > Suspect > Alive`).
    - если `stateRank(m.State) > stateRank(cur.State)` → обновить, `changed=true`.
    - иначе (`<=`, включая равное состояние) → игнор, `changed=false`.
- **Обновление записи** (когда входящее побеждает): заменить хранимую копию
  полями из `m`. При смене `State` (когда `cur.State != m.State`) — проставить
  `StateChangedAt = time.Now()`, если у `m.StateChangedAt` нулевое значение;
  если `m.StateChangedAt` задано — взять его. (Мягкое поведение: поле есть, но
  таймауты по нему в Этапе 1 не крутятся.)
- Вернуть `changed`.

Вспомогательная функция (не экспортировать):
```go
func stateRank(s State) int // StateAlive:0, StateSuspect:1, StateDead:2
```

**Инвариант хранения:** в map всегда лежит копия, не разделяемый с вызывающим
указатель. Merge не должен позволить внешнему коду мутировать запись мимо мьютекса.

### 1.4. `Members() []Member`

- Под `l.mu.RLock()`.
- Собрать срез значений (`Member`, не указателей — копии) из map.
- Отсортировать по `ID` (через `sort.Slice`) для детерминизма тестов и стабильного
  вывода CLI.
- Вернуть срез. Пустой список кластера теоретически невозможен (там всегда self),
  но на пустой map вернуть `[]Member{}` (не nil — удобнее для тестов; допустимо и nil,
  тесты писать под фактическое поведение).

### 1.5. Хелпер для probe-цикла (нужен swim) — добавить метод

`swim` нужно выбрать случайного члена, **кроме себя**, и знать адрес узла. Чтобы
не тащить внутренности List в swim, добавить:

```go
// Self returns this node's own ID.
func (l *List) Self() ID

// Others returns a snapshot of all members except self (any state), sorted by ID.
func (l *List) Others() []Member
```

- `Self()` — вернуть `l.self` (поле неизменяемо после `NewList`, можно без лока,
  но для чистоты можно под RLock; поле не меняется — лок не обязателен).
- `Others()` — под RLock, как `Members()`, но пропустить запись с `ID == l.self`,
  сортировать по ID.

Обоснование: probe должен выбирать среди *других* узлов; фильтрацию self держим в
`member`, а не размазываем по swim. В Этапе 1 probe пингует любого из `Others()`
независимо от состояния (suspect/dead ещё не появляются, все alive).

---

## 2. `internal/protocol/protocol.go` — Encode/Decode (заменить только заглушки)

Файл уже содержит `Kind`, константы, `Update`, `Message` — **не трогать**.
Заменить `Encode` и `Decode`.

```go
import "encoding/json"

func Encode(m Message) ([]byte, error) {
	return json.Marshal(m)
}

func Decode(payload []byte) (Message, error) {
	var m Message
	err := json.Unmarshal(payload, &m)
	return m, err
}
```

- Ничего умного: прямой `json.Marshal`/`Unmarshal`. Теги уже проставлены в
  структурах (`json:"kind"` и т.д.).
- **Не** фильтровать `Updates` вручную — `omitempty` уже делает так, что пустой
  срез не попадёт в wire. Заполнением `Updates` занимается Этап 2, здесь всегда
  пусто.
- `Decode` возвращает ошибку `json.Unmarshal` как есть — не оборачивать.

---

## 3. `internal/transport/transport.go` — UDP-адаптер (заменить заглушки UDP)

Файл содержит `Packet`, интерфейс `Transport`, тип `UDP` (заглушка),
`var _ Transport = (*UDP)(nil)` — **assertion не удалять**. Заменить тело `UDP`
и его методы.

### 3.1. Тип `UDP`

```go
import (
	"context"
	"fmt"
	"net"
)

type UDP struct {
	conn    *net.UDPConn
	local   string
	inbound chan Packet
	done    chan struct{} // closed by Close to stop the receive loop
}
```

Константа максимального размера датаграммы для read-буфера:
```go
const maxDatagram = 65507 // theoretical max UDP payload on IPv4
```

### 3.2. `NewUDP(addr string) (*UDP, error)`

- `net.ResolveUDPAddr("udp", addr)` → при ошибке вернуть `nil, fmt.Errorf(...)`.
- `net.ListenUDP("udp", udpAddr)` → conn, при ошибке вернуть обёрнутую ошибку.
- `local := conn.LocalAddr().String()` (после bind с портом 0 вернёт реальный порт —
  полезно для тестов, но в Этапе 1 адреса заданы явно).
- Создать `inbound := make(chan Packet, 64)` (небольшой буфер, чтобы receive-loop не
  блокировался на пиках), `done := make(chan struct{})`.
- Запустить `go u.readLoop()`.
- Вернуть `u`.

### 3.3. `readLoop()` (не экспортировать)

- Цикл: `buf := make([]byte, maxDatagram)` (выделять буфер **внутри** цикла на
  каждую итерацию, чтобы не делить срез между пакетами и не ловить гонку данных;
  либо выделять один и **копировать** прочитанные байты — выбрать копирование).
- `n, remote, err := u.conn.ReadFromUDP(buf)`.
- При `err`: если `done` закрыт — тихо выйти (`return`); иначе тоже выйти (сокет
  закрыт/сломан). Проверять закрытие: `select { case <-u.done: return; default: }`
  перед продолжением, и трактовать ошибку чтения как сигнал завершения (после
  `Close()` `ReadFromUDP` вернёт ошибку — это нормально).
- Скопировать: `payload := make([]byte, n); copy(payload, buf[:n])`.
- Сформировать `Packet{Addr: remote.String(), Payload: payload}`.
- Доставить в канал неблокирующе относительно завершения:
  `select { case u.inbound <- pkt: case <-u.done: return }`.

### 3.4. Методы интерфейса

```go
func (u *UDP) Send(ctx context.Context, addr string, payload []byte) error {
	// Resolve addr, WriteToUDP. Best-effort: return only real socket errors,
	// not "drops" (UDP has no drop signal anyway).
	// Respect ctx: if ctx.Err() != nil before sending, return ctx.Err().
}

func (u *UDP) Receive(ctx context.Context) (Packet, error) {
	select {
	case pkt := <-u.inbound:
		return pkt, nil
	case <-ctx.Done():
		return Packet{}, ctx.Err()
	}
}

func (u *UDP) LocalAddr() string { return u.local }

func (u *UDP) Close() error {
	// close(u.done) once (guard against double close if needed), then u.conn.Close().
	// Return conn.Close() error.
}
```

- `Send`: `net.ResolveUDPAddr` → `u.conn.WriteToUDP(payload, udpAddr)`. Проверить
  `ctx.Err()` в начале; вернуть ошибку резолва/записи как есть (best-effort:
  реальные сбои сокета — да, но «пакет мог потеряться» ошибкой не считается,
  UDP этого и не сообщает).
- `Close`: закрыть `done` (защитить от двойного закрытия — например, через
  `sync.Once` в поле `closeOnce sync.Once`, чтобы `Close()` был идемпотентен),
  затем `u.conn.Close()`.

**Рекомендация:** добавить в структуру поле `closeOnce sync.Once` для идемпотентного
`Close`. Тесты могут вызвать `Close` в `defer` и явно — двойной `close(chan)`
паникует, `sync.Once` это снимает.

---

## 4. `internal/transport/fake.go` — НОВЫЙ файл: минимальный in-memory transport

**Архитектурное решение (зафиксировано):** fake-транспорт кладём в
`internal/transport/fake.go`, в тот же пакет `transport`, а **не** в отдельный
testutil-пакет и **не** под `_test.go`.

Обоснование:
- Он реализует `transport.Transport` и обязан жить рядом с портом и держать
  compile-time assertion `var _ Transport = (*Fake)(nil)` — как того требует
  SKILL.md §1 («аналог для fake не удалять»).
- В Этапе 5 этот файл **дорастает** до полноценного симулятора с drop/delay/
  partition (TECHNICAL_PLAN прямо помещает `fake.go` в `internal/transport/`).
  Заводя его здесь тем же путём, мы не создаём миграцию файла на Этапе 5.
- Он нужен интеграционным тестам swim (`internal/swim`) — значит должен быть
  импортируемым не-тестовым кодом другого пакета; `_test.go` в `transport` этого
  бы не дал.

Это не «продовый» код в смысле бинаря: он не используется в `cmd`, только в
тестах. Но живёт как обычный экспортируемый тип пакета `transport`.

### 4.1. Модель

Fake-узлы общаются через общий **коммутатор** (`FakeNetwork`), в котором
регистрируются по адресу. Разделяем на две сущности:

```go
// FakeNetwork is the shared in-memory switch that routes datagrams between
// registered fake endpoints. Этап 1: no loss, no delay. Этап 5 will add
// per-link drop-rate / delay / partition here.
type FakeNetwork struct {
	mu        sync.RWMutex
	endpoints map[string]*Fake // addr -> endpoint
}

func NewFakeNetwork() *FakeNetwork

// Endpoint creates and registers a Fake transport bound to addr on this network.
func (n *FakeNetwork) Endpoint(addr string) *Fake
```

```go
// Fake is an in-memory Transport endpoint on a FakeNetwork. Этап 1: lossless.
type Fake struct {
	net     *FakeNetwork
	local   string
	inbound chan Packet
	closeOnce sync.Once
	done    chan struct{}
}

var _ Transport = (*Fake)(nil) // do not remove — keeps the adapter honest
```

### 4.2. Поведение (Этап 1 — без потерь, без задержки)

- `NewFakeNetwork()` — пустая карта endpoints.
- `Endpoint(addr)`:
  - создать `Fake{net: n, local: addr, inbound: make(chan Packet, 64), done: make(chan struct{})}`.
  - под `n.mu.Lock()` зарегистрировать `n.endpoints[addr] = f`.
  - вернуть `f`.
  - (Если адрес уже занят — можно перезаписать или паниковать; выбрать: паниковать
    с понятным сообщением, т.к. это тестовая ошибка настройки.)
- `Fake.Send(ctx, addr, payload)`:
  - проверить `ctx.Err()` → вернуть, если отменён.
  - под `n.mu.RLock()` найти `dst, ok := n.endpoints[addr]`.
  - если `!ok` — **best-effort**: вернуть `nil` (пакет «улетел в никуда», как UDP
    на несуществующий адрес; это НЕ ошибка транспорта). Не паниковать.
  - **скопировать payload** (`p := make([]byte, len(payload)); copy(p, payload)`) —
    отправитель не должен делить буфер с получателем (иначе гонка/мутация).
  - доставить неблокирующе:
    `select { case dst.inbound <- Packet{Addr: f.local, Payload: p}: case <-dst.done: /* dropped, closed */ default: /* buffer full: Этап 1 — считать доставленным best-effort; можно блокировать до записи или дропнуть. Выбрать: если буфер полон, всё равно поставить блокирующе, но с уважением к dst.done */ }`.
    - **Решение по переполнению буфера:** буфер 64 достаточен для юнит-сценариев
      Этапа 1 (мало сообщений). Сделать доставку блокирующей относительно `dst.done`:
      `select { case dst.inbound <- pkt: case <-dst.done: }`. Так тесты не теряют
      сообщения из-за переполнения (управляемый drop — только Этап 5).
  - `Addr` в доставленном `Packet` — адрес **отправителя** (`f.local`), чтобы
    получатель мог ответить. (Согласуется с UDP: там `Addr` = remote источника.)
- `Fake.Receive(ctx)` — как у UDP: `select` по `inbound` / `ctx.Done()`.
- `Fake.LocalAddr()` — `f.local`.
- `Fake.Close()` — `closeOnce`: `close(f.done)`; под `n.mu.Lock()` удалить себя из
  `n.endpoints`; вернуть `nil`.

**Границы Этапа 1 (не забегать):** никаких `dropRate`, `delay`, `partition`,
seeded `*rand.Rand` в fake — всё это Этап 5. Здесь fake детерминирован тем, что
ничего не теряет.

---

## 5. `internal/swim/swim.go` — НОВЫЙ пакет и файл: probe-loop

### 5.1. Решение по инжектируемому `*rand.Rand` (зафиксировано)

**Вводим `*rand.Rand` как параметр конструктора уже в Этапе 1.** Обоснование:
- probe выбирает *случайного* члена — если брать глобальный `math/rand`, тест
  «Ping ушёл выбранному узлу, пришёл Ack, узел alive» станет недетерминированным
  по выбору цели.
- TECHNICAL_PLAN §«Стек» и SKILL.md §5 фиксируют инжектируемый seedable rand как
  сквозное решение; вводить его сразу дешевле, чем переделывать конструктор на
  Этапе 3 (где K посредников тоже выбираются рандомом).
- `Clock` при этом **НЕ** вводим (это Этап 4) — RTT-дедлайн делаем через
  `context.WithTimeout`, это не suspicion-логика и не флейкает юнит-тесты merge/
  encode. Интеграционный тест probe в Этапе 1 работает на реальных (коротких)
  таймаутах через fake transport без потерь — Ack приходит практически мгновенно,
  дедлайн не срабатывает. Это допустимо; управляемое время появится в Этапе 4.

### 5.2. Тип `Node`

```go
package swim

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/akomyagin/swim-discovery/internal/member"
	"github.com/akomyagin/swim-discovery/internal/protocol"
	"github.com/akomyagin/swim-discovery/internal/transport"
)

// Config holds probe-loop timing and randomness. Clock stays системный in Этап 1
// (context deadlines only); a Clock abstraction arrives in Этап 4.
type Config struct {
	ProbeInterval time.Duration // how often to probe one random peer (e.g. 1s)
	RTTTimeout    time.Duration // how long to wait for an Ack (e.g. 300ms)
	Rand          *rand.Rand    // injectable RNG for deterministic peer selection
}

type Node struct {
	list *member.List
	tr   transport.Transport
	cfg  Config

	mu    sync.Mutex
	seqNo uint64 // monotonically increasing; correlates Ping with Ack

	// pending correlates an outstanding Ping's SeqNo with a channel that the
	// receive loop signals when the matching Ack arrives.
	pending map[uint64]chan struct{}
}
```

### 5.3. Конструктор

```go
func NewNode(list *member.List, tr transport.Transport, cfg Config) *Node
```
- Если `cfg.Rand == nil` — создать `rand.New(rand.NewSource(time.Now().UnixNano()))`
  (прод-путь). В тестах передаётся seeded rand.
- Дефолты: если `ProbeInterval == 0` → 1s; если `RTTTimeout == 0` → 300ms.
- Инициализировать `pending: map[uint64]chan struct{}{}`.

### 5.4. Главный цикл — `Run(ctx context.Context) error`

Запускает две горутины и ждёт отмены `ctx`:

1. **receiveLoop(ctx):** цикл `tr.Receive(ctx)` → `protocol.Decode` → диспетчер по
   `Kind`:
   - `KindPing`: ответить `Ack`. Собрать `protocol.Message{Kind: KindAck, From:
     list.Self(), SeqNo: msg.SeqNo}` (SeqNo эхом — коррелирует с пингом), Encode,
     `tr.Send(ctx, pkt.Addr, ...)`. **Updates не заполнять** (Этап 2).
     Также: узнать отправителя как члена — но в Этапе 1 **не** вводим gossip;
     достаточно ответить. (Опционально: при получении Ping можно `list.Merge`
     отправителя как alive, если он известен по Addr — НО без Updates мы не знаем
     его ID/incarnation достоверно; **не делать**, оставить на Этап 2, где придёт
     Update. В Этапе 1 членство наполняется из seed в main, см. §6.)
   - `KindAck`: найти `pending[msg.SeqNo]`, если есть — закрыть/сигналить канал
     (снять write-lock аккуратно), удалить из map. Так `probeOnce` узнаёт, что Ack
     пришёл. Подтверждение alive цели — см. `probeOnce`.
   - `KindPingReq`: **Этап 3, не реализовывать.** В Этапе 1 — игнорировать
     (можно залогировать «not implemented until Этап 3» и продолжить). НЕ паниковать.
   - Ошибку `Decode` — залогировать и продолжить (битый пакет не роняет узел).
   - Выход из цикла по `ctx.Done()` (когда `Receive` вернёт `ctx.Err()`).

2. **probeLoop(ctx):** `ticker := time.NewTicker(cfg.ProbeInterval)`; на каждый тик
   вызвать `probeOnce(ctx)`; выход по `ctx.Done()`. (Использование `time.Ticker`
   здесь — не suspicion-логика, а расписание probe; допустимо в Этапе 1. `Clock`
   для этого вводится в Этапе 4, но сейчас реальный ticker приемлем; интеграционный
   тест probe будет дёргать `probeOnce` напрямую, а не ждать тикер — см. §7.)

`Run` возвращается, когда `ctx` отменён; оба цикла завершаются; вернуть `ctx.Err()`
или `nil` (выбрать `nil` при чистой отмене — по вкусу; тесты писать под факт).

### 5.5. `probeOnce(ctx context.Context)` — сердце Этапа 1

- Взять `others := list.Others()`. Если пусто — ничего не делать (кластер из одного
  узла), `return`.
- Выбрать индекс `i := cfg.Rand.Intn(len(others))`, `target := others[i]`.
- Под `n.mu.Lock()`: `n.seqNo++; seq := n.seqNo; ackCh := make(chan struct{});
  n.pending[seq] = ackCh`. Unlock.
- Собрать `msg := protocol.Message{Kind: KindPing, From: list.Self(), SeqNo: seq}`
  (Updates пустой), `Encode`, `tr.Send(ctx, target.Addr, payload)`.
- Ждать Ack: `pctx, cancel := context.WithTimeout(ctx, cfg.RTTTimeout); defer cancel()`.
  `select { case <-ackCh: /* Ack got */ case <-pctx.Done(): /* timeout or cancel */ }`.
- **Cleanup:** под `n.mu.Lock()` удалить `n.pending[seq]` (если ещё есть) — чтобы
  receiveLoop не писал в закрытый/потерянный канал. Аккуратно с состоянием: receiveLoop,
  найдя pending, должен под тем же `n.mu` удалить запись и закрыть канал; probeOnce
  после select тоже под `n.mu` удаляет, если запись ещё на месте. Двойного close
  избежать: договоримся, что **закрывает канал только тот, кто первым забрал запись
  под локом** (receiveLoop при Ack, либо probeOnce при таймауте — оба сначала
  `delete` под локом; кто удалил — тот и закрывает/сигналит; если запись уже
  удалена — ничего не делать).

  **Рекомендуемый простой протокол без close (безопаснее):**
  - receiveLoop при Ack: под `n.mu` — `ch, ok := n.pending[seq]; if ok { delete(n.pending, seq) }`. Вне лока: `if ok { ch <- struct{}{} }` в неблокирующем `select { case ch<-struct{}{}: default: }` ИЛИ канал буферизованный размера 1 → просто `ch <- struct{}{}`. Использовать **буферизованный `make(chan struct{}, 1)`** и слать `ch <- struct{}{}` — без close, без гонки, без риска паники.
  - probeOnce после select: под `n.mu` — `delete(n.pending, seq)` (идемпотентно).
- **При получении Ack:** подтвердить/обновить target как alive:
  `list.Merge(member.Member{ID: target.ID, Addr: target.Addr, Incarnation:
  target.Incarnation, State: member.StateAlive})`. Т.к. target уже alive с той же
  incarnation, `Merge` вернёт `changed=false` — это ок (в Этапе 1 gossip нет,
  changed никуда не идёт). Это выполняет требование «при Ack — подтвердить alive».
- **При таймауте (нет Ack):** в Этапе 1 — **ничего** (только опционально лог
  «no ack from <target> — indirect probe deferred to Этап 3»). **НЕ** переводить в
  Suspect (Этап 4), **НЕ** запускать PingReq (Этап 3).

### 5.6. Замечания по конкурентности

- Всё гоняется под `-race`. `n.mu` защищает `seqNo` и `pending`. `member.List` — свой
  RWMutex. `transport` потокобезопасен по контракту (UDP conn безопасен для
  конкурентных Write; наш fake — под мьютексом сети + буферизованные каналы).
- receiveLoop и probeLoop работают конкурентно; их синхронизация — только через
  `pending` под `n.mu`.

---

## 6. `cmd/swim-discovery/main.go` — тонкая связка (заменить тело main)

Флаги `--addr` / `--seed` уже разобраны. Заменить блок TODO на реальный запуск.

Последовательность:
1. Разобрать флаги (как есть).
2. `self := member.Member{ID: member.ID(*addr), Addr: *addr, Incarnation: 0,
   State: member.StateAlive}` — ID = адрес (v1-конвенция из `member.go`:
   «ID = host:port»).
3. `list := member.NewList(self)`.
4. **Seed:** если `*seed != ""` и `*seed != *addr` — добавить сид-узел как известного
   члена: `list.Merge(member.Member{ID: member.ID(*seed), Addr: *seed,
   Incarnation: 0, State: member.StateAlive})`. Так probe с самого начала имеет кого
   пинговать. (Полноценный join/handshake с обменом membership — это Этап 2 gossip;
   в Этапе 1 достаточно занести сид как alive-члена, чтобы direct ping заработал в
   обе стороны: оба узла указывают друг на друга через `--seed`.)
5. `tr, err := transport.NewUDP(*addr)` → при ошибке `log.Fatalf`.
   `defer tr.Close()`.
6. `cfg := swim.Config{ProbeInterval: time.Second, RTTTimeout: 300 * time.Millisecond}`
   (Rand nil → NewNode подставит системный).
7. `node := swim.NewNode(list, tr, cfg)`.
8. Контекст с отменой по SIGINT/SIGTERM:
   `ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt,
   syscall.SIGTERM); defer stop()`.
9. Лог старта: `log.Printf("swim-discovery node %s started (seed=%q)", *addr, *seed)`.
10. `if err := node.Run(ctx); err != nil { log.Printf("node stopped: %v", err) }`.
11. Убрать `os.Exit(1)` и заглушку `Fprintf`.

Импорты: `context`, `log`, `os`, `os/signal`, `syscall`, `time`, `flag`, плюс
`internal/member`, `internal/transport`, `internal/swim`. Убрать `fmt`, если больше
не нужен.

**Границы:** никакой протокольной логики в main (SKILL.md §1). CLI-подкоманда
`members`/observe — Этап 5, не добавлять. Наблюдать сходимость в Этапе 1 можно по
логам probe (при желании исполнитель может добавить периодический
`log.Printf` со списком `list.Members()` — не обязательно, но безвредно; если
добавлять, то тонко, только вывод).

---

## 7. Тесты (обязательны; все под `-race`)

Файлы (все — новые):
- `internal/member/member_test.go`
- `internal/protocol/protocol_test.go`
- `internal/transport/fake_test.go` (базовый тест fake-роутинга)
- `internal/swim/swim_test.go` (ping/ack поверх fake)

### 7.1. `member_test.go` — merge-precedence, table-driven (несущий инвариант)

`TestList_Merge` — таблица кейсов. Для каждого: стартовый локальный `Member`,
входящий `Member`, ожидаемые `changed` и итоговое хранимое `State`/`Incarnation`.

Обязательные кейсы (перебор входящее×локальное×incarnation):
1. Новый ID (нет записи) → `changed=true`, запись появилась.
2. Входящее `Incarnation` выше, любое состояние → побеждает, `changed=true`.
3. Входящее `Incarnation` ниже → игнор, `changed=false`, локальное не изменилось.
4. Равная incarnation, входящее состояние «хуже»:
   - local Alive + incoming Suspect → Suspect побеждает, `changed=true`.
   - local Alive + incoming Dead → Dead, `changed=true`.
   - local Suspect + incoming Dead → Dead, `changed=true`.
5. Равная incarnation, входящее состояние «лучше или равно»:
   - local Suspect + incoming Alive → игнор, `changed=false` (Alive не вытесняет
     Suspect при равной incarnation).
   - local Dead + incoming Suspect → игнор, `changed=false`.
   - local Alive + incoming Alive → игнор, `changed=false` (нет изменения).
   - local Dead + incoming Dead → `changed=false`.
6. Refute-подобный кейс (без самого refute-механизма): local Suspect@5, incoming
   Alive@6 → Alive побеждает по incarnation, `changed=true`. (Демонстрирует, что
   более высокая incarnation вытесняет suspicion — фундамент refute Этапа 4.)

Проверять после `Merge`: возвращённый `changed`, и через `Members()`/`Others()`
— фактическое состояние записи.

Отдельные мелкие тесты:
- `TestNewList_SeedsSelf`: после `NewList(self)` — `Self()` == self.ID; `Members()`
  содержит self; `Others()` пуст.
- `TestList_Others_ExcludesSelf`: добавить пару членов через Merge; `Others()` не
  содержит self и отсортирован по ID.
- (Опц.) `TestList_MembersSorted`: детерминированный порядок по ID.

### 7.2. `protocol_test.go` — round-trip Encode/Decode

- `TestEncodeDecode_RoundTrip`: table-driven по нескольким `Message`:
  - `KindPing` с `From`, `SeqNo`.
  - `KindAck` с `From`, `SeqNo`.
  - `KindPingReq` с `From`, `SeqNo`, `Target` (структура поддерживает поле уже
    сейчас — round-trip проверить можно, обработку не реализуем).
  Для каждого: `Encode` → `Decode` → сравнить `reflect.DeepEqual` (или поле-в-поле)
  с исходником. `Updates` во всех кейсах — nil/пусто; проверить, что после round-trip
  тоже пусто (`omitempty`).
- (Опц.) `TestDecode_Garbage`: `Decode([]byte("{ broken"))` → ошибка != nil.

### 7.3. `fake_test.go` — базовый роутинг fake-транспорта

- `TestFake_SendReceive`: создать `net := NewFakeNetwork()`; `a := net.Endpoint("A")`;
  `b := net.Endpoint("B")`. `a.Send(ctx, "B", payload)`. `pkt, err := b.Receive(ctx)`
  (с таймаут-контекстом на всякий случай). Проверить `pkt.Payload == payload`,
  `pkt.Addr == "A"` (адрес отправителя).
- `TestFake_SendUnknownAddr`: `a.Send(ctx, "Z", ...)` → `err == nil` (best-effort,
  не паникует, пакет теряется).
- `TestFake_Close`: после `a.Close()` — `Send` на A от других не доставляется/не
  паникует; `Receive` на A с отменяемым ctx возвращает по ctx. (Мягкий тест — не
  ужесточать поведение сверх контракта.)
- (Опц.) `TestFake_PayloadCopied`: изменить исходный buf после Send — принятый
  payload не изменился (проверяет копирование).

### 7.4. `swim_test.go` — ping/ack поверх fake (без потерь), детерминированный rand

Ключевой интеграционный тест Этапа 1. Он должен быть **детерминирован** (seeded
rand) и **не флейкать** (не ждать реального тикера — дёргать `probeOnce` напрямую).

**Экспортируемость для теста:** `probeOnce` — приватный. Тест лежит в пакете `swim`
(`package swim`, не `swim_test`), поэтому имеет доступ к приватному `probeOnce`.
Использовать white-box тест.

- `TestNode_PingAck_ConfirmsAlive`:
  1. `net := transport.NewFakeNetwork()`; `trA := net.Endpoint("A")`;
     `trB := net.Endpoint("B")`.
  2. `listA := member.NewList(member.Member{ID:"A", Addr:"A", State:StateAlive})`;
     `listA.Merge(member.Member{ID:"B", Addr:"B", State:StateAlive})`.
  3. `listB := member.NewList(member.Member{ID:"B", Addr:"B", State:StateAlive})`;
     (B знает о A не обязательно для этого теста — B лишь отвечает Ack на Ping от A;
     receiveLoop B отвечает по `pkt.Addr`, ID отправителя брать из `msg.From`).
  4. `rng := rand.New(rand.NewSource(1))` — seeded.
  5. `nodeA := NewNode(listA, trA, Config{ProbeInterval: time.Hour, RTTTimeout:
     time.Second, Rand: rng})` (ProbeInterval большой, чтобы тикер не мешал —
     дёргаем probeOnce вручную).
  6. `nodeB := NewNode(listB, trB, Config{...})`.
  7. Запустить `nodeB.Run(ctxB)` в горутине (нужен его receiveLoop для ответа Ack).
     Для A тоже нужен receiveLoop (чтобы поймать Ack) — либо запустить `nodeA.Run`,
     либо (чище) запустить только приёмные циклы. **Решение:** запустить оба
     `Run(ctx)` в горутинах с большим ProbeInterval, а сам probe инициировать
     явным вызовом `nodeA.probeOnce(ctx)` синхронно в тесте.
  8. Вызвать `nodeA.probeOnce(ctx)` — он выберет единственного другого члена (B),
     пошлёт Ping, дождётся Ack (fake без потерь → Ack придёт), обновит B как alive.
  9. Проверить: B в `listA.Members()` присутствует и `State == StateAlive`. (Т.к.
     он и так был alive — проверка, что probe отработал без ошибок и не сломал
     состояние. Дополнительно можно проверить, что `probeOnce` не заблокировался
     дольше RTTTimeout — обёрнуть в горутину с таймаутом теста.)
  10. Отменить контексты, дождаться завершения `Run`.

- `TestNode_ProbeTimeout_NoAck_StaysAlive` (граница Этапа 1):
  1. Как выше, но B **не запускать** (никто не ответит Ack), либо послать Ping на
     несуществующий адрес.
  2. `nodeA.probeOnce(ctx)` завершается по RTTTimeout (не виснет).
  3. Проверить: B в `listA` **остался alive** — Этап 1 НЕ переводит в Suspect при
     отсутствии Ack (это Этап 3/4). Это фиксирует границу: probeOnce при таймауте
     ничего не портит.

- (Опц.) `TestNode_RespondsAckToPing`: сконструировать Ping вручную, `trX.Send` его
  на nodeA, убедиться, что nodeA прислал Ack с тем же SeqNo (через отдельный
  endpoint, читающий ответ). Проверяет ветку `KindPing` в receiveLoop.

**Все swim-тесты — с общим тайм-аутом контекста (напр. `context.WithTimeout(...,
5*time.Second)`), чтобы зависший тест падал, а не висел.** Обязательно закрывать
транспорты (`defer tr.Close()`) и отменять контексты.

---

## 8. Порядок реализации (рекомендуемый)

1. `member.go` (List/Merge/Members/Self/Others) + `member_test.go` — фундамент, без
   зависимостей. Прогнать `go test ./internal/member/`.
2. `protocol.go` (Encode/Decode) + `protocol_test.go` — тривиально.
3. `transport/fake.go` + `fake_test.go` — нужен для swim-тестов.
4. `transport.go` (UDP) — реальный сокет; отдельного юнит-теста можно не писать
   (покрывается запуском в main + опционально мини-тест send/receive на реальном
   loopback-сокете, если исполнитель захочет; не обязателен).
5. `swim/swim.go` (Node/Run/probeOnce/receiveLoop) + `swim_test.go`.
6. `cmd/swim-discovery/main.go` — связка.
7. Финальный прогон: `go build ./...`, `go vet ./...`, `gofmt -l .`,
   `go test -race ./...`.

---

## 9. Чек-лист «не забежать вперёд» (перечитать перед сдачей)

- [ ] `Message.Updates` нигде не заполняется и не обрабатывается (Этап 2).
- [ ] `KindPingReq` не обрабатывается, только игнор + опц. лог (Этап 3).
- [ ] Нет `Suspect`-переходов по отсутствию Ack; нет suspicion-таймеров; нет
      `Clock`-абстракции; нет refute (Этап 4).
- [ ] Fake-транспорт без drop/delay/partition, без seeded rand внутри fake (Этап 5).
- [ ] `*rand.Rand` инжектируется в `swim.Config` (введено сейчас осознанно).
- [ ] Все compile-time assertions на месте (`UDP`, `Fake`).
- [ ] Заглушки-`panic("TODO(Этап 1)")` заменены реализацией, файлы не продублированы
      в `*_v2.go`.
- [ ] `go build`/`go vet`/`gofmt -l`/`go test -race` — все чисто.
