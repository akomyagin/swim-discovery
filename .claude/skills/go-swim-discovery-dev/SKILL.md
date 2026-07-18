---
name: go-swim-discovery-dev
description: Конвенции проекта swim-discovery — учебный service discovery на gossip-протоколе (SWIM-подобный, мини-Serf/Consul) на Go. Формат gossip/probe-сообщений (Ping/Ack/PingReq + piggyback Update), merge-precedence по incarnation-номерам, паттерн тестирования через подменяемый порт transport.Transport (fake-симулятор сети с управляемой потерей пакетов вместо реального UDP), инжектируемые Clock и rand для детерминизма. Использовать при реализации любого этапа кодирования swim-discovery.
---

# SKILL: go-swim-discovery-dev — конвенции проекта `swim-discovery`

Конкретные конвенции **именно этого проекта** для написания Go-кода. Не общий
гайд «как писать Go», а специфика SWIM-реализации. Применяй при реализации
любого этапа. Опорные документы:
[`../../../docs/TECHNICAL_PLAN.md`](../../../docs/TECHNICAL_PLAN.md),
[`../../../docs/PLAN.md`](../../../docs/PLAN.md).

---

## 1. Структура: `internal/` + `cmd/`, почему нет `pkg/`

- Всё ядро — в `internal/`, компилятор запрещает внешний импорт: учебное
  приложение, а не публикуемая библиотека. `pkg/` не заводить.
- **По пакету на слой:** `internal/member` (модель узла + список),
  `internal/transport` (порт + адаптеры), `internal/protocol` (wire-сообщения),
  `internal/swim` (ядро циклов, заводится в Этапе 1).
- `cmd/swim-discovery/main.go` — **тонкий**: флаги, сборка узла, запуск. Никакой
  протокольной логики в `main`.
- Заглушки Этапа 0 помечены `TODO(Этап N)` и содержат `panic(...)` в теле.
  При реализации **заменяй содержимое файла**, не создавай `*_v2.go`.
- Compile-time assertion `var _ transport.Transport = (*UDP)(nil)` (и аналог для
  fake) **не удалять** — держит контракт адаптеров.

## 2. Порт `transport.Transport` — центральный паттерн тестируемости

Вся сеть за одним интерфейсом (ports & adapters):

```go
type Transport interface {
    Send(ctx context.Context, addr string, payload []byte) error
    Receive(ctx context.Context) (Packet, error)
    LocalAddr() string
    Close() error
}
```

- **Ядро SWIM зависит только от `Transport`, никогда от `*net.UDPConn`.** Это
  единственное, что делает false-positive-сценарии тестируемыми.
- Прод-адаптер `UDP` (`net.ListenUDP`) — best-effort, connectionless: `Send`
  может «потеряться», код обязан это переживать.
- Тестовый адаптер `Fake` (Этап 5, но минимальная версия без потерь — уже в
  Этапе 1 для юнитов) роутит пакеты in-memory между зарегистрированными узлами с
  **управляемыми per-link drop-rate, delay, partition**. Seedable `*rand.Rand`
  для воспроизводимости.
- `Send` best-effort: контракт не гарантирует доставку. Не возвращать ошибку на
  «дроп» — дроп это нормальный сценарий, а не сбой транспорта.

## 3. Формат gossip/probe-сообщений (протокольная схема)

Один конверт `protocol.Message` на все датаграммы; каждое сообщение
**пиггибекает** батч membership-дельт (эпидемическое распространение):

```go
type Kind uint8
const (
    KindPing    Kind = iota // прямой probe "жив?"
    KindAck                 // ответ на Ping / indirect-ping — доказательство живости
    KindPingReq            // "пропингуй Target от моего имени и релей ack" (indirect)
)

type Update struct {                 // одна gossip-дельта членства
    ID          member.ID
    Addr        string
    Incarnation uint64
    State       member.State          // alive / suspect / dead
}

type Message struct {
    Kind    Kind
    From    member.ID
    SeqNo   uint64                    // коррелирует Ping/PingReq с Ack
    Target  member.ID                 // только для KindPingReq
    Updates []Update                  // piggyback gossip (Этап 2+)
}
```

- **Wire-формат — JSON** (`encoding/json`) в v1; `Encode`/`Decode` в
  `internal/protocol`. Бинарный framing — POST_MVP, не сейчас.
- Каждое исходящее сообщение (Ping/Ack/PingReq) должно пиггибекать свежие
  `Updates` — **не** заводить отдельный «gossip-only» тип сообщения. Gossip
  едет на обычном probe-трафике.
- Ограничивать размер батча и число ретрансляций каждой записи (иначе штормы);
  предпочитать наименее распространённые/самые свежие апдейты.

## 4. Merge-precedence по incarnation — несущий инвариант членства

`member.List.Merge(m Member) (changed bool)` разрешает конфликт входящего слуха
с локальной записью строго по правилу:

- **Выше `Incarnation` всегда побеждает.**
- При **равной** `Incarnation`: `Dead > Suspect > Alive` (более «плохое»
  состояние побеждает).
- Слух с меньшей incarnation, чем известно, **игнорируется** (`changed=false`).
- **Refute:** узел, увидевший `Suspect`/`Dead` про себя, бампает **свой**
  `Incarnation` и рассылает `Alive` с новым номером — по этому же правилу он
  вытесняет слух. Это единственный способ опровергнуть ложное подозрение.
- `Merge` возвращает `changed=true` только при реальном изменении view —
  ровно эти записи уходят в исходящий piggyback.

Это самый тестируемый инвариант: table-driven тест на все пары
(входящее состояние × локальное состояние × соотношение incarnation).

## 5. Детерминизм: `Clock` и `rand` инжектируются

- Внутри probe/suspicion логики **никогда** не звать `time.Now()`/`time.After`
  напрямую — только через инжектированный `Clock` (заводится в Этапе 4). Прод —
  `SystemClock`; тест — управляемый fake с `Advance(d)`. Иначе suspicion-тесты
  флейкают на `Sleep`.
- Выбор случайного члена для probe и K посредников для indirect-probe — через
  инжектированный seedable `*rand.Rand`, не через глобальный `math/rand`. Тесты
  фиксируют seed и получают воспроизводимый выбор.

## 6. Тестирование: детерминизм прежде всего

- **Никаких «запустил 5 процессов, вроде сошлось».** Fake-транспорт + fakeClock
  + seeded rand → тесты воспроизводимы.
- **Ярусы:** (1) юниты — merge-precedence (table-driven), `Encode`/`Decode`
  round-trip; (2) интеграция протокола на fake-транспорте — сходимость gossip,
  indirect probe, suspect→dead; (3) `-race` на **весь пакет** для конкурентных
  циклов (обязательно перед коммитом кода Этапов 1+).
- **Центральный тест проекта (Этап 3):** fake-транспорт с **избирательной**
  потерей — дропаем только пакеты инициатор↔Target, но не посредники↔Target;
  ассертим, что Target **остаётся alive** (indirect probing погасил false
  positive). Регресс на этот тест недопустим.
- **Suspect→dead (Этап 4):** прокручивать `fakeClock.Advance`, не `time.Sleep`;
  отдельным кейсом — refute отменяет переход в dead.
