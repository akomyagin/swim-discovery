# План Этапа 2 — Gossip-рассылка дельт членства (piggyback)

Ветка: `stage/2-gossip` (уже создана от свежего `master` после мержа Этапа 1,
уже выбрана).
Опорные требования: `docs/TECHNICAL_PLAN.md` (раздел «Этап 2»), `docs/PLAN.md`,
конвенции `.claude/skills/go-swim-discovery-dev/SKILL.md`.

Модуль: `github.com/akomyagin/swim-discovery`, Go 1.23. Тулчейн `~/sdk/go/bin/go`
(может не быть в `PATH`). **Только стандартная библиотека** — `net`, `context`,
`encoding/json`, `sync`, `time`, `math`, `math/rand`, `sort`. Внешних
зависимостей и Docker нет.

Исполнитель плана — кодер-агент. План даёт пути файлов, сигнатуры, порядок
реализации и конкретные тест-кейсы; отклоняться от сигнатур без причины не нужно.

---

## 0. Цель и границы Этапа

**Цель.** Информация о членстве расходится по кластеру эпидемией слухов,
пиггибекаясь на обычном probe-трафике, а не через отдельный тип сообщения:

1. Каждое исходящее `Ping`/`Ack` (и формально `PingReq`) прикрепляет батч свежих
   `Updates` — до N наименее распространённых записей.
2. На приёме сообщения все `Updates` прогоняются через `List.Merge`; записи, где
   `Merge` вернул `changed=true`, помечаются к повторной рассылке (re-gossip).
3. Размер батча и число ретрансляций каждой записи ограничены — без штормов.
4. Тест сходимости: 5–10 узлов на fake-транспорте; новый/ушедший узел становится
   известен всем за ограниченное число раундов.

**Критерий готовности (весь набор обязателен).**
- `~/sdk/go/bin/go build ./...` — чисто.
- `~/sdk/go/bin/go vet ./...` — чисто.
- `~/sdk/go/bin/go test -race ./...` — все тесты зелёные под `-race`.
- `~/sdk/go/bin/gofmt -l .` — пустой вывод.
- Не осталось `TODO(Этап 2)` в затронутых телах (комментарии-ориентиры на будущие
  этапы — можно оставлять/обновлять).
- Все compile-time assertions (`var _ transport.Transport = (*UDP)(nil)`,
  аналог для `Fake`) на месте.

### Что НЕ делать в Этапе 2 (жёсткая граница — не забегать вперёд)

- **PingReq / indirect probing — Этап 3.** Обработку `KindPingReq` по существу
  (выбор K посредников, релей ack) **не** реализовывать. `receiveLoop` по-прежнему
  логирует и игнорирует `KindPingReq`. Пиггибек `Updates` на исходящем `PingReq`
  сейчас **не встаёт в строй**, т.к. `probeOnce` не шлёт `PingReq` в Этапе 2 —
  см. §4.4 (решение зафиксировано: не готовить формальный PingReq-код заранее).
- **Suspicion timeouts / `Clock` / refute — Этап 4.** **Не** вводить `Clock`, **не**
  заводить suspicion-таймеры, **не** реализовывать `suspect→dead` и refute.
  `Merge` уже поддерживает precedence по incarnation — этого достаточно, чтобы
  будущие Suspect/Dead-слухи распространялись; сам механизм переходов — Этап 4.
  В Этапе 2 реально по сети едут только `Alive`-слухи (никто не выставляет Suspect/
  Dead), но код gossip **не должен** зависеть от состояния — он гоняет любые
  изменённые записи, какими бы они ни были.
- **Управляемый drop/delay/partition в fake — Этап 5.** Fake остаётся lossless,
  без задержек, без per-link drop-rate. Тест сходимости работает на надёжной сети.
- **Бинарный wire-формат** — POST_MVP. Остаётся JSON.

Порог односторонний: если по ходу всплывёт необходимость сменить контракт
`Transport`, ввести `Clock` или реализовать indirect probing — **остановиться и
вернуть вопрос**, а не расширять скоуп.

---

## 1. Архитектурные решения Этапа 2 (зафиксированы — реализовать именно так)

### 1.1. Где живёт очередь исходящего gossip

**Решение: буфер ретрансляций живёт внутри `member.List`, под тем же
`sync.RWMutex`.** Не заводить отдельную структуру-очередь в `internal/swim` и не
дублировать блокировку. Обоснование: источник gossip — ровно изменения membership,
которые уже рождаются в `List.Merge`; счётчик ретрансляций логически принадлежит
записи членства и должен меняться атомарно с ней под одним локом (иначе гонка между
«Merge пометил запись свежей» и «PendingGossip уменьшил счётчик»).

Структура данных: **параллельная карта счётчиков ретрансляций** по ID:

```go
type List struct {
    self       ID
    mu         sync.RWMutex
    members    map[ID]*Member
    gossipTx   map[ID]int // remaining retransmissions for this member's latest change
    gossipCap  int        // full retransmit budget assigned on each change (0 => computed)
}
```

- `gossipTx[id] = k` означает «запись `id` ещё нужно разослать `k` раз».
- Когда `Merge(id)` вернул `changed=true` — **сбросить** `gossipTx[id]` в полный
  бюджет (см. §1.3). Даже если запись уже была в очереди — свежее изменение
  перезапускает бюджет с полного значения (последнее изменение приоритетнее).
- Когда запись отдаётся в исходящий батч через `PendingGossip` — **уменьшить**
  `gossipTx[id]` на 1; при достижении 0 — удалить ключ из `gossipTx` (запись
  «отгоссипилась», больше не рассылается, пока её снова не изменит `Merge`).
- `gossipCap` — полный бюджет ретрансляций на одно изменение; вычисляется лениво
  из размера кластера (см. §1.3).

**Инвариант:** `gossipTx` содержит ключи только для записей, ожидающих рассылки.
Записи с исчерпанным бюджетом в карте отсутствуют. Ключ в `gossipTx` всегда имеет
соответствующую запись в `members` (Merge их создаёт вместе).

### 1.2. Что попадает в исходящий батч и в каком порядке

`PendingGossip(limit int) []protocol.Update` (сигнатура — см. §2.2) возвращает до
`limit` записей, выбирая **наименее распространённые первыми** (наибольший
остаток `gossipTx` = меньше всего уже разослано). Обоснование: приоритет
наименее распространённым апдейтам — прямое требование TECHNICAL_PLAN «Этап 2»
(«выбирать самые свежие / наименее распространённые»); это ускоряет сходимость
и снижает вероятность, что свежий слух «застрянет».

Порядок выбора (детерминированный, чтобы тесты были воспроизводимы):
1. Собрать всех кандидатов — ID с `gossipTx[id] > 0`.
2. Отсортировать по убыванию `gossipTx[id]` (больше остаток → раньше);
   при равном остатке — по возрастанию `ID` (стабильный tie-break).
3. Взять первые `limit`, сформировать `[]protocol.Update` из соответствующих
   `members[id]`, уменьшить `gossipTx[id]` на каждую отданную запись (удалить ключ
   при обнулении).

Всё — под `l.mu.Lock()` (write-lock: читаем и мутируем счётчики).

### 1.3. Число ретрансляций и лимит батча — конкретные значения

**Retransmit-бюджет на изменение (`gossipCap`).** Классический SWIM рассылает
каждую дельту `λ·⌈log(N+1)⌉` раз (N — размер кластера), чтобы эпидемия покрыла
кластер с высокой вероятностью. Для учебного проекта берём **`⌈log2(N+1)⌉`,
минимум 3**, где N — число известных членов на момент изменения:

```go
func gossipRetransmitBudget(n int) int {
    // n = number of known members (including self).
    b := int(math.Ceil(math.Log2(float64(n + 1))))
    if b < 3 {
        b = 3
    }
    return b
}
```

Обоснование выбора:
- Логарифм от размера кластера — каноничный для gossip закон (число раундов до
  покрытия растёт как log N); это то, «почему так, а не константа».
- Нижняя граница 3 гарантирует, что в крошечном кластере (2–3 узла) слух всё равно
  повторится достаточно, чтобы пережить одиночные пропуски probe-цикла.
- `log2` вместо натурального лог — просто читаемее в юнит-тесте (log2(7+1)=3);
  множитель λ опускаем — тонкая настройка скорости сходимости не цель Этапа 2, о чём
  явно сказано в требованиях. Зафиксировать это в комментарии у функции.
- N берётся как `len(l.members)` в момент вызова `Merge` (под локом). Кластер
  растёт по мере узнавания новых узлов — бюджет считается по актуальному N.

**Лимит батча (`GossipFanout`).** Сколько апдейтов максимум пиггибекать в одно
сообщение. Значение — константа в `internal/swim`:

```go
// GossipMaxUpdates caps how many membership updates ride on one message, so a
// single datagram stays small and no gossip storm forms. Learning-grade fixed
// value; JSON+UDP on localhost comfortably fits this many Updates.
const GossipMaxUpdates = 6
```

Обоснование: 6 записей JSON (~по ~90 байт каждая) укладываются в один UDP-датаграм
с огромным запасом (лимит 65507). Фиксированная константа достаточна для кластера
5–10 узлов Этапа 2; настраиваемость — не цель. Держать константу в `swim` (там, где
собирается сообщение), а `List.PendingGossip(limit)` принимает лимит параметром,
чтобы `List` не знал про транспортные ограничения.

### 1.4. Self в первом gossip-батче

**Решение: self попадает в кандидаты на gossip при первом исходящем сообщении.**
Механизм — в `NewList(self)` **сразу** проставить `gossipTx[self.ID] = budget`
(бюджет по §1.3 для N=1, т.е. 3). Обоснование: если self не анонсировать, новый
узел останется невидимым для кластера до тех пор, пока какой-нибудь seed сам его не
пропингует — а seed узнаёт о нём только из его же gossip. Без self-анонса join
однобокий. С self-записью в очереди первый же `Ping` к seed-у прикрепит «я жив».

Бесконечного re-gossip себя это не вызывает: self проходит через тот же
retransmit-бюджет — после `budget` рассылок ключ `self.ID` уходит из `gossipTx`, и
self перестаёт гоняться, пока `Merge` его не изменит (в Этапе 2 никто не меняет
self; refute-бамп incarnation — Этап 4). То есть self анонсируется ограниченное
число раз при старте и затихает.

**Важно:** `PendingGossip` **не должен** исключать self из выборки — self это
легитимный член со своей записью в `members`. Единственная запись, которую gossip
трогать не может как источник изменений извне, — но Merge self от чужого слуха с той
же incarnation вернёт `changed=false` (précédence не даст), так что чужой слух про
нас нас не «перевзводит». Отдельного фильтра не нужно.

### 1.5. Куда на приёме писать входящие Updates и как формируется re-gossip

- **На приёме** (любое сообщение с `Updates`): для каждого `Update` вызвать
  `List.Merge(...)`. Merge сам, вернув `changed=true`, взведёт `gossipTx` для этой
  записи (см. §1.1). Значит **явного** «положить в очередь на re-gossip» в swim
  писать не надо — re-gossip обеспечивается тем, что `Merge` наполняет `gossipTx`, а
  следующее исходящее сообщение выгребет его через `PendingGossip`. Это и есть
  эпидемия: принятое изменение автоматически становится кандидатом на пересылку.
- **Порядок в receiveLoop:** входящие `Updates` прогонять через Merge **до** любой
  прочей обработки `Kind` (Ping/Ack), чтобы к моменту формирования Ack-ответа
  свежие изменения уже были в `gossipTx` и могли уехать обратным пиггибеком в том же
  цикле. (Функционально порядок не критичен — Ack всё равно тянет `PendingGossip`
  после Merge, — но обработать gossip первым логичнее и делает поведение
  предсказуемым.)

---

## 2. `internal/member/member.go` — буфер gossip внутри List

Не трогать: `State`, `String()`, `ID`, `Member`, `stateRank`, `snapshot`,
`Members`, `Self`, `Others`. Их поведение остаётся прежним. Изменения точечные.

Импорт: добавить `math` (для `gossipRetransmitBudget`). `protocol` **не**
импортировать в `member` (иначе цикл импортов: `protocol` уже импортирует
`member`). Поэтому `PendingGossip` возвращает **не** `[]protocol.Update`, а срез
самих `member.Member` — конвертацию в `protocol.Update` делает `swim`. См. §2.2 —
скорректированная сигнатура.

### 2.1. Поля и конструктор

Добавить в структуру `List` поля `gossipTx map[ID]int`. `gossipCap` не хранить —
бюджет считается на каждое изменение из актуального `len(members)` (см. §1.3), поле
не нужно; убрать его из наброска §1.1.

```go
type List struct {
    self     ID
    mu       sync.RWMutex
    members  map[ID]*Member
    gossipTx map[ID]int // remaining re-broadcasts per member's latest change
}
```

`NewList(self Member)`:
- инициализировать `gossipTx: map[ID]int{}`;
- после вставки self в `members` — взвести self в очередь:
  `l.gossipTx[self.ID] = gossipRetransmitBudget(len(l.members))` (len == 1 → budget 3).
  Так self анонсируется при первом исходящем сообщении (§1.4).

### 2.2. Изменения в `Merge`

В конце `Merge`, **в ветке, где входящее победило** (прямо перед `return true`,
после записи `l.members[m.ID] = &cp`), взвести retransmit-бюджет:

```go
l.gossipTx[m.ID] = gossipRetransmitBudget(len(l.members))
return true
```

- Бюджет считается ПОСЛЕ вставки/обновления записи, чтобы `len(l.members)` учитывал
  только что добавленного члена.
- Ветки, возвращающие `changed=false` (устаревшая/равная incarnation), `gossipTx`
  **не трогают** — не взводят и не сбрасывают. Слух, не изменивший view, не
  ретранслируется (иначе шторм на стабильном состоянии).

Хелпер (не экспортировать), рядом со `stateRank`:

```go
// gossipRetransmitBudget returns how many times a single membership change should
// be re-broadcast: ceil(log2(N+1)) with a floor of 3, N = known members. The
// logarithmic law is the standard gossip dissemination bound (rounds-to-cover
// grows ~log N); the floor keeps rumors alive in tiny clusters. Fine-tuning
// convergence speed is out of scope for Этап 2.
func gossipRetransmitBudget(n int) int { ... } // см. тело в §1.3
```

### 2.3. Новый метод `PendingGossip`

```go
// PendingGossip returns up to limit membership records that still need to be
// disseminated, least-spread first (largest remaining retransmit budget), and
// decrements each returned record's budget. A record whose budget reaches zero is
// dropped from the gossip queue until Merge marks it changed again. Returns a
// snapshot (copies), safe to encode after the lock is released.
func (l *List) PendingGossip(limit int) []Member {
    l.mu.Lock()
    defer l.mu.Unlock()
    // 1. collect ids with gossipTx[id] > 0
    // 2. sort: desc by gossipTx[id], tie-break asc by ID (deterministic)
    // 3. take first `limit`; for each: append copy of *members[id],
    //    gossipTx[id]--; if gossipTx[id] == 0 { delete(gossipTx, id) }
    // 4. return the slice (len 0 => return empty slice, not nil, для единообразия
    //    с существующими snapshot-методами)
}
```

- `limit <= 0` → вернуть пустой срез, ничего не декрементировать (защитный кейс).
- Возвращает `[]Member` (копии значений из `*members[id]`), НЕ указатели — вызывающий
  не должен мутировать внутренние записи мимо мьютекса (инвариант из Этапа 1).
- Конвертацию `member.Member → protocol.Update` делает `swim` (см. §4.1).

---

## 3. `internal/protocol/protocol.go` — без структурных изменений

Структуры `Update`, `Message`, `Encode`, `Decode` **уже** готовы к gossip
(поле `Updates []Update` с `omitempty`, `Encode`=`json.Marshal` сериализует его).
**Кода менять не нужно**, кроме косметики:

- Убрать/обновить устаревший комментарий-ориентир `TODO(Этап 2): include
  piggybacked Updates` над `Encode` — теперь Updates заполняются вызывающим
  (`swim`), а не самим `Encode`. Заменить на нейтральный комментарий о том, что
  Encode сериализует Message как есть, включая любые Updates, которые проставил
  вызывающий.
- Больше в этом файле ничего.

Функция-конвертер `member.Member → protocol.Update` **не** живёт в `protocol`
(чтобы не тянуть логику членства в протокол). Её место — в `swim` (§4.1) или как
свободная функция в `protocol`, принимающая примитивы. **Решение: конвертер в
`swim`**, приватная функция `toUpdate(m member.Member) protocol.Update` — она уже
импортирует оба пакета.

---

## 4. `internal/swim/swim.go` — пиггибек на исходящих, прогон входящих

Не трогать структуру циклов `Run`/`probeLoop`/`probeOnce`-скелет, `Config`,
`NewNode`, механику `pending`/`seqNo`. Изменения — точечные: сбор Updates перед
Encode и прогон Updates после Decode.

### 4.1. Хелперы (добавить в пакет)

```go
// GossipMaxUpdates caps updates per message (see план §1.3).
const GossipMaxUpdates = 6

// toUpdate projects a membership record onto the wire form.
func toUpdate(m member.Member) protocol.Update {
    return protocol.Update{
        ID:          m.ID,
        Addr:        m.Addr,
        Incarnation: m.Incarnation,
        State:       m.State,
    }
}

// collectGossip pulls the next outbound batch from the list and projects it.
func (n *Node) collectGossip() []protocol.Update {
    pending := n.list.PendingGossip(GossipMaxUpdates)
    if len(pending) == 0 {
        return nil // omitempty keeps the wire clean
    }
    ups := make([]protocol.Update, 0, len(pending))
    for _, m := range pending {
        ups = append(ups, toUpdate(m))
    }
    return ups
}

// applyGossip merges every piggybacked update into the local view. Records that
// change get re-queued for gossip automatically inside List.Merge.
func (n *Node) applyGossip(ups []protocol.Update) {
    for _, u := range ups {
        n.list.Merge(member.Member{
            ID:          u.ID,
            Addr:        u.Addr,
            Incarnation: u.Incarnation,
            State:       u.State,
        })
    }
}
```

### 4.2. Изменения в `receiveLoop`

Внутри цикла, **сразу после успешного `Decode` и до `switch msg.Kind`**, прогнать
входящий gossip:

```go
n.applyGossip(msg.Updates) // Этап 2: absorb piggybacked rumors first
```

Затем в ветках `switch`:

- **`KindPing`:** при формировании Ack прикрепить исходящий батч:
  ```go
  ack := protocol.Message{
      Kind:    protocol.KindAck,
      From:    n.list.Self(),
      SeqNo:   msg.SeqNo,
      Updates: n.collectGossip(),
  }
  ```
  Остальная логика (Encode, Send по `pkt.Addr`) без изменений.
- **`KindAck`:** обработка `pending`/сигнал в канал — без изменений. Gossip из
  входящего Ack уже впитан общим `applyGossip` выше. Дополнительно ничего слать не
  надо (Ack — терминальное сообщение).
- **`KindPingReq`:** без изменений — по-прежнему только лог «not implemented until
  Этап 3». `applyGossip` выше уже впитал его Updates (даже неисполняемый PingReq
  может нести полезные слухи — впитать их бесплатно и правильно; но **отвечать** на
  PingReq в Этапе 2 не надо).

### 4.3. Изменения в `probeOnce`

При формировании исходящего `Ping` прикрепить батч:

```go
msg := protocol.Message{
    Kind:    protocol.KindPing,
    From:    n.list.Self(),
    SeqNo:   seq,
    Updates: n.collectGossip(),
}
```

Остальное (выбор target через `cfg.Rand`, ожидание Ack, cleanup `pending`, Merge
target как alive при Ack) — без изменений.

### 4.4. PingReq и пиггибек — зафиксированное решение

`probeOnce` в Этапе 2 **не** отправляет `KindPingReq` (indirect probing — Этап 3).
Поэтому «готовить формальный пиггибек на исходящем PingReq» сейчас **негде и не
нужно** — исходящих PingReq нет. Когда Этап 3 введёт отправку PingReq, тот код
просто вызовет `n.collectGossip()` при сборке PingReq-сообщения — тем же паттерном,
что Ping/Ack. Явно фиксируем: **в Этапе 2 не добавлять мёртвый код отправки
PingReq ради пиггибека.** На приёме входящий PingReq (если вдруг прилетит от узла
с уже реализованным Этапом 3) свои Updates отдаст в общий `applyGossip` — это уже
покрыто §4.2.

### 4.5. Конкурентность

- `PendingGossip` и `Merge` — оба под `l.mu` внутри `List`; `receiveLoop` (через
  `applyGossip`→`Merge` и `collectGossip`→`PendingGossip`) и `probeOnce` (через
  `collectGossip`) могут работать конкурентно — блокировку держит `List`, гонок нет.
- `n.mu` по-прежнему защищает только `seqNo`/`pending`, gossip его не касается.
- Всё гонять под `-race`.

---

## 5. `cmd/swim-discovery/main.go` — без изменений логики

Запуск узла уже корректен: `NewList(self)` теперь сам взводит self в gossip-очередь
(§2.1), а `Node.Run` уже гоняет probe/receive циклы, которые теперь пиггибекают.
Gossip заработает без правок main. Достаточно:

- Обновить комментарий `TODO(Этап 2): start the gossip dissemination loop` — gossip
  **не** отдельный цикл, он едет на probe-трафике; заменить строку на пояснение, что
  gossip пиггибекается в probe/ack и отдельного цикла не требует. Прочие TODO
  (Этап 3/4/5) оставить.
- Seed-логика (`list.Merge(seed as alive)`) остаётся — она же теперь и взводит
  seed в gossip-очередь через Merge, что полезно.

Никакой протокольной логики в main не добавлять (SKILL.md §1).

---

## 6. Тесты (обязательны; все под `-race`)

### 6.1. `internal/member/member_test.go` — дополнить (не переписывать)

Существующие тесты Merge/Others/Self не трогать. Добавить:

- **`TestList_PendingGossip_SeedsSelf`:** `NewList(self)` → `PendingGossip(10)`
  содержит ровно self (одну запись), состояние alive, поля совпадают с self.
  Обоснование границы: self анонсируется сразу (§1.4).
- **`TestList_PendingGossip_BudgetDecrements`:** после `NewList(self)` бюджет self =
  3 (N=1 → floor 3). Вызвать `PendingGossip(10)` 3 раза подряд — каждый раз self
  возвращается; на 4-й раз `PendingGossip` возвращает пусто (бюджет исчерпан, ключ
  удалён). Проверяет декремент и выбывание из очереди.
- **`TestList_Merge_QueuesChanged`:** `NewList(self)`; исчерпать self-бюджет (3
  вызова PendingGossip). Затем `Merge(new member B, alive)` → `changed=true`.
  `PendingGossip(10)` возвращает B. Проверяет: изменённая запись встаёт в очередь.
- **`TestList_Merge_NoChange_NoQueue`:** `NewList(self)`; исчерпать self-бюджет;
  `Merge(B@inc5 alive)` (changed=true), исчерпать бюджет B (вызвать PendingGossip
  ceil(log2(3+1))=2 раза — N=2 членов на момент Merge B, budget=floor(3)=3, значит
  3 раза; посчитать точно: len(members)=2 → ceil(log2(3))=2 → floor 3 → **3**).
  Затем `Merge(B@inc4 alive)` (устаревшая incarnation, changed=false) →
  `PendingGossip` пусто (не-изменение не ставит в очередь). Фиксирует §2.2: только
  changed=true взводит бюджет.
- **`TestList_PendingGossip_LeastSpreadFirst`:** сконструировать состояние, где у
  двух членов разный остаток бюджета (напр. один раз выгрузить одного из них, чтобы
  его остаток стал меньше), затем `PendingGossip(1)` — вернётся тот, у кого остаток
  БОЛЬШЕ (наименее распространённый). Проверяет порядок сортировки §1.2.
  Практический способ создать разницу: `Merge(B)` и `Merge(C)` (у обоих budget=b),
  `PendingGossip(1)` вернёт одного по tie-break (меньший ID) и уменьшит его остаток;
  следующий `PendingGossip(1)` должен вернуть ДРУГОГО (у него остаток теперь больше).
- **`TestList_PendingGossip_RespectsLimit`:** взвести 5 членов через Merge,
  `PendingGossip(2)` возвращает ровно 2; повторный `PendingGossip(2)` — ещё 2 и т.д.
- **`TestGossipRetransmitBudget`** (table-driven, приватная функция, тест в
  `package member`): N=1→3, N=2→3, N=7→3 (ceil(log2(8))=3), N=15→4 (ceil(log2(16))=4),
  N=31→5. Фиксирует формулу §1.3.

Все проверки — через возвращаемые значения и `PendingGossip`; при необходимости
сверять поля через `Members()`.

### 6.2. `internal/protocol/protocol_test.go` — дополнить

Существующий round-trip оставить. Добавить:

- **`TestEncodeDecode_WithUpdates`:** `Message{Kind: KindPing, From: "A", SeqNo: 1,
  Updates: []Update{{ID:"B", Addr:"B", Incarnation: 2, State: member.StateSuspect},
  {ID:"C", Addr:"C", Incarnation: 5, State: member.StateAlive}}}` → Encode → Decode →
  `reflect.DeepEqual` совпадает, `len(Updates)==2`, поля (в т.ч. `State`) целы.
  Фиксирует, что gossip-батч переживает wire round-trip.
- Существующую проверку `len(got.Updates) != 0 → error` в round-trip БЕЗ Updates
  оставить как есть (пустой батч по-прежнему не попадает в wire благодаря omitempty).

### 6.3. `internal/swim/swim_test.go` — дополнить + главный тест сходимости

Существующие ping/ack тесты не ломать. Внимание: `TestNode_RespondsAckToPing` сейчас
ассертит `len(ack.Updates) != 0 → error` (Этап 1). **Этот ассерт нужно обновить**:
в Этапе 2 Ack на Ping ОТ узла, у которого self взведён в gossip-очередь, будет нести
Updates (как минимум self). Заменить проверку: убедиться, что Ack содержит self-запись
в Updates (или, если проще, снять жёсткий «must be empty» и проверить, что self
присутствует). Зафиксировать в комментарии, что пустой батч больше не инвариант.

Добавить:

- **`TestNode_PingCarriesGossip`:** узел A с self взведённым; вызвать `probeOnce` (или
  проверить через collectGossip напрямую — но лучше через реальный Ping). Простейший
  вариант — white-box: `nodeA.collectGossip()` сразу после NewList содержит self.
  Или интеграционно: A пингует B, B принимает Ping, в B.list появляется запись об A
  (A анонсировал себя через piggyback). Проверяет двусторонность join через gossip.
- **`TestNode_ReceiveAbsorbsGossip`:** сконструировать `Message{Kind: KindPing,
  From:"X", SeqNo: 1, Updates: [{ID:"Z", Addr:"Z", Incarnation:1, State: alive}]}`,
  послать на nodeA через отдельный endpoint; убедиться, что после обработки в
  `listA.Members()` появился Z (входящий gossip впитан через applyGossip). Также
  проверить, что ответный Ack (прочитанный на endpoint отправителя) несёт Updates
  (as минимум self A и/или Z как свежую запись — Z только что стал changed).

- **ГЛАВНЫЙ ТЕСТ ЭТАПА — `TestCluster_GossipConvergence`** (сходимость, требование
  TECHNICAL_PLAN п.4):
  1. Создать `net := transport.NewFakeNetwork()` и **7 узлов** (в диапазоне 5–10),
     адреса `"n0".."n6"`. Для каждого: `Endpoint`, `NewList(self)`, `NewNode` с
     seeded `rand` (разные seed или один — главное детерминизм), большим/умеренным
     `ProbeInterval` и коротким `RTTTimeout`.
  2. **Топология join:** каждый узел знает только о **соседе по кольцу** (n_i знает
     n_{(i+1)%7}) через `list.Merge(neighbor as alive)` — редкая связность, чтобы
     проверить, что слухи расходятся эпидемией, а не потому что все всех знают сразу.
     (Альтернатива: все знают только n0 как seed — тоже валидно; кольцо строже.)
  3. Запустить все `node.Run(ctx)` в горутинах с реальным (маленьким, напр. 10–20ms)
     `ProbeInterval`, чтобы probe-циклы реально крутились и разносили gossip.
     ctx с общим таймаутом (напр. 10s) как предохранитель.
  4. **Условие сходимости:** опрашивать в цикле (с коротким sleep между опросами,
     напр. 5–10ms) `list.Members()` каждого узла, пока **каждый** узел не увидит
     **все 7** ID в состоянии alive, ИЛИ пока не истечёт дедлайн (тогда `t.Fatal`
     с диагностикой — кто кого не увидел). Это фиксирует сходимость «за ограниченное
     число раундов» операционально: сошлось до дедлайна — успех.
  5. После сходимости отменить ctx, дождаться завершения всех `Run`.
  - **Замечание про время:** здесь допустим реальный `time.Ticker`/`Sleep` в опросе —
    это Этап 2, `Clock` (Этап 4) ещё не введён, а тест сходимости по своей природе
    ждёт нескольких probe-раундов. Держать таймаут-предохранитель, чтобы тест падал,
    а не висел. Это соответствует стратегии: детерминизм выбора цели (seeded rand)
    обеспечен, а «сошлось до дедлайна» — устойчивый критерий на lossless-сети.
  - **Опциональный второй сценарий (если время позволит) — уход узла:** после
    сходимости один узел выставить Dead через Merge на каком-то узле и убедиться, что
    Dead-слух доходит до всех (демонстрация распространения не только alive). НЕ
    обязательно для Этапа 2 (Dead-переходы по таймауту — Этап 4), но gossip Dead-
    записи, если её вручную инжектировать, должен расходиться. Пометить тест как
    дополнительный; при нехватке времени пропустить, зафиксировав пропуск.

- **Про `-race`:** тест сходимости запускает 7 узлов конкурентно — обязательно
  зелёный под `go test -race ./internal/swim/`.

### 6.4. Что не тестировать в Этапе 2

- PingReq-обработку/релей (Этап 3).
- Suspect→Dead по таймауту, refute (Этап 4).
- Потерю/задержку пакетов (Этап 5) — сеть в тестах lossless.

---

## 7. Порядок реализации (рекомендуемый)

1. `member.go`: поле `gossipTx`, `gossipRetransmitBudget`, взвод в `NewList` и в
   `Merge`, метод `PendingGossip`. Затем `member_test.go` (§6.1). Прогнать
   `~/sdk/go/bin/go test -race ./internal/member/`.
2. `protocol.go`: только обновить комментарий над `Encode`. `protocol_test.go`
   (§6.2). Прогнать тесты пакета.
3. `swim.go`: `GossipMaxUpdates`, `toUpdate`, `collectGossip`, `applyGossip`;
   вставки в `receiveLoop` (applyGossip + Updates в Ack) и `probeOnce` (Updates в
   Ping). Обновить ассерт в существующем `TestNode_RespondsAckToPing`. Добавить
   тесты §6.3, включая `TestCluster_GossipConvergence`. Прогнать
   `~/sdk/go/bin/go test -race ./internal/swim/`.
4. `main.go`: обновить комментарий про gossip-loop.
5. Финальный прогон: `~/sdk/go/bin/go build ./...`, `~/sdk/go/bin/go vet ./...`,
   `~/sdk/go/bin/gofmt -l .` (пусто), `~/sdk/go/bin/go test -race ./...` (всё
   зелёное).

---

## 8. Чек-лист «не забежать вперёд» (перечитать перед сдачей)

- [ ] `gossipTx` живёт в `member.List` под общим `mu`; отдельной очереди в `swim` нет.
- [ ] `Merge` взводит бюджет только при `changed=true`; не-изменение очередь не трогает.
- [ ] `PendingGossip` отдаёт наименее распространённые первыми, декрементит бюджет,
      удаляет исчерпанные ключи, возвращает копии (не указатели).
- [ ] self взведён в очередь в `NewList` и затихает после исчерпания бюджета
      (не бесконечный re-gossip).
- [ ] Каждое исходящее `Ping` и `Ack` несёт `collectGossip()`; входящие `Updates`
      прогоняются через `applyGossip` до `switch Kind`.
- [ ] `KindPingReq` НЕ обрабатывается по существу; исходящий PingReq НЕ отправляется
      (нет мёртвого пиггибек-кода) — Этап 3.
- [ ] Нет `Clock`, нет suspicion-таймеров, нет refute, нет suspect→dead (Этап 4).
- [ ] Fake остаётся lossless (Этап 5).
- [ ] `TestCluster_GossipConvergence` (5–10 узлов) зелёный под `-race` до дедлайна.
- [ ] Обновлён устаревший ассерт `len(ack.Updates)!=0` в `TestNode_RespondsAckToPing`.
- [ ] `go build`/`go vet`/`gofmt -l`/`go test -race` — всё чисто.
```
