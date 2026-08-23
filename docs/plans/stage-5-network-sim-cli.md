# План Этапа 5 — Симулятор сети + детерминированные false-positive-тесты + CLI observe

Опорные документы: [`../TECHNICAL_PLAN.md`](../TECHNICAL_PLAN.md) (раздел «Этап 5»,
строки ~233-243), хендофф
[`../handoff/2026-08-23-etap4-done-etap5-next.md`](../handoff/2026-08-23-etap4-done-etap5-next.md)
(§2 техдолг, §4 решения Этапа 4, важные для Этапа 5). Конвенции —
[`../../.claude/skills/go-swim-discovery-dev/SKILL.md`](../../.claude/skills/go-swim-discovery-dev/SKILL.md).

Этот план исполняется Fable 5 **одним проходом без диалога**: пути файлов,
сигнатуры и тест-кейсы даны конкретно. После кодинга — Sonnet 5 проверяет
покрытие и живой прогон, затем Opus делает независимое ревью.

---

## 0. Цель и границы Этапа

**Цель.** Дорастить `transport.Fake`/`FakeNetwork` из минимального
lossless-роутера с одним `DropLink` в полноценный **детерминированный симулятор
сети** с управляемыми per-link drop-rate, задержкой и полным partition, всё на
seedable `*rand.Rand`. На этом симуляторе свести false-positive-сценарии под
packet loss в детерминированный набор тестов (высокий loss без indirect probing
→ ложные смерти; с indirect probing → не даёт). Добавить CLI-подкоманду
наблюдения membership для ручного localhost-прогона.

**В границах Этапа:**
1. Расширение `internal/transport/fake.go`: drop-rate (0..1) per-link, delay
   (per-link + глобальный дефолт), partition (симметричная изоляция групп),
   seedable rand. `DropLink` сохраняется как частный случай (см. §3).
2. Детерминированный набор тестов false-positive под loss — часть в
   `internal/transport/fake_test.go` (юниты симулятора), часть в
   `internal/swim/swim_test.go` (протокольные сценарии).
3. CLI: подкоманда `observe` в `cmd/swim-discovery/main.go` — периодическая
   печать membership-view живого узла.

**Вне границ Этапа** (жёсткая граница, см. §7): wire-формат протокола,
precedence-логика `Merge`, публичный контракт `Transport` (4 метода
`Send`/`Receive`/`LocalAddr`/`Close`), eviction Dead-узлов (решение §1.2 —
НЕ вводим), бинарный framing, конфигурируемость таймаутов узла через CLI-флаги.

---

## 1. Явное решение по времени: `Clock` НЕ делится с транспортом; delay идёт на wall-clock. **Решение зафиксировано.**

Хендофф §4 и ревью-altitude Этапа 4 прямо требуют решить это в планировании
Этапа 5, не откладывать. **Решение: `Clock` (`internal/swim`) остаётся там, где
живёт; `transport.Fake` НЕ получает его и НЕ делит с ним временну́ю шкалу.
Задержка в симуляторе реализуется на реальном времени (`time.AfterFunc` /
горутина с реальным `time`), а не через инжектируемый fake-Clock.**

**Обоснование (по фактам текущего кода, не по вкусу):**

- **Delay и suspicion в тестах не пересекаются — сегодня и по этому плану.** Два
  яруса тестов, которые реально касаются времени, устроены так:
  - **Центральный false-positive-тест** (`TestNode_IndirectProbe_*`) поднимает
    **живые** узлы через `startNode` (реальные горутины `Node.Run`,
    `receiveLoop`/`probeLoop` крутятся), а `probeOnce` дёргает напрямую. Таймауты
    RTT/Indirect там идут через **дефолтный `systemClock`** (wall-clock),
    задержки на fake-сети сейчас нулевые. Это wall-clock мир.
  - **Suspicion-тесты** (`TestNode_Suspect*`, фикстура `suspicionFixture`) гоняют
    `fakeClock` и дёргают `suspect()`/`applyGossip()`/`escalate()` **напрямую**,
    без единого живого loop и без транспорта в пути — сеть там не участвует.
  Ни один тест не сводит «delay на транспорте» и «suspicion-дедлайн» под один
  `Advance`. Новые тесты по §6 сохраняют это разделение (delay проверяется на
  wall-clock-таймаутах RTT/Indirect у **живых** узлов; suspicion-эскалация — на
  `fakeClock` у изолированного узла).

- **Сведение delay под fakeClock структурно несовместимо с живыми узлами.**
  Чтобы `Advance` двигал задержку доставки пакета, доставку должен гнать тот же
  fakeClock — но fakeClock срабатывает только при явном `Advance` из теста, а
  живой `receiveLoop` блокируется на `Receive`. Получился бы тест, где надо
  вручную `Advance`-ить каждую фазу probe/ack/relay в правильном порядке при
  работающих горутинах — это ровно тот хрупкий ручной степпинг, которого проект
  избегает. Живые узлы + wall-clock delay проще и уже проверены на Этапах 1-4.

- **`SuspicionTimeout` (деф. 5s) на порядки больше RTT+Indirect (сотни мс) и
  тем более больше моделируемых delay (десятки мс).** Delay в тестах задаётся так,
  чтобы либо укладываться в RTT/Indirect (проба успевает), либо превышать RTT, но
  оставаться много меньше suspicion — то есть delay взаимодействует только с
  probe-таймаутами, которые и так на wall-clock. Общая шкала с suspicion не нужна.

- **Стоимость переезда `Clock` в отдельный пакет не окупается.** Хендофф §4
  фиксирует: `Clock` живёт в `internal/swim` как единственный потребитель.
  Вынести его в `internal/clock` только ради гипотетического общего `Advance`
  раздуло бы scope без единого теста, которому это нужно.

**Что кодер делает по этому решению:** ничего в `internal/swim` не трогает по
части времени. В `transport.Fake` задержка реализуется своим независимым
механизмом на `time` (см. §3), без импорта `internal/swim` (это и запрещено —
transport не может зависеть от swim, иначе цикл импортов).

**Триггер пересмотра (записать в TECHNICAL_PLAN при актуализации):** если
появится тест, где нужно синхронно двигать delay и suspicion одним `Advance`
(например, воспроизвести гонку «пакет refute задержан ровно настолько, что
suspicion успевает эскалировать») — тогда вынести `Clock` в `internal/clock` и
инжектировать его в `transport.Fake`. Пока такого теста нет.

---

## 2. Явное решение по eviction Dead-узлов: НЕ вводим в Этапе 5. **Решение зафиксировано.**

Хендофф §2 и наблюдение Этапа 4 держат eviction как кандидата на Этап 5+.
**Решение: eviction Dead-узлов в Этап 5 НЕ вводится.** Инвариант «из
`member.List.members` ничего не удаляется» сохраняется; `PendingGossip`'s
`*l.members[id]`-разыменование без проверки ключа остаётся корректным как есть;
шум в логе от вечного пробинга Dead-узлов остаётся известным следствием.

**Обоснование:**

- **Scope-несоразмерность.** Этап 5 по TECHNICAL_PLAN — это симулятор сети +
  тесты + CLI. Eviction — самостоятельная задача с собственными развилками:
  когда удалять (сразу по Dead? по таймеру «dead-timeout» после Dead?), как не
  сломать распространение факта смерти (Dead-узел должен пожить в списке, чтобы
  gossip успел разнести смерть — ровно то, ради чего eviction не ввели в Этапе 4),
  как согласовать удаление с `gossipTx` и `suspicions`-картами. Это тянет на
  отдельный Этап, а не «заодно».

- **Риск для центрального инварианта.** Ввести удаление записей из `members`
  прямо в Этапе, чей главный тест — false-positive-suppression, значит смешать
  два независимых изменения в одном diff и усложнить ревью именно того теста,
  который проект бережёт больше всего.

- **`PendingGossip`'s dereference держится на этом инварианте.** Пока ничего не
  удаляется, `*l.members[id]` в `PendingGossip` (member.go, ~строка 215) всегда
  валиден: ключи `gossipTx` — подмножество ключей `members`, и оба под одним
  мьютексом. Eviction сломал бы это (можно удалить из `members`, оставив хвост в
  `gossipTx`), и тогда потребовался бы аудит всех трёх карт — вне scope.

**Что это значит для симулятора и тестов:** тесты Этапа 5 работают на живом или
изолированном узле как есть; Dead-узлы остаются в `Members()`/`Others()`.
CLI-observe (§5) печатает и Dead-узлы — это желаемо (наблюдатель должен видеть
смерть). Логовый шум от повторного пробинга Dead-узлов НЕ чиним в этом Этапе.

**Триггер пересмотра (не менять формулировку в TECHNICAL_PLAN):** появление
потребности вычищать давно-мёртвые узлы из памяти на длинном прогоне (рост
`members` в мониторинге, или реальный многочасовой кластер). Тогда — отдельный
Этап «eviction», и в нём же закрыть логовый шум и проверить `PendingGossip`'s
dereference. До тех пор триггер §2 хендоффа остаётся открытым **без изменений**.

---

## 3. Расширение `transport.Fake` / `FakeNetwork` (`internal/transport/fake.go`)

Все новые управляющие методы висят на `*FakeNetwork` (общий свитч), а не на
отдельных `*Fake`-эндпоинтах — конфигурация сети централизована, как уже сделано
для `DropLink`. Симулятор остаётся потокобезопасным под тем же `n.mu`
(`sync.RWMutex`), которым уже защищены `endpoints`/`dropped`.

### 3.1. Seedable `*rand.Rand` в `FakeNetwork`

- В структуру `FakeNetwork` добавить поле `rng *rand.Rand` и `rngMu sync.Mutex`
  (`*rand.Rand` не concurrency-safe; drop-решение считается в горячем `Send`,
  возможно из нескольких горутин-отправителей одновременно — отдельный маленький
  мьютекс вокруг единственного `rng.Float64()`, не расширять им `n.mu`).
- Конструкторы:
  - `NewFakeNetwork() *FakeNetwork` — **сохранить сигнатуру** (её зовут во всех
    существующих тестах Этапов 1-4). Внутри засеивать rng детерминированно
    фиксированным seed по умолчанию (например `rand.NewSource(1)`), чтобы
    поведение существующих lossless-тестов не менялось (при drop-rate 0 rng
    вообще не влияет, но детерминизм по умолчанию — правило проекта).
  - `NewFakeNetworkSeed(seed int64) *FakeNetwork` — новый конструктор для тестов,
    которым нужен конкретный seed drop-распределения. Делегирует общей внутренней
    инициализации.
- Импортировать `math/rand` и `time` в fake.go (time — для delay, см. 3.4).

### 3.2. Per-link drop-rate

- Заменить поле `dropped map[[2]string]bool` на
  `dropRate map[[2]string]float64` (направленный линк `[from,to]` → вероятность
  потери в [0,1]).
- Новый метод:
  ```go
  // SetDropRate makes each packet sent from `from` to `to` vanish with
  // probability p (0 = never, 1 = always), one-way. Deterministic given the
  // network's seed. p is clamped to [0,1].
  func (n *FakeNetwork) SetDropRate(from, to string, p float64)
  ```
- **`DropLink(from, to)` сохраняется** как тонкая обёртка
  `n.SetDropRate(from, to, 1.0)` — не удалять: её зовёт центральный тест Этапа 3
  `TestNode_IndirectProbe_SuppressesFalsePositive` и `TestFake_DropLink`. Оставить
  её doc-комментарий, но обновить: теперь это частный случай `SetDropRate`, а не
  «единственная ручка».
- В `Send` заменить булеву проверку `drop := ...dropped[...]` на:
  ```go
  rate := n.dropRate[[2]string{f.local, addr}]
  // + partition-проверка, см. 3.3
  ```
  и после снятия `n.mu.RUnlock()` — розыгрыш под `rngMu`:
  ```go
  if rate > 0 && (rate >= 1 || n.roll() < rate) { return nil } // dropped
  ```
  где `roll()` под `rngMu` возвращает `n.rng.Float64()`. `rate >= 1` спрямляет
  100%-дроп без обращения к rng (сохраняет детерминизм `DropLink` независимо от
  того, сколько раз rng дёрнули до этого).

### 3.3. Partition (симметричная изоляция групп)

- Модель: набор именованных партиций, где узел в одной партиции не может слать
  узлу в другой (обе стороны). Реализация — множество направленных «cut» пар,
  выведенное из групп, ИЛИ прямая проверка «в одной ли группе».
- Хранение: `partitionOf map[string]int` (addr → номер партиции; отсутствие ключа
  = «партиция по умолчанию 0», видит всех в дефолтной группе). Проще и нагляднее
  cut-множества.
- Методы:
  ```go
  // Partition puts the given addrs into an isolated group identified by id:
  // packets between a member of this group and any node NOT in it vanish (both
  // directions). Calling Partition again reassigns membership. id 0 is the
  // default group every unlisted node belongs to.
  func (n *FakeNetwork) Partition(id int, addrs ...string)

  // Heal removes all partitioning: every node returns to the default group and
  // all links carry again (drop-rates set via SetDropRate stay in effect).
  func (n *FakeNetwork) Heal()
  ```
- Правило доставки в `Send`: пусть `gf = partitionOf[f.local]` (0 если нет),
  `gt = partitionOf[addr]` (0 если нет). Если `gf != gt` — пакет пропадает
  (partition drop), не доходя до розыгрыша drop-rate. Partition — жёстче rate:
  сначала проверяем partition, потом rate.
- Взаимодействие с drop-rate: независимы. Partition режет между группами
  безусловно; drop-rate добавляет вероятностную потерю на линках, которые
  partition не режет.

### 3.4. Delay (per-link + глобальный дефолт)

- Хранение: `delay map[[2]string]time.Duration` (направленный per-link delay) и
  `defaultDelay time.Duration` (применяется, если для линка нет своего).
- Методы:
  ```go
  // SetDelay makes packets from `from` to `to` arrive after d (one-way).
  // Overrides the network default for that link. Zero d restores immediate
  // delivery on that link.
  func (n *FakeNetwork) SetDelay(from, to string, d time.Duration)

  // SetDefaultDelay sets the delay applied to every link that has no explicit
  // SetDelay. Zero (the constructor default) means immediate delivery.
  func (n *FakeNetwork) SetDefaultDelay(d time.Duration)
  ```
- Реализация в `Send`: после того как пакет решено доставить (не dropped, не
  partitioned), вычислить `d` (per-link, иначе default). Если `d == 0` —
  доставлять как сейчас (прямая отправка в `dst.inbound`). Если `d > 0` —
  доставку отложить **на реальном времени**, не блокируя отправителя:
  ```go
  time.AfterFunc(d, func() {
      select {
      case dst.inbound <- pkt:
      case <-dst.done:
      }
  })
  return nil
  ```
  **Важно:** `Send` не должен блокироваться на delay (иначе probe-горутина
  инициатора зависнет на всю задержку и wall-clock RTT-таймаут отсчитается
  неверно). `time.AfterFunc` доставляет из своей горутины — отправитель
  возвращается сразу, как настоящий UDP.
- **Порядок и гонки при delay:** delayed-доставки могут переупорядочиться
  относительно immediate — это корректно моделирует сеть и не ломает протокол
  (SWIM устойчив к переупорядочиванию). Копирование payload (`p := make(...)`)
  уже есть и обязано остаться до планирования `AfterFunc` — буфер не должен
  делиться. Проверка `dst.done` в `select` защищает от паники при доставке в
  закрытый эндпоинт (та же, что в текущем коде).
- **Утечка горутин/таймеров при `Close`:** `time.AfterFunc` держит таймер до
  срабатывания. На масштабе тестов (десятки пакетов, delay десятки мс) это
  безвредно — таймеры отрабатывают и собираются GC, доставка в закрытый `done`
  отсекается через `select`. Не заводить реестр таймеров ради Stop: избыточно для
  scope. (Записать это как осознанное решение в комментарии рядом с `AfterFunc`.)

### 3.5. Итоговая структура `FakeNetwork` (сводка полей)

```go
type FakeNetwork struct {
    mu           sync.RWMutex
    endpoints    map[string]*Fake
    dropRate     map[[2]string]float64
    delay        map[[2]string]time.Duration
    defaultDelay time.Duration
    partitionOf  map[string]int
    rngMu        sync.Mutex
    rng          *rand.Rand
}
```

`Endpoint`, `Receive`, `LocalAddr`, `Close` — **без изменений** контракта. `Send`
переписывается внутри (drop-rate + partition + delay), но сигнатура и семантика
best-effort сохраняются.

---

## 4. Что НЕ трогать в `internal/swim`, `internal/member`, `internal/protocol`

Симулятор — исключительно в `internal/transport`. По §1 и §2:

- `internal/swim/swim.go` — **не трогать** (Clock не делится, delay туда не
  протекает). `probeOnce`, `waitAck`, suspicion — как есть.
- `internal/member/member.go` — **не трогать** (eviction не вводим; `PendingGossip`
  dereference остаётся). Единственное возможное касание — если CLI (§5) захочет
  read-only снапшот, он берёт уже существующий `List.Members()` (публичный,
  возвращает копию под `RLock`). Ничего нового в member.go не добавлять.
- `internal/protocol/protocol.go` — **не трогать** (wire-формат вне scope).

---

## 5. CLI-подкоманда observe (`cmd/swim-discovery/main.go`)

**Форма вызова.** Ввести подкоманды через первый позиционный аргумент, сохранив
обратную совместимость с текущим запуском узла:

- `swim-discovery run -addr ... -seed ...` — запуск узла (текущее поведение,
  вынести из голого `main` в функцию `runNode`).
- `swim-discovery observe -addr ... -seed ... [-interval 1s]` — запуск узла
  **плюс** периодическая печать его membership-view в stderr/stdout.
- **Обратная совместимость:** если первый аргумент начинается с `-` (флаг) или
  отсутствует, считать командой `run` по умолчанию — чтобы `swim-discovery -addr
  ...` (как в живых прогонах Этапов 1-4 и в хендоффе) продолжал работать без
  изменения инструкций.

Разбор — стандартный `flag` без внешних зависимостей: определить подкоманду по
`os.Args[1]`, затем `flag.NewFlagSet` на остаток аргументов. Никакой
протокольной логики в main — только сборка узла и печать снапшота (правило
«тонкий main»).

**Что печатает observe.** Каждые `-interval` (дефолт 1s) — снимок
`list.Members()` (уже сортирован по ID, уже копия под `RLock` — доступ к живому
membership без гонки и без блокировки основного цикла узла). Формат — по строке
на узел, человекочитаемо:

```
[observe 127.0.0.1:7947] t=3s  members=3
  127.0.0.1:7947   alive    inc=0   (self)
  127.0.0.1:7948   alive    inc=0
  127.0.0.1:7949   suspect  inc=0
```

- Состояние берётся из `Member.State.String()` (уже есть: alive/suspect/dead).
- `inc` — `Member.Incarnation`. `(self)` — если `m.ID == list.Self()`.
- Печатать через `fmt.Fprintf(os.Stdout, ...)` или `log` — согласовать с текущим
  стилем (в main используется `log`; для табличного вывода лучше `fmt` в stdout,
  чтобы не засорять лог префиксами времени, — выбрать `fmt`).

**Как устроен цикл observe без блокировки узла.** Узел уже крутится в
`node.Run(ctx)` (блокирующий). Observe-принтер запустить в отдельной горутине до
`node.Run`, гасить тем же `ctx`:

```go
if command == "observe" {
    go observeLoop(ctx, list, *interval)
}
node.Run(ctx)
```

где
```go
func observeLoop(ctx context.Context, list *member.List, interval time.Duration) {
    ticker := time.NewTicker(interval)
    defer ticker.Stop()
    start := time.Now()
    for {
        select {
        case <-ticker.C:
            printMembers(list, start)
        case <-ctx.Done():
            return
        }
    }
}
```

`printMembers` зовёт `list.Members()` (read-only, `RLock` внутри) — не трогает
`node`, не мешает probe/receive-loop. Dead-узлы печатаются (см. §2 — наблюдатель
должен видеть смерть).

**Границы CLI:** без TUI, без цвета, без интерактива — простой периодический
дамп, достаточный для ручного localhost-прогона. Конфигурация таймаутов узла
через флаги — вне scope (см. §0); observe добавляет только `-interval`.

---

## 6. Тест-кейсы (перечень, не код)

Два яруса, оба под `-race`. Симуляторные юниты — в `internal/transport/fake_test.go`;
протокольные false-positive-сценарии — в `internal/swim/swim_test.go`.

### 6.1. Юниты симулятора (`internal/transport/fake_test.go`)

Существующие тесты (`TestFake_SendReceive`, `_SendUnknownAddr`, `_Close`,
`_DropLink`, `_PayloadCopied`) **должны продолжать проходить без изменений** —
`DropLink` и lossless-путь сохранены. Новые:

1. **`TestFake_SetDropRate_Deterministic`** — `NewFakeNetworkSeed(seed)`,
   `SetDropRate("A","B", 0.5)`. Отправить N=1000 пакетов A→B, посчитать
   доставленные (дренаж `B.Receive` с коротким ctx-таймаутом или неблокирующий
   счётчик). Ожидание: доля потерь около 0.5 (допуск, напр. в [0.4, 0.6]) И при
   **том же seed** результат **побитово воспроизводим** между двумя прогонами
   (два `NewFakeNetworkSeed(1)` дают одинаковую последовательность доставок).
   Второе — главное: детерминизм, не статистика.
2. **`TestFake_SetDropRate_ZeroAndOne`** — `SetDropRate(...,0)` доставляет всё
   (эквивалент lossless); `SetDropRate(...,1)` не доставляет ничего (эквивалент
   `DropLink`); направленность сохраняется (обратный линк цел).
3. **`TestFake_Partition_Isolates`** — три узла A,B,C. `Partition(1, "A","B")`
   (C в дефолтной группе 0). Ожидание: A↔B доставляют; A↔C и B↔C — нет (обе
   стороны). `Heal()` — все линки снова доставляют.
4. **`TestFake_Delay_ArrivesLate`** — `SetDelay("A","B", 50*time.Millisecond)`.
   `A.Send` возвращается немедленно (замер `time.Since` < delay — доказать
   неблокирующесть). `B.Receive` с ctx-таймаутом 10ms сразу после Send — НЕ
   получает (ещё в полёте); `B.Receive` с ctx-таймаутом > delay — получает.
   `SetDefaultDelay` покрыть отдельным под-кейсом (линк без явного SetDelay берёт
   дефолт).
5. **`TestFake_Delay_Reordering`** (опционально, если дёшево) — два пакета A→B:
   первый с delay 50ms, второй immediate; второй приходит первым. Подтверждает,
   что переупорядочивание моделируется и не паникует.

### 6.2. Протокольные false-positive-сценарии (`internal/swim/swim_test.go`)

Живые узлы через `startNode`, wall-clock RTT/Indirect-таймауты (дефолтный
`systemClock`) — тот же паттерн, что у существующего
`TestNode_IndirectProbe_SuppressesFalsePositive`. **fakeClock здесь НЕ
используется** (delay на wall-clock, см. §1). Числа подобрать так, чтобы
RTTTimeout был коротким (проба гарантированно уходит в indirect-фазу), а
IndirectTimeout — щедрым (успевает пройти релей).

6. **`TestNode_HighDropRate_NoIndirect_MarksSuspect`** (негативный, показывает
   проблему без indirect) — два узла I, T. `SetDropRate("I","T", 1.0)` (100% —
   детерминированно, как `DropLink`, но через новую ручку). `IndirectNodes: 0`
   (indirect выключен). Один `probeOnce` инициатора: T переходит в **Suspect**
   (ложное подозрение — линк I→T мёртв, но T жив). Это демонстрация «без indirect
   — ложная смерть». Использовать 100% (`1.0`), а не дробную вероятность, чтобы
   тест был детерминированным без зависимости от seed.
7. **`TestNode_HighDropRate_WithIndirect_SuppressesFalsePositive`** (позитивный,
   центральный инвариант проекта) — три узла I, M, T. `SetDropRate("I","T", 1.0)`
   (только прямой линк инициатора к цели мёртв); `SetDropRate("T","I", 1.0)`
   при желании (чтобы и Ack прямой не проходил) — но M↔T и I↔M целы.
   `IndirectNodes ≥ 1`. Один `probeOnce`: T остаётся **Alive** — indirect через M
   гасит false positive. Это тот же инвариант, что
   `TestNode_IndirectProbe_SuppressesFalsePositive`, но сформулированный через
   новую `SetDropRate`-ручку, подтверждающий, что расширенный симулятор его не
   сломал. Ассертить, как в оригинале, что `probeOnce` реально сжёг RTT (elapsed
   ≥ RTTTimeout/2) — иначе проба ушла не в T.
8. **`TestNode_Delay_WithinRTT_NoSuspect`** — два узла I, T (или I, T, M).
   `SetDelay` на линке I↔T величиной **меньше** RTTTimeout. `probeOnce`: прямой
   Ack успевает вернуться внутри RTT — T остаётся Alive, indirect не
   запускается. Подтверждает, что умеренная задержка не даёт false positive.
   Требует RTTTimeout заметно больше delay (напр. delay 20ms, RTTTimeout 300ms).
9. **`TestNode_Delay_ExceedsRTT_IndirectRescues`** — I, M, T. `SetDelay` на
   прямом линке I→T **больше** RTTTimeout (прямой Ack опаздывает), но линки через
   M — быстрые (нулевой delay). `IndirectTimeout` щедрый. `probeOnce`: прямая
   фаза истекает по RTT, indirect через M успевает — T остаётся **Alive**.
   Показывает, что задержка на одном линке гасится indirect-путём, как и потеря.
10. **`TestNode_Partition_MarksSuspect`** (опционально, демонстрация partition в
    протоколе) — I, T (+ опц. M на стороне I). `Partition` изолирует T от группы
    I так, что ни прямой, ни indirect (через M, если он на стороне I) не проходит.
    `probeOnce`: T → **Suspect** (genuinely unreachable — партиция реальна). Это
    негативный аналог false-positive: когда узел действительно недостижим, система
    обязана его заподозрить. Держать таймауты короткими (все ожидания —
    настоящие таймауты, исход timing-независим, как в
    `TestNode_IndirectProbe_AllFail_MarksSuspect`).

**Устойчивость.** Все живые тесты 6-10 прогнать `-race -count=200` (паттерн
Этапов 3-4). Тайминги подбирать с запасом: RTTTimeout короткий, но не настолько,
чтобы флейкать на медленной CI; IndirectTimeout щедрый (секунды на fake — это
микросекунды реального времени плюс явный delay).

### 6.3. CLI — проверка

CLI-observe проверяется **живым прогоном** (Sonnet, шаг 4 пайплайна), не
юнит-тестом: запустить 3 процесса на localhost, у одного команду `observe`, убить
один узел, глазами убедиться, что observe печатает переход alive→suspect→dead.
Опционально — вынести `printMembers` в тестируемую функцию (форматирование
снапшота в строку) и покрыть её табличным юнитом форматирования, если это дёшево;
не обязательно.

---

## 7. Что НЕ трогать / вне рамок (жёсткая граница)

- **Публичный контракт `transport.Transport`** — 4 метода
  `Send`/`Receive`/`LocalAddr`/`Close`. Симулятор продолжает их реализовывать
  без изменения сигнатур. Compile-time assertion `var _ Transport = (*Fake)(nil)`
  **не удалять**.
- **`transport.UDP`** — прод-адаптер не трогать вовсе.
- **`internal/swim`** — не трогать (Clock не делится, delay туда не течёт).
- **`internal/member`** — не трогать (eviction не вводим, `PendingGossip`
  dereference остаётся, `Merge`-precedence неприкосновенна).
- **`internal/protocol`** — wire-формат не трогать.
- **`DropLink`** — не удалять (частный случай `SetDropRate(...,1.0)`, зовётся
  существующими тестами).
- **Eviction Dead-узлов** — не вводить (§2). Логовый шум от пробинга Dead-узлов
  не чинить.
- **Конфиг таймаутов узла через CLI** — вне scope; observe добавляет только
  `-interval`.
- **Существующие тесты Этапов 1-4** — должны продолжать проходить без правок
  (кроме, возможно, механической замены `DropLink` на `SetDropRate(...,1.0)` в
  новом коде — старые вызовы `DropLink` оставить как есть).

---

## 8. Затрагиваемые файлы (сводка)

| Файл | Что делаем |
|---|---|
| `internal/transport/fake.go` | Расширить `FakeNetwork`: `SetDropRate`, `Partition`/`Heal`, `SetDelay`/`SetDefaultDelay`, seedable rng, `NewFakeNetworkSeed`; переписать `Send` (drop-rate+partition+delay); `DropLink` → обёртка над `SetDropRate(...,1.0)`. `Endpoint`/`Receive`/`LocalAddr`/`Close` без изменений. |
| `internal/transport/fake_test.go` | Новые юниты 1-5 (§6.1); существующие 5 тестов не трогать. |
| `internal/swim/swim_test.go` | Новые протокольные тесты 6-10 (§6.2) на новых ручках симулятора; существующие тесты не трогать. |
| `cmd/swim-discovery/main.go` | Подкоманды `run`/`observe` (дефолт `run` для обратной совместимости), `observeLoop`/`printMembers`, флаг `-interval`. Убрать `// TODO(Этап 5)`. |
| `docs/TECHNICAL_PLAN.md` | (шаг 7 пайплайна, не кодером) Актуализировать раздел «Этап 5»: зафиксировать решения §1 (Clock не делится) и §2 (eviction не вводим) с триггерами. |

Пакеты стандартной библиотеки, которые понадобятся новыми (все уже разрешены
проектом): `math/rand`, `time` в `fake.go`; `flag`/`fmt`/`time` в `main.go` (flag
и time уже импортированы).

---

## 9. Критерий готовности

- `go build ./...`, `go vet ./...`, `gofmt -l .` (пусто) — чисто.
- `go test ./...` — зелено; `go test -race ./...` — зелено.
- `go test -race -count=200 ./internal/swim/...` — зелено (флейков нет), включая
  новые false-positive-тесты 6-10.
- `go test -race -count=200 ./internal/transport/...` — зелено (детерминизм
  drop-rate/delay/partition).
- **Центральный инвариант подтверждён на новой ручке:** тест 7
  (`SetDropRate 1.0` на I→T, indirect через M) держит T Alive; тест 6 без
  indirect даёт Suspect — контраст «indirect гасит false positive» показан
  детерминированно.
- **Живой прогон:** 3 процесса на localhost, один в режиме `observe`; убить узел
  → observe печатает alive→suspect→dead; вывод глазами прочитан на предмет
  странностей (правило хендоффа §3). Обратная совместимость: `swim-discovery
  -addr ...` (без подкоманды) по-прежнему запускает узел.
- Решения §1 и §2 отражены в `docs/TECHNICAL_PLAN.md` (раздел «Этап 5») с
  триггерами пересмотра.

---

## 10. Саммари ключевых решений (для ревьюера и кодера)

1. **`Clock` НЕ делится между `internal/swim` и `transport.Fake`.** Delay в
   симуляторе — на wall-clock (`time.AfterFunc`, неблокирующая доставка). Причина:
   delay и suspicion в тестах не пересекаются; сведение delay под fakeClock
   несовместимо с живыми узлами и раздуло бы scope. Триггер пересмотра — тест,
   требующий синхронного `Advance` для delay и suspicion (пока такого нет).
2. **Eviction Dead-узлов НЕ вводится в Этапе 5.** Инвариант «из `members` ничего
   не удаляется» цел, `PendingGossip` dereference корректен, логовый шум остаётся
   известным следствием. Причина: eviction — самостоятельный Этап со своими
   развилками, смешивать его с симулятором и центральным false-positive-тестом
   рискованно. Триггер §2 хендоффа остаётся открытым без изменений.
3. **`transport.Fake` растёт из `fake.go`:** `SetDropRate`(per-link 0..1),
   `Partition`/`Heal`, `SetDelay`/`SetDefaultDelay`, seedable rng через
   `NewFakeNetworkSeed`. `DropLink` сохраняется как `SetDropRate(...,1.0)`.
   Контракт `Transport` неизменен.
4. **CLI:** подкоманды `run`/`observe` (дефолт `run` для обратной
   совместимости), `observe` печатает снапшот `List.Members()` каждые `-interval`
   из отдельной горутины — read-only, без блокировки цикла узла.
