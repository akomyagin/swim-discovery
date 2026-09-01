# План Этапа 6 — Eviction Dead-узлов из membership-списка

Опорные документы: [`../TECHNICAL_PLAN.md`](../TECHNICAL_PLAN.md) (раздел «Этап 4»,
строки ~196-209 — техдолг eviction; раздел «Этап 5», строки ~254-258 — второе
осознанное откладывание), хендофф
[`../handoff/2026-08-23-etap5-done-mvp-next.md`](../handoff/2026-08-23-etap5-done-mvp-next.md)
(§2 «остаётся открытым», §5 п.2 — eviction первый кандидат на отдельный Этап).
Конвенции — [`../../.claude/skills/go-swim-discovery-dev/SKILL.md`](../../.claude/skills/go-swim-discovery-dev/SKILL.md).

Этот план исполняется Fable 5 **одним проходом без диалога**: пути файлов,
сигнатуры методов и имена тест-кейсов даны конкретно. После кодинга — Sonnet 5
проверяет покрытие и живой прогон, затем Opus делает независимое ревью.

---

## 0. Цель и границы Этапа

**Цель.** Закрыть техдолг, дважды осознанно отложенный (Этап 4 и Этап 5):
Dead-узел, пожив в списке ровно столько, чтобы факт смерти успел разойтись по
gossip, затем **удаляется** из `member.List` (evicted). Это снимает вечный рост
`members` на длинных прогонах, гасит логовый шум от бесконечного пробинга
Dead-узлов и одновременно закрывает скрытый инвариант «из `members` ничего не
удаляется», на котором до сих пор держался `PendingGossip`'s dereference.

**В границах Этапа:**
1. Новый метод `member.List.Evict(id, inc) (evicted bool)` — атомарное удаление
   записи из `members` **и** `gossipTx` под общим мьютексом, с precedence-guard
   (эвиктим только всё ещё Dead-запись на той же incarnation). См. §2, §4.1.
2. Eviction-таймер внутри `swim.Node` — parallel-механизм к suspicion-таймеру:
   `evictMu`, `evictions map[member.ID]*evictionTimer`, `armEviction`/
   `cancelEviction`/`evict`, взводится на переходе узла в Dead. См. §1, §3, §4.2.
3. Новое конфиг-поле `Config.DeadTimeout` (grace-период Dead→evicted) с
   дефолтом и обоснованием. См. §5.
4. Фильтрация Dead-целей из выбора пробинга в `probeOnce` — устранение логового
   шума в переходном окне до эвикции. См. §6, §4.2.
5. Детерминированные тесты на `fakeClock` (eviction-таймер, revive-гонка,
   `PendingGossip` не паникует при эвикции). См. §7.

**Вне границ Этапа** (жёсткая граница, см. §9): wire-формат протокола и
`protocol.Update` (eviction — чисто локальное решение о памяти, на wire ничего
нового не едет; не бывает «tombstone»-сообщения), публичный контракт
`transport.Transport`, `Merge`-precedence (правило (Incarnation,State) не
трогаем), CLI-флаги конфигурации таймаутов узла, бинарный framing. `DeadTimeout`
через CLI-флаг **не** выставляется (как и остальные таймауты — осознанно вне
scope, см. хендофф Этапа 5 §4).

---

## 1. Развилка 1 — где живёт таймер eviction. **Решение: в `swim.Node`, зеркало suspicion-таймера. Зафиксировано.**

**Решение.** Eviction-таймер живёт в `swim.Node` тем же паттерном, что
suspicion-таймер Этапа 4: отдельный мьютекс `evictMu`, карта
`evictions map[member.ID]*evictionTimer`, тройка `armEviction`/`cancelEviction`/
`evict`, взводимая на моменте, когда узел становится Dead в локальном view.
Само **удаление** записи делает новый метод `member.List.Evict` (List — владелец
своих карт), но **когда** удалять решает Node через свой Clock-driven таймер.

**Обоснование (по существующим границам ответственности, не по вкусу):**

- **`member.List` = чистая структура данных + precedence, без времени.**
  В `member.go` нет ни одного обращения к времени для *решений* (`time` там —
  только для диагностического `StateChangedAt`, который явно «Diagnostics only»,
  member.go строки 60-67). Ввести в `List` самоочистку по TTL значило бы протащить
  в него понятие «сейчас» и клок — прямо против зафиксированного разделения
  «`List` таймингом не занимается, таймеры живут в `swim.Node`» (member.go
  строки 64-67, TECHNICAL_PLAN Этап 4). Вариант «`List` сам чистит старьё при
  следующем обращении» отвергнут: он завязывает удаление на частоту обращений
  (ленивая чистка), недетерминированен без инжектируемого клока в `List`, и
  ломает то самое разделение.

- **`swim.Node` уже владеет ровно этим паттерном.** `suspicions` +
  `armSuspicion`/`cancelSuspicion`/`escalate` (swim.go строки 105-120, 554-627)
  — это уже «Clock-driven, idempotent per (ID,incarnation), arm/cancel/fire,
  результат применяется через `member.List` под precedence-guard». Eviction —
  тот же скелет со сдвигом на один шаг жизненного цикла: suspicion переводит
  Suspect→Dead, eviction переводит Dead→(нет записи). Переиспользуем структуру
  один-в-один, включая мотивацию отдельного мьютекса (не расширять горячую
  seqNo/pending/Rand-секцию; callbacks из Clock-горутин контендят только между
  собой).

- **Комбинация (часть в List, часть в Node) не нужна.** Разделение чистое:
  Node решает «когда и можно ли» (таймер + precedence-guard), List исполняет
  «удали атомарно под своим мьютексом». Никакой третьей стороны.

**Что кодер делает:** добавляет в `swim.Node` карту `evictions` и тройку
методов по образцу suspicion; добавляет в `member.List` единственный метод
`Evict` (§4.1). `List` не получает ни `Clock`, ни `time`-логики решений.

---

## 2. Развилка 2 — grace-период Dead→evicted. **Решение: `Config.DeadTimeout`, дефолт 10s. Зафиксировано.**

**Решение.** Новое поле `Config.DeadTimeout time.Duration`; нулевое значение →
прод-дефолт **`10 * time.Second`** (устанавливается в `NewNode`, как остальные
таймауты). Отсчёт стартует с момента, когда узел стал Dead в локальном view.

**Обоснование выбора значения (по фактам gossip-механики проекта, не наугад):**

- Grace-период должен покрыть **дораспространение Dead-рамора** прежде, чем узел
  можно безопасно забыть. Бюджет ретрансляций одной membership-дельты —
  `3·ceil(log2(N+1))` (member.go `gossipRetransmitBudget`, строки 118-129), и
  дельта уходит на обычном probe-трафике раз в `ProbeInterval` (дефолт **1s**,
  swim.go строка 129). На малом кластере (N до ~15) бюджет — 12-18 ретрансляций;
  при одной ретрансляции за probe-тик это ~12-18s теоретического максимума, но
  реально дельта расходится эпидемически за O(log N) раундов — единицы секунд.
  **10s даёт запас поверх типичной сходимости Dead-рамора, оставаясь ≥ дефолтного
  `SuspicionTimeout` (5s)** — то есть узел, объявленный Dead, живёт в списке ещё
  как минимум столько же, сколько он жил в Suspect, прежде чем исчезнуть.

- **Инвариант порядка величин (записать в код рядом с дефолтом):**
  `DeadTimeout (10s) > SuspicionTimeout (5s) >> RTTTimeout+IndirectTimeout
  (сотни мс)`. Это продолжение уже зафиксированной лестницы таймаутов Этапа 4
  (swim.go строки 145-151). Смысл: сначала успевает indirect-rescue (не дать
  ложную смерть), потом suspicion-escalation (подтвердить настоящую), и только
  сильно позже — eviction (забыть подтверждённо-мёртвого).

- **Почему не «эвиктить сразу по Dead»** — прямо запрещено обоснованием, ради
  которого eviction не ввели в Этапе 4: Dead-узел обязан пожить в списке, чтобы
  gossip успел разнести смерть; удаление в момент escalate стёрло бы запись до её
  распространения, и другие узлы, ещё не узнавшие о смерти, при следующем контакте
  «воскресили» бы её как неизвестного заново. Grace-период — суть решения.

**Триггер пересмотра (записать в TECHNICAL_PLAN при актуализации):** если появится
конфигурируемость таймаутов узла через CLI или сценарий с крупным кластером
(N в десятки-сотни), где `10s` окажется меньше реального времени сходимости
Dead-рамора — сделать `DeadTimeout` функцией от N (как `gossipRetransmitBudget`)
либо поднять дефолт. Пока фиксированное значение достаточно для localhost-масштаба.

---

## 3. Развилка 3 — судьба `gossipTx[id]` при эвикции. **Решение: удаляется синхронно вместе с `members[id]`, в одном методе под одним мьютексом. Зафиксировано.**

**Решение.** `member.List.Evict` удаляет `members[id]` **и** `gossipTx[id]`
внутри одной критической секции под `l.mu.Lock()`. Оба `delete` — атомарно
относительно любого другого доступа к `List`.

**Обоснование:**

- **Это ровно то, что чинит скрытый инвариант.** `PendingGossip` разыменовывает
  `*l.members[id]` без проверки ключа (member.go строка 215), полагаясь на
  «ключи `gossipTx` ⊆ ключей `members`, оба под одним мьютексом» (member.go
  строки 79-84, 194-220). Если бы `Evict` удалял только из `members`, оставив
  хвост в `gossipTx`, `PendingGossip` словил бы nil-разыменование при
  `*l.members[id]`. Удаляя **оба** ключа под тем же `l.mu`, инвариант
  «gossipTx-ключи ⊆ members-ключей» сохраняется — dereference остаётся корректным
  **по-прежнему**, теперь уже не как «случайно повезло», а как поддержанный
  инвариант. **Записать это в doc-комментарий `Evict` и обновить комментарий
  `PendingGossip`** (member.go строки 173-184): dereference безопасен, потому что
  `Evict` — единственный путь удаления — снимает оба ключа атомарно.

- **Утечки/паники нет.** `gossipTx[id]` мог ещё держать ненулевой остаток бюджета
  (Dead-рамор не дорассылан) в момент эвикции — синхронный `delete(gossipTx, id)`
  просто отменяет остаток дорассылки. Это корректно: раз узел эвиктится, его
  Dead-рамор больше не нужен (grace-период на дорассылку уже прошёл, §2). Если
  запись в `gossipTx[id]` отсутствует (бюджет уже исчерпан ранее) — `delete`
  по отсутствующему ключу в Go безвреден (no-op).

**Что кодер делает:** в теле `Evict` — `delete(l.members, id)` и
`delete(l.gossipTx, id)` под одним `Lock`. Больше `List` в эвикции ничего не
трогает (нет других карт: `self`/`mu` — не про членов).

---

## 4. Файлы и сигнатуры

### 4.1. `internal/member/member.go` — новый метод `Evict`

Единственное добавление в `List` (не трогать `Merge`, `PendingGossip`-логику
кроме комментария, `snapshot`, `Get`, `Others`, `Members`).

```go
// Evict removes id from the membership view entirely — the only path that
// deletes a record, and only for a member this node still sees as Dead at the
// given incarnation. It drops the record from BOTH members and gossipTx under
// one lock, preserving the "gossipTx keys ⊆ members keys" invariant that lets
// PendingGossip dereference *l.members[id] without a presence check. The
// incarnation+Dead guard makes eviction race-safe against a revive: if a
// higher-incarnation Alive rumor resurrected id after the eviction timer was
// armed (see swim.Node.evict), the record is no longer Dead@inc and Evict is a
// no-op, so a live member is never silently dropped. self is never evicted.
// Returns true iff a record was actually removed.
func (l *List) Evict(id ID, inc uint64) (evicted bool)
```

Тело (точная логика, кодеру реализовать буквально):

1. `l.mu.Lock()`, `defer l.mu.Unlock()`.
2. Если `id == l.self` → `return false` (себя не эвиктим никогда — self-запись
   несёт актуальную incarnation узла).
3. `cur, ok := l.members[id]`; если `!ok` → `return false` (уже эвикчена/нет).
4. **Precedence-guard:** если `cur.State != StateDead || cur.Incarnation != inc`
   → `return false`. Эвиктим только всё ещё Dead-запись ровно на той incarnation,
   на которой таймер был взведён. Любое расхождение (revive до Alive на бо́льшей
   incarnation, или запись уже сменилась) — отмена эвикции. Это зеркало
   guard'а `escalate` (swim.go строки 609-615: `cur.incarnation != inc → return`).
5. `delete(l.members, id)`; `delete(l.gossipTx, id)`; `return true`.

**Также обновить комментарий `PendingGossip`** (member.go ~строки 173-184):
добавить одну фразу, что dereference `*l.members[id]` безопасен благодаря
инварианту «gossipTx-ключи ⊆ members-ключей», который держит `Evict` — единственный
путь удаления — снимая оба ключа атомарно под тем же `l.mu`. (Не переписывать весь
комментарий, добавить пояснение.)

### 4.2. `internal/swim/swim.go` — eviction-таймер + фильтр Dead в probe

**Config (строки 64-86):** добавить поле после `SuspicionTimeout`:

```go
// DeadTimeout is the grace period a Dead record lingers before this node
// evicts it from the list entirely. Zero => production default (see NewNode).
// Ordered ABOVE SuspicionTimeout: a node lives in the list as Dead at least as
// long as it lived as Suspect, so the death rumor has time to spread by gossip
// before the record is forgotten (evicting sooner would erase it before peers
// learn of the death). See member.List.Evict.
DeadTimeout time.Duration
```

**NewNode (строки 124-163):** добавить дефолт после блока `SuspicionTimeout`
(строки 145-151):

```go
if cfg.DeadTimeout == 0 {
    // Above SuspicionTimeout (5s): the Dead rumor must fully disseminate
    // before the record is forgotten. See member.List.Evict and the timeout
    // ladder note above (RTT+Indirect << Suspicion < Dead).
    cfg.DeadTimeout = 10 * time.Second
}
```

И инициализировать карту в возвращаемом `&Node{...}`: `evictions: map[member.ID]*evictionTimer{}`.

**Node struct (строки 88-112):** добавить рядом с suspicion-полями:

```go
// evictions holds the armed Dead→evicted timers, one per target, each valid
// only for the incarnation it was armed at (mirrors suspicions). Guarded by
// its own evictMu, same rationale as suspMu: eviction bookkeeping stays out
// of the hot seqNo/pending/Rand section, and Clock-goroutine callbacks contend
// only with each other.
evictMu   sync.Mutex
evictions map[member.ID]*evictionTimer
```

**Новый тип** рядом с `suspicionTimer` (строки 114-120):

```go
// evictionTimer is one armed Dead→evicted removal, valid only for the
// incarnation the target was declared Dead at: a revive at a higher
// incarnation obsoletes and cancels it (mirrors suspicionTimer).
type evictionTimer struct {
    incarnation uint64
    timer       Timer
}
```

**Три метода** по образцу `armSuspicion`/`cancelSuspicion`/`escalate`
(строки 554-627). Разместить рядом с ними:

```go
// armEviction arms the Dead→evicted removal timer for target, valid for
// target.Incarnation. Idempotent per (ID, incarnation): a repeat at the same
// (or older) incarnation is a no-op, so eviction fires at most once per
// incarnation. A Dead at a HIGHER incarnation replaces the armed timer (the
// old one is obsolete by precedence). Mirror of armSuspicion.
func (n *Node) armEviction(target member.Member)

// cancelEviction disarms target's eviction timer when the record leaves Dead:
// a revive (Alive) at a HIGHER incarnation resurrects the node, so it must no
// longer be evicted. (A Dead at a higher incarnation is handled by armEviction
// re-arming.) Mirror of cancelSuspicion, but the trigger is "no longer Dead@inc".
func (n *Node) cancelEviction(id member.ID, inc uint64, state member.State)

// evict is the eviction timer's payload: the grace period elapsed with the
// target still Dead, so remove it from the list. Guard mirrors escalate: if
// the map entry was disarmed or re-armed at a newer incarnation between the
// timer firing and this callback, it is not ours to evict. The actual removal
// goes through member.List.Evict, which re-checks Dead@inc under the list lock
// (a revive racing the timer then makes Evict a no-op — precedence has the
// final word, same as escalate → Merge).
func (n *Node) evict(id member.ID, inc uint64)
```

Точная логика тел:

- **`armEviction(target)`** — копия `armSuspicion` (строки 560-576) с заменой
  `suspMu`→`evictMu`, `suspicions`→`evictions`, `suspicionTimer`→`evictionTimer`,
  `SuspicionTimeout`→`DeadTimeout`, `escalate`→`evict`. Callback:
  `n.clock.AfterFunc(n.cfg.DeadTimeout, func() { n.evict(id, inc) })`.
  Не нужны `addr` в замыкании (Evict берёт только id+inc).

- **`cancelEviction(id, inc, state)`** — по образцу `cancelSuspicion`
  (строки 584-598): под `evictMu`, `cur, ok := n.evictions[id]`; если `!ok` →
  return. Условие отмены: `state == member.StateAlive && inc > cur.incarnation`
  (revive на бо́льшей incarnation). Если условие выполнено — `cur.timer.Stop()`,
  `delete(n.evictions, id)`. (Dead на той же/бо́льшей incarnation НЕ отменяет —
  это либо тот же Dead, либо re-arm через armEviction; Suspect на бо́льшей
  incarnation в норме не приходит после Dead, но если придёт — не отменяем, узел
  снова под подозрением и не должен быть забыт по старому таймеру: armEviction/
  cancelEviction симметричны suspicion-логике.)

- **`evict(id, inc)`** — по образцу `escalate` (строки 607-627):
  1. `n.evictMu.Lock()`; `cur, ok := n.evictions[id]`; если `!ok ||
     cur.incarnation != inc` → `n.evictMu.Unlock(); return`.
  2. `delete(n.evictions, id)`; `n.evictMu.Unlock()`.
  3. `if n.list.Evict(id, inc) { log.Printf("swim: evicting %s after %v dead (incarnation %d)", id, n.cfg.DeadTimeout, inc) }`.

**Взведение таймера — две точки, где узел становится Dead в локальном view:**

1. **`escalate`** (swim.go строки 607-627): после успешного `n.list.Merge(...Dead)`
   (внутри `if n.list.Merge(...) { ... }`, где сейчас только log) — добавить
   `n.armEviction(member.Member{ID: id, Addr: addr, Incarnation: inc})`. Это
   локальная эскалация Suspect→Dead: узел стал Dead, стартуем grace-таймер.

2. **`applyGossip`** (swim.go строки 241-268): в `switch u.State` есть ветка
   `case member.StateAlive, member.StateDead: n.cancelSuspicion(...)`. **Разбить
   Dead и Alive:**
   ```go
   case member.StateSuspect:
       n.armSuspicion(m)
   case member.StateDead:
       n.cancelSuspicion(u.ID, u.Incarnation, u.State)
       n.armEviction(m)
   case member.StateAlive:
       n.cancelSuspicion(u.ID, u.Incarnation, u.State)
       n.cancelEviction(u.ID, u.Incarnation, u.State)
   }
   ```
   Смысл: Dead, узнанный по gossip (не только по локальной эскалации), тоже
   взводит eviction-таймер у этого узла — иначе Dead, пришедший рамором, никогда
   бы не эвиктился (лишь тот, что мы escalate'нули сами). Alive (revive на
   бо́льшей incarnation через Merge, `changed=true`) — отменяет и suspicion, и
   eviction. `armEviction` идемпотентен per (ID,inc), так что повторные Dead-рамор
   на той же incarnation не плодят таймеры.

   **Важно:** этот блок исполняется только при `changed == true` (уже
   гарантировано `if !changed { continue }` строки 257-258) — то есть только на
   реальном переходе, отвергнутый precedence Dead не взводит таймер.

**`stopSuspicionTimers` (строки 629-638) → добавить зеркальный
`stopEvictionTimers`** и вызвать его в `Run`'s `defer` (строки 171-172) рядом с
`stopSuspicionTimers`:

```go
func (n *Node) stopEvictionTimers() {
    n.evictMu.Lock()
    defer n.evictMu.Unlock()
    for id, cur := range n.evictions {
        cur.timer.Stop()
        delete(n.evictions, id)
    }
}
```

В `Run`: `defer n.stopEvictionTimers()` (порядок с `stopSuspicionTimers`
неважен — обе идемпотентны и берут разные мьютексы).

**Фильтр Dead-целей в `probeOnce` (строки 647-712) — развилка 4:** см. §6.

---

## 5. Развилка 5 — revive эвикнутого узла. **Проверено: работает сегодня, дополнительный код не нужен, но покрыть тестом. Зафиксировано.**

**Проверка по коду (не предположение):** после `Evict` записи `id` в `members`
нет. Входящий рамор про `id` (любой State/incarnation) попадёт в `Merge`
(member.go строки 135-171) как **свежая вставка** — `cur, ok := l.members[m.ID]`
даёт `ok=false`, весь switch по incarnation/precedence пропускается (он только
`if ok`), запись вставляется как есть, `changed=true`, бюджет gossip
переармируется. Это ровно то же, что происходит при первом знакомстве с любым
новым узлом. Никакой «памяти» об эвикнутой записи не остаётся — и не должно.

**Единственная тонкость — гонка eviction ↔ revive**, закрыта двумя guard'ами:
1. `evict`-callback проверяет `cur.incarnation != inc` под `evictMu` (запись в
   `evictions` могла быть отменена `cancelEviction` при revive до Alive@higher-inc).
2. `List.Evict` **повторно** проверяет `State==Dead && Incarnation==inc` под
   `l.mu` — если revive проскочил между снятием `evictMu` и взятием `l.mu`,
   запись уже Alive@higher-inc, и `Evict` возвращает `false` (no-op). Живой узел
   не будет удалён. Это тот же двухуровневый precedence-guard, что у
   `escalate`→`Merge` (swim.go строки 607-624).

**Что кодер делает:** ничего дополнительного в `Merge` — он уже корректен для
свежей вставки. Только тест `TestNode_Evict_ThenReviveIsFreshInsert` (§7) и
тест гонки `TestNode_ReviveBeforeEvict_NotEvicted` (§7) фиксируют это поведение.

---

## 6. Развилка 4 — фильтр Dead-целей в `probeOnce`. **Решение: фильтровать Dead из выбора цели в `probeOnce`. Зафиксировано.**

**Решение.** В `probeOnce` (swim.go строки 647-712) исключать Dead-узлы из пула
кандидатов на пробинг **до** выбора цели. Не полагаться только на то, что после
grace-периода узел исчезнет сам.

**Обоснование:**
- Развилка напрямую решает логовый шум в переходном окне `[Dead → evicted]`
  длиной `DeadTimeout` (10s). Без фильтра `probeOnce` продолжает выбирать
  Dead-цель из `Others()`, слать ей Ping, ловить таймаут и логировать
  «marking suspect» на уже-Dead узел (swim.go строка 710) — ровно тот шум,
  который зафиксирован как техдолг Этапа 4 (TECHNICAL_PLAN строки 202-209) и
  воспроизведён в живом прогоне Этапа 5 (хендофф §1). Grace-период 10s — это
  10 probe-тиков (ProbeInterval 1s), то есть ~10 шумных строк на каждый Dead-узел
  до эвикции. Фильтр убирает их полностью.
- `Others()` **не трогаем** (он используется и пробингом, и `pickMediators`, и
  `handlePingReq` — там Dead-узлы как посредники безвредны, они просто не
  ответят; менять контракт `Others()` = чинить не то место). Фильтр — локально
  в `probeOnce`, на пуле кандидатов.

**Что кодер делает** — в `probeOnce`, заменить начало (строки 648-651):

```go
others := n.list.Others()
if len(others) == 0 {
    return // single-node cluster: nobody to probe
}
```

на выбор только не-Dead целей:

```go
others := n.list.Others()
targets := make([]member.Member, 0, len(others))
for _, m := range others {
    if m.State == member.StateDead {
        continue // a Dead record is being gossiped out and will be evicted
        // shortly; probing it only yields timeout noise (Этап 6).
    }
    targets = append(targets, m)
}
if len(targets) == 0 {
    return // nobody probeable (single node, or every peer already Dead)
}
```

и ниже (строка 657) выбирать из `targets`, а не `others`:
`target := targets[n.cfg.Rand.Intn(len(targets))]`.

**Тонкость с детерминизмом тестов:** фильтр меняет размер пула, из которого
`Rand.Intn` выбирает цель. Существующие живые тесты (`TestNode_PingAck_*`,
`TestNode_IndirectProbe_*`) не держат Dead-узлов в списке на момент `probeOnce`,
так что их выбор не сдвигается. Новый тест §7 `TestNode_ProbeSkipsDead`
проверяет фильтр напрямую. Suspect-узлы **не** фильтруются (они ещё под
активным пробингом — их и надо пробить).

---

## 7. Тест-кейсы (перечень, не код)

Все под `-race`. Юниты `Evict` — в `internal/member/member_test.go`; протокольные
eviction-сценарии — в `internal/swim/swim_test.go`, на `fakeClock` через
`suspicionFixture`-паттерн (fakeClock, изолированный узел A, узел B в списке).
Существующие тесты Этапов 1-5 **должны продолжать проходить без правок**.

### 7.1. Юниты `member.List.Evict` (`internal/member/member_test.go`)

1. **`TestList_Evict_RemovesDeadRecord`** — список с self + B@Dead@inc.
   `Evict("B", inc)` возвращает `true`; после — `Get("B")` даёт `ok=false`,
   `Members()` не содержит B, `Others()` не содержит B.
2. **`TestList_Evict_DropsGossipTx`** — merge B@Dead (создаёт `gossipTx["B"]`),
   `Evict("B", inc)`; затем `PendingGossip(GossipMaxUpdates, nil)` **не паникует**
   и **не** возвращает B (ключ снят из gossipTx). Это прямой тест на
   `PendingGossip`'s dereference после эвикции — центральный техдолг Этапа.
3. **`TestList_Evict_GuardsIncarnation`** — B@Dead@inc=4. `Evict("B", 3)` (старая
   incarnation) → `false`, B на месте. `Evict("B", 5)` (несуществующая) → `false`,
   B на месте. Только `Evict("B", 4)` → `true`.
4. **`TestList_Evict_GuardsState`** — B@Alive@inc (не Dead). `Evict("B", inc)` →
   `false`, B на месте (эвиктим только Dead).
5. **`TestList_Evict_Unknown`** — `Evict("Z", 0)` на отсутствующем id → `false`,
   без паники.
6. **`TestList_Evict_NeverSelf`** — `Evict(self, inc)` (даже если бы self был
   Dead — искусственно) → `false`, self остаётся в списке.

### 7.2. Протокольные eviction-сценарии (`internal/swim/swim_test.go`)

Использовать `suspicionFixture` (fakeClock, node A, B@Alive@inc=4 в списке).
Дефолтный `DeadTimeout` в фикстуре не задан → возьмётся прод-дефолт 10s; для
детерминизма тестов **лучше задать явный** `DeadTimeout` в тестовом Config
(например 20s), чтобы Advance-числа были очевидны и не совпадали случайно с
suspicion — либо добавить в `suspicionFixture` установку `DeadTimeout` (проверить,
что это не ломает существующие suspicion-тесты; безопаснее — новый локальный
Config в eviction-тестах, не трогая общую фикстуру).

7. **`TestNode_DeadEvictsAfterTimeout`** (центральный тест Этапа) — узел A,
   B@Alive@4. `nodeA.suspect(B@4)` → Suspect; `clock.Advance(SuspicionTimeout)` →
   B становится Dead (escalate арм'ит eviction-таймер). Проверить B ещё в списке
   (Dead). `clock.Advance(DeadTimeout - 1ms)` → B всё ещё в списке (таймер не
   сработал рано). `clock.Advance(1ms)` → `Get("B")` даёт `ok=false` (эвикчен).
   Проверить порядок: суммарный Advance = SuspicionTimeout + DeadTimeout.
8. **`TestNode_DeadFromGossip_Evicts`** — узел A, B@Alive@4 в списке.
   `nodeA.applyGossip([]Update{{ID:"B", Incarnation:4, State:Dead}})` → B@Dead
   (eviction-таймер взведён из applyGossip, не из escalate). `clock.Advance(DeadTimeout)`
   → B эвикчен (`Get("B")` → `ok=false`). Доказывает, что Dead, узнанный рамором,
   тоже эвиктится.
9. **`TestNode_ReviveBeforeEvict_NotEvicted`** (гонка revive ↔ eviction) — B@Dead@4
   (через suspect+Advance(SuspicionTimeout) или applyGossip Dead@4).
   `clock.Advance(DeadTimeout - 1ms)` (таймер почти сработал, но нет). Прислать
   revive: `applyGossip([]Update{{ID:"B", Incarnation:5, State:Alive}})` → B@Alive@5,
   `cancelEviction` снял таймер. `clock.Advance(1ms)` **и ещё** `Advance(DeadTimeout)`
   → B **остаётся** Alive@5 в списке (эвикция отменена revive). Центральный
   guard-тест: живой узел не удаляется.
10. **`TestNode_Evict_ThenReviveIsFreshInsert`** — довести B до эвикции
    (`Get("B")` → `ok=false`, тест 7). Затем `applyGossip([]Update{{ID:"B",
    Incarnation:1, State:Alive}})` → `Get("B")` даёт B@Alive@1 (свежая вставка,
    incarnation стартует с рамора, не «помнит» старую 4). Доказывает §5: revive
    после эвикции — новый Member, без остаточной памяти.
11. **`TestNode_ProbeAllDead_NoOp`** (фильтр §6) — узел A, список: self A,
    B@Dead (единственный Other, никого живого). Вызвать `probeOnce(ctx)`
    напрямую: должен вернуться как no-op — не шлёт Ping/PingReq (нечего слать,
    `targets` пуст), не переводит B ни в какое новое состояние (B остаётся
    Dead, не «marking suspect»-ится повторно), `n.list.Get("B")` не меняется.
    Детерминированный тест на пустой пул `targets` после фильтра — это и есть
    прямая проверка §6 (без завязки на `Rand`/выбор конкретной живой цели).
12. **`TestNode_EvictionTimerStoppedOnShutdown`** (опционально, дёшево) — арм'ить
    eviction через applyGossip Dead, затем прогнать `Run` под ctx и отменить ctx
    до `DeadTimeout`; убедиться, что после возврата `Run` карта `evictions` пуста
    (через `stopEvictionTimers`) — зеркало гарантии `stopSuspicionTimers`. Если
    прямого доступа к приватной карте из теста хватает (white-box, тот же пакет) —
    проверить `len(nodeA.evictions) == 0`.

**Устойчивость.** Протокольные тесты 7-12 прогнать `-race -count=200` (паттерн
Этапов 4-5). Fakeclock-тесты детерминированы по построению (Advance синхронен),
живой тест 11 — держать таймауты короткими, исход timing-независим.

### 7.3. Живой прогон (Sonnet, шаг 4 пайплайна)

3 процесса на localhost, один в режиме `observe`; убить один узел. Ожидание:
observe печатает alive→suspect→dead, **затем** (через `DeadTimeout` после dead)
Dead-узел **исчезает** из вывода `observe` (`members` уменьшается на 1). Лог
выживших: «marking suspect» на убитый узел прекращается после его эвикции (шум
из хендоффа §1 устранён), появляется одна строка «evicting <id> after 10s dead».
Это прямая проверка, что техдолг закрыт: раньше Dead висел вечно и шумел, теперь
исчезает и замолкает.

---

## 8. Затрагиваемые файлы (сводка)

| Файл | Что делаем |
|---|---|
| `internal/member/member.go` | Новый метод `Evict(id, inc) (evicted bool)` — атомарное удаление из `members`+`gossipTx` под `l.mu` с guard (Dead@inc, не self). Обновить doc-комментарий `PendingGossip`: dereference безопасен благодаря инварианту, который держит `Evict`. `Merge`/`snapshot`/остальное — не трогать. |
| `internal/member/member_test.go` | Юниты 1-6 (§7.1) на `Evict`. Существующие 13 тестов не трогать. |
| `internal/swim/swim.go` | `Config.DeadTimeout` + дефолт 10s в `NewNode`; поля `evictMu`/`evictions` + init карты; тип `evictionTimer`; методы `armEviction`/`cancelEviction`/`evict`/`stopEvictionTimers`; арм в `escalate` и в `applyGossip` (разбить Dead/Alive-ветку); `defer n.stopEvictionTimers()` в `Run`; фильтр Dead-целей в `probeOnce`. Убрать TODO про eviction, если есть в комментариях. |
| `internal/swim/swim_test.go` | Протокольные тесты 7-12 (§7.2). Существующие тесты не трогать. |
| `docs/TECHNICAL_PLAN.md` | (шаг 7 пайплайна, НЕ кодером) Новый раздел «Этап 6 — Eviction Dead-узлов (готов)»; отметить закрытие техдолга Этапов 4/5 (eviction введён, `PendingGossip` dereference теперь поддержан инвариантом `Evict`, логовый шум устранён фильтром §6). |

Пакеты stdlib новыми не нужны (`time` уже в member.go и swim.go).

---

## 9. Что НЕ трогать / вне рамок (жёсткая граница)

- **Wire-формат / `protocol`** — не трогать. Eviction чисто локальное решение о
  памяти: на wire ничего нового не едет, tombstone-сообщений нет. Узлы
  эвиктят независимо, каждый по своему таймеру от момента, когда сам увидел Dead
  (как suspicion — локальное дело каждого, TECHNICAL_PLAN Этап 4).
- **`Merge`-precedence** — правило (Incarnation, State) неприкосновенно.
  `Evict` не участвует в precedence, он снаружи: удаляет только уже-Dead@inc.
  Revive после эвикции идёт обычной вставкой `Merge`, §5.
- **`transport.Transport` / `transport.Fake` / `UDP`** — не трогать (симулятор
  Этапа 5 достаточен; eviction на транспорт не влияет). Compile-time assertions
  не удалять.
- **`Others()` / `Members()` / `snapshot`** — контракт не менять; фильтр Dead —
  локально в `probeOnce`, не в `Others()` (см. §6: Dead-посредники безвредны).
- **CLI** — `DeadTimeout` через флаг НЕ вводим (все таймауты узла вне CLI-scope,
  как на Этапах 4-5). `observe`/`printMembers` не менять — они уже печатают
  `Members()`, эвикнутый узел просто перестанет появляться (желаемое поведение,
  §7.3).
- **`StateChangedAt`** — не задействовать для eviction-времени (это диагностика,
  не источник решений; grace-таймер — в `swim.Node` через Clock, §1).
- **Бюджет ретрансляций / `gossipRetransmitBudget`** — не трогать. `DeadTimeout`
  подобран с запасом над типичной сходимостью Dead-рамора (§2), пересчёт бюджета
  по N остаётся отдельным несвязанным техдолгом (хендофф §2).

---

## 10. Критерий готовности

- `go build ./...`, `go vet ./...`, `gofmt -l .` (пусто) — чисто.
- `go test ./...` — зелено; `go test -race ./...` — зелено.
- `go test -race -count=200 ./internal/swim/...` — зелено (флейков нет), включая
  новые eviction-тесты 7-12.
- `go test -race -count=200 ./internal/member/...` — зелено (юниты `Evict`).
- **Техдолг закрыт и подтверждён:**
  - `PendingGossip` не паникует при эвикции (тест 2) — скрытый инвариант теперь
    поддержан `Evict` (оба ключа снимаются атомарно), а не «случайно верен».
  - Логовый шум устранён: `probeOnce` не выбирает Dead-цели (тест 11/`ProbeAllDead_NoOp`).
  - Живой узел не удаляется гонкой revive (тест 9) — двойной guard (evict-callback
    + `List.Evict`) держит.
- **Живой прогон:** 3 процесса, один `observe`; убитый узел проходит
  alive→suspect→dead, затем **исчезает** из observe через `DeadTimeout`; «marking
  suspect» на него прекращается; одна строка «evicting … dead» в логе выживших.
- Раздел «Этап 6» добавлен в `docs/TECHNICAL_PLAN.md` с фиксацией решений
  §1 (таймер в Node), §2 (DeadTimeout=10s, ordered above Suspicion), §6 (фильтр
  Dead в probe) и закрытия техдолга Этапов 4/5.

---

## 11. Саммари ключевых решений (для ревьюера и кодера)

1. **Таймер eviction — в `swim.Node`**, зеркало suspicion-таймера
   (`evictMu`/`evictions`/`armEviction`/`cancelEviction`/`evict`). `List` таймингом
   не занимается — только исполняет удаление через новый `Evict`. Причина: `List` =
   чистая структура + precedence без времени; `Node` уже владеет ровно этим паттерном.
2. **`Config.DeadTimeout` = 10s** (дефолт), упорядочен ВЫШЕ `SuspicionTimeout` (5s):
   Dead-рамор должен дораспространиться прежде, чем узел забыт. Лестница
   RTT+Indirect << Suspicion < Dead. Триггер пересмотра — крупный кластер или
   CLI-конфиг таймаутов.
3. **`gossipTx[id]` удаляется синхронно** с `members[id]` в одном `Evict` под
   одним `l.mu` — это и есть починка `PendingGossip`'s dereference: инвариант
   «gossipTx-ключи ⊆ members-ключей» теперь поддержан, а не случаен.
4. **Фильтр Dead-целей в `probeOnce`** (не в `Others()`): устраняет логовый шум
   «marking suspect» на уже-Dead в grace-окне. `Others()`-контракт цел (Dead-
   посредники безвредны).
5. **Revive эвикнутого узла** работает сегодня без нового кода — свежая вставка в
   `Merge` (`ok=false`). Гонка revive↔eviction закрыта двойным guard'ом
   (evict-callback по `evictMu` + `List.Evict` перепроверяет Dead@inc под `l.mu`).
   Только покрыть тестами.
6. **Wire/protocol/Merge-precedence/Transport — не трогаем.** Eviction локальное,
   каждый узел эвиктит по своему таймеру, tombstone на wire нет.
