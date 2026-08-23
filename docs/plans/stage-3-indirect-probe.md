# План Этапа 3 — Indirect probing (PingReq к K посредникам)

Ветка: `stage/3-indirect-probe` (уже создана от свежего `master` после мержа
Этапов 0/1/2, уже выбрана).
Опорные требования: `docs/TECHNICAL_PLAN.md` (раздел «Этап 3»), `docs/PLAN.md`
(центральный инвариант проекта), конвенции
`.claude/skills/go-swim-discovery-dev/SKILL.md`.

Модуль: `github.com/akomyagin/swim-discovery`, Go 1.23. Тулчейн `~/sdk/go/bin/go`
(может не быть в `PATH`). **Только стандартная библиотека** — `net`, `context`,
`encoding/json`, `sync`, `time`, `math`, `math/rand`, `sort`. Внешних
зависимостей и Docker нет.

Исполнитель плана — кодер-агент, стартует «холодным» и этот диалог не видит. План
даёт пути файлов, сигнатуры, порядок реализации и конкретные тест-кейсы;
отклоняться от сигнатур без причины не нужно.

---

## 0. Цель и границы Этапа

**Цель.** Реализовать центральную SWIM-специфику проекта — **indirect probing**.
Если прямой Ping не получил Ack за RTT-таймаут, узел не сдаётся: он просит K
случайных **других** членов пропинговать Target от его имени (`PingReq`).
Посредник пингует Target и релеит полученный Ack обратно инициатору. Target
считается недостижимым (→ `Suspect`) **только если** молчат и прямой пинг, и все
K косвенных. Это гасит центральный false positive: потеря именно *твоих* пакетов
к Target не приводит к ошибочному объявлению живого узла подозреваемым.

Границы: Этап 3 доводит цепочку до перехода Target в `StateSuspect` при полном
провале (прямой + все косвенные молчат). **Suspicion-таймер `Suspect→Dead`,
refute и абстракция `Clock` — Этап 4, не здесь** (см. §7).

**Критерий готовности (весь набор обязателен).**
- `~/sdk/go/bin/go build ./...` — чисто.
- `~/sdk/go/bin/go vet ./...` — чисто.
- `~/sdk/go/bin/go test -race ./...` — все тесты зелёные под `-race`.
- `~/sdk/go/bin/gofmt -l .` — пустой вывод.
- Не осталось `TODO(Этап 3)` в затронутых телах (комментарии-ориентиры на будущие
  этапы — можно оставлять/обновлять).
- Все compile-time assertions (`var _ transport.Transport = (*UDP)(nil)`, аналог
  для `Fake`) на месте.
- Центральный тест `TestNode_IndirectProbe_SuppressesFalsePositive` (§6) —
  зелёный, устойчивый под `-count=200`.

---

## 1. Ключевое архитектурное решение — корреляция релейного Ack (зафиксировано)

Это тот самый открытый вопрос, ради которого пишется план. **Решение принято —
реализовать именно так.**

### 1.1. `chan struct{}` остаётся; менять его на `chan protocol.Update` НЕ нужно

Сейчас ожидание Ack в `swim.Node` — это `pending map[uint64]chan struct{}`:
получатель по `SeqNo` находит канал и шлёт в него пустой сигнал. Для indirect
probing этого достаточно, **если релейный Ack несёт тот же `SeqNo`, что и
исходный прямой Ping инициатора**. Тогда инициатору всё равно, пришёл Ack прямо
от Target или релеем через посредника — любой Ack с этим `SeqNo` доказывает
живость Target и закрывает пробу. Инициатору **не нужно знать, ОТ КОГО** пришёл
релей: сам факт «кто-то дозвонился до Target от моего имени» — это и есть ответ.
Поэтому данные в канале не нужны, `chan struct{}` не меняем.

**Механизм корреляции целиком строится на `SeqNo` инициатора, который путешествует
неизменным через посредника и возвращается в релейном Ack.** Никаких новых
relay-ID / request-ID в wire-протокол вводить не нужно (см. §2).

### 1.2. Единая проба = один `SeqNo`, одно ожидание, две фазы

`probeOnce` выделяет **один** `seq` на всю пробу Target и регистрирует **один**
`pending[seq]` канал. Фаза 1 — прямой Ping (как в Этапе 1). Если Ack пришёл —
проба закрыта, Target alive. Если прямой таймаут истёк без Ack — фаза 2: тот же
`seq` переиспользуется, рассылаются K `PingReq{Target, SeqNo: seq}`, и снова
ждём **тот же** `pending[seq]` канал в пределах отдельного indirect-таймаута.
Любой релейный Ack (посредник вернёт его с `SeqNo: seq`) закрывает пробу.

**Почему один seq на обе фазы, а не по seq на посредника:** инициатору не важно,
который из K посредников дозвонился — важно лишь, что *хоть один* дозвонился.
Один канал, в который может прийти сигнал от любого релея, — самое простое
корректное решение. Буфер канала — 1 (как сейчас): первый же сигнал закрывает
пробу, последующие релеи (если придут) не должны блокировать отправителя —
см. §3.5 про дренаж/очистку.

### 1.3. Роль посредника — трансляция Ack с подменой SeqNo

Посредник M, получив `PingReq{From: I, Target: T, SeqNo: S}`:
1. Шлёт **свой** обычный `KindPing` к T со **своим собственным** новым `seqNo`
   `S_m` (взятым из счётчика M, не из S), регистрируя `pending[S_m]` у себя —
   ровно как в прямой пробе. Пиггибек gossip — как обычно.
2. Ждёт Ack от T в пределах RTT-таймаута M.
3. **Если** Ack от T пришёл (сработал `pending[S_m]`) — M формирует
   `KindAck{From: M, SeqNo: S}` (обратно **исходный** `S` инициатора!) и шлёт его
   инициатору I. Это и есть релей.
4. **Если** T не ответил M за таймаут — M **ничего не шлёт** инициатору (молчание
   = «я тоже не дозвонился»). Инициатор различает провал по своему indirect-
   таймауту, а не по явному NACK. Явного отрицательного ответа в протоколе нет.

Так `SeqNo` инициатора — сквозной корреляционный ключ пробы; `SeqNo` посредника —
локальный ключ его собственного вложенного Ping→Ack к Target. Они независимы и не
пересекаются (у каждого узла свой монотонный счётчик).

### 1.4. Почему нужна фильтрация релейного Ack по From (важная тонкость)

Инициатор I при appи-ложенном сценарии может получить обычный Ack прямо от Target
(если прямой пакет всё-таки дошёл) И релейный Ack от посредника — оба с `SeqNo:
S`. Оба валидны, оба должны закрывать пробу; дубликат безопасен благодаря буферу
канала 1 и идемпотентной очистке `pending[S]` (см. §3.5). **Дополнительная
проверка From на входящем Ack инициатору не требуется** — любой Ack с известным
активным `SeqNo` легитимен. (Защита от подделки SeqNo — вне скоупа учебного
проекта, wire не аутентифицирован по замыслу.)

---

## 2. Wire-протокол (`internal/protocol/protocol.go`)

**Заготовка PingReq уже есть.** В `protocol.go` уже объявлены `KindPingReq` и
поле `Target member.ID json:"target,omitempty"` в `Message`, а `SeqNo`
задокументирован как «correlates a Ping/PingReq with its Ack». **Структурных
изменений wire-формата Этап 3 НЕ требует** — поля достаточно.

Что сделать в `protocol.go`:
- **Ничего структурно не добавлять.** `Message{Kind, From, SeqNo, Target,
  Updates}` уже покрывает PingReq полностью:
  - `Kind = KindPingReq`;
  - `From` = ID инициатора (нужен посреднику, чтобы знать, кому релеить Ack);
  - `Target` = ID цели, которую посредник должен пропинговать;
  - `SeqNo` = сквозной `S` инициатора (посредник вернёт его в релейном Ack);
  - `Updates` = обычный piggyback gossip.
- **Обновить doc-комментарии** под фактическую реализацию: у поля `Target`
  снять формулировку «only meaningful for KindPingReq» на более точную «set on
  KindPingReq (the node to probe) and echoed nowhere else»; у `SeqNo` уточнить,
  что для PingReq это сквозной ключ, который посредник возвращает в релейном Ack
  неизменным. Комментарии — на английском (конвенция проекта).
- Round-trip `Encode`/`Decode` для `KindPingReq` с непустым `Target` покрыть
  тестом (§6, T-ENC).

**Проверка перед началом:** если в `protocol.go` вдруг обнаружится, что `Target`
или `KindPingReq` отсутствуют (не должно, но сверься) — добавить как выше,
не плодя новых полей/типов.

---

## 3. Изменения в ядре (`internal/swim/swim.go`)

### 3.1. Config: параметры indirect probing

Добавить в `Config`:

```go
// IndirectNodes is K: how many random mediators receive a PingReq when the
// direct Ping goes unanswered. Zero => production default (see NewNode).
IndirectNodes int
// IndirectTimeout bounds how long probeOnce waits for ANY relayed Ack after
// fanning out PingReqs. Zero => production default (see NewNode).
IndirectTimeout time.Duration
```

`NewNode` проставляет дефолты при нуле:
- `IndirectNodes` → `3` (классический SWIM `k=3`; на маленьком кластере
  фактическое число ограничивается доступными посредниками — см. §4).
- `IndirectTimeout` → равен `RTTTimeout` (посреднику нужен свой RTT до Target
  плюс релей обратно; для localhost/fake разумно взять тот же порядок; отдельная
  константа оставляет тесту гибкость сузить окно). Разумный дефолт —
  `cfg.RTTTimeout` (после того как сам RTTTimeout уже дефолтнут).

Не вводить `Clock` — время по-прежнему через `context.WithTimeout` (как в
Этапе 1). `Clock` появляется в Этапе 4.

### 3.2. `probeOnce` — встраивание фазы indirect

Текущий `probeOnce` (Этап 1/2): выбирает Target, шлёт Ping, ждёт `ackCh` или
`RTTTimeout`; при Ack — `Merge(alive)`, при таймауте — только логирует. **Заменить
хвост «иначе только лог» на фазу indirect.** Структура после правки:

```
probeOnce(ctx):
    others := list.Others()
    if len(others)==0 { return }
    // --- выбор Target + seq + регистрация pending (как сейчас, под n.mu) ---
    target := others[Rand.Intn(len(others))]
    seq := ++seqNo
    ackCh := make(chan struct{}, 1); pending[seq]=ackCh   // буфер 1
    // (Rand и seqNo/pending — под общим n.mu, как сейчас)

    // --- ФАЗА 1: прямой Ping ---
    send Ping{From:self, SeqNo:seq, Updates: collectGossip(nil)} -> target.Addr
    direct := waitAck(ctx, ackCh, RTTTimeout)   // true если Ack пришёл

    if !direct {
        // --- ФАЗА 2: indirect ---
        mediators := n.pickMediators(target.ID)   // §4, тот же n.mu вокруг Rand
        for _, m := range mediators {
            send PingReq{From:self, Target:target.ID, SeqNo:seq,
                         Updates: collectGossip(nil)} -> m.Addr
        }
        if len(mediators) > 0 {
            direct = waitAck(ctx, ackCh, IndirectTimeout)  // тот же ackCh, тот же seq
        }
        // len(mediators)==0 (кластер из 2 узлов, посредников нет): пропускаем
        // ожидание, direct остаётся false — сразу к выводу ниже.
    }

    // --- очистка pending[seq] (идемпотентно, как сейчас) ---
    n.mu.Lock(); delete(pending,seq); n.mu.Unlock()

    if direct {
        list.Merge(target as StateAlive at target.Incarnation)   // как сейчас
    } else {
        // Полный провал: и прямой, и все K косвенных молчат → Target недостижим.
        n.suspect(target)   // §5
    }
```

**Извлечь ожидание Ack в помощник `waitAck`**, т.к. теперь оно вызывается дважды
(фаза 1 и фаза 2) с разными таймаутами:

```go
// waitAck blocks until an Ack lands on ackCh or the timeout elapses (or ctx is
// cancelled). Returns true iff an Ack arrived. It does NOT touch pending — the
// caller owns pending[seq] lifetime.
func (n *Node) waitAck(ctx context.Context, ackCh <-chan struct{}, timeout time.Duration) bool {
    wctx, cancel := context.WithTimeout(ctx, timeout)
    defer cancel()
    select {
    case <-ackCh:
        return true
    case <-wctx.Done():
        return false
    }
}
```

**Инвариант, который легко сломать:** `pending[seq]` должен жить **всю пробу
целиком** (обе фазы), удаляться **один раз** в самом конце. Не удалять `pending[seq]`
между фазой 1 и фазой 2 — иначе релейный Ack от посредника прилетит на пустой
`pending` и потеряется. Сейчас удаление стоит сразу после `select`; **перенести
его за фазу 2**.

### 3.3. Отправка PingReq — вынести в помощник (по желанию)

Рассылка K одинаковых `PingReq` (разные адреса) — цикл `Send`. Ошибки `Send`
best-effort: логировать и продолжать, не прерывать рассылку (потеря PingReq —
нормальный сценарий, ровно тот, от которого защищаемся). Пиггибек —
`collectGossip(nil)` на каждом PingReq (можно и `nil`-exclude: PingReq
инициируется нами, а не отвечает кому-то).

### 3.4. `receiveLoop` — обработка `KindPingReq` (сейчас игнорируется)

Ветка `case protocol.KindPingReq` сейчас только логирует. **Заменить телом
посредника** (§1.3). Обработчик, назовём `handlePingReq(ctx, msg, fromAddr)`:

```
handlePingReq(ctx, msg):     // msg.From=инициатор, msg.Target=цель, msg.SeqNo=S
    // 0. gossip из msg уже впитан выше (applyGossip до switch) — не трогать.
    target := list lookup msg.Target
    if target нет в списке ИЛИ target.ID == self:
        // Нечего пинговать / просят пинговать нас самих — молча выходим.
        // (self как target быть не должно: инициатор исключает себя из mediators,
        //  но и Target тоже; на всякий случай — guard.)
        return
    // 1. свой Ping к Target со СВОИМ seq
    n.mu.Lock(); n.seqNo++; s2 := n.seqNo; ch := make(chan struct{},1); pending[s2]=ch; n.mu.Unlock()
    send Ping{From:self, SeqNo:s2, Updates: collectGossip(nil)} -> target.Addr
    ok := waitAck(ctx, ch, RTTTimeout)
    n.mu.Lock(); delete(pending,s2); n.mu.Unlock()
    // 2. если Target ответил — релеим Ack инициатору с ИСХОДНЫМ SeqNo S
    if ok {
        relay := Ack{From:self, SeqNo: msg.SeqNo, Updates: collectGossip(nil)}
        send relay -> адрес инициатора   // см. §3.6 про адрес
    }
    // если !ok — молчим (инициатор поймёт по своему таймауту)
```

**КРИТИЧНО — не блокировать `receiveLoop`.** `handlePingReq` **ждёт** Ack от
Target (до `RTTTimeout`) — это блокирующая операция. Если выполнять её прямо в
`receiveLoop`, узел на время ожидания **перестаёт принимать любые другие пакеты**
(в т.ч. Ack на собственные пробы и чужие Ping) — это дедлок-риск и деградация.
**Поэтому `handlePingReq` запускать в отдельной горутине:** `go
n.handlePingReq(ctx, msg)`. То же соображение стоит проверить для существующей
ветки `KindPing` (она НЕ блокирует — только шлёт Ack синхронно, это дёшево,
оставить как есть) и `KindAck` (не блокирует). Блокирующая только новая
PingReq-ветка.

**Следствие для конкурентности (`-race`):** горутина посредника обращается к
`n.mu` (seqNo/pending/Rand), `n.list` (потокобезопасен по своему RWMutex),
`n.tr` (потокобезопасен). Все обращения к `pending`/`seqNo`/`cfg.Rand` — под
`n.mu`, как уже заведено. Убедиться, что `collectGossip`/`applyGossip` и
`list.Merge` вызываются без удержания `n.mu` (они берут свой лок в `member.List`)
— не создавать вложенный захват двух локов.

### 3.5. Дренаж и идемпотентная очистка `pending[seq]` (инициатор)

Буфер `ackCh` = 1. Сценарии двойного Ack (прямой дошёл поздно + релей; или
несколько релеев от разных посредников):
- Первый сигнал буферизуется/потребляется `waitAck` → проба закрыта.
- Второй и последующие сигналы: к моменту их прихода `receiveLoop` уже мог
  удалить `pending[seq]` (в конце `probeOnce`), тогда `ch, ok := pending[seq]`
  даст `ok=false` и сигнал просто не отправляется — как в существующем коде для
  KindAck. Если же `pending[seq]` ещё жив, а буфер уже занят — **send в буфер-1
  канал заблокируется**. Существующий код шлёт `ch <- struct{}{}` **из
  receiveLoop синхронно** — на занятом буфере это заблокирует receiveLoop.

  **В Этапе 1/2 это не всплывало** (один Ack на один seq). В Этапе 3 двойной Ack
  реален. **Решение: неблокирующая отправка сигнала в `receiveLoop`:**
  ```go
  case protocol.KindAck:
      n.mu.Lock()
      ch, ok := n.pending[msg.SeqNo]
      n.mu.Unlock()             // НЕ удаляем здесь — владелец pending[seq] — probeOnce
      if ok {
          select {
          case ch <- struct{}{}:
          default:              // буфер занят => проба уже получила сигнал, дубликат отбрасываем
          }
      }
  ```
  **Важное изменение относительно текущего кода:** сейчас `KindAck` в
  `receiveLoop` сам делает `delete(n.pending, msg.SeqNo)`. С двухфазной пробой
  удалять запись в receiveLoop **нельзя** — иначе между фазой 1 и фазой 2 запись
  исчезнет и релей потеряется. **Владение `pending[seq]` переходит целиком к
  `probeOnce`**: только он удаляет ключ, один раз, в самом конце пробы. Аналогично
  посредник владеет своим `pending[s2]` и удаляет его сам после `waitAck`.
  receiveLoop только **шлёт сигнал** (неблокирующе) и запись **не удаляет**.

Перепроверить, что это изменение не ломает существующие тесты Этапа 1/2
(TestNode_PingAck_ConfirmsAlive и др.): там один Ack, `probeOnce` удаляет
`pending[seq]` после `select` — поведение сохраняется, просто удаление теперь
всегда на стороне probeOnce, а receiveLoop перестаёт дублировать delete.

### 3.6. Адрес инициатора для релея

Посредник шлёт релейный Ack инициатору. Адрес взять из **`pkt.Addr`** (адрес
отправителя PingReq на транспорте) — самый надёжный источник, не зависящий от
консистентности membership-списка посредника. Значит `handlePingReq` должен
получать `pkt.Addr` (передать `pkt` или `pkt.Addr` в сигнатуру). Не полагаться на
`list.lookup(msg.From).Addr` — посредник может ещё не знать инициатора (join не
досошёлся), а `pkt.Addr` есть всегда. (В текущем коде ветка KindPing уже так и
делает — шлёт Ack на `pkt.Addr`.)

---

## 4. Выбор K посредников (`pickMediators`)

Добавить метод:

```go
// pickMediators returns up to K distinct random members to relay a PingReq for
// target, excluding self and target itself. Fewer than K are returned when the
// cluster is too small. Uses cfg.Rand under n.mu (Rand is not concurrency-safe
// and is shared with seqNo/pending).
func (n *Node) pickMediators(target member.ID) []member.Member
```

Правила:
- **K = `cfg.IndirectNodes`** (дефолт 3, см. §3.1). Конфигурируемо через Config,
  не хардкод-константа в теле — тесту нужно уметь задавать K (в т.ч. K=1 для
  простых кейсов) и сид Rand для детерминизма.
- **Кандидаты = `list.Others()`** (все, кроме self) **минус сам `target`**.
  Посредник ≠ self (гарантирует `Others()`), посредник ≠ target (фильтруем явно:
  просить Target пинговать сам себя бессмысленно).
- **Дополнительно исключить не-`Alive` кандидатов?** — В Этапе 3 состояний, кроме
  Alive, по сети ещё почти нет (Suspect выставляется только этим же этапом,
  локально). Чтобы не усложнять и не завязываться на будущий Этап 4: **посредников
  выбирать из всех Others, кроме target** (любое состояние). Suspect-узел как
  посредник — не идеален, но безвреден (не ответит → просто не даст релея). Явно
  зафиксировать это как осознанное упрощение комментарием; фильтр по Alive —
  кандидат на Этап 4, не сейчас.
- **Выбор без повторов:** если кандидатов ≤ K — вернуть всех (в перемешанном или
  отсортированном порядке — неважно, все всё равно получат PingReq). Если больше K
  — выбрать K различных. Реализация: скопировать срез кандидатов, частичный
  Fisher–Yates на `cfg.Rand` (перемешать первые K позиций), взять первые K.
  **Детерминизм:** всё через `cfg.Rand` под `n.mu`, тест с фиксированным сидом
  получает воспроизводимый набор.
- **Пустой результат** (кластер из 2 узлов: единственный Other — это Target;
  посредников не осталось) — вернуть пустой срез. `probeOnce` при пустом наборе
  посредников пропускает ожидание фазы 2 и идёт сразу к выводу (Target
  недостижим прямым пингом, косвенно проверить некем → suspect). Это корректно:
  в кластере из 2 узлов indirect probing физически невозможен.

**Захват Rand:** `pickMediators` вызывается из `probeOnce` (сама по себе может
идти конкурентно с посредническими горутинами, которые тоже дёргают `cfg.Rand`
для своих seq — нет, seq через `n.seqNo++`, но Rand только тут и в выборе target).
Все обращения к `cfg.Rand` — строго под `n.mu`. Убедиться, что `pickMediators`
берёт `n.mu` внутри (или вызывается уже под ним) — но НЕ держит `n.mu` во время
`list.Others()` (Others берёт свой RWLock; вложенный захват двух локов не нужен и
опасен порядком). Рекомендация: `Others()` вызвать **до** взятия `n.mu`, затем
под `n.mu` только перемешать/выбрать индексы через Rand.

---

## 5. Провал обеих фаз → `StateSuspect` (`n.suspect`)

`member.StateSuspect` **уже объявлен** в `internal/member/member.go` (константа
есть, `String()` возвращает `"suspect"`). Автомат состояний менять не нужно;
`Merge` уже умеет precedence `Dead > Suspect > Alive` при равной incarnation.

Добавить помощник:

```go
// suspect demotes target to StateSuspect at its current incarnation after both
// the direct ping and every indirect ping went unanswered. Merge's equal-
// incarnation precedence (Suspect > Alive) lets this stick and re-queues it for
// gossip so the whole cluster learns the suspicion. The suspicion TIMER that
// escalates Suspect->Dead — and refute — are Этап 4, not here.
func (n *Node) suspect(target member.Member) {
    n.list.Merge(member.Member{
        ID:          target.ID,
        Addr:        target.Addr,
        Incarnation: target.Incarnation,   // SAME incarnation: Suspect>Alive at equal inc
        State:       member.StateSuspect,
    })
}
```

**Тонкость incarnation.** Suspect должен ставиться на **той же** incarnation, что
у известной alive-записи Target: тогда `stateRank(Suspect) > stateRank(Alive)`
при равной incarnation даёт `changed=true`, запись меняется на Suspect и уходит в
gossip. Если бы ставили на incarnation-1 — `Merge` бы её отверг. Брать
`target.Incarnation` из снапшота, с которым работал `probeOnce` (значение на
момент старта пробы). Гонки: если за время пробы Target успел заrefute'ить себя с
более высокой incarnation (Этап 4), наш Suspect на старой incarnation будет
корректно отвергнут `Merge` — это желаемое поведение, refute побеждает.

**Логирование.** Заменить нынешний лог «no ack … indirect probe deferred to
Этап 3» на осмысленный: при провале обеих фаз — `log.Printf("swim: %s
unreachable (direct + %d indirect) — marking suspect", target.ID, k)`.

**Граница с Этапом 4.** Здесь только переход Alive→Suspect. Таймер, который потом
двигает Suspect→Dead, взводится в Этапе 4 (`Clock` + suspicion-scheduler).
Пометить это `// TODO(Этап 4): arm suspicion timer to escalate Suspect->Dead`
рядом с `suspect`, чтобы Этап 4 знал точку встраивания.

---

## 6. Тест-кейсы (`internal/swim/swim_test.go`, `internal/protocol/*_test.go`)

Все swim-тесты — white-box (`package swim`), дёргают приватный `probeOnce`
напрямую (как существующие). Fake-транспорт из Этапа 1 lossless; для
избирательной потери **на Этап 3 нужна минимальная управляемая потеря на
fake** — см. §6.1. Всё под `-race`.

### 6.1. Управляемый drop на fake — минимально необходимое для Этапа 3

TECHNICAL_PLAN относит «полный симулятор drop/delay/partition» к Этапу 5, но
**центральный тест Этапа 3 невозможен без избирательной потери** (дропнуть только
линк инициатор→Target, оставив линки посредник↔Target рабочими). Разрешено ввести
**минимальный per-link drop** в `internal/transport/fake.go`, ровно столько,
сколько нужно тесту, не тащя весь Этап 5:

- Добавить в `FakeNetwork` управление потерей по направленному линку `(from,to)`:
  ```go
  // DropLink makes every packet sent from `from` to `to` vanish (one-way).
  // Minimal knob for the Этап 3 false-positive test; the full drop/delay/
  // partition simulator is Этап 5.
  func (n *FakeNetwork) DropLink(from, to string)
  ```
  Хранить в `FakeNetwork` `map[[2]string]bool` (или `map[string]map[string]bool`)
  под уже имеющимся `mu`. В `Fake.Send` перед доставкой проверить: если
  `(f.local, addr)` помечен как drop — вернуть `nil` (best-effort «пакет
  испарился»), не доставляя. Направленность обязательна: дропаем I→T, но НЕ T→I и
  НЕ M↔T.
- **Не добавлять** delay, partition, вероятностный drop-rate, seedable-drop —
  это Этап 5. Только детерминированный полный drop именованного направленного
  линка. Зафиксировать границу комментарием.
- Round-trip совместимость: существующие тесты (без DropLink) должны работать
  без изменений — DropLink по умолчанию пуст, поведение lossless сохраняется.
- Обновить doc-комментарий `FakeNetwork`/`Fake` («Этап 1: lossless…»): теперь
  «минимальный направленный drop для Этапа 3; полный симулятор — Этап 5».

### 6.2. Центральный тест проекта

**`TestNode_IndirectProbe_SuppressesFalsePositive`** — регресс на него
недопустим (ключевой инвариант всего проекта).

Топология: 3 узла I, T, M на одном `FakeNetwork`, все alive и знают друг друга.
- `network.DropLink("I", "T")` — пакеты I→T теряются (прямой Ping инициатора не
  дойдёт). Линки T→I, I↔M, M↔T **рабочие**.
- Запустить `Run` у T и M (они должны отвечать на Ping/PingReq). У I —
  дёрнуть `probeOnce` так, чтобы Target выпал именно T. **Как гарантировать выбор
  T:** либо сид `cfg.Rand` подобрать так, что `Others()[Intn]` даёт T (хрупко),
  либо — надёжнее — временно свести список I к `{self, T}` без M в `Others`… но M
  нужен как посредник. Поэтому: держать в списке I ровно `{I, T, M}`, а выбор
  target сделать детерминированным подбором сида (Others сортируется по ID —
  подобрать ID так, чтобы при известном сиде Intn(2) дал индекс T). Проще:
  **назвать узлы так, чтобы `Others()` = [M, T] или [T, M] и подобрать
  `rand.NewSource(seed)` дающий индекс T**; проверить фактический выбор в тесте
  ассертом или подобрать эмпирически (в комментарии зафиксировать сид). K=1 или
  K=2 — при 3 узлах единственный посредник для T это M, так что фаза 2 гарантирует
  PingReq к M.
- **Assert:** после `probeOnce(ctx)` вернулся — T в списке I остаётся
  `StateAlive` (**НЕ** Suspect). Косвенный пинг через M дозвонился до T (линк M↔T
  рабочий), M релеанул Ack инициатору → проба закрылась успехом → false positive
  погашен.
- Прогнать `-count=200` (тайминг-чувствительный тест: релей идёт через две
  сетевые фазы; подобрать RTTTimeout/IndirectTimeout щедро — секунды, как в
  существующих тестах, чтобы fake успевал).

### 6.3. Негативный кейс — реально недостижимый Target → Suspect

**`TestNode_IndirectProbe_AllFail_MarksSuspect`**: T недостижим отовсюду.
- Либо `DropLink` на всех линках к T (I→T, M→T), либо просто **не запускать** T
  (его Endpoint закрыт/не читает — Ping никому не отвечает).
- I знает I, T, M; M запущен и отвечает на PingReq (но его вложенный Ping к T
  тоже не получает Ack).
- **Assert:** после `probeOnce` T в списке I → `StateSuspect`. Проверяет, что при
  подлинном отказе цепочка доводит до Suspect (не зависает, не остаётся Alive).
- Прогнать `-count=200`.

### 6.4. Посредник релеит Ack (изоляция роли посредника)

**`TestNode_RelaysPingReq`** (аналог существующего `TestNode_RespondsAckToPing`,
но для PingReq): узел M запущен; отдельный «инициатор»-endpoint I вручную шлёт M
`PingReq{From:I, Target:T, SeqNo:S}`; T-endpoint запущен и отвечает на Ping.
- **Assert:** I получает `KindAck` с `SeqNo == S` и `From == M` (релей пришёл,
  seq сохранён). Проверяет §1.3 шаги 1–3 в изоляции, без фазовой логики
  инициатора.
- Вариант **`TestNode_PingReq_TargetSilent_NoRelay`**: T не запущен (молчит) → M
  за свой RTT не получает Ack от T → M **ничего** не шлёт I. Assert: I за разумный
  таймаут `Receive` **не** получает релейный Ack (§1.3 шаг 4 — молчание при
  провале). Осторожно с гонкой: дать I `Receive` с коротким context-таймаутом,
  ожидать `ctx deadline`, а не пакет.

### 6.5. Не-регресс существующего поведения

- **`TestNode_PingAck_ConfirmsAlive`, `TestNode_ProbeTimeout_NoAck_*`,
  `TestNode_RespondsAckToPing`, `TestNode_PingCarriesGossip`,
  `TestNode_ReceiveAbsorbsGossip`, `TestCluster_GossipConvergence`** —
  **все должны остаться зелёными без изменения смысла.** ВНИМАНИЕ:
  `TestNode_ProbeTimeout_NoAck_StaysAlive` (Этап 1) утверждал «нет Ack → Target
  остаётся Alive». В Этапе 3 при отсутствии посредников (кластер `{A, B}`, B не
  запущен) `Others()`=[B], `pickMediators(B)` исключает B (это target) и self →
  **пустой набор посредников** → фаза 2 пропускается → **Target B помечается
  Suspect**. Это **меняет ожидание старого теста.** Явно обновить его:
  переименовать/переосмыслить в `TestNode_ProbeTimeout_NoIndirect_MarksSuspect` и
  ассертить `StateSuspect` (в кластере из 2 узлов indirect невозможен, прямой
  провал → сразу suspect — это корректное SWIM-поведение). Зафиксировать смену
  ожидания комментарием со ссылкой на Этап 3.
- `TestCluster_GossipConvergence`: сеть lossless, все отвечают, никто не должен
  становиться Suspect. Убедиться, что фаза indirect не срабатывает ложно на
  здоровой сети (все прямые Ping получают Ack) и тест по-прежнему сходится к
  «все alive». **Прогнать `-count=200`** — это тайминг-чувствительный кластерный
  тест, на Этапе 2 именно он ловил флейк.

### 6.6. Wire round-trip

**`TestEncodeDecode_PingReq`** (в `internal/protocol/protocol_test.go` рядом с
существующими round-trip тестами): `Encode`→`Decode` для
`Message{Kind:KindPingReq, From:"I", Target:"T", SeqNo:42, Updates:[…]}` —
проверить, что `Kind`, `From`, `Target`, `SeqNo`, `Updates` сохранились. Если
round-trip тестов протокола ещё нет отдельного PingReq-кейса — добавить.

### 6.7. Обязательный прогон

Финально прогнать:
```
~/sdk/go/bin/go build ./...
~/sdk/go/bin/go vet ./...
~/sdk/go/bin/gofmt -l .
~/sdk/go/bin/go test -race ./...
~/sdk/go/bin/go test -race -count=200 ./internal/swim/   # тайминг-чувствительные
```

---

## 7. Что НЕ трогать / вне рамок Этапа 3 (жёсткая граница)

- **Suspicion-таймер `Suspect→Dead` — Этап 4.** Здесь только Alive→Suspect. Не
  заводить таймеры, не реализовывать переход в Dead. Оставить
  `// TODO(Этап 4): …` в `suspect` (§5).
- **`Clock`-абстракция — Этап 4.** Время по-прежнему через `context.WithTimeout`.
  Не вводить `Clock`, `SystemClock`, fake-clock, `Advance`.
- **Refute (опровержение своего Suspect бампом incarnation) — Этап 4.** Узел,
  услышавший Suspect про себя, пока НЕ реагирует (в Этапе 3 такой слух ещё некому
  сгенерировать против самого узла в тестах, а механизм refute — Этап 4).
- **Полный симулятор сети (drop-rate, delay, partition, seedable) — Этап 5.** В
  Этап 3 добавляется ТОЛЬКО детерминированный направленный `DropLink` (§6.1) —
  минимум под центральный тест. Не тащить вероятностный drop/delay.
- **Бинарный wire-формат — POST_MVP.** Остаётся JSON.
- **Техдолг Этапа 2 не чинить попутно** (разыменование в `PendingGossip`,
  заморозка бюджета на `len(members)`, перенос `StateChangedAt` на wire) — их
  триггеры Этап 4/5, не Этап 3. Не расширять скоуп.
- **CLI (`cmd/swim-discovery`)** в Этапе 3 не трогать (наблюдение
  membership-view — Этап 5). Живой прогон — существующим CLI Этапа 1/2.

Порог односторонний: если по ходу всплывёт необходимость ввести `Clock`,
реализовать suspicion-таймер, сменить контракт `Transport` шире, чем `DropLink`,
или вынести refute — **остановиться и вернуть вопрос**, а не расширять скоуп.

---

## 8. Порядок реализации (рекомендуемый)

1. `internal/protocol/protocol.go`: сверить наличие `KindPingReq`/`Target`,
   уточнить doc-комментарии (§2). Добавить `TestEncodeDecode_PingReq` (§6.6).
2. `internal/transport/fake.go`: `FakeNetwork.DropLink` + проверка в `Send`
   (§6.1). Прогнать существующие транспорт-тесты — должны быть зелёными.
3. `internal/swim/swim.go`:
   a. `Config.IndirectNodes`/`IndirectTimeout` + дефолты в `NewNode` (§3.1).
   b. Извлечь `waitAck` (§3.2); перевести `receiveLoop` KindAck на неблокирующую
      отправку сигнала без `delete` (§3.5) — прогнать старые тесты Этапа 1/2.
   c. `pickMediators` (§4).
   d. Двухфазный `probeOnce` (§3.2) + `suspect` (§5).
   e. `handlePingReq` в отдельной горутине из `receiveLoop` (§3.4).
4. Тесты §6.2–6.5, обновить `TestNode_ProbeTimeout_*` (§6.5).
5. Полный прогон §6.7, включая `-count=200` для swim-пакета.

---

## 9. Саммари ключевых решений (для ревьюера и Этапа 4)

- **Корреляция релейного Ack — через сквозной `SeqNo` инициатора**, который
  посредник возвращает неизменным; `chan struct{}` в `pending` менять на
  `chan Update` НЕ потребовалось (§1.1).
- **Одна проба = один seq = один канал на обе фазы**; `pending[seq]` живёт всю
  пробу, удаляется один раз в конце `probeOnce`; **владение перенесено из
  `receiveLoop` в `probeOnce`**, receiveLoop только шлёт неблокирующий сигнал
  (§3.5) — это правка существующего Ack-пути, требует прогона старых тестов.
- **Посредник обрабатывает PingReq в отдельной горутине** (блокирующее ожидание
  Ack не должно морозить receiveLoop) (§3.4).
- **K = `Config.IndirectNodes` (дефолт 3)**, посредники — random из
  `Others()` минус target, без повторов, через `cfg.Rand` под `n.mu` (§4).
- **Провал обеих фаз → `Merge(target, StateSuspect, той же incarnation)`**;
  suspicion-таймер и refute — Этап 4 (§5, §7).
- **`FakeNetwork.DropLink(from,to)`** — минимальный направленный drop, только под
  центральный тест; полный симулятор — Этап 5 (§6.1).
- **Старый `TestNode_ProbeTimeout_NoAck_StaysAlive` меняет ожидание**: в кластере
  из 2 узлов посредников нет → прямой провал ведёт прямо в Suspect (§6.5).
