# swim-discovery — TECHNICAL_PLAN

Детальный стек, архитектура и разбивка по Этапам. Опорный высокоуровневый
документ — [`PLAN.md`](PLAN.md). Конвенции кодирования —
[`../.claude/skills/go-swim-discovery-dev/SKILL.md`](../.claude/skills/go-swim-discovery-dev/SKILL.md).

## Стек и ключевые решения

- **Язык:** Go 1.23 (тулчейн `~/sdk/go/bin`). Только стандартная библиотека —
  `net`, `context`, `encoding/json`, `sync`, `time`, `math/rand`. Внешних
  зависимостей и Docker нет.
- **Транспорт — UDP.** SWIM датаграммный и connectionless; TCP спрятал бы
  главную учебную мину (потеря пакетов). Прод-адаптер — `net.ListenUDP` на
  localhost; в тестах тот же порт `transport.Transport` подменяется на fake.
- **Wire-формат — JSON (v1).** Читаемость важнее компактности, пока учишь
  протокол. Компактный бинарный framing — POST_MVP.
- **Конкурентность — goroutine + channels.** Один узел = несколько горутин:
  receive-loop транспорта, probe-loop, suspicion-scheduler. `member.List` под
  `sync.RWMutex`. Всё гоняется под `-race`.
- **Время — инжектируемое.** Внутри probe/suspicion логики **не звать
  `time.Now()`/`time.After` напрямую** — только через абстракцию `Clock`
  (заводится в Этапе 4), иначе suspicion-тесты станут флейки на `Sleep`.
- **Рандом — инжектируемый `*rand.Rand`.** Выбор случайного члена для probe и K
  посредников для indirect-probe должен быть seedable, чтобы тесты были
  детерминированы.

### Структура: `internal/` + `cmd/`, почему нет `pkg/`

Ядро в `internal/`, компилятор запрещает внешний импорт — это учебное
приложение, а не публикуемая библиотека. `pkg/` не заводить.

```
internal/member/    member.go      модель узла (Member, State), List + merge
internal/transport/ transport.go   порт Transport + UDP-адаптер
                    fake.go        (Этап 5) симулятор сети drop/delay
internal/protocol/  protocol.go    Message-конверт, Ping/Ack/PingReq, Update, Encode/Decode
internal/swim/      swim.go        (Этап 1+) ядро: probe/gossip/suspicion-циклы, склейка
cmd/swim-discovery/ main.go        тонкий CLI: флаги, запуск узла, (Этап 5) observe
```

`main.go` — **тонкий**: разбор флагов, сборка узла, запуск. Никакой
протокольной логики в `main`.

## Разбивка по Этапам

### Этап 0 — Скелет (готов)

`go mod init github.com/akomyagin/swim-discovery`. Пакеты-заглушки
`member`/`transport`/`protocol` с `// TODO(Этап N): implement` и `panic(...)` в
телах, CLI-заглушка. Compile-time assertion `var _ Transport = (*UDP)(nil)`.
`go build ./...` и `go vet ./...` проходят чисто.

### Этап 1 — Модель узла + membership-список + direct ping/ack (UDP, localhost) (готов)

- `member.List`: `map[member.ID]*Member` под `sync.RWMutex`, seed локальным
  узлом; `Merge` реализует **precedence по (Incarnation, State)**: выше
  incarnation всегда побеждает; при равной incarnation `Dead > Suspect > Alive`;
  возвращает `changed` для будущего re-gossip.
- `transport.UDP`: `net.ListenUDP`, `Send` через `WriteToUDP`, receive-loop над
  `ReadFromUDP` → канал `Packet`. Плюс `transport.Fake`/`FakeNetwork` —
  минимальный in-memory роутер без потерь для юнит-тестов (полноценный
  симулятор с drop/delay — по-прежнему Этап 5, дорастает из этого же файла).
- `protocol.Encode/Decode`: JSON, пока только `KindPing`/`KindAck` (без Updates).
- `internal/swim`: `Node`/`Config` с инжектируемым `*rand.Rand` (введён уже в
  Этапе 1 — нужен детерминированный выбор цели пробы и переиспользуется в
  Этапе 3 для K посредников); probe-loop раз в интервал выбирает случайного
  члена, шлёт Ping, ждёт Ack в пределах RTT-таймаута (`context.WithTimeout`);
  при Ack — подтверждает alive, при таймауте в Этапе 1 не делает ничего
  (Suspect/PingReq — будущие этапы).
- CLI: два+ процесса на разных портах, `--seed` для присоединения (сид сразу
  заносится в список как alive-член, чтобы probe заработал в обе стороны).
- Тесты: merge-precedence (table-driven), round-trip Encode/Decode, ping/ack
  над fake-транспортом без потерь; всё под `-race`.

Живой прогон подтверждён на реальном UDP (localhost) и независимым ревью
(8 углов + верификация находок).

### Этап 2 — Gossip-рассылка дельт членства (piggyback) (готов)

- В `protocol.Message` активируются `Updates []Update`; каждый исходящий
  Ping/Ack пиггибекает батч свежих изменений членства (`PingReq` формально
  готов принять Updates на приёме, но сам не отправляется — Этап 3).
- На приёме сообщения — прогнать все `Updates` через `List.Merge`; изменённые
  записи попадают в исходящий piggyback (эпидемическое распространение).
  `member.List` хранит параллельную карту `gossipTx map[ID]int` (остаток
  ретрансляций) под тем же `sync.RWMutex`; `PendingGossip(limit, exclude)`
  отдаёт наименее распространённые первыми, декрементит бюджет.
- Бюджет ретрансляций на изменение — `3·⌈log2(N+1)⌉` (N — известные члены).
  **Отклонение от первоначального плана**, зафиксированное в коде
  (`gossipRetransmitBudget` в `member.go`): план предполагал
  `⌈log2(N+1)⌉` без множителя, но это эмпирически не сходилось на редкой
  топологии (9/10 наборов seed не сошлись на кольце из 7 узлов) — множитель
  `λ=3` classic SWIM оказался нужен по факту, не по вкусу.
- `PendingGossip` принимает `exclude map[ID]bool`: Ack не эхо-рассылает
  Updates обратно тому пиру, который их только что прислал (гарантированно
  бесполезная ретрансляция). Без этого исключения бюджет мог перманентно
  исчерпаться до охвата всего кластера — обнаружено как флейк
  `TestCluster_GossipConvergence` (~4% прогонов), устранено, подтверждено
  200/200 повторов.
- self анонсируется сразу в `NewList` (взвод в `gossipTx`), иначе join
  однобокий — seed не узнает о новом узле, пока тот сам не пропингует.
- Тесты: 7 узлов на fake-транспорте в разреженной кольцевой топологии (каждый
  знает только соседа), `TestCluster_GossipConvergence` проверяет сходимость
  до дедлайна; всё под `-race`.

Живой прогон подтверждён на реальном UDP (localhost, 4–5 процессов,
цепочечная топология) и независимым ревью (8 углов + верификация находок).

### Этап 3 — Indirect probing (PingReq к K посредникам) — SWIM-специфика (готов)

- Если прямой Ping не получил Ack за `RTTTimeout`: `probeOnce` рассылает
  `KindPingReq{Target, SeqNo}` до `Config.IndirectNodes` (K, деф. 3) случайным
  **другим** членам (`pickMediators`, частичный Fisher–Yates под тем же `n.mu`,
  что и `seqNo`/`pending`, `Others()` вызывается до захвата лока). Посредник
  (`handlePingReq`, в отдельной горутине — блокируется до `RTTTimeout`,
  receiveLoop не должен из-за этого простаивать) пингует Target под **своим**
  seq и, если тот ответил, релеит Ack инициатору со **исходным** `SeqNo`.
- **Корреляция релейного Ack — сквозной `SeqNo` инициатора**, без изменения wire
  или `chan struct{}` в `pending`: любой Ack (прямой или релейный) с известным
  активным `SeqNo` закрывает пробу. Одна проба = один `seq` = один канал на обе
  фазы; владение записью `pending[seq]` — целиком у `probeOnce`
  (`handlePingReq` — у своей вложенной пробы), receiveLoop только шлёт
  неблокирующий сигнал и **не** удаляет запись (см. `pending`'s doc-комментарий
  в `swim.go`) — иначе релей между фазами потерялся бы.
- Target объявляется недостижимым **только если** молчат и прямой, и все K
  косвенных пингов — тогда `probeOnce` мержит его как `StateSuspect` на той же
  incarnation (`Merge`'s Suspect>Alive precedence при равной incarnation даёт
  `changed=true`). Suspicion-**таймер** `Suspect→Dead` и refute — Этап 4, здесь
  только переход в Suspect.
- Минимальный `transport.FakeNetwork.DropLink(from, to)` — направленный
  детерминированный дроп линка, ровно то, что нужно центральному тесту; полный
  симулятор (rate/delay/partition) остаётся Этапом 5.
- Тесты (центральный для проекта —
  `TestNode_IndirectProbe_SuppressesFalsePositive`): fake-транспорт с
  **избирательной** потерей через `DropLink` — дропаем только пакеты между
  инициатором и Target, но не между посредниками и Target; ассертим, что Target
  остаётся Alive (false positive НЕ случился). Устойчивость подтверждена
  `-race -count=200`.
- **Техдолг Этапа 3** (не баги, найдены независимым ревью, осознанно не
  исправлены — триггеры пересмотра):
  - `handlePingReq`/`probeOnce`-фаза-1 дублируют один и тот же примитив «прямой
    Ping под свежим seq → wait → cleanup». **Триггер: Этап 4**, когда
    `waitAck`'s таймаут станет `Clock`-driven — если поменять только в
    `probeOnce`, посреднические пробы останутся wall-clock-таймингом, а прямые
    уйдут на fake-clock, реинтродуцируя как раз ту флейкующую гонку, ради
    которой вводился `Clock`. Рассмотреть общий `directPing` хелпер.
  - Успешный ре-пробинг цели, уже помеченной `Suspect` на той же incarnation,
    **не** возвращает её в Alive — `Merge`'s precedence отклоняет это по
    дизайну (только self-refute с бо́льшим incarnation снимает подозрение, как
    в классическом SWIM). Не баг, но неочевидное поведение — задокументировано
    комментарием у `Merge` в `probeOnce` (`swim.go`).
  - Обработка входящего `KindPingReq` порождает горутину без ограничения на
    число одновременных медиаций (`// TODO(Этап 5)` рядом в `swim.go`). Не
    проблема на масштабе localhost-теста; при реальных партициях/большом
    кластере (Этап 5) может стать вектором роста горутин — пересмотреть тогда.

### Этап 4 — Suspicion-механизм с таймаутами

- Переход в `Suspect` уже реализован в Этапе 3 (`swim.suspect`, `probeOnce`);
  здесь добавляется **таймер**, который взводится при этом переходе и по
  истечении без опровержения переводит узел в `Dead` (факт гоняется в gossip).
- **Опровержение (refute):** узел, узнавший о слухе `Suspect`/`Dead` про себя,
  бампает свой `Incarnation` и рассылает `Alive` с новым номером — по
  merge-precedence он вытесняет слух.
- Ввести абстракцию `Clock` (прод — системные часы; тест — управляемый fake с
  `Advance`), suspicion-таймеры крутить через неё.
- Тесты: suspect→dead по таймауту (прокрутка fakeClock, без `Sleep`); refute
  отменяет переход; гонки таймеров под `-race`.

### Этап 5 — Симулятор сети + детерминированные тесты + CLI

- `transport.Fake`: реализует `Transport`, роутит пакеты между
  зарегистрированными узлами in-memory с **управляемыми потерей и задержкой**
  (per-link drop-rate, delay, полный partition). Seedable rand для
  воспроизводимости.
- Свести false-positive-сценарии в детерминированный набор: высокий packet loss
  без indirect probing даёт ложные смерти; с indirect probing — не даёт.
- CLI-подкоманда наблюдения: печать текущего membership-view узла (кто
  alive/suspect/dead) для ручного прогона кластера на localhost.

## Тестовая стратегия (сквозная)

- **Детерминизм прежде всего.** Никаких «прогнал 5 процессов, вроде сошлось».
  Fake-транспорт + fakeClock + seeded rand → тесты воспроизводимы и не флейки.
- **Ярусы:** (1) юниты на merge-precedence и Encode/Decode round-trip;
  (2) интеграция протокола на fake-транспорте (сходимость gossip, indirect
  probe, suspect→dead); (3) `-race` на весь пакет для конкурентных циклов.
- **Центральный инвариант проекта:** под контролируемой потерей *только* твоих
  пакетов к цели живой узел **не** должен объявляться мёртвым (indirect probing
  работает). Это ключевой тест Этапа 3, регресс на него недопустим.
- End-to-end (по требованию): реальный запуск N процессов на localhost и ручное
  наблюдение сходимости через CLI.
