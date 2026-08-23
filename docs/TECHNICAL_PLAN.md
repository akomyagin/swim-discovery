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

### Этап 2 — Gossip-рассылка дельт членства (piggyback)

- В `protocol.Message` активируются `Updates []Update`; каждый Ping/Ack/PingReq
  пиггибекает батч свежих изменений членства.
- На приёме сообщения — прогнать все `Updates` через `List.Merge`; изменённые
  записи попадают в исходящий piggyback (эпидемическое распространение).
- Ограничить размер батча и число ретрансляций записи (чтобы не было штормов);
  выбирать «самые свежие/наименее распространённые» апдейты.
- Тесты: 5–10 узлов на fake-транспорте, новый/ушедший узел становится известен
  всем за ограниченное число раундов (сходимость).

### Этап 3 — Indirect probing (PingReq к K посредникам) — SWIM-специфика

- Если прямой Ping не получил Ack в RTT-таймаут: выбрать K случайных **других**
  членов, послать каждому `KindPingReq{Target}`; посредник пингует Target и
  релеит Ack обратно инициатору.
- Target объявляется недостижимым (→ Suspect в Этапе 4) **только если** молчат и
  прямой, и все K косвенных пингов. Так потеря именно *твоего* пакета к Target не
  приводит к false positive.
- Тесты (центральные для проекта): fake-транспорт с **избирательной** потерей —
  дропаем только пакеты между инициатором и Target, но не между посредниками и
  Target; ассертим, что Target остаётся alive (false positive НЕ случился).

### Этап 4 — Suspicion-механизм с таймаутами

- Недостижимый после indirect-probe узел → `Suspect`, взводится
  suspicion-таймер. По истечении без опровержения → `Dead`, факт гоняется в
  gossip.
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
