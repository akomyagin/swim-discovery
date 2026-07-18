# CLAUDE.md

Guidance for Claude Code when working in the `swim-discovery` repository.

## Что это

Учебная реализация **service discovery на gossip-протоколе** (мини-Serf /
мини-Consul membership) на Go: узлы находят друг друга и детектят отказы через
SWIM-подобный протокол, **без центрального координатора**. Каждый узел держит
собственный, со временем сходящийся (eventually consistent) membership-список.
Pet-проект соло-разработчика: цель — техническая сложность и обучение, не
бизнес. Бюджет $0, только стандартная библиотека Go, без внешних зависимостей и
Docker — кластер эмулируется процессами на localhost.

Документация: [`docs/PLAN.md`](docs/PLAN.md),
[`docs/TECHNICAL_PLAN.md`](docs/TECHNICAL_PLAN.md),
[`docs/POST_MVP_PLAN.md`](docs/POST_MVP_PLAN.md). Конвенции разработки —
[`.claude/skills/go-swim-discovery-dev/SKILL.md`](.claude/skills/go-swim-discovery-dev/SKILL.md).

## Раскладка

| Путь | Содержимое |
|---|---|
| `internal/member/` | модель узла (`Member`, `State`) + membership-список с merge по incarnation |
| `internal/transport/` | порт `Transport` + UDP-адаптер (+ fake-симулятор сети, Этап 5) |
| `internal/protocol/` | wire-сообщения SWIM (Ping/Ack/PingReq + gossip `Update`), `Encode`/`Decode` |
| `internal/swim/` | (Этап 1+) ядро: probe/gossip/suspicion-циклы |
| `cmd/swim-discovery/` | тонкий CLI: запуск узла, наблюдение за кластером |
| `docs/` | PLAN, TECHNICAL_PLAN, POST_MVP_PLAN |

## Ключевые технические решения (зафиксированы)

- **Транспорт — UDP, за портом `transport.Transport`.** SWIM датаграммный;
  прод-адаптер — `net.ListenUDP` на localhost, в тестах тот же порт подменяется
  на fake-симулятор с управляемой потерей/задержкой. Код ядра зависит **только**
  от интерфейса, не от `*net.UDPConn`.
- **Только стандартная библиотека.** `net`, `context`, `encoding/json`, `sync`,
  `time`, `math/rand`. Внешних зависимостей и Docker нет.
- **Wire-формат — JSON в v1.** Читаемость важнее компактности; бинарный
  framing — POST_MVP.
- **Инжектируемые `Clock` и `*rand.Rand`.** Внутри probe/suspicion логики не
  звать `time.Now()`/`time.After` напрямую и не брать глобальный rand — иначе
  тесты failure detection станут флейки. Прод — системные реализации, тест —
  управляемые fake.
- **Центральный инвариант проекта:** под потерей *только* твоих пакетов к цели
  живой узел **не** должен объявляться мёртвым — indirect probing обязан гасить
  false positive (ключевой тест Этапа 3).

## Команды

```bash
go build ./...   # сборка
go vet ./...     # статический анализ
go test ./...    # тесты (юниты + интеграция на fake-транспорте, без реальной сети)
go test -race ./...   # гонки — обязательно для конкурентных циклов (Этапы 1+)
gofmt -l .       # проверка форматирования (пусто = ок)
```

Тулчейн Go — в `~/sdk/go/bin` (может не быть в `PATH`; при необходимости
`~/sdk/go/bin/go`). Перед коммитом прогнать: `gofmt -l .` (пусто),
`go vet ./...`, `go test -race ./...`.

## Конвенции проекта

- **Язык:** документация и subject коммитов — на русском; код, идентификаторы и
  комментарии в коде — на английском.
- **Коммиты:** conventional-commit с русским subject, напр.
  `feat(swim): indirect probing через K посредников для защиты от false positive`.
  Завершать коммит трейлером `Co-Authored-By: Claude`.
- **Ветки:** новая ветка от `master` на каждый Этап (напр. `stage/3-indirect-probe`)
  → PR → merge в `master`. PR target — `master`.
- Заглушки помечать `// TODO(Этап N): ...`; при реализации **заменять
  содержимое файла**, не плодить `*_v2.go`. Compile-time assertions
  (`var _ Transport = (*UDP)(nil)`) не удалять — они держат контракт.

## Пайплайн разработки (по Этапам)

Актуальный проверенный пайплайн портфеля (с Fable 5). Для каждого Этапа:

1. **Проверка готовности — Sonnet 5 (основной чат).**
2. **Планирование — Opus 4.8**, только если этап требует детального плана
   (отдельный Agent-вызов, `model: opus`). Спланировать Этап по
   `TECHNICAL_PLAN.md`, не писать код.
3. **Написание кода — Fable 5** (отдельный Agent-вызов, `model: claude-fable-5`)
   по плану, или напрямую если план не потребовался.
4. **Проверка покрытия + тестирование + работоспособность — Sonnet 5 (основной
   чат).** Проверить тестовое покрытие, дописать тесты (обязательно —
   детерминированные тесты на fake-транспорте: сходимость gossip, indirect probe
   гасит false positive, suspect→dead по таймауту; всё под `-race`), проверить,
   что реально работает (в т.ч. живой прогон N процессов на localhost).
5. **Независимое ревью — Opus через Agent-тул (`model: opus`), `/code-review`
   на diff ветки.** Ревью запускать на diff ветки Этапа против `master`.
6. **Цикл исправлений — до 3 итераций.** Правки по замечаниям ревью, повторное
   ревью; не более трёх кругов.
7. **Commit + push + PR в `master`.** Conventional-commit, русский subject,
   трейлер `Co-Authored-By: Claude`. Мержить в `master`.

Не коммитить и не пушить без явной просьбы пользователя.
