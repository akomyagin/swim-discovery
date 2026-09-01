# Handoff — 2026-08-27: Этап 6 готов, PR #6 открыт (не смержен)

Этот файл заменяет `2026-08-23-etap5-done-mvp-next.md` (устарел — Этап 6
реализован, техдолг оттуда §2 разобран явно ниже). Старый файл удалён.

## 1. Где мы

- **Этап 6 (eviction Dead-узлов из membership-списка) реализован, протестирован,
  прошёл независимое ревью и закоммичен** на ветке `stage/6-eviction` —
  коммит `26e7e53` («Этап 6: eviction Dead-узлов из membership-списка +
  фильтр Dead в probeOnce»). Ветка запушена на `origin`.
- **PR #6 открыт, НЕ смержен:**
  <https://github.com/akomyagin/swim-discovery/pull/6> (`stage/6-eviction` →
  `master`, `mergeable: MERGEABLE` на момент этого хендоффа).
  **Первое действие новой сессии: проверить `gh pr view 6` — не появились ли
  комментарии/изменения статуса, затем спросить пользователя, мержить ли.**
  Мерж — только по явному подтверждению (см. `CLAUDE.md` проекта, шаг 8
  пайплайна: «Commit + push + PR — только в основной сессии», мерж отдельно).
- Диф PR (для справки): 7 файлов, +1224/-8 строк — `cmd/swim-discovery/main.go`,
  `docs/TECHNICAL_PLAN.md`, `docs/plans/stage-6-eviction.md` (новый),
  `internal/member/member.go`, `internal/member/member_test.go`,
  `internal/swim/swim.go`, `internal/swim/swim_test.go`.
- Этап 6 прошёл полный пайплайн: план (`Agent model=opus`, без
  `subagent_type` — обычный `claude` с Write-доступом, чтобы не упереться в
  read-only `Plan`-тип, см. память `plan-agent-no-write`) → кодинг
  (`Agent model=fable`, с первого раза) → тесты + живой прогон (сам, основная
  сессия) → 8-угловое независимое ревью (`Agent model=opus`, параллельно) →
  один цикл мелких фиксов → актуализация `docs/TECHNICAL_PLAN.md` → commit →
  push → PR (все три — по явному запросу пользователя, отдельными сообщениями).

## 2. Ключевые решения Этапа 6 (полные обоснования — `docs/TECHNICAL_PLAN.md`
§«Этап 6» и `docs/plans/stage-6-eviction.md`)

- **`member.List.Evict(id, inc) (evicted bool)`** — атомарно удаляет запись
  из `members` **и** `gossipTx` под одним `l.mu`, с precedence-guard (только
  `Dead` на той же incarnation) и запретом эвикции self. Закрывает скрытый
  инвариант, на котором держался `PendingGossip`'s `*l.members[id]`
  (техдолг Этапов 2-4, дважды отложен — Этап 4, Этап 5).
- **Таймер эвикции живёт в `swim.Node`, не в `List`** — зеркало
  suspicion-таймера Этапа 4 (`evictMu`/`evictions`/`armEviction`/
  `cancelEviction`/`evict`/`stopEvictionTimers`). `List` остаётся чистой
  структурой + precedence, без понятия времени.
- **`Config.DeadTimeout`, дефолт `10s`, упорядочен ВЫШЕ `SuspicionTimeout`
  (`5s`)** — лестница `RTTTimeout+IndirectTimeout ≪ SuspicionTimeout <
  DeadTimeout` продолжена. Grace-период даёт запас над типичной эпидемической
  сходимостью Dead-рамора.
- **Фильтр Dead-целей — в `probeOnce`, не в `Others()`** (контракт `Others()`
  не менялся; Dead-посредники в `pickMediators`/`handlePingReq` по-прежнему
  безвредны). Устраняет техдолг Этапа 4 «probeOnce пробит Dead вечно».
- **Revive эвикнутого узла — без нового кода**: отсутствующая запись в
  `Merge` — свежая вставка (`ok=false`). Гонка revive↔eviction закрыта
  двойным guard'ом (evict-callback под `evictMu` + `List.Evict` под `l.mu`).

## 3. Тестирование и живой прогон (подтверждено фактически)

- `go build/vet/gofmt` чисто; `go test -race ./...` зелено (полный прогон,
  все 4 пакета).
- 6 юнитов `member.List.Evict` + 7 протокольных тестов на `fakeClock`
  (`internal/swim/swim_test.go`), включая `TestNode_EvictionConcurrentArmCancel`
  (добавлен по находке ревью — реальная гонка `evict`-колбэка с `applyGossip`
  в разных горутинах, аналог `TestNode_SuspicionConcurrentArmCancel` Этапа 4).
  Все — `-race -count=200`, 0 падений.
- Живой прогон: 3 UDP-процесса на localhost (`127.0.0.1:7901/7902/7903`,
  третий в `observe`). Убитый узел (7901, `kill -9`) прошёл
  alive→suspect (21:20:26 dead-объявление) →evicted (21:20:36, ровно
  `DeadTimeout`=10s после dead). Лог: ровно одна строка «evicting
  127.0.0.1:7901 after 10s dead», «marking suspect» после dead-объявления
  больше не логировался, `observe` показал `members` 3→2.

## 4. Независимое ревью — 8 углов параллельно (`Agent model=opus`)

Углы: line-by-line, concurrency/race safety, altitude (doc-comment accuracy),
simplification/reuse, efficiency, cross-file consistency/tracer,
removed-behavior/regression, test coverage adequacy. **Критичных находок и
регрессов не выявлено.**

Один цикл фиксов (все применены и подтверждены зелёным прогоном):
- `cmd/swim-discovery/main.go:120-124` — устаревший doc-комментарий
  («Этап 5 decision: no eviction of Dead records») прямо противоречил
  новому поведению. Найден **независимо двумя углами** (altitude,
  cross-file tracer) — сильный сигнал по конвенции проекта, исправлен без
  колебаний.
- `internal/swim/swim.go` (`applyGossip`'s doc-комментарий) — не упоминал,
  что Dead-рамор теперь тоже взводит eviction-таймер (раньше комментарий
  описывал Dead только как отменяющий suspicion). Уточнён.
- Добавлен `TestNode_EvictionConcurrentArmCancel` — угол concurrency/race
  отметил, что готовые eviction-тесты гоняли `evict` только последовательно
  через `fakeClock.Advance` (синхронно на том же горутине), без реальной
  конкурентности с `applyGossip`, в отличие от suspicion-аналога Этапа 4.
  Закрыто прямым переносом паттерна `TestNode_SuspicionConcurrentArmCancel`.

**Находки, осознанно НЕ исправленные** (зафиксированы в
`docs/TECHNICAL_PLAN.md` §«Этап 6» с триггерами пересмотра):
- `Config` не валидирует `DeadTimeout ≥ SuspicionTimeout` — консистентно с
  остальным `Config` (тайминги нигде не валидируются друг против друга).
  Триггер: появление CLI-конфигурации таймаутов.
- Множественная одновременная эвикция и цикл
  Suspect→Dead→Suspect(higher-inc)→Dead — не покрыты отдельными тестами;
  код зеркалит уже проверенный suspicion-паттерн Этапа 4 один-в-один, риск
  низкий. Триггер: баг-репорт по многоузловому одновременному отказу.
- `evictionFixture` (`swim_test.go`) почти дублирует `suspicionFixture` —
  план осознанно завёл отдельную фикстуру, чтобы не трогать общую (риск
  сдвинуть тайминги существующих suspicion-тестов). Косметика, не техдолг.

## 5. Дальше

1. **Проверить статус PR #6** (`gh pr view 6`) и спросить пользователя,
   мержить ли — не мержить самовольно.
2. **После мержа — снова нет предопределённого «Этапа 7».** Eviction был
   первым кандидатом из техдолга (см. старый хендофф §5.2) — теперь закрыт.
   Как и после Этапа 5, нужно явно спросить пользователя: остановиться на
   текущем результате (MVP + eviction), или выбрать идею из
   `docs/POST_MVP_PLAN.md` (реальная многомашинная сеть; TCP-антиэнтропия/
   push-pull sync; шифрование трафика; произвольные теги узла + запросы по
   членству; бинарный wire-формат; Lifeguard-адаптивные таймауты;
   метрики/трейсинг сходимости; persistent snapshot) и завести новый Этап тем
   же пайплайном. POST_MVP_PLAN — непроритизированный список, не план.
3. Пайплайн (план → Fable-кодинг → тесты/живой прогон → 8-угловое ревью →
   фиксы → доки → commit/push/PR по явному запросу) подтверждён 6 раз подряд
   без серьёзных находок на финальном ревью — использовать тот же для
   следующего Этапа, если будет.
