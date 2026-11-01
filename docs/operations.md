# Эксплуатация

## Подготовка окружения

Helm устанавливает API и worker одного сервиса, probes, лимиты ресурсов, autoscaling и политики доступности/сети. PostgreSQL, Kafka, ingress controller и их HA/backup lifecycle предоставляются окружением. Образ необходимо заранее собрать и опубликовать; `image.tag` обязателен.

Создайте отдельные external Secrets для профилей `apiSecret`, `workerSecret`, `migrationSecret`; храните значения в External Secrets/Vault/облачном Secret Manager, а не в values или Git:

- API: `DATABASE_URL` с правами чтения/создания опросов, `VOTE_DATABASE_URLS` с правами чтения проекции, `ADMIN_TOKEN`, `SIGNING_KEY`, producer Kafka credentials.
- Worker: `DATABASE_URL` только для чтения метаданных, `VOTE_DATABASE_URLS` с правами вставки метаданных голосовых шардов/голосов и обновления агрегатов; consumer Kafka credentials. Admin token и signing key worker не нужны.
- Migration: адреса всех БД с отдельными DDL credentials. Ни API, ни worker не должны иметь CREATE/ALTER/DROP. `MODE=migrate` применяет схему к primary и всем уникальным vote DSN; runtime режимы миграции не запускают.

Одинаковые физические БД могут иметь разные DSN/пользователей в профилях. Важно сохранить одинаковый порядок и соответствие данных в `VOTE_DATABASE_URLS` всех API/worker. Это JSON-массив, например `["postgres://.../votes_a?sslmode=verify-full","postgres://.../votes_b?sslmode=verify-full"]`; credentials не выводите в логи. По умолчанию массив отсутствует и голосование использует `DATABASE_URL`. Изменение порядка/числа шардов без миграции нарушает дедупликацию и поиск receipts.

Предоставьте PostgreSQL TLS, HA и проверяемые backups/PITR. Отзовите ненужные права `PUBLIC`; выдайте конкретным runtime-ролям только необходимые table/schema privileges. Начальная миграция создаёт таблицы, но не является системой управления корпоративными SQL-ролями. Для миграции из внешнего pipeline отключите Helm hook (`migration.enabled=false`).

Kafka topic `votes.v1` создаётся заранее: partitions по capacity-модели, RF=3+, min.insync.replicas=2+, unclean election disabled, TLS/SASL ACL. Producer имеет write/describe, consumer — read/describe и доступ только к своей group `voting-projector-v1`. Runtime не создаёт topics. SASL PLAIN допускается только с TLS. Retention покрывает максимальный сбой, replay и drain; committed offsets не защищают сообщения от удаления retention-политикой.

Установите Metrics Server для HPA; для ServiceMonitor/PrometheusRule — CRD Prometheus Operator, для KEDA — controller/CRD и TriggerAuthentication с TLS/SASL. Ограничьте админские `/admin` и `/api/admin/*` private ingress/OIDC gateway с MFA. Встроенный bearer token даёт единственную административную роль; отдельных ролей автора опросов и аналитика в приложении нет. Анонимному посетителю доступны только публичные сценарии и собственный receipt. Gateway должен защищать API, а не только UI.

Ingress должен завершать TLS, cookie в production — Secure. NetworkPolicy должна разрешать реальные адреса всех Kafka brokers из metadata, всех SQL-шардов и DNS, включая NodeLocal DNS при его использовании. Учитывайте NAT конкретного CNI. Для migration hook также необходима namespace baseline policy. Служебный порт метрик не публикуйте наружу.

## Настройки и ресурсный бюджет

Все значения читаются и проверяются в `cmd/config`; невалидная конфигурация останавливает старт. Основные defaults:

- SQL: `DB_MAX_CONNECTIONS=20`, `DB_MIN_CONNECTIONS=2`, lifetime 30 мин, idle 5 мин, statement timeout 5 с, lock timeout 1 с; `DB_READ_CONCURRENCY=8`, `COUNTER_SHARDS=256`.
- Кеши: `CACHE_CAPACITY=10000`, `CACHE_SHARDS=64`, `CACHE_MAX_LOADS=32`, `CACHE_LOAD_TIMEOUT=2s`; poll TTL 5 мин, negative TTL 2 с, results TTL 1 с, positive receipt TTL 2 мин. Capacity и load limit применяются отдельно к metadata cache и API read cache.
- Kafka: producer batch 1000/5 мс, consumer batch 500/25 мс; fetch max 10 000 000 байт. `ENQUEUE_TIMEOUT=6s`, `PROJECTION_TIMEOUT=10s`, retry backoff 250 мс. Consumer commits синхронны после всего успешно применённого пакета.
- HTTP: `VOTE_MAX_IN_FLIGHT=1024`, body limit 8192 байт, header limit 16384 байта; request timeout 10 с, write timeout 12 с; `QUERY_TIMEOUT=2s`. Админский список: 100 записей по умолчанию, максимум 1000, пагинация limit/offset.
- Lifecycle: startup timeout 15 с, health interval 3 с, health timeout 2 с, shutdown timeout 20 с. `WARM_POLL_IDS` — comma-separated список активных опросов для загрузки до запуска обслуживания.

Connection budget для конкретной БД — сумма `DB_MAX_CONNECTIONS` всех процессов, открывающих к ней pool, плюс миграции и операционный запас. У процесса до одного пула на каждый уникальный DSN; совпадающие primary/vote DSN переиспользуются. Добавление физических шардов увеличивает число пулов и минимальных соединений, добавление replicas увеличивает их снова. Учитывайте лимиты PostgreSQL/PgBouncer до увеличения HPA/KEDA.

Логические counter buckets не добавляют БД. Число/порядок физических шардов и `COUNTER_SHARDS` фиксируются для записанных данных; до изменения нужен согласованный migration/routing plan. Начальная миграция идемпотентна и защищена advisory lock, но не заменяет полноценные последующие versioned expand/contract migrations. Helm rollback не откатывает схему БД.

## Развёртывание

Выполните `helm upgrade --install ... --atomic --wait` с образом, профилями Secrets и параметрами окружения. Startup проверяет соединения со всеми настроенными БД и прогревает указанные опросы. Периодическая readiness API зависит от metadata DB и Kafka; временная остановка проекции может накапливать durable backlog без остановки приёма. Readiness не доказывает доступность каждого SQL-шарда: это контролируется exporter/алертами и запросами чтения/проекции. Недоступный обязательный warm poll блокирует старт.

По SIGTERM API снимает readiness и завершает HTTP; worker прекращает работу, неподтверждённые offsets будут перечитаны. Grace period Kubernetes должен превышать shutdown budget; chart использует 30 с. API масштабируется CPU HPA, worker — Kafka lag через KEDA; число активных consumers ограничено partitions. PDB и распределение по зонам защищают только в рамках настроек кластера: `ScheduleAnyway` — мягкое ограничение, PDB — только добровольные disruptions. При уменьшении dev replicas согласуйте PDB.

До эфира заранее увеличьте minReplicas/число нод, прогрейте `WARM_POLL_IDS`, проверьте TLS, DNS, топологию шардов, ISR, лимиты соединений и запас retention. Autoscaler после начала минутного пика не заменяет предварительно выделенный capacity. Один Helm release не является подтверждением 100 млн RPS; расчёты и границы описаны в [архитектуре](architecture.md).

## Наблюдаемость и runbook

Приложение публикует `voting_http_requests_total`, `voting_enqueue_total`, `voting_enqueue_duration_seconds`, `voting_cache_access_total` (hit/miss/negative_hit/overloaded), `voting_worker_events_total` и `voting_worker_last_processed_timestamp_seconds`, а также Go/process metrics. Labels не содержат poll ID, voter key или cookie. При частичном SQL commit/retry метрики новых вставок могут недооценить прежний успешный commit; итоговые SQL-агрегаты авторитетны. Подключите Kafka exporter для lag/oldest age/ISR/disk, PostgreSQL exporter для connections/WAL/locks/replication lag и ingress/node metrics. Эти exporters не поставляются внутри приложения.

- **503/overload при vote:** проверить readiness, in-flight/load limits, cold cache, Kafka produce latency и ISR. Не заменять durable ack ранним 202; повтор с той же cookie безопасен. Metadata cache уменьшает SQL только после загрузки.
- **SQL-нагрузка от receipts:** pending не кешируется, положительный кеш локален и ограничен. Ограничить polling, добавить backoff/jitter; для целевого глобального пика развернуть отдельную распределённую read model статусов.
- **Результаты отстают:** проверить lag, rate проекции, retries/locks/WAL, partitions и connection budget. Закрытие опроса не означает финальность результатов; дождаться drain. Результаты нескольких БД — eventual sum согласованных локальных снимков.
- **Ошибка одной БД:** worker повторяет весь пакет; уже записанные шарды не пересчитывают дубликаты. Offsets не подтверждаются до общего успеха. Восстановить шард, не менять routing list ради обхода сбоя.
- **Poison event:** worker завершается без commit. Исследовать событие в защищённом контуре, исправить причину и replay. Ручной skip требует отдельной аудируемой процедуры и учёта потерянного голоса; автоматического DLQ/skip нет.
- **Backlog близок к retention:** увеличить доступный storage/retention и drain capacity до удаления сообщений. Оценивать и объём, и возраст; `last_processed` сам по себе не определяет backlog.
- **Восстановление БД:** восстановить согласованные голоса, counters и progress, затем replay. Для полного rebuild нужны все связанные state tables и согласованный offset/group plan; нельзя обнулить только часть проекции.
- **Ротация секрета:** admin token заменить с rollout. Signing key нельзя просто менять во время активного опроса: это изменит идентичности/дедупликацию; нужен keyring migration или завершение активных опросов. Runtime-роли и Kafka ACL ротируются отдельно.

## Локальная разработка

Для стандартного стенда используйте команды [README](../README.md). Для запуска Go вне контейнера после поднятия PostgreSQL/Kafka укажите `DATABASE_URL` на `localhost:15432`, `KAFKA_BROKERS=localhost:19092`, загрузите локальные секреты и выполните последовательно миграцию, затем в отдельных терминалах worker/API:

```sh
MODE=migrate go run ./cmd/voting
MODE=worker METRICS_ADDR=:9092 go run ./cmd/voting
MODE=api COOKIE_SECURE=false go run ./cmd/voting
```

Сначала соберите frontend (`cd web && npm ci && npm run build`), чтобы API обслуживал `web/dist`, либо запустите frontend dev server с proxy `/api`. `COOKIE_SECURE=false` допустим только для локального HTTP. Порты Compose публикуются на localhost; для телефона понадобится отдельно настроенный доступный dev адрес. Для тестового проверяются функциональность, конкурентность, replay/rollback и ограниченность ресурсов; реальные дорогостоящие испытания на 100 млн RPS не требуются.
