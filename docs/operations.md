# Эксплуатация

## Provisioning до Helm

1. Отдельный namespace и external Secret `voting-secrets`: `DATABASE_URL` (TLS к БД), `ADMIN_TOKEN`, `SIGNING_KEY`, опционально Kafka credentials. Secret создаётся до migration hook. В production используйте External Secrets/Vault/облачный Secret Manager, а не секреты в values.
2. PostgreSQL HA с backups/PITR; роли с минимальными правами. Migration identity требует DDL; для строгого разделения ролей запускайте migration отдельным внешним pipeline (`migration.enabled=false`) и используйте DML-only credentials у runtime.
3. Kafka topic `votes.v1`, partitions по измерениям, RF=3+, min.insync.replicas=2+, unclean election disabled, TLS/SASL ACL. Runtime producer — write/describe, worker — read и consumer group; создание topic ему не разрешается.
4. Ingress controller, TLS certificate Secret (например cert-manager), Metrics Server для HPA, CNI с NetworkPolicy. Для ServiceMonitor/PrometheusRule нужны CRD Prometheus Operator. Для KEDA — CRD/controller и TriggerAuthentication с TLS/SASL параметрами.
5. Настройте админский OIDC gateway/MFA и private ingress на `/admin` **и** `/api/admin/*`. Одного ограничения UI недостаточно. Пример chart использует общий ingress; это не готовый корпоративный auth gateway.
6. Соберите и опубликуйте образ через workflow `Publish image` либо свой registry. `image.tag` обязателен. По умолчанию образ в GHCR не обещается существующим до запуска workflow.

Пример environment values (замените адреса/CIDR своими):

```yaml
config:
  kafkaBrokers: broker-1.internal:9093,broker-2.internal:9093,broker-3.internal:9093
  kafkaTLS: "true"
  cookieSecure: "true"
ingress:
  enabled: true
  className: nginx
  host: vote.example.org
  tlsSecret: voting-tls
networkPolicy:
  dependencyCIDRs: ["10.20.0.0/16"]
  kafkaPort: 9093
  databasePort: 5432
serviceMonitor:
  enabled: true
  labels:
    release: kube-prometheus-stack
worker:
  keda:
    enabled: true
    maxReplicas: 12
    authenticationRef: voting-kafka-auth
```

NetworkPolicy разрешает DNS к kube-system; при NodeLocal DNS необходимо добавить его адрес в политику. CIDR должны покрывать реальные адреса всех brokers из Kafka metadata и БД, а не только bootstrap endpoint. Некоторые CNI иначе обрабатывают Service NAT: проверьте политику в целевом кластере. Migration hook имеет отдельную pod label set и не выбирается app policy — к нему должны применяться namespace baseline policies. Это явно учитывается при настройке окружения.

## Развёртывание и откат

`helm upgrade --install ... --atomic --wait`. API запускается неготовым, каждые 3 сек проверяет БД/Kafka, graceful shutdown прекращает readiness и завершает HTTP в пределах 20 сек. Worker прекращает fetch по SIGTERM; незакоммиченный offset будет перечитан. `terminationGracePeriodSeconds=30`.

Migration — idempotent initial schema под advisory lock; это не полноценный versioned migration engine. Следующие версии схемы должны быть пронумерованными expand/contract migrations. Helm rollback не откатывает схему БД. Внешние Kafka/PostgreSQL lifecycle отделён от приложения.

API HPA — CPU baseline, не доказательство достаточного capacity. Worker KEDA — lag, replicas ≤ partitions. Масштабирование ограничивается connection budget: каждый процесс имеет pool до 20 SQL connections; увеличивая replicas, учитывайте лимиты БД/PgBouncer. В chart PDB `minAvailable=2`, min replicas 3; если уменьшаете replicas в dev, меняйте/выключайте PDB. PDB защищает только от добровольных disruptions. Zone spread `ScheduleAnyway` — мягкое распределение, не hard HA guarantee.

До эфира заранее увеличьте minReplicas/число нод и проверьте соединения, DNS, topic ownership, TLS и downstream. CPU HPA и cluster autoscaler не заменяют предварительный прогрев.

## Метрики и алерты

Реализованы `voting_http_requests_total{route,status}`, `voting_accepted_total`, `voting_enqueue_duration_seconds`, `voting_worker_events_total{outcome}`, `voting_worker_last_processed_timestamp_seconds` и Go/process metrics. Лейблы маршрутов не включают poll_id/voter_key. Worker `retry` показывает SQL retries. Chart содержит примеры alerts на 503 при публикации и SQL retries.

Дополнительно в production подключаются Kafka exporter / broker metrics (lag, oldest unprocessed age, ISR, disk, produce latency), PostgreSQL exporter (connections, WAL, replication lag, lock wait), ingress и node metrics. Эти exporters не входят в приложение. Нужны алерты на unavailable Deployment, отсутствие scrape targets и poison event crashloop. Одного `last_processed` недостаточно для определения backlog при отсутствии трафика. Метрики служебного порта не открывайте публично.

## Runbook

- **503 при vote:** смотреть readiness, broker reachability/ISR и SQL metadata. Не возвращать 202 до durable ack. Повторы с той же cookie безопасны.
- **Результаты отстают:** сравнить accepted/count rate и consumer lag. Проверить SQL retries/locks, replicas/partitions и disk. После закрытия дождаться drain; `closes_at` не означает финальный итог.
- **Poison event:** worker выходит с ошибкой, offset не продвигается. Сохранить raw event в защищённое расследование; исправить причину и replay. Ручной skip допускается только по процедуре с аудитом и учётом потерянного голоса.
- **Истекает retention:** расширить capacity/retention до удаления backlog, следить за диском. Retention не управляет Kafka по committed offsets.
- **Восстановление БД:** вернуть совместимую schema/data и replay журнала. При частичном сбросе таблиц нельзя сохранять старые counters или offsets без согласованного плана. Изолированный rebuild projection требует очистки всех связанных state tables и отдельной consumer group.
- **Секрет скомпрометирован:** admin token заменить и rollout. Signing key требует keyring rollout либо закрытия текущих polls: простая замена сбрасывает дедупликацию.

## Локальная разработка без сборки образа

```sh
./scripts/init-env.sh
docker compose up -d postgres kafka topics
export DATABASE_URL='postgres://voting:local-dev-only@localhost:15432/voting?sslmode=disable'
set -a; . ./.env; set +a
MODE=migrate go run ./cmd/voting
MODE=worker METRICS_ADDR=:9092 go run ./cmd/voting  # отдельный терминал
MODE=api go run ./cmd/voting                     # отдельный терминал
cd web && npm ci && npm run dev                 # frontend dev :5173, proxy /api
```

Для одного backend URL сначала `cd web && npm ci && npm run build`, затем API обслужит `web/dist` на :8080. `KAFKA_BROKERS` по умолчанию localhost:19092. На реальном телефоне нужны доступный LAN адрес и соответствующая dev bind-настройка Docker; localhost-порты Compose намеренно не публикуются в LAN.
