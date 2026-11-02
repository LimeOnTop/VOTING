# Настройка и развёртывание

## Окружение

Приложение запускается с `MODE=api`, `worker` или `migrate`. Настройки читаются из окружения и проверяются в `cmd/config/config.go`.

- `DATABASE_URL` — PostgreSQL с метаданными опросов.
- `VOTE_DATABASE_URLS` — JSON-массив адресов БД голосов, например `["postgres://.../votes_a","postgres://.../votes_b"]`. По умолчанию используется `DATABASE_URL`. Все API и worker используют одинаковый порядок шардов.
- `KAFKA_BROKERS`, `KAFKA_TOPIC`, `KAFKA_GROUP` — брокеры, topic и consumer group.
- `KAFKA_TLS=true`; для SASL PLAIN нужны оба `KAFKA_USER` и `KAFKA_PASSWORD`. Credentials без TLS отклоняются.
- Для API: `ADMIN_TOKEN` от 24 байт, `SIGNING_KEY` от 32 байт. `COOKIE_SECURE=false` используется только для локального HTTP.
- `WARM_POLL_IDS` — список опросов через запятую для прогрева до старта HTTP.

Лимиты по умолчанию: `DB_MAX_CONNECTIONS=20` на уникальный DSN/процесс, `DB_MIN_CONNECTIONS=2`, `DB_READ_CONCURRENCY=8`, `COUNTER_SHARDS=256`. Число/порядок физических шардов и число корзин счётчиков фиксируются для записанных данных.

Память API: `DEDUP_CAPACITY=1000000` записей, `DEDUP_RETENTION=1h` после закрытия опроса. Capacity и memory limits выбираются вместе с учётом RSS/GC и участников всех активных опросов. Заполнение карты возвращает 429. `VOTE_MAX_IN_FLIGHT=1024` ограничивает параллельные публикации.

## Запуск без контейнера

Поднимите PostgreSQL и Kafka из Compose, соберите frontend и загрузите `.env`. Для приложения на хосте задайте адреса `localhost`:

```sh
cd web && npm ci && npm run build && cd ..
set -a; . ./.env; set +a
export DATABASE_URL='postgres://voting:local-dev-only@localhost:15432/voting?sslmode=disable'
export KAFKA_BROKERS=localhost:19092
MODE=migrate go run ./cmd/voting
```

Затем в отдельных терминалах с тем же окружением:

```sh
MODE=worker METRICS_ADDR=:9092 go run ./cmd/voting
MODE=api COOKIE_SECURE=false go run ./cmd/voting
```

## Kubernetes / Helm

Окружение предоставляет PostgreSQL, Kafka и ingress controller. Заранее создайте topic `votes.v1`, настройте replication factor, min ISR и retention. Для отказоустойчивой Kafka используйте RF≥3 и min ISR≥2.

Создайте отдельные Secrets:

- `voting-api-secrets`: `DATABASE_URL` с правами создания/чтения опросов; `VOTE_DATABASE_URLS` с чтением агрегатов; `ADMIN_TOKEN`, `SIGNING_KEY`, Kafka producer credentials.
- `voting-worker-secrets`: metadata DB только для чтения; vote DB с записью метаданных, dedup-ключей и агрегатов; Kafka consumer credentials.
- `voting-migration-secrets`: адреса всех БД с DDL-правами. Runtime-роли не должны иметь CREATE/ALTER/DROP.

Producer получает write/describe на topic, consumer — read/describe и доступ к своей group. Административный bearer token даёт доступ ко всей админке; отдельных ролей автора и аналитика нет. При внешнем контроле доступа защищайте и `/admin`, и `/api/v1/admin/*`.

В файле настроек окружения укажите Kafka, TLS ingress, namespace ingress controller, ресурсы и `networkPolicy.dependencyCIDRs`. При включённой NetworkPolicy список CIDR обязателен; без него Helm прекращает рендеринг. Разрешите адреса всех brokers, возвращаемых Kafka metadata, и всех SQL-шардов. Metrics Server нужен для HPA, KEDA/Prometheus Operator — для соответствующих опций chart. KEDA получает TLS из `config.kafkaTLS`; `TriggerAuthentication` передаёт SASL credentials и при необходимости CA, без повторного параметра `tls`. Доступ KEDA controller к Kafka разрешается сетевой политикой его namespace.

```sh
helm upgrade --install voting deploy/helm/voting \
  --namespace voting --create-namespace \
  --set image.tag=<commit-sha> \
  --set apiSecret=voting-api-secrets \
  --set workerSecret=voting-worker-secrets \
  --set migrationSecret=voting-migration-secrets \
  -f /path/to/environment-values.yaml \
  --atomic --wait --timeout 5m
```

Migration Job выполняется до установки/обновления. Перед ним создаётся отдельная hook NetworkPolicy с доступом только к DNS и PostgreSQL; она удаляется после успешного завершения hooks. При ошибке Job и policy остаются для диагностики и пересоздаются при следующей попытке. `migration.startupTimeoutSeconds=90` ограничивает запуск приложения и миграцию, `migration.activeDeadlineSeconds=120` — весь Job. При внешнем запуске миграций установите `migration.enabled=false`. Миграции должны быть совместимы со старым runtime на время rolling update; Helm rollback не откатывает схему БД.

По умолчанию cookie affinity настраивается для HAProxy Kubernetes Ingress Controller: `ingress.className=haproxy`, `ingress.sticky.provider=haproxy`. Chart задаёт `haproxy.org/cookie-persistence` на API Service. У реплик контроллера должен совпадать dynamic-cookie-key. Для существующего ingress-nginx есть профиль `sticky.provider=nginx` с соответствующим className.

Если используется NodeLocal DNS, задайте адреса в `networkPolicy.dnsCIDRs` как `/32` или `/128`; `dnsNamespace` указывает namespace обычного кластерного DNS.

TLS завершается на ingress. Прокси передаёт cookie и не кеширует session/vote/receipt. Прямой доступ к Service обходит cookie affinity. Документация параметра: [HAProxy cookie persistence](https://www.haproxy.com/documentation/kubernetes-ingress/community/configuration-reference/service/).

Перед эфиром увеличьте число подов и нод, прогрейте опросы и проверьте connection budget каждой БД. Число активных consumers ограничено Kafka partitions. `lifecycle.terminationGracePeriodSeconds=120` задаёт grace period, `lifecycle.shutdownTimeoutSeconds=20` — timeout завершения каждого HTTP-сервера. Grace period должен превышать два shutdown timeout и оставлять запас на закрытие Kafka-клиентов.

## Эксплуатация

`/healthz` проверяет процесс, `/readyz` — metadata DB и Kafka. При старте проверяются все настроенные БД. Ошибка отдельного vote-шарда требует отдельного мониторинга: API может продолжать приём, пока worker накапливает backlog.

Метрики доступны на служебном порту 9090: HTTP, enqueue latency/outcomes, cache access, worker outcomes и время последней обработки. Метрики попыток могут включать повторы; итоговые агрегаты читаются из SQL. Kafka lag/ISR/disk и PostgreSQL WAL/locks/connections контролируются внешними exporters. Служебный порт не публикуйте наружу.

- **429:** проверьте in-flight limit и заполнение admission map; увеличивайте capacity вместе с памятью.
- **503 при голосовании:** проверьте metadata DB, Kafka latency и ISR. Повтор с той же cookie безопасен даже при неоднозначном результате публикации.
- **Отставание результатов:** проверьте consumer lag, retries, SQL locks/WAL и connection budget. Retention должен покрывать backlog и время его обработки.
- **Ошибка шарда:** восстановите БД; не меняйте routing list для обхода сбоя. Worker повторит пакет без двойного подсчёта.
- **Невалидное событие:** worker останавливается без commit. Исправьте причину и повторите обработку; автоматического skip/DLQ нет.
- **Восстановление:** восстанавливайте dedup ledger и агрегаты согласованно с Kafka offsets. Удаление только части состояния нарушает подсчёт.
- **Ротация:** смена signing key меняет идентичности участников. Выполняйте её после завершения активных опросов либо через миграцию ключей.
