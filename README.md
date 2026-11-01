# VOTING

Анонимные телевизионные опросы: один вопрос, один или несколько вариантов ответа, создание опросов и обезличенные результаты в админке.

**Go · Kafka · PostgreSQL · React / TypeScript · Docker Compose · Kubernetes / Helm**

> Требование для проектирования: **100 млн запросов голосования в секунду в течение минуты** (6 млрд запросов). Репозиторий содержит работающую локальную реализацию и архитектуру её развития. Пропускная способность 100 млн RPS **не измерена и не заявляется достигнутой**. Один PostgreSQL и один Kafka broker из Compose предназначены для разработки.

## Быстрый запуск

Нужны Docker Engine / Docker Desktop с Compose v2; для комфортной работы выделите Docker не менее 4 ГБ памяти. Go и Node на хосте для этого способа не нужны.

```sh
git clone https://github.com/LimeOnTop/VOTING.git
cd VOTING
./scripts/init-env.sh
docker compose up --build -d
curl http://localhost:8080/readyz
```

- Сервис: <http://localhost:8080>.
- Админка: <http://localhost:8080/admin>. Ключ входа — значение `ADMIN_TOKEN` из созданного локального `.env`.
- Создайте вопрос, выберите тип и длительность. Опрос открывается сразу; скопируйте ссылку `/p/<id>` и откройте её в другом браузере или приватном окне для независимого участника.
- Выберите ответ. Сначала появится подтверждение записи в очередь, затем «Ваш голос учтён». Повторное открытие страницы показывает уже учтённый ответ.
- Результаты обновляются в админке каждые 3 секунды. Для multiple choice проценты считаются от числа участников; сумма может превышать 100%.
- Завершённый опрос перестаёт принимать новые запросы; очередь продолжает обрабатывать ранее принятые голоса.

Остановка с сохранением данных: `docker compose down`. **Удаление локальных тестовых данных:** `docker compose down -v`.

Локальная конфигурация использует HTTP, поэтому `COOKIE_SECURE=false`. В Kubernetes по умолчанию `true`, нужен HTTPS. Секреты из `.env` не входят в git и не встраиваются в frontend bundle.

## Как это работает

```mermaid
flowchart LR
    Browser[Браузер / React] --> API[Go API]
    API -->|публикация, acks=all| Kafka[Kafka votes.v1]
    API -->|конфигурация опросов / кеш| PG[(PostgreSQL)]
    Kafka --> Worker[Go projector]
    Worker -->|одна транзакция: дедуп + счётчики| PG
    Admin[Админка] -->|Bearer ключ| API
    API -->|агрегированные итоги| Admin
```

Публичный API не делает SQL-запись на каждый голос: `202 Accepted` возвращается после подтверждённой Kafka записи. Poll metadata кешируется на 10 секунд. Варианты и окно опубликованного опроса неизменяемы.

Анонимный посетитель получает подписанную HttpOnly cookie. В событие попадает только отдельный для опроса `HMAC(secret, poll_id + browser_id)`. Уникальность `(poll_id, voter_key)` в PostgreSQL обеспечивает один учтённый бюллетень с браузера. Вставка бюллетеня и увеличение счётчиков — одна транзакция. Worker коммитит Kafka offset после её завершения: сбой между SQL commit и offset commit приводит к безопасному повтору. Первый успешно зафиксированный бюллетень побеждает; изменить ответ нельзя.

В PostgreSQL счётчики распределены по 256 логическим бакетам, чтобы уменьшить конкуренцию за одну строку. Это **не физическое шардирование** и не решение для 100 млн RPS. Подробности: [архитектура и расчёты](docs/architecture.md), [эксплуатация Kubernetes](docs/operations.md), [контракт API](docs/api.md), [проверенные сценарии](docs/verification.md).

## Проверки

```sh
# Go 1.25+, Node.js 22+, Helm 3.18+
go test -race ./...
go vet ./...
cd web && npm ci && npm run build && cd ..
helm lint deploy/helm/voting --strict --set image.tag=check

# Интеграционный тест с локальным PostgreSQL из Compose
TEST_DATABASE_URL='postgres://voting:local-dev-only@localhost:15432/voting?sslmode=disable' \
  go test -race ./internal/store -count=1

# Black-box: 40 конкурирующих повторов + независимый участник = ровно 2 голоса
set -a; . ./.env; set +a
python3 scripts/smoke.py
```

SQL-тест работает с `TEST_DATABASE_URL`, создаёт отдельный тестовый опрос и временное ограничение; запускайте его только на тестовой БД. Без переменной интеграционный тест явно пропускается. CI запускает его на отдельном PostgreSQL.

Проверка накопления очереди: остановите `worker`, отправьте голос, убедитесь в `202` и отсутствии прироста результатов; запустите `worker` и дождитесь учёта. Скрипт `scripts/recovery.py` автоматизирует этот сценарий на локальном Compose.

Нагрузка через k6 (опрос должен быть открыт):

```sh
k6 run -e POLL_ID=<id> -e RATE=100 -e DURATION=30s scripts/load.js
```

Этот сценарий измеряет ingress с новой cookie на каждой итерации (стандартное поведение k6, явно зафиксированное в скрипте). Он не ждёт учёта голоса, поэтому это не измерение скорости подсчёта уникальных участников. Для полной оценки отдельно нужны уникальные браузеры, consumer lag, SQL throughput, время опустошения очереди и тесты отказов. 100 млн RPS нельзя экстраполировать из результата ноутбука.

## Kubernetes / Helm

Chart: `deploy/helm/voting`. Внешние PostgreSQL и Kafka обязательны; chart намеренно не устанавливает single-node базы в production.

```sh
# Предварительно: namespace, Secret, внешние Kafka/PostgreSQL и topic votes.v1.
# Значения секретов передавайте через Secret manager / External Secrets.
helm upgrade --install voting deploy/helm/voting \
  --namespace voting --create-namespace \
  --set image.tag=<commit-sha> \
  --set existingSecret=voting-secrets \
  -f /path/to/environment-values.yaml \
  --atomic --wait --timeout 5m
```

Нужны ключи Secret: `DATABASE_URL`, `ADMIN_TOKEN` (≥24 байт), `SIGNING_KEY` (≥32 байт); для SASL — `KAFKA_USER`, `KAFKA_PASSWORD`. В `environment-values.yaml` задаются адреса Kafka, TLS ingress, CIDR зависимостей и ресурсы. При включённом NetworkPolicy исходящий доступ к базам по умолчанию закрыт, пока не заданы `networkPolicy.dependencyCIDRs`. Migration Job использует тот же Secret и выполняется до установки/обновления.

Chart включает отдельные API/worker Deployment, Services, startup/readiness/liveness probes, graceful shutdown, requests/limits, HPA, PDB, topology spread, непривилегированный read-only контейнер, ServiceAccount без токена и NetworkPolicy. Опционально: KEDA (lag Kafka), TLS Ingress, ServiceMonitor и PrometheusRule. Подробные зависимости и ограничения — в [operations.md](docs/operations.md).

## Наблюдаемость и CI

```sh
docker compose --profile observability up -d prometheus
```

Prometheus: <http://localhost:9091>. Метрики API: <http://localhost:9090/metrics>. Счётчики принятых запросов включают повторы и **не равны** числу голосов. Метрики worker различают `counted`, `duplicate`, `retry`. Логи JSON не содержат cookie, IP и варианты ответа.

GitHub Actions: unit/integration tests с race detector, vet, TypeScript build, npm audit, Helm lint/template и Compose smoke. Отдельный ручной workflow `Publish image` публикует amd64/arm64 образ в GHCR с тегом commit SHA, SBOM и provenance. Автоматический production deploy не настроен: инфраструктура и секреты зависят от окружения.

## Границы решения

- Это базовая дедупликация браузера, а не доказательство «один человек — один голос». Очистка cookie, другой браузер и автоматизация обходят её.
- Защита от масштабных ботов и DDoS, WAF/rate limits, глобальное распределение и физические шарды описаны как production work; локально реализованы ограничение размера запроса и числа одновременных публикаций.
- Admin Bearer key — упрощение тестового. Для production нужен OIDC на admin ingress, MFA, RBAC и аудит; сам сервис пока не реализует OIDC.
- Результаты eventual consistent. `updated_at` — время последнего учтённого голоса, не гарантия отсутствия backlog. Автоматического статуса «финальный результат» нет.
- Kafka producer может повторно записать событие при неоднозначной сетевой ошибке; дедупликация обеспечивается в SQL, а не обещанием exactly-once транспорта.
- Страница receipt делает SQL-чтения и polling: на целевом масштабе её надо заменить на распределённое/кешированное хранилище статусов и ограниченное адаптивное опрашивание.
- Single-node Compose не защищает от потери диска. Kubernetes chart сам по себе не создаёт отказоустойчивость Kafka или PostgreSQL.

## Технологический выбор

Go — явная модель конкурентности и компактный runtime. Kafka — журнал, буфер телевизионного пика и воспроизводимая доставка. PostgreSQL — понятные транзакционные гарантии, constraints и удобство локальной проверки. React/TypeScript — небольшой статический клиент без SSR. Helm — повторяемые конфигурации Kubernetes. Добавлять Redis в критический путь только ради `SET NX` не требуется: отдельные Redis-дедуп и SQL/Kafka-запись создают ещё одну границу атомарности.
