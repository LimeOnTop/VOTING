# Проверка реализации

Проверено локально 2 ноября 2026 (Europe/Moscow), macOS arm64 / Docker Desktop. Это функциональная проверка локальной конфигурации, не сертификация production capacity.

## Выполнено

- `go test -race ./...`: проверки валидации, границ окна `[open, close)`, неизвестных/повторных вариантов, cookie, Origin, admin auth и ошибок Kafka. Успешно.
- `go vet ./...`: успешно.
- PostgreSQL integration (`TEST_DATABASE_URL`, настоящий PostgreSQL 17): 40 конкурентных доставок одного бюллетеня → ровно один участник и по одному счётчику выбранных вариантов. Принудительная ошибка constraint после вставки бюллетеня → полное отсутствие частичной записи и неизменные counters. Успешно.
- Docker multi-stage image: React/TypeScript build и Go binary, runtime distroless nonroot. Сборка и запуск успешно.
- Compose smoke, настоящая Kafka 3.9.1: 40 конкурирующих запросов с одной cookie + независимый браузер → ровно 2 участника, сумма single-choice counters = 2. Запрос без cookie → 428. Успешно.
- Recovery: остановлен worker, API подтвердил запись нового голоса, результат оставался нулевым; после запуска worker появился ровно 1 участник. Успешно.
- Браузер: открытие ссылки опроса, выбор ответа, отправка, состояние ожидания, затем «Ваш голос учтён». Успешно.
- `npm run build`: TypeScript и Vite 7.3.7 успешно. `npm audit`: 0 vulnerabilities на момент проверки.
- Helm 3.18.6 `lint --strict`: успешно; `template` baseline и сочетание ingress + ServiceMonitor/PrometheusRule + KEDA: успешно. Обязательный image.tag передан явно.
- k6 1.3.0 в Docker, `RATE=100 DURATION=10s`: 1001 итерация / 1001 ответа 202, ошибок HTTP 0%, vote p99=8,85 мс. С учётом GET session — 2002 HTTP-запроса. Сценарий с повторами cookie измеряет ingress; не throughput уникального подсчёта и не предельную capacity.
- `.env`, зависимости и build artifacts исключены из git; `git diff --check` успешно.

На macOS linker вывел предупреждение LC_DYSYMTAB при race-сборке HTTP tests; команды завершились успешно. В CI тесты запускаются на Linux.

## Не проверено

100 млн RPS, геораспределённый failover, реальное Kubernetes-развёртывание/политики CNI, KEDA controller, TLS/SASL конкретного managed Kafka, production OIDC и восстановление при потере диска. Helm rendering не заменяет cluster integration. На физическом телефоне не проверялось: safe areas, клавиатура и touch feel требуют устройства.

GitHub workflows включены в репозиторий; их фактический статус следует смотреть на вкладке Actions. Локально пройденные проверки не означают автоматически зелёный удалённый CI.
