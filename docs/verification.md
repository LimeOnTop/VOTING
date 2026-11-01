# Проверка реализации

Локальная проверка после рефакторинга 2 ноября 2026, macOS arm64 / Docker Desktop. Проверки функциональности и конкурентности соответствуют масштабу тестового; реальный прогон 100 млн RPS не является условием сдачи.

## Выполнено после рефакторинга

- Go 1.27.3: `go test -race ./...` с настоящим PostgreSQL через `TEST_DATABASE_URL` — успешно. Покрыты чистая валидация, внедрение зависимостей, дедлайны/отмена, admission limit, кеши/singleflight, пакетная проекция и порядок commit offsets.
- SQL integration: конкурентная дедупликация, replay, несколько шардов, частичный commit и безопасный повтор, rollback голосов/счётчиков при ошибке, deadlines и пагинация. Используются изолированные схемы с очисткой.
- AST architecture tests: запрещённые импорты между слоями и скрытая инициализация во внутренних слоях; проверяются также фикстуры с нарушениями.
- HTTP/security: admin guard/no-store, cross-origin rejection, Secure/HttpOnly/SameSite cookie, строгий и ограниченный JSON, запрет выхода static FS через symlink; подделка cookie и разделение псевдонимов между опросами.
- `go vet ./...` и golangci-lint 2.15.0 — успешно, 0 замечаний. Профиль включает gosec, errorlint, noctx и проверки закрытия SQL/HTTP ресурсов; узкие исключения снабжены объяснениями.
- `govulncheck` 1.9.0 — уязвимостей не найдено на момент проверки.
- Docker multi-stage image на Go 1.27.3 и runtime distroless nonroot: сборка и запуск успешны.
- Compose smoke с настоящими Kafka 3.9.1/PostgreSQL 17: 40 повторов одной cookie и независимый браузер дают ровно 2 участника; запрос без cookie отклоняется.
- Recovery: worker остановлен, API подтверждает durable enqueue; после запуска worker голос учитывается ровно один раз.
- Frontend: `npm ci`, TypeScript/Vite build и `npm audit --audit-level=high` — успешно, 0 vulnerabilities.
- Helm 3.18.6: `lint --strict`, baseline rendering и сочетание ingress + ServiceMonitor/PrometheusRule + KEDA — успешно. API/worker/migration получают разные Secret profiles.
- `git diff --check` — успешно. `.env`, зависимости и артефакты сборки исключены из git.

## Границы проверки

Результат старого локального k6 smoke не переносится на новую реализацию и не используется как доказательство capacity. Не выполнялись реальные испытания 100 млн RPS, multi-region failover, cluster integration Kubernetes/CNI/KEDA, production TLS/SASL, OIDC и восстановление после потери диска. Helm rendering не заменяет кластерную проверку.

CI включает race/integration tests, vet, golangci-lint, govulncheck, frontend/audit, Helm и Compose smoke/recovery. Фактический удалённый статус доступен во вкладке Actions; локальный PASS сам по себе не означает зелёный CI.
