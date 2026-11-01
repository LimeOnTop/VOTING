# API v1

Машиночитаемый контракт: [Swagger 2.0](../api/swagger.yml), импортируется в Swagger UI/Editor.

Все даты — RFC3339 UTC. JSON ≤8 KiB, неизвестные поля отклоняются. Сервер использует свои часы для времени принятия. Mutation из браузера допускается с того же Origin; CORS не включён. Admin routes требуют `Authorization: Bearer <ADMIN_TOKEN>`.

## Публичные маршруты

- `GET /api/v1/session` → 200 `{"ready":true}`, выдаёт/проверяет signed HttpOnly cookie `voting_id`, SameSite=Lax, Path=/, Max-Age=30 дней. `Cache-Control: no-store`. В production Secure=true.
- `GET /api/v1/polls/{id}` → 200 metadata: id, question, type (`single`/`multiple`), options `[{id,label}]`, opens_at, closes_at, created_at. Кеширование 30 секунд. При отсутствии — 404.
- `POST /api/v1/polls/{id}/votes`, body `{"options":["1"]}`, с cookie → 202 `{"status":"accepted","receipt_url":"/api/v1/polls/<id>/receipt"}` после подтверждения приёма. Повтор на том же поде после ACK получает 409, во время публикации — 425. После перезапуска сервиса повтор может получить 202; итоговый ответ всё равно учитывается один раз. Первый учтённый ответ сохраняется; изменить его нельзя.
- `GET /api/v1/polls/{id}/receipt`, с cookie → `{"status":"accepted","options":["1"]}` либо `{"status":"not_submitted_or_forgotten"}`. accepted подтверждает приём ответа, не завершение подсчёта. После перезапуска сервиса подтверждение может отсутствовать. Отсутствующую cookie сервер отклоняет с 428. Cache-Control no-store. Не раскрывает чужие бюллетени.

400 — некорректный JSON, content type/поля; 403 — чужой Origin; 428 — нет валидной cookie; 422 — недопустимые варианты или закрытое окно; 409 — ответ уже принят; 425 — публикация выполняется, Retry-After=1; 429 — превышен лимит запросов сервиса, Retry-After=1; 503 — ошибка хранилища/неоднозначная публикация, Retry-After=1 при vote. На 503 запись могла сохраниться; повторяйте тот же бюллетень с той же cookie. HTTP body limit в текущем API возвращается как общий 400 JSON error.

## Admin

- `GET /api/v1/admin/polls` → список последних 100 опросов (новые первыми).
- `POST /api/v1/admin/polls` → 201 опубликованный неизменяемый опрос:

```json
{
  "question": "Как вы добираетесь до работы?",
  "type": "single",
  "options": ["Пешком", "Общественный транспорт", "Автомобиль"],
  "opens_at": "2030-01-01T12:00:00Z",
  "closes_at": "2030-01-01T12:01:00Z"
}
```

Вопрос 1–300 символов, 2–10 уникальных непустых вариантов по 1–120 символов, окно ≤24 часов, закрытие в будущем. UI открывает сразу; API поддерживает запланированное открытие.

- `GET /api/v1/admin/polls/{id}/results` → `{"poll":{...},"voters":2,"counts":{"1":1,"2":1},"updated_at":"...","consistency":"eventual"}`. Нулевые варианты включены. До первого голоса updated_at=null. Данные обновляются асинхронно; проценты frontend считает от voters. Это не финализированные итоги.

401 — отсутствует/неверен ключ. Изменение и удаление опубликованных опросов не поддерживаются. Публичной выдачи результатов нет.

## Health и metrics

API :8080 `/healthz` (процесс), `/readyz` (фоновая проверка DB/Kafka). Служебный :9090 предоставляет `/metrics`, `/healthz`, `/readyz`; у worker это единственный HTTP-порт. Readiness проверяет metadata DB и Kafka. Ошибка чтения или commit Kafka завершает worker; оркестратор перезапускает процесс.
