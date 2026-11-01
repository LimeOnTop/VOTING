# API v1

Машиночитаемый контракт: [OpenAPI 3.0](../api/openapi.json), импортируется в Swagger UI/Editor.

Все даты — RFC3339 UTC. JSON ≤8 KiB, неизвестные поля отклоняются. Сервер использует свои часы для времени принятия. Mutation из браузера допускается с того же Origin; CORS не включён. Admin routes требуют `Authorization: Bearer <ADMIN_TOKEN>`.

## Публичные маршруты

- `GET /api/session` → 200 `{"ready":true}`, выдаёт/проверяет signed HttpOnly cookie `voting_id`, SameSite=Lax, Path=/, Max-Age=30 дней. `Cache-Control: no-store`. В production Secure=true.
- `GET /api/polls/{id}` → 200 metadata: id, question, type (`single`/`multiple`), options `[{id,label}]`, opens_at, closes_at, created_at. Кеширование 30 секунд. При отсутствии — 404.
- `POST /api/polls/{id}/votes`, body `{"options":["1"]}`, с cookie → 202 `{"status":"accepted","receipt_url":"/api/polls/<id>/receipt"}` после Kafka ack. Повтор на том же поде после ACK получает 409, во время публикации — 425. После потери памяти/смены пода повтор может получить 202, но worker не увеличит итог. Первый учтённый бюллетень побеждает. Нет гарантии возможности сменить ответ.
- `GET /api/polls/{id}/receipt`, с cookie → `{"status":"accepted","options":["1"]}` либо `{"status":"not_submitted_or_forgotten"}`. Читает только память пода, не SQL. accepted подтверждает Kafka ACK, не завершение подсчёта. Потеря памяти/маршрута может скрыть ранее принятый ответ. Отсутствующую cookie сервер отклоняет с 428. Cache-Control no-store. Не раскрывает чужие бюллетени.

400 — некорректный JSON, content type/поля; 403 — чужой Origin; 428 — нет валидной cookie; 422 — недопустимые варианты или закрытое окно; 409 — ответ уже принят; 425 — публикация выполняется, Retry-After=1; 429 — превышен per-pod in-flight limit или ёмкость admission map, Retry-After=1; 503 — ошибка хранилища/неоднозначная публикация, Retry-After=1 при vote. На 503 запись могла сохраниться; повторяйте тот же бюллетень с той же cookie. HTTP body limit в текущем API возвращается как общий 400 JSON error.

## Admin

- `GET /api/admin/polls` → список последних 100 опросов (новые первыми).
- `POST /api/admin/polls` → 201 опубликованный неизменяемый опрос:

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

- `GET /api/admin/polls/{id}/results` → `{"poll":{...},"voters":2,"counts":{"1":1,"2":1},"updated_at":"...","consistency":"eventual"}`. Нулевые варианты включены. До первого голоса updated_at=null. Количество участников и counts согласованы внутри каждого SQL-шарда; между БД snapshot eventual; проценты frontend считает от voters. Это не финализированные итоги.

401 — отсутствует/неверен ключ. Изменение и удаление опубликованных опросов намеренно отсутствуют: нельзя менять семантику уже записанных событий. Публичной выдачи результатов нет.

## Health и metrics

API :8080 `/healthz` (процесс), `/readyz` (фоновая проверка DB/Kafka). Служебный :9090 `/metrics`, `/healthz`, `/readyz` (DB). Worker HTTP :9090 только служебный; Kafka failure приводит к завершению worker и рестарту оркестратором. Kafka topic ACL и внутренний доступ обязательны: consumers доверяют источнику accepted_at после проверки окна.
