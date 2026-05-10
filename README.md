# Task Management API

REST API backend for the Task Manager application. Provides CRUD operations for tasks, filtering, aggregate stats, and webhook notifications.

## Running locally

```bash
pip install -r requirements.txt
python app.py
```

Or with gunicorn:

```bash
gunicorn app:app --bind 0.0.0.0:8080
```

## Environment variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DB_HOST` | PostgreSQL host | `localhost` |
| `DB_PORT` | PostgreSQL port | `5432` |
| `DB_NAME` | Database name | `taskdb` |
| `DB_USER` | Database user | `tasks` |
| `DB_PASS` | Database password | `TaskPass123` |
| `PORT` | Server port | `8080` |
| `WEBHOOK_SECRET` | Secret sent with webhook deliveries | `dev-webhook-secret` |

## API endpoints

- `GET /health` — health check
- `GET /api/tasks` — list tasks (query params: `status`, `assignee`, `priority`)
- `GET /api/tasks/:id` — get task
- `POST /api/tasks` — create task
- `PATCH /api/tasks/:id` — update task
- `DELETE /api/tasks/:id` — delete task
- `GET /api/tasks/stats` — aggregate statistics
- `GET /api/webhooks` — list webhooks
- `POST /api/webhooks` — register webhook
- `DELETE /api/webhooks/:id` — remove webhook
