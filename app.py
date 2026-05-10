"""
Task Management API — a Jira-lite REST microservice.

Provides CRUD for tasks with filtering, stats, and webhook notifications.
Backed by PostgreSQL via psycopg2.
"""

import os
import json
import logging
import threading
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
import requests
from flask import Flask, request, jsonify

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

app = Flask(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "localhost"),
    "port": int(os.environ.get("DB_PORT", 5432)),
    "dbname": os.environ.get("DB_NAME", "taskdb"),
    "user": os.environ.get("DB_USER", "tasks"),
    "password": os.environ.get("DB_PASS", "TaskPass123"),
}

WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "dev-webhook-secret")

VALID_PRIORITIES = {"low", "medium", "high", "critical"}
VALID_STATUSES = {"todo", "in-progress", "review", "done"}

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------


def get_conn():
    """Return a new database connection."""
    return psycopg2.connect(**DB_CONFIG)


def query(sql, params=None, fetch="all"):
    """Execute a read query and return rows as dicts."""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            if fetch == "one":
                row = cur.fetchone()
                return dict(row) if row else None
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def execute(sql, params=None, returning=False):
    """Execute a write query. Optionally return the affected row."""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            conn.commit()
            if returning:
                row = cur.fetchone()
                return dict(row) if row else None
            return cur.rowcount
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Schema & seed data
# ---------------------------------------------------------------------------

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS tasks (
    id          SERIAL PRIMARY KEY,
    title       VARCHAR(255) NOT NULL,
    description TEXT DEFAULT '',
    assignee    VARCHAR(128) DEFAULT '',
    priority    VARCHAR(16)  NOT NULL DEFAULT 'medium',
    status      VARCHAR(16)  NOT NULL DEFAULT 'todo',
    created_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS webhooks (
    id     SERIAL PRIMARY KEY,
    url    VARCHAR(512) NOT NULL,
    events VARCHAR(255) NOT NULL DEFAULT 'task.created,task.updated,task.deleted'
);
"""

SEED_TASKS = [
    ("Fix auth middleware", "JWT validation fails on token refresh", "alice", "critical", "in-progress"),
    ("Add rate limiting", "Implement sliding-window rate limiter for public endpoints", "bob", "high", "todo"),
    ("Update API docs", "Swagger spec is out of date after v2 migration", "carol", "medium", "todo"),
    ("Database connection pooling", "Replace naive connections with pgBouncer or internal pool", "alice", "high", "review"),
    ("Write integration tests", "Cover task CRUD and webhook delivery", "dave", "medium", "todo"),
    ("Set up CI pipeline", "GitHub Actions for lint, test, build, deploy", "bob", "high", "in-progress"),
    ("Refactor error handling", "Centralise error responses and logging", "carol", "medium", "todo"),
    ("Add pagination to list endpoint", "Support limit/offset query params", "dave", "low", "done"),
    ("Implement soft deletes", "Add deleted_at column instead of hard deletes", "alice", "medium", "todo"),
    ("Optimize stats query", "Stats endpoint is slow on large datasets — add indices", "bob", "low", "review"),
]


def init_db():
    """Create tables and seed sample data if the tasks table is empty."""
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
            conn.commit()

            cur.execute("SELECT COUNT(*) FROM tasks")
            count = cur.fetchone()[0]
            if count == 0:
                log.info("Seeding %d sample tasks", len(SEED_TASKS))
                for title, desc, assignee, priority, status in SEED_TASKS:
                    cur.execute(
                        """INSERT INTO tasks (title, description, assignee, priority, status)
                           VALUES (%s, %s, %s, %s, %s)""",
                        (title, desc, assignee, priority, status),
                    )
                conn.commit()
        conn.close()
        log.info("Database initialised successfully")
    except Exception as exc:
        log.error("Database init failed: %s", exc)


# ---------------------------------------------------------------------------
# Webhook dispatch
# ---------------------------------------------------------------------------


def _fire_webhooks(event: str, payload: dict):
    """Send webhook notifications in a background thread."""

    def _deliver():
        try:
            hooks = query(
                "SELECT id, url, events FROM webhooks"
            )
            for hook in hooks:
                subscribed = {e.strip() for e in hook["events"].split(",")}
                if event in subscribed:
                    body = {
                        "event": event,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "data": payload,
                    }
                    try:
                        requests.post(
                            hook["url"],
                            json=body,
                            headers={
                                "X-Webhook-Secret": WEBHOOK_SECRET,
                                "Content-Type": "application/json",
                            },
                            timeout=5,
                        )
                        log.info("Webhook delivered to %s for %s", hook["url"], event)
                    except requests.RequestException as exc:
                        log.warning("Webhook delivery failed for %s: %s", hook["url"], exc)
        except Exception as exc:
            log.error("Webhook dispatch error: %s", exc)

    threading.Thread(target=_deliver, daemon=True).start()


# ---------------------------------------------------------------------------
# JSON serialisation helper
# ---------------------------------------------------------------------------


def _serialise_task(task: dict) -> dict:
    """Ensure datetime fields are ISO-formatted strings."""
    out = dict(task)
    for key in ("created_at", "updated_at"):
        if isinstance(out.get(key), datetime):
            out[key] = out[key].isoformat()
    return out


# ---------------------------------------------------------------------------
# Routes — Health
# ---------------------------------------------------------------------------


@app.route("/health")
def health():
    """Health check — verifies DB connectivity."""
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        conn.close()
        return jsonify({"status": "healthy", "db": "connected"}), 200
    except Exception as exc:
        return jsonify({"status": "unhealthy", "db": str(exc)}), 503


# ---------------------------------------------------------------------------
# Routes — Tasks CRUD
# ---------------------------------------------------------------------------


@app.route("/api/tasks", methods=["GET"])
def list_tasks():
    """List tasks with optional filtering by status and assignee."""
    conditions = []
    params = []

    status_filter = request.args.get("status")
    if status_filter:
        conditions.append("status = %s")
        params.append(status_filter)

    assignee_filter = request.args.get("assignee")
    if assignee_filter:
        conditions.append("assignee = %s")
        params.append(assignee_filter)

    priority_filter = request.args.get("priority")
    if priority_filter:
        conditions.append("priority = %s")
        params.append(priority_filter)

    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
    sql = f"SELECT * FROM tasks{where} ORDER BY created_at DESC"

    tasks = query(sql, params or None)
    return jsonify([_serialise_task(t) for t in tasks]), 200


@app.route("/api/tasks/stats", methods=["GET"])
def task_stats():
    """Aggregate statistics: counts by status, priority, and assignee."""
    by_status = query(
        "SELECT status, COUNT(*) AS count FROM tasks GROUP BY status ORDER BY status"
    )
    by_priority = query(
        "SELECT priority, COUNT(*) AS count FROM tasks GROUP BY priority ORDER BY priority"
    )
    by_assignee = query(
        "SELECT assignee, COUNT(*) AS count FROM tasks GROUP BY assignee ORDER BY count DESC"
    )
    total = query("SELECT COUNT(*) AS count FROM tasks", fetch="one")

    return jsonify({
        "total": total["count"] if total else 0,
        "by_status": by_status,
        "by_priority": by_priority,
        "by_assignee": by_assignee,
    }), 200


@app.route("/api/tasks/<int:task_id>", methods=["GET"])
def get_task(task_id):
    """Get a single task by ID."""
    task = query("SELECT * FROM tasks WHERE id = %s", (task_id,), fetch="one")
    if not task:
        return jsonify({"error": "Task not found"}), 404
    return jsonify(_serialise_task(task)), 200


@app.route("/api/tasks", methods=["POST"])
def create_task():
    """Create a new task. Required: title. Optional: description, assignee, priority, status."""
    data = request.get_json(silent=True)
    if not data:
        return jsonify({"error": "Request body must be JSON"}), 400

    title = (data.get("title") or "").strip()
    if not title:
        return jsonify({"error": "Title is required"}), 400
    if len(title) > 255:
        return jsonify({"error": "Title must be 255 characters or fewer"}), 400

    priority = data.get("priority", "medium")
    if priority not in VALID_PRIORITIES:
        return jsonify({"error": f"Invalid priority. Must be one of: {', '.join(sorted(VALID_PRIORITIES))}"}), 400

    status = data.get("status", "todo")
    if status not in VALID_STATUSES:
        return jsonify({"error": f"Invalid status. Must be one of: {', '.join(sorted(VALID_STATUSES))}"}), 400

    description = data.get("description", "")
    assignee = data.get("assignee", "")

    task = execute(
        """INSERT INTO tasks (title, description, assignee, priority, status)
           VALUES (%s, %s, %s, %s, %s)
           RETURNING *""",
        (title, description, assignee, priority, status),
        returning=True,
    )

    serialised = _serialise_task(task)
    _fire_webhooks("task.created", serialised)
    return jsonify(serialised), 201


@app.route("/api/tasks/<int:task_id>", methods=["PATCH"])
def update_task(task_id):
    """Partially update a task. Only supplied fields are changed."""
    existing = query("SELECT * FROM tasks WHERE id = %s", (task_id,), fetch="one")
    if not existing:
        return jsonify({"error": "Task not found"}), 404

    data = request.get_json(silent=True)
    if not data:
        return jsonify({"error": "Request body must be JSON"}), 400

    updates = []
    params = []

    if "title" in data:
        title = (data["title"] or "").strip()
        if not title:
            return jsonify({"error": "Title cannot be empty"}), 400
        if len(title) > 255:
            return jsonify({"error": "Title must be 255 characters or fewer"}), 400
        updates.append("title = %s")
        params.append(title)

    if "description" in data:
        updates.append("description = %s")
        params.append(data["description"])

    if "assignee" in data:
        updates.append("assignee = %s")
        params.append(data["assignee"])

    if "priority" in data:
        if data["priority"] not in VALID_PRIORITIES:
            return jsonify({"error": f"Invalid priority. Must be one of: {', '.join(sorted(VALID_PRIORITIES))}"}), 400
        updates.append("priority = %s")
        params.append(data["priority"])

    if "status" in data:
        if data["status"] not in VALID_STATUSES:
            return jsonify({"error": f"Invalid status. Must be one of: {', '.join(sorted(VALID_STATUSES))}"}), 400
        updates.append("status = %s")
        params.append(data["status"])

    if not updates:
        return jsonify({"error": "No valid fields to update"}), 400

    updates.append("updated_at = NOW()")
    params.append(task_id)

    set_clause = ", ".join(updates)
    task = execute(
        f"UPDATE tasks SET {set_clause} WHERE id = %s RETURNING *",
        params,
        returning=True,
    )

    serialised = _serialise_task(task)
    _fire_webhooks("task.updated", serialised)
    return jsonify(serialised), 200


@app.route("/api/tasks/<int:task_id>", methods=["DELETE"])
def delete_task(task_id):
    """Delete a task by ID."""
    task = query("SELECT * FROM tasks WHERE id = %s", (task_id,), fetch="one")
    if not task:
        return jsonify({"error": "Task not found"}), 404

    execute("DELETE FROM tasks WHERE id = %s", (task_id,))
    _fire_webhooks("task.deleted", _serialise_task(task))
    return jsonify({"message": "Task deleted", "id": task_id}), 200


# ---------------------------------------------------------------------------
# Routes — Webhooks
# ---------------------------------------------------------------------------


@app.route("/api/webhooks", methods=["GET"])
def list_webhooks():
    """List all registered webhooks."""
    hooks = query("SELECT * FROM webhooks ORDER BY id")
    return jsonify(hooks), 200


@app.route("/api/webhooks", methods=["POST"])
def create_webhook():
    """Register a new webhook. Required: url. Optional: events (comma-separated)."""
    data = request.get_json(silent=True)
    if not data:
        return jsonify({"error": "Request body must be JSON"}), 400

    url = (data.get("url") or "").strip()
    if not url:
        return jsonify({"error": "URL is required"}), 400
    if not url.startswith(("http://", "https://")):
        return jsonify({"error": "URL must start with http:// or https://"}), 400

    events = data.get("events", "task.created,task.updated,task.deleted")

    hook = execute(
        "INSERT INTO webhooks (url, events) VALUES (%s, %s) RETURNING *",
        (url, events),
        returning=True,
    )

    return jsonify(hook), 201


@app.route("/api/webhooks/<int:hook_id>", methods=["DELETE"])
def delete_webhook(hook_id):
    """Remove a registered webhook."""
    affected = execute("DELETE FROM webhooks WHERE id = %s", (hook_id,))
    if affected == 0:
        return jsonify({"error": "Webhook not found"}), 404
    return jsonify({"message": "Webhook deleted", "id": hook_id}), 200


# ---------------------------------------------------------------------------
# Error handlers
# ---------------------------------------------------------------------------


@app.errorhandler(404)
def not_found(_):
    return jsonify({"error": "Not found"}), 404


@app.errorhandler(405)
def method_not_allowed(_):
    return jsonify({"error": "Method not allowed"}), 405


@app.errorhandler(500)
def internal_error(_):
    return jsonify({"error": "Internal server error"}), 500


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
else:
    # Running under gunicorn — initialise DB on import
    init_db()
