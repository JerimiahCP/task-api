-- Task Management API — initial schema
-- This file is provided as a reference. The application creates tables
-- automatically on startup via app.py init_db().

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

CREATE INDEX idx_tasks_status   ON tasks (status);
CREATE INDEX idx_tasks_assignee ON tasks (assignee);
CREATE INDEX idx_tasks_priority ON tasks (priority);

CREATE TABLE IF NOT EXISTS webhooks (
    id     SERIAL PRIMARY KEY,
    url    VARCHAR(512) NOT NULL,
    events VARCHAR(255) NOT NULL DEFAULT 'task.created,task.updated,task.deleted'
);
