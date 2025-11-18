import os
import psycopg2


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS jobs (
    id VARCHAR(32) PRIMARY KEY,
    user_id UUID NOT NULL,
    upload_id UUID NOT NULL REFERENCES uploads(id),
    status VARCHAR(32) NOT NULL DEFAULT 'pending',
    progress INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    error_message TEXT,
    input_path TEXT NOT NULL,
    result_path TEXT
);
"""

CREATE_INDEX_STATUS_CREATED_SQL = (
    "CREATE INDEX IF NOT EXISTS idx_jobs_status_created ON jobs (status, created_at);"
)
CREATE_INDEX_UPLOAD_SQL = "CREATE INDEX IF NOT EXISTS idx_jobs_upload ON jobs (upload_id);"
CREATE_INDEX_USER_SQL = "CREATE INDEX IF NOT EXISTS idx_jobs_user ON jobs (user_id);"


def get_connection():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL environment variable not set")
    return psycopg2.connect(database_url)


def ensure_jobs_table():
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(CREATE_TABLE_SQL)
                cur.execute(CREATE_INDEX_STATUS_CREATED_SQL)
                cur.execute(CREATE_INDEX_UPLOAD_SQL)
                cur.execute(CREATE_INDEX_USER_SQL)
        print("jobs table ensured.")
    finally:
        conn.close()


if __name__ == "__main__":
    ensure_jobs_table()
