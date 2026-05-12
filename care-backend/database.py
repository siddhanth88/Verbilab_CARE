"""
CARE Database Layer — PostgreSQL
=================================
- AWS RDS PostgreSQL via psycopg2
- SSL support (verify-full for RDS)
- Full CRUD for calls, users, orgs, drive configs
- Auto-creates tables + default admin on first run
"""

import os
import json
import uuid
from datetime import datetime, timezone
from contextlib import contextmanager

import psycopg2
import psycopg2.extras
import bcrypt

# ── Connection ────────────────────────────────────────────────────────────────
# Set DATABASE_URL in Railway environment variables:
# postgresql://postgres:<password>@<host>:5432/postgres?sslmode=require
DATABASE_URL = os.getenv("DATABASE_URL", "")

# Optional: path to RDS SSL cert (download global-bundle.pem and set this)
SSL_CERT = os.getenv("PG_SSL_CERT", "")  # e.g. "./global-bundle.pem"


def _build_conn_kwargs():
    """Build psycopg2 connection kwargs from DATABASE_URL."""
    if not DATABASE_URL:
        raise EnvironmentError(
            "DATABASE_URL not set. Add it to Railway environment variables.\n"
            "Format: postgresql://postgres:<password>@<host>:5432/postgres?sslmode=require"
        )
    kwargs = {"dsn": DATABASE_URL}
    if SSL_CERT and os.path.exists(SSL_CERT):
        kwargs["sslrootcert"] = SSL_CERT
        kwargs["sslmode"] = "verify-full"
    return kwargs


@contextmanager
def get_conn():
    """Context manager — auto commit/rollback/close."""
    conn = psycopg2.connect(**_build_conn_kwargs())
    conn.autocommit = False
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _fetchone(cur) -> dict | None:
    row = cur.fetchone()
    if row is None:
        return None
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, row))


def _fetchall(cur) -> list[dict]:
    rows = cur.fetchall()
    if not rows:
        return []
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in rows]


# ── Schema Init ───────────────────────────────────────────────────────────────

def init_db():
    """Create tables + seed default admin. Safe to call on every startup."""
    with get_conn() as conn:
        cur = conn.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS organisations (
            id          TEXT PRIMARY KEY,
            name        TEXT NOT NULL,
            slug        TEXT UNIQUE NOT NULL,
            created_at  TIMESTAMPTZ DEFAULT NOW()
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id             TEXT PRIMARY KEY,
            org_id         TEXT,
            email          TEXT UNIQUE NOT NULL,
            password_hash  TEXT NOT NULL,
            role           TEXT NOT NULL,
            name           TEXT NOT NULL,
            is_active      INTEGER DEFAULT 1,
            created_at     TIMESTAMPTZ DEFAULT NOW()
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS calls (
            id                TEXT PRIMARY KEY,
            org_id            TEXT,
            filename          TEXT,
            file_path         TEXT,
            file_size         BIGINT,
            agent_id          TEXT,
            campaign_id       TEXT,
            loan_id           TEXT,
            customer_id       TEXT,
            source            TEXT,
            source_uri        TEXT,
            status            TEXT,
            score             REAL,
            score_pct         REAL,
            confidence_pct    INTEGER DEFAULT 0,
            scores_breakdown  TEXT,
            compliance_flags  TEXT,
            ptp_detected      INTEGER DEFAULT 0,
            ptp_amount        TEXT,
            ptp_date          TEXT,
            ptp_mode          TEXT,
            agent_sentiment   TEXT,
            sentiment_notes   TEXT,
            summary           TEXT,
            key_issues        TEXT,
            strengths         TEXT,
            coaching_tip      TEXT,
            transcript        TEXT,
            error             TEXT,
            uploaded_at       TIMESTAMPTZ,
            processed_at      TIMESTAMPTZ
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS drive_configs (
            id          TEXT PRIMARY KEY,
            org_id      TEXT,
            folder_url  TEXT,
            folder_id   TEXT,
            auto_sync   INTEGER DEFAULT 0,
            last_synced TIMESTAMPTZ
        );
        """)

        # Seed default org
        cur.execute("""
        INSERT INTO organisations (id, name, slug)
        VALUES (%s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
        """, ("org_default", "Company Finance", "company-finance"))

        # Seed admin user (upsert password every deploy so it's always fresh)
        admin_hash = bcrypt.hashpw(
            "care@2025".encode("utf-8"),
            bcrypt.gensalt()
        ).decode("utf-8")

        cur.execute("""
        INSERT INTO users (id, org_id, email, password_hash, role, name)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (email) DO UPDATE SET password_hash = EXCLUDED.password_hash;
        """, ("user_admin", "org_default", "admin@care.ai", admin_hash, "super_admin", "QA Manager"))

    print(f"[DB] ✓ PostgreSQL initialised — {DATABASE_URL.split('@')[-1].split('/')[0]}")


# ── Call CRUD ─────────────────────────────────────────────────────────────────

def save_call(call: dict):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO calls (
            id, org_id, filename, file_path, file_size,
            agent_id, campaign_id, loan_id, customer_id,
            source, source_uri, status,
            score, score_pct, confidence_pct,
            scores_breakdown, compliance_flags,
            ptp_detected, ptp_amount, ptp_date, ptp_mode,
            agent_sentiment, sentiment_notes,
            summary, key_issues, strengths, coaching_tip,
            transcript, error, uploaded_at, processed_at
        ) VALUES (
            %s,%s,%s,%s,%s,
            %s,%s,%s,%s,
            %s,%s,%s,
            %s,%s,%s,
            %s,%s,
            %s,%s,%s,%s,
            %s,%s,
            %s,%s,%s,%s,
            %s,%s,%s,%s
        )
        ON CONFLICT (id) DO UPDATE SET
            status          = EXCLUDED.status,
            score           = EXCLUDED.score,
            score_pct       = EXCLUDED.score_pct,
            scores_breakdown= EXCLUDED.scores_breakdown,
            compliance_flags= EXCLUDED.compliance_flags,
            ptp_detected    = EXCLUDED.ptp_detected,
            ptp_amount      = EXCLUDED.ptp_amount,
            ptp_date        = EXCLUDED.ptp_date,
            ptp_mode        = EXCLUDED.ptp_mode,
            agent_sentiment = EXCLUDED.agent_sentiment,
            sentiment_notes = EXCLUDED.sentiment_notes,
            summary         = EXCLUDED.summary,
            key_issues      = EXCLUDED.key_issues,
            strengths       = EXCLUDED.strengths,
            coaching_tip    = EXCLUDED.coaching_tip,
            transcript      = EXCLUDED.transcript,
            error           = EXCLUDED.error,
            processed_at    = EXCLUDED.processed_at;
        """, (
            call.get("id"),
            call.get("org_id", "org_default"),
            call.get("filename"),
            call.get("file_path"),
            call.get("file_size"),
            call.get("agent_id"),
            call.get("campaign_id"),
            call.get("loan_id"),
            call.get("customer_id"),
            call.get("source", "upload"),
            call.get("source_uri"),
            call.get("status", "queued"),
            call.get("score"),
            call.get("score_pct"),
            call.get("confidence_pct", 0),
            json.dumps(call.get("scores_breakdown") or {}),
            json.dumps(call.get("compliance_flags") or []),
            1 if call.get("ptp_detected") else 0,
            call.get("ptp_amount"),
            call.get("ptp_date"),
            call.get("ptp_mode"),
            call.get("agent_sentiment"),
            call.get("sentiment_notes"),
            call.get("summary"),
            json.dumps(call.get("key_issues") or []),
            json.dumps(call.get("strengths") or []),
            call.get("coaching_tip"),
            call.get("transcript"),
            call.get("error"),
            call.get("uploaded_at", datetime.now(timezone.utc).isoformat()),
            call.get("processed_at"),
        ))


def update_call(call_id: str, fields: dict):
    """Patch specific fields on a call record."""
    if not fields:
        return

    # Serialize any dict/list fields to JSON strings
    for key in ("scores_breakdown", "compliance_flags", "key_issues", "strengths"):
        if key in fields and not isinstance(fields[key], str):
            fields[key] = json.dumps(fields[key])

    set_clause = ", ".join(f"{k} = %s" for k in fields)
    values = list(fields.values()) + [call_id]

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"UPDATE calls SET {set_clause} WHERE id = %s",
            values
        )


def get_call(call_id: str) -> dict | None:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM calls WHERE id = %s", (call_id,))
        row = _fetchone(cur)
    if row:
        _deserialize_call(row)
    return row


def list_calls(
    org_id: str = None,
    date_from: str = None,
    date_to: str = None,
    agent_id: str = None,
    status: str = None,
    limit: int = 200,
) -> list[dict]:
    filters = []
    params = []

    if org_id:
        filters.append("org_id = %s")
        params.append(org_id)
    if date_from:
        filters.append("uploaded_at >= %s")
        params.append(date_from)
    if date_to:
        filters.append("uploaded_at <= %s")
        params.append(date_to)
    if agent_id:
        filters.append("agent_id = %s")
        params.append(agent_id)
    if status:
        filters.append("status = %s")
        params.append(status)

    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    params.append(limit)

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"SELECT * FROM calls {where} ORDER BY uploaded_at DESC LIMIT %s",
            params
        )
        rows = _fetchall(cur)

    for r in rows:
        _deserialize_call(r)
    return rows


def _deserialize_call(row: dict):
    """Parse JSON string fields back to Python objects."""
    for key in ("scores_breakdown", "compliance_flags", "key_issues", "strengths"):
        val = row.get(key)
        if isinstance(val, str):
            try:
                row[key] = json.loads(val)
            except Exception:
                row[key] = {} if key == "scores_breakdown" else []


# ── Auth ──────────────────────────────────────────────────────────────────────

def get_user_by_email(email: str) -> dict | None:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM users WHERE email = %s AND is_active = 1",
            (email,)
        )
        return _fetchone(cur)


def get_user_by_id(user_id: str) -> dict | None:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM users WHERE id = %s", (user_id,))
        return _fetchone(cur)


def create_user(user_id, org_id, email, password_hash, role, name):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO users (id, org_id, email, password_hash, role, name)
        VALUES (%s, %s, %s, %s, %s, %s)
        """, (user_id, org_id, email, password_hash, role, name))


# ── Drive Config ──────────────────────────────────────────────────────────────

def get_drive_config(org_id: str) -> dict | None:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM drive_configs WHERE org_id = %s", (org_id,))
        return _fetchone(cur)


def save_drive_config(org_id: str, folder_url: str, folder_id: str, auto_sync: bool = False):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO drive_configs (id, org_id, folder_url, folder_id, auto_sync)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            folder_url = EXCLUDED.folder_url,
            folder_id  = EXCLUDED.folder_id,
            auto_sync  = EXCLUDED.auto_sync;
        """, (f"dc_{org_id}", org_id, folder_url, folder_id, 1 if auto_sync else 0))


def update_drive_last_synced(org_id: str):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE drive_configs SET last_synced = NOW() WHERE org_id = %s",
            (org_id,)
        )
