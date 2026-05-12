"""
CARE Database Layer — SQLite
============================
Production-grade schema with all PRD requirements
"""

import sqlite3
import json
import os
from datetime import datetime, timezone
from contextlib import contextmanager

DB_PATH = os.path.join(os.path.dirname(__file__), "care.db")


@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    """Create all tables with complete schema"""
    with get_conn() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS organisations (
            id          TEXT PRIMARY KEY,
            name        TEXT NOT NULL,
            slug        TEXT UNIQUE NOT NULL,
            created_at  TEXT DEFAULT (datetime('now'))
        );

        -- keep all your existing CREATE TABLE code here

        INSERT OR IGNORE INTO organisations (id, name, slug)
        VALUES ('org_default', 'Company Finance', 'company-finance');

        INSERT OR IGNORE INTO users (id, org_id, email, password_hash, role, name)
        VALUES (
            'user_admin',
            'org_default',
            'admin@care.ai',
            'temp',
            'super_admin',
            'QA Manager'
        );
        """)

        admin_hash = bcrypt.hashpw(
            "care@2025".encode("utf-8"),
            bcrypt.gensalt()
        ).decode("utf-8")

        conn.execute(
            "UPDATE users SET password_hash=? WHERE email=?",
            (admin_hash, "admin@care.ai")
        )
    
    # Migrate existing database if needed
    try:
        with get_conn() as conn:
            # Check if confidence_pct column exists
            cursor = conn.execute("PRAGMA table_info(calls)")
            columns = [row[1] for row in cursor.fetchall()]
            
            if 'confidence_pct' not in columns:
                print("[DB] Migrating: Adding confidence_pct column...")
                conn.execute("ALTER TABLE calls ADD COLUMN confidence_pct INTEGER DEFAULT 0")
            
            if 'customer_id' not in columns:
                print("[DB] Migrating: Adding customer_id column...")
                conn.execute("ALTER TABLE calls ADD COLUMN customer_id TEXT")
    except Exception as e:
        print(f"[DB] Migration check: {e}")
    
    print(f"[DB] ✓ Initialized at {DB_PATH}")


# ── Call CRUD ──────────────────────────────────────────────────────────────────

def save_call(call: dict):
    """Insert or update call record"""
    with get_conn() as conn:
        conn.execute("""
        INSERT OR REPLACE INTO calls
        (id, org_id, filename, file_path, file_size, agent_id, campaign_id,
         loan_id, customer_id, source, source_uri, status, score, score_pct,
         confidence_pct, scores_breakdown, compliance_flags, ptp_detected,
         ptp_amount, ptp_date, ptp_mode, agent_sentiment, sentiment_notes,
         summary, key_issues, strengths, coaching_tip, transcript, error,
         uploaded_at, processed_at)
        VALUES
        (:id, :org_id, :filename, :file_path, :file_size, :agent_id, :campaign_id,
         :loan_id, :customer_id, :source, :source_uri, :status, :score, :score_pct,
         :confidence_pct, :scores_breakdown, :compliance_flags, :ptp_detected,
         :ptp_amount, :ptp_date, :ptp_mode, :agent_sentiment, :sentiment_notes,
         :summary, :key_issues, :strengths, :coaching_tip, :transcript, :error,
         :uploaded_at, :processed_at)
        """, {
            "id": call.get("id"),
            "org_id": call.get("org_id", "org_default"),
            "filename": call.get("filename"),
            "file_path": call.get("file_path"),
            "file_size": call.get("file_size"),
            "agent_id": call.get("agent_id"),
            "campaign_id": call.get("campaign_id"),
            "loan_id": call.get("loan_id"),
            "customer_id": call.get("customer_id"),
            "source": call.get("source", "upload"),
            "source_uri": call.get("source_uri"),
            "status": call.get("status", "queued"),
            "score": call.get("score"),
            "score_pct": call.get("score_pct"),
            "confidence_pct": call.get("confidence_pct"),
            "scores_breakdown": json.dumps(call.get("scores_breakdown") or {}),
            "compliance_flags": json.dumps(call.get("compliance_flags") or []),
            "ptp_detected": 1 if call.get("ptp_detected") else 0,
            "ptp_amount": call.get("ptp_amount"),
            "ptp_date": call.get("ptp_date"),
            "ptp_mode": call.get("ptp_mode"),
            "agent_sentiment": call.get("agent_sentiment"),
            "sentiment_notes": call.get("sentiment_notes"),
            "summary": call.get("summary"),
            "key_issues": json.dumps(call.get("key_issues") or []),
            "strengths": json.dumps(call.get("strengths") or []),
            "coaching_tip": call.get("coaching_tip"),
            "transcript": call.get("transcript"),
            "error": call.get("error"),
            "uploaded_at": call.get("uploaded_at", datetime.now(timezone.utc).isoformat()),
            "processed_at": call.get("processed_at"),
        })


def update_call(call_id: str, fields: dict):
    """Update specific fields"""
    if not fields:
        return
    
    # Serialize lists/dicts
    for k in ["scores_breakdown", "compliance_flags", "key_issues", "strengths"]:
        if k in fields and not isinstance(fields[k], str):
            fields[k] = json.dumps(fields[k])
    
    if "ptp_detected" in fields:
        fields["ptp_detected"] = 1 if fields["ptp_detected"] else 0
    
    set_clause = ", ".join(f"{k} = :{k}" for k in fields)
    fields["call_id"] = call_id
    
    with get_conn() as conn:
        conn.execute(f"UPDATE calls SET {set_clause} WHERE id = :call_id", fields)


def get_call(call_id: str, org_id: str = None) -> dict | None:
    with get_conn() as conn:
        if org_id:
            row = conn.execute("SELECT * FROM calls WHERE id=? AND org_id=?", (call_id, org_id)).fetchone()
        else:
            row = conn.execute("SELECT * FROM calls WHERE id=?", (call_id,)).fetchone()
    return _row_to_dict(row) if row else None


def list_calls(org_id: str = "org_default", date_from: str = None,
               date_to: str = None, agent_id: str = None,
               status: str = None, limit: int = 200) -> list:
    query = "SELECT * FROM calls WHERE org_id=?"
    params = [org_id]
    
    if date_from:
        query += " AND uploaded_at >= ?"
        params.append(date_from)
    if date_to:
        query += " AND uploaded_at <= ?"
        params.append(date_to + "T23:59:59")
    if agent_id:
        query += " AND agent_id=?"
        params.append(agent_id)
    if status:
        query += " AND status=?"
        params.append(status)
    
    query += " ORDER BY uploaded_at DESC LIMIT ?"
    params.append(limit)
    
    with get_conn() as conn:
        rows = conn.execute(query, params).fetchall()
    
    return [_row_to_dict(r) for r in rows]


def _row_to_dict(row) -> dict:
    d = dict(row)
    
    # Deserialize JSON fields
    for k in ["scores_breakdown", "compliance_flags", "key_issues", "strengths"]:
        if k in d and isinstance(d[k], str):
            try:
                d[k] = json.loads(d[k])
            except Exception:
                d[k] = []
    
    d["ptp_detected"] = bool(d.get("ptp_detected"))
    return d


# ── User / Auth ────────────────────────────────────────────────────────────────

def get_user_by_email(email: str) -> dict | None:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM users WHERE email=? AND is_active=1", (email,)).fetchone()
    return dict(row) if row else None


def get_user_by_id(user_id: str) -> dict | None:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
    return dict(row) if row else None


def create_user(user_id, org_id, email, password_hash, role, name):
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO users (id,org_id,email,password_hash,role,name) VALUES (?,?,?,?,?,?)",
            (user_id, org_id, email, password_hash, role, name)
        )


# ── Drive Config ───────────────────────────────────────────────────────────────

def get_drive_config(org_id: str) -> dict | None:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM drive_configs WHERE org_id=?", (org_id,)).fetchone()
    return dict(row) if row else None


def save_drive_config(org_id: str, folder_url: str, folder_id: str, auto_sync: bool = False):
    with get_conn() as conn:
        conn.execute("""
        INSERT OR REPLACE INTO drive_configs (id, org_id, folder_url, folder_id, auto_sync)
        VALUES (?, ?, ?, ?, ?)
        """, (f"dc_{org_id}", org_id, folder_url, folder_id, 1 if auto_sync else 0))


def update_drive_last_synced(org_id: str):
    with get_conn() as conn:
        conn.execute(
            "UPDATE drive_configs SET last_synced=? WHERE org_id=?",
            (datetime.now(timezone.utc).isoformat(), org_id)
        )
