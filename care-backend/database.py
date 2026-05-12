"""
CARE Database Layer — SQLite
============================
Production-grade schema with authentication fix
"""

import sqlite3
import json
import os
from datetime import datetime, timezone
from contextlib import contextmanager
import bcrypt

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
    """Initialize database"""

    with get_conn() as conn:

        conn.executescript("""

        CREATE TABLE IF NOT EXISTS organisations (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            slug TEXT UNIQUE NOT NULL,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS users (
            id TEXT PRIMARY KEY,
            org_id TEXT,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL,
            name TEXT NOT NULL,
            is_active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS calls (
            id TEXT PRIMARY KEY,
            org_id TEXT,
            filename TEXT,
            file_path TEXT,
            file_size INTEGER,
            agent_id TEXT,
            campaign_id TEXT,
            loan_id TEXT,
            customer_id TEXT,
            source TEXT,
            source_uri TEXT,
            status TEXT,
            score REAL,
            score_pct REAL,
            confidence_pct INTEGER DEFAULT 0,
            scores_breakdown TEXT,
            compliance_flags TEXT,
            ptp_detected INTEGER DEFAULT 0,
            ptp_amount TEXT,
            ptp_date TEXT,
            ptp_mode TEXT,
            agent_sentiment TEXT,
            sentiment_notes TEXT,
            summary TEXT,
            key_issues TEXT,
            strengths TEXT,
            coaching_tip TEXT,
            transcript TEXT,
            error TEXT,
            uploaded_at TEXT,
            processed_at TEXT
        );

        CREATE TABLE IF NOT EXISTS drive_configs (
            id TEXT PRIMARY KEY,
            org_id TEXT,
            folder_url TEXT,
            folder_id TEXT,
            auto_sync INTEGER DEFAULT 0,
            last_synced TEXT
        );

        INSERT OR IGNORE INTO organisations (
            id,
            name,
            slug
        )
        VALUES (
            'org_default',
            'Company Finance',
            'company-finance'
        );

        INSERT OR IGNORE INTO users (
            id,
            org_id,
            email,
            password_hash,
            role,
            name
        )
        VALUES (
            'user_admin',
            'org_default',
            'admin@care.ai',
            'temp',
            'super_admin',
            'QA Manager'
        );

        """)

        # FORCE RESET ADMIN PASSWORD
        admin_hash = bcrypt.hashpw(
            "care@2025".encode("utf-8"),
            bcrypt.gensalt()
        ).decode("utf-8")

        conn.execute("""
            UPDATE users
            SET password_hash=?
            WHERE email=?
        """, (
            admin_hash,
            "admin@care.ai"
        ))

    print(f"[DB] ✓ Initialized at {DB_PATH}")


# ─────────────────────────────────────────────────────────────
# CALL CRUD
# ─────────────────────────────────────────────────────────────

def save_call(call: dict):

    with get_conn() as conn:

        conn.execute("""
        INSERT OR REPLACE INTO calls
        (
            id,
            org_id,
            filename,
            file_path,
            file_size,
            agent_id,
            campaign_id,
            loan_id,
            customer_id,
            source,
            source_uri,
            status,
            score,
            score_pct,
            confidence_pct,
            scores_breakdown,
            compliance_flags,
            ptp_detected,
            ptp_amount,
            ptp_date,
            ptp_mode,
            agent_sentiment,
            sentiment_notes,
            summary,
            key_issues,
            strengths,
            coaching_tip,
            transcript,
            error,
            uploaded_at,
            processed_at
        )
        VALUES
        (
            :id,
            :org_id,
            :filename,
            :file_path,
            :file_size,
            :agent_id,
            :campaign_id,
            :loan_id,
            :customer_id,
            :source,
            :source_uri,
            :status,
            :score,
            :score_pct,
            :confidence_pct,
            :scores_breakdown,
            :compliance_flags,
            :ptp_detected,
            :ptp_amount,
            :ptp_date,
            :ptp_mode,
            :agent_sentiment,
            :sentiment_notes,
            :summary,
            :key_issues,
            :strengths,
            :coaching_tip,
            :transcript,
            :error,
            :uploaded_at,
            :processed_at
        )
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
            "uploaded_at": call.get(
                "uploaded_at",
                datetime.now(timezone.utc).isoformat()
            ),
            "processed_at": call.get("processed_at"),
        })


# ─────────────────────────────────────────────────────────────
# AUTH
# ─────────────────────────────────────────────────────────────

def get_user_by_email(email: str):

    with get_conn() as conn:

        row = conn.execute(
            """
            SELECT *
            FROM users
            WHERE email=? AND is_active=1
            """,
            (email,)
        ).fetchone()

    return dict(row) if row else None


def get_user_by_id(user_id: str):

    with get_conn() as conn:

        row = conn.execute(
            """
            SELECT *
            FROM users
            WHERE id=?
            """,
            (user_id,)
        ).fetchone()

    return dict(row) if row else None


def create_user(
    user_id,
    org_id,
    email,
    password_hash,
    role,
    name
):

    with get_conn() as conn:

        conn.execute("""
            INSERT INTO users
            (
                id,
                org_id,
                email,
                password_hash,
                role,
                name
            )
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            user_id,
            org_id,
            email,
            password_hash,
            role,
            name
        ))


# ─────────────────────────────────────────────────────────────
# DRIVE CONFIG
# ─────────────────────────────────────────────────────────────

def get_drive_config(org_id: str):

    with get_conn() as conn:

        row = conn.execute(
            """
            SELECT *
            FROM drive_configs
            WHERE org_id=?
            """,
            (org_id,)
        ).fetchone()

    return dict(row) if row else None


def save_drive_config(
    org_id: str,
    folder_url: str,
    folder_id: str,
    auto_sync: bool = False
):

    with get_conn() as conn:

        conn.execute("""
        INSERT OR REPLACE INTO drive_configs
        (
            id,
            org_id,
            folder_url,
            folder_id,
            auto_sync
        )
        VALUES (?, ?, ?, ?, ?)
        """, (
            f"dc_{org_id}",
            org_id,
            folder_url,
            folder_id,
            1 if auto_sync else 0
        ))


def update_drive_last_synced(org_id: str):

    with get_conn() as conn:

        conn.execute("""
            UPDATE drive_configs
            SET last_synced=?
            WHERE org_id=?
        """, (
            datetime.now(timezone.utc).isoformat(),
            org_id
        ))
