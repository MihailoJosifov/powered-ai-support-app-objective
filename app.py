"""
Databricks App: internal support ticket system.
- Serves a small Flask API + UI
- Reads/writes to Lakebase (Databricks-managed Postgres) via lakebase.py

Run locally:
    python app.py
Deploy as a Databricks App using app.yaml.
"""

import logging
import os

from databricks.sdk import WorkspaceClient
from flask import Flask, jsonify, render_template, request

import lakebase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("support-ticket-app")

app = Flask(__name__)
_w = WorkspaceClient()

TICKETS_TABLE = os.environ.get("TICKETS_TABLE_NAME", "tickets")
MESSAGES_TABLE = os.environ.get("MESSAGES_TABLE_NAME", "ticket_messages")

VALID_STATUSES = {"open", "in_progress", "resolved"}


def ensure_tables():
    """Create the tickets/ticket_messages tables in Lakebase if they don't exist yet."""
    lakebase.run_write(
        f"""
        CREATE TABLE IF NOT EXISTS {TICKETS_TABLE} (
            ticket_id   SERIAL PRIMARY KEY,
            title       TEXT NOT NULL,
            status      TEXT NOT NULL DEFAULT 'open',
            created_by  TEXT NOT NULL,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    lakebase.run_write(
        f"""
        CREATE TABLE IF NOT EXISTS {MESSAGES_TABLE} (
            message_id   SERIAL PRIMARY KEY,
            ticket_id    INTEGER NOT NULL REFERENCES {TICKETS_TABLE}(ticket_id),
            message_text TEXT NOT NULL,
            author       TEXT NOT NULL,
            created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )


def _current_user_email() -> str:
    """
    Resolve the current user's email.

    Databricks Apps inject the logged-in user's identity via the
    X-Forwarded-Email header on every request. Fall back to the Databricks
    SDK's current_user API for local development where that header isn't set.
    """
    header_email = request.headers.get("X-Forwarded-Email")
    if header_email:
        return header_email
    return _w.current_user.me().user_name


@app.route("/healthz")
def healthz():
    return jsonify({"status": "ok"})


@app.errorhandler(Exception)
def handle_exception(err):
    """Ensure all unhandled errors return JSON (not an HTML error page),
    so the frontend's resp.json() call never chokes on HTML."""
    logger.exception("Unhandled exception while processing request")
    status_code = getattr(err, "code", 500)
    if not isinstance(status_code, int):
        status_code = 500
    return jsonify({"error": str(err)}), status_code


@app.route("/")
def index():
    """Main UI: ticket list + ticket detail panel."""
    ensure_tables()
    return render_template("index.html")


@app.route("/tickets", methods=["GET"])
def list_tickets():
    """Return all tickets, optionally filtered by status."""
    ensure_tables()
    status = request.args.get("status")
    if status:
        rows = lakebase.run_query(
            f"SELECT * FROM {TICKETS_TABLE} WHERE status = %s ORDER BY created_at DESC",
            (status,),
        )
    else:
        rows = lakebase.run_query(
            f"SELECT * FROM {TICKETS_TABLE} ORDER BY created_at DESC"
        )
    return jsonify(rows)


@app.route("/tickets", methods=["POST"])
def create_ticket():
    """Create a new ticket."""
    ensure_tables()
    data = request.get_json(silent=True) or request.form

    title = (data.get("title") or "").strip()
    if not title:
        return jsonify({"error": "Title is required"}), 400

    created_by = _current_user_email()

    rows = lakebase.run_query(
        f"""
        INSERT INTO {TICKETS_TABLE} (title, status, created_by)
        VALUES (%s, 'open', %s)
        RETURNING *
        """,
        (title, created_by),
    )
    return jsonify(rows[0]), 201


@app.route("/tickets/<int:ticket_id>", methods=["GET"])
def get_ticket(ticket_id):
    """Return a single ticket plus its messages."""
    ensure_tables()
    ticket_rows = lakebase.run_query(
        f"SELECT * FROM {TICKETS_TABLE} WHERE ticket_id = %s", (ticket_id,)
    )
    if not ticket_rows:
        return jsonify({"error": f"Ticket {ticket_id} not found"}), 404

    message_rows = lakebase.run_query(
        f"SELECT * FROM {MESSAGES_TABLE} WHERE ticket_id = %s ORDER BY created_at ASC",
        (ticket_id,),
    )
    return jsonify({"ticket": ticket_rows[0], "messages": message_rows})


@app.route("/tickets/<int:ticket_id>/status", methods=["POST"])
def update_status(ticket_id):
    """Update a ticket's status."""
    ensure_tables()
    data = request.get_json(silent=True) or request.form
    new_status = (data.get("status") or "").strip()

    if new_status not in VALID_STATUSES:
        return jsonify({
            "error": f"Invalid status {new_status!r}. Must be one of {sorted(VALID_STATUSES)}"
        }), 400

    rows = lakebase.run_query(
        f"""
        UPDATE {TICKETS_TABLE} SET status = %s
        WHERE ticket_id = %s
        RETURNING *
        """,
        (new_status, ticket_id),
    )
    if not rows:
        return jsonify({"error": f"Ticket {ticket_id} not found"}), 404
    return jsonify(rows[0])


@app.route("/tickets/<int:ticket_id>/messages", methods=["POST"])
def add_message(ticket_id):
    """Add a message to an existing ticket."""
    ensure_tables()
    data = request.get_json(silent=True) or request.form
    message_text = (data.get("message_text") or "").strip()

    if not message_text:
        return jsonify({"error": "message_text is required"}), 400

    ticket_exists = lakebase.run_query(
        f"SELECT ticket_id FROM {TICKETS_TABLE} WHERE ticket_id = %s", (ticket_id,)
    )
    if not ticket_exists:
        return jsonify({"error": f"Ticket {ticket_id} not found"}), 404

    author = _current_user_email()

    rows = lakebase.run_query(
        f"""
        INSERT INTO {MESSAGES_TABLE} (ticket_id, message_text, author)
        VALUES (%s, %s, %s)
        RETURNING *
        """,
        (ticket_id, message_text, author),
    )
    return jsonify(rows[0]), 201


if __name__ == '__main__':
    host = os.getenv('FLASK_RUN_HOST', '0.0.0.0')
    port = int(os.getenv('FLASK_RUN_PORT', 8000))
    app.run(debug=True, host=host, port=port)
