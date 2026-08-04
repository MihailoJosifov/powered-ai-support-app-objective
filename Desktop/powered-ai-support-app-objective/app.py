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

VALID_STATUSES = {"open", "in_progress", "resolved", "closed"}
VALID_PRIORITIES = {"low", "medium", "high", "urgent"}
VALID_CATEGORIES = {"bug", "feature_request", "question", "documentation", "other"}


_tables_ready = False


def ensure_tables():
    """
    Create the tickets/ticket_messages tables in Lakebase if they don't exist
    yet, and add any columns that are missing from an older version of the
    schema. This is intentionally NON-destructive - it never drops data.

    Runs at most once per app process (guarded by _tables_ready), not on
    every request, since it's only needed to bring the schema up to date.
    """
    global _tables_ready
    if _tables_ready:
        return

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

    # Backfill any columns added by later feature work (priority, category,
    # etc.) onto a table that may have been created by an earlier version of
    # this app - without ever touching existing rows.
    for column_def in (
        "priority TEXT NOT NULL DEFAULT 'medium'",
        "category TEXT NOT NULL DEFAULT 'other'",
        "assigned_to TEXT",
        "updated_at TIMESTAMPTZ NOT NULL DEFAULT now()",
    ):
        lakebase.run_write(
            f"ALTER TABLE {TICKETS_TABLE} ADD COLUMN IF NOT EXISTS {column_def}"
        )

    logger.info("Lakebase schema is up to date")
    _tables_ready = True


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

    priority = (data.get("priority") or "medium").strip()
    if priority not in VALID_PRIORITIES:
        priority = "medium"

    category = (data.get("category") or "other").strip()
    if category not in VALID_CATEGORIES:
        category = "other"

    created_by = _current_user_email()

    rows = lakebase.run_query(
        f"""
        INSERT INTO {TICKETS_TABLE} (title, status, priority, category, created_by)
        VALUES (%s, 'open', %s, %s, %s)
        RETURNING *
        """,
        (title, priority, category, created_by),
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
        UPDATE {TICKETS_TABLE} SET status = %s, updated_at = now()
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


@app.route("/stats", methods=["GET"])
def get_stats():
    """Return ticket statistics for the dashboard."""
    ensure_tables()

    # Count by status - explicitly include every valid status, defaulting
    # to 0, so the dashboard always has a real number to show (not just
    # whichever statuses happen to have at least one ticket right now).
    status_counts = lakebase.run_query(
        f"""
        SELECT s.status, COALESCE(t.count, 0) AS count
        FROM unnest(%s::text[]) AS s(status)
        LEFT JOIN (
            SELECT status, COUNT(*) AS count FROM {TICKETS_TABLE} GROUP BY status
        ) t ON t.status = s.status
        """,
        (list(VALID_STATUSES),),
    )

    priority_counts = lakebase.run_query(
        f"""
        SELECT p.priority, COALESCE(t.count, 0) AS count
        FROM unnest(%s::text[]) AS p(priority)
        LEFT JOIN (
            SELECT priority, COUNT(*) AS count FROM {TICKETS_TABLE} GROUP BY priority
        ) t ON t.priority = p.priority
        """,
        (list(VALID_PRIORITIES),),
    )

    category_counts = lakebase.run_query(
        f"SELECT category, COUNT(*) as count FROM {TICKETS_TABLE} GROUP BY category"
    )

    recent_tickets = lakebase.run_query(
        f"SELECT COUNT(*) as count FROM {TICKETS_TABLE} WHERE created_at > NOW() - INTERVAL '7 days'"
    )

    return jsonify({
        "by_status": status_counts,
        "by_priority": priority_counts,
        "by_category": category_counts,
        "recent_count": recent_tickets[0]["count"] if recent_tickets else 0
    })


@app.route("/search", methods=["GET"])
def search_tickets():
    """Search tickets by title or other criteria."""
    ensure_tables()
    query = request.args.get("q", "").strip()
    
    if not query:
        return jsonify([])
    
    rows = lakebase.run_query(
        f"SELECT * FROM {TICKETS_TABLE} WHERE title ILIKE %s ORDER BY created_at DESC",
        (f"%{query}%",)
    )
    return jsonify(rows)


@app.route("/seed-data", methods=["GET", "POST"])
def seed_data():
    """Populate database with 50+ sample users and one ticket each (for demo/testing)."""
    from datetime import datetime, timedelta
    import random

    ensure_tables()

    first_names = [
        "Alice", "Bob", "Carol", "David", "Emma", "Frank", "Grace", "Henry",
        "Isabel", "Jack", "Karen", "Liam", "Maria", "Noah", "Olivia", "Peter",
        "Quinn", "Rachel", "Sam", "Tina", "Uma", "Victor", "Wendy", "Xavier",
        "Yasmin", "Zack", "Aaron", "Bella", "Carlos", "Diana", "Ethan", "Fiona",
        "George", "Hannah", "Ivan", "Julia", "Kevin", "Laura", "Marcus", "Nina",
        "Oscar", "Priya", "Quentin", "Rosa", "Steven", "Talia", "Umar", "Vera",
        "Walter", "Ximena",
    ]
    last_names = [
        "Johnson", "Smith", "White", "Brown", "Davis", "Wilson", "Taylor", "Moore",
        "Martin", "Clark", "Lewis", "Walker", "Hall", "Allen", "Young", "King",
        "Wright", "Scott", "Green", "Baker", "Adams", "Nelson", "Carter", "Mitchell",
        "Perez", "Roberts", "Turner", "Phillips", "Campbell", "Parker", "Evans",
        "Edwards", "Collins", "Stewart", "Sanchez", "Morris", "Rogers", "Reed",
        "Cook", "Morgan", "Bell", "Murphy", "Bailey", "Rivera", "Cooper", "Richardson",
        "Cox", "Howard", "Ward", "Peterson",
    ]
    users = [f"{f.lower()}.{l.lower()}@company.com" for f, l in zip(first_names, last_names)]

    support_staff = [
        "support.team@company.com",
        "tech.support@company.com",
        "admin@company.com",
    ]

    # Ticket "templates" - cycled through for each of the 50 users, so every
    # user gets a realistic title/priority/category/status/messages combo
    # instead of 50 identical tickets.
    ticket_templates = [
        {"title": "Login page not loading after recent update", "priority": "urgent", "category": "bug", "status": "open",
         "messages": ["Since the latest deployment yesterday, the login page returns a 500 error. Multiple users are affected."]},
        {"title": "Data export functionality returns empty CSV files", "priority": "high", "category": "bug", "status": "in_progress",
         "messages": ["When I try to export customer data, the CSV file downloads but it's completely empty.",
                      "Thanks for reporting! I've reproduced the issue. Investigating now.",
                      "Update: Found the issue - working on a fix."]},
        {"title": "Dashboard charts not rendering on mobile devices", "priority": "medium", "category": "bug", "status": "open",
         "messages": ["The analytics dashboard works fine on desktop but all charts show as blank boxes on mobile."]},
        {"title": "Search function returns duplicate results", "priority": "low", "category": "bug", "status": "resolved",
         "messages": ["When searching for products, I'm seeing each item appear 2-3 times.", "Fixed and deployed."]},
        {"title": "Add dark mode theme option", "priority": "low", "category": "feature_request", "status": "open",
         "messages": ["Would love to have a dark mode option for the application."]},
        {"title": "Bulk upload functionality for customer records", "priority": "high", "category": "feature_request", "status": "in_progress",
         "messages": ["We need to be able to upload customer records in bulk via CSV.", "This is now on our roadmap for Q1."]},
        {"title": "Email notifications for ticket status changes", "priority": "medium", "category": "feature_request", "status": "open",
         "messages": ["It would be helpful to receive email notifications when a support ticket gets updated."]},
        {"title": "How to configure two-factor authentication?", "priority": "low", "category": "question", "status": "resolved",
         "messages": ["I want to enable 2FA but can't find the option.", "You can find it under Account > Security Settings.", "Found it, thanks!"]},
        {"title": "What are the API rate limits?", "priority": "medium", "category": "question", "status": "resolved",
         "messages": ["I need to know the API rate limits for production use.", "1000 requests per hour for standard, 5000 for enterprise accounts."]},
        {"title": "Best practices for data backup and recovery", "priority": "medium", "category": "question", "status": "open",
         "messages": ["What's the recommended approach for backing up our data?"]},
        {"title": "Installation guide is outdated for version 3.0", "priority": "medium", "category": "documentation", "status": "in_progress",
         "messages": ["The installation docs still reference version 2.5 commands.", "We're updating the docs now."]},
        {"title": "Missing API endpoint documentation for webhooks", "priority": "low", "category": "documentation", "status": "open",
         "messages": ["Can we get documentation for the webhook configuration and payload formats?"]},
        {"title": "Account access for new team member", "priority": "urgent", "category": "other", "status": "resolved",
         "messages": ["We have a new developer starting tomorrow who needs access.", "Go to Settings > Team Management > Invite User.", "Done!"]},
        {"title": "Billing inquiry - unexpected charges", "priority": "high", "category": "other", "status": "in_progress",
         "messages": ["Our invoice shows higher charges than expected.", "Can you provide the invoice number?", "Invoice #INV-2026-08-001", "Reviewing now."]},
        {"title": "Performance degradation during peak hours", "priority": "high", "category": "bug", "status": "open",
         "messages": ["Between 2-4 PM EST, the app becomes very slow. Page loads take 10+ seconds."]},
        {"title": "Unable to reset password via email link", "priority": "high", "category": "bug", "status": "open",
         "messages": ["The 'forgot password' email never arrives, even after multiple attempts."]},
        {"title": "Request for a public API changelog", "priority": "low", "category": "feature_request", "status": "closed",
         "messages": ["Could you publish a changelog whenever the API changes?", "Added - see /changelog.", "Perfect, thanks!"]},
        {"title": "Clarify data retention policy", "priority": "medium", "category": "question", "status": "closed",
         "messages": ["How long is user data retained after account deletion?", "90 days, then permanently purged."]},
        {"title": "Typo in onboarding email template", "priority": "low", "category": "bug", "status": "resolved",
         "messages": ["The welcome email says 'you're' instead of 'your' in the subject line.", "Fixed in the next send batch."]},
        {"title": "Slow response times on the reporting endpoint", "priority": "urgent", "category": "bug", "status": "in_progress",
         "messages": ["The /reports endpoint is timing out for large date ranges.", "Adding pagination - ETA end of week."]},
    ]

    base_time = datetime.now()
    created_count = 0

    for i, user_email in enumerate(users):
        template = ticket_templates[i % len(ticket_templates)]
        days_ago = random.randint(0, 30)
        created_at = base_time - timedelta(days=days_ago, hours=random.randint(0, 23))

        ticket_rows = lakebase.run_query(
            f"INSERT INTO {TICKETS_TABLE} (title, status, priority, category, created_by, created_at, updated_at) "
            f"VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING ticket_id",
            (template["title"], template["status"], template["priority"],
             template["category"], user_email, created_at, created_at)
        )

        ticket_id = ticket_rows[0]["ticket_id"]
        created_count += 1

        for j, message_text in enumerate(template["messages"]):
            author = user_email if j == 0 else (random.choice(support_staff) if j % 2 == 1 else user_email)
            message_time = created_at + timedelta(hours=j * random.randint(1, 12))

            lakebase.run_query(
                f"INSERT INTO {MESSAGES_TABLE} (ticket_id, message_text, author, created_at) VALUES (%s, %s, %s, %s)",
                (ticket_id, message_text, author, message_time)
            )

    return jsonify({
        "success": True,
        "users_created": len(users),
        "tickets_created": created_count,
        "message": f"Created {created_count} tickets across {len(users)} users."
    })


if __name__ == '__main__':
    host = os.getenv('FLASK_RUN_HOST', '0.0.0.0')
    port = int(os.getenv('FLASK_RUN_PORT', 8000))
    app.run(debug=True, host=host, port=port)
