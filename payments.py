from flask import (
    Blueprint, render_template, request, redirect, url_for,
    flash, jsonify, Response, session
)
import pymysql
import io
import os
import json
import datetime
from pathlib import Path
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

try:
    from zoneinfo import ZoneInfo
    KARACHI_TZ = ZoneInfo("Asia/Karachi")
except Exception:
    KARACHI_TZ = None

payment_bp = Blueprint("payment", __name__, template_folder="templates")

BASE_DIR = Path(__file__).resolve().parent

# ==================== DATABASE CONFIG ====================
DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "gateway01.ap-southeast-1.prod.aws.tidbcloud.com"),
    "port": int(os.environ.get("DB_PORT", 4000)),
    "user": os.environ.get("DB_USER", "CNohVtpAqayoKTZ.root"),
    "password": os.environ.get("DB_PASSWORD", "e2uvtqjuE5tWuUJ4"),
    "database": os.environ.get("DB_NAME", "awaisyarnbroker"),
    "cursorclass": pymysql.cursors.DictCursor,
    "autocommit": False,
    "ssl": {"ca": "ca.pem"}
}

CA_FILE = os.environ.get("DB_CA_FILE", str(BASE_DIR / "ca.pem"))
if CA_FILE and os.path.exists(CA_FILE):
    DB_CONFIG["ssl"] = {"ca": CA_FILE}

# ==================== CUSTOMERS CACHE ====================
ALL_CUSTOMERS_FILE = BASE_DIR / "allcustomers.json"


def get_db_connection():
    return pymysql.connect(**DB_CONFIG)


def today_date():
    if KARACHI_TZ:
        return datetime.datetime.now(KARACHI_TZ).date()
    return datetime.date.today()


def fmt_date(value):
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.strftime("%Y-%m-%d")
    return str(value) if value else ""


def normalize_search(value):
    return (value or "").strip()


def like_contains(value):
    return f"%{normalize_search(value).lower()}%"


def like_prefix(value):
    return f"{normalize_search(value).lower()}%"


def get_quick_filter_range(filter_name):
    today = today_date()

    if filter_name == "today":
        return today, today
    if filter_name == "yesterday":
        y = today - datetime.timedelta(days=1)
        return y, y
    if filter_name == "last3":
        return today - datetime.timedelta(days=2), today
    if filter_name == "last5":
        return today - datetime.timedelta(days=4), today
    if filter_name == "last7":
        return today - datetime.timedelta(days=6), today
    if filter_name == "last10":
        return today - datetime.timedelta(days=9), today
    return None, None


def build_filters():
    """
    Filters on created_at and supports quick filter + type filter.
    Search is kept simple to avoid SQL escaping issues.
    """
    search = request.args.get("search", "").strip().lower()
    from_date = request.args.get("from_date", "").strip()
    to_date = request.args.get("to_date", "").strip()
    quick_filter = request.args.get("filter", "").strip().lower()
    type_filter = request.args.get("type_filter", "").strip().lower()

    if quick_filter in {"today", "yesterday", "last3", "last5", "last7", "last10"}:
        q_from, q_to = get_quick_filter_range(quick_filter)
        from_date = q_from.isoformat()
        to_date = q_to.isoformat()

    where_clauses = []
    params = []

    if search:
        contains = like_contains(search)
        where_clauses.append(
            """
            (
                LOWER(TRIM(c.name)) LIKE %s OR
                LOWER(TRIM(p.type)) LIKE %s OR
                LOWER(TRIM(p.mode)) LIKE %s OR
                LOWER(TRIM(p.bank_name)) LIKE %s OR
                LOWER(TRIM(p.reference)) LIKE %s
            )
            """
        )
        params.extend([contains, contains, contains, contains, contains])

    if from_date and to_date:
        where_clauses.append("DATE(p.created_at) BETWEEN %s AND %s")
        params.extend([from_date, to_date])
    elif from_date:
        where_clauses.append("DATE(p.created_at) >= %s")
        params.append(from_date)
    elif to_date:
        where_clauses.append("DATE(p.created_at) <= %s")
        params.append(to_date)

    if type_filter in {"received", "paid"}:
        where_clauses.append("LOWER(TRIM(p.type)) = %s")
        params.append(type_filter)

    where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    return where_sql, params, from_date, to_date, type_filter


def fetch_summary(cursor, where_sql, params):
    cursor.execute(
        f"""
        SELECT
            COUNT(*) AS total_count,
            COALESCE(SUM(CASE WHEN LOWER(TRIM(p.type)) = 'paid' THEN p.amount ELSE 0 END), 0) AS total_paid,
            COALESCE(SUM(CASE WHEN LOWER(TRIM(p.type)) = 'received' THEN p.amount ELSE 0 END), 0) AS total_received
        FROM payments p
        JOIN customer c ON p.customer_id = c.id
        {where_sql}
        """,
        params,
    )

    row = cursor.fetchone() or {}
    total_count = int(row.get("total_count", 0) or 0)
    total_paid = float(row.get("total_paid", 0) or 0)
    total_received = float(row.get("total_received", 0) or 0)
    current_balance = total_received - total_paid

    return total_count, total_paid, total_received, current_balance


@payment_bp.route("/pm", methods=["GET", "POST"])
def payment():
    if "user" not in session:
        return redirect(url_for("login"))

    if request.method == "POST":
        conn = None
        try:
            payment_id = request.form.get("id")
            customer_id = request.form.get("customer_id")
            amount = request.form.get("amount")
            ptype = request.form.get("type")
            mode = request.form.get("mode")
            bank_name = request.form.get("bank_name")
            reference = request.form.get("reference")
            date_str = request.form.get("date")

            if not customer_id or not amount or not ptype:
                flash("❌ Customer, Amount and Type are required!", "error")
                return redirect(url_for("payment.payment"))

            amount = float(amount)
            conn = get_db_connection()
            with conn.cursor() as cursor:
                if payment_id:
                    cursor.execute(
                        """
                        UPDATE payments
                        SET customer_id=%s, amount=%s, type=%s, mode=%s,
                            bank_name=%s, reference=%s, date=%s
                        WHERE id=%s
                        """,
                        (customer_id, amount, ptype, mode, bank_name, reference, date_str, payment_id),
                    )
                    flash("✅ Payment updated successfully!", "success")
                else:
                    cursor.execute(
                        """
                        INSERT INTO payments
                        (customer_id, amount, type, mode, bank_name, reference, date)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (customer_id, amount, ptype, mode, bank_name, reference, date_str),
                    )
                    flash("✅ Payment added successfully!", "success")

            conn.commit()

        except Exception as e:
            if conn:
                conn.rollback()
            flash(f"❌ Error: {str(e)}", "error")

        finally:
            if conn:
                conn.close()

        return redirect(url_for("payment.payment"))

    return render_template("pm.html")


@payment_bp.route("/api/customers/load-temp", methods=["POST"])
def load_customers_to_temp():
    if "user" not in session:
        return jsonify({"error": "Unauthorized"}), 401

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, name, city, phone
                FROM customer
                ORDER BY LOWER(TRIM(name)) ASC
                """
            )
            customers = cursor.fetchall()

        safe_customers = [
            {
                "id": c["id"],
                "name": c["name"] or "",
                "city": c["city"] or "",
                "phone": str(c["phone"] or ""),
            }
            for c in customers
        ]

        tmp_file = ALL_CUSTOMERS_FILE.with_suffix(".json.tmp")
        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(safe_customers, f, ensure_ascii=False, indent=2)
        os.replace(tmp_file, ALL_CUSTOMERS_FILE)

        return jsonify({"success": True, "customers": safe_customers})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        if conn:
            conn.close()


@payment_bp.route("/api/customers", methods=["GET"])
def get_customers():
    if "user" not in session:
        return jsonify({"error": "Unauthorized"}), 401

    search = request.args.get("search", "").strip().lower()
    per_page = 12
    conn = None

    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            if search:
                prefix = like_prefix(search)
                term = normalize_search(search)

                cursor.execute(
                    """
                    SELECT id, name, city, phone
                    FROM customer
                    WHERE (
                        LOWER(TRIM(name)) LIKE %s OR
                        LOWER(TRIM(city)) LIKE %s OR
                        CAST(phone AS CHAR) LIKE %s
                    )
                    ORDER BY
                        CASE
                            WHEN LOWER(TRIM(name)) = %s THEN 0
                            WHEN LOWER(TRIM(name)) LIKE %s THEN 1
                            WHEN LOWER(TRIM(city)) LIKE %s THEN 2
                            WHEN CAST(phone AS CHAR) LIKE %s THEN 3
                            ELSE 4
                        END,
                        LOWER(TRIM(name)) ASC
                    LIMIT %s
                    """,
                    (prefix, prefix, prefix, term, prefix, prefix, prefix, per_page),
                )
            else:
                cursor.execute(
                    """
                    SELECT id, name, city, phone
                    FROM customer
                    ORDER BY LOWER(TRIM(name)) ASC
                    LIMIT %s
                    """,
                    (per_page,),
                )

            customers = cursor.fetchall()

        return jsonify({"customers": customers})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        if conn:
            conn.close()


@payment_bp.route("/api/payments", methods=["GET"])
def get_payments():
    if "user" not in session:
        return jsonify({"error": "Unauthorized"}), 401

    try:
        page = int(request.args.get("page", 1))
    except ValueError:
        page = 1

    try:
        per_page = int(request.args.get("per_page", 30))
    except ValueError:
        per_page = 30

    if per_page <= 0:
        per_page = 30

    offset = (page - 1) * per_page
    where_sql, params, from_date, to_date, type_filter = build_filters()

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            total, total_paid, total_received, current_balance = fetch_summary(cursor, where_sql, params)

            cursor.execute(
                f"""
                SELECT
                    p.id,
                    p.customer_id,
                    c.name AS customer_name,
                    p.amount,
                    p.type,
                    p.mode,
                    p.bank_name,
                    p.reference,
                    DATE_FORMAT(p.date, '%%Y-%%m-%%d') AS payment_date,
                    DATE_FORMAT(p.created_at, '%%d-%%m-%%Y %%H:%%i') AS created_at
                FROM payments p
                JOIN customer c ON p.customer_id = c.id
                {where_sql}
                ORDER BY p.created_at DESC, p.id DESC
                LIMIT %s OFFSET %s
                """,
                params + [per_page, offset],
            )

            payments = cursor.fetchall()

            for p in payments:
                if p.get("amount") is not None:
                    p["amount"] = float(p["amount"])

            total_pages = max(1, (total + per_page - 1) // per_page)

        return jsonify(
            {
                "payments": payments,
                "page": page,
                "total_pages": total_pages,
                "total": total,
                "total_paid": total_paid,
                "total_received": total_received,
                "current_balance": current_balance,
                "from_date": from_date,
                "to_date": to_date,
                "type_filter": type_filter,
            }
        )

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        if conn:
            conn.close()


@payment_bp.route("/api/payments/<int:payment_id>", methods=["DELETE"])
def delete_payment(payment_id):
    if "user" not in session:
        return jsonify({"success": False, "message": "Unauthorized"}), 401

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM payments WHERE id=%s", (payment_id,))
        conn.commit()
        return jsonify({"success": True, "message": "Payment deleted successfully!"})

    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "message": str(e)}), 500

    finally:
        if conn:
            conn.close()


@payment_bp.route("/api/payments/<int:payment_id>", methods=["PUT"])
def update_payment(payment_id):
    if "user" not in session:
        return jsonify({"success": False, "message": "Unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    customer_id = data.get("customer_id")
    amount = data.get("amount")
    ptype = data.get("type")
    mode = data.get("mode")
    bank_name = data.get("bank_name")
    reference = data.get("reference")
    date_str = data.get("date")

    if not customer_id or amount is None or not ptype:
        return jsonify({"success": False, "message": "Customer, Amount and Type are required"}), 400

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE payments
                SET customer_id=%s, amount=%s, type=%s, mode=%s,
                    bank_name=%s, reference=%s, date=%s
                WHERE id=%s
                """,
                (customer_id, float(amount), ptype, mode, bank_name, reference, date_str, payment_id),
            )
        conn.commit()
        return jsonify({"success": True, "message": "Payment updated successfully!"})

    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "message": str(e)}), 500

    finally:
        if conn:
            conn.close()


@payment_bp.route("/pm/download_pdf")
def download_payments_pdf():
    if "user" not in session:
        return redirect(url_for("login"))

    where_sql, params, from_date, to_date, type_filter = build_filters()
    search = request.args.get("search", "").strip()
    quick_filter = request.args.get("filter", "").strip().lower()

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    c.name,
                    p.amount,
                    p.type,
                    p.mode,
                    p.bank_name,
                    p.reference,
                    p.date,
                    p.created_at
                FROM payments p
                JOIN customer c ON p.customer_id = c.id
                {where_sql}
                ORDER BY p.created_at DESC, p.id DESC
                """,
                params,
            )
            payments = cursor.fetchall()
            total, total_paid, total_received, current_balance = fetch_summary(cursor, where_sql, params)

        buffer = io.BytesIO()
        c = canvas.Canvas(buffer, pagesize=letter)
        width, height = letter

        c.setFont("Helvetica-Bold", 18)
        c.drawString(50, height - 50, "Awais Yarn Waste Broker")
        c.setFont("Helvetica-Bold", 14)
        c.drawString(50, height - 75, "Payments / Transactions List")
        c.setFont("Helvetica", 10)
        c.drawString(50, height - 95, f"Generated: {datetime.datetime.now().strftime('%d %B %Y %H:%M')}")

        summary_line = (
            f"Transactions: {total}   Paid: {total_paid:.2f}   "
            f"Received: {total_received:.2f}   Balance: {current_balance:.2f}"
        )
        c.drawString(50, height - 110, summary_line[:110])

        if search:
            c.drawString(50, height - 125, f"Search: {search[:90]}")
        if from_date or to_date:
            c.drawString(50, height - 140, f"Date Range: {from_date or '-'} to {to_date or '-'}")
        elif quick_filter:
            c.drawString(50, height - 140, f"Filter: {quick_filter}")
        if type_filter:
            c.drawString(50, height - 155, f"Type: {type_filter}")

        def draw_header(y_pos):
            c.setFont("Helvetica-Bold", 10)
            c.drawString(40, y_pos, "#")
            c.drawString(60, y_pos, "Customer")
            c.drawString(190, y_pos, "Amount")
            c.drawString(255, y_pos, "Type")
            c.drawString(315, y_pos, "Mode")
            c.drawString(385, y_pos, "Bank")
            c.drawString(465, y_pos, "Ref")
            c.drawString(540, y_pos, "Date")
            y_pos -= 10
            c.line(35, y_pos, width - 35, y_pos)
            return y_pos - 14

        start_y = height - 180 if type_filter else height - 165
        y = draw_header(start_y)
        c.setFont("Helvetica", 9)

        for idx, pay in enumerate(payments, start=1):
            if y < 55:
                c.showPage()
                y = draw_header(height - 50)
                c.setFont("Helvetica", 9)

            ref_text = str(pay["reference"] or "")
            if len(ref_text) > 14:
                ref_text = ref_text[:14] + "..."

            c.drawString(40, y, str(idx))
            c.drawString(60, y, str(pay["name"] or "")[:22])
            c.drawString(190, y, f"{float(pay['amount'] or 0):.2f}")
            c.drawString(255, y, str(pay["type"] or "")[:10])
            c.drawString(315, y, str(pay["mode"] or "-")[:12])
            c.drawString(385, y, str(pay["bank_name"] or "-")[:12])
            c.drawString(465, y, ref_text[:16])

            date_val = pay["date"]
            if isinstance(date_val, (datetime.date, datetime.datetime)):
                date_text = date_val.strftime("%d-%m-%Y")
            else:
                date_text = str(date_val or "-")
            c.drawString(540, y, date_text)
            y -= 16

        c.save()
        buffer.seek(0)

        return Response(
            buffer.getvalue(),
            mimetype="application/pdf",
            headers={"Content-Disposition": "attachment; filename=payments_list.pdf"},
        )

    except Exception as e:
        flash(f"PDF generation failed: {str(e)}", "error")
        return redirect(url_for("payment.payment"))

    finally:
        if conn:
            conn.close()