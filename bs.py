from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, Response, session
import pymysql
import io
import datetime
import json
import os
from pathlib import Path

try:
    from zoneinfo import ZoneInfo
    KARACHI_TZ = ZoneInfo("Asia/Karachi")
except Exception:
    KARACHI_TZ = None

bs_bp = Blueprint("balance_sheet", __name__, template_folder="templates")

BASE_DIR = Path(__file__).resolve().parent

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

CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bs.json")


def get_db():
    return pymysql.connect(**DB_CONFIG)


def get_cache_path():
    return CACHE_FILE


def to_float(value):
    try:
        return float(value or 0)
    except Exception:
        return 0.0


def refresh_bs_cache_from_db():
    conn = None
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    customer_id,
                    `Customer (City)`,
                    `Total To Pay`,
                    `Total To Receive`,
                    `Payments Made`,
                    `Payments Received`,
                    `Balance`
                FROM balance_sheet
                ORDER BY `Customer (City)` ASC
            """)
            rows = cur.fetchall() or []

        cache_rows = []
        for r in rows:
            cache_rows.append({
                "customer_id": r.get("customer_id"),
                "Customer (City)": r.get("Customer (City)") or "",
                "Total To Pay": to_float(r.get("Total To Pay")),
                "Total To Receive": to_float(r.get("Total To Receive")),
                "Payments Made": to_float(r.get("Payments Made")),
                "Payments Received": to_float(r.get("Payments Received")),
                "Balance": to_float(r.get("Balance")),
            })

        cache_path = get_cache_path()
        tmp_path = cache_path + ".tmp"

        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(cache_rows, f, ensure_ascii=False, indent=2)

        if os.path.exists(cache_path):
            os.remove(cache_path)
        os.replace(tmp_path, cache_path)

        return cache_rows
    finally:
        if conn:
            conn.close()


def load_bs_cache():
    cache_path = get_cache_path()
    if not os.path.exists(cache_path):
        return []
    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        return []
    except Exception:
        return []


def ensure_cache():
    try:
        return refresh_bs_cache_from_db()
    except Exception:
        cached = load_bs_cache()
        if cached:
            return cached
        raise


def apply_bs_filters(rows):
    search = request.args.get("search", "").strip().lower()
    balance_filter = request.args.get("balance_filter", "all").strip().lower()

    filtered = []
    for row in rows:
        customer = str(row.get("Customer (City)", "") or "").lower()
        bal = to_float(row.get("Balance"))

        if search and search not in customer:
            continue

        if balance_filter == "positive" and not (bal > 0):
            continue
        if balance_filter == "negative" and not (bal < 0):
            continue
        if balance_filter == "zero" and not (bal == 0):
            continue

        filtered.append(row)

    return filtered


def sort_bs_rows(rows, sort_field="customer", sort_dir="asc"):
    sort_field = (sort_field or "customer").strip().lower()
    sort_dir = (sort_dir or "asc").strip().lower()

    sort_map = {
        "customer": lambda r: str(r.get("Customer (City)", "") or "").lower(),
        "topay": lambda r: to_float(r.get("Total To Pay")),
        "toreceive": lambda r: to_float(r.get("Total To Receive")),
        "balance": lambda r: to_float(r.get("Balance")),
    }

    key_func = sort_map.get(sort_field, sort_map["customer"])
    reverse = sort_dir == "desc"
    return sorted(rows, key=key_func, reverse=reverse)


# ── Page route ────────────────────────────────────────────────────
@bs_bp.route("/bs")
def balance_sheet():
    if "user" not in session:
        return redirect(url_for("login"))

    try:
        refresh_bs_cache_from_db()
    except Exception as e:
        flash(f"Could not refresh balance sheet cache from database: {str(e)}", "error")

    return render_template("bs.html")


# ── API: cached data with search/sort/pagination ──────────────────
@bs_bp.route("/api/balance_sheet", methods=["GET"])
def api_balance_sheet():
    if "user" not in session:
        return jsonify({"error": "Unauthorized"}), 401

    sort_field = request.args.get("sort_field", "customer").strip().lower()
    sort_dir = request.args.get("sort_dir", "asc").strip().lower()

    try:
        page = max(1, int(request.args.get("page", 1)))
    except ValueError:
        page = 1

    try:
        per_page = max(1, int(request.args.get("per_page", 40)))
    except ValueError:
        per_page = 40

    try:
        rows = load_bs_cache()
        if not rows:
            rows = ensure_cache()
    except Exception as e:
        return jsonify({"error": f"Could not load balance sheet cache: {str(e)}"}), 500

    filtered_rows = apply_bs_filters(rows)
    sorted_rows = sort_bs_rows(filtered_rows, sort_field=sort_field, sort_dir=sort_dir)

    total_count = len(sorted_rows)
    total_pages = max(1, (total_count + per_page - 1) // per_page)
    offset = (page - 1) * per_page
    paginated_rows = sorted_rows[offset:offset + per_page]

    sum_topay = sum(to_float(r.get("Total To Pay")) for r in filtered_rows)
    sum_toreceive = sum(to_float(r.get("Total To Receive")) for r in filtered_rows)
    sum_pmade = sum(to_float(r.get("Payments Made")) for r in filtered_rows)
    sum_preceived = sum(to_float(r.get("Payments Received")) for r in filtered_rows)
    sum_balance = sum(to_float(r.get("Balance")) for r in filtered_rows)

    safe_rows = [{
        "customer_id": r.get("customer_id"),
        "Customer (City)": r.get("Customer (City)", ""),
        "Total To Pay": to_float(r.get("Total To Pay")),
        "Total To Receive": to_float(r.get("Total To Receive")),
        "Payments Made": to_float(r.get("Payments Made")),
        "Payments Received": to_float(r.get("Payments Received")),
        "Balance": to_float(r.get("Balance")),
    } for r in paginated_rows]

    return jsonify({
        "rows": safe_rows,
        "total": total_count,
        "total_pages": total_pages,
        "page": page,
        "sum_topay": float(sum_topay),
        "sum_toreceive": float(sum_toreceive),
        "sum_pmade": float(sum_pmade),
        "sum_preceived": float(sum_preceived),
        "sum_balance": float(sum_balance),
    })


# ── NEW: Customer Records API (for popup treeview) ────────────────
@bs_bp.route("/api/customer/<int:customer_id>/records", methods=["GET"])
def api_customer_records(customer_id):
    if "user" not in session:
        return jsonify({"error": "Unauthorized"}), 401

    conn = None
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("SELECT id, name, city FROM customer WHERE id = %s", (customer_id,))
            customer = cur.fetchone()
            if not customer:
                return jsonify({"error": "Customer not found"}), 404

            cur.execute("""
                SELECT
                    r.id,
                    r.customer_id,
                    c.name AS customer_name,
                    r.type,
                    r.detail,
                    r.weight,
                    r.rate,
                    r.total_amount,
                    r.reference,
                    DATE_FORMAT(r.date, '%%Y-%%m-%%d') AS record_date,
                    DATE_FORMAT(r.created_at, '%%d-%%m-%%Y %%H:%%i') AS created_at
                FROM records r
                JOIN customer c ON r.customer_id = c.id
                WHERE r.customer_id = %s
                ORDER BY r.created_at DESC, r.id DESC
            """, (customer_id,))
            records = cur.fetchall() or []

            for rec in records:
                for field in ("weight", "rate", "total_amount"):
                    if rec.get(field) is not None:
                        rec[field] = float(rec[field])

            total_to_pay = sum(
                float(r["total_amount"] or 0)
                for r in records
                if (r.get("type") or "").strip().lower() == "to pay"
            )
            total_to_receive = sum(
                float(r["total_amount"] or 0)
                for r in records
                if (r.get("type") or "").strip().lower() == "to receive"
            )

        return jsonify({
            "customer": {"id": customer["id"], "name": customer["name"], "city": customer.get("city", "")},
            "records": records,
            "total": len(records),
            "total_to_pay": total_to_pay,
            "total_to_receive": total_to_receive,
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        if conn:
            conn.close()


# ── NEW: Customer Payments API (for popup treeview) ───────────────
@bs_bp.route("/api/customer/<int:customer_id>/payments", methods=["GET"])
def api_customer_payments(customer_id):
    if "user" not in session:
        return jsonify({"error": "Unauthorized"}), 401

    conn = None
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("SELECT id, name, city FROM customer WHERE id = %s", (customer_id,))
            customer = cur.fetchone()
            if not customer:
                return jsonify({"error": "Customer not found"}), 404

            cur.execute("""
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
                WHERE p.customer_id = %s
                ORDER BY p.created_at DESC, p.id DESC
            """, (customer_id,))
            payments = cur.fetchall() or []

            for p in payments:
                if p.get("amount") is not None:
                    p["amount"] = float(p["amount"])

            total_paid = sum(
                float(p["amount"] or 0)
                for p in payments
                if (p.get("type") or "").strip().lower() == "paid"
            )
            total_received = sum(
                float(p["amount"] or 0)
                for p in payments
                if (p.get("type") or "").strip().lower() == "received"
            )

        return jsonify({
            "customer": {"id": customer["id"], "name": customer["name"], "city": customer.get("city", "")},
            "payments": payments,
            "total": len(payments),
            "total_paid": total_paid,
            "total_received": total_received,
            "net_balance": total_received - total_paid,
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        if conn:
            conn.close()


# ── NEW: Customer Records PDF ─────────────────────────────────────
@bs_bp.route("/bs/customer/<int:customer_id>/records/pdf")
def download_customer_records_pdf(customer_id):
    if "user" not in session:
        return redirect(url_for("login"))

    conn = None
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("SELECT id, name, city FROM customer WHERE id = %s", (customer_id,))
            customer = cur.fetchone()
            if not customer:
                flash("Customer not found", "error")
                return redirect(url_for("balance_sheet.balance_sheet"))

            cur.execute("""
                SELECT
                    r.type,
                    r.detail,
                    r.weight,
                    r.rate,
                    r.total_amount,
                    r.reference,
                    DATE_FORMAT(r.date, '%%Y-%%m-%%d') AS record_date,
                    DATE_FORMAT(r.created_at, '%%d-%%m-%%Y %%H:%%i') AS created_at
                FROM records r
                WHERE r.customer_id = %s
                ORDER BY r.type ASC, r.created_at DESC
            """, (customer_id,))
            records = cur.fetchall() or []

            for rec in records:
                for field in ("weight", "rate", "total_amount"):
                    if rec.get(field) is not None:
                        rec[field] = float(rec[field])

        total_to_pay = sum(
            float(r["total_amount"] or 0)
            for r in records
            if (r.get("type") or "").strip().lower() == "to pay"
        )
        total_to_receive = sum(
            float(r["total_amount"] or 0)
            for r in records
            if (r.get("type") or "").strip().lower() == "to receive"
        )

        from reportlab.lib.pagesizes import letter, landscape
        from reportlab.pdfgen import canvas as rl_canvas

        buffer = io.BytesIO()
        c = rl_canvas.Canvas(buffer, pagesize=landscape(letter))
        width, height = landscape(letter)

        c.setFont("Helvetica-Bold", 16)
        c.drawString(40, height - 45, "Awais Yarn Waste Broker")
        c.setFont("Helvetica-Bold", 12)
        customer_label = f"{customer['name']}" + (f" ({customer['city']})" if customer.get("city") else "")
        c.drawString(40, height - 65, f"Records for: {customer_label}")
        c.setFont("Helvetica", 9)
        c.drawString(40, height - 82, f"Generated: {datetime.datetime.now().strftime('%d %B %Y %H:%M')}")
        c.drawString(
            40,
            height - 96,
            f"Total Records: {len(records)}   To Pay: Rs. {total_to_pay:,.2f}   To Receive: Rs. {total_to_receive:,.2f}"
        )

        COL_X = [35, 55, 130, 300, 360, 415, 510, 585, 670]
        HEADERS = ["#", "Type", "Detail", "Weight", "Rate", "Amount", "Date", "Reference", "Created"]
        ROW_H = 18

        def draw_header_rec(y_pos):
            c.setFont("Helvetica-Bold", 9)
            for i, hdr in enumerate(HEADERS):
                c.drawString(COL_X[i] + 2, y_pos - 6, hdr)
            c.line(35, y_pos - ROW_H + 2, width - 20, y_pos - ROW_H + 2)
            return y_pos - ROW_H

        y = draw_header_rec(height - 112)
        c.setFont("Helvetica", 8)

        for idx, rec in enumerate(records, 1):
            if y - ROW_H < 50:
                c.showPage()
                y = draw_header_rec(height - 40)
                c.setFont("Helvetica", 8)

            row_vals = [
                str(idx),
                str(rec.get("type") or "")[:12],
                str(rec.get("detail") or "-")[:26],
                f"{rec['weight']:.3f}" if rec.get("weight") else "-",
                f"{rec['rate']:.2f}" if rec.get("rate") else "-",
                f"Rs. {rec['total_amount']:,.2f}" if rec.get("total_amount") else "-",
                str(rec.get("record_date") or "-"),
                str(rec.get("reference") or "-")[:16],
                str(rec.get("created_at") or "-"),
            ]
            text_y = y - 6
            for i, val in enumerate(row_vals):
                c.drawString(COL_X[i] + 2, text_y, val)
            c.line(35, y - ROW_H + 2, width - 20, y - ROW_H + 2)
            y -= ROW_H

        c.save()
        buffer.seek(0)

        safe_name = (customer["name"] or "customer").replace(" ", "_")[:30]
        return Response(
            buffer.getvalue(),
            mimetype="application/pdf",
            headers={"Content-Disposition": f"attachment; filename=records_{safe_name}.pdf"}
        )

    except Exception as e:
        flash(f"PDF generation failed: {str(e)}", "error")
        return redirect(url_for("balance_sheet.balance_sheet"))

    finally:
        if conn:
            conn.close()


# ── NEW: Customer Payments PDF ────────────────────────────────────
@bs_bp.route("/bs/customer/<int:customer_id>/payments/pdf")
def download_customer_payments_pdf(customer_id):
    if "user" not in session:
        return redirect(url_for("login"))

    conn = None
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("SELECT id, name, city FROM customer WHERE id = %s", (customer_id,))
            customer = cur.fetchone()
            if not customer:
                flash("Customer not found", "error")
                return redirect(url_for("balance_sheet.balance_sheet"))

            cur.execute("""
                SELECT
                    p.amount,
                    p.type,
                    p.mode,
                    p.bank_name,
                    p.reference,
                    DATE_FORMAT(p.date, '%%Y-%%m-%%d') AS payment_date,
                    DATE_FORMAT(p.created_at, '%%d-%%m-%%Y %%H:%%i') AS created_at
                FROM payments p
                WHERE p.customer_id = %s
                ORDER BY p.created_at DESC, p.id DESC
            """, (customer_id,))
            payments = cur.fetchall() or []

            for p in payments:
                if p.get("amount") is not None:
                    p["amount"] = float(p["amount"])

        total_paid = sum(
            float(p["amount"] or 0)
            for p in payments
            if (p.get("type") or "").strip().lower() == "paid"
        )
        total_received = sum(
            float(p["amount"] or 0)
            for p in payments
            if (p.get("type") or "").strip().lower() == "received"
        )

        from reportlab.lib.pagesizes import letter, landscape
        from reportlab.pdfgen import canvas as rl_canvas

        buffer = io.BytesIO()
        c = rl_canvas.Canvas(buffer, pagesize=landscape(letter))
        width, height = landscape(letter)

        c.setFont("Helvetica-Bold", 16)
        c.drawString(40, height - 45, "Awais Yarn Waste Broker")
        c.setFont("Helvetica-Bold", 12)
        customer_label = f"{customer['name']}" + (f" ({customer['city']})" if customer.get("city") else "")
        c.drawString(40, height - 65, f"Payments for: {customer_label}")
        c.setFont("Helvetica", 9)
        c.drawString(40, height - 82, f"Generated: {datetime.datetime.now().strftime('%d %B %Y %H:%M')}")
        c.drawString(
            40,
            height - 96,
            f"Total Transactions: {len(payments)}   Paid: Rs. {total_paid:,.2f}   "
            f"Received: Rs. {total_received:,.2f}   Net Balance: Rs. {total_received - total_paid:,.2f}"
        )

        COL_X = [40, 60, 155, 245, 320, 395, 480, 590]
        HEADERS = ["#", "Amount", "Type", "Mode", "Bank", "Reference", "Date", "Created"]
        ROW_H = 18

        def draw_header_pay(y_pos):
            c.setFont("Helvetica-Bold", 9)
            for i, hdr in enumerate(HEADERS):
                c.drawString(COL_X[i] + 2, y_pos - 6, hdr)
            c.line(40, y_pos - ROW_H + 2, width - 20, y_pos - ROW_H + 2)
            return y_pos - ROW_H

        y = draw_header_pay(height - 112)
        c.setFont("Helvetica", 8)

        for idx, p in enumerate(payments, 1):
            if y - ROW_H < 50:
                c.showPage()
                y = draw_header_pay(height - 40)
                c.setFont("Helvetica", 8)

            row_vals = [
                str(idx),
                f"Rs. {p['amount']:,.2f}" if p.get("amount") is not None else "-",
                str(p.get("type") or "-"),
                str(p.get("mode") or "-")[:14],
                str(p.get("bank_name") or "-")[:14],
                str(p.get("reference") or "-")[:16],
                str(p.get("payment_date") or "-"),
                str(p.get("created_at") or "-"),
            ]
            text_y = y - 6
            for i, val in enumerate(row_vals):
                c.drawString(COL_X[i] + 2, text_y, val)
            c.line(40, y - ROW_H + 2, width - 20, y - ROW_H + 2)
            y -= ROW_H

        c.save()
        buffer.seek(0)

        safe_name = (customer["name"] or "customer").replace(" ", "_")[:30]
        return Response(
            buffer.getvalue(),
            mimetype="application/pdf",
            headers={"Content-Disposition": f"attachment; filename=payments_{safe_name}.pdf"}
        )

    except Exception as e:
        flash(f"PDF generation failed: {str(e)}", "error")
        return redirect(url_for("balance_sheet.balance_sheet"))

    finally:
        if conn:
            conn.close()


# ── NEW: Customer A-Ledger PDF (Records + Payments combined) ──────
@bs_bp.route("/bs/customer/<int:customer_id>/ledger/pdf")
def download_customer_ledger_pdf(customer_id):
    if "user" not in session:
        return redirect(url_for("login"))

    conn = None
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("SELECT id, name, city FROM customer WHERE id = %s", (customer_id,))
            customer = cur.fetchone()
            if not customer:
                flash("Customer not found", "error")
                return redirect(url_for("balance_sheet.balance_sheet"))

            cur.execute("""
                SELECT
                    r.type,
                    r.detail,
                    r.weight,
                    r.rate,
                    r.total_amount,
                    r.reference,
                    DATE_FORMAT(r.date, '%%Y-%%m-%%d') AS record_date,
                    DATE_FORMAT(r.created_at, '%%d-%%m-%%Y %%H:%%i') AS created_at
                FROM records r
                WHERE r.customer_id = %s
                ORDER BY
                    CASE WHEN LOWER(TRIM(r.type)) = 'to pay' THEN 0 ELSE 1 END ASC,
                    r.created_at DESC
            """, (customer_id,))
            records = cur.fetchall() or []

            cur.execute("""
                SELECT
                    p.amount,
                    p.type,
                    p.mode,
                    p.bank_name,
                    p.reference,
                    DATE_FORMAT(p.date, '%%Y-%%m-%%d') AS payment_date,
                    DATE_FORMAT(p.created_at, '%%d-%%m-%%Y %%H:%%i') AS created_at
                FROM payments p
                WHERE p.customer_id = %s
                ORDER BY p.created_at DESC, p.id DESC
            """, (customer_id,))
            payments = cur.fetchall() or []

            for rec in records:
                for field in ("weight", "rate", "total_amount"):
                    if rec.get(field) is not None:
                        rec[field] = float(rec[field])

            for p in payments:
                if p.get("amount") is not None:
                    p["amount"] = float(p["amount"])

        total_to_pay = sum(
            float(r["total_amount"] or 0)
            for r in records
            if (r.get("type") or "").strip().lower() == "to pay"
        )
        total_to_receive = sum(
            float(r["total_amount"] or 0)
            for r in records
            if (r.get("type") or "").strip().lower() == "to receive"
        )
        total_paid = sum(
            float(p["amount"] or 0)
            for p in payments
            if (p.get("type") or "").strip().lower() == "paid"
        )
        total_received = sum(
            float(p["amount"] or 0)
            for p in payments
            if (p.get("type") or "").strip().lower() == "received"
        )

        net_balance = (total_to_receive - total_to_pay) - (total_received - total_paid)

        from reportlab.lib.pagesizes import letter, landscape
        from reportlab.pdfgen import canvas as rl_canvas

        buffer = io.BytesIO()
        c = rl_canvas.Canvas(buffer, pagesize=landscape(letter))
        width, height = landscape(letter)

        LEFT = 35
        RIGHT = width - 20
        ROW_H = 18

        customer_label = customer["name"] + (f" ({customer['city']})" if customer.get("city") else "")

        def draw_page_header(y_pos):
            c.setFont("Helvetica-Bold", 15)
            c.drawString(LEFT, y_pos, "Awais Yarn Waste Broker")
            y_pos -= 18
            c.setFont("Helvetica-Bold", 11)
            c.drawString(LEFT, y_pos, f"Account Ledger: {customer_label}")
            y_pos -= 14
            c.setFont("Helvetica", 8)
            c.drawString(LEFT, y_pos, f"Generated: {datetime.datetime.now().strftime('%d %B %Y %H:%M')}")
            y_pos -= 18
            return y_pos

        REC_COLS = [LEFT, LEFT + 20, LEFT + 95, LEFT + 265, LEFT + 325, LEFT + 380, LEFT + 475, LEFT + 550, LEFT + 635]
        REC_HEADS = ["#", "Type", "Detail", "Weight", "Rate", "Amount", "Date", "Reference", "Created"]

        def draw_rec_header(y_pos, label="Records"):
            c.setFont("Helvetica-Bold", 9)
            c.drawString(LEFT, y_pos, label)
            y_pos -= 14
            c.setFont("Helvetica-Bold", 8)
            for i, hdr in enumerate(REC_HEADS):
                c.drawString(REC_COLS[i] + 2, y_pos - 5, hdr)
            c.setLineWidth(0.5)
            c.line(LEFT, y_pos - ROW_H + 2, RIGHT, y_pos - ROW_H + 2)
            return y_pos - ROW_H

        PAY_COLS = [LEFT, LEFT + 20, LEFT + 115, LEFT + 195, LEFT + 275, LEFT + 355, LEFT + 435, LEFT + 545]
        PAY_HEADS = ["#", "Amount", "Type", "Mode", "Bank / Details", "Reference", "Date", "Created"]

        def draw_pay_header(y_pos, label="Payments"):
            c.setFont("Helvetica-Bold", 9)
            c.drawString(LEFT, y_pos, label)
            y_pos -= 14
            c.setFont("Helvetica-Bold", 8)
            for i, hdr in enumerate(PAY_HEADS):
                c.drawString(PAY_COLS[i] + 2, y_pos - 5, hdr)
            c.setLineWidth(0.5)
            c.line(LEFT, y_pos - ROW_H + 2, RIGHT, y_pos - ROW_H + 2)
            return y_pos - ROW_H

        y = draw_page_header(height - 40)

        c.setFont("Helvetica", 8)
        c.drawString(
            LEFT, y,
            f"Records: {len(records)}   To Pay: Rs. {total_to_pay:,.2f}   To Receive: Rs. {total_to_receive:,.2f}    "
            f"Payments: {len(payments)}   Paid: Rs. {total_paid:,.2f}   Received: Rs. {total_received:,.2f}"
        )
        y -= 20

        y = draw_rec_header(y, label="▌ RECORDS")
        c.setFont("Helvetica", 8)

        for idx, rec in enumerate(records, 1):
            if y - ROW_H < 50:
                c.showPage()
                c.setFont("Helvetica", 7)
                c.drawString(LEFT, height - 20, f"Account Ledger (cont.): {customer_label}")
                c.setFillColorRGB(0, 0, 0)
                y = draw_rec_header(height - 35, label="Records (continued)")
                c.setFont("Helvetica", 8)

            text_y = y - 5
            row_vals = [
                str(idx),
                str(rec.get("type") or "")[:12],
                str(rec.get("detail") or "-")[:26],
                f"{rec['weight']:.3f}" if rec.get("weight") else "-",
                f"{rec['rate']:.2f}" if rec.get("rate") else "-",
                f"Rs. {rec['total_amount']:,.2f}" if rec.get("total_amount") else "-",
                str(rec.get("record_date") or "-"),
                str(rec.get("reference") or "-")[:16],
                str(rec.get("created_at") or "-"),
            ]

            for i, val in enumerate(row_vals):
                c.setFillColorRGB(0, 0, 0)
                c.drawString(REC_COLS[i] + 2, text_y, val)

            c.setLineWidth(0.3)
            c.line(LEFT, y - ROW_H + 2, RIGHT, y - ROW_H + 2)
            y -= ROW_H

        y -= 10
        if y - 40 < 55:
            c.showPage()
            y = height - 40

        c.setLineWidth(2.5)
        c.line(LEFT, y, RIGHT, y)
        c.setLineWidth(1.0)
        c.line(LEFT, y - 4, RIGHT, y - 4)

        y -= 16
        y = draw_pay_header(y, label="▌ PAYMENTS")
        c.setFont("Helvetica", 8)

        for idx, p in enumerate(payments, 1):
            if y - ROW_H < 55:
                c.showPage()
                c.setFont("Helvetica", 7)
                c.drawString(LEFT, height - 20, f"Account Ledger (cont.): {customer_label}")
                c.setFillColorRGB(0, 0, 0)
                y = draw_pay_header(height - 35, label="Payments (continued)")
                c.setFont("Helvetica", 8)

            row_vals = [
                str(idx),
                f"Rs. {p['amount']:,.2f}" if p.get("amount") is not None else "-",
                str(p.get("type") or "-"),
                str(p.get("mode") or "-")[:14],
                str(p.get("bank_name") or "-")[:16],
                str(p.get("reference") or "-")[:16],
                str(p.get("payment_date") or "-"),
                str(p.get("created_at") or "-"),
            ]

            text_y = y - 5
            for i, val in enumerate(row_vals):
                c.setFillColorRGB(0, 0, 0)
                c.drawString(PAY_COLS[i] + 2, text_y, val)

            c.setLineWidth(0.3)
            c.line(LEFT, y - ROW_H + 2, RIGHT, y - ROW_H + 2)
            y -= ROW_H

        y -= 14
        if y - 50 < 40:
            c.showPage()
            y = height - 60

        c.setLineWidth(2.0)
        c.line(LEFT, y + 6, RIGHT, y + 6)
        c.setLineWidth(1)

        if net_balance > 0:
            bal_color = (0.05, 0.50, 0.30)
            bal_note = "(We will receive this amount)"
            bal_str = f"+ Rs. {net_balance:,.2f}"
        elif net_balance < 0:
            bal_color = (0.72, 0.07, 0.07)
            bal_note = "(We have to pay this amount)"
            bal_str = f"- Rs. {abs(net_balance):,.2f}"
        else:
            bal_color = (0.3, 0.3, 0.3)
            bal_note = "(Account Settled)"
            bal_str = "Rs. 0.00"

        c.setFont("Helvetica-Bold", 11)
        c.setFillColorRGB(0, 0, 0)
        c.drawString(LEFT, y - 8, "Net Balance:")

        c.setFillColorRGB(*bal_color)
        c.drawString(LEFT + 90, y - 8, bal_str)

        c.setFont("Helvetica", 9)
        c.setFillColorRGB(*bal_color)
        c.drawString(LEFT + 90 + c.stringWidth(bal_str, "Helvetica-Bold", 11) + 10, y - 8, bal_note)

        c.setFillColorRGB(0, 0, 0)
        c.save()
        buffer.seek(0)

        safe_name = (customer["name"] or "customer").replace(" ", "_")[:30]
        return Response(
            buffer.getvalue(),
            mimetype="application/pdf",
            headers={"Content-Disposition": f"attachment; filename=ledger_{safe_name}.pdf"}
        )

    except Exception as e:
        flash(f"Ledger PDF generation failed: {str(e)}", "error")
        return redirect(url_for("balance_sheet.balance_sheet"))

    finally:
        if conn:
            conn.close()


@bs_bp.route("/bs/download_pdf")
def download_bs_pdf():
    if "user" not in session:
        return redirect(url_for("login"))

    balance_filter = request.args.get("balance_filter", "all").strip().lower()
    sort_field = request.args.get("sort_field", "customer").strip().lower()
    sort_dir = request.args.get("sort_dir", "asc").strip().lower()

    try:
        rows = load_bs_cache()
        if not rows:
            rows = ensure_cache()
    except Exception as e:
        flash(f"PDF generation failed: {str(e)}", "error")
        return redirect(url_for("balance_sheet.balance_sheet"))

    filtered_rows = apply_bs_filters(rows)
    sorted_rows = sort_bs_rows(filtered_rows, sort_field=sort_field, sort_dir=sort_dir)

    agg_total_count = len(filtered_rows)
    agg_sum_topay = sum(to_float(r.get("Total To Pay")) for r in filtered_rows)
    agg_sum_toreceive = sum(to_float(r.get("Total To Receive")) for r in filtered_rows)
    agg_sum_pmade = sum(to_float(r.get("Payments Made")) for r in filtered_rows)
    agg_sum_preceived = sum(to_float(r.get("Payments Received")) for r in filtered_rows)
    agg_sum_balance = sum(to_float(r.get("Balance")) for r in filtered_rows)

    from reportlab.lib.pagesizes import letter, landscape
    from reportlab.pdfgen import canvas

    try:
        buffer = io.BytesIO()
        c = canvas.Canvas(buffer, pagesize=landscape(letter))
        width, height = landscape(letter)

        c.setFont("Helvetica-Bold", 18)
        c.drawString(40, height - 50, "Awais Yarn Waste Broker")
        c.setFont("Helvetica-Bold", 14)
        c.drawString(40, height - 75, "Balance Sheet")
        c.setFont("Helvetica", 10)
        c.drawString(40, height - 95, f"Generated: {datetime.datetime.now().strftime('%d %B %Y %H:%M')}")

        summary_line = (
            f"Particulars: {int(agg_total_count)}   "
            f"To Pay: {float(agg_sum_topay):,.2f}   "
            f"To Receive: {float(agg_sum_toreceive):,.2f}   "
            f"Pmts Made: {float(agg_sum_pmade):,.2f}   "
            f"Pmts Received: {float(agg_sum_preceived):,.2f}   "
            f"Net Balance: {float(agg_sum_balance):,.2f}"
        )
        c.drawString(40, height - 110, summary_line)

        search = request.args.get("search", "").strip()
        bal_lbl = {
            "all": "All",
            "positive": "We Owe (+)",
            "negative": "They Owe (-)",
            "zero": "Settled",
        }.get(balance_filter, "All")

        current_y = height - 130
        if search:
            c.drawString(40, current_y, f"Search: {search[:90]}")
            current_y -= 15
        c.drawString(40, current_y, f"Balance Filter: {bal_lbl}")
        current_y -= 35

        COL_X = [40, 80, 310, 395, 480, 565, 650]
        COL_W = [40, 230, 85, 85, 85, 85, 102]
        HEADERS = ["#", "Particular (City)", "To Pay", "To Receive", "Pmts Made", "Pmts Received", "Balance"]
        ROW_HEIGHT = 20

        def draw_table_header(y_pos):
            c.setFont("Helvetica-Bold", 10)
            text_y = y_pos - 7
            for i, txt in enumerate(HEADERS):
                if i in (0, 1):
                    c.drawString(COL_X[i] + 5, text_y, txt)
                else:
                    c.drawRightString(COL_X[i] + COL_W[i] - 5, text_y, txt)
            c.line(COL_X[0], y_pos - ROW_HEIGHT + 2, COL_X[-1] + COL_W[-1], y_pos - ROW_HEIGHT + 2)
            return y_pos - ROW_HEIGHT

        y = draw_table_header(current_y)
        c.setFont("Helvetica", 9)

        for idx, r in enumerate(sorted_rows, start=1):
            if y - ROW_HEIGHT < 55:
                c.showPage()
                y = draw_table_header(height - 50)
                c.setFont("Helvetica", 9)

            cust = str(r.get("Customer (City)", "") or "")[:38]
            topay = f"{to_float(r.get('Total To Pay')):,.2f}"
            toreceive = f"{to_float(r.get('Total To Receive')):,.2f}"
            pmade = f"{to_float(r.get('Payments Made')):,.2f}"
            preceived = f"{to_float(r.get('Payments Received')):,.2f}"
            bal = f"{to_float(r.get('Balance')):,.2f}"

            row_data = [str(idx), cust, topay, toreceive, pmade, preceived, bal]
            text_y = y - 7
            for i, txt in enumerate(row_data):
                if i in (0, 1):
                    c.drawString(COL_X[i] + 5, text_y, txt)
                else:
                    c.drawRightString(COL_X[i] + COL_W[i] - 5, text_y, txt)

            line_y = y - ROW_HEIGHT + 2
            c.line(COL_X[0], line_y, COL_X[-1] + COL_W[-1], line_y)
            y -= ROW_HEIGHT

        c.save()
        buffer.seek(0)

        now_fn = datetime.datetime.now().strftime("%Y%m%d_%H%M")
        return Response(
            buffer.getvalue(),
            mimetype="application/pdf",
            headers={"Content-Disposition": f"attachment; filename=balance_sheet_{now_fn}.pdf"}
        )

    except Exception as e:
        flash(f"PDF generation failed: {str(e)}", "error")
        return redirect(url_for("balance_sheet.balance_sheet"))