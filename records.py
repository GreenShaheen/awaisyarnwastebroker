from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, Response, session
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

record_bp = Blueprint("record", __name__, template_folder="templates")

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

# ==================== CACHE FILES ====================
ALL_CUSTOMERS_FILE = BASE_DIR / "allcustomers.json"
ALL_RECORDS_FILE = BASE_DIR / "records.json"


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
                LOWER(TRIM(r.type)) LIKE %s OR
                LOWER(TRIM(r.detail)) LIKE %s OR
                LOWER(TRIM(r.reference)) LIKE %s
            )
            """
        )
        params.extend([contains, contains, contains, contains])

    if from_date and to_date:
        where_clauses.append("DATE(r.created_at) BETWEEN %s AND %s")
        params.extend([from_date, to_date])
    elif from_date:
        where_clauses.append("DATE(r.created_at) >= %s")
        params.append(from_date)
    elif to_date:
        where_clauses.append("DATE(r.created_at) <= %s")
        params.append(to_date)

    if type_filter in {"to pay", "to receive"}:
        where_clauses.append("LOWER(TRIM(r.type)) = %s")
        params.append(type_filter)

    where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    return where_sql, params, from_date, to_date, type_filter


def fetch_summary(cursor, where_sql, params):
    cursor.execute(
        f"""
        SELECT
            COUNT(*) AS total_count,
            COALESCE(SUM(CASE WHEN LOWER(TRIM(r.type)) = 'to pay' THEN r.total_amount ELSE 0 END), 0) AS total_to_pay,
            COALESCE(SUM(CASE WHEN LOWER(TRIM(r.type)) = 'to receive' THEN r.total_amount ELSE 0 END), 0) AS total_to_receive
        FROM records r
        JOIN customer c ON r.customer_id = c.id
        {where_sql}
        """,
        params,
    )

    row = cursor.fetchone() or {}
    total_count = int(row.get("total_count", 0) or 0)
    total_to_pay = float(row.get("total_to_pay", 0) or 0)
    total_to_receive = float(row.get("total_to_receive", 0) or 0)
    current_balance = total_to_receive - total_to_pay

    return total_count, total_to_pay, total_to_receive, current_balance


@record_bp.route("/rm", methods=["GET", "POST"])
def record():
    if "user" not in session:
        return redirect(url_for("login"))

    if request.method == "POST":
        conn = None
        try:
            record_id = request.form.get("id")
            customer_id = request.form.get("customer_id")
            rtype = request.form.get("type")
            detail = request.form.get("detail")
            weight = request.form.get("weight") or None
            rate = request.form.get("rate") or None
            total_amount = request.form.get("total_amount")
            reference = request.form.get("reference")
            date_str = request.form.get("date")

            if not customer_id or not rtype or not total_amount:
                flash("❌ Customer, Type and Total Amount are required!", "error")
                return redirect(url_for("record.record"))

            total_amount = float(total_amount)
            weight = float(weight) if weight else None
            rate = float(rate) if rate else None

            conn = get_db_connection()
            with conn.cursor() as cursor:
                if record_id:
                    cursor.execute(
                        """
                        UPDATE records
                        SET customer_id=%s, date=%s, detail=%s, weight=%s,
                            rate=%s, type=%s, total_amount=%s, reference=%s
                        WHERE id=%s
                        """,
                        (customer_id, date_str, detail, weight, rate, rtype, total_amount, reference, record_id),
                    )
                    flash("✅ Record updated successfully!", "success")
                else:
                    cursor.execute(
                        """
                        INSERT INTO records
                        (customer_id, date, detail, weight, rate, type, total_amount, reference)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (customer_id, date_str, detail, weight, rate, rtype, total_amount, reference),
                    )
                    flash("✅ Record added successfully!", "success")

            conn.commit()

        except Exception as e:
            if conn:
                conn.rollback()
            flash(f"❌ Error: {str(e)}", "error")

        finally:
            if conn:
                conn.close()

        return redirect(url_for("record.record"))

    return render_template("rm.html")


@record_bp.route("/api/records/customers/load-temp", methods=["POST"])
def load_customers_to_temp():
    if "user" not in session:
        return jsonify({"error": "Unauthorized"}), 401

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT id, name, city, phone
                FROM customer
                ORDER BY LOWER(TRIM(name)) ASC
            """)
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


@record_bp.route("/api/records/load-temp-records", methods=["POST"])
def load_records_to_temp():
    if "user" not in session:
        return jsonify({"error": "Unauthorized"}), 401

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT
                    r.id,
                    c.name AS customer_name,
                    r.detail,
                    r.weight,
                    r.rate,
                    r.type,
                    r.total_amount,
                    r.reference,
                    DATE_FORMAT(r.date, '%%Y-%%m-%%d') AS record_date,
                    DATE_FORMAT(r.created_at, '%%d-%%m-%%Y %%H:%%i') AS created_at
                FROM records r
                JOIN customer c ON r.customer_id = c.id
                ORDER BY r.created_at DESC, r.id DESC
            """)
            records = cursor.fetchall()

            for rec in records:
                for field in ("weight", "rate", "total_amount"):
                    if rec.get(field) is not None:
                        rec[field] = float(rec[field])

        tmp_file = ALL_RECORDS_FILE.with_suffix(".json.tmp")
        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)
        os.replace(tmp_file, ALL_RECORDS_FILE)

        return jsonify({"success": True, "records_count": len(records)})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        if conn:
            conn.close()


@record_bp.route("/api/records/customers", methods=["GET"])
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


@record_bp.route("/api/records", methods=["GET"])
def get_records():
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
            total, total_to_pay, total_to_receive, current_balance = fetch_summary(cursor, where_sql, params)

            cursor.execute(
                f"""
                SELECT
                    r.id,
                    r.customer_id,
                    c.name AS customer_name,
                    r.detail,
                    r.weight,
                    r.rate,
                    r.type,
                    r.total_amount,
                    r.reference,
                    DATE_FORMAT(r.date, '%%Y-%%m-%%d') AS record_date,
                    DATE_FORMAT(r.created_at, '%%d-%%m-%%Y %%H:%%i') AS created_at
                FROM records r
                JOIN customer c ON r.customer_id = c.id
                {where_sql}
                ORDER BY r.created_at DESC, r.id DESC
                LIMIT %s OFFSET %s
                """,
                params + [per_page, offset],
            )

            records = cursor.fetchall()

            for rec in records:
                for field in ("weight", "rate", "total_amount"):
                    if rec.get(field) is not None:
                        rec[field] = float(rec[field])

            total_pages = max(1, (total + per_page - 1) // per_page)

        return jsonify({
            "records": records,
            "page": page,
            "total_pages": total_pages,
            "total": total,
            "total_to_pay": total_to_pay,
            "total_to_receive": total_to_receive,
            "current_balance": current_balance,
            "from_date": from_date,
            "to_date": to_date,
            "type_filter": type_filter,
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        if conn:
            conn.close()


@record_bp.route("/api/records/<int:record_id>", methods=["DELETE"])
def delete_record(record_id):
    if "user" not in session:
        return jsonify({"success": False, "message": "Unauthorized"}), 401

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM records WHERE id=%s", (record_id,))
        conn.commit()
        return jsonify({"success": True, "message": "Record deleted successfully!"})

    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "message": str(e)}), 500

    finally:
        if conn:
            conn.close()


@record_bp.route("/api/records/<int:record_id>", methods=["PUT"])
def update_record(record_id):
    if "user" not in session:
        return jsonify({"success": False, "message": "Unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    customer_id = data.get("customer_id")
    rtype = data.get("type")
    detail = data.get("detail")
    weight = data.get("weight")
    rate = data.get("rate")
    total_amount = data.get("total_amount")
    reference = data.get("reference")
    date_str = data.get("date")

    if not customer_id or total_amount is None or not rtype:
        return jsonify({"success": False, "message": "Customer, Type and Total Amount are required"}), 400

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE records
                SET customer_id=%s, date=%s, detail=%s, weight=%s,
                    rate=%s, type=%s, total_amount=%s, reference=%s
                WHERE id=%s
                """,
                (
                    customer_id,
                    date_str,
                    detail,
                    float(weight) if weight not in (None, "") else None,
                    float(rate) if rate not in (None, "") else None,
                    rtype,
                    float(total_amount),
                    reference,
                    record_id,
                ),
            )
        conn.commit()
        return jsonify({"success": True, "message": "Record updated successfully!"})

    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "message": str(e)}), 500

    finally:
        if conn:
            conn.close()


@record_bp.route("/rm/download_pdf")
def download_records_pdf():
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
                    r.detail,
                    r.weight,
                    r.rate,
                    r.type,
                    r.total_amount,
                    r.reference,
                    r.date,
                    r.created_at
                FROM records r
                JOIN customer c ON r.customer_id = c.id
                {where_sql}
                ORDER BY r.created_at DESC, r.id DESC
                """,
                params,
            )

            records = cursor.fetchall()
            total, total_to_pay, total_to_receive, current_balance = fetch_summary(cursor, where_sql, params)

        buffer = io.BytesIO()
        c = canvas.Canvas(buffer, pagesize=letter)
        width, height = letter

        c.setFont("Helvetica-Bold", 18)
        c.drawString(50, height - 50, "Awais Yarn Waste Broker")
        c.setFont("Helvetica-Bold", 14)
        c.drawString(50, height - 75, f"Records List - {type_filter.upper() if type_filter else 'ALL'}")
        c.setFont("Helvetica", 10)
        c.drawString(50, height - 95, f"Generated: {datetime.datetime.now().strftime('%d %B %Y %H:%M')}")

        summary_line = (
            f"Records: {total}   To Pay: {total_to_pay:.2f}   "
            f"To Receive: {total_to_receive:.2f}   Balance: {current_balance:.2f}"
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
            c.setFont("Helvetica-Bold", 9)
            c.drawString(35, y_pos, "#")
            c.drawString(52, y_pos, "Customer")
            c.drawString(170, y_pos, "Detail")
            c.drawString(280, y_pos, "Weight")
            c.drawString(330, y_pos, "Rate")
            c.drawString(375, y_pos, "Type")
            c.drawString(445, y_pos, "Amount")
            c.drawString(505, y_pos, "Ref")
            c.drawString(555, y_pos, "Date")
            y_pos -= 10
            c.line(30, y_pos, width - 30, y_pos)
            return y_pos - 14

        start_y = height - 180 if type_filter else height - 165
        y = draw_header(start_y)
        c.setFont("Helvetica", 8)

        for idx, rec in enumerate(records, start=1):
            if y < 55:
                c.showPage()
                y = draw_header(height - 50)
                c.setFont("Helvetica", 8)

            ref_text = str(rec["reference"] or "")
            if len(ref_text) > 10:
                ref_text = ref_text[:10] + "..."

            detail_text = str(rec["detail"] or "")
            if len(detail_text) > 14:
                detail_text = detail_text[:14] + "..."

            c.drawString(35, y, str(idx))
            c.drawString(52, y, str(rec["name"] or "")[:18])
            c.drawString(170, y, detail_text)
            c.drawString(280, y, f"{float(rec['weight'] or 0):.2f}" if rec["weight"] else "-")
            c.drawString(330, y, f"{float(rec['rate'] or 0):.2f}" if rec["rate"] else "-")
            c.drawString(375, y, str(rec["type"] or "")[:12])
            c.drawString(445, y, f"{float(rec['total_amount'] or 0):.2f}")
            c.drawString(505, y, ref_text)

            date_val = rec["date"]
            if isinstance(date_val, (datetime.date, datetime.datetime)):
                date_text = date_val.strftime("%d-%m-%Y")
            else:
                date_text = str(date_val or "-")
            c.drawString(555, y, date_text)
            y -= 15

        c.save()
        buffer.seek(0)

        return Response(
            buffer.getvalue(),
            mimetype="application/pdf",
            headers={"Content-Disposition": f"attachment; filename=records_{type_filter or 'all'}.pdf"},
        )

    except Exception as e:
        flash(f"PDF generation failed: {str(e)}", "error")
        return redirect(url_for("record.record"))

    finally:
        if conn:
            conn.close()