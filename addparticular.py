from flask import (
    Blueprint, render_template, request, redirect, url_for,
    flash, jsonify, Response, session
)
import pymysql
import io
import os
import datetime
from pathlib import Path
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

addparticular_bp = Blueprint("addparticular", __name__, template_folder="templates")

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


def get_db_connection():
    return pymysql.connect(**DB_CONFIG)


# ==================== MAIN ROUTE (ADD / UPDATE) ====================
@addparticular_bp.route("/addparticular", methods=["GET", "POST"])
def addparticular():
    if "user" not in session:
        return redirect(url_for("login"))

    if request.method == "POST":
        conn = None
        try:
            customer_id = request.form.get("id", "").strip()
            name = request.form.get("name", "").strip()
            city = request.form.get("city", "").strip()
            phone = request.form.get("phone", "").strip()

            if not name:
                flash("❌ Name is required!", "error")
                return redirect(url_for("addparticular.addparticular"))

            conn = get_db_connection()
            with conn.cursor() as cursor:
                if customer_id:
                    sql = "UPDATE customer SET name=%s, city=%s, phone=%s WHERE id=%s"
                    cursor.execute(sql, (name, city, phone, customer_id))
                    flash("✅ Particular updated successfully!", "success")
                else:
                    sql = "INSERT INTO customer (name, city, phone) VALUES (%s, %s, %s)"
                    cursor.execute(sql, (name, city, phone))
                    flash("✅ Particular added successfully!", "success")

            conn.commit()

        except Exception as e:
            if conn:
                conn.rollback()
            flash(f"❌ Error: {str(e)}", "error")

        finally:
            if conn:
                conn.close()

        return redirect(url_for("addparticular.addparticular"))

    return render_template("addparticular.html")


# ==================== API - GET CUSTOMERS (SEARCH + PAGINATION) ====================
@addparticular_bp.route("/api/customers", methods=["GET"])
def get_customers():
    if "user" not in session:
        return jsonify({"error": "Unauthorized"}), 401

    search = request.args.get("search", "").strip()

    try:
        page = int(request.args.get("page", 1))
        if page < 1:
            page = 1
    except ValueError:
        page = 1

    per_page = 5
    offset = (page - 1) * per_page

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            if search:
                count_sql = """
                    SELECT COUNT(*) AS total
                    FROM customer
                    WHERE name LIKE %s OR city LIKE %s OR phone LIKE %s
                """
                cursor.execute(count_sql, (f"%{search}%", f"%{search}%", f"%{search}%"))
            else:
                cursor.execute("SELECT COUNT(*) AS total FROM customer")

            total_row = cursor.fetchone() or {"total": 0}
            total = total_row["total"]
            total_pages = (total + per_page - 1) // per_page if total else 0

            if search:
                sql = """
                    SELECT id, name, city, phone
                    FROM customer
                    WHERE name LIKE %s OR city LIKE %s OR phone LIKE %s
                    ORDER BY id DESC
                    LIMIT %s OFFSET %s
                """
                cursor.execute(
                    sql,
                    (f"%{search}%", f"%{search}%", f"%{search}%", per_page, offset)
                )
            else:
                sql = """
                    SELECT id, name, city, phone
                    FROM customer
                    ORDER BY id DESC
                    LIMIT %s OFFSET %s
                """
                cursor.execute(sql, (per_page, offset))

            customers = cursor.fetchall()

        return jsonify({
            "customers": customers,
            "page": page,
            "total_pages": total_pages,
            "total": total
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        if conn:
            conn.close()


# ==================== API - UPDATE ====================
@addparticular_bp.route("/api/customers/<int:customer_id>", methods=["PUT"])
def update_customer(customer_id):
    if "user" not in session:
        return jsonify({"success": False, "message": "Unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    name = data.get("name", "").strip()
    city = data.get("city", "").strip()
    phone = data.get("phone", "").strip()

    if not name:
        return jsonify({"success": False, "message": "Name is required"}), 400

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            sql = "UPDATE customer SET name=%s, city=%s, phone=%s WHERE id=%s"
            cursor.execute(sql, (name, city, phone, customer_id))
        conn.commit()
        return jsonify({"success": True, "message": "Updated successfully!"})

    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "message": str(e)}), 500

    finally:
        if conn:
            conn.close()


# ==================== API - DELETE ====================
@addparticular_bp.route("/api/customers/<int:customer_id>", methods=["DELETE"])
def delete_customer(customer_id):
    if "user" not in session:
        return jsonify({"success": False, "message": "Unauthorized"}), 401

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM customer WHERE id=%s", (customer_id,))
        conn.commit()
        return jsonify({"success": True, "message": "Deleted successfully!"})

    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "message": str(e)}), 500

    finally:
        if conn:
            conn.close()


# ==================== DOWNLOAD PDF (REPORTLAB) ====================
@addparticular_bp.route("/addparticular/download_pdf")
def download_customers_pdf():
    if "user" not in session:
        return redirect(url_for("login"))

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("SELECT name, city, phone FROM customer ORDER BY name")
            customers = cursor.fetchall()

        buffer = io.BytesIO()
        c = canvas.Canvas(buffer, pagesize=letter)
        width, height = letter

        c.setFont("Helvetica-Bold", 18)
        c.drawString(50, height - 60, "Awais Yarn Waste Broker")

        c.setFont("Helvetica-Bold", 14)
        c.drawString(50, height - 85, "Customers / Particulars List")

        c.setFont("Helvetica", 10)
        c.drawString(
            50,
            height - 110,
            f"Generated: {datetime.datetime.now().strftime('%d %B %Y %H:%M')}"
        )

        y = height - 150
        c.setFont("Helvetica-Bold", 12)
        c.drawString(50, y, "Name")
        c.drawString(250, y, "City")
        c.drawString(400, y, "Phone")
        y -= 20
        c.line(40, y + 10, width - 40, y + 10)

        c.setFont("Helvetica", 11)
        for cust in customers:
            if y < 60:
                c.showPage()
                y = height - 60
                c.setFont("Helvetica-Bold", 12)
                c.drawString(50, y, "Name")
                c.drawString(250, y, "City")
                c.drawString(400, y, "Phone")
                y -= 20
                c.line(40, y + 10, width - 40, y + 10)
                c.setFont("Helvetica", 11)

            c.drawString(50, y, str(cust.get("name") or ""))
            c.drawString(250, y, str(cust.get("city") or ""))
            c.drawString(400, y, str(cust.get("phone") or ""))
            y -= 18

        c.save()
        buffer.seek(0)

        return Response(
            buffer.getvalue(),
            mimetype="application/pdf",
            headers={"Content-Disposition": "attachment; filename=customers_list.pdf"}
        )

    except Exception as e:
        flash(f"PDF generation failed: {str(e)}", "error")
        return redirect(url_for("addparticular.addparticular"))

    finally:
        if conn:
            conn.close()