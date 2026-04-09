from flask import (
    Flask, render_template, request, redirect, url_for,
    session, flash, jsonify, Response
)
import pymysql
import io
import os
import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "awaisyarnwastebroker032696666692")

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

# Optional TiDB SSL certificate
CA_FILE = os.environ.get("DB_CA_FILE", str(BASE_DIR / "ca.pem"))
if CA_FILE and os.path.exists(CA_FILE):
    DB_CONFIG["ssl"] = {"ca": CA_FILE}


def get_db_connection():
    return pymysql.connect(**DB_CONFIG)


def check_tidb_connection():
    try:
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT DATABASE() AS db;")
                result = cursor.fetchone()
            return True, result["db"] if result else "Connected"
        finally:
            conn.close()
    except Exception as e:
        return False, f"Connection failed: {str(e)}"


def format_backup_date(value):
    if value is None:
        return "No backup found"

    if isinstance(value, datetime.datetime):
        return value.strftime("%d %b %Y, %I:%M %p")

    if isinstance(value, datetime.date):
        return value.strftime("%d %b %Y")

    return str(value)


def get_first_backup_created():
    """
    Reads the latest 'dated' value from the backup table.
    """
    try:
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT dated FROM `backup` ORDER BY dated DESC LIMIT 1")
                row = cursor.fetchone()

            if row and row.get("dated") is not None:
                return format_backup_date(row["dated"])
        finally:
            conn.close()
    except Exception:
        pass

    return "No backup found"


def update_backup_date(now):
    """
    Updates the existing backup date in the same row.
    If the table has no row, it inserts one.
    """
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) AS count FROM `backup`")
            row = cursor.fetchone()
            count = row["count"] if row else 0

            if count > 0:
                cursor.execute(
                    "UPDATE `backup` SET `dated`=%s ORDER BY `dated` DESC LIMIT 1",
                    (now,)
                )
            else:
                cursor.execute(
                    "INSERT INTO `backup` (`dated`) VALUES (%s)",
                    (now,)
                )

        conn.commit()
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


# ==================== IMPORT BLUEPRINTS ====================
from addparticular import addparticular_bp
app.register_blueprint(addparticular_bp)

from payments import payment_bp
app.register_blueprint(payment_bp)

from bs import bs_bp
app.register_blueprint(bs_bp)

from records import record_bp
app.register_blueprint(record_bp)

from notes import notes_bp
app.register_blueprint(notes_bp)


# ==================== LOGIN ====================
@app.route("/", methods=["GET", "POST"])
def login():
    error = None
    db_connected, db_message = check_tidb_connection()

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()

        if not db_connected:
            error = "Database not connected!"
        else:
            try:
                conn = get_db_connection()
                try:
                    with conn.cursor() as cursor:
                        cursor.execute(
                            "SELECT * FROM users WHERE username=%s LIMIT 1",
                            (username,)
                        )
                        user = cursor.fetchone()
                finally:
                    conn.close()

                if user and user["password"] == password:
                    session["user"] = username
                    return redirect(url_for("dashboard"))
                else:
                    error = "Invalid username or password!"
            except Exception as e:
                error = f"DB Error: {str(e)}"

    return render_template(
        "login.html",
        error=error,
        db_connected=db_connected,
        db_message=db_message
    )


# ==================== DASHBOARD ====================
@app.route("/dashboard")
def dashboard():
    if "user" not in session:
        return redirect(url_for("login"))

    shop_name = "Awais Yarn Waste Broker"
    total_records = 0
    total_particulars = 0
    last_backup_created = get_first_backup_created()

    try:
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) AS count FROM records")
                total_records = cursor.fetchone()["count"]

                cursor.execute("SELECT COUNT(*) AS count FROM customer")
                total_particulars = cursor.fetchone()["count"]
        finally:
            conn.close()
    except Exception:
        pass

    return render_template(
        "dashboard.html",
        shop_name=shop_name,
        total_records=total_records,
        total_particulars=total_particulars,
        last_backup_created=last_backup_created
    )


# ==================== LOGOUT ====================
@app.route("/logout")
def logout():
    session.pop("user", None)
    return redirect(url_for("login"))


# ==================== OTHER ROUTES ====================
@app.route("/rm")
def records_management():
    if "user" not in session:
        return redirect(url_for("login"))
    return render_template("rm.html")


@app.route("/bs")
def balance_sheet():
    if "user" not in session:
        return redirect(url_for("login"))
    return render_template("bs.html")


@app.route("/pb")
def notes():
    if "user" not in session:
        return redirect(url_for("login"))
    return render_template("notes.html")


@app.route("/settings")
def settings():
    if "user" not in session:
        return redirect(url_for("login"))
    return render_template("settings.html", shop_name="Awais Yarn Waste Broker")


# ==================== CHANGE CREDENTIALS ====================
@app.route("/change_credentials", methods=["POST"])
def change_credentials():
    if "user" not in session:
        return jsonify({"success": False, "message": "Not authenticated"}), 401

    data = request.get_json(silent=True) or {}

    current_username = data.get("current_username", "").strip()
    current_password = data.get("current_password", "").strip()
    new_username = data.get("new_username", "").strip()
    new_password = data.get("new_password", "").strip()
    confirm_password = data.get("confirm_password", "").strip()

    if not current_username or not current_password:
        return jsonify({
            "success": False,
            "message": "Current username and password are required."
        })

    if not new_username and not new_password:
        return jsonify({
            "success": False,
            "message": "Please provide a new username or a new password."
        })

    if new_password and new_password != confirm_password:
        return jsonify({
            "success": False,
            "message": "New passwords do not match."
        })

    if new_password and len(new_password) < 6:
        return jsonify({
            "success": False,
            "message": "New password must be at least 6 characters."
        })

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT * FROM users WHERE username=%s AND password=%s LIMIT 1",
                (current_username, current_password)
            )
            user = cursor.fetchone()

            if not user:
                return jsonify({
                    "success": False,
                    "message": "Current username or password is incorrect."
                })

            if new_username and new_username != current_username:
                cursor.execute(
                    "SELECT id FROM users WHERE username=%s LIMIT 1",
                    (new_username,)
                )
                existing = cursor.fetchone()
                if existing:
                    return jsonify({
                        "success": False,
                        "message": "That username is already taken."
                    })

            updates = []
            values = []

            if new_username and new_username != current_username:
                updates.append("username=%s")
                values.append(new_username)

            if new_password:
                updates.append("password=%s")
                values.append(new_password)

            if not updates:
                return jsonify({
                    "success": False,
                    "message": "No changes detected."
                })

            values.append(current_username)
            sql = f"UPDATE users SET {', '.join(updates)} WHERE username=%s"
            cursor.execute(sql, tuple(values))
            conn.commit()

        if new_username and new_username != current_username:
            session["user"] = new_username

        return jsonify({
            "success": True,
            "message": "Credentials updated successfully!"
        })

    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({
            "success": False,
            "message": f"Database error: {str(e)}"
        })
    finally:
        if conn:
            conn.close()


# ==================== DOWNLOAD BACKUP ====================
@app.route("/download_backup")
def download_backup():
    if "user" not in session:
        return redirect(url_for("login"))

    conn = None
    try:
        conn = get_db_connection()
        output = io.StringIO()
        now = datetime.datetime.now()
        now_text = now.strftime("%Y-%m-%d %H:%M:%S")

        try:
            update_backup_date(now)
        except Exception:
            pass

        output.write("-- ============================================================\n")
        output.write("-- Database Backup: awaisyarnbroker\n")
        output.write(f"-- Generated: {now_text}\n")
        output.write("-- By: Awais Yarn Waste Broker System\n")
        output.write("-- ============================================================\n\n")
        output.write("SET FOREIGN_KEY_CHECKS=0;\n\n")

        with conn.cursor() as cursor:
            cursor.execute("SHOW TABLES;")
            tables = cursor.fetchall()
            table_names = [list(t.values())[0] for t in tables]

            for table in table_names:
                output.write("-- ------------------------------------------------------------\n")
                output.write(f"-- Table: `{table}`\n")
                output.write("-- ------------------------------------------------------------\n\n")

                cursor.execute(f"SHOW CREATE TABLE `{table}`;")
                create_result = cursor.fetchone()
                create_sql = create_result.get("Create Table") if create_result else None

                if create_sql:
                    output.write(f"DROP TABLE IF EXISTS `{table}`;\n")
                    output.write(f"{create_sql};\n\n")

                cursor.execute(f"SELECT * FROM `{table}`;")
                rows = cursor.fetchall()

                if rows:
                    columns = list(rows[0].keys())
                    col_str = ", ".join(f"`{c}`" for c in columns)

                    for row in rows:
                        values = []
                        for val in row.values():
                            if val is None:
                                values.append("NULL")
                            elif isinstance(val, (int, float)):
                                values.append(str(val))
                            elif isinstance(val, (datetime.datetime, datetime.date)):
                                values.append(f"'{val}'")
                            else:
                                escaped = str(val).replace("\\", "\\\\").replace("'", "\\'")
                                values.append(f"'{escaped}'")

                        val_str = ", ".join(values)
                        output.write(f"INSERT INTO `{table}` ({col_str}) VALUES ({val_str});\n")
                    output.write("\n")
                else:
                    output.write(f"-- No data in `{table}`\n\n")

        output.write("SET FOREIGN_KEY_CHECKS=1;\n")
        output.write("\n-- Backup complete.\n")

        filename = f"awaisyarnbroker_backup_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.sql"
        return Response(
            output.getvalue(),
            mimetype="application/octet-stream",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )

    except Exception as e:
        flash(f"Backup failed: {str(e)}", "error")
        return redirect(url_for("settings"))

    finally:
        if conn is not None:
            conn.close()


# ==================== RUN ====================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    debug_mode = os.environ.get("FLASK_DEBUG", "0") == "1"
    print(f"Server running on 0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port, debug=debug_mode)