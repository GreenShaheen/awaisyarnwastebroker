from flask import Blueprint, render_template, request, redirect, url_for, flash, session
import os
import json
from pathlib import Path

notes_bp = Blueprint("notes", __name__, template_folder="templates")

BASE_DIR = Path(__file__).resolve().parent
NOTES_FILE = BASE_DIR / "notes.json"


def load_notes():
    """Load existing notes from JSON file. Returns empty string if file missing or invalid."""
    if not NOTES_FILE.exists():
        return ""
    try:
        with open(NOTES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                return data.get("content", "")
            return ""
    except Exception:
        return ""


def save_notes(content):
    """Overwrite notes.json with new content."""
    tmp_file = NOTES_FILE.with_suffix(".json.tmp")
    with open(tmp_file, "w", encoding="utf-8") as f:
        json.dump({"content": content}, f, ensure_ascii=False, indent=2)
    os.replace(tmp_file, NOTES_FILE)


@notes_bp.route("/pb", methods=["GET", "POST"])
def notes():
    if "user" not in session:
        return redirect(url_for("login"))

    if request.method == "POST":
        new_content = request.form.get("notes_content", "").strip()
        try:
            save_notes(new_content)
            flash("✅ Notes saved successfully!", "success")
        except Exception as e:
            flash(f"❌ Error saving notes: {str(e)}", "error")
        return redirect(url_for("notes.notes"))

    current_notes = load_notes()
    return render_template("notes.html", notes=current_notes)