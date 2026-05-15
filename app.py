import json
from copy import deepcopy
from datetime import datetime
from os import getenv
from pathlib import Path
from secrets import compare_digest
from threading import Lock, Thread

from dotenv import load_dotenv
from flask import Flask, Response, jsonify, redirect, render_template, request, url_for

from analytics import CUSTOM_PERIOD_KEY, build_period_data, empty_dashboard


load_dotenv()
app = Flask(__name__)
BASE_DIR = Path(__file__).resolve().parent
CACHE_PATH = Path(getenv("CACHE_PATH", "") or BASE_DIR / "dashboard_cache.json")
APP_USERNAME = getenv("APP_USERNAME", "").strip()
APP_PASSWORD = getenv("APP_PASSWORD", "").strip()
APP_VERSION = getenv("APP_VERSION", "ui-v5-manager-filter-insights-2026-05-15")
REFRESH_LOCK = Lock()
REFRESH_STATE = {
    "running": False,
    "period_key": CUSTOM_PERIOD_KEY,
    "period_title": "",
    "percent": 0,
    "message": "",
    "error": "",
}


def is_auth_enabled():
    return bool(APP_USERNAME and APP_PASSWORD)


def is_authorized():
    if not is_auth_enabled():
        return True

    auth = request.authorization
    if not auth:
        return False

    return compare_digest(auth.username, APP_USERNAME) and compare_digest(auth.password, APP_PASSWORD)


@app.before_request
def require_basic_auth():
    if request.path == "/health":
        return None

    if is_authorized():
        return None

    return Response(
        "Требуется авторизация",
        401,
        {"WWW-Authenticate": 'Basic realm="amoCRM Analytics"'},
    )


@app.after_request
def disable_browser_cache(response):
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@app.context_processor
def inject_app_version():
    return {"app_version": APP_VERSION}


def load_dashboard_cache():
    if not CACHE_PATH.exists():
        return empty_dashboard()

    dashboard = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    return normalize_dashboard(dashboard)


def save_dashboard_cache(dashboard):
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    CACHE_PATH.write_text(
        json.dumps(dashboard, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def normalize_dashboard(dashboard):
    normalized = deepcopy(empty_dashboard())
    normalized["group_users_count"] = dashboard.get("group_users_count", 0)
    normalized["updated_at"] = dashboard.get("updated_at", "-")
    normalized["selected"] = dashboard.get("selected", normalized["selected"])

    existing_by_key = {
        period.get("key"): period
        for period in dashboard.get("periods", [])
        if period.get("key")
    }
    existing_by_title = {
        period.get("title"): period
        for period in dashboard.get("periods", [])
        if period.get("title")
    }

    periods = []
    for period in normalized["periods"]:
        period_key = period["key"]
        source_period = existing_by_key.get(period_key) or existing_by_title.get(period["title"])
        if source_period:
            merged = deepcopy(period)
            merged.update(source_period)
            merged["key"] = period_key
            merged.setdefault("updated_at", dashboard.get("updated_at", "-"))
            merged.setdefault("per_manager_insights", {})
            periods.append(merged)
        else:
            periods.append(period)

    normalized["periods"] = periods
    return normalized


def set_refresh_state(running, period_key=CUSTOM_PERIOD_KEY, period_title="", percent=0, message="", error=""):
    with REFRESH_LOCK:
        REFRESH_STATE["running"] = running
        REFRESH_STATE["period_key"] = period_key
        REFRESH_STATE["period_title"] = period_title
        REFRESH_STATE["percent"] = percent
        REFRESH_STATE["message"] = message
        REFRESH_STATE["error"] = error


def get_refresh_state():
    with REFRESH_LOCK:
        return deepcopy(REFRESH_STATE)


def attach_refresh_state(dashboard):
    result = deepcopy(dashboard)
    result["refresh"] = get_refresh_state()
    return result


def update_period_in_cache(period_key, period_payload):
    dashboard = load_dashboard_cache()
    dashboard["group_users_count"] = max(
        dashboard.get("group_users_count", 0),
        period_payload.get("group_users_count", 0),
    )
    dashboard["updated_at"] = period_payload["period"]["updated_at"]

    updated_periods = []
    replaced = False
    for period in dashboard["periods"]:
        if period["key"] == period_key:
            updated_periods.append(period_payload["period"])
            replaced = True
        else:
            updated_periods.append(period)

    if not replaced:
        updated_periods.append(period_payload["period"])

    dashboard["periods"] = updated_periods
    dashboard["selected"] = period_payload.get("selected", dashboard.get("selected", {}))
    save_dashboard_cache(dashboard)


def rebuild_period_in_background(period_key, date_from, date_to):
    try:
        period_title = f"{date_from} - {date_to}"
        set_refresh_state(
            True,
            period_key=period_key,
            period_title=period_title,
            percent=1,
            message=f"Запущен сбор данных: {period_title}",
        )

        def on_progress(percent, message):
            set_refresh_state(
                True,
                period_key=period_key,
                period_title=period_title,
                percent=percent,
                message=message,
            )

        period_payload = build_period_data(
            period_key,
            progress_callback=on_progress,
            date_from=date_from,
            date_to=date_to,
        )
        update_period_in_cache(period_key, period_payload)
        set_refresh_state(
            False,
            period_key=period_key,
            period_title=period_title,
            percent=100,
            message=f"Данные за период «{period_title}» обновлены.",
        )
    except Exception as exc:
        set_refresh_state(
            False,
            period_key=period_key,
            period_title=period_title,
            percent=0,
            message="Не удалось обновить данные. Показана последняя сохраненная версия.",
            error=str(exc),
        )


@app.route("/")
def index():
    dashboard = load_dashboard_cache()
    return render_template("index.html", dashboard=attach_refresh_state(dashboard))


@app.route("/refresh", methods=["POST"])
def refresh():
    state = get_refresh_state()
    if state["running"]:
        return redirect(url_for("index"))

    date_from = request.form.get("date_from", "").strip()
    date_to = request.form.get("date_to", "").strip()
    if not date_from or not date_to:
        dashboard = load_dashboard_cache()
        dashboard["error"] = "Выберите дату начала и дату окончания"
        return render_template("index.html", dashboard=attach_refresh_state(dashboard)), 400

    try:
        datetime.strptime(date_from, "%Y-%m-%d")
        datetime.strptime(date_to, "%Y-%m-%d")
    except ValueError:
        dashboard = load_dashboard_cache()
        dashboard["error"] = "Некорректный формат даты"
        return render_template("index.html", dashboard=attach_refresh_state(dashboard)), 400

    Thread(
        target=rebuild_period_in_background,
        args=(CUSTOM_PERIOD_KEY, date_from, date_to),
        daemon=True,
    ).start()
    return redirect(url_for("index"))


@app.route("/status")
def status():
    return jsonify(get_refresh_state())


@app.route("/health")
def health():
    return {"status": "ok"}


@app.route("/version")
def version():
    return {"version": APP_VERSION}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(getenv("PORT", "5000")), debug=False)
