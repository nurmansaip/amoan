import json
import time
from copy import deepcopy
from datetime import datetime
from os import getenv
from typing import Optional
from pathlib import Path
from secrets import compare_digest
from threading import Lock, Thread

from dotenv import load_dotenv
from flask import Flask, Response, jsonify, redirect, render_template, request, url_for

from analytics import (
    ACTIVITY_MAX_PERIOD_DAYS,
    CUSTOM_PERIOD_KEY,
    build_activity_period_data,
    build_conversion_period_data,
    build_custom_period,
    empty_dashboard,
    load_group_users,
    period_length_days,
)
from amo_client import AmoCRMClient


load_dotenv()
app = Flask(__name__)
BASE_DIR = Path(__file__).resolve().parent
CACHE_PATH = Path(getenv("CACHE_PATH", "") or BASE_DIR / "dashboard_cache.json")
APP_USERNAME = getenv("APP_USERNAME", "").strip()
APP_PASSWORD = getenv("APP_PASSWORD", "").strip()
APP_VERSION = getenv("APP_VERSION", "ui-v9-form-dates-fix-2026-06-11")
REFRESH_LOCK = Lock()
REFRESH_STATE = {
    "running": False,
    "mode": "",
    "period_key": CUSTOM_PERIOD_KEY,
    "period_title": "",
    "percent": 0,
    "message": "",
    "error": "",
    "started_at": 0,
}
REFRESH_TIMEOUT_SECONDS = 40 * 60


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
    normalized["group_managers"] = dashboard.get("group_managers", [])
    normalized["updated_at"] = dashboard.get("updated_at", "-")
    normalized["selected"] = dashboard.get("selected", normalized["selected"])
    normalized["selected"].setdefault("conversion_manager_ids", [])

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
            merged.setdefault(
                "conversion",
                {
                    "totals": {},
                    "rows": [],
                    "excluded_reasons": [],
                    "excluded_reason_labels": [],
                    "rules": "",
                },
            )
            periods.append(merged)
        else:
            periods.append(period)

    normalized["periods"] = periods
    return normalized


def set_refresh_state(
    running,
    mode="",
    period_key=CUSTOM_PERIOD_KEY,
    period_title="",
    percent=0,
    message="",
    error="",
):
    with REFRESH_LOCK:
        REFRESH_STATE["running"] = running
        REFRESH_STATE["mode"] = mode
        REFRESH_STATE["period_key"] = period_key
        REFRESH_STATE["period_title"] = period_title
        REFRESH_STATE["percent"] = percent
        REFRESH_STATE["message"] = message
        REFRESH_STATE["error"] = error
        if running:
            REFRESH_STATE["started_at"] = int(time.time())
        else:
            REFRESH_STATE["started_at"] = 0


def get_refresh_state():
    with REFRESH_LOCK:
        state = deepcopy(REFRESH_STATE)
        if state["running"] and not state.get("started_at"):
            REFRESH_STATE["running"] = False
            REFRESH_STATE["message"] = (
                "Зависший сбор сброшен. Для периода больше 3 месяцев используйте «Собрать конверсию»."
            )
            REFRESH_STATE["error"] = REFRESH_STATE["message"]
            state = deepcopy(REFRESH_STATE)
        elif state["running"] and state.get("started_at"):
            elapsed = int(time.time()) - int(state["started_at"])
            if elapsed > REFRESH_TIMEOUT_SECONDS:
                REFRESH_STATE["running"] = False
                REFRESH_STATE["error"] = (
                    "Сбор остановлен: превышен лимит 40 минут. "
                    "Для периода больше 3 месяцев используйте «Собрать конверсию»."
                )
                REFRESH_STATE["message"] = REFRESH_STATE["error"]
                REFRESH_STATE["started_at"] = 0
                state = deepcopy(REFRESH_STATE)
            else:
                state["elapsed_sec"] = elapsed
        return state


def attach_refresh_state(dashboard):
    result = deepcopy(dashboard)
    result["refresh"] = get_refresh_state()
    return result


def merge_period_in_cache(period_key, period_payload):
    dashboard = load_dashboard_cache()
    dashboard["group_users_count"] = max(
        dashboard.get("group_users_count", 0),
        period_payload.get("group_users_count", 0),
    )

    incoming_period = period_payload["period"]
    updated_periods = []
    replaced = False

    for period in dashboard["periods"]:
        if period["key"] == period_key:
            merged = deepcopy(period)
            merged.update(incoming_period)
            merged["key"] = period_key
            updated_periods.append(merged)
            replaced = True
        else:
            updated_periods.append(period)

    if not replaced:
        shell = deepcopy(empty_dashboard()["periods"][0])
        shell.update(incoming_period)
        shell["key"] = period_key
        updated_periods.append(shell)

    dashboard["periods"] = updated_periods
    dashboard["updated_at"] = incoming_period.get(
        "updated_at",
        datetime.now().strftime("%d.%m.%Y %H:%M:%S"),
    )
    dashboard["selected"] = period_payload.get("selected", dashboard.get("selected", {}))
    if period_payload.get("group_managers"):
        dashboard["group_managers"] = period_payload["group_managers"]
    save_dashboard_cache(dashboard)


def parse_refresh_dates():
    date_from = request.form.get("date_from", "").strip()
    date_to = request.form.get("date_to", "").strip()
    if not date_from or not date_to:
        return None, None, "Выберите дату начала и дату окончания"

    try:
        datetime.strptime(date_from, "%Y-%m-%d")
        datetime.strptime(date_to, "%Y-%m-%d")
    except ValueError:
        return None, None, "Некорректный формат даты"

    return date_from, date_to, ""


def parse_conversion_manager_ids():
    manager_ids = []
    for value in request.form.getlist("conversion_manager_ids"):
        value = value.strip()
        if value.isdigit():
            manager_ids.append(int(value))
    return manager_ids


def format_date_ru(iso_date: str) -> str:
    return datetime.strptime(iso_date, "%Y-%m-%d").strftime("%d.%m.%Y")


def apply_submitted_form_state(
    dashboard,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    conversion_manager_ids: Optional[list] = None,
):
    dashboard.setdefault("selected", {})
    if date_from:
        dashboard["selected"]["date_from"] = date_from
    if date_to:
        dashboard["selected"]["date_to"] = date_to
    if conversion_manager_ids is not None:
        dashboard["selected"]["conversion_manager_ids"] = [
            str(manager_id) for manager_id in conversion_manager_ids
        ]
    return dashboard


def stash_form_selection(date_from: str, date_to: str, conversion_manager_ids: Optional[list] = None):
    dashboard = load_dashboard_cache()
    apply_submitted_form_state(dashboard, date_from, date_to, conversion_manager_ids)
    save_dashboard_cache(dashboard)


def ensure_group_managers(dashboard):
    if dashboard.get("group_managers"):
        return dashboard

    try:
        client = AmoCRMClient()
        group_users = load_group_users(client)
        dashboard["group_managers"] = [
            {"id": manager_id, "name": manager_name}
            for manager_id, manager_name in sorted(group_users.items(), key=lambda item: item[1])
        ]
        dashboard["group_users_count"] = max(dashboard.get("group_users_count", 0), len(group_users))
    except Exception:
        dashboard["group_managers"] = []

    return dashboard


def rebuild_in_background(mode, period_key, date_from, date_to, conversion_manager_ids=None):
    period_title = f"{date_from} - {date_to}"
    mode_labels = {
        "activity": "активности",
        "conversion": "конверсии",
    }

    try:
        set_refresh_state(
            True,
            mode=mode,
            period_key=period_key,
            period_title=period_title,
            percent=1,
            message=f"Запущен сбор {mode_labels.get(mode, 'данных')}: {period_title}",
        )

        def on_progress(percent, message):
            set_refresh_state(
                True,
                mode=mode,
                period_key=period_key,
                period_title=period_title,
                percent=percent,
                message=message,
            )

        if mode == "activity":
            period_payload = build_activity_period_data(
                period_key,
                progress_callback=on_progress,
                date_from=date_from,
                date_to=date_to,
            )
            success_message = f"Активность за период «{period_title}» обновлена."
        elif mode == "conversion":
            period_payload = build_conversion_period_data(
                period_key,
                progress_callback=on_progress,
                date_from=date_from,
                date_to=date_to,
                manager_ids=conversion_manager_ids,
            )
            success_message = f"Конверсия за период «{period_title}» обновлена."
        else:
            raise ValueError(f"Неизвестный режим сбора: {mode}")

        merge_period_in_cache(period_key, period_payload)
        set_refresh_state(
            False,
            mode=mode,
            period_key=period_key,
            period_title=period_title,
            percent=100,
            message=success_message,
        )
    except Exception as exc:
        set_refresh_state(
            False,
            mode=mode,
            period_key=period_key,
            period_title=period_title,
            percent=0,
            message="Не удалось обновить данные. Показана последняя сохраненная версия.",
            error=str(exc),
        )


def start_refresh(mode):
    state = get_refresh_state()
    if state["running"]:
        return redirect(url_for("index"))

    date_from, date_to, error = parse_refresh_dates()
    conversion_manager_ids = parse_conversion_manager_ids() if mode == "conversion" else None

    if error:
        dashboard = ensure_group_managers(load_dashboard_cache())
        apply_submitted_form_state(
            dashboard,
            date_from,
            date_to,
            conversion_manager_ids,
        )
        dashboard["error"] = error
        return render_template("index.html", dashboard=attach_refresh_state(dashboard)), 400

    if mode == "activity":
        period = build_custom_period(date_from, date_to)
        days = period_length_days(period)
        if days > ACTIVITY_MAX_PERIOD_DAYS:
            dashboard = ensure_group_managers(load_dashboard_cache())
            apply_submitted_form_state(dashboard, date_from, date_to, conversion_manager_ids)
            dashboard["error"] = (
                f"Период {format_date_ru(date_from)} — {format_date_ru(date_to)} ({days} дн.) "
                f"слишком длинный для активности (лимит {ACTIVITY_MAX_PERIOD_DAYS} дн.). "
                "Нажмите «Собрать конверсию» — для полугода и года нужна она, не активность."
            )
            return render_template("index.html", dashboard=attach_refresh_state(dashboard)), 400

    if mode == "conversion":
        if not conversion_manager_ids:
            dashboard = ensure_group_managers(load_dashboard_cache())
            apply_submitted_form_state(dashboard, date_from, date_to, conversion_manager_ids)
            dashboard["error"] = "Выберите хотя бы одного менеджера для конверсии"
            return render_template("index.html", dashboard=attach_refresh_state(dashboard)), 400

    stash_form_selection(date_from, date_to, conversion_manager_ids)

    Thread(
        target=rebuild_in_background,
        args=(mode, CUSTOM_PERIOD_KEY, date_from, date_to, conversion_manager_ids),
        daemon=True,
    ).start()
    return redirect(url_for("index"))


@app.route("/")
def index():
    dashboard = ensure_group_managers(load_dashboard_cache())
    return render_template("index.html", dashboard=attach_refresh_state(dashboard))


@app.route("/refresh/activity", methods=["POST"])
def refresh_activity():
    return start_refresh("activity")


@app.route("/refresh/conversion", methods=["POST"])
def refresh_conversion():
    return start_refresh("conversion")


@app.route("/refresh", methods=["POST"])
def refresh():
    return start_refresh("activity")


@app.route("/status")
def status():
    return jsonify(get_refresh_state())


@app.route("/refresh/cancel", methods=["POST"])
def refresh_cancel():
    set_refresh_state(
        False,
        message="Сбор данных остановлен вручную.",
    )
    return redirect(url_for("index"))


@app.route("/health")
def health():
    return {"status": "ok"}


@app.route("/version")
def version():
    return {"version": APP_VERSION}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(getenv("PORT", "5000")), debug=False)
