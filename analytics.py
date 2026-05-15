from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from os import getenv
from typing import Any, Optional
from zoneinfo import ZoneInfo

import pandas as pd

from amo_client import AmoCRMClient


def app_timezone() -> ZoneInfo:
    """Часовой пояс для календарных периодов и часов на графиках (Railway по умолчанию в UTC)."""
    name = getenv("APP_TIMEZONE", "Asia/Almaty").strip() or "Asia/Almaty"
    try:
        return ZoneInfo(name)
    except Exception:
        return ZoneInfo("Asia/Almaty")


def utc_ts_to_app_local(ts: int) -> datetime:
    """amoCRM хранит created_at как unix time в UTC — переводим в локальный пояс приложения."""
    return datetime.fromtimestamp(ts, tz=app_timezone())


GROUP_KEYWORDS = ("shymkent", "шымкент")
CLOSED_STATUS_IDS = {142, 143}
TRACKED_EVENT_TYPES = (
    "incoming_call",
    "incoming_chat_message",
    "outgoing_call",
    "outgoing_chat_message",
    "lead_status_changed",
    "lead_added",
    "task_added",
    "task_completed",
)
APPEAL_EVENT_TYPES = (
    "lead_added",
    "incoming_call",
    "incoming_chat_message",
)
CUSTOM_PERIOD_KEY = "custom"
METRIC_COLUMNS = [
    "Действия в amoCRM",
    "Звонки",
    "Отправленные сообщения",
    "Закрытые сделки",
    "Открытые сделки",
    "Поставленные задачи",
    "Закрытые задачи",
]


@dataclass(frozen=True)
class Period:
    title: str
    started_at: datetime
    ended_at: datetime

    @property
    def started_ts(self) -> int:
        return int(self.started_at.timestamp())

    @property
    def ended_ts(self) -> int:
        return int(self.ended_at.timestamp())


def build_periods(now: Optional[datetime] = None) -> list[Period]:
    tz = app_timezone()
    now = now or datetime.now(tz)
    if now.tzinfo is None:
        now = now.replace(tzinfo=tz)
    today_start = datetime.combine(now.date(), time.min, tzinfo=tz)

    return [
        Period("Сегодня", today_start, now),
        Period("Неделя", now - timedelta(days=7), now),
        Period("Месяц", now - timedelta(days=30), now),
    ]


def build_period_by_key(period_key: str, now: Optional[datetime] = None) -> Period:
    tz = app_timezone()
    now = now or datetime.now(tz)
    if now.tzinfo is None:
        now = now.replace(tzinfo=tz)
    today_start = datetime.combine(now.date(), time.min, tzinfo=tz)

    if period_key == "today":
        return Period("Сегодня", today_start, now)
    if period_key == "week":
        return Period("Неделя", now - timedelta(days=7), now)
    if period_key == "month":
        return Period("Месяц", now - timedelta(days=30), now)

    raise ValueError(f"Неизвестный период: {period_key}")


def build_custom_period(date_from: str, date_to: str) -> Period:
    started_date = datetime.strptime(date_from, "%Y-%m-%d").date()
    ended_date = datetime.strptime(date_to, "%Y-%m-%d").date()

    if started_date > ended_date:
        raise ValueError("Дата начала не может быть позже даты окончания")

    tz = app_timezone()
    started_at = datetime.combine(started_date, time.min, tzinfo=tz)
    ended_at = datetime.combine(ended_date, time.max, tzinfo=tz)
    title = f"{started_date.strftime('%d.%m.%Y')} - {ended_date.strftime('%d.%m.%Y')}"

    return Period(title, started_at, ended_at)


def fetch_users(client: AmoCRMClient) -> dict[int, str]:
    data = client.get("/api/v4/users", params=[("limit", 250)])
    users = data.get("_embedded", {}).get("users", [])
    return {int(user["id"]): user["name"] for user in users}


def fetch_status_names(client: AmoCRMClient) -> dict[int, str]:
    data = client.get("/api/v4/leads/pipelines")
    status_names = {}
    for pipeline in data.get("_embedded", {}).get("pipelines", []):
        pipeline_name = pipeline.get("name", "Воронка")
        statuses = pipeline.get("_embedded", {}).get("statuses", [])
        for status in statuses:
            status_names[int(status["id"])] = f"{pipeline_name}: {status['name']}"

    return status_names


def fetch_tasks(
    client: AmoCRMClient,
    started_ts: int,
    ended_ts: int,
    user_ids: list[int],
) -> list[dict[str, Any]]:
    tasks = []
    page = 1

    while True:
        params: list[tuple[str, Any]] = [
            ("filter[complete_till][from]", started_ts),
            ("filter[complete_till][to]", ended_ts),
            ("limit", 250),
            ("page", page),
        ]
        for user_id in user_ids:
            params.append(("filter[responsible_user_id][]", user_id))

        data = client.get("/api/v4/tasks", params=params)
        page_tasks = data.get("_embedded", {}).get("tasks", [])
        tasks.extend(page_tasks)

        if not page_tasks or not data.get("_links", {}).get("next"):
            break

        page += 1

    return tasks


def filter_group_users(users_by_id: dict[int, str]) -> dict[int, str]:
    group_users = {}
    for user_id, name in users_by_id.items():
        normalized_name = name.lower()
        if any(keyword in normalized_name for keyword in GROUP_KEYWORDS):
            group_users[user_id] = name

    return group_users


def fetch_events(
    client: AmoCRMClient,
    started_ts: int,
    ended_ts: int,
    user_ids: list[int],
    progress_callback=None,
) -> list[dict[str, Any]]:
    events = []
    page = 1

    while True:
        if progress_callback:
            progress_callback(page, len(events))

        params: list[tuple[str, Any]] = [
            ("filter[created_at][from]", started_ts),
            ("filter[created_at][to]", ended_ts),
            ("limit", 100),
            ("page", page),
        ]
        for user_id in user_ids:
            params.append(("filter[created_by][]", user_id))
        for event_type in TRACKED_EVENT_TYPES:
            params.append(("filter[type][]", event_type))

        data = client.get("/api/v4/events", params=params)
        page_events = data.get("_embedded", {}).get("events", [])
        events.extend(page_events)

        if not page_events or not data.get("_links", {}).get("next"):
            break

        page += 1

    return events


def fetch_appeal_events(
    client: AmoCRMClient,
    started_ts: int,
    ended_ts: int,
    progress_callback=None,
) -> list[dict[str, Any]]:
    events = []
    page = 1

    while True:
        if progress_callback:
            progress_callback(page, len(events))

        params: list[tuple[str, Any]] = [
            ("filter[created_at][from]", started_ts),
            ("filter[created_at][to]", ended_ts),
            ("limit", 100),
            ("page", page),
        ]
        for event_type in APPEAL_EVENT_TYPES:
            params.append(("filter[type][]", event_type))

        data = client.get("/api/v4/events", params=params)
        page_events = data.get("_embedded", {}).get("events", [])
        events.extend(page_events)

        if not page_events or not data.get("_links", {}).get("next"):
            break

        page += 1

    return events


def get_event_manager_id(event: dict[str, Any]) -> Optional[int]:
    manager_id = event.get("created_by") or event.get("created_by_id")
    if not manager_id:
        return None

    return int(manager_id)


def get_new_status_id(event: dict[str, Any]) -> Optional[int]:
    for item in event.get("value_after") or []:
        lead_status = item.get("lead_status") or {}
        status_id = lead_status.get("id")
        if status_id is not None:
            return int(status_id)

    return None


def get_previous_status_id(event: dict[str, Any]) -> Optional[int]:
    for item in event.get("value_before") or []:
        lead_status = item.get("lead_status") or {}
        status_id = lead_status.get("id")
        if status_id is not None:
            return int(status_id)

    return None


def event_to_metric_flags(event: dict[str, Any]) -> dict[str, int]:
    event_type = event.get("type")

    return {
        "Действия в amoCRM": int(event_type in TRACKED_EVENT_TYPES),
        "Звонки": int(event_type in {"incoming_call", "outgoing_call"}),
        "Отправленные сообщения": int(event_type == "outgoing_chat_message"),
        "Закрытые сделки": int(
            event_type == "lead_status_changed" and get_new_status_id(event) in CLOSED_STATUS_IDS
        ),
        "Открытые сделки": int(event_type == "lead_added"),
        "Поставленные задачи": int(event_type == "task_added"),
        "Закрытые задачи": int(event_type == "task_completed"),
    }


def build_period_report(
    events: list[dict[str, Any]],
    period: Period,
    group_users: dict[int, str],
) -> pd.DataFrame:
    rows = []
    for event in events:
        created_at = int(event.get("created_at") or 0)
        if created_at < period.started_ts or created_at > period.ended_ts:
            continue

        manager_id = get_event_manager_id(event)
        if manager_id not in group_users:
            continue

        rows.append(
            {
                "ID менеджера": manager_id,
                "Менеджер": group_users[manager_id],
                **event_to_metric_flags(event),
            }
        )

    if not rows:
        return pd.DataFrame(
            [
                {
                    "ID менеджера": manager_id,
                    "Менеджер": manager_name,
                    **{column: 0 for column in METRIC_COLUMNS},
                }
                for manager_id, manager_name in group_users.items()
            ]
        )

    df = pd.DataFrame(rows)
    report = df.groupby(["ID менеджера", "Менеджер"], as_index=False)[METRIC_COLUMNS].sum()

    # Показываем всех менеджеров группы, даже если за период у кого-то нули.
    all_managers = pd.DataFrame(
        [
            {"ID менеджера": manager_id, "Менеджер": manager_name}
            for manager_id, manager_name in group_users.items()
        ]
    )
    report = all_managers.merge(report, on=["ID менеджера", "Менеджер"], how="left")
    report[METRIC_COLUMNS] = report[METRIC_COLUMNS].fillna(0).astype(int)

    return report.sort_values("Действия в amoCRM", ascending=False)


def format_seconds(seconds: Optional[float]) -> str:
    if seconds is None:
        return "-"

    if seconds < 3600:
        return f"{round(seconds / 60, 1)} мин"
    if seconds < 86400:
        return f"{round(seconds / 3600, 1)} ч"
    return f"{round(seconds / 86400, 1)} дн"


def build_manager_details(
    events: list[dict[str, Any]],
    tasks: list[dict[str, Any]],
    period: Period,
    group_users: dict[int, str],
    status_names: dict[int, str],
) -> list[dict[str, Any]]:
    metric_rows = build_period_report(events, period, group_users).to_dict(orient="records")
    managers = {
        int(row["ID менеджера"]): {
            **row,
            "Среднее время до первого контакта": "-",
            "Просроченные задачи": 0,
            "Звонки и сообщения на сделку": 0,
            "Сделки с 2+ касаниями": 0,
            "Сделки с 3+ касаниями": 0,
            "Сделки с 5+ касаниями": 0,
            "Рейтинг": 0,
            "Конверсия по этапам": [],
            "Скорость по этапам": [],
        }
        for row in metric_rows
    }

    lead_created_at: dict[int, int] = {}
    first_contact_by_lead: dict[int, int] = {}
    touches_by_manager_lead: dict[tuple[int, int], int] = defaultdict(int)
    stage_counts_by_manager: dict[int, dict[int, int]] = defaultdict(lambda: defaultdict(int))
    stage_durations_by_manager: dict[int, dict[int, list[int]]] = defaultdict(lambda: defaultdict(list))
    lead_last_stage: dict[int, tuple[int, int]] = {}

    sorted_events = sorted(events, key=lambda event: int(event.get("created_at") or 0))
    for event in sorted_events:
        event_type = event.get("type")
        created_at = int(event.get("created_at") or 0)
        manager_id = get_event_manager_id(event)
        entity_id = event.get("entity_id")
        entity_type = event.get("entity_type")

        if event_type == "lead_added" and entity_id:
            lead_created_at[int(entity_id)] = created_at

        if manager_id not in group_users:
            continue

        lead_id = int(entity_id) if entity_id and entity_type == "lead" else None
        if lead_id and event_type in {"incoming_call", "outgoing_call", "outgoing_chat_message"}:
            first_contact_by_lead.setdefault(lead_id, created_at)

        if lead_id and event_type in TRACKED_EVENT_TYPES:
            touches_by_manager_lead[(manager_id, lead_id)] += 1

        if event_type == "lead_status_changed":
            new_status_id = get_new_status_id(event)
            previous_status_id = get_previous_status_id(event)
            if new_status_id is not None:
                stage_counts_by_manager[manager_id][new_status_id] += 1

            if lead_id and previous_status_id is not None:
                previous_seen = lead_last_stage.get(lead_id)
                if previous_seen:
                    previous_status_seen, started_at = previous_seen
                    duration = max(0, created_at - started_at)
                    stage_durations_by_manager[manager_id][previous_status_seen].append(duration)

            if lead_id and new_status_id is not None:
                lead_last_stage[lead_id] = (new_status_id, created_at)

    overdue_by_manager: dict[int, int] = defaultdict(int)
    for task in tasks:
        manager_id = int(task.get("responsible_user_id") or 0)
        if manager_id not in group_users:
            continue

        complete_till = int(task.get("complete_till") or 0)
        updated_at = int(task.get("updated_at") or 0)
        is_completed = bool(task.get("is_completed"))
        if complete_till and ((is_completed and updated_at > complete_till) or not is_completed):
            overdue_by_manager[manager_id] += 1

    first_contact_seconds_by_manager: dict[int, list[int]] = defaultdict(list)
    for (manager_id, lead_id), _touches in touches_by_manager_lead.items():
        created_at = lead_created_at.get(lead_id)
        contacted_at = first_contact_by_lead.get(lead_id)
        if created_at and contacted_at and contacted_at >= created_at:
            first_contact_seconds_by_manager[manager_id].append(contacted_at - created_at)

    for manager_id, manager in managers.items():
        lead_ids = {
            lead_id
            for (touch_manager_id, lead_id), _touches in touches_by_manager_lead.items()
            if touch_manager_id == manager_id
        }
        touches = [touches_by_manager_lead[(manager_id, lead_id)] for lead_id in lead_ids]
        contact_values = first_contact_seconds_by_manager.get(manager_id, [])

        manager["Среднее время до первого контакта"] = format_seconds(
            sum(contact_values) / len(contact_values) if contact_values else None
        )
        manager["Просроченные задачи"] = overdue_by_manager.get(manager_id, 0)
        manager["Звонки и сообщения на сделку"] = round(
            (manager["Звонки"] + manager["Отправленные сообщения"]) / len(lead_ids),
            2,
        ) if lead_ids else 0
        manager["Сделки с 2+ касаниями"] = sum(1 for value in touches if value >= 2)
        manager["Сделки с 3+ касаниями"] = sum(1 for value in touches if value >= 3)
        manager["Сделки с 5+ касаниями"] = sum(1 for value in touches if value >= 5)
        manager["Рейтинг"] = round(
            manager["Закрытые сделки"] * 5
            + manager["Звонки"]
            + manager["Отправленные сообщения"]
            + manager["Закрытые задачи"] * 0.5
            - manager["Просроченные задачи"] * 2,
            1,
        )
        manager["Конверсия по этапам"] = [
            {
                "stage": status_names.get(status_id, f"Этап #{status_id}"),
                "count": count,
            }
            for status_id, count in sorted(
                stage_counts_by_manager.get(manager_id, {}).items(),
                key=lambda item: item[1],
                reverse=True,
            )[:8]
        ]
        manager["Скорость по этапам"] = [
            {
                "stage": status_names.get(status_id, f"Этап #{status_id}"),
                "avg_time": format_seconds(sum(values) / len(values)),
            }
            for status_id, values in sorted(
                stage_durations_by_manager.get(manager_id, {}).items(),
                key=lambda item: sum(item[1]) / len(item[1]) if item[1] else 0,
                reverse=True,
            )[:8]
            if values
        ]

    return sorted(managers.values(), key=lambda manager: manager["Рейтинг"], reverse=True)


def period_length_seconds(period: Period) -> int:
    return max(1, period.ended_ts - period.started_ts + 1)


def build_previous_period(period: Period) -> Period:
    length = period_length_seconds(period)
    previous_end = period.started_at - timedelta(seconds=1)
    previous_start = previous_end - timedelta(seconds=length - 1)
    return Period("Предыдущий период", previous_start, previous_end)


def dataframe_totals(report: pd.DataFrame) -> dict[str, int]:
    if report.empty:
        return {column: 0 for column in METRIC_COLUMNS}
    return {column: int(report[column].sum()) for column in METRIC_COLUMNS}


def build_comparison(current_report: pd.DataFrame, previous_report: pd.DataFrame) -> list[dict[str, Any]]:
    current_totals = dataframe_totals(current_report)
    previous_totals = dataframe_totals(previous_report)
    comparison = []

    for metric in METRIC_COLUMNS:
        current_value = current_totals.get(metric, 0)
        previous_value = previous_totals.get(metric, 0)
        diff = current_value - previous_value
        percent = round(diff / previous_value * 100, 1) if previous_value else None
        comparison.append(
            {
                "metric": metric,
                "current": current_value,
                "previous": previous_value,
                "diff": diff,
                "percent": percent,
            }
        )

    return comparison


def build_per_manager_insights(
    events: list[dict[str, Any]],
    period: Period,
    group_users: dict[int, str],
    report: pd.DataFrame,
    previous_report: pd.DataFrame,
) -> dict[str, dict[str, Any]]:
    """Метрики периода в разрезе одного менеджера (для фильтра на фронте)."""
    result: dict[str, dict[str, Any]] = {}
    for manager_id in group_users:
        mgr_events = [e for e in events if get_event_manager_id(e) == manager_id]
        single_user = {manager_id: group_users[manager_id]}
        cur_df = report[report["ID менеджера"] == manager_id]
        prev_df = previous_report[previous_report["ID менеджера"] == manager_id]

        heatmap = build_heatmap(mgr_events, single_user)
        appeal_subset = [e for e in mgr_events if e.get("type") in APPEAL_EVENT_TYPES]
        appeals_heatmap = build_event_type_heatmap(appeal_subset)

        result[str(manager_id)] = {
            "totals": dataframe_totals(cur_df),
            "comparison": build_comparison(cur_df, prev_df),
            "daily_dynamics": build_daily_dynamics(mgr_events, period, single_user),
            "heatmap": heatmap,
            "top_activity_hours": build_top_activity_hours(heatmap),
            "appeals_heatmap": appeals_heatmap,
            "top_appeal_hours": build_top_activity_hours(appeals_heatmap),
            "appeal_summary": build_appeal_summary(appeal_subset),
        }
    return result


def build_daily_dynamics(events: list[dict[str, Any]], period: Period, group_users: dict[int, str]) -> list[dict[str, Any]]:
    dynamics: dict[str, dict[str, int]] = defaultdict(lambda: {column: 0 for column in METRIC_COLUMNS})
    current_date = period.started_at.date()
    end_date = period.ended_at.date()

    while current_date <= end_date:
        dynamics[current_date.strftime("%d.%m")] = {column: 0 for column in METRIC_COLUMNS}
        current_date += timedelta(days=1)

    for event in events:
        manager_id = get_event_manager_id(event)
        if manager_id not in group_users:
            continue

        created_at = utc_ts_to_app_local(int(event.get("created_at") or 0))
        day_key = created_at.strftime("%d.%m")
        flags = event_to_metric_flags(event)
        for metric, value in flags.items():
            dynamics[day_key][metric] += value

    return [{"date": date, **values} for date, values in dynamics.items()]


def build_heatmap(events: list[dict[str, Any]], group_users: dict[int, str]) -> list[dict[str, Any]]:
    weekday_names = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
    buckets: dict[tuple[int, int], int] = defaultdict(int)

    for event in events:
        manager_id = get_event_manager_id(event)
        if manager_id not in group_users or event.get("type") not in TRACKED_EVENT_TYPES:
            continue

        created_at = utc_ts_to_app_local(int(event.get("created_at") or 0))
        buckets[(created_at.weekday(), created_at.hour)] += 1

    max_value = max(buckets.values()) if buckets else 0
    rows = []
    for weekday in range(7):
        hours = []
        for hour in range(0, 24):
            value = buckets.get((weekday, hour), 0)
            intensity = round(value / max_value, 2) if max_value else 0
            hours.append({"hour": hour, "value": value, "intensity": intensity})
        rows.append({"weekday": weekday_names[weekday], "hours": hours})

    return rows


def build_event_type_heatmap(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    weekday_names = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
    buckets: dict[tuple[int, int], int] = defaultdict(int)

    for event in events:
        created_at = utc_ts_to_app_local(int(event.get("created_at") or 0))
        buckets[(created_at.weekday(), created_at.hour)] += 1

    max_value = max(buckets.values()) if buckets else 0
    rows = []
    for weekday in range(7):
        hours = []
        for hour in range(0, 24):
            value = buckets.get((weekday, hour), 0)
            intensity = round(value / max_value, 2) if max_value else 0
            hours.append({"hour": hour, "value": value, "intensity": intensity})
        rows.append({"weekday": weekday_names[weekday], "hours": hours})

    return rows


def build_appeal_summary(events: list[dict[str, Any]]) -> dict[str, int]:
    return {
        "Всего обращений": len(events),
        "Новые лиды": sum(1 for event in events if event.get("type") == "lead_added"),
        "Входящие звонки": sum(1 for event in events if event.get("type") == "incoming_call"),
        "Входящие сообщения": sum(1 for event in events if event.get("type") == "incoming_chat_message"),
    }


def build_top_activity_hours(heatmap: list[dict[str, Any]]) -> list[dict[str, Any]]:
    hours = []
    for row in heatmap:
        for hour in row["hours"]:
            if hour["value"]:
                hours.append(
                    {
                        "weekday": row["weekday"],
                        "hour": f"{hour['hour']:02d}:00",
                        "value": hour["value"],
                    }
                )

    return sorted(hours, key=lambda item: item["value"], reverse=True)[:6]


def build_lead_quality(events: list[dict[str, Any]], group_users: dict[int, str]) -> dict[str, Any]:
    lead_created_at: dict[int, int] = {}
    first_contact: dict[int, tuple[int, int]] = {}

    for event in sorted(events, key=lambda item: int(item.get("created_at") or 0)):
        entity_id = event.get("entity_id")
        entity_type = event.get("entity_type")
        event_type = event.get("type")
        if not entity_id or entity_type != "lead":
            continue

        lead_id = int(entity_id)
        created_at = int(event.get("created_at") or 0)
        if event_type == "lead_added":
            lead_created_at.setdefault(lead_id, created_at)

        manager_id = get_event_manager_id(event)
        if manager_id in group_users and event_type in {"incoming_call", "outgoing_call", "outgoing_chat_message"}:
            first_contact.setdefault(lead_id, (manager_id, created_at))

    rows_by_manager: dict[int, dict[str, Any]] = {
        manager_id: {
            "manager_id": manager_id,
            "manager": manager_name,
            "new_leads_contacted": 0,
            "within_5_min": 0,
            "within_15_min": 0,
            "within_30_min": 0,
        }
        for manager_id, manager_name in group_users.items()
    }

    for lead_id, created_at in lead_created_at.items():
        contact = first_contact.get(lead_id)
        if not contact:
            continue

        manager_id, contacted_at = contact
        row = rows_by_manager[manager_id]
        delay = contacted_at - created_at
        row["new_leads_contacted"] += 1
        row["within_5_min"] += int(delay <= 5 * 60)
        row["within_15_min"] += int(delay <= 15 * 60)
        row["within_30_min"] += int(delay <= 30 * 60)

    rows = sorted(rows_by_manager.values(), key=lambda row: row["within_15_min"], reverse=True)
    totals = {
        "new_leads": len(lead_created_at),
        "new_leads_contacted": sum(row["new_leads_contacted"] for row in rows),
        "within_5_min": sum(row["within_5_min"] for row in rows),
        "within_15_min": sum(row["within_15_min"] for row in rows),
        "within_30_min": sum(row["within_30_min"] for row in rows),
    }
    totals["without_contact"] = max(0, totals["new_leads"] - totals["new_leads_contacted"])

    return {"totals": totals, "rows": rows}


def build_problem_deals(events: list[dict[str, Any]], group_users: dict[int, str]) -> list[dict[str, Any]]:
    lead_touches: dict[int, dict[str, Any]] = defaultdict(lambda: {"touches": 0, "contacts": 0, "managers": set()})

    for event in events:
        entity_id = event.get("entity_id")
        entity_type = event.get("entity_type")
        manager_id = get_event_manager_id(event)
        if not entity_id or entity_type != "lead" or manager_id not in group_users:
            continue

        lead_id = int(entity_id)
        event_type = event.get("type")
        lead_touches[lead_id]["touches"] += int(event_type in TRACKED_EVENT_TYPES)
        lead_touches[lead_id]["contacts"] += int(event_type in {"incoming_call", "outgoing_call", "outgoing_chat_message"})
        lead_touches[lead_id]["managers"].add(manager_id)

    problems = []
    for lead_id, values in lead_touches.items():
        if values["contacts"] == 0 or values["touches"] <= 1:
            manager_names = [group_users[manager_id] for manager_id in values["managers"]]
            problems.append(
                {
                    "lead_id": lead_id,
                    "managers": ", ".join(manager_names),
                    "touches": values["touches"],
                    "contacts": values["contacts"],
                    "reason": "Нет контакта" if values["contacts"] == 0 else "Мало касаний",
                }
            )

    return sorted(problems, key=lambda item: (item["contacts"], item["touches"]))[:25]


def build_risk_anti_rating(managers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    risks = []
    for manager in managers:
        overdue_points = manager["Просроченные задачи"] * 3
        contacts_gap = max(0, 5 - manager["Звонки и сообщения на сделку"])
        contacts_points = contacts_gap * 2
        followup_gap = max(0, 3 - manager["Сделки с 3+ касаниями"])
        followup_points = followup_gap
        score = (
            overdue_points
            + contacts_points
            + followup_points
        )
        risks.append(
            {
                "manager": manager["Менеджер"],
                "manager_id": manager["ID менеджера"],
                "risk_score": round(score, 1),
                "overdue_tasks": manager["Просроченные задачи"],
                "contacts_per_lead": manager["Звонки и сообщения на сделку"],
                "deals_3_plus": manager["Сделки с 3+ касаниями"],
                "reasons": [
                    {
                        "label": "Просроченные задачи",
                        "value": manager["Просроченные задачи"],
                        "points": round(overdue_points, 1),
                    },
                    {
                        "label": "Мало звонков/сообщений на сделку",
                        "value": manager["Звонки и сообщения на сделку"],
                        "points": round(contacts_points, 1),
                    },
                    {
                        "label": "Мало сделок с 3+ касаниями",
                        "value": manager["Сделки с 3+ касаниями"],
                        "points": round(followup_points, 1),
                    },
                ],
            }
        )

    return sorted(risks, key=lambda item: item["risk_score"], reverse=True)[:10]


def build_funnel(managers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    stage_totals: dict[str, int] = defaultdict(int)
    for manager in managers:
        for stage in manager["Конверсия по этапам"]:
            stage_totals[stage["stage"]] += stage["count"]

    return [
        {"stage": stage, "count": count}
        for stage, count in sorted(stage_totals.items(), key=lambda item: item[1], reverse=True)[:12]
    ]


def build_dashboard_data() -> dict[str, Any]:
    client = AmoCRMClient()
    periods = build_periods()
    users_by_id = fetch_users(client)
    group_users = filter_group_users(users_by_id)

    if not group_users:
        return {
            "group_users_count": 0,
            "periods": [],
            "updated_at": datetime.now(app_timezone()).strftime("%d.%m.%Y %H:%M:%S"),
        }

    month_period = periods[-1]
    events = fetch_events(
        client,
        month_period.started_ts,
        month_period.ended_ts,
        list(group_users.keys()),
    )

    period_reports = []
    for period in periods:
        report = build_period_report(events, period, group_users)
        period_reports.append(
            {
                "title": period.title,
                "date_range": (
                    f"{period.started_at.strftime('%d.%m.%Y %H:%M')} - "
                    f"{period.ended_at.strftime('%d.%m.%Y %H:%M')}"
                ),
                "rows": report.to_dict(orient="records"),
                "totals": report.drop(columns=["ID менеджера", "Менеджер"]).sum().to_dict(),
            }
        )

    return {
        "group_users_count": len(group_users),
        "periods": period_reports,
        "updated_at": datetime.now(app_timezone()).strftime("%d.%m.%Y %H:%M:%S"),
    }


def empty_dashboard() -> dict[str, Any]:
    return {
        "group_users_count": 0,
        "periods": [
            {
                "key": CUSTOM_PERIOD_KEY,
                "title": "Выбранный период",
                "date_range": "Выберите даты и нажмите «Собрать данные»",
                "rows": [],
                "totals": {},
                "managers": [],
                "comparison": [],
                "daily_dynamics": [],
                "heatmap": [],
                "top_activity_hours": [],
                "appeals_heatmap": [],
                "top_appeal_hours": [],
                "appeal_summary": {},
                "lead_quality": {"totals": {}, "rows": []},
                "problem_deals": [],
                "risk_anti_rating": [],
                "funnel": [],
                "per_manager_insights": {},
                "updated_at": "-",
            }
        ],
        "updated_at": "-",
        "selected": {
            "date_from": "",
            "date_to": "",
        },
    }


def build_period_data(
    period_key: str,
    progress_callback=None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> dict[str, Any]:
    client = AmoCRMClient()
    if period_key == CUSTOM_PERIOD_KEY:
        if not date_from or not date_to:
            raise ValueError("Укажите дату начала и дату окончания")
        period = build_custom_period(date_from, date_to)
    else:
        period = build_period_by_key(period_key)

    if progress_callback:
        progress_callback(5, "Загружаем менеджеров Шымкента")

    users_by_id = fetch_users(client)
    group_users = filter_group_users(users_by_id)
    if not group_users:
        return {
            "group_users_count": 0,
            "period": {
                "key": period_key,
                "title": period.title,
                "date_range": "Менеджеры группы Шымкент не найдены",
                "rows": [],
                "totals": {},
                "per_manager_insights": {},
                "updated_at": datetime.now(app_timezone()).strftime("%d.%m.%Y %H:%M:%S"),
            },
        }

    def on_page(page: int, events_count: int) -> None:
        if not progress_callback:
            return

        percent = min(80, 10 + page * 3)
        progress_callback(percent, f"Загружена страница {page}, событий: {events_count}")

    events = fetch_events(
        client,
        period.started_ts,
        period.ended_ts,
        list(group_users.keys()),
        progress_callback=on_page,
    )
    if progress_callback:
        progress_callback(82, "Загружаем предыдущий период для сравнения")
    previous_period = build_previous_period(period)
    previous_events = fetch_events(
        client,
        previous_period.started_ts,
        previous_period.ended_ts,
        list(group_users.keys()),
    )
    if progress_callback:
        progress_callback(86, "Загружаем справочник этапов")
    status_names = fetch_status_names(client)
    if progress_callback:
        progress_callback(90, "Загружаем задачи для проверки просрочек")
    tasks = fetch_tasks(client, period.started_ts, period.ended_ts, list(group_users.keys()))
    if progress_callback:
        progress_callback(94, "Считаем расширенные метрики и риски")

    report = build_period_report(events, period, group_users)
    previous_report = build_period_report(previous_events, previous_period, group_users)
    manager_details = build_manager_details(events, tasks, period, group_users, status_names)
    comparison = build_comparison(report, previous_report)
    daily_dynamics = build_daily_dynamics(events, period, group_users)
    heatmap = build_heatmap(events, group_users)
    top_activity_hours = build_top_activity_hours(heatmap)
    if progress_callback:
        progress_callback(96, "Строим тепловую карту обращений")
    appeal_events = [
        event for event in events
        if event.get("type") in APPEAL_EVENT_TYPES
    ]
    appeals_heatmap = build_event_type_heatmap(appeal_events)
    top_appeal_hours = build_top_activity_hours(appeals_heatmap)
    appeal_summary = build_appeal_summary(appeal_events)
    lead_quality = build_lead_quality(events, group_users)
    problem_deals = build_problem_deals(events, group_users)
    risk_anti_rating = build_risk_anti_rating(manager_details)
    funnel = build_funnel(manager_details)
    per_manager_insights = build_per_manager_insights(
        events, period, group_users, report, previous_report
    )
    updated_at = datetime.now(app_timezone()).strftime("%d.%m.%Y %H:%M:%S")

    if progress_callback:
        progress_callback(100, "Готово")

    return {
        "group_users_count": len(group_users),
        "period": {
            "key": period_key,
            "title": period.title,
            "date_range": (
                f"{period.started_at.strftime('%d.%m.%Y %H:%M')} - "
                f"{period.ended_at.strftime('%d.%m.%Y %H:%M')}"
            ),
            "rows": report.to_dict(orient="records"),
            "totals": report.drop(columns=["ID менеджера", "Менеджер"]).sum().to_dict(),
            "managers": manager_details,
            "comparison": comparison,
            "daily_dynamics": daily_dynamics,
            "heatmap": heatmap,
            "top_activity_hours": top_activity_hours,
            "appeals_heatmap": appeals_heatmap,
            "top_appeal_hours": top_appeal_hours,
            "appeal_summary": appeal_summary,
            "lead_quality": lead_quality,
            "problem_deals": problem_deals,
            "risk_anti_rating": risk_anti_rating,
            "funnel": funnel,
            "per_manager_insights": per_manager_insights,
            "updated_at": updated_at,
        },
        "selected": {
            "date_from": date_from or "",
            "date_to": date_to or "",
        },
    }
