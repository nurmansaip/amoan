import argparse
from datetime import datetime, timedelta
from typing import Any

import pandas as pd

from amo_client import AmoCRMClient


EVENT_TYPES = {
    "lead_added": "Новые сделки",
    "lead_status_changed": "Смены этапов",
    "task_completed": "Закрытые задачи",
    "outgoing_chat_message": "Ответы клиентам",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Аналитика активности менеджеров в amoCRM за выбранный период."
    )
    parser.add_argument(
        "--from",
        dest="date_from",
        help="Начало периода в ISO-формате, например 2026-05-12T09:00:00",
    )
    parser.add_argument(
        "--to",
        dest="date_to",
        help="Конец периода в ISO-формате, например 2026-05-12T18:00:00",
    )
    parser.add_argument(
        "--hours",
        type=int,
        default=24,
        help="Период в часах от текущего момента, если --from и --to не указаны.",
    )
    return parser.parse_args()


def parse_period(args: argparse.Namespace) -> tuple[int, int]:
    """Возвращает границы периода в Unix timestamp для фильтра amoCRM."""
    if args.date_from and args.date_to:
        date_from = datetime.fromisoformat(args.date_from)
        date_to = datetime.fromisoformat(args.date_to)
    else:
        date_to = datetime.now()
        date_from = date_to - timedelta(hours=args.hours)

    if date_from >= date_to:
        raise ValueError("Дата начала периода должна быть меньше даты окончания")

    return int(date_from.timestamp()), int(date_to.timestamp())


def fetch_events(client: AmoCRMClient, created_from: int, created_to: int) -> list[dict[str, Any]]:
    """Загружает события amoCRM постранично, пока API возвращает следующую страницу."""
    events = []
    page = 1

    while True:
        params: list[tuple[str, Any]] = [
            ("filter[created_at][from]", created_from),
            ("filter[created_at][to]", created_to),
            ("limit", 100),
            ("page", page),
        ]
        for event_type in EVENT_TYPES:
            params.append(("filter[type][]", event_type))

        data = client.get("/api/v4/events", params=params)
        page_events = data.get("_embedded", {}).get("events", [])
        events.extend(page_events)

        if not data.get("_links", {}).get("next") or not page_events:
            break

        page += 1

    return events


def fetch_users(client: AmoCRMClient) -> dict[int, str]:
    """Получает имена пользователей, чтобы таблица была понятнее."""
    data = client.get("/api/v4/users", params=[("limit", 250)])
    users = data.get("_embedded", {}).get("users", [])
    return {int(user["id"]): user["name"] for user in users}


def build_report(events: list[dict[str, Any]], users_by_id: dict[int, str]) -> pd.DataFrame:
    rows = []
    for event in events:
        event_type = event.get("type")
        manager_id = event.get("created_by") or event.get("created_by_id")

        if event_type not in EVENT_TYPES or not manager_id:
            continue

        manager_id = int(manager_id)
        rows.append(
            {
                "ID менеджера": manager_id,
                "Менеджер": users_by_id.get(manager_id, f"Пользователь #{manager_id}"),
                "Тип события": EVENT_TYPES[event_type],
            }
        )

    columns = ["ID менеджера", "Менеджер", *EVENT_TYPES.values(), "Всего"]
    if not rows:
        return pd.DataFrame(columns=columns)

    df = pd.DataFrame(rows)
    report = pd.pivot_table(
        df,
        index=["ID менеджера", "Менеджер"],
        columns="Тип события",
        aggfunc="size",
        fill_value=0,
    )

    # Добавляем отсутствующие колонки, если за период не было части типов событий.
    for column in EVENT_TYPES.values():
        if column not in report.columns:
            report[column] = 0

    report = report[list(EVENT_TYPES.values())]
    report["Всего"] = report.sum(axis=1)
    return report.reset_index().sort_values("Всего", ascending=False)


def main() -> None:
    args = parse_args()
    created_from, created_to = parse_period(args)

    client = AmoCRMClient()
    users_by_id = fetch_users(client)
    events = fetch_events(client, created_from, created_to)
    report = build_report(events, users_by_id)

    if report.empty:
        print("За выбранный период активности менеджеров не найдено.")
        return

    print("\nИтоговая активность менеджеров:")
    print(report.to_string(index=False))


if __name__ == "__main__":
    main()
