"""Generate CSV reports from the analytics DB for a given period.

Usage:
    python report.py --from 2026-01-01 --to 2026-04-01 [--out reports/]

Produces:
    summary.csv           — totals, MAU/WAU/DAU, language split
    events_by_day.csv     — day, events, distinct_users
    new_users_by_day.csv  — day, new_users
    top_sections.csv      — event_type, section_key, count
"""

from __future__ import annotations

import argparse
import asyncio
import csv
from datetime import datetime, timezone
from pathlib import Path

from data.analytics import analytics, days_ago


def _parse_date(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def _write_csv(path: Path, header: list[str], rows: list[tuple]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


async def generate_report(since: datetime, until: datetime, out_dir: Path) -> None:
    await analytics.init()
    try:
        total = await analytics.total_users()
        dau = await analytics.active_users_since(days_ago(1))
        wau = await analytics.active_users_since(days_ago(7))
        mau = await analytics.active_users_since(days_ago(30))
        period_active = await analytics.active_users_since(since)
        langs = await analytics.language_split()

        summary_rows: list[tuple] = [
            ("total_users", total),
            ("active_last_24h", dau),
            ("active_last_7d", wau),
            ("active_last_30d", mau),
            (f"active_since_{since.date()}", period_active),
            ("period_from", since.date().isoformat()),
            ("period_to", until.date().isoformat()),
        ]
        for lang, count in sorted(langs.items()):
            summary_rows.append((f"lang_{lang}", count))
        _write_csv(out_dir / "summary.csv", ["metric", "value"], summary_rows)

        events = await analytics.events_per_day(since, until)
        _write_csv(
            out_dir / "events_by_day.csv",
            ["date", "events", "distinct_users"],
            events,
        )

        new_users = await analytics.new_users_per_day(since, until)
        _write_csv(out_dir / "new_users_by_day.csv", ["date", "new_users"], new_users)

        top = await analytics.top_sections(
            event_types=("category", "law_section", "translation_section"),
            since=since,
            limit=100,
        )
        _write_csv(
            out_dir / "top_sections.csv",
            ["event_type", "section_key", "count"],
            top,
        )

        print(f"Report written to {out_dir.resolve()}")
        print(f"  total_users={total}  period_active={period_active}")
        print(f"  languages={dict(sorted(langs.items()))}")
    finally:
        await analytics.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate analytics report for a period.")
    parser.add_argument("--from", dest="since", required=True, help="YYYY-MM-DD (UTC)")
    parser.add_argument("--to", dest="until", required=True, help="YYYY-MM-DD (UTC, exclusive)")
    parser.add_argument("--out", default="reports", help="Output directory")
    args = parser.parse_args()

    since = _parse_date(args.since)
    until = _parse_date(args.until)
    if until <= since:
        raise SystemExit("--to must be after --from")

    out_dir = Path(args.out) / f"{since.date()}_{until.date()}"
    asyncio.run(generate_report(since, until, out_dir))


if __name__ == "__main__":
    main()
