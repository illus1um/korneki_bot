"""SQLite-based analytics for the bot: user tracking and event logging."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import AsyncIterator, Iterable, Optional

import aiosqlite

DB_PATH = Path(__file__).resolve().parent.parent / "korneki.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    user_id     INTEGER PRIMARY KEY,
    first_seen  TEXT NOT NULL,
    last_seen   TEXT NOT NULL,
    lang        TEXT,
    username    TEXT,
    first_name  TEXT
);

CREATE TABLE IF NOT EXISTS events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id      INTEGER NOT NULL,
    ts           TEXT NOT NULL,
    event_type   TEXT NOT NULL,
    lang         TEXT,
    section_key  TEXT
);

CREATE INDEX IF NOT EXISTS idx_events_ts       ON events(ts);
CREATE INDEX IF NOT EXISTS idx_events_user_ts  ON events(user_id, ts);
CREATE INDEX IF NOT EXISTS idx_events_type_ts  ON events(event_type, ts);
"""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class Analytics:
    def __init__(self, db_path: Path = DB_PATH) -> None:
        self.db_path = db_path
        self._conn: Optional[aiosqlite.Connection] = None

    async def init(self) -> None:
        self._conn = await aiosqlite.connect(self.db_path)
        await self._conn.executescript(SCHEMA)
        await self._conn.commit()
        logging.info("Analytics DB ready at %s", self.db_path)

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    @property
    def conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("Analytics.init() must be called before use")
        return self._conn

    async def upsert_user(
        self,
        user_id: int,
        lang: Optional[str] = None,
        username: Optional[str] = None,
        first_name: Optional[str] = None,
    ) -> None:
        now = _now_iso()
        await self.conn.execute(
            """
            INSERT INTO users (user_id, first_seen, last_seen, lang, username, first_name)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                last_seen = excluded.last_seen,
                lang      = COALESCE(excluded.lang,      users.lang),
                username  = COALESCE(excluded.username,  users.username),
                first_name= COALESCE(excluded.first_name,users.first_name)
            """,
            (user_id, now, now, lang, username, first_name),
        )
        await self.conn.commit()

    async def log_event(
        self,
        user_id: int,
        event_type: str,
        lang: Optional[str] = None,
        section_key: Optional[str] = None,
    ) -> None:
        await self.conn.execute(
            "INSERT INTO events (user_id, ts, event_type, lang, section_key) VALUES (?, ?, ?, ?, ?)",
            (user_id, _now_iso(), event_type, lang, section_key),
        )
        await self.conn.commit()

    async def total_users(self) -> int:
        async with self.conn.execute("SELECT COUNT(*) FROM users") as cur:
            row = await cur.fetchone()
            return int(row[0]) if row else 0

    async def active_users_since(self, since: datetime) -> int:
        async with self.conn.execute(
            "SELECT COUNT(DISTINCT user_id) FROM events WHERE ts >= ?",
            (since.isoformat(timespec="seconds"),),
        ) as cur:
            row = await cur.fetchone()
            return int(row[0]) if row else 0

    async def language_split(self) -> dict[str, int]:
        async with self.conn.execute(
            "SELECT lang, COUNT(*) FROM users WHERE lang IS NOT NULL GROUP BY lang"
        ) as cur:
            return {lang: int(count) async for lang, count in cur}

    async def top_sections(
        self,
        event_types: Iterable[str],
        since: Optional[datetime] = None,
        limit: int = 10,
    ) -> list[tuple[str, str, int]]:
        placeholders = ",".join("?" for _ in event_types)
        params: list = list(event_types)
        where_ts = ""
        if since is not None:
            where_ts = " AND ts >= ?"
            params.append(since.isoformat(timespec="seconds"))
        query = (
            f"SELECT event_type, section_key, COUNT(*) AS c "
            f"FROM events "
            f"WHERE event_type IN ({placeholders}) AND section_key IS NOT NULL{where_ts} "
            f"GROUP BY event_type, section_key "
            f"ORDER BY c DESC "
            f"LIMIT ?"
        )
        params.append(limit)
        async with self.conn.execute(query, params) as cur:
            return [(row[0], row[1], int(row[2])) async for row in cur]

    async def events_per_day(
        self, since: datetime, until: Optional[datetime] = None
    ) -> list[tuple[str, int, int]]:
        until = until or datetime.now(timezone.utc)
        query = (
            "SELECT substr(ts, 1, 10) AS day, "
            "       COUNT(*) AS events, "
            "       COUNT(DISTINCT user_id) AS users "
            "FROM events "
            "WHERE ts >= ? AND ts < ? "
            "GROUP BY day ORDER BY day"
        )
        async with self.conn.execute(
            query, (since.isoformat(timespec="seconds"), until.isoformat(timespec="seconds"))
        ) as cur:
            return [(row[0], int(row[1]), int(row[2])) async for row in cur]

    async def new_users_per_day(
        self, since: datetime, until: Optional[datetime] = None
    ) -> list[tuple[str, int]]:
        until = until or datetime.now(timezone.utc)
        query = (
            "SELECT substr(first_seen, 1, 10) AS day, COUNT(*) "
            "FROM users "
            "WHERE first_seen >= ? AND first_seen < ? "
            "GROUP BY day ORDER BY day"
        )
        async with self.conn.execute(
            query, (since.isoformat(timespec="seconds"), until.isoformat(timespec="seconds"))
        ) as cur:
            return [(row[0], int(row[1])) async for row in cur]


analytics = Analytics()


@asynccontextmanager
async def lifespan() -> AsyncIterator[Analytics]:
    await analytics.init()
    try:
        yield analytics
    finally:
        await analytics.close()


def days_ago(days: int) -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=days)
