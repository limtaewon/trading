#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from typing import Any

import requests

from env_bootstrap import bootstrap_openclaw_env

bootstrap_openclaw_env()

CLICKHOUSE_URL = os.environ.get("CLICKHOUSE_URL", "http://localhost:8123").strip()
CLICKHOUSE_DB = os.environ.get("CLICKHOUSE_DB", "trading").strip() or "trading"


def ch_query(sql: str, timeout: int = 120) -> list[dict[str, Any]]:
    resp = requests.post(
        f"{CLICKHOUSE_URL}?database={CLICKHOUSE_DB}&default_format=JSON",
        data=sql.encode("utf-8"),
        timeout=timeout,
    )
    resp.raise_for_status()
    obj = json.loads(resp.text)
    data = obj.get("data", [])
    return data if isinstance(data, list) else []


def ch_exec(sql: str, timeout: int = 120) -> None:
    resp = requests.post(
        f"{CLICKHOUSE_URL}?database={CLICKHOUSE_DB}",
        data=sql.encode("utf-8"),
        timeout=timeout,
    )
    resp.raise_for_status()


def dedupe_key_expr() -> str:
    return """
    if(
        source_url != '',
        source_url,
        concat(replaceRegexpAll(title, '\\\\s+', ' '), '||', toString(published_at))
    )
    """.strip()


def collect_stats() -> dict[str, int]:
    key_expr = dedupe_key_expr()
    stats_sql = f"""
    SELECT
        count() AS rows_total,
        uniqExact(source_url) AS uniq_source_urls,
        uniqExact(tuple(title, published_at)) AS uniq_title_published,
        uniqExact({key_expr}) AS uniq_effective_keys
    FROM trading.news
    FORMAT JSON
    """
    rows = ch_query(stats_sql)
    return rows[0] if rows else {}


def build_dedupe_insert_sql(target_table: str) -> str:
    key_expr = dedupe_key_expr()
    return f"""
    INSERT INTO {target_table}
    SELECT
        id,
        published_at,
        collected_at,
        title,
        summary,
        source_url,
        category,
        importance,
        sentiment,
        impact_type,
        tickers,
        trigger_type,
        embedding
    FROM
    (
        SELECT
            *,
            row_number() OVER (
                PARTITION BY {key_expr}
                ORDER BY collected_at DESC, importance DESC, id DESC
            ) AS rn
        FROM trading.news
    )
    WHERE rn = 1
    """


def main() -> int:
    parser = argparse.ArgumentParser(description="Deduplicate trading.news with backup and table swap")
    parser.add_argument("--apply", action="store_true", help="Actually create backup, rebuild, and swap tables")
    args = parser.parse_args()

    before = collect_stats()
    print(json.dumps({"before": before}, ensure_ascii=False, indent=2))

    if not args.apply:
        return 0

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    tmp_table = f"trading.news_dedup_tmp_{ts}"
    backup_table = f"trading.news_backup_{ts}"

    ch_exec(f"CREATE TABLE {tmp_table} AS trading.news")
    ch_exec(build_dedupe_insert_sql(tmp_table), timeout=600)

    after_rows = ch_query(f"SELECT count() AS rows_total FROM {tmp_table} FORMAT JSON")
    print(json.dumps({"tmp_rows": after_rows[0] if after_rows else {}}, ensure_ascii=False, indent=2))

    ch_exec(f"RENAME TABLE trading.news TO {backup_table}, {tmp_table} TO trading.news", timeout=300)

    after = collect_stats()
    print(json.dumps({"after": after, "backup_table": backup_table}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
