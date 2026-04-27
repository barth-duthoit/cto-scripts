"""Databricks Job entry: fetch usage → Delta table.

Configure the Job environment (or cluster env) with:

- ``NOTION_COOKIE`` — browser session cookie (inject from Secrets, e.g.
  ``{{secrets/cto-notion/notion-cookie}}`` where your workspace supports it).
- ``NOTION_AI_DELTA_TABLE`` — Unity Catalog table name, e.g.
  ``main.analytics.notion_ai_usage_top_entities``. Omit or leave empty to skip Delta.

Requires a Databricks Runtime with Delta Lake and PySpark (standard).

Flattening uses ``pandas.json_normalize`` on extracted entity records (see
``usage_response_to_rows``).
"""

from __future__ import annotations

import json
import os
import sys
import traceback
from datetime import datetime, timezone
from typing import Any

import pandas as pd
from pydantic import ValidationError

from notion_ai_usage.env import NotionCookieEnv
from notion_ai_usage.top_entities import (
    NOTION_SPACE_ID,
    fetch_top_entities_by_usage,
    utc_service_period_bounds_ms,
)


def _stderr(msg: str) -> None:
    print(msg, file=sys.stderr)


def _extract_entity_list(data: Any) -> list[Any]:
    if isinstance(data, list):
        return data
    if not isinstance(data, dict):
        return []
    for key in (
        "results",
        "entities",
        "topEntities",
        "items",
        "workflowResults",
        "workflowUsageResults",
        "records",
    ):
        v = data.get(key)
        if isinstance(v, list):
            return v
    rm = data.get("recordMap")
    if isinstance(rm, dict):
        gathered: list[Any] = []
        for bucket in rm.values():
            if isinstance(bucket, dict):
                gathered.extend(bucket.values())
            elif isinstance(bucket, list):
                gathered.extend(bucket)
        if gathered:
            return gathered
    return []


def _ensure_alias(
    df: pd.DataFrame,
    target: str,
    candidates: tuple[str, ...],
) -> None:
    if target in df.columns:
        return
    for c in candidates:
        if c in df.columns:
            df[target] = df[c]
            return
    for col in df.columns:
        leaf = col.rsplit(".", 1)[-1]
        if leaf in candidates:
            df[target] = df[col]
            return


def usage_response_to_rows(
    response: dict[str, Any],
    *,
    space_id: str,
    service_period_start_ms: int,
    service_period_end_ms: int,
    ingested_at: datetime | None = None,
) -> list[dict[str, Any]]:
    """Flatten API JSON with ``pandas.json_normalize`` for Delta."""
    ts = ingested_at or datetime.now(timezone.utc)
    ingested_iso = ts.isoformat()

    items = _extract_entity_list(response)
    if not items:
        return [
            {
                "ingested_at": ingested_iso,
                "space_id": space_id,
                "service_period_start_ms": service_period_start_ms,
                "service_period_end_ms": service_period_end_ms,
                "entity_index": -1,
                "entity_id": None,
                "title": None,
                "usage": None,
                "entity_json": json.dumps(response, ensure_ascii=False),
                "parse_note": "no_entity_list_fallback_raw_response",
            }
        ]

    df = pd.json_normalize(items)
    df["entity_index"] = range(len(df))
    df["ingested_at"] = ingested_iso
    df["space_id"] = space_id
    df["service_period_start_ms"] = service_period_start_ms
    df["service_period_end_ms"] = service_period_end_ms
    df["entity_json"] = [json.dumps(x, ensure_ascii=False) for x in items]
    df["parse_note"] = None

    _ensure_alias(
        df,
        "title",
        ("title", "name", "workflowName", "displayName", "label"),
    )
    _ensure_alias(
        df,
        "usage",
        ("usage", "totalUsage", "credits", "meteredUsage", "aggregateUsage", "count"),
    )
    _ensure_alias(df, "entity_id", ("id", "workflowId", "entityId", "blockId"))

    # JSON round-trip yields native Python types for Spark / APIs.
    return json.loads(df.to_json(orient="records", date_format="iso"))


def write_delta_append(rows: list[dict[str, object]], table_name: str) -> None:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    if not rows:
        _stderr("write_delta_append: no rows; skipping.")
        return
    df = spark.createDataFrame(rows)
    df.write.format("delta").mode("append").saveAsTable(table_name)


def run_pipeline() -> int:
    delta_table = os.environ.get("NOTION_AI_DELTA_TABLE", "").strip()

    try:
        cookie_env = NotionCookieEnv()
    except ValidationError as e:
        _stderr(
            "Missing NOTION_COOKIE. Set it in the Job environment "
            "(e.g. from Databricks Secrets).\n"
            f"{e}"
        )
        return 1

    ps, pe = utc_service_period_bounds_ms()
    ingested = datetime.now(timezone.utc)

    try:
        raw = fetch_top_entities_by_usage(cookie_env.NOTION_COOKIE.strip())
    except Exception as e:
        _stderr(f"Fetch failed: {e}\n{traceback.format_exc()}")
        return 1

    rows = usage_response_to_rows(
        raw,
        space_id=NOTION_SPACE_ID,
        service_period_start_ms=ps,
        service_period_end_ms=pe,
        ingested_at=ingested,
    )

    _stderr(f"Parsed {len(rows)} row(s); space={NOTION_SPACE_ID} period_ms=({ps}, {pe})")

    if delta_table:
        try:
            write_delta_append(rows, delta_table)
            _stderr(f"Appended to Delta table {delta_table}.")
        except Exception as e:
            _stderr(f"Delta write failed: {e}\n{traceback.format_exc()}")
            return 1
    else:
        _stderr("NOTION_AI_DELTA_TABLE not set; skipping Delta write.")

    if os.environ.get("NOTION_AI_PRINT_JSON"):
        print(json.dumps(raw, indent=2, ensure_ascii=False))

    return 0


def main() -> int:
    return run_pipeline()


if __name__ == "__main__":
    raise SystemExit(main())
