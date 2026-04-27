"""Databricks Job entry: fetch usage → tabular rows → Delta table.

Configure the Job environment (or cluster env) with:

- ``NOTION_COOKIE`` — browser cookie string **or** leave unset and use
  ``DATABRICKS_SECRET_SCOPE`` + ``DATABRICKS_SECRET_KEY_NOTION_COOKIE`` so the job
  loads it via ``dbutils.secrets`` (see ``notion_ai_usage.databricks_secrets``).
- ``NOTION_AI_DELTA_TABLE`` — Unity Catalog table name for **tabular** storage (rows/columns), e.g.
  ``main.analytics.notion_ai_usage_top_entities``. Omit or leave empty to skip Delta.
- ``NOTION_AI_CSV_PATH`` — optional: export the same table to a **comma-separated file** (for Excel
  etc.). Tabular data itself is not “CSV-specific”; it is the flattened DataFrame / Delta rows.

Requires a Databricks Runtime with Delta Lake and PySpark (standard).

Rows are ``pandas.json_normalize`` output plus a few run-metadata columns (see
``usage_response_to_rows``). Nested keys become dotted names, then dots are
replaced with underscores for Delta-friendly columns.
"""

from __future__ import annotations

import json
import os
import sys
import traceback
from datetime import datetime, timezone
from typing import IO, Any, TextIO

import pandas as pd
from pydantic import ValidationError

from notion_ai_usage.databricks_secrets import hydrate_notion_cookie_from_databricks_secrets
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


def _tabular_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Flat column names (no dots) for Spark / SQL."""
    df = df.copy()
    df.columns = [str(c).replace(".", "_") for c in df.columns]
    return df


def usage_response_to_rows(
    response: dict[str, Any],
    *,
    space_id: str,
    service_period_start_ms: int,
    service_period_end_ms: int,
    ingested_at: datetime | None = None,
) -> list[dict[str, Any]]:
    """One row per entity: normalized fields + metadata. Fallback: single row with ``response_json``."""
    ts = ingested_at or datetime.now(timezone.utc)
    ingested_iso = ts.isoformat()

    items = _extract_entity_list(response)
    if not items:
        df = pd.DataFrame(
            [
                {
                    "ingested_at": ingested_iso,
                    "space_id": space_id,
                    "service_period_start_ms": service_period_start_ms,
                    "service_period_end_ms": service_period_end_ms,
                    "entity_index": -1,
                    "response_json": json.dumps(response, ensure_ascii=False),
                }
            ]
        )
        return json.loads(df.to_json(orient="records", date_format="iso"))

    df = _tabular_frame(pd.json_normalize(items))
    df["entity_index"] = range(len(df))
    df["ingested_at"] = ingested_iso
    df["space_id"] = space_id
    df["service_period_start_ms"] = service_period_start_ms
    df["service_period_end_ms"] = service_period_end_ms
    meta = (
        "ingested_at",
        "space_id",
        "service_period_start_ms",
        "service_period_end_ms",
        "entity_index",
    )
    rest = [c for c in df.columns if c not in meta]
    df = df[list(meta) + rest]

    return json.loads(df.to_json(orient="records", date_format="iso"))


def print_tabular(
    rows: list[dict[str, Any]],
    *,
    file: TextIO | None = None,
) -> None:
    """Print rows as an aligned text table (tabular), not comma-separated."""
    out = file if file is not None else sys.stdout
    if not rows:
        print("(no rows)", file=out)
        return
    df = pd.DataFrame(rows)
    with pd.option_context(
        "display.max_columns", None,
        "display.max_rows", None,
        "display.width", None,
        "display.max_colwidth", 72,
    ):
        print(df.to_string(index=False), file=out)


def write_csv_rows(
    rows: list[dict[str, Any]],
    path_or_buf: str | os.PathLike[str] | IO[str],
) -> None:
    """Optional comma-separated export (UTF-8, no index). Prefer ``print_tabular`` for CLI viewing."""
    pd.DataFrame(rows).to_csv(path_or_buf, index=False)


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
    csv_path = os.environ.get("NOTION_AI_CSV_PATH", "").strip()

    hydrate_notion_cookie_from_databricks_secrets()

    try:
        cookie_env = NotionCookieEnv()
    except ValidationError as e:
        _stderr(
            "Missing NOTION_COOKIE. Set NOTION_COOKIE on the job/cluster, "
            "or DATABRICKS_SECRET_SCOPE + DATABRICKS_SECRET_KEY_NOTION_COOKIE "
            "(secret must exist in that scope).\n"
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

    if csv_path:
        try:
            write_csv_rows(rows, csv_path)
            _stderr(f"Wrote CSV to {csv_path}.")
        except OSError as e:
            _stderr(f"CSV write failed: {e}\n{traceback.format_exc()}")
            return 1

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
