"""Load ``NOTION_COOKIE`` from Databricks Secrets when the env var is unset.

Set ``DATABRICKS_SECRET_SCOPE`` and optionally ``DATABRICKS_SECRET_KEY_NOTION_COOKIE``
(default ``notion-cookie``). Runs on the driver after Spark is available (notebook or job).

No-op locally if Spark/dbutils are unavailable.
"""

from __future__ import annotations

import os
import sys


def _dbutils_secrets_get(scope: str, key: str) -> str | None:
    try:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
    except Exception:
        spark = None

    if spark is not None:
        try:
            from pyspark.dbutils import DBUtils

            return DBUtils(spark).secrets.get(scope, key)
        except Exception:
            pass

    main = sys.modules.get("__main__")
    if main is not None:
        dbutils = getattr(main, "dbutils", None)
        if dbutils is not None:
            try:
                return dbutils.secrets.get(scope, key)
            except Exception:
                pass

    try:
        from IPython import get_ipython

        ip = get_ipython()
        if ip is not None:
            dbutils = ip.ns_globals.get("dbutils") or ip.user_ns.get("dbutils")
            if dbutils is not None:
                return dbutils.secrets.get(scope, key)
    except Exception:
        pass

    return None


def hydrate_notion_cookie_from_databricks_secrets() -> None:
    """If ``NOTION_COOKIE`` is empty, fill it from ``dbutils.secrets`` when scope/key are set."""
    if os.environ.get("NOTION_COOKIE", "").strip():
        return
    scope = os.environ.get("cto-scripts", "").strip()
    key = os.environ.get("NOTION_COOKIE", "notion-cookie").strip()
    if not scope:
        return
    val = _dbutils_secrets_get(scope, key)
    if val:
        os.environ["NOTION_COOKIE"] = val
