"""Notion getTopEntitiesByUsage — ``NOTION_COOKIE`` from ``.env``; tabular output on stdout."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from typing import Any

import requests
from pydantic import ValidationError

from notion_ai_usage.env import NotionCookieEnv

# Same workspace / user as ``notion.curl`` snapshot — edit here if needed.
NOTION_SPACE_ID = "3830cb16-8953-40aa-a8af-b8653d0ed8cc"
NOTION_ACTIVE_USER_ID = "31ed872b-594c-8145-aab4-000203075c6d"
NOTION_CLIENT_VERSION = "23.13.20260427.0804"

# Billing-style window in UTC: [22nd 00:00, next 22nd 00:00) containing the request instant.
_SERVICE_PERIOD_ANCHOR_DAY = 22

TOP_ENTITIES_URL = "https://www.notion.so/api/v3/getTopEntitiesByUsage"
TOP_ENTITIES_LIMIT = 500

_BAGGAGE = (
    "sentry-environment=production,"
    "sentry-release=23.13.20260427.0804,"
    "sentry-public_key=704fe3b1898d4ccda1d05fe1ee79a1f7,"
    "sentry-trace_id=6a87e6f17d284d6eb87f8788b9117d79,"
    "sentry-org_id=324374,sentry-sampled=false,"
    "sentry-sample_rand=0.296694536119965,sentry-sample_rate=0.00001"
)
_SENTRY_TRACE = "6a87e6f17d284d6eb87f8788b9117d79-aee6ae9bf27176b4-0"


def utc_service_period_bounds_ms(now: datetime | None = None) -> tuple[int, int]:
    """Return ``(start_ms, end_ms)`` for the UTC half-open interval [anchor, next anchor)."""
    t = datetime.now(timezone.utc) if now is None else now
    if t.tzinfo is None:
        t = t.replace(tzinfo=timezone.utc)
    else:
        t = t.astimezone(timezone.utc)

    y, m, d = t.year, t.month, t.day
    a = _SERVICE_PERIOD_ANCHOR_DAY

    if d >= a:
        sy, sm = y, m
    else:
        sy, sm = (y - 1, 12) if m == 1 else (y, m - 1)

    ey, em = (sy + 1, 1) if sm == 12 else (sy, sm + 1)

    start = datetime(sy, sm, a, 0, 0, 0, tzinfo=timezone.utc)
    end = datetime(ey, em, a, 0, 0, 0, tzinfo=timezone.utc)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)


def fetch_top_entities_by_usage(cookie: str) -> dict[str, Any]:
    start_ms, end_ms = utc_service_period_bounds_ms()
    payload: dict[str, Any] = {
        "spaceId": NOTION_SPACE_ID,
        "servicePeriodStart": start_ms,
        "servicePeriodEnd": end_ms,
        "limit": TOP_ENTITIES_LIMIT,
        "sortDirection": "desc",
        "entityTable": "workflow",
        "forceMetronomeDataPath": False,
    }

    headers = {
        "accept": "*/*",
        "accept-language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
        "baggage": _BAGGAGE,
        "content-type": "application/json",
        "Cookie": cookie.strip(),
        "notion-audit-log-platform": "web",
        "notion-client-version": NOTION_CLIENT_VERSION,
        "origin": "https://www.notion.so",
        "priority": "u=1, i",
        "referer": "https://www.notion.so/ai",
        "sec-ch-ua": '"Google Chrome";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "sentry-trace": _SENTRY_TRACE,
        "user-agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
        ),
        "x-notion-active-user-header": NOTION_ACTIVE_USER_ID,
        "x-notion-space-id": NOTION_SPACE_ID,
    }

    r = requests.post(TOP_ENTITIES_URL, headers=headers, json=payload, timeout=60)
    r.raise_for_status()
    return r.json()


def main() -> int:
    try:
        cookie_env = NotionCookieEnv()
    except ValidationError as e:
        print(
            "Environment validation failed (set NOTION_COOKIE in the environment "
            "or in a ``.env`` file next to ``env.py``):\n",
            e,
            file=sys.stderr,
        )
        return 1

    ps, pe = utc_service_period_bounds_ms()
    start_day = datetime.fromtimestamp(ps / 1000, tz=timezone.utc).date()
    end_day = datetime.fromtimestamp(pe / 1000, tz=timezone.utc).date()
    print(
        f"Notion usage period (UTC, {_SERVICE_PERIOD_ANCHOR_DAY} → next month): "
        f"{start_day} → {end_day} (end exclusive)",
        file=sys.stderr,
    )
    print(f"spaceId (module): {NOTION_SPACE_ID}", file=sys.stderr)

    try:
        data = fetch_top_entities_by_usage(cookie_env.NOTION_COOKIE.strip())
    except requests.HTTPError as e:
        resp = e.response
        body = resp.text if resp is not None else ""
        code = getattr(resp, "status_code", None) if resp is not None else None
        print(f"HTTP {code}: {body}", file=sys.stderr)
        return 1
    except requests.RequestException as e:
        print(str(e), file=sys.stderr)
        return 1

    from notion_ai_usage.databricks_job import print_tabular, usage_response_to_rows

    rows = usage_response_to_rows(
        data,
        space_id=NOTION_SPACE_ID,
        service_period_start_ms=ps,
        service_period_end_ms=pe,
        ingested_at=datetime.now(timezone.utc),
    )
    print_tabular(rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
