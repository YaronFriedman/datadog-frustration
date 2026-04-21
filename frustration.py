#!/usr/bin/env python3
"""Pull Datadog RUM frustration signals and summarize with Claude."""
import argparse
import json
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

from anthropic import Anthropic
from datadog_api_client import ApiClient, Configuration
from datadog_api_client.v2.api.rum_api import RUMApi
from datadog_api_client.v2.model.rum_query_filter import RUMQueryFilter
from datadog_api_client.v2.model.rum_query_options import RUMQueryOptions
from datadog_api_client.v2.model.rum_query_page_options import RUMQueryPageOptions
from datadog_api_client.v2.model.rum_search_events_request import RUMSearchEventsRequest
from datadog_api_client.v2.model.rum_sort import RUMSort
from dotenv import load_dotenv

load_dotenv()

if not os.environ.get("SSL_CERT_FILE"):
    try:
        import certifi
        os.environ["SSL_CERT_FILE"] = certifi.where()
    except ImportError:
        pass

FRUSTRATION_QUERY = "@type:action @session.type:user @action.frustration.type:*"


def fetch_frustration_events(hours: int, app_id: str | None, limit: int) -> list[dict]:
    """Fetch RUM action events with frustration signals."""
    cfg = Configuration()
    cfg.api_key["apiKeyAuth"] = os.environ["DD_API_KEY"]
    cfg.api_key["appKeyAuth"] = os.environ["DD_APP_KEY"]
    cfg.server_variables["site"] = os.getenv("DD_SITE", "datadoghq.com")

    query = FRUSTRATION_QUERY
    if app_id:
        query += f" @application.id:{app_id}"

    to_time = datetime.now(timezone.utc)
    from_time = to_time - timedelta(hours=hours)

    events: list[dict] = []
    cursor: str | None = None
    page_size = min(1000, limit)

    with ApiClient(cfg) as client:
        api = RUMApi(client)
        while len(events) < limit:
            page = RUMQueryPageOptions(limit=page_size)
            if cursor:
                page.cursor = cursor
            req = RUMSearchEventsRequest(
                filter=RUMQueryFilter(
                    query=query,
                    _from=from_time.isoformat(),
                    to=to_time.isoformat(),
                ),
                options=RUMQueryOptions(timezone="UTC"),
                page=page,
                sort=RUMSort.TIMESTAMP_DESCENDING,
            )
            resp = api.search_rum_events(body=req)
            data = resp.to_dict().get("data", [])
            if not data:
                break
            events.extend(data)
            cursor = resp.to_dict().get("meta", {}).get("page", {}).get("after")
            if not cursor:
                break

    return events[:limit]


def _replay_url(site: str, app_id: str | None, session_id: str | None, view_id: str | None, ts_ms: int | None) -> str | None:
    if not (app_id and session_id):
        return None
    base = f"https://app.{site}/rum/replay/sessions/{session_id}"
    params = [f"application_id={app_id}"]
    if view_id:
        params.append(f"view_id={view_id}")
    if ts_ms:
        params.append(f"seek={ts_ms}")
    return f"{base}?{'&'.join(params)}"


def extract_signals(events: list[dict], site: str) -> list[dict]:
    """Keep only the fields useful for pattern summarization."""
    rows = []
    for e in events:
        attrs = e.get("attributes", {}).get("attributes", {})
        action = attrs.get("action", {}) or {}
        frustration_types = (action.get("frustration") or {}).get("type") or []
        if not frustration_types:
            continue
        view = attrs.get("view", {}) or {}
        app = attrs.get("application", {}) or {}
        session = attrs.get("session", {}) or {}
        target = action.get("target", {}) or {}
        ts = e.get("attributes", {}).get("timestamp")
        ts_ms = None
        if isinstance(ts, str):
            try:
                ts_ms = int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp() * 1000)
            except ValueError:
                ts_ms = None
        rows.append({
            "frustration": frustration_types,
            "action_type": action.get("type"),
            "action_name": action.get("name") or target.get("name"),
            "target_selector": target.get("selector"),
            "view_url": view.get("url"),
            "view_name": view.get("name"),
            "application_id": app.get("id"),
            "session_id": session.get("id"),
            "error_count": (action.get("error") or {}).get("count"),
            "replay_url": _replay_url(site, app.get("id"), session.get("id"), view.get("id"), ts_ms),
        })
    return rows


def aggregate(rows: list[dict]) -> dict:
    by_type: Counter = Counter()
    by_url: dict[str, Counter] = defaultdict(Counter)
    by_target: dict[str, Counter] = defaultdict(Counter)
    sample_replays: dict[str, list[str]] = defaultdict(list)
    for r in rows:
        for ftype in r["frustration"]:
            by_type[ftype] += 1
            if r["view_url"]:
                by_url[ftype][r["view_url"]] += 1
            label = r["action_name"] or r["target_selector"] or "<unknown>"
            by_target[ftype][label] += 1
            if r.get("replay_url") and len(sample_replays[ftype]) < 5:
                sample_replays[ftype].append(r["replay_url"])
    return {
        "total_frustrations": sum(by_type.values()),
        "total_actions": len(rows),
        "by_type": dict(by_type),
        "top_urls_by_type": {k: v.most_common(10) for k, v in by_url.items()},
        "top_targets_by_type": {k: v.most_common(10) for k, v in by_target.items()},
        "sample_replays_by_type": dict(sample_replays),
    }


def summarize_with_claude(agg: dict, sample: list[dict], hours: int) -> str:
    client = Anthropic()
    prompt = f"""You are analyzing Datadog RUM frustration signals from the last {hours} hours.

Frustration signal types:
- rage_click: user clicks same element repeatedly, feature likely broken or unresponsive
- dead_click: click does nothing
- error_click: click causes a JS error

Aggregate stats:
{json.dumps(agg, indent=2, default=str)}

Sample raw events (first 50):
{json.dumps(sample[:50], indent=2, default=str)}

Give a concise report:
1. Top user frustrations (ranked by impact)
2. Specific URLs / UI targets that are hotspots
3. Likely root-cause hypotheses
4. Recommended next investigation steps

Be direct. Bullet points. No preamble."""

    msg = client.messages.create(
        model="claude-opus-4-7",
        max_tokens=2048,
        messages=[{"role": "user", "content": prompt}],
    )
    return msg.content[0].text


def main() -> int:
    p = argparse.ArgumentParser(description="Summarize Datadog RUM frustration signals.")
    p.add_argument("--hours", type=int, default=24, help="Lookback window in hours.")
    p.add_argument("--app-id", help="Filter to a specific RUM application ID.")
    p.add_argument("--limit", type=int, default=1000, help="Max events to fetch.")
    p.add_argument("--raw", action="store_true", help="Dump aggregated JSON only, skip Claude.")
    args = p.parse_args()

    for key in ("DD_API_KEY", "DD_APP_KEY"):
        if not os.getenv(key):
            print(f"Missing env var: {key}", file=sys.stderr)
            return 1

    site = os.getenv("DD_SITE", "datadoghq.com")
    print(f"Fetching frustration events from last {args.hours}h...", file=sys.stderr)
    events = fetch_frustration_events(args.hours, args.app_id, args.limit)
    rows = extract_signals(events, site)
    print(f"Got {len(events)} events, {len(rows)} with frustration signals.", file=sys.stderr)

    if not rows:
        print("No frustration signals found. Sababa — users happy.", file=sys.stderr)
        return 0

    agg = aggregate(rows)

    if args.raw:
        print(json.dumps({"aggregate": agg, "sample": rows[:50]}, indent=2, default=str))
        return 0

    if not os.getenv("ANTHROPIC_API_KEY"):
        print("Missing ANTHROPIC_API_KEY — use --raw or set the key.", file=sys.stderr)
        return 1

    print("Summarizing with Claude...", file=sys.stderr)
    print(summarize_with_claude(agg, rows, args.hours))
    return 0


if __name__ == "__main__":
    sys.exit(main())
