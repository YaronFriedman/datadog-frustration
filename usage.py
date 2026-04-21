#!/usr/bin/env python3
"""Pull RUM sessions by email-domain filter and report usage trend."""
import argparse
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv

load_dotenv()
if not os.environ.get("SSL_CERT_FILE"):
    try:
        import certifi
        os.environ["SSL_CERT_FILE"] = certifi.where()
    except ImportError:
        pass

from datadog_api_client import ApiClient, Configuration
from datadog_api_client.v2.api.rum_api import RUMApi
from datadog_api_client.v2.model.rum_query_filter import RUMQueryFilter
from datadog_api_client.v2.model.rum_query_options import RUMQueryOptions
from datadog_api_client.v2.model.rum_query_page_options import RUMQueryPageOptions
from datadog_api_client.v2.model.rum_search_events_request import RUMSearchEventsRequest
from datadog_api_client.v2.model.rum_sort import RUMSort


def fetch_sessions(days: int, domain: str, app_id: str | None, limit: int = 20000) -> list[dict]:
    cfg = Configuration()
    cfg.api_key["apiKeyAuth"] = os.environ["DD_API_KEY"]
    cfg.api_key["appKeyAuth"] = os.environ["DD_APP_KEY"]
    cfg.server_variables["site"] = os.getenv("DD_SITE", "datadoghq.com")

    q = f"@type:session @session.type:user @usr.name:*@{domain}"
    if app_id:
        q += f" @application.id:{app_id}"

    to_t = datetime.now(timezone.utc)
    from_t = to_t - timedelta(days=days)

    out: list[dict] = []
    cursor: str | None = None
    with ApiClient(cfg) as c:
        api = RUMApi(c)
        while len(out) < limit:
            page = RUMQueryPageOptions(limit=min(1000, limit - len(out)))
            if cursor:
                page.cursor = cursor
            req = RUMSearchEventsRequest(
                filter=RUMQueryFilter(query=q, _from=from_t.isoformat(), to=to_t.isoformat()),
                options=RUMQueryOptions(timezone="UTC"),
                page=page,
                sort=RUMSort.TIMESTAMP_DESCENDING,
            )
            r = api.search_rum_events(body=req).to_dict()
            data = r.get("data", [])
            if not data:
                break
            out.extend(data)
            cursor = r.get("meta", {}).get("page", {}).get("after")
            if not cursor:
                break
    return out


def _session_rows(events: list[dict], excluded_emails: set[str]) -> list[dict]:
    rows = []
    for e in events:
        a = e.get("attributes", {}).get("attributes", {})
        usr = a.get("usr") or {}
        email = (usr.get("email") or usr.get("name") or "").lower()
        if not email or "@" not in email:
            continue
        if email in excluded_emails:
            continue
        sess = a.get("session") or {}
        ts = e.get("attributes", {}).get("timestamp")
        if isinstance(ts, str):
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except ValueError:
                continue
        else:
            continue
        rows.append({
            "email": email,
            "session_id": sess.get("id"),
            "view_count": (a.get("view") or {}).get("count") or sess.get("view", {}).get("count"),
            "dt": dt,
        })
    return rows


def _iso_week(dt: datetime) -> str:
    y, w, _ = dt.isocalendar()
    return f"{y}-W{w:02d}"


def _date(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


def summarize(rows: list[dict], now: datetime) -> dict:
    # Dedup — RUM returns a session-end event; sometimes multiple events per session id.
    by_session: dict[str, dict] = {}
    for r in rows:
        sid = r["session_id"]
        if not sid:
            continue
        if sid not in by_session or r["dt"] > by_session[sid]["dt"]:
            by_session[sid] = r
    sessions = list(by_session.values())
    sessions.sort(key=lambda x: x["dt"])

    last_per_user: dict[str, datetime] = {}
    first_per_user: dict[str, datetime] = {}
    sessions_per_user: Counter = Counter()
    sessions_per_week: Counter = Counter()
    sessions_per_day_recent: Counter = Counter()
    per_user_per_week: dict[str, Counter] = defaultdict(Counter)

    last_14 = now - timedelta(days=14)
    prev_14_start = now - timedelta(days=28)

    recent_14 = 0
    prev_14 = 0
    per_user_recent: Counter = Counter()
    per_user_prev: Counter = Counter()

    for s in sessions:
        u = s["email"]; dt = s["dt"]
        sessions_per_user[u] += 1
        per_user_per_week[u][_iso_week(dt)] += 1
        if u not in first_per_user or dt < first_per_user[u]:
            first_per_user[u] = dt
        if u not in last_per_user or dt > last_per_user[u]:
            last_per_user[u] = dt
        sessions_per_week[_iso_week(dt)] += 1
        if dt >= last_14:
            recent_14 += 1
            per_user_recent[u] += 1
            sessions_per_day_recent[_date(dt)] += 1
        elif dt >= prev_14_start:
            prev_14 += 1
            per_user_prev[u] += 1

    users = sorted(sessions_per_user.keys(), key=lambda u: -sessions_per_user[u])
    user_rows = []
    for u in users:
        last = last_per_user[u]
        first = first_per_user[u]
        span_days = max(1, (last - first).days or 1)
        user_rows.append({
            "email": u,
            "total_sessions": sessions_per_user[u],
            "first_seen": _date(first),
            "last_seen": _date(last),
            "days_since_last": (now - last).days,
            "avg_sessions_per_week_overall": round(sessions_per_user[u] / (span_days / 7), 2),
            "sessions_last_14d": per_user_recent.get(u, 0),
            "sessions_prev_14d": per_user_prev.get(u, 0),
        })

    return {
        "total_sessions": len(sessions),
        "unique_users": len(sessions_per_user),
        "sessions_last_14d": recent_14,
        "sessions_prev_14d": prev_14,
        "delta_pct": (
            round((recent_14 - prev_14) / prev_14 * 100, 1) if prev_14 else None
        ),
        "sessions_per_week": sorted(sessions_per_week.items()),
        "users": user_rows,
    }


def print_report(domain: str, excluded: set[str], summary: dict, days: int) -> None:
    print(f"\n=== {domain} usage report (last {days}d, excluded: {sorted(excluded) or 'none'}) ===")
    print(f"Total sessions: {summary['total_sessions']}")
    print(f"Unique users: {summary['unique_users']}")
    print(f"Sessions last 14d: {summary['sessions_last_14d']}   prev 14d: {summary['sessions_prev_14d']}   delta: {summary['delta_pct']}%")
    print()
    print("Weekly session volume:")
    for w, c in summary["sessions_per_week"]:
        bar = "#" * min(c, 60)
        print(f"  {w}  {c:4d}  {bar}")
    print()
    print("Per-user breakdown (sorted by total):")
    print(f"  {'email':<38} {'total':>6} {'first':>12} {'last':>12} {'d_since':>8} {'wk_avg':>7} {'L14':>5} {'P14':>5}")
    for u in summary["users"]:
        print(f"  {u['email']:<38} {u['total_sessions']:>6} {u['first_seen']:>12} {u['last_seen']:>12} "
              f"{u['days_since_last']:>8} {u['avg_sessions_per_week_overall']:>7} "
              f"{u['sessions_last_14d']:>5} {u['sessions_prev_14d']:>5}")


def main() -> int:
    p = argparse.ArgumentParser(description="RUM usage trend for a customer email domain.")
    p.add_argument("--domain", required=True, help="Customer email domain (e.g. wix.com).")
    p.add_argument("--days", type=int, default=90, help="Lookback window in days.")
    p.add_argument("--exclude-email", action="append", default=[], help="Exact email to exclude (repeatable).")
    p.add_argument("--app-id", help="Filter to a specific RUM application ID.")
    args = p.parse_args()

    for key in ("DD_API_KEY", "DD_APP_KEY"):
        if not os.getenv(key):
            print(f"Missing env var: {key}", file=sys.stderr)
            return 1

    excluded = {e.lower() for e in args.exclude_email}
    print(f"Fetching sessions for @{args.domain} over last {args.days}d...", file=sys.stderr)
    events = fetch_sessions(args.days, args.domain, args.app_id)
    print(f"Got {len(events)} session events.", file=sys.stderr)
    rows = _session_rows(events, excluded)
    summary = summarize(rows, datetime.now(timezone.utc))
    print_report(args.domain, excluded, summary, args.days)
    return 0


if __name__ == "__main__":
    sys.exit(main())
