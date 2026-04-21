"""Datadog RUM data access. Pure functions — no CLI, no agent glue."""
from __future__ import annotations

import os
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from typing import Iterable

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

FRUSTRATION_QUERY = "@type:action @session.type:user @action.frustration.type:*"
DEFAULT_EXCLUDED_DOMAINS = ("deepchecks.com",)


def _cfg() -> Configuration:
    cfg = Configuration()
    cfg.api_key["apiKeyAuth"] = os.environ["DD_API_KEY"]
    cfg.api_key["appKeyAuth"] = os.environ["DD_APP_KEY"]
    cfg.server_variables["site"] = os.getenv("DD_SITE", "datadoghq.com")
    return cfg


def _paged_search(query: str, hours: float, limit: int) -> list[dict]:
    to_t = datetime.now(timezone.utc)
    from_t = to_t - timedelta(hours=hours)
    events: list[dict] = []
    cursor: str | None = None
    with ApiClient(_cfg()) as client:
        api = RUMApi(client)
        while len(events) < limit:
            page = RUMQueryPageOptions(limit=min(1000, limit - len(events)))
            if cursor:
                page.cursor = cursor
            req = RUMSearchEventsRequest(
                filter=RUMQueryFilter(query=query, _from=from_t.isoformat(), to=to_t.isoformat()),
                options=RUMQueryOptions(timezone="UTC"),
                page=page,
                sort=RUMSort.TIMESTAMP_DESCENDING,
            )
            resp = api.search_rum_events(body=req).to_dict()
            data = resp.get("data", [])
            if not data:
                break
            events.extend(data)
            cursor = resp.get("meta", {}).get("page", {}).get("after")
            if not cursor:
                break
    return events


def _user_email(attrs: dict) -> str | None:
    usr = attrs.get("usr") or {}
    for k in ("email", "name"):
        v = usr.get(k)
        if isinstance(v, str) and "@" in v:
            return v.lower()
    return None


def _domain_excluded(email: str | None, excluded: Iterable[str]) -> bool:
    if not email:
        return False
    domain = email.split("@")[-1]
    return any(d.lower() in domain for d in excluded)


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


def fetch_frustration_rows(
    hours: float = 168,
    app_id: str | None = None,
    env: str | None = None,
    limit: int = 5000,
    excluded_domains: Iterable[str] = DEFAULT_EXCLUDED_DOMAINS,
) -> dict:
    """Fetch frustration action events and return filtered rows + excluded count."""
    query = FRUSTRATION_QUERY
    if app_id:
        query += f" @application.id:{app_id}"
    if env:
        query += f" env:{env}"
    events = _paged_search(query, hours, limit)
    site = os.getenv("DD_SITE", "datadoghq.com")
    rows: list[dict] = []
    excluded = 0
    for e in events:
        attrs = e.get("attributes", {}).get("attributes", {})
        action = attrs.get("action", {}) or {}
        ftypes = (action.get("frustration") or {}).get("type") or []
        if not ftypes:
            continue
        email = _user_email(attrs)
        if _domain_excluded(email, excluded_domains):
            excluded += 1
            continue
        view = attrs.get("view", {}) or {}
        app = attrs.get("application", {}) or {}
        sess = attrs.get("session", {}) or {}
        target = action.get("target", {}) or {}
        ts = e.get("attributes", {}).get("timestamp")
        ts_ms: int | None = None
        if isinstance(ts, str):
            try:
                ts_ms = int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp() * 1000)
            except ValueError:
                pass
        rows.append({
            "frustration": ftypes,
            "action_name": action.get("name") or target.get("name"),
            "target_selector": target.get("selector"),
            "view_url": view.get("url"),
            "view_name": view.get("name"),
            "application_id": app.get("id"),
            "session_id": sess.get("id"),
            "user_email": email,
            "user_domain": email.split("@")[-1] if email else None,
            "timestamp": ts,
            "error_count": (action.get("error") or {}).get("count"),
            "replay_url": _replay_url(site, app.get("id"), sess.get("id"), view.get("id"), ts_ms),
        })
    return {"rows": rows, "fetched": len(events), "excluded": excluded}


def aggregate_frustrations(rows: list[dict], top_n: int = 10) -> dict:
    by_type: Counter = Counter()
    by_url: dict[str, Counter] = defaultdict(Counter)
    by_target: dict[str, Counter] = defaultdict(Counter)
    by_domain: Counter = Counter()
    sample_replays: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        if r.get("user_domain"):
            by_domain[r["user_domain"]] += 1
        for ftype in r["frustration"]:
            by_type[ftype] += 1
            if r["view_url"]:
                by_url[ftype][r["view_url"]] += 1
            label = r["action_name"] or r["target_selector"] or "<unknown>"
            by_target[ftype][label] += 1
            if r.get("replay_url") and len(sample_replays[ftype]) < 5:
                sample_replays[ftype].append({
                    "replay_url": r["replay_url"],
                    "user_email": r.get("user_email"),
                    "action_name": r.get("action_name"),
                    "view_url": r.get("view_url"),
                })
    return {
        "total_frustrations": sum(by_type.values()),
        "total_actions": len(rows),
        "by_type": dict(by_type),
        "by_customer_domain": dict(by_domain.most_common(top_n * 2)),
        "top_urls_by_type": {k: v.most_common(top_n) for k, v in by_url.items()},
        "top_targets_by_type": {k: v.most_common(top_n) for k, v in by_target.items()},
        "sample_replays_by_type": dict(sample_replays),
    }


def fetch_usage_rows(
    domain: str,
    days: float = 90,
    app_id: str | None = None,
    limit: int = 20000,
    excluded_emails: Iterable[str] = (),
) -> list[dict]:
    """Fetch RUM session-end events for users with email domain=`domain`."""
    q = f"@type:session @session.type:user @usr.name:*@{domain}"
    if app_id:
        q += f" @application.id:{app_id}"
    events = _paged_search(q, hours=days * 24, limit=limit)
    excluded = {e.lower() for e in excluded_emails}
    rows: list[dict] = []
    for e in events:
        attrs = e.get("attributes", {}).get("attributes", {})
        email = _user_email(attrs)
        if not email or email in excluded:
            continue
        sess = attrs.get("session") or {}
        ts = e.get("attributes", {}).get("timestamp")
        if not isinstance(ts, str):
            continue
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except ValueError:
            continue
        rows.append({
            "email": email,
            "session_id": sess.get("id"),
            "dt": dt,
        })
    return rows


def summarize_usage(rows: list[dict], now: datetime | None = None) -> dict:
    now = now or datetime.now(timezone.utc)
    by_session: dict[str, dict] = {}
    for r in rows:
        sid = r["session_id"]
        if not sid:
            continue
        if sid not in by_session or r["dt"] > by_session[sid]["dt"]:
            by_session[sid] = r
    sessions = sorted(by_session.values(), key=lambda x: x["dt"])

    per_user_total: Counter = Counter()
    first: dict[str, datetime] = {}
    last: dict[str, datetime] = {}
    per_week: Counter = Counter()
    last_14 = now - timedelta(days=14)
    prev_14_start = now - timedelta(days=28)
    recent_14 = prev_14 = 0
    per_user_recent: Counter = Counter()
    per_user_prev: Counter = Counter()

    for s in sessions:
        u, dt = s["email"], s["dt"]
        per_user_total[u] += 1
        first.setdefault(u, dt)
        if dt < first[u]:
            first[u] = dt
        if u not in last or dt > last[u]:
            last[u] = dt
        y, w, _ = dt.isocalendar()
        per_week[f"{y}-W{w:02d}"] += 1
        if dt >= last_14:
            recent_14 += 1
            per_user_recent[u] += 1
        elif dt >= prev_14_start:
            prev_14 += 1
            per_user_prev[u] += 1

    users = []
    for u in sorted(per_user_total, key=lambda x: -per_user_total[x]):
        span_days = max(1, (last[u] - first[u]).days or 1)
        users.append({
            "email": u,
            "total_sessions": per_user_total[u],
            "first_seen": first[u].strftime("%Y-%m-%d"),
            "last_seen": last[u].strftime("%Y-%m-%d"),
            "days_since_last": (now - last[u]).days,
            "avg_sessions_per_week": round(per_user_total[u] / (span_days / 7), 2),
            "sessions_last_14d": per_user_recent.get(u, 0),
            "sessions_prev_14d": per_user_prev.get(u, 0),
        })

    return {
        "total_sessions": len(sessions),
        "unique_users": len(per_user_total),
        "sessions_last_14d": recent_14,
        "sessions_prev_14d": prev_14,
        "delta_pct": (
            round((recent_14 - prev_14) / prev_14 * 100, 1) if prev_14 else None
        ),
        "sessions_per_week": sorted(per_week.items()),
        "users": users,
    }
