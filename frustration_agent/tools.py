"""ADK tools for the frustration agent.

Each function is an ADK tool — its signature and Google-style docstring
are what ADK turns into the LLM-visible schema. Keep docstrings precise.
"""
from __future__ import annotations

from datetime import datetime, timezone

from datadog_rum import (
    DEFAULT_EXCLUDED_DOMAINS,
    aggregate_frustrations,
    fetch_frustration_rows,
    fetch_usage_rows,
    summarize_usage,
)


def get_frustration_overview(
    hours: int = 168,
    exclude_internal: bool = True,
    env: str = "",
) -> dict:
    """Fetch Datadog RUM frustration signals and return an aggregated overview.

    Use this when the user asks "what is frustrating our users", "show me
    dead/rage/error clicks", or wants a high-level picture of UX issues.

    Args:
        hours: Lookback window in hours. Default 168 (7 days).
        exclude_internal: If True, exclude sessions whose user email is from
            deepchecks.com (internal staff). Default True.
        env: Optional Datadog env tag to filter on (e.g. "prod"). Empty string
            means no env filter.

    Returns:
        A dict with keys:
        - status: "ok" or "error".
        - total_frustrations, total_actions: overall counts.
        - by_type: map of rage_click/dead_click/error_click -> count.
        - by_customer_domain: {email_domain: count}, top 20.
        - top_urls_by_type: {type: [(url, count), ...]} up to 10 per type.
        - top_targets_by_type: {type: [(target_name, count), ...]} up to 10 per type.
        - fetched, excluded: number of raw events fetched and internal ones excluded.
    """
    excluded = DEFAULT_EXCLUDED_DOMAINS if exclude_internal else ()
    fetched = fetch_frustration_rows(
        hours=hours,
        env=env or None,
        excluded_domains=excluded,
    )
    agg = aggregate_frustrations(fetched["rows"], top_n=10)
    agg.pop("sample_replays_by_type", None)
    return {
        "status": "ok",
        "fetched": fetched["fetched"],
        "excluded": fetched["excluded"],
        **agg,
    }


def get_sample_replays(
    hours: int = 168,
    frustration_type: str = "",
    domain: str = "",
    max_per_type: int = 5,
) -> dict:
    """Return session-replay deep links for frustration events so a human can watch them.

    Call this after get_frustration_overview when the user wants to investigate
    specific cases — e.g. "show me replays of rage clicks", "send me moovit
    replay links".

    Args:
        hours: Lookback window in hours. Default 168 (7 days).
        frustration_type: If set, only return replays for this type
            (rage_click | dead_click | error_click). Empty means all types.
        domain: If set, only include events whose user email matches this
            domain substring (e.g. "moovit.com"). Empty means all customers.
        max_per_type: Max number of replays to return per frustration type.

    Returns:
        A dict with:
        - status: "ok" or "error".
        - replays_by_type: {type: [{replay_url, user_email, action_name, view_url}, ...]}
    """
    fetched = fetch_frustration_rows(
        hours=hours,
        excluded_domains=DEFAULT_EXCLUDED_DOMAINS,
    )
    filtered = []
    for r in fetched["rows"]:
        if domain and (not r.get("user_domain") or domain.lower() not in r["user_domain"].lower()):
            continue
        if frustration_type and frustration_type not in (r.get("frustration") or []):
            continue
        if not r.get("replay_url"):
            continue
        filtered.append(r)
    by_type: dict[str, list[dict]] = {}
    for r in filtered:
        for ftype in r["frustration"]:
            if frustration_type and ftype != frustration_type:
                continue
            bucket = by_type.setdefault(ftype, [])
            if len(bucket) >= max_per_type:
                continue
            bucket.append({
                "replay_url": r["replay_url"],
                "user_email": r.get("user_email"),
                "action_name": r.get("action_name"),
                "view_url": r.get("view_url"),
                "timestamp": r.get("timestamp"),
            })
    return {"status": "ok", "replays_by_type": by_type}


def get_customer_usage(
    domain: str,
    days: int = 90,
    exclude_emails: list[str] | None = None,
) -> dict:
    """Return usage trend for a customer email domain: last seen, per-user frequency, 14d delta.

    Use when the user asks "is customer X still using us", "when did Y last
    log in", or wants to know activity for a given domain.

    Args:
        domain: Email domain to query (e.g. "moovit.com"). Required.
        days: Lookback window in days. Default 90.
        exclude_emails: Exact emails to exclude from the analysis.

    Returns:
        A dict with:
        - status: "ok" or "error".
        - domain, days_window: echoed inputs.
        - total_sessions, unique_users: overall counts.
        - sessions_last_14d, sessions_prev_14d, delta_pct: recent trend.
        - users: per-user breakdown with email, total_sessions, first_seen,
          last_seen, days_since_last, avg_sessions_per_week, sessions_last_14d,
          sessions_prev_14d.
        - sessions_per_week: [[iso_week, count], ...] for plotting.
    """
    if not domain:
        return {"status": "error", "message": "domain is required"}
    rows = fetch_usage_rows(
        domain=domain,
        days=days,
        excluded_emails=exclude_emails or [],
    )
    summary = summarize_usage(rows, now=datetime.now(timezone.utc))
    return {"status": "ok", "domain": domain, "days_window": days, **summary}


def list_top_frustrated_customers(hours: int = 168, top_n: int = 10) -> dict:
    """Rank customer domains by total frustration-signal volume.

    Use when the user asks "which customer is hurting most", "who should we
    prioritize", or wants a ranked list.

    Args:
        hours: Lookback window in hours. Default 168 (7 days).
        top_n: How many domains to return. Default 10.

    Returns:
        A dict with status and customers=[{domain, signals}].
    """
    fetched = fetch_frustration_rows(hours=hours, excluded_domains=DEFAULT_EXCLUDED_DOMAINS)
    agg = aggregate_frustrations(fetched["rows"], top_n=top_n)
    ranked = [
        {"domain": d, "signals": c}
        for d, c in list(agg["by_customer_domain"].items())[:top_n]
    ]
    return {"status": "ok", "customers": ranked}


ALL_TOOLS = [
    get_frustration_overview,
    get_sample_replays,
    get_customer_usage,
    list_top_frustrated_customers,
]
