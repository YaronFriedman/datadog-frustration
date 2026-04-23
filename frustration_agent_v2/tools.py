"""v2 tools: filters enforced in the tool; scope and empty-state
guidance returned so the LLM cannot misattribute or omit next-steps.

Design goals driven by Deepchecks findings on v1:
- Scope must be echoed in every tool output so the model can never
  present global data as if it were filtered.
- Empty results must carry concrete next-filter suggestions so the
  model isn't left guessing.
- Filters (domain, frustration_type) are applied inside the tool when
  provided; unused filters never secretly widen the scope.
- `list_top_frustrated_customers` refuses to run when a specific
  domain is already supplied — the tool name encodes "global ranking",
  which is never appropriate for a single-domain question.
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


def _filter_rows_by_domain_and_type(rows: list[dict], domain: str, frustration_type: str) -> list[dict]:
    out = []
    d = domain.lower() if domain else ""
    for r in rows:
        if d and (not r.get("user_domain") or d not in r["user_domain"].lower()):
            continue
        if frustration_type and frustration_type not in (r.get("frustration") or []):
            continue
        out.append(r)
    return out


def _suggest_empty_next_steps(scope: dict) -> list[str]:
    tips: list[str] = []
    if scope.get("frustration_type"):
        tips.append(f"Remove the frustration_type='{scope['frustration_type']}' filter to include all types.")
    if scope.get("domain"):
        tips.append(
            f"Try a different customer domain. The most active domains in the last 168h are usually "
            "moovit.com, wix.com, or gmail.com."
        )
    if (scope.get("hours") or 0) <= 72:
        tips.append("Widen the time window (e.g. hours=168 or 336).")
    if scope.get("exclude_internal"):
        tips.append("Set exclude_internal=False if you suspect the activity is mostly internal staff.")
    if not tips:
        tips.append("Widen the time window or remove filters; there may simply be no data for this scope.")
    return tips


def get_frustration_overview(
    hours: int = 168,
    domain: str = "",
    frustration_type: str = "",
    exclude_internal: bool = True,
    env: str = "",
) -> dict:
    """Fetch Datadog RUM frustration signals and return an aggregated overview, strictly scoped.

    IMPORTANT: When the user specifies a customer domain or a frustration
    type, you MUST pass them here. The tool applies these filters before
    aggregating — results in `by_type`, `top_urls_by_type`,
    `top_targets_by_type`, and `by_customer_domain` only reflect rows that
    match the scope. Report the returned `scope` dict verbatim to the user.

    Args:
        hours: Lookback window in hours. Default 168 (7 days).
        domain: If set, keep only events whose user email domain contains
            this string (e.g. "moovit.com"). Empty string means all customers.
        frustration_type: If set, keep only events with this type
            ("rage_click" | "dead_click" | "error_click"). Empty means all types.
        exclude_internal: If True, exclude @deepchecks.com users. Default True.
        env: Optional Datadog env tag. Empty means no env filter.

    Returns:
        A dict with:
        - status: "ok".
        - scope: {hours, domain, frustration_type, exclude_internal, env}
          — the filters ACTUALLY applied. Any number in this result is
          scoped to these filters; do not describe it as "global".
        - empty: True iff no rows matched the scope.
        - suggested_next_filters: list[str] present only when empty=True.
        - fetched: raw events pulled from Datadog before filtering.
        - excluded_internal: count of internal rows dropped.
        - total_frustrations, total_actions.
        - by_type: {type: count} within scope.
        - by_customer_domain: {domain: count} within scope (single entry if domain filter set).
        - top_urls_by_type, top_targets_by_type: top 10 each within scope.
    """
    excluded = DEFAULT_EXCLUDED_DOMAINS if exclude_internal else ()
    fetched = fetch_frustration_rows(
        hours=hours,
        env=env or None,
        excluded_domains=excluded,
    )
    rows = _filter_rows_by_domain_and_type(fetched["rows"], domain, frustration_type)
    scope = {
        "hours": hours,
        "domain": domain,
        "frustration_type": frustration_type,
        "exclude_internal": exclude_internal,
        "env": env,
    }
    if not rows:
        return {
            "status": "ok",
            "scope": scope,
            "empty": True,
            "suggested_next_filters": _suggest_empty_next_steps(scope),
            "fetched": fetched["fetched"],
            "excluded_internal": fetched["excluded"],
            "total_frustrations": 0,
            "total_actions": 0,
            "by_type": {},
            "by_customer_domain": {},
            "top_urls_by_type": {},
            "top_targets_by_type": {},
        }
    agg = aggregate_frustrations(rows, top_n=10)
    agg.pop("sample_replays_by_type", None)
    return {
        "status": "ok",
        "scope": scope,
        "empty": False,
        "fetched": fetched["fetched"],
        "excluded_internal": fetched["excluded"],
        **agg,
    }


def get_sample_replays(
    hours: int = 168,
    domain: str = "",
    frustration_type: str = "",
    max_per_type: int = 5,
) -> dict:
    """Return session-replay deep links for frustration events, strictly scoped.

    Call this after get_frustration_overview when the user wants to watch
    specific cases. Always pass the same domain and frustration_type as the
    overview call so the links match what the user asked about.

    Args:
        hours: Lookback window in hours. Default 168.
        domain: If set, only include events whose user email domain contains this.
        frustration_type: If set, only include events of this type.
        max_per_type: Max replays returned per type.

    Returns:
        A dict with:
        - status: "ok".
        - scope: filters actually applied.
        - empty: True iff no replays matched.
        - suggested_next_filters: when empty=True, concrete steps to try next.
        - replays_by_type: {type: [{replay_url, user_email, action_name, view_url, timestamp}]}.
    """
    fetched = fetch_frustration_rows(hours=hours, excluded_domains=DEFAULT_EXCLUDED_DOMAINS)
    rows = _filter_rows_by_domain_and_type(fetched["rows"], domain, frustration_type)
    scope = {"hours": hours, "domain": domain, "frustration_type": frustration_type}
    by_type: dict[str, list[dict]] = {}
    for r in rows:
        if not r.get("replay_url"):
            continue
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
    empty = not any(by_type.values())
    result = {"status": "ok", "scope": scope, "empty": empty, "replays_by_type": by_type}
    if empty:
        result["suggested_next_filters"] = _suggest_empty_next_steps({**scope, "exclude_internal": True})
    return result


def get_customer_usage(
    domain: str,
    days: int = 90,
    exclude_emails: list[str] | None = None,
) -> dict:
    """Return usage trend (sessions over time) for a specific customer email domain.

    Use for "is customer X still using us" / churn questions. Always required
    when the user asks about usage, activity, or drop-off.

    Args:
        domain: Email domain to query. Required.
        days: Lookback window in days. Default 90.
        exclude_emails: Exact emails to exclude.

    Returns:
        Dict with status, scope, empty, suggested_next_filters (if empty),
        plus summary metrics identical to v1 (see datadog_rum.summarize_usage).
    """
    if not domain:
        return {"status": "error", "message": "domain is required"}
    rows = fetch_usage_rows(domain=domain, days=days, excluded_emails=exclude_emails or [])
    scope = {"domain": domain, "days": days}
    if not rows:
        return {
            "status": "ok",
            "scope": scope,
            "empty": True,
            "suggested_next_filters": [
                "Widen the time window (try days=180 or 365).",
                "Check if the domain spelling is correct.",
                "Remove any exclude_emails filters.",
            ],
            "users": [],
        }
    summary = summarize_usage(rows, now=datetime.now(timezone.utc))
    return {"status": "ok", "scope": scope, "empty": False, **summary}


def list_top_frustrated_customers(hours: int = 168, top_n: int = 10, domain: str = "") -> dict:
    """Rank customer domains by total frustration-signal volume.

    USE ONLY when the user has NOT specified a domain. If a domain is given,
    pass it here and this tool will refuse (status="skipped") so the agent
    does not mix global rankings into a domain-specific analysis.

    Args:
        hours: Lookback window in hours. Default 168.
        top_n: Number of domains to return. Default 10.
        domain: If non-empty, the tool refuses to run — pass the user's
            requested domain here as a guard when you aren't sure.

    Returns:
        Dict with status ("ok" or "skipped"), scope, and customers=[{domain, signals}].
    """
    if domain:
        return {
            "status": "skipped",
            "scope": {"hours": hours, "requested_domain": domain},
            "reason": (
                f"list_top_frustrated_customers is a global ranking tool and was called with "
                f"domain='{domain}'. A single-domain request does not need this tool. "
                "Use get_frustration_overview(domain=...) instead."
            ),
            "customers": [],
        }
    fetched = fetch_frustration_rows(hours=hours, excluded_domains=DEFAULT_EXCLUDED_DOMAINS)
    agg = aggregate_frustrations(fetched["rows"], top_n=top_n)
    ranked = [
        {"domain": d, "signals": c}
        for d, c in list(agg["by_customer_domain"].items())[:top_n]
    ]
    return {"status": "ok", "scope": {"hours": hours, "top_n": top_n}, "customers": ranked}


def compare_windows(
    hours_a: int,
    hours_b: int,
    domain: str = "",
    frustration_type: str = "",
) -> dict:
    """Compare frustration signal volume between two rolling windows — useful for regression questions.

    Window A is the most recent `hours_a` hours. Window B is the
    `hours_b` hours BEFORE window A (i.e. days `hours_a`..`hours_a+hours_b`
    ago). Filters are applied identically to both windows.

    Args:
        hours_a: Length of the recent window in hours.
        hours_b: Length of the prior window in hours.
        domain: Optional customer domain filter (substring match).
        frustration_type: Optional frustration type filter.

    Returns:
        Dict with status, scope, window_a={counts_by_type,total},
        window_b={counts_by_type,total}, delta_pct.
    """
    from collections import Counter

    def _window(start_hours_ago: float, length_hours: float) -> dict:
        # fetch everything up to start_hours_ago and keep only the chosen window
        fetched = fetch_frustration_rows(
            hours=start_hours_ago + length_hours,
            excluded_domains=DEFAULT_EXCLUDED_DOMAINS,
        )
        rows = _filter_rows_by_domain_and_type(fetched["rows"], domain, frustration_type)
        cutoff_ms_high = (datetime.now(timezone.utc).timestamp() - start_hours_ago * 3600) * 1000
        cutoff_ms_low = cutoff_ms_high - length_hours * 3600 * 1000
        counts: Counter = Counter()
        total = 0
        for r in rows:
            ts = r.get("timestamp")
            try:
                ts_ms = datetime.fromisoformat(str(ts).replace("Z", "+00:00")).timestamp() * 1000
            except (ValueError, AttributeError):
                continue
            if not (cutoff_ms_low <= ts_ms <= cutoff_ms_high):
                continue
            total += 1
            for ft in (r.get("frustration") or []):
                counts[ft] += 1
        return {"counts_by_type": dict(counts), "total": total}

    wa = _window(0, hours_a)
    wb = _window(hours_a, hours_b)
    delta = None
    if wb["total"]:
        delta = round((wa["total"] - wb["total"]) / wb["total"] * 100, 1)
    return {
        "status": "ok",
        "scope": {
            "hours_a": hours_a,
            "hours_b": hours_b,
            "domain": domain,
            "frustration_type": frustration_type,
        },
        "window_a": wa,
        "window_b": wb,
        "delta_pct": delta,
    }


ALL_TOOLS = [
    get_frustration_overview,
    get_sample_replays,
    get_customer_usage,
    list_top_frustrated_customers,
    compare_windows,
]
