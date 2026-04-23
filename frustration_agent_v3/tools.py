"""v3 tools — hardened contracts.

Design goals informed by Deepchecks' evaluation of v2:
1. Every user-facing filter is a REQUIRED argument (no defaults). The
   model cannot accidentally omit it and silently widen the scope.
2. Input validation returns `status="error"` with a specific `error`
   message when a contract is violated; the agent must self-correct.
3. Tool surface is minimal (4 tools). Redundant ranking tool removed
   — `get_frustration_overview.by_customer_domain` already ranks.
4. Docstrings show at least one exact Example call per tool.
5. Every result includes `scope` so the model can quote what was
   applied verbatim and cannot overstate precision.
6. Empty results ship `suggested_next_filters` so the model always has
   a concrete next step to relay.
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

_VALID_TYPES = {"", "dead_click", "rage_click", "error_click"}


def _validate_hours(hours: int, field: str = "hours") -> dict | None:
    if not isinstance(hours, int) or hours <= 0 or hours > 24 * 60:
        return {"status": "error", "error": f"'{field}' must be an integer between 1 and {24*60}; got {hours!r}"}
    return None


def _validate_type(ft: str) -> dict | None:
    if ft not in _VALID_TYPES:
        return {
            "status": "error",
            "error": f"'frustration_type' must be one of {sorted(_VALID_TYPES)!r}; got {ft!r}",
        }
    return None


def _filter(rows: list[dict], domain: str, frustration_type: str) -> list[dict]:
    out = []
    d = domain.lower()
    for r in rows:
        if d and (not r.get("user_domain") or d not in r["user_domain"].lower()):
            continue
        if frustration_type and frustration_type not in (r.get("frustration") or []):
            continue
        out.append(r)
    return out


def _empty_suggestions(scope: dict) -> list[str]:
    tips: list[str] = []
    if scope.get("frustration_type"):
        tips.append(f"Remove the frustration_type='{scope['frustration_type']}' filter to include all types.")
    if scope.get("domain"):
        tips.append(
            "Try a different customer domain. The highest-volume domains are usually "
            "moovit.com, wix.com, and gmail.com."
        )
    if (scope.get("hours") or 0) <= 72:
        tips.append("Widen the time window (e.g. hours=168 or hours=336).")
    if scope.get("exclude_internal"):
        tips.append("Set exclude_internal=False if internal staff activity may be relevant.")
    if not tips:
        tips.append("Widen the time window or drop filters; this scope simply has no data.")
    return tips


def get_frustration_overview(
    hours: int,
    domain: str,
    frustration_type: str,
    exclude_internal: bool,
) -> dict:
    """Fetch Datadog RUM frustration signals and return a strictly-scoped aggregated overview.

    ALL four arguments are REQUIRED. Pass empty strings for filters that
    the user did not ask for — never omit them.

    Args:
        hours: Lookback window in hours. Integer 1..1440.
        domain: Customer email domain substring (e.g. "moovit.com") or "".
            If "", no domain filter is applied.
        frustration_type: One of "", "dead_click", "rage_click", "error_click".
            If "", all types are included.
        exclude_internal: True to drop @deepchecks.com internal users. Default
            True for user-facing analyses.

    Returns:
        Dict with:
        - status: "ok" or "error" (plus `error` on error).
        - scope: {hours, domain, frustration_type, exclude_internal}
          echoing the exact filters applied. Quote this to the user.
        - empty: True iff no rows matched.
        - suggested_next_filters: list[str] present only when empty=True.
        - fetched: raw events pulled before filtering.
        - excluded_internal: count of internal rows dropped.
        - total_frustrations, total_actions.
        - by_type: {type: count} within scope.
        - by_customer_domain: {domain: count} within scope, top-ranked.
          (This is the canonical "which customers are most affected" ranking.)
        - top_urls_by_type, top_targets_by_type: {type: [(label, count), ...]} top 10 each.

    Example (moovit-only, rage clicks, 7 days):
        get_frustration_overview(hours=168, domain="moovit.com",
                                 frustration_type="rage_click", exclude_internal=True)

    Example (all customers, all types, last 24h):
        get_frustration_overview(hours=24, domain="", frustration_type="",
                                 exclude_internal=True)
    """
    err = _validate_hours(hours) or _validate_type(frustration_type)
    if err:
        return err

    fetched = fetch_frustration_rows(
        hours=hours,
        excluded_domains=DEFAULT_EXCLUDED_DOMAINS if exclude_internal else (),
    )
    rows = _filter(fetched["rows"], domain, frustration_type)
    scope = {
        "hours": hours,
        "domain": domain,
        "frustration_type": frustration_type,
        "exclude_internal": exclude_internal,
    }
    if not rows:
        return {
            "status": "ok",
            "scope": scope,
            "empty": True,
            "suggested_next_filters": _empty_suggestions(scope),
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
    hours: int,
    domain: str,
    frustration_type: str,
    max_per_type: int,
) -> dict:
    """Return Datadog session-replay deep links for frustration events, strictly scoped.

    ALL four arguments are REQUIRED. Pass empty strings for filters the
    user did not ask for. Always pass the SAME domain and frustration_type
    you passed to get_frustration_overview for the same question.

    Args:
        hours: Lookback window in hours. Integer 1..1440.
        domain: Customer email domain substring or "".
        frustration_type: One of "", "dead_click", "rage_click", "error_click".
        max_per_type: Max replays per type. Integer 1..20.

    Returns:
        Dict with status, scope, empty, suggested_next_filters (on empty),
        replays_by_type = {type: [{replay_url, user_email, action_name, view_url, timestamp}, ...]}.

    Example:
        get_sample_replays(hours=168, domain="moovit.com",
                           frustration_type="rage_click", max_per_type=5)
    """
    err = _validate_hours(hours) or _validate_type(frustration_type)
    if err:
        return err
    if not isinstance(max_per_type, int) or not (1 <= max_per_type <= 20):
        return {"status": "error", "error": f"'max_per_type' must be an integer in 1..20; got {max_per_type!r}"}

    fetched = fetch_frustration_rows(hours=hours, excluded_domains=DEFAULT_EXCLUDED_DOMAINS)
    rows = _filter(fetched["rows"], domain, frustration_type)
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
    out: dict = {"status": "ok", "scope": scope, "empty": empty, "replays_by_type": by_type}
    if empty:
        out["suggested_next_filters"] = _empty_suggestions({**scope, "exclude_internal": True})
    return out


def get_customer_usage(domain: str, days: int) -> dict:
    """Return usage trend (sessions over time) for a specific customer email domain.

    Both arguments are REQUIRED. Use this for churn / "is X still using us"
    questions. Never pass domain="" — that is not a valid usage query.

    Args:
        domain: Email domain (e.g. "moovit.com"). Non-empty.
        days: Lookback window in days. Integer 1..365.

    Returns:
        Dict with status, scope, empty, suggested_next_filters (on empty),
        plus the summary fields from datadog_rum.summarize_usage: total_sessions,
        unique_users, sessions_last_14d, sessions_prev_14d, delta_pct,
        sessions_per_week, users.

    Example:
        get_customer_usage(domain="wix.com", days=90)
    """
    if not domain:
        return {"status": "error", "error": "'domain' is required and must be non-empty"}
    if not isinstance(days, int) or not (1 <= days <= 365):
        return {"status": "error", "error": f"'days' must be an integer in 1..365; got {days!r}"}

    rows = fetch_usage_rows(domain=domain, days=days, excluded_emails=[])
    scope = {"domain": domain, "days": days}
    if not rows:
        return {
            "status": "ok",
            "scope": scope,
            "empty": True,
            "suggested_next_filters": [
                "Widen the time window (try days=180 or days=365).",
                "Check that the domain is spelled correctly.",
            ],
            "users": [],
        }
    summary = summarize_usage(rows, now=datetime.now(timezone.utc))
    return {"status": "ok", "scope": scope, "empty": False, **summary}


def compare_windows(
    hours_a: int,
    hours_b: int,
    domain: str,
    frustration_type: str,
) -> dict:
    """Compare frustration-signal volume between two rolling windows (regression analysis).

    ALL four arguments are REQUIRED. Window A is the most recent `hours_a`
    hours. Window B is the `hours_b` hours immediately BEFORE window A.
    The same filters (domain, frustration_type) apply to both windows.

    Args:
        hours_a: Length of the recent window. Integer 1..1440.
        hours_b: Length of the prior window. Integer 1..1440.
        domain: Customer email domain substring or "".
        frustration_type: "", "dead_click", "rage_click", or "error_click".

    Returns:
        Dict with status, scope, window_a = {counts_by_type, total},
        window_b = {counts_by_type, total}, delta_pct.

    Example (is moovit.com rage-clicking more this week vs last?):
        compare_windows(hours_a=168, hours_b=168, domain="moovit.com",
                        frustration_type="rage_click")
    """
    err = _validate_hours(hours_a, "hours_a") or _validate_hours(hours_b, "hours_b") or _validate_type(frustration_type)
    if err:
        return err
    from collections import Counter

    def _win(start_ago: float, length: float) -> dict:
        fetched = fetch_frustration_rows(
            hours=start_ago + length,
            excluded_domains=DEFAULT_EXCLUDED_DOMAINS,
        )
        rows = _filter(fetched["rows"], domain, frustration_type)
        hi = (datetime.now(timezone.utc).timestamp() - start_ago * 3600) * 1000
        lo = hi - length * 3600 * 1000
        counts: Counter = Counter()
        total = 0
        for r in rows:
            ts = r.get("timestamp")
            try:
                ts_ms = datetime.fromisoformat(str(ts).replace("Z", "+00:00")).timestamp() * 1000
            except (ValueError, AttributeError):
                continue
            if not (lo <= ts_ms <= hi):
                continue
            total += 1
            for ft in (r.get("frustration") or []):
                counts[ft] += 1
        return {"counts_by_type": dict(counts), "total": total}

    wa, wb = _win(0, hours_a), _win(hours_a, hours_b)
    delta = round((wa["total"] - wb["total"]) / wb["total"] * 100, 1) if wb["total"] else None
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
    compare_windows,
]
