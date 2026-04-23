"""v4 tools — pre-render strings that must be relayed verbatim.

Design response to v3 Deepchecks findings:

- Previous versions told the model to "quote verbatim". The model
  paraphrased anyway. v4 eliminates the opportunity: the tools return
  pre-rendered strings (`scope_line`, `suggested_next_filters_markdown`,
  `replays_markdown`) that the agent only has to paste. No formatting
  decisions are left to the LLM for these fields.
- `render_hints` tells the agent whether a section must appear. This
  removes the judgment call on when to omit Most Affected Customers.
- `get_sample_replays` now REQUIRES `exclude_internal` — the v3 judge
  flagged its omission. Parity across tools.
- Validation errors remain explicit so the model self-corrects.
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


def _val_hours(h: int, field: str = "hours") -> dict | None:
    if not isinstance(h, int) or h <= 0 or h > 24 * 60:
        return {"status": "error", "error": f"'{field}' must be an integer 1..1440; got {h!r}"}
    return None


def _val_type(t: str) -> dict | None:
    if t not in _VALID_TYPES:
        return {"status": "error", "error": f"'frustration_type' must be one of {sorted(_VALID_TYPES)!r}; got {t!r}"}
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


def _scope_line_overview(hours: int, domain: str, frustration_type: str, exclude_internal: bool) -> str:
    d = domain if domain else '""'
    t = frustration_type if frustration_type else '""'
    return f"Scope: hours={hours}, domain={d}, frustration_type={t}, exclude_internal={exclude_internal}"


def _scope_line_replays(hours: int, domain: str, frustration_type: str, exclude_internal: bool) -> str:
    d = domain if domain else '""'
    t = frustration_type if frustration_type else '""'
    return f"Replay Scope: hours={hours}, domain={d}, frustration_type={t}, exclude_internal={exclude_internal}"


def _next_filters_md(tips: list[str]) -> str:
    return "\n".join(f"- {t}" for t in tips)


def _empty_tips(scope: dict) -> list[str]:
    tips: list[str] = []
    if scope.get("frustration_type"):
        tips.append(f"Remove the frustration_type='{scope['frustration_type']}' filter to include all types.")
    if scope.get("domain"):
        tips.append("Try a different customer domain. The highest-volume domains are usually moovit.com, wix.com, and gmail.com.")
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
    """Fetch Datadog RUM frustration signals and return strictly-scoped aggregates.

    ALL four arguments are REQUIRED. Pass empty strings for unused filters.

    Args:
        hours: Lookback window in hours. Integer 1..1440.
        domain: Customer email domain substring, or "" for no filter.
        frustration_type: One of "", "dead_click", "rage_click", "error_click".
        exclude_internal: True to drop @deepchecks.com internal users.

    Returns pre-rendered strings to be pasted verbatim in the response:
        - scope_line: e.g. 'Scope: hours=168, domain=moovit.com, frustration_type=rage_click, exclude_internal=True'
        - suggested_next_filters_markdown: bullet list (present only on empty=True)
        - render_hints.most_affected_section: "show" (domain="") or "omit" (domain!=""). Follow this literally.

    Also returns the scoped aggregates: by_type, by_customer_domain,
    top_urls_by_type, top_targets_by_type, total_frustrations, etc.

    Example:
        get_frustration_overview(hours=168, domain="moovit.com",
                                 frustration_type="rage_click", exclude_internal=True)
    """
    err = _val_hours(hours) or _val_type(frustration_type)
    if err:
        return err

    fetched = fetch_frustration_rows(
        hours=hours,
        excluded_domains=DEFAULT_EXCLUDED_DOMAINS if exclude_internal else (),
    )
    rows = _filter(fetched["rows"], domain, frustration_type)
    scope = {"hours": hours, "domain": domain, "frustration_type": frustration_type, "exclude_internal": exclude_internal}
    scope_line = _scope_line_overview(hours, domain, frustration_type, exclude_internal)
    render_hints = {"most_affected_section": "omit" if domain else "show"}

    if not rows:
        tips = _empty_tips(scope)
        return {
            "status": "ok",
            "scope": scope,
            "scope_line": scope_line,
            "render_hints": render_hints,
            "empty": True,
            "suggested_next_filters": tips,
            "suggested_next_filters_markdown": _next_filters_md(tips),
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
        "scope_line": scope_line,
        "render_hints": render_hints,
        "empty": False,
        "fetched": fetched["fetched"],
        "excluded_internal": fetched["excluded"],
        **agg,
    }


def _render_replays_md(by_type: dict[str, list[dict]]) -> str:
    lines: list[str] = []
    for ftype, items in by_type.items():
        lines.append(f"**{ftype}**")
        for r in items:
            # Preserve every character (including zero-width spaces) as-is.
            lines.append(
                f'- {r["replay_url"]}  \n'
                f'  user: {r.get("user_email") or "?"}, target: "{r.get("action_name") or ""}", view: {r.get("view_url") or "?"}'
            )
    return "\n".join(lines)


def get_sample_replays(
    hours: int,
    domain: str,
    frustration_type: str,
    exclude_internal: bool,
    max_per_type: int,
) -> dict:
    """Return Datadog session-replay deep links for frustration events, strictly scoped.

    ALL five arguments are REQUIRED. Pass exclude_internal=True unless the
    user explicitly asks for internal users too. Pass empty strings for
    unused filters; never omit them.

    Returns pre-rendered strings to paste verbatim:
        - scope_line: 'Replay Scope: hours=..., domain=..., frustration_type=..., exclude_internal=...'
        - replays_markdown: formatted list of replays (paste as-is)
        - suggested_next_filters_markdown: bullet list (only when empty=True)

    Example:
        get_sample_replays(hours=168, domain="moovit.com",
                           frustration_type="rage_click",
                           exclude_internal=True, max_per_type=5)
    """
    err = _val_hours(hours) or _val_type(frustration_type)
    if err:
        return err
    if not isinstance(max_per_type, int) or not (1 <= max_per_type <= 20):
        return {"status": "error", "error": f"'max_per_type' must be 1..20; got {max_per_type!r}"}

    fetched = fetch_frustration_rows(
        hours=hours,
        excluded_domains=DEFAULT_EXCLUDED_DOMAINS if exclude_internal else (),
    )
    rows = _filter(fetched["rows"], domain, frustration_type)
    scope = {"hours": hours, "domain": domain, "frustration_type": frustration_type, "exclude_internal": exclude_internal}
    scope_line = _scope_line_replays(hours, domain, frustration_type, exclude_internal)

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
    out: dict = {
        "status": "ok",
        "scope": scope,
        "scope_line": scope_line,
        "empty": empty,
        "replays_by_type": by_type,
        "replays_markdown": _render_replays_md(by_type) if not empty else "",
    }
    if empty:
        tips = _empty_tips(scope)
        out["suggested_next_filters"] = tips
        out["suggested_next_filters_markdown"] = _next_filters_md(tips)
    return out


def get_customer_usage(domain: str, days: int) -> dict:
    """Return usage trend (sessions) for one customer email domain.

    Both arguments REQUIRED. Use for churn / "is X still using us" questions.

    Returns scope_line and suggested_next_filters_markdown as pre-rendered
    strings, plus the summary metrics from datadog_rum.summarize_usage.

    Example:
        get_customer_usage(domain="wix.com", days=90)
    """
    if not domain:
        return {"status": "error", "error": "'domain' is required and must be non-empty"}
    if not isinstance(days, int) or not (1 <= days <= 365):
        return {"status": "error", "error": f"'days' must be 1..365; got {days!r}"}

    rows = fetch_usage_rows(domain=domain, days=days, excluded_emails=[])
    scope = {"domain": domain, "days": days}
    scope_line = f"Usage Scope: domain={domain}, days={days}"
    if not rows:
        tips = [
            "Widen the time window (try days=180 or days=365).",
            "Check that the domain is spelled correctly.",
        ]
        return {
            "status": "ok",
            "scope": scope,
            "scope_line": scope_line,
            "empty": True,
            "suggested_next_filters": tips,
            "suggested_next_filters_markdown": _next_filters_md(tips),
            "users": [],
        }
    summary = summarize_usage(rows, now=datetime.now(timezone.utc))
    return {"status": "ok", "scope": scope, "scope_line": scope_line, "empty": False, **summary}


def compare_windows(
    hours_a: int,
    hours_b: int,
    domain: str,
    frustration_type: str,
) -> dict:
    """Compare frustration signal volume between two rolling windows.

    Window A is the most recent `hours_a` hours. Window B is the `hours_b`
    hours immediately before A. Filters apply identically to both.

    All four arguments REQUIRED. Use for regression / "recent vs prior"
    questions.

    Returns scope_line pre-rendered.

    Example:
        compare_windows(hours_a=168, hours_b=168,
                        domain="moovit.com", frustration_type="rage_click")
    """
    err = _val_hours(hours_a, "hours_a") or _val_hours(hours_b, "hours_b") or _val_type(frustration_type)
    if err:
        return err
    from collections import Counter

    def _w(start_ago: float, length: float) -> dict:
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

    wa, wb = _w(0, hours_a), _w(hours_a, hours_b)
    delta = round((wa["total"] - wb["total"]) / wb["total"] * 100, 1) if wb["total"] else None
    d = domain if domain else '""'
    t = frustration_type if frustration_type else '""'
    scope_line = f"Compare Scope: hours_a={hours_a}, hours_b={hours_b}, domain={d}, frustration_type={t}"
    return {
        "status": "ok",
        "scope": {"hours_a": hours_a, "hours_b": hours_b, "domain": domain, "frustration_type": frustration_type},
        "scope_line": scope_line,
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
