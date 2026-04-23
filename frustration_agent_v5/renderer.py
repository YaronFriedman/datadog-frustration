"""Deterministic renderer for the frustration report.

This module does the heavy lifting in pure Python — no LLM involved.
It calls the data layer, builds the PLAN block, every report section,
and templated sub-question answers. The v5 agent just pastes the
resulting `report_markdown` string verbatim, closing the gap where
smaller LLMs were drifting on verbatim quoting and section omission.
"""
from __future__ import annotations

import re
from collections import Counter
from datetime import datetime, timedelta, timezone

from datadog_rum import (
    DEFAULT_EXCLUDED_DOMAINS,
    aggregate_frustrations,
    fetch_frustration_rows,
    fetch_usage_rows,
    summarize_usage,
)

_VALID_TYPES = {"", "dead_click", "rage_click", "error_click"}


# ---------- PLAN ----------

def _plan_block(hours: int, domain: str, frustration_type: str, exclude_internal: bool,
                sub_questions: list[str]) -> str:
    d = f'"{domain}"' if domain else '""'
    t = f'"{frustration_type}"' if frustration_type else '""'
    sq = "[" + ", ".join(f'"{q}"' for q in sub_questions) + "]" if sub_questions else "[]"
    tool_args = (f'hours={hours}, domain="{domain}", frustration_type="{frustration_type}", '
                 f'exclude_internal={str(exclude_internal).lower()}')
    lines = [
        "```plan",
        f"hours: {hours}",
        f"domain: {d}",
        f"frustration_type: {t}",
        f"exclude_internal: {str(exclude_internal).lower()}",
        f"sub_questions: {sq}",
        "tool_calls_planned:",
        f"  - analyze_frustrations({tool_args})",
        "```",
    ]
    return "\n".join(lines)


# ---------- section helpers ----------

def _scope_line(hours: int, domain: str, frustration_type: str, exclude_internal: bool) -> str:
    d = domain if domain else '""'
    t = frustration_type if frustration_type else '""'
    return f"Scope: hours={hours}, domain={d}, frustration_type={t}, exclude_internal={exclude_internal}"


def _replay_scope_line(hours: int, domain: str, frustration_type: str, exclude_internal: bool) -> str:
    d = domain if domain else '""'
    t = frustration_type if frustration_type else '""'
    return f"Replay Scope: hours={hours}, domain={d}, frustration_type={t}, exclude_internal={exclude_internal}"


def _empty_tips(hours: int, domain: str, frustration_type: str, exclude_internal: bool) -> list[str]:
    tips: list[str] = []
    if frustration_type:
        tips.append(f"Remove the frustration_type='{frustration_type}' filter to include all types.")
    if domain:
        tips.append("Try a different customer domain. The highest-volume domains are usually moovit.com, wix.com, and gmail.com.")
    if hours <= 72:
        tips.append("Widen the time window (e.g. hours=168 or hours=336).")
    if exclude_internal:
        tips.append("Set exclude_internal=False if internal staff activity may be relevant.")
    if not tips:
        tips.append("Widen the time window or drop filters; this scope simply has no data.")
    return tips


def _filter_rows(rows: list[dict], domain: str, frustration_type: str) -> list[dict]:
    out = []
    d = domain.lower()
    for r in rows:
        if d and (not r.get("user_domain") or d not in r["user_domain"].lower()):
            continue
        if frustration_type and frustration_type not in (r.get("frustration") or []):
            continue
        out.append(r)
    return out


# ---------- sub-question templates ----------

def _classify_sub_question(q: str) -> str:
    low = q.lower()
    if any(k in low for k in ("regression", "recent", "signs of", "trend")):
        return "regression"
    if any(k in low for k in ("call first", "prioritize", "who should we", "which customer")):
        return "priority"
    if "dropped off" in low or "churn" in low or "stopped using" in low:
        return "churn"
    if any(k in low for k in ("date picker", "calendar", "broken")):
        return "broken_pattern"
    if any(k in low for k in ("most frustrating button", "single most", "worst button")):
        return "worst_element"
    if any(k in low for k in ("rank pages", "ranking", "rank")):
        return "page_ranking"
    if any(k in low for k in ("engineer", "investigation", "investigate")):
        return "investigation"
    if any(k in low for k in ("pm", "product manager", "brief", "summary", "summarize")):
        return "summary"
    return "generic"


def _answer_regression(hours: int, domain: str, frustration_type: str) -> str:
    wa = _window(0, hours, domain, frustration_type)
    wb = _window(hours, hours, domain, frustration_type)
    delta = None
    if wb["total"]:
        delta = round((wa["total"] - wb["total"]) / wb["total"] * 100, 1)
    lines = [
        f"Direct comparison (same filters, two consecutive windows of {hours}h each):",
        f"- Recent window: {wa['total']} signals — by type: {wa['counts_by_type']}",
        f"- Prior window: {wb['total']} signals — by type: {wb['counts_by_type']}",
    ]
    if delta is None:
        lines.append("- Delta: n/a (prior window had 0 signals).")
    else:
        lines.append(f"- Delta: {delta}%.")
    return "\n".join(lines)


def _window(start_ago_h: float, length_h: float, domain: str, frustration_type: str) -> dict:
    fetched = fetch_frustration_rows(
        hours=start_ago_h + length_h,
        excluded_domains=DEFAULT_EXCLUDED_DOMAINS,
    )
    rows = _filter_rows(fetched["rows"], domain, frustration_type)
    hi = (datetime.now(timezone.utc).timestamp() - start_ago_h * 3600) * 1000
    lo = hi - length_h * 3600 * 1000
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


def _answer_priority(by_customer_domain: dict, domain: str) -> str:
    if domain:
        n = by_customer_domain.get(domain, 0)
        return f"Direct evidence: {domain} has {n} signals in scope."
    if not by_customer_domain:
        return "No customer signals in scope — nothing to prioritize."
    top = list(by_customer_domain.items())[:3]
    line1 = f"Top by signal count in scope: " + ", ".join(f"{d} ({c})" for d, c in top) + "."
    return line1 + f"\nSuggested contact order based on signal volume: {', '.join(d for d, _ in top)}."


def _answer_churn(domain: str) -> str:
    if not domain:
        return "No domain specified — cannot analyse churn without a target customer."
    rows = fetch_usage_rows(domain=domain, days=90, excluded_emails=[])
    if not rows:
        return f"Direct evidence: 0 session events recorded for @{domain} in the last 90 days."
    summary = summarize_usage(rows, now=datetime.now(timezone.utc))
    delta = summary.get("delta_pct")
    return (
        f"Direct evidence for @{domain} (90d): {summary['total_sessions']} sessions from "
        f"{summary['unique_users']} users. Last 14d: {summary['sessions_last_14d']}; "
        f"prior 14d: {summary['sessions_prev_14d']}; delta_pct: {delta}."
    )


def _answer_broken_pattern(top_targets_by_type: dict, keywords: list[str]) -> str:
    hits: list[tuple[str, str, int]] = []
    for ftype, rows in top_targets_by_type.items():
        for label, count in rows:
            if any(k in (label or "").lower() for k in keywords):
                hits.append((ftype, label, count))
    if not hits:
        return f"No direct evidence linking targets to the pattern ({', '.join(keywords)})."
    lines = [f"Direct evidence — targets whose labels match {keywords}:"]
    for ftype, label, count in hits[:5]:
        lines.append(f'- {count} — "{label}" ({ftype})')
    return "\n".join(lines)


def _answer_worst_element(top_targets_by_type: dict) -> str:
    best_count = -1
    best = None
    for ftype, rows in top_targets_by_type.items():
        for label, count in rows:
            if count > best_count:
                best_count = count
                best = (ftype, label, count)
    if not best:
        return "No direct evidence — no targets in scope."
    ftype, label, count = best
    return f'Highest-volume UI target in scope: "{label}" ({ftype}) with {count} signals.'


def _answer_page_ranking(top_urls_by_type: dict) -> str:
    url_counts: Counter = Counter()
    for rows in top_urls_by_type.values():
        for url, c in rows:
            url_counts[url] += c
    if not url_counts:
        return "No direct evidence — no URLs in scope."
    lines = ["Pages ranked by total signals in scope:"]
    for i, (url, count) in enumerate(url_counts.most_common(10), 1):
        lines.append(f"{i}. {count} — {url}")
    return "\n".join(lines)


def _answer_investigation(top_urls_by_type: dict, top_targets_by_type: dict) -> str:
    parts: list[str] = []
    url_counts: Counter = Counter()
    for rows in top_urls_by_type.values():
        for url, c in rows:
            url_counts[url] += c
    target_counts: Counter = Counter()
    for rows in top_targets_by_type.values():
        for label, c in rows:
            target_counts[label] += c
    if url_counts:
        parts.append("Start with the top URL by signal count:")
        for url, c in url_counts.most_common(2):
            parts.append(f"- {c} — {url}")
    if target_counts:
        parts.append("Top UI targets to inspect:")
        for label, c in target_counts.most_common(3):
            parts.append(f'- {c} — "{label}"')
    if not parts:
        return "No direct evidence — scope has no data to investigate."
    return "\n".join(parts)


def _answer_summary(total: int, by_type: dict, by_customer_domain: dict) -> str:
    top_cust = list(by_customer_domain.items())[:3]
    cust_line = ", ".join(f"{d} ({c})" for d, c in top_cust) if top_cust else "n/a"
    return (
        f"Total signals in scope: {total}. Breakdown: {by_type}.\n"
        f"Top customers by volume: {cust_line}."
    )


def _answer_generic(total: int, by_type: dict) -> str:
    return f"No direct evidence addressing that question. Scope totals: {total} signals, by_type={by_type}."


def _render_sub_question(q: str, hours: int, domain: str, frustration_type: str,
                        overview_result: dict) -> str:
    intent = _classify_sub_question(q)
    top_urls = overview_result.get("top_urls_by_type", {})
    top_targets = overview_result.get("top_targets_by_type", {})
    by_cust = overview_result.get("by_customer_domain", {})
    total = overview_result.get("total_frustrations", 0)
    by_type = overview_result.get("by_type", {})

    if intent == "regression":
        return _answer_regression(hours, domain, frustration_type)
    if intent == "priority":
        return _answer_priority(by_cust, domain)
    if intent == "churn":
        return _answer_churn(domain)
    if intent == "broken_pattern":
        # Extract the keyword hint from the question itself.
        kws = [w for w in ("date picker", "calendar", "broken", "button", "filter") if w in q.lower()]
        if not kws:
            kws = ["broken"]
        return _answer_broken_pattern(top_targets, kws)
    if intent == "worst_element":
        return _answer_worst_element(top_targets)
    if intent == "page_ranking":
        return _answer_page_ranking(top_urls)
    if intent == "investigation":
        return _answer_investigation(top_urls, top_targets)
    if intent == "summary":
        return _answer_summary(total, by_type, by_cust)
    return _answer_generic(total, by_type)


# ---------- recommended next steps (deterministic, data-grounded) ----------

def _recommended_next_steps(overview: dict, replays: dict, domain: str) -> list[str]:
    steps: list[str] = []
    if not replays.get("empty") and replays.get("replays_by_type"):
        for ftype, items in replays["replays_by_type"].items():
            if items:
                steps.append(f"Watch the first {ftype} replay linked above to confirm the pattern.")
                break
    top_targets = overview.get("top_targets_by_type") or {}
    for ftype, rows in top_targets.items():
        if rows:
            label, count = rows[0]
            steps.append(f'Inspect the UI element "{label}" ({count} {ftype} signals) for the cause.')
            break
    top_urls = overview.get("top_urls_by_type") or {}
    for ftype, rows in top_urls.items():
        if rows:
            url, count = rows[0]
            steps.append(f"Investigate the page with highest signal volume ({count} {ftype}): {url}")
            break
    if domain:
        steps.append(f"Reach out to a user from @{domain} to collect direct qualitative feedback.")
    if not steps:
        steps = ["Widen the time window or relax filters; this scope returned no data to investigate."]
    return steps[:4]


# ---------- main render ----------

def build_report(
    hours: int,
    domain: str,
    frustration_type: str,
    exclude_internal: bool,
    question: str = "",
    max_replays_per_type: int = 5,
) -> dict:
    """Fetch data and build the full report_markdown string. Pure, deterministic."""
    # Validate inputs.
    if not isinstance(hours, int) or hours <= 0 or hours > 24 * 60:
        return {"status": "error", "error": f"'hours' must be integer 1..1440; got {hours!r}"}
    if frustration_type not in _VALID_TYPES:
        return {"status": "error", "error": f"'frustration_type' must be in {sorted(_VALID_TYPES)!r}"}

    # Sub-questions: split on common separators if the user glued several.
    sub_questions: list[str] = []
    if question and question.strip():
        # Keep it one entry unless the user explicitly bulleted/numbered it — simple heuristic.
        sub_questions = [question.strip()]

    # Fetch once for overview.
    fetched = fetch_frustration_rows(
        hours=hours,
        excluded_domains=DEFAULT_EXCLUDED_DOMAINS if exclude_internal else (),
    )
    rows = _filter_rows(fetched["rows"], domain, frustration_type)
    overview: dict
    if rows:
        agg = aggregate_frustrations(rows, top_n=10)
        agg.pop("sample_replays_by_type", None)
        overview = {"empty": False, **agg}
    else:
        overview = {
            "empty": True, "total_frustrations": 0, "total_actions": 0,
            "by_type": {}, "by_customer_domain": {},
            "top_urls_by_type": {}, "top_targets_by_type": {},
        }

    # Fetch again for replays (same scope, but we need the replay URLs regardless of overview rows).
    replay_rows = _filter_rows(fetched["rows"], domain, frustration_type)
    replays_by_type: dict[str, list[dict]] = {}
    for r in replay_rows:
        if not r.get("replay_url"):
            continue
        for ftype in r["frustration"]:
            if frustration_type and ftype != frustration_type:
                continue
            bucket = replays_by_type.setdefault(ftype, [])
            if len(bucket) >= max_replays_per_type:
                continue
            bucket.append(r)
    replays = {
        "empty": not any(replays_by_type.values()),
        "replays_by_type": replays_by_type,
    }

    # Assemble report.
    md: list[str] = []
    md.append(_plan_block(hours, domain, frustration_type, exclude_internal, sub_questions))
    md.append("")
    md.append("### Scope")
    md.append(_scope_line(hours, domain, frustration_type, exclude_internal))
    md.append("")
    md.append("### Headline")
    n_customers = len(overview.get("by_customer_domain") or {})
    md.append(
        f"{overview.get('total_frustrations', 0)} signals in scope across {n_customers} customer(s). "
        f"By type: {overview.get('by_type') or {}}."
    )

    # Top URLs — include section only if there is data.
    top_urls = overview.get("top_urls_by_type") or {}
    any_urls = any(rows for rows in top_urls.values())
    if any_urls:
        md.append("")
        md.append("### Top URLs")
        for ftype, urows in top_urls.items():
            if not urows:
                continue
            md.append(f"{ftype}:")
            for url, count in urows[:5]:
                md.append(f"- {count} — {url}")

    top_targets = overview.get("top_targets_by_type") or {}
    any_targets = any(rows for rows in top_targets.values())
    if any_targets:
        md.append("")
        md.append("### Top UI Targets")
        for ftype, trows in top_targets.items():
            if not trows:
                continue
            md.append(f"{ftype}:")
            for label, count in trows[:5]:
                md.append(f'- {count} — "{label}"')

    # Most Affected Customers — include ONLY when no domain filter.
    by_cust = overview.get("by_customer_domain") or {}
    if not domain and by_cust:
        md.append("")
        md.append("### Most Affected Customers")
        for d, c in list(by_cust.items())[:3]:
            md.append(f"- {c} — {d}")

    # Sample Replays — always include the section (with empty-state text) so the user always knows.
    md.append("")
    md.append("### Sample Replays")
    md.append(_replay_scope_line(hours, domain, frustration_type, exclude_internal))
    if replays["empty"]:
        md.append("No replays in this scope.")
        md.append("Suggested next filters:")
        for t in _empty_tips(hours, domain, frustration_type, exclude_internal):
            md.append(f"- {t}")
    else:
        for ftype, items in replays["replays_by_type"].items():
            md.append(f"**{ftype}**")
            for r in items:
                md.append(f"- {r['replay_url']}")
                md.append(f'  user: {r.get("user_email") or "?"}, target: "{r.get("action_name") or ""}", view: {r.get("view_url") or "?"}')

    # Sub-Question Answers
    if sub_questions:
        md.append("")
        md.append("### Sub-Question Answers")
        for q in sub_questions:
            md.append(f"#### {q}")
            md.append(_render_sub_question(q, hours, domain, frustration_type, overview))

    # Recommended Next Steps — mandatory.
    md.append("")
    md.append("### Recommended Next Steps")
    for step in _recommended_next_steps(overview, replays, domain):
        md.append(f"- {step}")

    return {
        "status": "ok",
        "report_markdown": "\n".join(md),
        "scope": {
            "hours": hours, "domain": domain, "frustration_type": frustration_type,
            "exclude_internal": exclude_internal,
        },
        "counts": {
            "fetched": fetched["fetched"],
            "excluded_internal": fetched["excluded"],
            "in_scope": overview.get("total_frustrations", 0),
        },
    }
