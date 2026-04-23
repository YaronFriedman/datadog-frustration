"""v5 tools — single tool that wraps the deterministic renderer."""
from __future__ import annotations

from .renderer import build_report


def analyze_frustrations(
    hours: int,
    domain: str,
    frustration_type: str,
    exclude_internal: bool,
    question: str,
) -> dict:
    """Fetch and analyze Datadog RUM frustration signals, returning a fully-formatted report.

    This is the ONLY tool. It handles everything: fetching, filtering,
    aggregation, replay collection, sub-question answering, and final
    markdown rendering. You MUST call it exactly once per user request
    with the filter values you extracted from the prompt.

    The returned `report_markdown` is the complete user-facing answer,
    already formatted with the PLAN block and every required section.
    Your final response MUST be that string and nothing else — no
    prefix, no suffix, no commentary.

    ALL five arguments are REQUIRED. Pass empty strings for unused
    filters; never omit them.

    Args:
        hours: Lookback window in hours. Integer 1..1440.
        domain: Customer email domain substring (e.g. "moovit.com") or "".
        frustration_type: One of "", "dead_click", "rage_click", "error_click".
        exclude_internal: True to drop @deepchecks.com internal users.
            Default True for customer-facing analyses.
        question: The user's additional sub-question, or "" if none.

    Returns:
        Dict with:
        - status: "ok" on success, "error" with `error` message otherwise.
        - report_markdown: the COMPLETE formatted report to return verbatim.
        - scope: echo of the filter values actually applied.
        - counts: {fetched, excluded_internal, in_scope}.

    Example (moovit.com rage clicks in the last 7 days, with a priority question):
        analyze_frustrations(
            hours=168, domain="moovit.com", frustration_type="rage_click",
            exclude_internal=True,
            question="Which customer should we call first and why?",
        )

    Example (no filters, quick look at the last day):
        analyze_frustrations(
            hours=24, domain="", frustration_type="",
            exclude_internal=True, question="",
        )
    """
    return build_report(
        hours=hours,
        domain=domain,
        frustration_type=frustration_type,
        exclude_internal=exclude_internal,
        question=question,
    )


ALL_TOOLS = [analyze_frustrations]
