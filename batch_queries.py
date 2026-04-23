"""Shared generator for randomized batch-query prompts.

Used by run_batch.py (v1) and run_batch_v2.py (v2) so the *same seed*
yields the *same 30 inputs* to both versions, enabling clean comparison
in Deepchecks.
"""
from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone

CUSTOMER_DOMAINS = [
    "moovit.com",
    "wix.com",
    "gmail.com",
    "novee.security",
    "mafat.ai",
    "getblaise.com",
]
FRUSTRATION_TYPES = ["dead_click", "rage_click", "error_click"]

QUESTION_TEMPLATES = [
    "Which customer should we call first and why?",
    "Summarize the three biggest UX problems.",
    "Are rage clicks concentrated on any one screen?",
    "What is the single most frustrating button in the product?",
    "Give me a product manager friendly brief.",
    "Is there any pattern that suggests a broken date picker?",
    "Which customers have dropped off recently?",
    "Any signs of a recent regression?",
    "What investigation should an engineer do first?",
    "Rank pages from most to least painful.",
]


def _random_window(rng: random.Random) -> tuple[str, dict]:
    if rng.random() < 0.5:
        hours = rng.choice([24, 48, 72, 120, 168])
        return f"hours={hours}", {"hours": hours}
    span_days = rng.choice([1, 2, 3, 5, 7, 10])
    to_d = datetime.now(timezone.utc).date() - timedelta(days=rng.randint(0, 3))
    from_d = to_d - timedelta(days=span_days)
    hours = span_days * 24
    return f"from {from_d} to {to_d}", {
        "hours": hours,
        "from_date": from_d.isoformat(),
        "to_date": to_d.isoformat(),
    }


def _build_prompt(cfg: dict, window_desc: str) -> str:
    parts = [f"Analyze Datadog RUM frustration signals for {window_desc}."]
    if cfg.get("domain"):
        parts.append(f"Focus on the customer domain '{cfg['domain']}'.")
    if cfg.get("type"):
        parts.append(f"Only consider frustration type '{cfg['type']}'.")
    parts.append("Exclude internal @deepchecks.com users — customers only.")
    tool_call = "Call the tools with hours=" + str(cfg["hours"])
    if cfg.get("domain"):
        tool_call += f", domain='{cfg['domain']}'"
    if cfg.get("type"):
        tool_call += f", frustration_type='{cfg['type']}'"
    tool_call += (
        ". Give a concise report ranked by impact with top URLs, top UI targets, "
        "the 3 most affected customers, and sample session replay links."
    )
    parts.append(tool_call)
    if cfg.get("question"):
        parts.append(f"Additionally answer: {cfg['question']}")
    return "\n".join(parts)


def random_query(rng: random.Random) -> tuple[str, dict]:
    """Return (prompt, cfg) for one randomized query. Same rng+seed -> same output."""
    window_desc, cfg = _random_window(rng)
    if rng.random() < 0.65:
        cfg["domain"] = rng.choice(CUSTOMER_DOMAINS)
    if rng.random() < 0.5:
        cfg["type"] = rng.choice(FRUSTRATION_TYPES)
    if rng.random() < 0.7:
        cfg["question"] = rng.choice(QUESTION_TEMPLATES)
    return _build_prompt(cfg, window_desc), cfg
