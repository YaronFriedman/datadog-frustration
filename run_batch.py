#!/usr/bin/env python3
"""Fire N randomized agent queries, each as its own Deepchecks session.

Used to populate Deepchecks with trace data for demos / review.
"""
from __future__ import annotations

import argparse
import asyncio
import random
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv

load_dotenv()

from frustration_agent.deepchecks_setup import configure as configure_deepchecks

configure_deepchecks()

from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai.types import Content, Part

from frustration_agent.agent import root_agent

APP_NAME = "frustration_agent"

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
    """Pick between hours or explicit date range."""
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


def _build_prompt(cfg: dict, window_desc: str, rng: random.Random) -> str:
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


def _random_query(rng: random.Random) -> tuple[str, dict]:
    window_desc, cfg = _random_window(rng)
    if rng.random() < 0.65:
        cfg["domain"] = rng.choice(CUSTOMER_DOMAINS)
    if rng.random() < 0.5:
        cfg["type"] = rng.choice(FRUSTRATION_TYPES)
    if rng.random() < 0.7:
        cfg["question"] = rng.choice(QUESTION_TEMPLATES)
    return _build_prompt(cfg, window_desc, rng), cfg


async def _run_one(runner: Runner, session_service: InMemorySessionService, prompt: str) -> int:
    user_id = f"batch-{uuid.uuid4().hex[:8]}"
    session_id = str(uuid.uuid4())
    await session_service.create_session(app_name=APP_NAME, user_id=user_id, session_id=session_id)
    msg = Content(role="user", parts=[Part(text=prompt)])
    tokens = 0
    async for event in runner.run_async(user_id=user_id, session_id=session_id, new_message=msg):
        if event.is_final_response() and event.content and event.content.parts:
            for part in event.content.parts:
                if part.text:
                    tokens += len(part.text)
    return tokens


async def main_async(n: int, seed: int, delay: float) -> int:
    rng = random.Random(seed)
    session_service = InMemorySessionService()
    runner = Runner(agent=root_agent, app_name=APP_NAME, session_service=session_service)

    successes = 0
    t0 = time.time()
    for i in range(1, n + 1):
        prompt, cfg = _random_query(rng)
        short = {k: v for k, v in cfg.items() if k in {"hours", "domain", "type", "question", "from_date", "to_date"}}
        print(f"\n[{i}/{n}] cfg={short}", file=sys.stderr)
        try:
            length = await _run_one(runner, session_service, prompt)
            successes += 1
            print(f"  ok — {length} chars out", file=sys.stderr)
        except Exception as e:
            print(f"  FAILED: {e}", file=sys.stderr)
        if delay and i < n:
            await asyncio.sleep(delay)
    dt = time.time() - t0
    print(f"\nDone: {successes}/{n} queries in {dt:.1f}s ({dt/n:.1f}s each).", file=sys.stderr)
    return 0 if successes == n else 1


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("-n", "--count", type=int, default=30)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--delay", type=float, default=1.0, help="Seconds between queries.")
    args = p.parse_args()
    return asyncio.run(main_async(args.count, args.seed, args.delay))


if __name__ == "__main__":
    sys.exit(main())
