#!/usr/bin/env python3
"""Run the frustration ADK agent with structured inputs.

The agent receives a prompt built from your inputs (date range, customer
filter, type, optional follow-up question), calls its tools, and every
span is logged to Deepchecks via the GoogleAdkIntegration exporter.

Examples:
    python run_agent.py --from 2026-04-14 --to 2026-04-21
    python run_agent.py --hours 168 --domain moovit.com --type rage_click
    python run_agent.py --hours 168 --question "who should we talk to first?"
"""
from __future__ import annotations

import argparse
import asyncio
import sys
import uuid
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv()

# Deepchecks OTEL exporter MUST be registered before the ADK Runner is built.
from frustration_agent.deepchecks_setup import configure as configure_deepchecks

configure_deepchecks()

from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai.types import Content, Part

from frustration_agent.agent import root_agent

APP_NAME = "frustration_agent"


def _parse_date(s: str) -> datetime:
    try:
        return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError as e:
        raise argparse.ArgumentTypeError(f"expected YYYY-MM-DD, got '{s}': {e}")


def _build_prompt(args: argparse.Namespace) -> tuple[str, int]:
    """Translate structured inputs into a single natural-language task for the agent."""
    if args.from_date and args.to_date:
        hours = max(1, int((args.to_date - args.from_date).total_seconds() / 3600))
        range_desc = f"from {args.from_date.date()} to {args.to_date.date()} ({hours}h window)"
    else:
        hours = args.hours
        range_desc = f"the last {hours} hours"

    parts = [f"Analyze Datadog RUM frustration signals for {range_desc}."]

    if args.domain:
        parts.append(f"Focus on the customer domain '{args.domain}'.")
    if args.type:
        parts.append(f"Only consider frustration type '{args.type}'.")
    if args.include_internal:
        parts.append("Include internal @deepchecks.com users in the analysis.")
    else:
        parts.append("Exclude internal @deepchecks.com users — customers only.")

    parts.append(
        "Call the tools with hours="
        f"{hours}"
        + (f", domain='{args.domain}'" if args.domain else "")
        + (f", frustration_type='{args.type}'" if args.type else "")
        + ". Give a concise, specific report ranked by impact, with the top URLs,"
        " top UI targets, the 3 most affected customers, and links to sample"
        " session replays. End with a short list of recommended next investigation steps."
    )

    if args.question:
        parts.append(f"Additionally answer: {args.question}")

    return "\n".join(parts), hours


async def _run(prompt: str) -> int:
    session_service = InMemorySessionService()
    user_id = "cli-user"
    session_id = str(uuid.uuid4())
    await session_service.create_session(app_name=APP_NAME, user_id=user_id, session_id=session_id)
    runner = Runner(agent=root_agent, app_name=APP_NAME, session_service=session_service)

    msg = Content(role="user", parts=[Part(text=prompt)])
    final = ""
    async for event in runner.run_async(user_id=user_id, session_id=session_id, new_message=msg):
        if event.is_final_response() and event.content and event.content.parts:
            for part in event.content.parts:
                if part.text:
                    final += part.text
    print(final or "(no response)")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    window = p.add_argument_group("Time window (use --hours OR --from/--to)")
    window.add_argument("--hours", type=int, default=168, help="Lookback window in hours. Default 168 (7d).")
    window.add_argument("--from", dest="from_date", type=_parse_date, help="Start date (YYYY-MM-DD, UTC).")
    window.add_argument("--to", dest="to_date", type=_parse_date, help="End date (YYYY-MM-DD, UTC).")

    flt = p.add_argument_group("Filters")
    flt.add_argument("--domain", help="Customer email domain substring (e.g. moovit.com).")
    flt.add_argument("--type", choices=["dead_click", "rage_click", "error_click"], help="Frustration type.")
    flt.add_argument("--include-internal", action="store_true", help="Keep @deepchecks.com users in the analysis.")
    flt.add_argument("--question", help="Additional free-form question for the agent.")

    p.add_argument("--print-prompt", action="store_true", help="Print the generated prompt and exit.")
    args = p.parse_args()

    if bool(args.from_date) ^ bool(args.to_date):
        print("--from and --to must be provided together, or use --hours.", file=sys.stderr)
        return 2
    if args.from_date and args.to_date and args.to_date <= args.from_date:
        print("--to must be after --from.", file=sys.stderr)
        return 2

    prompt, _ = _build_prompt(args)
    if args.print_prompt:
        print(prompt)
        return 0

    return asyncio.run(_run(prompt))


if __name__ == "__main__":
    sys.exit(main())
