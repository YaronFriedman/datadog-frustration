#!/usr/bin/env python3
"""Direct (non-agent) CLI for Datadog RUM frustration signals.

For the agentic interface, see run_agent.py.
"""
from __future__ import annotations

import argparse
import json
import sys

from dotenv import load_dotenv

load_dotenv()

from datadog_rum import (
    DEFAULT_EXCLUDED_DOMAINS,
    aggregate_frustrations,
    fetch_frustration_rows,
)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--hours", type=int, default=24)
    p.add_argument("--app-id")
    p.add_argument("--env")
    p.add_argument("--exclude-domain", action="append", default=list(DEFAULT_EXCLUDED_DOMAINS))
    p.add_argument("--no-default-exclude", action="store_true")
    p.add_argument("--limit", type=int, default=5000)
    args = p.parse_args()

    excluded = [] if args.no_default_exclude else list(dict.fromkeys(args.exclude_domain))
    print(f"Fetching last {args.hours}h (env={args.env or 'any'}, exclude={excluded})...", file=sys.stderr)
    fetched = fetch_frustration_rows(
        hours=args.hours,
        app_id=args.app_id,
        env=args.env,
        limit=args.limit,
        excluded_domains=excluded,
    )
    print(f"Fetched {fetched['fetched']}; kept {len(fetched['rows'])} after excluding {fetched['excluded']} internal.", file=sys.stderr)
    if not fetched["rows"]:
        return 0
    agg = aggregate_frustrations(fetched["rows"])
    print(json.dumps({"aggregate": agg, "sample": fetched["rows"][:50]}, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
