#!/usr/bin/env python3
"""Direct (non-agent) CLI for RUM usage trend by customer email domain.

For the agentic interface, see run_agent.py.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv()

from datadog_rum import fetch_usage_rows, summarize_usage


def _print(domain: str, excluded: list[str], summary: dict, days: int) -> None:
    print(f"\n=== {domain} usage report (last {days}d, excluded: {sorted(excluded) or 'none'}) ===")
    print(f"Total sessions: {summary['total_sessions']}")
    print(f"Unique users: {summary['unique_users']}")
    print(f"Sessions last 14d: {summary['sessions_last_14d']}   prev 14d: {summary['sessions_prev_14d']}   delta: {summary['delta_pct']}%")
    print()
    print("Weekly session volume:")
    for w, c in summary["sessions_per_week"]:
        bar = "#" * min(c, 60)
        print(f"  {w}  {c:4d}  {bar}")
    print()
    print(f"  {'email':<38} {'total':>6} {'first':>12} {'last':>12} {'d_since':>8} {'wk_avg':>7} {'L14':>5} {'P14':>5}")
    for u in summary["users"]:
        print(f"  {u['email']:<38} {u['total_sessions']:>6} {u['first_seen']:>12} {u['last_seen']:>12} "
              f"{u['days_since_last']:>8} {u['avg_sessions_per_week']:>7} "
              f"{u['sessions_last_14d']:>5} {u['sessions_prev_14d']:>5}")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--domain", required=True)
    p.add_argument("--days", type=int, default=90)
    p.add_argument("--exclude-email", action="append", default=[])
    p.add_argument("--app-id")
    args = p.parse_args()

    excluded = [e.lower() for e in args.exclude_email]
    print(f"Fetching sessions for @{args.domain} over last {args.days}d...", file=sys.stderr)
    rows = fetch_usage_rows(
        domain=args.domain,
        days=args.days,
        app_id=args.app_id,
        excluded_emails=excluded,
    )
    print(f"Got {len(rows)} session events.", file=sys.stderr)
    summary = summarize_usage(rows, datetime.now(timezone.utc))
    _print(args.domain, excluded, summary, args.days)
    return 0


if __name__ == "__main__":
    sys.exit(main())
