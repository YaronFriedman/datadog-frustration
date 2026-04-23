#!/usr/bin/env python3
"""Fire the same 30 seeded queries against frustration_agent v3 and log to Deepchecks v3."""
from __future__ import annotations

import argparse
import asyncio
import os
import random
import sys
import time
import uuid

os.environ.setdefault("DEEPCHECKS_VERSION", "v3")
_pinned = os.environ["DEEPCHECKS_VERSION"]

from dotenv import load_dotenv

load_dotenv(override=False)
os.environ["DEEPCHECKS_VERSION"] = _pinned

from frustration_agent.deepchecks_setup import configure as configure_deepchecks

configure_deepchecks()

from batch_queries import random_query
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai.types import Content, Part

from frustration_agent_v3.agent import root_agent

APP_NAME = "frustration_agent_v3"


async def _run_one(runner: Runner, session_service: InMemorySessionService, prompt: str) -> int:
    user_id = f"batch-v3-{uuid.uuid4().hex[:8]}"
    session_id = str(uuid.uuid4())
    await session_service.create_session(app_name=APP_NAME, user_id=user_id, session_id=session_id)
    msg = Content(role="user", parts=[Part(text=prompt)])
    out = 0
    async for event in runner.run_async(user_id=user_id, session_id=session_id, new_message=msg):
        if event.is_final_response() and event.content and event.content.parts:
            for part in event.content.parts:
                if part.text:
                    out += len(part.text)
    return out


async def main_async(n: int, seed: int, delay: float) -> int:
    rng = random.Random(seed)
    session_service = InMemorySessionService()
    runner = Runner(agent=root_agent, app_name=APP_NAME, session_service=session_service)

    ok = 0
    t0 = time.time()
    for i in range(1, n + 1):
        prompt, cfg = random_query(rng)
        short = {k: v for k, v in cfg.items() if k in {"hours", "domain", "type", "question", "from_date", "to_date"}}
        print(f"\n[{i}/{n}] cfg={short}", file=sys.stderr)
        try:
            length = await _run_one(runner, session_service, prompt)
            ok += 1
            print(f"  ok — {length} chars out", file=sys.stderr)
        except Exception as e:
            print(f"  FAILED: {e}", file=sys.stderr)
        if delay and i < n:
            await asyncio.sleep(delay)
    dt = time.time() - t0
    print(f"\nDone: {ok}/{n} queries in {dt:.1f}s ({dt/n:.1f}s each).", file=sys.stderr)
    return 0 if ok == n else 1


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("-n", "--count", type=int, default=30)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--delay", type=float, default=1.0)
    args = p.parse_args()
    return asyncio.run(main_async(args.count, args.seed, args.delay))


if __name__ == "__main__":
    sys.exit(main())
