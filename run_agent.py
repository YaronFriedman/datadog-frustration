#!/usr/bin/env python3
"""Run the frustration ADK agent with Deepchecks instrumentation.

Usage:
    python run_agent.py "what is frustrating moovit this week?"
    python run_agent.py            # interactive REPL
"""
from __future__ import annotations

import argparse
import asyncio
import sys
import uuid

from dotenv import load_dotenv

load_dotenv()

# Order matters: Deepchecks must register the OTEL exporter before the
# ADK Runner is built so all spans are captured.
from frustration_agent.deepchecks_setup import configure as configure_deepchecks

configure_deepchecks()

from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai.types import Content, Part

from frustration_agent.agent import root_agent

APP_NAME = "frustration_agent"


async def _ask(runner: Runner, session_service: InMemorySessionService, user_id: str, session_id: str, prompt: str) -> str:
    msg = Content(role="user", parts=[Part(text=prompt)])
    final = ""
    async for event in runner.run_async(user_id=user_id, session_id=session_id, new_message=msg):
        if event.is_final_response() and event.content and event.content.parts:
            for part in event.content.parts:
                if part.text:
                    final += part.text
    return final or "(no response)"


async def _run(prompt: str | None) -> int:
    session_service = InMemorySessionService()
    user_id = "cli-user"
    session_id = str(uuid.uuid4())
    await session_service.create_session(app_name=APP_NAME, user_id=user_id, session_id=session_id)
    runner = Runner(agent=root_agent, app_name=APP_NAME, session_service=session_service)

    if prompt:
        print(await _ask(runner, session_service, user_id, session_id, prompt))
        return 0

    print("Frustration agent ready. Ctrl-C to exit.", file=sys.stderr)
    while True:
        try:
            line = input("you> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return 0
        if not line:
            continue
        if line in {"/quit", "/exit"}:
            return 0
        answer = await _ask(runner, session_service, user_id, session_id, line)
        print(f"\n{answer}\n")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("prompt", nargs="*", help="One-shot prompt. Omit for REPL.")
    args = p.parse_args()
    prompt = " ".join(args.prompt).strip() if args.prompt else None
    return asyncio.run(_run(prompt))


if __name__ == "__main__":
    sys.exit(main())
