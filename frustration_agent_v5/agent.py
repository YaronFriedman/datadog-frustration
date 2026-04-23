"""v5 agent — minimal LLM surface: extract filters, call one tool, paste verbatim."""
from __future__ import annotations

import os

from dotenv import load_dotenv
from google.adk.agents import Agent
from google.adk.models.lite_llm import LiteLlm

from .tools import ALL_TOOLS

load_dotenv()

_CLAUDE_MODEL = os.getenv("ANTHROPIC_MODEL", "anthropic/claude-opus-4-7")
_GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")
_AZURE_MODEL = os.getenv("AZURE_MODEL", "azure/gpt-4.1-mini")


def _pick_model():
    if os.getenv("AZURE_OPENAI_API_KEY") and os.getenv("AZURE_OPENAI_ENDPOINT"):
        return LiteLlm(
            model=_AZURE_MODEL,
            api_base=os.environ["AZURE_OPENAI_ENDPOINT"],
            api_key=os.environ["AZURE_OPENAI_API_KEY"],
            api_version=os.environ.get("AZURE_OPENAI_API_VERSION", "2024-12-01-preview"),
        )
    if os.getenv("ANTHROPIC_API_KEY"):
        return LiteLlm(model=_CLAUDE_MODEL)
    if os.getenv("GOOGLE_API_KEY"):
        return _GEMINI_MODEL
    raise RuntimeError("No LLM credentials set.")


INSTRUCTION = r"""You are the Deepchecks Frustration Analyst v5. Your job has TWO steps.

Step 1 — EXTRACT the filter values from the user's prompt:
  - hours: integer. If the user gave "from/to" dates, compute the hour span.
  - domain: the customer email domain mentioned (e.g. "moovit.com"), or "" if none.
  - frustration_type: one of "", "dead_click", "rage_click", "error_click".
  - exclude_internal: true, unless the user explicitly says to include internal users.
  - question: the user's additional "Additionally answer:" text, trimmed, or "".

Step 2 — CALL the tool exactly once:

  analyze_frustrations(hours=<int>, domain=<str>, frustration_type=<str>,
                       exclude_internal=<bool>, question=<str>)

Every argument is REQUIRED. Pass empty strings "" for unused string
filters. Pass booleans as true/false.

Step 3 — RESPOND with the `report_markdown` field from the tool,
VERBATIM and COMPLETE. Your entire response to the user MUST be that
string. No prefix. No suffix. No commentary. No "Here is your report:".
No summary after. Zero extra characters.

If the tool returns status="error", respond with exactly the `error`
field from the tool and nothing else.

The tool itself already:
  - Builds the mandatory PLAN block.
  - Assembles every required section in the correct order.
  - Omits sections that should be omitted.
  - Answers any sub-question using only direct tool evidence.
  - Writes data-grounded Recommended Next Steps.

You do NOT need to add any of that yourself — and you MUST NOT try.
Your only creative step is extracting the right filter values from
the user's prompt. Everything else is deterministic.
"""


root_agent = Agent(
    name="frustration_agent_v5",
    model=_pick_model(),
    description=(
        "v5: single-tool agent that extracts filters, calls a deterministic "
        "renderer, and relays the pre-formatted report verbatim."
    ),
    instruction=INSTRUCTION,
    tools=ALL_TOOLS,
)
