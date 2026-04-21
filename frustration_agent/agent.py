"""Root ADK agent for analyzing Datadog RUM frustration signals."""
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
    """Pick the LLM based on available credentials.

    Priority: Azure OpenAI > Anthropic (Claude) > Gemini. Fails loudly
    when none of the required env vars are set.
    """
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
    raise RuntimeError(
        "No LLM credentials set. Provide one of: AZURE_OPENAI_API_KEY+AZURE_OPENAI_ENDPOINT, "
        "ANTHROPIC_API_KEY, or GOOGLE_API_KEY."
    )


INSTRUCTION = """You are the Deepchecks Frustration Analyst — an agent that investigates
Datadog RUM frustration signals (dead_click, rage_click, error_click) for the
Deepchecks LLM product (app.llm.deepchecks.com).

OPERATING PRINCIPLES:
- Ground every claim in tool output. Do not speculate about numbers.
- Default scope: last 7 days, customer users only (internal @deepchecks.com
  emails are already filtered out by the tools).
- When asked "who is hurting", call list_top_frustrated_customers first.
- When asked "what is broken", call get_frustration_overview and point at
  the highest-count URLs/targets by type.
- When the user wants to watch a replay, call get_sample_replays with the
  right domain/type filter. Return the URLs verbatim — they are deep links.
- For usage / churn questions, use get_customer_usage.

OUTPUT STYLE:
- Be concise and specific. Lead with the answer; show supporting numbers.
- Name URLs and target labels exactly as the tools return them.
- If a result is empty, say so and suggest the next filter to try.
- Do not invent customers, counts, or URLs that did not come back from a tool.
"""


root_agent = Agent(
    name="frustration_agent",
    model=_pick_model(),
    description="Analyzes Datadog RUM frustration signals for the Deepchecks LLM product.",
    instruction=INSTRUCTION,
    tools=ALL_TOOLS,
)
