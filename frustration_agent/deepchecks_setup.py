"""Wire Deepchecks auto-instrumentation for the Google ADK agent.

MUST be imported and `configure()` called BEFORE the ADK Runner is built,
otherwise spans are emitted before the exporter is registered and get
dropped.
"""
from __future__ import annotations

import os
import sys


def configure() -> bool:
    """Register the Deepchecks OTEL exporter for Google ADK.

    Returns True if instrumentation was registered, False if it was skipped
    (missing token or optional dep). A missing setup should never block the
    agent from running — we warn and continue.
    """
    token = os.getenv("DEEPCHECKS_API_TOKEN") or os.getenv("DEEPCHECKS_API_KEY")
    if not token:
        print("[deepchecks] DEEPCHECKS_API_TOKEN not set — skipping instrumentation.", file=sys.stderr)
        return False
    try:
        from deepchecks_llm_client.data_types import EnvType
        from deepchecks_llm_client.otel import GoogleAdkIntegration
    except ImportError as e:
        print(f"[deepchecks] deepchecks-llm-client[otel] not installed ({e}) — skipping.", file=sys.stderr)
        return False

    host = os.getenv("DEEPCHECKS_HOST") or os.getenv("DEEPCHECKS_LLM_HOST", "https://app.llm.deepchecks.com/")
    app_name = os.getenv("DEEPCHECKS_APP_NAME", "datadog-frustration-agent")
    version_name = os.getenv("DEEPCHECKS_VERSION", "v0.1.0")
    env_name = os.getenv("DEEPCHECKS_ENV", "EVAL").upper()
    env_type = getattr(EnvType, env_name, EnvType.EVAL)

    GoogleAdkIntegration().register_dc_exporter(
        host=host,
        api_key=token,
        app_name=app_name,
        version_name=version_name,
        env_type=env_type,
        log_to_console=os.getenv("DEEPCHECKS_LOG_TO_CONSOLE", "0") == "1",
    )
    print(
        f"[deepchecks] instrumentation on → app={app_name} version={version_name} env={env_name} host={host}",
        file=sys.stderr,
    )
    return True
