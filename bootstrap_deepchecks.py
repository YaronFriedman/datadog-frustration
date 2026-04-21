#!/usr/bin/env python3
"""Create the Deepchecks application + version if it doesn't exist yet.

Idempotent: safe to run many times. Reads DEEPCHECKS_* env vars.
"""
from __future__ import annotations

import os
import sys

from dotenv import load_dotenv

load_dotenv()


def main() -> int:
    token = os.getenv("DEEPCHECKS_API_TOKEN") or os.getenv("DEEPCHECKS_API_KEY")
    if not token:
        print("Missing DEEPCHECKS_API_TOKEN", file=sys.stderr)
        return 1
    host = os.getenv("DEEPCHECKS_HOST", "https://app.llm.deepchecks.com")
    app_name = os.getenv("DEEPCHECKS_APP_NAME", "datadog-frustration-agent")
    version = os.getenv("DEEPCHECKS_VERSION", "v1")

    from deepchecks_llm_client.client import DeepchecksLLMClient
    from deepchecks_llm_client.data_types import ApplicationType, ApplicationVersionSchema

    client = DeepchecksLLMClient(host=host, api_token=token)

    existing = {a.name for a in (client.get_applications() or [])}
    if app_name in existing:
        print(f"App '{app_name}' already exists.")
    else:
        client.create_application(
            app_name=app_name,
            app_type=ApplicationType.OTHER,
            description="Agent that analyzes Datadog RUM frustration signals.",
            versions=[ApplicationVersionSchema(name=version)],
        )
        print(f"Created app '{app_name}' with version '{version}'.")
        return 0

    # App exists — ensure the version exists too.
    try:
        client.create_app_version(app_name=app_name, version_name=version)
        print(f"Created version '{version}' for app '{app_name}'.")
    except Exception as e:
        msg = str(e).lower()
        if "already" in msg or "exist" in msg or "409" in msg:
            print(f"Version '{version}' already exists for '{app_name}'.")
        else:
            print(f"Version bootstrap failed: {e}", file=sys.stderr)
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
