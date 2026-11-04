#!/usr/bin/env python3
"""Create private deployment configuration without replacing existing values."""

import os
from pathlib import Path
import secrets


def initialize(environment: Path, template: Path) -> bool:
    """Copy the template with fresh service passwords, preserving an existing file."""
    content = template.read_text(encoding="utf-8")
    for name in ("POSTGRES_PASSWORD", "RABBITMQ_PASSWORD"):
        content = content.replace(f"{name}=\n", f"{name}={secrets.token_urlsafe(32)}\n")
    try:
        descriptor = os.open(environment, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        return False
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(content)
    return True


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[1]
    created = initialize(root / ".env", root / ".env.example")
    print("Created private .env." if created else "Existing .env preserved.")
