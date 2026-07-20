from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import subprocess
import sys
from pathlib import Path


MANDANT_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


def parse_stichtage(raw: str) -> list[str]:
    try:
        values = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise argparse.ArgumentTypeError("stichtage must be a JSON array") from exc
    if not isinstance(values, list) or not values:
        raise argparse.ArgumentTypeError("stichtage must be a non-empty JSON array")
    if len(values) > 366:
        raise argparse.ArgumentTypeError("at most 366 stichtage are allowed")
    for value in values:
        if not isinstance(value, str):
            raise argparse.ArgumentTypeError("every stichtag must be a string")
        try:
            dt.date.fromisoformat(value)
        except ValueError as exc:
            raise argparse.ArgumentTypeError(f"invalid ISO date: {value}") from exc
    return values


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mandant", required=True)
    parser.add_argument("--stichtage", required=True, type=parse_stichtage)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--project-dir", default="/opt/pipeline")
    parser.add_argument("--profiles-dir", default="/opt/pipeline")
    parser.add_argument("--target", default="openshift")
    args = parser.parse_args()

    if not MANDANT_RE.fullmatch(args.mandant):
        parser.error("mandant must match [A-Za-z0-9_-]{1,64}")
    safe_run_id = re.sub(r"[^A-Za-z0-9_-]", "_", args.run_id)[:128]
    dbt_vars = json.dumps(
        {"mandant": args.mandant, "stichtage": args.stichtage, "run_id": safe_run_id}
    )
    command = [
        sys.executable,
        "-m",
        "dbt",
        "build",
        "--project-dir",
        str(Path(args.project_dir)),
        "--profiles-dir",
        str(Path(args.profiles_dir)),
        "--target",
        args.target,
        "--vars",
        dbt_vars,
    ]
    return subprocess.run(command, check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())

