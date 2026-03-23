import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict


SCRIPTS_DIR = Path(__file__).resolve().parent


def run_json(script_path: Path, args: list[str]) -> Dict[str, Any]:
    cmd = [sys.executable, str(script_path), *args]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise RuntimeError(
            f"{script_path.name} failed.\n"
            f"exit_code={proc.returncode}\n"
            f"stderr={proc.stderr.strip()}\n"
            f"stdout={proc.stdout.strip()}"
        )
    out = proc.stdout.strip()
    if not out:
        raise RuntimeError(f"{script_path.name} returned empty output.")
    return json.loads(out)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Single-entry answer execution with safe Ava phrasing wrapper.\n"
            "By default this uses deterministic phrasing. Add --use-ava to call Ava."
        )
    )
    parser.add_argument("--query", type=str, required=True)
    parser.add_argument("--use-ava", action="store_true")
    parser.add_argument("--strict-validation", action="store_true")
    parser.add_argument(
        "--thread-id",
        type=str,
        default="",
        help="Optional thread id to isolate Ava sessions within the same user.",
    )
    parser.add_argument(
        "--app-user-id",
        type=str,
        default="",
        help="Application user id for Ava session scoping (not auth username).",
    )
    args = parser.parse_args()

    phraser_args = ["--query", args.query]
    if args.use_ava:
        phraser_args.append("--use-ava")
    if args.strict_validation:
        phraser_args.append("--strict-validation")
    if args.thread_id.strip():
        phraser_args.extend(["--thread-id", args.thread_id.strip()])
    if args.app_user_id.strip():
        phraser_args.extend(["--app-user-id", args.app_user_id.strip()])

    result = run_json(SCRIPTS_DIR / "ava_phraser_v1.py", phraser_args)
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()

