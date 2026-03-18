import argparse
import json
import logging
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Optional


logger = logging.getLogger(__name__)


SCRIPTS_DIR = Path(__file__).resolve().parent


HANDLER_TO_SCRIPT = {
    # precise
    "precise_list_buyer_upsheets": SCRIPTS_DIR / "precise_list_buyer_upsheets.py",
    "precise_get_buyer_quarter_kpis": SCRIPTS_DIR / "precise_get_buyer_quarter_kpis.py",
    # semantic
    "semantic_buyer_performance_summary": SCRIPTS_DIR / "semantic_search_pinecone_final.py",
}


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def run_python_json(script_path: Path, query: str) -> Dict[str, Any]:
    """
    Run a python script that prints JSON to stdout.
    Returns parsed JSON dict or raises on failure.
    """
    if not script_path.exists():
        raise FileNotFoundError(f"Handler script not found: {script_path}")

    cmd = [sys.executable, str(script_path), "--query", query]
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=False,
    )

    if proc.returncode != 0:
        raise RuntimeError(
            "Handler failed.\n"
            f"script={script_path}\n"
            f"exit_code={proc.returncode}\n"
            f"stderr={proc.stderr.strip()}\n"
            f"stdout={proc.stdout.strip()}"
        )

    out = proc.stdout.strip()
    if not out:
        raise RuntimeError(f"Handler produced no output: {script_path}")

    try:
        return json.loads(out)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            "Handler output is not valid JSON.\n"
            f"script={script_path}\n"
            f"error={exc}\n"
            f"raw_output={out[:2000]}"
        ) from exc


def main() -> None:
    setup_logging()

    parser = argparse.ArgumentParser(
        description=(
            "End-to-end execution flow: route a query, run the selected handler, "
            "and return a combined normalized payload."
        )
    )
    parser.add_argument("--query", type=str, required=True, help="User query to execute.")
    args = parser.parse_args()

    q = args.query.strip()
    if not q:
        raise SystemExit("Query must not be empty.")

    # 1) Route
    router_script = SCRIPTS_DIR / "intent_router_v1.py"
    logger.info("Routing query with intent_router_v1.py...")
    plan = run_python_json(router_script, q)

    handler = ((plan.get("retrieval_plan") or {}).get("handler")) if isinstance(plan, dict) else None
    if not isinstance(handler, str) or not handler:
        raise RuntimeError("Router plan missing retrieval_plan.handler")

    script_path = HANDLER_TO_SCRIPT.get(handler)
    if script_path is None:
        raise RuntimeError(
            f"Unknown handler '{handler}'. "
            f"Known handlers: {sorted(HANDLER_TO_SCRIPT.keys())}"
        )

    # 2) Execute handler
    logger.info("Executing handler=%s via %s", handler, script_path.name)
    handler_output = run_python_json(script_path, q)

    # 3) Combined normalized payload
    combined = {
        "execution_plan": plan,
        "selected_handler": handler,
        "handler_output": handler_output,
    }

    print(json.dumps(combined, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()

