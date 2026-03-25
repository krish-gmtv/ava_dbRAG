import argparse
import csv
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CHUNKS = ROOT / "KPIs" / "large_seed_res" / "buyer_quarterly_chunks_v2_final.csv"
EXEC_SCRIPT = ROOT / "scripts" / "execute_answer_with_ava_v1.py"


def parse_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def load_rows(path: Path) -> List[Dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def pick_row(rows: List[Dict[str, Any]], buyer_id: int, year: int, quarter: int) -> Optional[Dict[str, Any]]:
    for r in rows:
        try:
            b = int(r.get("assigned_user_id", ""))
            y = int(r.get("period_year", ""))
            q = int(r.get("period_quarter", ""))
        except ValueError:
            continue
        if b == buyer_id and y == year and q == quarter:
            return r
    return None


def top_value_row(rows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    candidates: List[Tuple[float, Dict[str, Any]]] = []
    for r in rows:
        v = parse_float(r.get("total_sale_value"))
        if v is None:
            continue
        candidates.append((v, r))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]


def run_json(query: str, use_ava: bool, strict_validation: bool) -> Dict[str, Any]:
    cmd = [sys.executable, str(EXEC_SCRIPT), "--query", query]
    if use_ava:
        cmd.append("--use-ava")
    if strict_validation:
        cmd.append("--strict-validation")
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise RuntimeError(
            f"Execution failed for query: {query}\n"
            f"exit={proc.returncode}\n"
            f"stderr={proc.stderr.strip()}\n"
            f"stdout={proc.stdout.strip()}"
        )
    return json.loads(proc.stdout)


def summarize(payload: Dict[str, Any]) -> Dict[str, Any]:
    phrasing = payload.get("phrasing", {}) or {}
    final_response = payload.get("final_response", {}) or {}
    text = str(phrasing.get("text", "") or "").strip()
    first_line = text.splitlines()[0] if text else ""
    return {
        "source_mode": final_response.get("mode"),
        "phrasing_mode": phrasing.get("mode"),
        "first_line": first_line,
        "text_preview": text[:280],
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run contrast test queries using KPI chunk distribution."
    )
    parser.add_argument("--chunks", type=str, default=str(DEFAULT_CHUNKS))
    parser.add_argument("--output-jsonl", type=str, default="logs/ui_contrast_matrix_v1.jsonl")
    parser.add_argument("--use-ava", action="store_true")
    parser.add_argument("--strict-validation", action="store_true")
    args = parser.parse_args()

    chunks_path = Path(args.chunks)
    if not chunks_path.exists():
        raise SystemExit(f"Chunk file not found: {chunks_path}")

    rows = load_rows(chunks_path)
    if not rows:
        raise SystemExit("No rows loaded from chunks file.")

    # Fixed baseline pair from your checks.
    b1_q1_2018 = pick_row(rows, buyer_id=1, year=2018, quarter=1)
    b3_q1_2018 = pick_row(rows, buyer_id=3, year=2018, quarter=1)
    best = top_value_row(rows)

    queries: List[str] = []
    if b1_q1_2018:
        queries.append("How did Buyer 1 perform in Q1 2018?")
    if b3_q1_2018:
        queries.append("How did Buyer 3 perform in Q1 2018?")
    queries.append("What was Buyer 1's close rate in Q1 2018?")
    queries.append("List all upsheets for Buyer 1 in Q1 2018")

    if best:
        bid = int(best["assigned_user_id"])
        py = int(best["period_year"])
        pq = int(best["period_quarter"])
        queries.append(f"How did Buyer {bid} perform in Q{pq} {py}?")
        queries.append(f"What was Buyer {bid}'s close rate in Q{pq} {py}?")

    output_path = Path(args.output_jsonl)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Running {len(queries)} contrast tests (use_ava={args.use_ava})")
    print(f"Top value row: {best['buyer_name']} {best['period_label']} total_sale_value={best['total_sale_value']}" if best else "Top value row: not found")

    with output_path.open("w", encoding="utf-8") as out:
        for i, q in enumerate(queries, start=1):
            payload = run_json(
                query=q,
                use_ava=args.use_ava,
                strict_validation=args.strict_validation,
            )
            summary = summarize(payload)
            record = {
                "idx": i,
                "query": q,
                "summary": summary,
                "payload": payload,
            }
            out.write(json.dumps(record, ensure_ascii=False) + "\n")
            print(f"[{i}/{len(queries)}] {q}")
            print(f"  -> source_mode={summary['source_mode']} phrasing_mode={summary['phrasing_mode']}")
            if summary["first_line"]:
                print(f"  -> {summary['first_line']}")

    print(f"Saved detailed results: {output_path}")


if __name__ == "__main__":
    main()

