import argparse
import csv
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Union


logger = logging.getLogger(__name__)


def setup_logging() -> None:
    """Configure basic logging for the script."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def parse_float(value: Optional[str]) -> Optional[float]:
    """Safely parse a string into a float, returning None for blanks/invalids."""
    if value is None:
        return None
    v = value.strip()
    if v == "" or v.lower() == "null":
        return None
    try:
        return float(v)
    except ValueError:
        return None


def parse_int(value: Optional[str]) -> Optional[int]:
    """Safely parse a string into an int, returning None for blanks/invalids."""
    if value is None:
        return None
    v = value.strip()
    if v == "" or v.lower() == "null":
        return None
    try:
        # some CSVs encode ints as "1.0"
        return int(float(v))
    except ValueError:
        return None


def normalize_list_field(value: Optional[str]) -> List[str]:
    """
    Normalize a possibly string-based list-like field into a list of strings.

    - None/blank -> []
    - JSON array string -> parsed list
    - Comma-separated string -> split & strip
    """
    if value is None:
        return []
    v = value.strip()
    if not v:
        return []

    # Try JSON array first
    if v.startswith("[") and v.endswith("]"):
        try:
            parsed = json.loads(v)
            if isinstance(parsed, list):
                return [str(item) for item in parsed]
        except json.JSONDecodeError:
            pass

    separators = [",", ";"]
    for sep in separators:
        if sep in v:
            return [part.strip() for part in v.split(sep) if part.strip()]

    return [v]


def build_doc_id(assigned_user_id: Optional[int], period_year: Optional[int], period_quarter: Optional[int]) -> str:
    """Build the document ID from buyer and period info."""
    buyer_part = str(assigned_user_id) if assigned_user_id is not None else "NA"
    year_part = str(period_year) if period_year is not None else "NA"
    quarter_part = f"q{period_quarter}" if period_quarter is not None else "qNA"
    return f"buyer_{buyer_part}_{year_part}_{quarter_part}"


def build_period_label(period_year: Optional[int], period_quarter: Optional[int]) -> str:
    """Build a human-readable period label, e.g. 'Q1 2025'."""
    if period_year is None or period_quarter is None:
        return "Unknown period"
    return f"Q{period_quarter} {period_year}"


def generate_embedding_text(
    buyer_full_name: str,
    period_label: str,
    canonical_kpis: Dict[str, Optional[Union[int, float]]],
    interpretation_text: str,
) -> str:
    """
    Generate a natural-language embedding text for the document.

    Includes buyer, period, the six canonical KPIs, and a short summary sentence.
    """
    lines: List[str] = []
    lines.append(f"Quarterly performance summary for {buyer_full_name} in {period_label}.")

    # Canonical KPIs
    tl = canonical_kpis.get("total_leads")
    ltor = canonical_kpis.get("lead_to_opportunity_conversion_rate")
    ou = canonical_kpis.get("opportunity_upsheets")
    cr = canonical_kpis.get("close_rate")
    tsv = canonical_kpis.get("total_sale_value")
    rp = canonical_kpis.get("realization_percent")

    def fmt(v: Optional[Union[int, float]]) -> str:
        return "N/A" if v is None else f"{v:.2f}"

    # More natural narrative while still grounded in numbers
    lines.append(
        f"They handled {fmt(tl)} leads, converting {fmt(ltor)}% into opportunities "
        f"with {fmt(ou)} upsheets that had at least one opportunity."
    )
    lines.append(
        f"Their close rate was {fmt(cr)}%, generating a total sale value of {fmt(tsv)}, "
        f"with realization at {fmt(rp)}% versus expected pricing."
    )

    # Short summary based on interpretation text
    if interpretation_text:
        # Use the first sentence as a compact summary if possible.
        first_sentence = interpretation_text.split("\n")[0]
        lines.append(f"Summary: {first_sentence}")
    else:
        lines.append(
            "Summary: Quarterly performance profile including workload, conversion, execution, value and realization."
        )

    return "\n".join(lines)


def validate_row(row: Dict[str, Any], row_index: int) -> bool:
    """
    Basic validation for required fields.

    We require at minimum:
    - assigned_user_id
    - period_year
    - period_quarter
    """
    assigned_user_id = parse_int(row.get("assigned_user_id"))
    period_year = parse_int(row.get("period_year"))
    period_quarter = parse_int(row.get("period_quarter"))

    if assigned_user_id is None or period_year is None or period_quarter is None:
        logger.warning(
            "Skipping row %d due to missing key identifiers: assigned_user_id=%r, period_year=%r, period_quarter=%r",
            row_index,
            row.get("assigned_user_id"),
            row.get("period_year"),
            row.get("period_quarter"),
        )
        return False

    return True


def build_document_from_row(row: Dict[str, Any], row_index: int) -> Optional[Dict[str, Any]]:
    """Transform a chunk CSV row into a structured JSON-ready document."""
    if not validate_row(row, row_index):
        return None

    # Core identifiers
    assigned_user_id = parse_int(row.get("assigned_user_id"))
    period_year = parse_int(row.get("period_year"))
    period_quarter = parse_int(row.get("period_quarter"))

    period_start = (row.get("period_start") or "").strip()
    period_end = (row.get("period_end") or "").strip()

    buyer_fname = (row.get("buyer_fname") or "").strip()
    buyer_lname = (row.get("buyer_lname") or "").strip()
    buyer_full_name = (f"{buyer_fname} {buyer_lname}".strip()) or (row.get("buyer_name") or "").strip() or "Unknown Buyer"

    # Standardize period label from year/quarter to avoid upstream formatting drift
    period_label = build_period_label(period_year, period_quarter)

    # Canonical KPIs
    # Counts are parsed as integers where appropriate; percentages/money as floats.
    total_leads = parse_int(row.get("total_leads"))
    lead_to_opp_rate = parse_float(row.get("lead_to_opportunity_conversion_rate"))
    opportunity_upsheets = parse_int(row.get("opportunity_upsheets"))
    close_rate = parse_float(row.get("close_rate"))
    total_sale_value = parse_float(row.get("total_sale_value"))
    realization_percent = parse_float(row.get("realization_percent"))

    canonical_kpis = {
        "total_leads": total_leads,
        "lead_to_opportunity_conversion_rate": lead_to_opp_rate,
        "opportunity_upsheets": opportunity_upsheets,
        "close_rate": close_rate,
        "total_sale_value": total_sale_value,
        "realization_percent": realization_percent,
    }

    # Supporting KPIs (count metrics as integers)
    total_upsheets = parse_int(row.get("total_upsheets"))
    leads_with_opportunity = parse_int(row.get("leads_with_opportunity"))
    total_opportunities = parse_int(row.get("total_opportunities"))
    delivered_upsheets = parse_int(row.get("delivered_upsheets"))
    sold_upsheets = parse_int(row.get("sold_upsheets"))

    supporting_kpis = {
        "total_upsheets": total_upsheets,
        "leads_with_opportunity": leads_with_opportunity,
        "total_opportunities": total_opportunities,
        "delivered_upsheets": delivered_upsheets,
        "sold_upsheets": sold_upsheets,
    }

    # Financial detail KPIs
    latest_expected_amount = parse_float(row.get("latest_expected_amount"))
    total_expected_amount = parse_float(row.get("total_expected_amount"))
    avg_sale_value = parse_float(row.get("avg_sale_value"))
    realization_amount = parse_float(row.get("realization_amount"))

    financial_detail_kpis = {
        "latest_expected_amount": latest_expected_amount,
        "total_expected_amount": total_expected_amount,
        "avg_sale_value": avg_sale_value,
        "realization_amount": realization_amount,
    }

    # Interpretation
    interpretation_text = (row.get("interpretation") or "").strip()

    # Use bands from the chunk file to provide a lightweight trend narrative
    workload_band = (row.get("workload_band") or "").strip()
    conversion_band = (row.get("conversion_band") or "").strip()
    execution_band = (row.get("execution_band") or "").strip()
    value_band = (row.get("value_band") or "").strip()
    realization_band = (row.get("realization_band") or "").strip()

    workload_band_l = workload_band.lower()
    conversion_band_l = conversion_band.lower()
    execution_band_l = execution_band.lower()
    value_band_l = value_band.lower()
    realization_band_l = realization_band.lower()

    trend_narrative_parts: List[str] = []
    if workload_band:
        trend_narrative_parts.append(f"Workload was classified as {workload_band}.")
    if conversion_band:
        trend_narrative_parts.append(f"Lead conversion performance was {conversion_band}.")
    if execution_band:
        trend_narrative_parts.append(f"Closing execution was {execution_band}.")
    if value_band:
        trend_narrative_parts.append(f"Value generation was {value_band}.")
    if realization_band:
        trend_narrative_parts.append(f"Pricing realization was {realization_band}.")
    trend_narrative = " ".join(trend_narrative_parts)

    # Derive simple strengths/weaknesses/risks from bands, falling back to any explicit list fields
    strengths = normalize_list_field(row.get("strengths"))
    weaknesses = normalize_list_field(row.get("weaknesses"))
    risks = normalize_list_field(row.get("risks"))
    notable_highlights = normalize_list_field(row.get("notable_highlights"))

    if not strengths:
        if workload_band_l in {"high workload", "moderate workload"}:
            strengths.append("Sustained lead workload.")
        if conversion_band_l == "strong conversion":
            strengths.append("Strong lead-to-opportunity conversion.")
        if execution_band_l == "strong execution":
            strengths.append("Strong closing execution.")
        if value_band_l == "strong value generation":
            strengths.append("Strong realized value from sales.")
        if realization_band_l == "above target realization":
            strengths.append("Pricing above expected realization.")

    if not weaknesses:
        if workload_band_l == "low workload":
            weaknesses.append("Low lead workload.")
        if conversion_band_l == "weak conversion":
            weaknesses.append("Weak lead-to-opportunity conversion.")
        if execution_band_l == "weak execution":
            weaknesses.append("Weak closing execution.")
        if value_band_l == "low value generation":
            weaknesses.append("Low realized sales value.")
        if realization_band_l == "below expected pricing":
            weaknesses.append("Pricing below expected realization.")

    if not risks:
        if execution_band_l == "no closing activity":
            risks.append("No closing activity observed in this period.")
        if realization_band_l == "no realization data":
            risks.append("No pricing realization data available for this period.")

    interpretation_block = {
        "executive_summary": interpretation_text or "",
        "trend_narrative": trend_narrative,
        "strengths": strengths,
        "weaknesses": weaknesses,
        "notable_highlights": notable_highlights,
        "risks": risks,
    }

    # Embedding text
    embedding_text = generate_embedding_text(
        buyer_full_name=buyer_full_name,
        period_label=period_label,
        canonical_kpis=canonical_kpis,
        interpretation_text=interpretation_text,
    )

    # Provenance
    generated_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    document = {
        "doc_id": build_doc_id(assigned_user_id, period_year, period_quarter),
        "summary_level": "quarter",
        "buyer": {
            "assigned_user_id": assigned_user_id,
            "buyer_fname": buyer_fname or None,
            "buyer_lname": buyer_lname or None,
            "buyer_full_name": buyer_full_name,
        },
        "period": {
            "period_start": period_start or None,
            "period_end": period_end or None,
            "period_year": period_year,
            "period_quarter": period_quarter,
            "period_label": period_label,
        },
        "canonical_kpis": canonical_kpis,
        "supporting_kpis": supporting_kpis,
        "financial_detail_kpis": financial_detail_kpis,
        "interpretation": interpretation_block,
        "embedding_text": embedding_text,
        "provenance": {
            "source_type": row.get("doc_type") or "buyer_kpi_quarterly_summary",
            "source_sql_version": row.get("kpi_version") or "buyer_kpi_quarterly_v2",
            "generated_at": generated_at,
        },
    }

    return document


def process_file(input_path: Path, output_path: Path) -> None:
    """Read the quarterly chunks CSV and write a JSONL of vector-ready docs."""
    with input_path.open("r", encoding="utf-8-sig", newline="") as infile, output_path.open(
        "w", encoding="utf-8", newline=""
    ) as outfile:
        reader = csv.DictReader(infile)

        logger.info("Detected input columns: %s", reader.fieldnames)

        count_total = 0
        count_written = 0
        seen_doc_ids: set[str] = set()

        for idx, row in enumerate(reader, start=1):
            count_total += 1
            try:
                doc = build_document_from_row(row, idx)
            except Exception as exc:
                logger.warning("Error processing row %d: %s", idx, exc)
                continue

            if doc is None:
                continue

            doc_id = doc.get("doc_id")
            if isinstance(doc_id, str):
                if doc_id in seen_doc_ids:
                    logger.warning("Skipping row %d due to duplicate doc_id: %s", idx, doc_id)
                    continue
                seen_doc_ids.add(doc_id)

            outfile.write(json.dumps(doc, ensure_ascii=False) + "\n")
            count_written += 1

        logger.info("Processed %d rows, wrote %d documents.", count_total, count_written)


def main() -> None:
    """CLI entrypoint."""
    setup_logging()

    parser = argparse.ArgumentParser(
        description="Build buyer-quarter vector-ready JSONL documents from quarterly KPI chunks."
    )
    parser.add_argument(
        "--input",
        type=str,
        default="KPIs/large_seed_res/buyer_quarterly_chunks_v2.csv",
        help="Path to the buyer quarterly chunks CSV file.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="KPIs/large_seed_res/buyer_quarter_vector_docs.jsonl",
        help="Path to write the JSONL output.",
    )

    args = parser.parse_args()
    input_path = Path(args.input)
    output_path = Path(args.output)

    if not input_path.exists():
        logger.error("Input file not found: %s", input_path)
        raise SystemExit(1)

    output_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info("Building quarterly vector docs from %s to %s", input_path, output_path)
    process_file(input_path, output_path)


if __name__ == "__main__":
    main()

