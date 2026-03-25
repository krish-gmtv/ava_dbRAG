import argparse
import csv
from pathlib import Path
from typing import Optional


def parse_float(value: str) -> Optional[float]:
    if value is None:
        return None
    v = value.strip()
    if v == "" or v.lower() == "null":
        return None
    try:
        return float(v)
    except ValueError:
        return None


def parse_int(value: str) -> Optional[int]:
    if value is None:
        return None
    v = value.strip()
    if v == "" or v.lower() == "null":
        return None
    try:
        return int(float(v))
    except ValueError:
        return None


def workload_band(total_leads: Optional[float]) -> str:
    if total_leads is None:
        return "no workload data"
    if total_leads >= 8:
        return "high workload"
    if total_leads >= 4:
        return "moderate workload"
    return "low workload"


def conversion_band(rate: Optional[float]) -> str:
    if rate is None:
        return "no conversion activity"
    if rate >= 70:
        return "strong conversion"
    if rate >= 40:
        return "moderate conversion"
    return "weak conversion"


def execution_band(rate: Optional[float]) -> str:
    if rate is None:
        return "no closing activity"
    if rate >= 70:
        return "strong execution"
    if rate >= 40:
        return "moderate execution"
    return "weak execution"


def value_band(total_sale_value: Optional[float]) -> str:
    if total_sale_value is None:
        return "no realized sale value"
    if total_sale_value >= 25000:
        return "strong value generation"
    if total_sale_value >= 10000:
        return "moderate value generation"
    return "low value generation"


def realization_band(realization_percent: Optional[float]) -> str:
    if realization_percent is None:
        return "no realization data"
    if realization_percent > 5:
        return "above target realization"
    if realization_percent >= -5:
        return "near expected pricing"
    return "below expected pricing"


def format_percent(value: Optional[float]) -> str:
    if value is None:
        return "N/A"
    return f"{value:.2f}%"


def format_number(value: Optional[float]) -> str:
    if value is None:
        return "N/A"
    return f"{value:.2f}"


def build_chunk_id(
    assigned_user_id: Optional[int],
    period_year: Optional[int],
    period_quarter: Optional[int],
) -> str:
    return f"kpi_buyer_{assigned_user_id or 'NA'}_{period_year or 'NA'}_q{period_quarter or 'NA'}"


def build_interpretation_text(
    buyer_label: str,
    period_year: Optional[int],
    period_quarter: Optional[int],
    total_sale_value: Optional[float],
    close_rate: Optional[float],
    workload_band_label: str,
    conversion_band_label: str,
    execution_band_label: str,
    value_band_label: str,
    realization_band_label: str,
) -> str:
    year_str = str(period_year) if period_year is not None else "Unknown year"
    quarter_str = f"Q{period_quarter}" if period_quarter is not None else "Unknown quarter"
    period_label = f"{year_str} {quarter_str}"

    lines = []
    wl_l = workload_band_label.strip().lower()
    if wl_l == "low workload":
        lines.append(
            f"In {period_label}, despite relatively low lead volume, {buyer_label} showed "
            f"{conversion_band_label} on lead conversion and {execution_band_label} on closing execution."
        )
    else:
        lines.append(
            f"In {period_label}, {buyer_label} showed {conversion_band_label} on lead conversion "
            f"and {execution_band_label} on closing execution."
        )
    lines.append(
        f"Activity level (lead volume) was classified as {workload_band_label}."
    )
    lines.append(
        f"The buyer generated {value_band_label}, with pricing performance classified as "
        f"{realization_band_label}."
    )
    if total_sale_value is not None:
        lines.append(
            f"Total realized sale value for this period was {format_number(total_sale_value)}."
        )
    if close_rate is None:
        lines.append(
            "No closing basis was available for this quarter (no opportunities reached a closing stage)."
        )

    return "\n".join(lines)


def build_chunk_text(
    buyer_label: str,
    period_year: Optional[int],
    period_quarter: Optional[int],
    period_start: str,
    period_end: str,
    total_leads: Optional[float],
    total_upsheets: Optional[float],
    leads_with_opportunity: Optional[float],
    total_opportunities: Optional[float],
    opportunity_upsheets: Optional[float],
    delivered_upsheets: Optional[float],
    sold_upsheets: Optional[float],
    conversion_rate: Optional[float],
    close_rate: Optional[float],
    total_sale_value: Optional[float],
    workload_band_label: str,
    conversion_band_label: str,
    execution_band_label: str,
    value_band_label: str,
    realization_band_label: str,
) -> str:
    year_str = str(period_year) if period_year is not None else "Unknown year"
    quarter_str = f"Q{period_quarter}" if period_quarter is not None else "Unknown quarter"
    period_label = f"{year_str} {quarter_str}"

    lines = []
    lines.append("Buyer Quarterly Performance Summary")
    lines.append("")
    lines.append(f"Buyer: {buyer_label}")
    lines.append(f"Period: {period_label}")
    lines.append(f"Period Start: {period_start or 'N/A'}")
    lines.append(f"Period End: {period_end or 'N/A'}")
    lines.append("")
    lines.append("KPI Snapshot")
    lines.append(f"Total Leads: {int(total_leads) if total_leads is not None else 'N/A'}")
    lines.append(f"Total Upsheets: {int(total_upsheets) if total_upsheets is not None else 'N/A'}")
    lines.append(
        f"Leads with Opportunity: {int(leads_with_opportunity) if leads_with_opportunity is not None else 'N/A'}"
    )
    lines.append(
        f"Total Opportunities: {int(total_opportunities) if total_opportunities is not None else 'N/A'}"
    )
    lines.append(
        f"Opportunity Upsheets: {int(opportunity_upsheets) if opportunity_upsheets is not None else 'N/A'}"
    )
    lines.append(
        f"Delivered Upsheets: {int(delivered_upsheets) if delivered_upsheets is not None else 'N/A'}"
    )
    lines.append(
        f"Sold Upsheets: {int(sold_upsheets) if sold_upsheets is not None else 'N/A'}"
    )
    lines.append(f"Conversion Rate: {format_percent(conversion_rate)}")
    lines.append(f"Close Rate: {format_percent(close_rate)}")
    lines.append(f"Total Sale Value: {format_number(total_sale_value)}")
    lines.append("")
    lines.append("Interpretation")
    interpretation_text = build_interpretation_text(
        buyer_label=buyer_label,
        period_year=period_year,
        period_quarter=period_quarter,
        total_sale_value=total_sale_value,
        close_rate=close_rate,
        workload_band_label=workload_band_label,
        conversion_band_label=conversion_band_label,
        execution_band_label=execution_band_label,
        value_band_label=value_band_label,
        realization_band_label=realization_band_label,
    )
    lines.append(interpretation_text)

    return "\n".join(lines)


def process_file(input_path: Path, output_path: Path) -> None:
    with input_path.open("r", encoding="utf-8-sig", newline="") as infile, output_path.open(
        "w", encoding="utf-8", newline=""
    ) as outfile:
        reader = csv.DictReader(infile)

        fieldnames = [
            "chunk_id",
            "assigned_user_id",
            "buyer_name",
            "buyer_fname",
            "buyer_lname",
            "period_start",
            "period_end",
            "period_year",
            "period_quarter",
            "period_label",
            "total_leads",
            "total_upsheets",
            "leads_with_opportunity",
            "total_opportunities",
            "opportunity_upsheets",
            "delivered_upsheets",
            "sold_upsheets",
            "delivered_opportunity_upsheets",
            "total_expected_amount",
            "latest_expected_amount",
            "avg_sale_value",
            "realization_amount",
            "lead_to_opportunity_conversion_rate",
            "close_rate",
            "total_sale_value",
            "realization_percent",
            "workload_band",
            "conversion_band",
            "execution_band",
            "value_band",
            "realization_band",
            "doc_type",
            "kpi_version",
            "source_file",
            "interpretation",
            "chunk_text",
        ]

        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            assigned_user_id = parse_int(row.get("assigned_user_id"))
            buyer_fname = (row.get("buyer_fname") or "").strip()
            buyer_lname = (row.get("buyer_lname") or "").strip()
            period_start = (row.get("period_start") or "").strip()
            period_end = (row.get("period_end") or "").strip()
            period_year = parse_int(row.get("period_year"))
            period_quarter = parse_int(row.get("period_quarter"))

            total_leads = parse_float(row.get("total_leads"))
            total_upsheets = parse_float(row.get("total_upsheets"))
            leads_with_opportunity = parse_float(row.get("leads_with_opportunity"))
            total_opportunities = parse_float(row.get("total_opportunities"))
            opportunity_upsheets = parse_float(row.get("opportunity_upsheets"))
            delivered_upsheets = parse_float(row.get("delivered_upsheets"))
            sold_upsheets = parse_float(row.get("sold_upsheets"))
            delivered_opportunity_upsheets = parse_float(
                row.get("delivered_opportunity_upsheets")
            )
            total_expected_amount = parse_float(row.get("total_expected_amount"))
            latest_expected_amount = parse_float(row.get("latest_expected_amount"))
            avg_sale_value = parse_float(row.get("avg_sale_value"))
            realization_amount = parse_float(row.get("realization_amount"))
            lead_to_opp_rate = parse_float(
                row.get("lead_to_opportunity_conversion_rate")
            )
            close_rate = parse_float(row.get("close_rate"))
            total_sale_value = parse_float(row.get("total_sale_value"))
            realization_pct = parse_float(row.get("realization_percent"))

            workload = workload_band(total_leads)
            conversion = conversion_band(lead_to_opp_rate)
            execution = execution_band(close_rate)
            value = value_band(total_sale_value)
            realization = realization_band(realization_pct)

            buyer_label = f"{buyer_fname} {buyer_lname}".strip() or (
                f"Buyer{assigned_user_id}" if assigned_user_id is not None else "Unknown Buyer"
            )

            chunk_id = build_chunk_id(assigned_user_id, period_year, period_quarter)
            period_label = (
                f"{period_year} Q{period_quarter}"
                if period_year is not None and period_quarter is not None
                else ""
            )

            chunk_text = build_chunk_text(
                buyer_label=buyer_label,
                period_year=period_year,
                period_quarter=period_quarter,
                period_start=period_start,
                period_end=period_end,
                total_leads=total_leads,
                total_upsheets=total_upsheets,
                leads_with_opportunity=leads_with_opportunity,
                total_opportunities=total_opportunities,
                opportunity_upsheets=opportunity_upsheets,
                delivered_upsheets=delivered_upsheets,
                sold_upsheets=sold_upsheets,
                conversion_rate=lead_to_opp_rate,
                close_rate=close_rate,
                total_sale_value=total_sale_value,
                workload_band_label=workload,
                conversion_band_label=conversion,
                execution_band_label=execution,
                value_band_label=value,
                realization_band_label=realization,
            )

            interpretation_text = build_interpretation_text(
                buyer_label=buyer_label,
                period_year=period_year,
                period_quarter=period_quarter,
                total_sale_value=total_sale_value,
                close_rate=close_rate,
                workload_band_label=workload,
                conversion_band_label=conversion,
                execution_band_label=execution,
                value_band_label=value,
                realization_band_label=realization,
            )

            writer.writerow(
                {
                    "chunk_id": chunk_id,
                    "assigned_user_id": assigned_user_id if assigned_user_id is not None else "",
                    "buyer_name": buyer_label,
                    "buyer_fname": buyer_fname,
                    "buyer_lname": buyer_lname,
                    "period_start": period_start,
                    "period_end": period_end,
                    "period_year": period_year if period_year is not None else "",
                    "period_quarter": period_quarter if period_quarter is not None else "",
                    "period_label": period_label,
                    "total_leads": "" if total_leads is None else int(total_leads),
                    "total_upsheets": "" if total_upsheets is None else int(total_upsheets),
                    "leads_with_opportunity": ""
                    if leads_with_opportunity is None
                    else int(leads_with_opportunity),
                    "total_opportunities": ""
                    if total_opportunities is None
                    else int(total_opportunities),
                    "opportunity_upsheets": ""
                    if opportunity_upsheets is None
                    else int(opportunity_upsheets),
                    "delivered_upsheets": ""
                    if delivered_upsheets is None
                    else int(delivered_upsheets),
                    "sold_upsheets": "" if sold_upsheets is None else int(sold_upsheets),
                    "delivered_opportunity_upsheets": ""
                    if delivered_opportunity_upsheets is None
                    else int(delivered_opportunity_upsheets),
                    "total_expected_amount": ""
                    if total_expected_amount is None
                    else f"{total_expected_amount:.2f}",
                    "latest_expected_amount": ""
                    if latest_expected_amount is None
                    else f"{latest_expected_amount:.2f}",
                    "avg_sale_value": ""
                    if avg_sale_value is None
                    else f"{avg_sale_value:.2f}",
                    "realization_amount": ""
                    if realization_amount is None
                    else f"{realization_amount:.2f}",
                    "lead_to_opportunity_conversion_rate": ""
                    if lead_to_opp_rate is None
                    else f"{lead_to_opp_rate:.2f}",
                    "close_rate": "" if close_rate is None else f"{close_rate:.2f}",
                    "total_sale_value": "" if total_sale_value is None else f"{total_sale_value:.2f}",
                    "realization_percent": ""
                    if realization_pct is None
                    else f"{realization_pct:.2f}",
                    "workload_band": workload,
                    "conversion_band": conversion,
                    "execution_band": execution,
                    "value_band": value,
                    "realization_band": realization,
                    "doc_type": "buyer_quarterly_kpi",
                    "kpi_version": "v2",
                    "source_file": str(input_path),
                    "interpretation": interpretation_text,
                    "chunk_text": chunk_text,
                }
            )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Generate buyer-quarter level semantic chunks (v2) from the quarterly KPI CSV."
        )
    )
    parser.add_argument(
        "--input",
        type=str,
        default="KPIs/large_seed_res/kpi_buyer_quarterly.csv",
        help="Path to the quarterly KPI summary CSV.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="KPIs/large_seed_res/buyer_quarterly_chunks_v2.csv",
        help="Path to write the generated chunks CSV.",
    )

    args = parser.parse_args()
    input_path = Path(args.input)
    output_path = Path(args.output)

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    process_file(input_path, output_path)


if __name__ == "__main__":
    main()

