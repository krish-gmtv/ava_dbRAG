#!/usr/bin/env python3
"""Add created_at column and spread timestamps to sandbox_seed_pg.sql for time-based querying."""

from datetime import datetime, timedelta

SEED_PATH = "sandbox_seed_pg.sql"
ROWS_PER_TABLE = 120
DAYS_STEP = 9  # spread over ~3 years: 120 * 9 = 1080 days
START = datetime(2023, 1, 1, 12, 0, 0)

def timestamps():
    for i in range(ROWS_PER_TABLE):
        t = START + timedelta(days=i * DAYS_STEP)
        yield t.strftime("%Y-%m-%d %H:%M:%S+00")

def main():
    with open(SEED_PATH, "r", encoding="utf-8") as f:
        content = f.read()

    ts = list(timestamps())

    # 1) main_buyers: add ", created_at" to header and ", 'ts'" to each value line
    content = content.replace(
        "INSERT INTO main_buyers (buyer_phone, buyer_first_name, buyer_last_name, buyer_email)\nVALUES",
        "INSERT INTO main_buyers (buyer_phone, buyer_first_name, buyer_last_name, buyer_email, created_at)\nVALUES",
    )
    lines = content.split("\n")
    in_buyers = False
    buyer_row = 0
    new_lines = []
    for i, line in enumerate(lines):
        if "INSERT INTO main_buyers" in line:
            in_buyers = True
            buyer_row = 0
            new_lines.append(line)
            continue
        if in_buyers and buyer_row < ROWS_PER_TABLE and ("@mail.com')," in line or "@mail.com');" in line):
            # append timestamp before the );
            if line.rstrip().endswith(");"):
                line = line.replace(");", ", '" + ts[buyer_row] + "');")
            else:
                line = line.replace("),", ", '" + ts[buyer_row] + "'),")
            buyer_row += 1
            new_lines.append(line)
            if buyer_row == ROWS_PER_TABLE:
                in_buyers = False
            continue
        new_lines.append(line)

    content = "\n".join(new_lines)

    # 2) main_leads
    content = content.replace(
        "INSERT INTO main_leads (lead_phone, lead_fname, lead_lname, lead_email, lead_buyer)\nVALUES",
        "INSERT INTO main_leads (lead_phone, lead_fname, lead_lname, lead_email, lead_buyer, created_at)\nVALUES",
    )
    lines = content.split("\n")
    in_leads = False
    lead_row = 0
    new_lines = []
    for i, line in enumerate(lines):
        if "INSERT INTO main_leads" in line:
            in_leads = True
            lead_row = 0
            new_lines.append(line)
            continue
        if in_leads and lead_row < ROWS_PER_TABLE and "@leadmail.com'" in line and ("), " in line or "); " in line or line.strip().endswith(");")):
            if ");" in line:
                line = line.replace(");", ", '" + ts[lead_row] + "');")
            else:
                line = line.replace("),", ", '" + ts[lead_row] + "'),")
            lead_row += 1
            new_lines.append(line)
            if lead_row == ROWS_PER_TABLE:
                in_leads = False
            continue
        new_lines.append(line)
    content = "\n".join(new_lines)

    # 3) main_cars: value lines end with , N); or , N),
    content = content.replace(
        "INSERT INTO main_cars (car_vin, car_year, car_make, car_model, car_body, car_mileage, car_external_condition, car_internal_condition, car_title_condition, car_lead)\nVALUES",
        "INSERT INTO main_cars (car_vin, car_year, car_make, car_model, car_body, car_mileage, car_external_condition, car_internal_condition, car_title_condition, car_lead, created_at)\nVALUES",
    )
    lines = content.split("\n")
    in_cars = False
    car_row = 0
    new_lines = []
    for line in lines:
        if "INSERT INTO main_cars" in line:
            in_cars = True
            car_row = 0
            new_lines.append(line)
            continue
        if in_cars and car_row < ROWS_PER_TABLE and "car_lead)" not in line and ("Clean', " in line or "Clean', " in line) and (line.strip().endswith(");") or line.strip().endswith("),")):
            if ");" in line:
                line = line.replace(");", ", '" + ts[car_row] + "');")
            else:
                line = line.replace("),", ", '" + ts[car_row] + "'),")
            car_row += 1
            new_lines.append(line)
            if car_row == ROWS_PER_TABLE:
                in_cars = False
            continue
        new_lines.append(line)
    content = "\n".join(new_lines)

    # 4) main_pickups: (pickup_lead, pickup_car, pickup_contact_name, pickup_contact_phone, pickup_pincode)
    content = content.replace(
        "INSERT INTO main_pickups (pickup_lead, pickup_car, pickup_contact_name, pickup_contact_phone, pickup_pincode)\nVALUES",
        "INSERT INTO main_pickups (pickup_lead, pickup_car, pickup_contact_name, pickup_contact_phone, pickup_pincode, created_at)\nVALUES",
    )
    lines = content.split("\n")
    in_pickups = False
    pickup_row = 0
    new_lines = []
    for line in lines:
        if "INSERT INTO main_pickups" in line:
            in_pickups = True
            pickup_row = 0
            new_lines.append(line)
            continue
        if in_pickups and pickup_row < ROWS_PER_TABLE and "')," in line or "');" in line:
            # line like (1, 1, 'Name', '+1-...', '32801'), or (120, 120, '...', '...', '50302');
            if ");" in line:
                line = line.replace(");", ", '" + ts[pickup_row] + "');")
            else:
                line = line.replace("),", ", '" + ts[pickup_row] + "'),")
            pickup_row += 1
            new_lines.append(line)
            if pickup_row == ROWS_PER_TABLE:
                in_pickups = False
            continue
        new_lines.append(line)
    content = "\n".join(new_lines)

    with open(SEED_PATH, "w", encoding="utf-8") as f:
        f.write(content)
    print("Added created_at and timestamps to sandbox_seed_pg.sql")

if __name__ == "__main__":
    main()
