#!/usr/bin/env python3
"""Fix main_leads section: add one created_at per row (for rows 1-119 that are missing it)."""
from datetime import datetime, timedelta

SEED_PATH = "sandbox_seed_pg.sql"
ROWS = 120
START = datetime(2023, 1, 1, 12, 0, 0)

def main():
    ts = [(START + timedelta(days=i * 9)).strftime("%Y-%m-%d %H:%M:%S+00") for i in range(ROWS)]
    with open(SEED_PATH, "r", encoding="utf-8") as f:
        c = f.read()
    for i in range(1, ROWS):
        old = ", " + str(i) + "),"
        new = ", " + str(i) + ", '" + ts[i - 1] + "'),"
        c = c.replace(old, new, 1)
    with open(SEED_PATH, "w", encoding="utf-8") as f:
        f.write(c)
    print("Added created_at to main_leads rows 1-119")

if __name__ == "__main__":
    main()
