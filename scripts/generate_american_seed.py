#!/usr/bin/env python3
"""Generate Americanized seed data for ava_sandbox (names, US phones, US ZIPs)."""

import random

# American first names (mix M/F)
FIRST = [
    "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
    "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Charles", "Karen", "Christopher", "Lisa", "Daniel", "Nancy",
    "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra", "Donald", "Ashley",
    "Steven", "Kimberly", "Paul", "Emily", "Andrew", "Donna", "Joshua", "Michelle",
    "Kenneth", "Carol", "Kevin", "Amanda", "Brian", "Dorothy", "George", "Melissa",
    "Edward", "Deborah", "Ronald", "Stephanie", "Timothy", "Rebecca", "Jason", "Sharon",
    "Jeffrey", "Laura", "Ryan", "Cynthia", "Jacob", "Kathleen", "Gary", "Amy",
    "Nicholas", "Angela", "Eric", "Shirley", "Jonathan", "Anna", "Stephen", "Brenda",
    "Larry", "Pamela", "Justin", "Emma", "Scott", "Nicole", "Brandon", "Helen",
    "Benjamin", "Samantha", "Samuel", "Katherine", "Frank", "Christine", "Gregory", "Debra",
    "Raymond", "Rachel", "Patrick", "Carolyn", "Jack", "Janet", "Dennis", "Catherine",
    "Jerry", "Maria", "Tyler", "Heather", "Aaron", "Diane", "Henry", "Ruth",
    "Douglas", "Joyce", "Peter", "Virginia", "Adam", "Victoria", "Nathan", "Kelly",
]
# American last names
LAST = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas",
    "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson", "White",
    "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker", "Young",
    "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
    "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
    "Carter", "Roberts", "Gomez", "Phillips", "Evans", "Turner", "Diaz", "Parker",
    "Cruz", "Edwards", "Collins", "Reyes", "Stewart", "Morris", "Morales", "Murphy",
    "Cook", "Rogers", "Gutierrez", "Ortiz", "Morgan", "Cooper", "Peterson", "Bailey",
    "Reed", "Kelly", "Howard", "Ramos", "Kim", "Cox", "Ward", "Richardson",
    "Watson", "Brooks", "Chavez", "Wood", "James", "Bennett", "Gray", "Mendoza",
    "Ruiz", "Hughes", "Price", "Alvarez", "Castillo", "Sanders", "Myers",
]
# US area codes
AREAS = ["212", "310", "312", "415", "512", "617", "702", "305", "404", "202", "503", "206", "303", "619", "713", "214", "818", "347", "646", "718"]
# US ZIP codes (spread across regions)
ZIPS = [
    "10001", "10002", "90210", "90211", "60601", "60602", "94102", "94103",
    "75201", "75202", "02101", "02102", "33101", "33102", "30301", "30302",
    "85001", "85002", "98101", "98102", "19101", "19102", "43201", "43202",
    "55401", "55402", "48201", "48202", "37201", "37202", "80201", "80202",
    "46201", "46202", "53201", "53202", "64101", "64102", "73101", "73102",
    "28201", "28202", "35201", "35202", "32801", "32802", "76101", "76102",
    "87101", "87102", "50301", "50302", "83701", "83702", "89101", "89102",
]

def us_phone(n):
    area = AREAS[n % len(AREAS)]
    rest = 1000 + (n % 9000)
    return f"+1-{area}-555-{rest:04d}"

def email(fname, lname, n, domain="mail.com"):
    return f"{fname.lower()}.{lname.lower()}{n}@{domain}"

def main():
    random.seed(42)
    n = 120
    # Build unique (fname, lname) for buyers
    names_used = set()
    buyers = []
    while len(buyers) < n:
        f, l = random.choice(FIRST), random.choice(LAST)
        if (f, l) in names_used:
            continue
        names_used.add((f, l))
        buyers.append((f, l))
    # Leads: same count, can overlap names with buyers
    leads = []
    for i in range(n):
        f, l = random.choice(FIRST), random.choice(LAST)
        leads.append((f, l))
    # Contact names for pickups
    contacts = []
    for i in range(n):
        f, l = random.choice(FIRST), random.choice(LAST)
        contacts.append(f"{f} {l}")

    # ZIPs for pickups (repeat to get 120)
    zip_list = [ZIPS[i % len(ZIPS)] for i in range(n)]
    random.shuffle(zip_list)

    # Generate PG file
    pg_lines = [
        "-- Realistic sandbox seed data for ava_sandbox (PostgreSQL)",
        "-- American names, US phone numbers, US ZIP codes.",
        "-- Run after 01_sandbox_schema.sql. Connect to database: ava_sandbox",
        "-- Safe for reruns: truncates and repopulates.",
        "",
        "TRUNCATE TABLE main_pickups, main_cars, main_leads, main_buyers RESTART IDENTITY CASCADE;",
        "",
        "INSERT INTO main_buyers (buyer_id, buyer_phone, buyer_first_name, buyer_last_name, buyer_email)",
        "OVERRIDING SYSTEM VALUE",
        "VALUES",
    ]
    for i in range(n):
        f, l = buyers[i]
        comma = "," if i < n - 1 else ";"
        pg_lines.append(f"({i+1}, '{us_phone(i)}', '{f}', '{l}', '{email(f,l,i+1)}'){comma}")
    pg_lines.append("")
    pg_lines.append("INSERT INTO main_leads (lead_id, lead_phone, lead_fname, lead_lname, lead_email, lead_buyer)")
    pg_lines.append("OVERRIDING SYSTEM VALUE")
    pg_lines.append("VALUES")
    for i in range(n):
        f, l = leads[i]
        comma = "," if i < n - 1 else ";"
        lead_dom = "leadmail.com"
        pg_lines.append(f"({i+1}, '{us_phone(n+i)}', '{f}', '{l}', '{email(f,l,i+1,lead_dom)}', {i+1}){comma}")
    pg_lines.append("")

    # Cars: American makes/models
    car_data = [
        (2023, 'Ford', 'F-150', 'Pickup', 31868, 'Fair', 'Fair', 'Clean'),
        (2015, 'Chevrolet', 'Silverado', 'Pickup', 122757, 'Fair', 'Good', 'Clean'),
        (2025, 'Tesla', 'Model 3', 'Sedan', 95165, 'Excellent', 'Excellent', 'Clean'),
        (2012, 'Honda', 'Civic', 'Sedan', 64742, 'Good', 'Excellent', 'Clean'),
        (2020, 'Toyota', 'Camry', 'Sedan', 69175, 'Excellent', 'Good', 'Clean'),
        (2016, 'Ford', 'Fusion', 'Sedan', 22350, 'Excellent', 'Fair', 'Clean'),
        (2021, 'Jeep', 'Wrangler', 'SUV', 109700, 'Good', 'Excellent', 'Clean'),
        (2018, 'Chevrolet', 'Malibu', 'Sedan', 127696, 'Fair', 'Good', 'Clean'),
        (2019, 'Ford', 'Explorer', 'SUV', 5850, 'Fair', 'Fair', 'Clean'),
        (2024, 'GMC', 'Sierra', 'Pickup', 47349, 'Fair', 'Fair', 'Clean'),
    ]
    # Expand to 120 with pattern
    cars_full = []
    for i in range(n):
        c = car_data[i % len(car_data)]
        y, make, model, body, mi, ext, int_, title = c
        vin = f"1HGBH41JXMN{i+1:06d}"[:17]  # US-style VIN prefix
        cars_full.append((vin, y, make, model, body, mi, ext, int_, title))
    pg_lines.append("INSERT INTO main_cars (car_id, car_vin, car_year, car_make, car_model, car_body, car_mileage, car_external_condition, car_internal_condition, car_title_condition, car_lead)")
    pg_lines.append("OVERRIDING SYSTEM VALUE")
    pg_lines.append("VALUES")
    for i in range(n):
        vin, y, make, model, body, mi, ext, int_, title = cars_full[i]
        comma = "," if i < n - 1 else ";"
        pg_lines.append(f"({i+1}, '{vin}', {y}, '{make}', '{model}', '{body}', {mi}, '{ext}', '{int_}', '{title}', {i+1}){comma}")
    pg_lines.append("")

    pg_lines.append("INSERT INTO main_pickups (pickup_id, pickup_lead, pickup_car, pickup_contact_name, pickup_contact_phone, pickup_pincode)")
    pg_lines.append("OVERRIDING SYSTEM VALUE")
    pg_lines.append("VALUES")
    for i in range(n):
        comma = "," if i < n - 1 else ";"
        pg_lines.append(f"({i+1}, {i+1}, {i+1}, '{contacts[i]}', '{us_phone(2*n+i)}', '{zip_list[i]}'){comma}")
    pg_lines.append("")
    pg_lines.append("-- Set sequences so next inserts get IDs from 1000+")
    pg_lines.append("SELECT setval(pg_get_serial_sequence('main_buyers', 'buyer_id'), 1000);")
    pg_lines.append("SELECT setval(pg_get_serial_sequence('main_leads', 'lead_id'), 1000);")
    pg_lines.append("SELECT setval(pg_get_serial_sequence('main_cars', 'car_id'), 1000);")
    pg_lines.append("SELECT setval(pg_get_serial_sequence('main_pickups', 'pickup_id'), 1000);")

    with open("sandbox_seed_pg.sql", "w", encoding="utf-8") as f:
        f.write("\n".join(pg_lines))

    # MySQL file: same data, different header and no OVERRIDING SYSTEM VALUE / setval
    mysql_lines = [
        "-- Realistic sandbox seed data for ava_sandbox (MySQL only)",
        "-- American names, US phone numbers, US ZIP codes.",
        "-- For PostgreSQL use: sandbox_seed_pg.sql",
        "",
        "USE ava_sandbox;",
        "",
        "SET FOREIGN_KEY_CHECKS=0;",
        "TRUNCATE TABLE main_pickups;",
        "TRUNCATE TABLE main_cars;",
        "TRUNCATE TABLE main_leads;",
        "TRUNCATE TABLE main_buyers;",
        "SET FOREIGN_KEY_CHECKS=1;",
        "",
        "INSERT INTO main_buyers (buyer_id, buyer_phone, buyer_first_name, buyer_last_name, buyer_email) VALUES",
    ]
    for i in range(n):
        f, l = buyers[i]
        comma = "," if i < n - 1 else ";"
        mysql_lines.append(f"({i+1}, '{us_phone(i)}', '{f}', '{l}', '{email(f,l,i+1)}'){comma}")
    mysql_lines.append("")
    mysql_lines.append("INSERT INTO main_leads (lead_id, lead_phone, lead_fname, lead_lname, lead_email, lead_buyer) VALUES")
    for i in range(n):
        f, l = leads[i]
        comma = "," if i < n - 1 else ";"
        lead_dom = "leadmail.com"
        mysql_lines.append(f"({i+1}, '{us_phone(n+i)}', '{f}', '{l}', '{email(f,l,i+1,lead_dom)}', {i+1}){comma}")
    mysql_lines.append("")
    mysql_lines.append("INSERT INTO main_cars (car_id, car_vin, car_year, car_make, car_model, car_body, car_mileage, car_external_condition, car_internal_condition, car_title_condition, car_lead) VALUES")
    for i in range(n):
        vin, y, make, model, body, mi, ext, int_, title = cars_full[i]
        comma = "," if i < n - 1 else ";"
        mysql_lines.append(f"({i+1}, '{vin}', {y}, '{make}', '{model}', '{body}', {mi}, '{ext}', '{int_}', '{title}', {i+1}){comma}")
    mysql_lines.append("")
    mysql_lines.append("INSERT INTO main_pickups (pickup_id, pickup_lead, pickup_car, pickup_contact_name, pickup_contact_phone, pickup_pincode) VALUES")
    for i in range(n):
        comma = "," if i < n - 1 else ";"
        mysql_lines.append(f"({i+1}, {i+1}, {i+1}, '{contacts[i]}', '{us_phone(2*n+i)}', '{zip_list[i]}'){comma}")
    mysql_lines.append("")
    mysql_lines.append("ALTER TABLE main_buyers AUTO_INCREMENT = 1000;")
    mysql_lines.append("ALTER TABLE main_leads AUTO_INCREMENT = 1000;")
    mysql_lines.append("ALTER TABLE main_cars AUTO_INCREMENT = 1000;")
    mysql_lines.append("ALTER TABLE main_pickups AUTO_INCREMENT = 1000;")
    mysql_lines.append("")
    mysql_lines.append("SELECT COUNT(*) AS buyers_count FROM main_buyers;")
    mysql_lines.append("SELECT COUNT(*) AS leads_count FROM main_leads;")
    mysql_lines.append("SELECT COUNT(*) AS cars_count FROM main_cars;")
    mysql_lines.append("SELECT COUNT(*) AS pickups_count FROM main_pickups;")
    mysql_lines.append("SELECT pickup_pincode, COUNT(*) AS cnt FROM main_pickups GROUP BY pickup_pincode ORDER BY cnt DESC, pickup_pincode;")

    with open("sandbox_seed.sql", "w", encoding="utf-8") as f:
        f.write("\n".join(mysql_lines))

    print("Generated sandbox_seed_pg.sql and sandbox_seed.sql with American data.")

if __name__ == "__main__":
    main()
