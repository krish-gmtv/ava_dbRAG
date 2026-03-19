-- Postgres sandbox schema derived from Ava Main DB API v1.0.0.1
-- Changes from main schema:
-- 1) main_tasks table intentionally omitted
-- 2) main_pickups reduced to minimal pickup contact/location fields
-- 3) created_at added on all tables for precise time-based querying
-- Run all DDL inside sandbox DB.
\connect ava_sandbox

CREATE TABLE IF NOT EXISTS main_buyers (
  buyer_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  buyer_phone VARCHAR(32),
  buyer_first_name VARCHAR(100),
  buyer_last_name VARCHAR(100),
  buyer_email VARCHAR(255),
  created_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC')
);

CREATE TABLE IF NOT EXISTS main_leads (
  lead_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  lead_phone VARCHAR(32),
  lead_fname VARCHAR(100),
  lead_lname VARCHAR(100),
  lead_email VARCHAR(255),
  lead_buyer INTEGER NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC'),
  CONSTRAINT fk_main_leads_buyer
    FOREIGN KEY (lead_buyer)
    REFERENCES main_buyers (buyer_id)
    ON UPDATE CASCADE
    ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS main_cars (
  car_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  car_vin VARCHAR(32),
  car_year INTEGER,
  car_make VARCHAR(80),
  car_model VARCHAR(80),
  car_body VARCHAR(80),
  car_mileage INTEGER,
  car_external_condition VARCHAR(120),
  car_internal_condition VARCHAR(120),
  car_title_condition VARCHAR(120),
  car_lead INTEGER NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC'),
  CONSTRAINT fk_main_cars_lead
    FOREIGN KEY (car_lead)
    REFERENCES main_leads (lead_id)
    ON UPDATE CASCADE
    ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS main_pickups (
  pickup_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  pickup_lead INTEGER NOT NULL,
  pickup_car INTEGER NOT NULL,
  pickup_contact_name VARCHAR(120),
  pickup_contact_phone VARCHAR(32),
  pickup_pincode VARCHAR(16),
  created_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC'),
  CONSTRAINT fk_main_pickups_lead
    FOREIGN KEY (pickup_lead)
    REFERENCES main_leads (lead_id)
    ON UPDATE CASCADE
    ON DELETE RESTRICT,
  CONSTRAINT fk_main_pickups_car
    FOREIGN KEY (pickup_car)
    REFERENCES main_cars (car_id)
    ON UPDATE CASCADE
    ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_main_leads_buyer ON main_leads (lead_buyer);
CREATE INDEX IF NOT EXISTS idx_main_leads_created_at ON main_leads (created_at);
CREATE INDEX IF NOT EXISTS idx_main_cars_lead ON main_cars (car_lead);
CREATE INDEX IF NOT EXISTS idx_main_cars_created_at ON main_cars (created_at);
CREATE INDEX IF NOT EXISTS idx_main_pickups_lead ON main_pickups (pickup_lead);
CREATE INDEX IF NOT EXISTS idx_main_pickups_car ON main_pickups (pickup_car);
CREATE INDEX IF NOT EXISTS idx_main_pickups_pincode ON main_pickups (pickup_pincode);
CREATE INDEX IF NOT EXISTS idx_main_pickups_created_at ON main_pickups (created_at);
