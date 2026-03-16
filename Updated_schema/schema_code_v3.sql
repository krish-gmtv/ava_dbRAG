DROP TABLE IF EXISTS vin_history CASCADE;
DROP TABLE IF EXISTS historical_sales CASCADE;
DROP TABLE IF EXISTS opportunities CASCADE;
DROP TABLE IF EXISTS main_pickups CASCADE;
DROP TABLE IF EXISTS upsheets CASCADE;
DROP TABLE IF EXISTS leads_cstm CASCADE;
DROP TABLE IF EXISTS leads CASCADE;
DROP TABLE IF EXISTS accounts CASCADE;
DROP TABLE IF EXISTS buyers CASCADE;

CREATE TABLE buyers (
    assigned_user_id INTEGER PRIMARY KEY,
    buyer_fname VARCHAR(100) NOT NULL,
    buyer_lname VARCHAR(100) NOT NULL,
    buyer_contact VARCHAR(50),
    buyer_email VARCHAR(255),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE accounts (
    account_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    account_name VARCHAR(255) NOT NULL,
    assigned_user_id INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_accounts_buyer
        FOREIGN KEY (assigned_user_id)
        REFERENCES buyers(assigned_user_id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT
);

CREATE TABLE leads (
    lead_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    date_entered TIMESTAMPTZ NOT NULL,
    assigned_user_id INTEGER NOT NULL,
    status VARCHAR(100),
    account_id INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_leads_buyer
        FOREIGN KEY (assigned_user_id)
        REFERENCES buyers(assigned_user_id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,
    CONSTRAINT fk_leads_account
        FOREIGN KEY (account_id)
        REFERENCES accounts(account_id)
        ON UPDATE CASCADE
        ON DELETE SET NULL
);

CREATE TABLE leads_cstm (
    lead_cstm_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    lead_id INTEGER NOT NULL,
    vin VARCHAR(32),
    upsheet_id INTEGER,
    year INTEGER,
    make VARCHAR(100),
    model VARCHAR(100),
    miles INTEGER,
    date_bought TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_leads_cstm_lead
        FOREIGN KEY (lead_id)
        REFERENCES leads(lead_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE
);

CREATE TABLE upsheets (
    upsheet_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    lead_id INTEGER NOT NULL,
    account_id INTEGER,
    assigned_user_id INTEGER NOT NULL,
    insert_date TIMESTAMPTZ NOT NULL,
    status VARCHAR(100),
    sold_date TIMESTAMPTZ,
    delivered_date TIMESTAMPTZ,
    sale_price NUMERIC(12,2),
    opportunity_amount NUMERIC(12,2),
    vin VARCHAR(32),
    year INTEGER,
    make VARCHAR(100),
    model VARCHAR(100),
    current_mileage INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_upsheets_lead
        FOREIGN KEY (lead_id)
        REFERENCES leads(lead_id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,
    CONSTRAINT fk_upsheets_account
        FOREIGN KEY (account_id)
        REFERENCES accounts(account_id)
        ON UPDATE CASCADE
        ON DELETE SET NULL,
    CONSTRAINT fk_upsheets_buyer
        FOREIGN KEY (assigned_user_id)
        REFERENCES buyers(assigned_user_id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT
);

CREATE TABLE opportunities (
    opportunity_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    upsheet_id INTEGER NOT NULL,
    stage VARCHAR(100),
    expected_amount NUMERIC(12,2),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_opportunities_upsheet
        FOREIGN KEY (upsheet_id)
        REFERENCES upsheets(upsheet_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE
);

CREATE TABLE main_pickups (
    pickup_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    upsheet_id INTEGER NOT NULL,
    pickup_lead_id INTEGER,
    pickup_contact_name VARCHAR(150),
    pickup_zipcode VARCHAR(16),
    pickup_email VARCHAR(255),
    pickup_phone VARCHAR(50),
    pickup_created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_pickups_upsheet
        FOREIGN KEY (upsheet_id)
        REFERENCES upsheets(upsheet_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    CONSTRAINT fk_pickups_lead
        FOREIGN KEY (pickup_lead_id)
        REFERENCES leads(lead_id)
        ON UPDATE CASCADE
        ON DELETE SET NULL
);

CREATE TABLE historical_sales (
    historical_sale_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    vin VARCHAR(32) NOT NULL,
    purchase_date TIMESTAMPTZ,
    sold_date TIMESTAMPTZ,
    sale_price NUMERIC(12,2),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE vin_history (
    vin_history_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    vin VARCHAR(32) NOT NULL,
    source_table VARCHAR(100) NOT NULL,
    source_record_id INTEGER NOT NULL,
    event_type VARCHAR(100),
    event_date TIMESTAMPTZ,
    lead_id INTEGER,
    upsheet_id INTEGER,
    account_id INTEGER,
    assigned_user_id INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_vin_history_lead
        FOREIGN KEY (lead_id)
        REFERENCES leads(lead_id)
        ON UPDATE CASCADE
        ON DELETE SET NULL,
    CONSTRAINT fk_vin_history_upsheet
        FOREIGN KEY (upsheet_id)
        REFERENCES upsheets(upsheet_id)
        ON UPDATE CASCADE
        ON DELETE SET NULL,
    CONSTRAINT fk_vin_history_account
        FOREIGN KEY (account_id)
        REFERENCES accounts(account_id)
        ON UPDATE CASCADE
        ON DELETE SET NULL,
    CONSTRAINT fk_vin_history_buyer
        FOREIGN KEY (assigned_user_id)
        REFERENCES buyers(assigned_user_id)
        ON UPDATE CASCADE
        ON DELETE SET NULL
);

CREATE INDEX idx_accounts_assigned_user_id ON accounts(assigned_user_id);

CREATE INDEX idx_leads_assigned_user_id ON leads(assigned_user_id);
CREATE INDEX idx_leads_account_id ON leads(account_id);
CREATE INDEX idx_leads_date_entered ON leads(date_entered);

CREATE INDEX idx_leads_cstm_lead_id ON leads_cstm(lead_id);
CREATE INDEX idx_leads_cstm_vin ON leads_cstm(vin);
CREATE INDEX idx_leads_cstm_upsheet_id ON leads_cstm(upsheet_id);

CREATE INDEX idx_upsheets_lead_id ON upsheets(lead_id);
CREATE INDEX idx_upsheets_account_id ON upsheets(account_id);
CREATE INDEX idx_upsheets_assigned_user_id ON upsheets(assigned_user_id);
CREATE INDEX idx_upsheets_insert_date ON upsheets(insert_date);
CREATE INDEX idx_upsheets_delivered_date ON upsheets(delivered_date);
CREATE INDEX idx_upsheets_sold_date ON upsheets(sold_date);
CREATE INDEX idx_upsheets_vin ON upsheets(vin);

CREATE INDEX idx_main_pickups_upsheet_id ON main_pickups(upsheet_id);
CREATE INDEX idx_main_pickups_lead_id ON main_pickups(pickup_lead_id);
CREATE INDEX idx_main_pickups_zipcode ON main_pickups(pickup_zipcode);

CREATE INDEX idx_opportunities_upsheet_id ON opportunities(upsheet_id);

CREATE INDEX idx_historical_sales_vin ON historical_sales(vin);
CREATE INDEX idx_historical_sales_purchase_date ON historical_sales(purchase_date);
CREATE INDEX idx_historical_sales_sold_date ON historical_sales(sold_date);

CREATE INDEX idx_vin_history_vin ON vin_history(vin);
CREATE INDEX idx_vin_history_event_date ON vin_history(event_date);
CREATE INDEX idx_vin_history_source_table ON vin_history(source_table);

ALTER TABLE upsheets
ALTER COLUMN vin SET NOT NULL;

ALTER TABLE upsheets
ADD CONSTRAINT uq_upsheets_vin UNIQUE (vin);

ALTER TABLE historical_sales
ALTER COLUMN vin SET NOT NULL;

ALTER TABLE historical_sales
ADD CONSTRAINT uq_historical_sales_vin UNIQUE (vin);

ALTER TABLE historical_sales
ADD CONSTRAINT fk_historical_sales_vin
FOREIGN KEY (vin)
REFERENCES upsheets(vin)
ON UPDATE CASCADE
ON DELETE RESTRICT;

