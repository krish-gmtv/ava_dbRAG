-- Small seed for Updated_schema/schema_code_v3.sql (no leads.opportunity_id)
-- Safe for reruns: truncates and repopulates all related tables.

TRUNCATE TABLE vin_history,
               historical_sales,
               main_pickups,
               opportunities,
               upsheets,
               leads_cstm,
               leads,
               accounts,
               buyers
RESTART IDENTITY CASCADE;

INSERT INTO buyers (assigned_user_id, buyer_fname, buyer_lname, buyer_contact, buyer_email)
VALUES
(1, 'Alice', 'Anderson', '+1-212-555-0101', 'alice.anderson@example.com'),
(2, 'Bob',   'Baker',    '+1-310-555-0102', 'bob.baker@example.com'),
(3, 'Carol', 'Clark',    '+1-415-555-0103', 'carol.clark@example.com');

INSERT INTO accounts (account_id, account_name, assigned_user_id)
OVERRIDING SYSTEM VALUE
VALUES
(1, 'Anderson Auto Sales', 1),
(2, 'Baker Family Motors', 2),
(3, 'Clark Wholesale',     3);

INSERT INTO leads (lead_id, date_entered, assigned_user_id, status, account_id)
OVERRIDING SYSTEM VALUE
VALUES
(1, '2018-03-01 10:00:00+00', 1, 'Sold to Us',         1),
(2, '2018-04-05 11:00:00+00', 2, 'Bought at Auction',  2),
(3, '2022-05-20 09:30:00+00', 2, 'Returned as Seller', 2),
(4, '2019-06-10 14:15:00+00', 3, 'Sold to Us',         3),
(5, '2020-02-15 13:00:00+00', 1, 'Open',               1),
(6, '2021-11-01 16:45:00+00', 2, 'Sold to Us',         2);

INSERT INTO leads_cstm (lead_cstm_id, lead_id, vin, upsheet_id, year, make, model, miles, date_bought)
OVERRIDING SYSTEM VALUE
VALUES
(1, 1, 'VINEDGE00123', NULL, 2015, 'Toyota', 'Camry',  85000, '2018-02-20 00:00:00+00'),
(2, 2, 'VINEDGE00123', NULL, 2015, 'Toyota', 'Camry',  86000, '2018-04-05 00:00:00+00'),
(3, 3, 'VINEDGE00123', NULL, 2015, 'Toyota', 'Camry', 110000, '2022-05-15 00:00:00+00'),
(4, 4, 'VINNORMAL0001', NULL, 2018, 'Honda',  'Civic',  40000, '2019-06-01 00:00:00+00'),
(5, 5, 'VINOPEN000001', NULL, 2017, 'Ford',   'Fusion', 67000, '2020-02-10 00:00:00+00'),
(6, 6, 'VINDELIV00001', NULL, 2019, 'Nissan', 'Altima', 52000, '2021-10-20 00:00:00+00');

INSERT INTO upsheets (
    upsheet_id, lead_id, account_id, assigned_user_id, insert_date, status,
    sold_date, delivered_date, sale_price, opportunity_amount,
    vin, year, make, model, current_mileage
)
OVERRIDING SYSTEM VALUE
VALUES
(1, 1, 1, 1,
 '2018-03-02 09:00:00+00',
 'Closed Won',
 '2018-04-01 00:00:00+00', '2018-04-03 00:00:00+00',
 8500.00, 7800.00,
 'VINEDGE00123', 2015, 'Toyota', 'Camry', 85500),
(2, 4, 3, 3,
 '2019-06-11 10:30:00+00',
 'Closed Won',
 '2019-07-01 00:00:00+00', '2019-07-05 00:00:00+00',
 10250.00, 9400.00,
 'VINNORMAL0001', 2018, 'Honda', 'Civic', 40500),
(3, 5, 1, 1,
 '2020-02-16 09:15:00+00',
 'Open',
 NULL, NULL,
 NULL, 6200.00,
 'VINOPEN000001', 2017, 'Ford', 'Fusion', 67500),
(4, 6, 2, 2,
 '2021-11-02 08:45:00+00',
 'Delivered',
 NULL, '2021-11-08 00:00:00+00',
 NULL, 8900.00,
 'VINDELIV00001', 2019, 'Nissan', 'Altima', 52500);

UPDATE leads_cstm lc
SET upsheet_id = u.upsheet_id
FROM upsheets u
WHERE lc.lead_id = u.lead_id
  AND lc.vin = u.vin;

INSERT INTO main_pickups (
    upsheet_id, pickup_lead_id, pickup_contact_name,
    pickup_zipcode, pickup_email, pickup_phone, pickup_created_at
)
VALUES
(1, 1, 'Alice Anderson', '10001', 'alice.anderson@example.com', '+1-212-555-0101', '2018-03-02 08:30:00+00'),
(1, 2, 'Bob Baker',      '94102', 'bob.baker@example.com',      '+1-310-555-0102', '2018-04-02 08:30:00+00'),
(2, 4, 'Carol Clark',    '90210', 'carol.clark@example.com',    '+1-415-555-0103', '2019-06-11 09:00:00+00'),
(4, 6, 'Bob Baker',      '60601', 'bob.baker@example.com',      '+1-310-555-0102', '2021-11-03 10:15:00+00');

INSERT INTO historical_sales (
    historical_sale_id, vin, purchase_date, sold_date, sale_price
)
OVERRIDING SYSTEM VALUE
VALUES
(1, 'VINEDGE00123',  '2018-03-02 09:00:00+00', '2018-04-01 00:00:00+00',  8500.00),
(2, 'VINNORMAL0001', '2019-06-11 10:30:00+00', '2019-07-01 00:00:00+00', 10250.00);

INSERT INTO opportunities (opportunity_id, upsheet_id, stage, expected_amount)
OVERRIDING SYSTEM VALUE
VALUES
(1, 1, 'Closed Won', 7800.00),
(2, 2, 'Closed Won', 9400.00),
(3, 3, 'Open',       6200.00),
(4, 4, 'Delivered',  8900.00),
(5, 1, 'Counter Offer', 7600.00);

INSERT INTO vin_history (
    vin_history_id, vin, source_table, source_record_id,
    event_type, event_date, lead_id, upsheet_id,
    account_id, assigned_user_id, created_at
)
OVERRIDING SYSTEM VALUE
VALUES
(1, 'VINEDGE00123', 'leads_cstm',       1, 'offer_from_seller',   '2018-02-20 00:00:00+00', 1, NULL, 1, 1, '2018-02-20 12:00:00+00'),
(2, 'VINEDGE00123', 'upsheets',         1, 'purchase_from_seller','2018-03-02 09:00:00+00', 1, 1,    1, 1, '2018-03-02 10:00:00+00'),
(3, 'VINEDGE00123', 'historical_sales', 1, 'sold_to_buyer',       '2018-04-01 00:00:00+00', 2, 1,    1, 1, '2018-04-01 12:00:00+00'),
(4, 'VINEDGE00123', 'leads_cstm',       3, 'returned_as_seller',  '2022-05-20 09:30:00+00', 3, NULL, 2, 2, '2022-05-20 10:00:00+00'),
(5, 'VINNORMAL0001', 'leads_cstm',       4, 'offer_from_seller',   '2019-06-01 00:00:00+00', 4, NULL, 3, 3, '2019-06-01 12:00:00+00'),
(6, 'VINNORMAL0001', 'upsheets',         2, 'purchase_from_seller','2019-06-11 10:30:00+00', 4, 2,    3, 3, '2019-06-11 11:00:00+00'),
(7, 'VINNORMAL0001', 'historical_sales', 2, 'sold_to_buyer',       '2019-07-01 00:00:00+00', 4, 2,    3, 3, '2019-07-01 12:00:00+00'),
(8,  'VINOPEN000001',  'leads_cstm', 5, 'offer_from_seller',    '2020-02-10 00:00:00+00', 5, NULL, 1, 1, '2020-02-10 12:00:00+00'),
(9,  'VINOPEN000001',  'upsheets',   3, 'offer_logged',         '2020-02-16 09:15:00+00', 5, 3,    1, 1, '2020-02-16 10:00:00+00'),
(10, 'VINDELIV00001',  'leads_cstm', 6, 'offer_from_seller',    '2021-10-20 00:00:00+00', 6, NULL, 2, 2, '2021-10-20 12:00:00+00'),
(11, 'VINDELIV00001',  'upsheets',   4, 'purchase_from_seller', '2021-11-02 08:45:00+00', 6, 4,    2, 2, '2021-11-02 10:00:00+00'),
(12, 'VINDELIV00001',  'main_pickups',4, 'delivered',           '2021-11-08 00:00:00+00', 6, 4,    2, 2, '2021-11-08 08:00:00+00');

