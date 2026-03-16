-- High-volume seed for Updated_schema/schema_code_v2.sql
-- Safe for reruns: truncates and repopulates.

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
SELECT
  i,
  'Buyer' || i,
  'User' || i,
  '+1-555-01' || LPAD(i::text, 2, '0'),
  'buyer' || i || '@example.com'
FROM generate_series(1, 120) AS s(i);

INSERT INTO accounts (account_name, assigned_user_id)
SELECT
  'Account ' || i,
  i
FROM generate_series(1, 120) AS s(i);

INSERT INTO leads (date_entered, assigned_user_id, status, account_id, opportunity_id)
SELECT
  TIMESTAMPTZ '2018-01-01 09:00:00+00' + (i * 10) * INTERVAL '1 day',
  i,
  CASE
    WHEN i % 5 = 0 THEN 'Open'
    WHEN i % 5 = 1 THEN 'Sold to Us'
    WHEN i % 5 = 2 THEN 'Bought at Auction'
    WHEN i % 5 = 3 THEN 'Returned as Seller'
    ELSE 'In Negotiation'
  END,
  i,
  1000 + i
FROM generate_series(1, 120) AS s(i);

INSERT INTO leads_cstm (lead_id, vin, upsheet_id, year, make, model, miles, date_bought)
SELECT
  l.lead_id,
  'VIN' || LPAD(l.lead_id::text, 8, '0'),
  NULL::INTEGER,
  2005 + (l.lead_id % 15),
  CASE (l.lead_id % 4)
    WHEN 0 THEN 'Toyota'
    WHEN 1 THEN 'Honda'
    WHEN 2 THEN 'Ford'
    ELSE 'Chevrolet'
  END,
  CASE (l.lead_id % 4)
    WHEN 0 THEN 'Camry'
    WHEN 1 THEN 'Civic'
    WHEN 2 THEN 'F-150'
    ELSE 'Malibu'
  END,
  40000 + l.lead_id * 350,
  l.date_entered - INTERVAL '10 days'
FROM leads l
ORDER BY l.lead_id;

INSERT INTO upsheets (
  lead_id,
  account_id,
  assigned_user_id,
  insert_date,
  status,
  sold_date,
  delivered_date,
  sale_price,
  opportunity_amount,
  vin,
  year,
  make,
  model,
  current_mileage
)
SELECT
  l.lead_id,
  l.account_id,
  l.assigned_user_id,
  l.date_entered + INTERVAL '1 day',
  CASE
    WHEN l.lead_id % 6 = 0 THEN 'Open'
    WHEN l.lead_id % 6 IN (1,2) THEN 'Delivered'
    ELSE 'Closed Won'
  END,
  CASE
    WHEN l.lead_id % 6 IN (3,4,5) THEN l.date_entered + INTERVAL '30 days'
    ELSE NULL
  END,
  CASE
    WHEN l.lead_id % 6 IN (1,2,3,4,5) THEN l.date_entered + INTERVAL '20 days'
    ELSE NULL
  END,
  CASE
    WHEN l.lead_id % 6 IN (3,4,5)
      THEN (5600 + l.lead_id * 85 + ((l.lead_id % 5) * 120))::NUMERIC(12,2)
    ELSE NULL
  END,
  (5000 + l.lead_id * 75 + ((l.lead_id % 3) * 50))::NUMERIC(12,2),
  c.vin,
  c.year,
  c.make,
  c.model,
  c.miles + 2000
FROM leads l
JOIN leads_cstm c
  ON c.lead_id = l.lead_id
ORDER BY l.lead_id;

UPDATE leads_cstm lc
SET upsheet_id = u.upsheet_id
FROM upsheets u
WHERE lc.lead_id = u.lead_id
  AND lc.vin = u.vin;

INSERT INTO main_pickups (
  upsheet_id,
  pickup_lead_id,
  pickup_contact_name,
  pickup_zipcode,
  pickup_email,
  pickup_phone,
  pickup_created_at
)
SELECT
  u.upsheet_id,
  u.lead_id,
  'Contact ' || u.upsheet_id,
  LPAD((10000 + u.upsheet_id)::text, 5, '0'),
  CASE WHEN u.upsheet_id % 10 = 0 THEN NULL ELSE 'contact' || u.upsheet_id || '@example.com' END,
  CASE WHEN u.upsheet_id % 12 = 0 THEN NULL ELSE '+1-555-02' || LPAD(u.upsheet_id::text, 2, '0') END,
  u.insert_date - INTERVAL '6 hours'
FROM upsheets u
ORDER BY u.upsheet_id;

INSERT INTO main_pickups (
  upsheet_id,
  pickup_lead_id,
  pickup_contact_name,
  pickup_zipcode,
  pickup_email,
  pickup_phone,
  pickup_created_at
)
SELECT
  u.upsheet_id,
  u.lead_id,
  'Followup Contact ' || u.upsheet_id,
  LPAD((20000 + u.upsheet_id)::text, 5, '0'),
  'followup' || u.upsheet_id || '@example.com',
  '+1-555-99' || LPAD(u.upsheet_id::text, 2, '0'),
  u.insert_date + INTERVAL '2 days'
FROM upsheets u
WHERE u.upsheet_id % 15 = 0
ORDER BY u.upsheet_id;

INSERT INTO historical_sales (vin, purchase_date, sold_date, sale_price)
SELECT
  u.vin,
  u.insert_date,
  u.sold_date,
  u.sale_price
FROM upsheets u
WHERE u.sold_date IS NOT NULL
ORDER BY u.upsheet_id;

INSERT INTO opportunities (upsheet_id, stage, expected_amount)
SELECT
  u.upsheet_id,
  CASE u.status
    WHEN 'Closed Won' THEN 'Won'
    WHEN 'Delivered'  THEN 'In Fulfillment'
    WHEN 'Open'       THEN 'Open'
    ELSE u.status
  END,
  u.opportunity_amount
FROM upsheets u
ORDER BY u.upsheet_id;

-- additional offers on a subset of upsheets to exercise 1:n
INSERT INTO opportunities (upsheet_id, stage, expected_amount)
SELECT
  u.upsheet_id,
  'Revised Offer',
  (u.opportunity_amount * 0.9)::NUMERIC(12,2)
FROM upsheets u
WHERE u.upsheet_id % 20 = 0
ORDER BY u.upsheet_id;

INSERT INTO vin_history (
  vin,
  source_table,
  source_record_id,
  event_type,
  event_date,
  lead_id,
  upsheet_id,
  account_id,
  assigned_user_id,
  created_at
)
SELECT
  c.vin,
  'leads_cstm',
  c.lead_cstm_id,
  'offer_from_seller',
  c.date_bought,
  c.lead_id,
  NULL::INTEGER,
  l.account_id,
  l.assigned_user_id,
  c.date_bought + INTERVAL '4 hours'
FROM leads_cstm c
JOIN leads l
  ON l.lead_id = c.lead_id
ORDER BY c.lead_cstm_id;

INSERT INTO vin_history (
  vin,
  source_table,
  source_record_id,
  event_type,
  event_date,
  lead_id,
  upsheet_id,
  account_id,
  assigned_user_id,
  created_at
)
SELECT
  u.vin,
  'upsheets',
  u.upsheet_id,
  CASE
    WHEN u.delivered_date IS NULL AND u.sold_date IS NULL THEN 'offer_logged'
    ELSE 'purchase_from_seller'
  END,
  u.insert_date,
  u.lead_id,
  u.upsheet_id,
  u.account_id,
  u.assigned_user_id,
  u.insert_date + INTERVAL '2 hours'
FROM upsheets u
ORDER BY u.upsheet_id;

INSERT INTO vin_history (
  vin,
  source_table,
  source_record_id,
  event_type,
  event_date,
  lead_id,
  upsheet_id,
  account_id,
  assigned_user_id,
  created_at
)
SELECT
  u.vin,
  'main_pickups',
  MIN(p.pickup_id),
  'delivered',
  u.delivered_date,
  u.lead_id,
  u.upsheet_id,
  u.account_id,
  u.assigned_user_id,
  u.delivered_date + INTERVAL '6 hours'
FROM upsheets u
JOIN main_pickups p
  ON p.upsheet_id = u.upsheet_id
WHERE u.delivered_date IS NOT NULL
GROUP BY
  u.vin, u.delivered_date, u.lead_id, u.upsheet_id, u.account_id, u.assigned_user_id
ORDER BY u.upsheet_id;

INSERT INTO vin_history (
  vin,
  source_table,
  source_record_id,
  event_type,
  event_date,
  lead_id,
  upsheet_id,
  account_id,
  assigned_user_id,
  created_at
)
SELECT
  hs.vin,
  'historical_sales',
  hs.historical_sale_id,
  'sold_to_buyer',
  hs.sold_date,
  u.lead_id,
  u.upsheet_id,
  u.account_id,
  u.assigned_user_id,
  hs.sold_date + INTERVAL '8 hours'
FROM historical_sales hs
JOIN upsheets u
  ON u.vin = hs.vin
ORDER BY hs.historical_sale_id;

INSERT INTO vin_history (
  vin,
  source_table,
  source_record_id,
  event_type,
  event_date,
  lead_id,
  upsheet_id,
  account_id,
  assigned_user_id,
  created_at
)
SELECT
  c.vin,
  'leads_cstm',
  c.lead_cstm_id,
  'returned_as_seller',
  c.date_bought + INTERVAL '3 years',
  c.lead_id,
  NULL::INTEGER,
  l.account_id,
  l.assigned_user_id,
  c.date_bought + INTERVAL '3 years 4 hours'
FROM leads_cstm c
JOIN leads l
  ON l.lead_id = c.lead_id
WHERE c.lead_id BETWEEN 1 AND 5
ORDER BY c.lead_cstm_id;

