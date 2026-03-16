/* =====================================================
   SANDBOX DATA AUDIT
   Checks data quality for ava_sandbox database
   ===================================================== */


/* =====================================================
   1. ROW COUNTS
   ===================================================== */

SELECT 'main_buyers' AS table_name, COUNT(*) AS row_count FROM main_buyers
UNION ALL
SELECT 'main_leads', COUNT(*) FROM main_leads
UNION ALL
SELECT 'main_cars', COUNT(*) FROM main_cars
UNION ALL
SELECT 'main_pickups', COUNT(*) FROM main_pickups;


/* =====================================================
   2. NULL VALUE CHECKS
   ===================================================== */

-- BUYERS
SELECT
  'buyers_nulls' AS audit,
  COUNT(*) FILTER (WHERE buyer_phone IS NULL) AS buyer_phone_nulls,
  COUNT(*) FILTER (WHERE buyer_first_name IS NULL) AS buyer_fname_nulls,
  COUNT(*) FILTER (WHERE buyer_last_name IS NULL) AS buyer_lname_nulls,
  COUNT(*) FILTER (WHERE buyer_email IS NULL) AS buyer_email_nulls,
  COUNT(*) FILTER (WHERE created_at IS NULL) AS created_nulls
FROM main_buyers;

-- LEADS
SELECT
  'leads_nulls' AS audit,
  COUNT(*) FILTER (WHERE lead_phone IS NULL) AS lead_phone_nulls,
  COUNT(*) FILTER (WHERE lead_fname IS NULL) AS lead_fname_nulls,
  COUNT(*) FILTER (WHERE lead_lname IS NULL) AS lead_lname_nulls,
  COUNT(*) FILTER (WHERE lead_email IS NULL) AS lead_email_nulls,
  COUNT(*) FILTER (WHERE lead_buyer IS NULL) AS lead_buyer_nulls,
  COUNT(*) FILTER (WHERE created_at IS NULL) AS created_nulls
FROM main_leads;

-- CARS
SELECT
  'cars_nulls' AS audit,
  COUNT(*) FILTER (WHERE car_vin IS NULL) AS car_vin_nulls,
  COUNT(*) FILTER (WHERE car_year IS NULL) AS car_year_nulls,
  COUNT(*) FILTER (WHERE car_make IS NULL) AS car_make_nulls,
  COUNT(*) FILTER (WHERE car_model IS NULL) AS car_model_nulls,
  COUNT(*) FILTER (WHERE car_body IS NULL) AS car_body_nulls,
  COUNT(*) FILTER (WHERE car_mileage IS NULL) AS car_mileage_nulls,
  COUNT(*) FILTER (WHERE car_external_condition IS NULL) AS ext_condition_nulls,
  COUNT(*) FILTER (WHERE car_internal_condition IS NULL) AS int_condition_nulls,
  COUNT(*) FILTER (WHERE car_title_condition IS NULL) AS title_condition_nulls,
  COUNT(*) FILTER (WHERE car_lead IS NULL) AS car_lead_nulls,
  COUNT(*) FILTER (WHERE created_at IS NULL) AS created_nulls
FROM main_cars;

-- PICKUPS
SELECT
  'pickups_nulls' AS audit,
  COUNT(*) FILTER (WHERE pickup_lead IS NULL) AS pickup_lead_nulls,
  COUNT(*) FILTER (WHERE pickup_car IS NULL) AS pickup_car_nulls,
  COUNT(*) FILTER (WHERE pickup_contact_name IS NULL) AS contact_name_nulls,
  COUNT(*) FILTER (WHERE pickup_contact_phone IS NULL) AS contact_phone_nulls,
  COUNT(*) FILTER (WHERE pickup_pincode IS NULL) AS pincode_nulls,
  COUNT(*) FILTER (WHERE created_at IS NULL) AS created_nulls
FROM main_pickups;


/* =====================================================
   3. EMPTY STRING CHECKS
   ===================================================== */

SELECT
  COUNT(*) FILTER (WHERE TRIM(COALESCE(buyer_phone,''))='') AS buyer_phone_blank,
  COUNT(*) FILTER (WHERE TRIM(COALESCE(buyer_first_name,''))='') AS buyer_fname_blank,
  COUNT(*) FILTER (WHERE TRIM(COALESCE(buyer_last_name,''))='') AS buyer_lname_blank,
  COUNT(*) FILTER (WHERE TRIM(COALESCE(buyer_email,''))='') AS buyer_email_blank
FROM main_buyers;

SELECT
  COUNT(*) FILTER (WHERE TRIM(COALESCE(lead_phone,''))='') AS lead_phone_blank,
  COUNT(*) FILTER (WHERE TRIM(COALESCE(lead_fname,''))='') AS lead_fname_blank,
  COUNT(*) FILTER (WHERE TRIM(COALESCE(lead_lname,''))='') AS lead_lname_blank,
  COUNT(*) FILTER (WHERE TRIM(COALESCE(lead_email,''))='') AS lead_email_blank
FROM main_leads;

SELECT
  COUNT(*) FILTER (WHERE TRIM(COALESCE(car_vin,''))='') AS car_vin_blank,
  COUNT(*) FILTER (WHERE TRIM(COALESCE(car_make,''))='') AS car_make_blank,
  COUNT(*) FILTER (WHERE TRIM(COALESCE(car_model,''))='') AS car_model_blank,
  COUNT(*) FILTER (WHERE TRIM(COALESCE(car_body,''))='') AS car_body_blank
FROM main_cars;

SELECT
  COUNT(*) FILTER (WHERE TRIM(COALESCE(pickup_contact_name,''))='') AS pickup_name_blank,
  COUNT(*) FILTER (WHERE TRIM(COALESCE(pickup_contact_phone,''))='') AS pickup_phone_blank,
  COUNT(*) FILTER (WHERE TRIM(COALESCE(pickup_pincode,''))='') AS pickup_zip_blank
FROM main_pickups;


/* =====================================================
   4. FOREIGN KEY / ORPHAN CHECKS
   ===================================================== */

-- leads without buyers
SELECT COUNT(*) AS orphan_leads
FROM main_leads l
LEFT JOIN main_buyers b
ON l.lead_buyer = b.buyer_id
WHERE b.buyer_id IS NULL;

-- cars without leads
SELECT COUNT(*) AS orphan_cars
FROM main_cars c
LEFT JOIN main_leads l
ON c.car_lead = l.lead_id
WHERE l.lead_id IS NULL;

-- pickups without leads
SELECT COUNT(*) AS orphan_pickups_lead
FROM main_pickups p
LEFT JOIN main_leads l
ON p.pickup_lead = l.lead_id
WHERE l.lead_id IS NULL;

-- pickups without cars
SELECT COUNT(*) AS orphan_pickups_car
FROM main_pickups p
LEFT JOIN main_cars c
ON p.pickup_car = c.car_id
WHERE c.car_id IS NULL;


/* =====================================================
   5. DUPLICATE CHECKS
   ===================================================== */

-- duplicate buyer emails
SELECT buyer_email, COUNT(*)
FROM main_buyers
GROUP BY buyer_email
HAVING COUNT(*) > 1;

-- duplicate lead emails
SELECT lead_email, COUNT(*)
FROM main_leads
GROUP BY lead_email
HAVING COUNT(*) > 1;

-- duplicate VIN
SELECT car_vin, COUNT(*)
FROM main_cars
GROUP BY car_vin
HAVING COUNT(*) > 1;


/* =====================================================
   6. FORMAT VALIDATION
   ===================================================== */

-- invalid ZIP codes
SELECT pickup_id, pickup_pincode
FROM main_pickups
WHERE pickup_pincode !~ '^[0-9]{5}$';

-- invalid buyer emails
SELECT buyer_id, buyer_email
FROM main_buyers
WHERE buyer_email !~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$';

-- invalid lead emails
SELECT lead_id, lead_email
FROM main_leads
WHERE lead_email !~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$';

-- invalid phone numbers
SELECT 'buyers' AS source, buyer_id AS id, buyer_phone AS phone
FROM main_buyers
WHERE buyer_phone !~ '^\+1-[0-9]{3}-[0-9]{3}-[0-9]{4}$'

UNION ALL

SELECT 'leads', lead_id, lead_phone
FROM main_leads
WHERE lead_phone !~ '^\+1-[0-9]{3}-[0-9]{3}-[0-9]{4}$'

UNION ALL

SELECT 'pickups', pickup_id, pickup_contact_phone
FROM main_pickups
WHERE pickup_contact_phone !~ '^\+1-[0-9]{3}-[0-9]{3}-[0-9]{4}$';


/* =====================================================
   7. TIMESTAMP CHECKS
   ===================================================== */

SELECT
  (SELECT COUNT(*) FROM main_buyers WHERE created_at IS NULL) AS buyers_created_null,
  (SELECT COUNT(*) FROM main_leads WHERE created_at IS NULL) AS leads_created_null,
  (SELECT COUNT(*) FROM main_cars WHERE created_at IS NULL) AS cars_created_null,
  (SELECT COUNT(*) FROM main_pickups WHERE created_at IS NULL) AS pickups_created_null;


-- leads created before buyer
SELECT COUNT(*) AS leads_before_buyer
FROM main_leads l
JOIN main_buyers b
ON l.lead_buyer = b.buyer_id
WHERE l.created_at < b.created_at;

-- cars created before lead
SELECT COUNT(*) AS cars_before_lead
FROM main_cars c
JOIN main_leads l
ON c.car_lead = l.lead_id
WHERE c.created_at < l.created_at;

-- pickups created before car
SELECT COUNT(*) AS pickups_before_car
FROM main_pickups p
JOIN main_cars c
ON p.pickup_car = c.car_id
WHERE p.created_at < c.created_at;