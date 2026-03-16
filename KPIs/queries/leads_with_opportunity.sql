WITH lead_opps AS (
    SELECT DISTINCT
        l.lead_id,
        l.assigned_user_id
    FROM leads l
    JOIN upsheets u
        ON u.lead_id = l.lead_id
    JOIN opportunities o
        ON o.upsheet_id = u.upsheet_id
)
SELECT
    b.assigned_user_id,
    b.buyer_fname,
    b.buyer_lname,
    COUNT(DISTINCT lo.lead_id) AS leads_with_opportunity
FROM buyers b
LEFT JOIN lead_opps lo
    ON lo.assigned_user_id = b.assigned_user_id
GROUP BY
    b.assigned_user_id,
    b.buyer_fname,
    b.buyer_lname
ORDER BY leads_with_opportunity DESC;