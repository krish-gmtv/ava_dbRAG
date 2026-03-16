SELECT
    b.assigned_user_id,
    b.buyer_fname,
    b.buyer_lname,
    COUNT(DISTINCT l.lead_id) AS total_leads
FROM buyers b
LEFT JOIN leads l
    ON l.assigned_user_id = b.assigned_user_id
GROUP BY
    b.assigned_user_id,
    b.buyer_fname,
    b.buyer_lname
ORDER BY total_leads DESC;
