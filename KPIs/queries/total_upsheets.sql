SELECT
    b.assigned_user_id,
    b.buyer_fname,
    b.buyer_lname,
    COUNT(DISTINCT u.upsheet_id) AS total_upsheets
FROM buyers b
LEFT JOIN upsheets u
    ON u.assigned_user_id = b.assigned_user_id
GROUP BY
    b.assigned_user_id,
    b.buyer_fname,
    b.buyer_lname
ORDER BY total_upsheets DESC;