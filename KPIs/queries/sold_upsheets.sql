SELECT
    b.assigned_user_id,
    b.buyer_fname,
    b.buyer_lname,
    COUNT(u.upsheet_id) AS sold_upsheets
FROM buyers b
LEFT JOIN upsheets u
    ON u.assigned_user_id = b.assigned_user_id
   AND u.sold_date IS NOT NULL
GROUP BY
    b.assigned_user_id,
    b.buyer_fname,
    b.buyer_lname
ORDER BY sold_upsheets DESC;