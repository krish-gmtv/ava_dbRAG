SELECT
    b.assigned_user_id,
    b.buyer_fname,
    b.buyer_lname,
    SUM(o.expected_amount) AS total_expected_amount
FROM buyers b
LEFT JOIN upsheets u
    ON u.assigned_user_id = b.assigned_user_id
LEFT JOIN opportunities o
    ON o.upsheet_id = u.upsheet_id
GROUP BY
    b.assigned_user_id,
    b.buyer_fname,
    b.buyer_lname
ORDER BY total_expected_amount DESC NULLS LAST;