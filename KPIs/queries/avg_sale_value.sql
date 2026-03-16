SELECT
    b.assigned_user_id,
    b.buyer_fname,
    b.buyer_lname,
    COALESCE(ROUND(AVG(u.sale_price), 2), 0) AS avg_sale_value
FROM buyers b
LEFT JOIN upsheets u
    ON u.assigned_user_id = b.assigned_user_id
   AND u.sold_date IS NOT NULL
GROUP BY
    b.assigned_user_id,
    b.buyer_fname,
    b.buyer_lname
ORDER BY avg_sale_value DESC;