SELECT
    b.assigned_user_id,
    b.buyer_fname,
    b.buyer_lname,
    COALESCE(SUM(u.sale_price),0) AS total_sale_value
FROM buyers b
LEFT JOIN upsheets u
    ON u.assigned_user_id = b.assigned_user_id
   AND u.sold_date IS NOT NULL
GROUP BY
    b.assigned_user_id,
    b.buyer_fname,
    b.buyer_lname
ORDER BY total_sale_value DESC;