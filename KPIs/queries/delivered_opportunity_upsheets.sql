WITH delivered_opportunity_upsheet_cte AS (
    SELECT DISTINCT
        u.upsheet_id,
        u.assigned_user_id
    FROM upsheets u
    JOIN opportunities o
        ON o.upsheet_id = u.upsheet_id
    WHERE u.delivered_date IS NOT NULL
)
SELECT
    b.assigned_user_id,
    b.buyer_fname,
    b.buyer_lname,
    COUNT(dou.upsheet_id) AS delivered_opportunity_upsheets
FROM buyers b
LEFT JOIN delivered_opportunity_upsheet_cte dou
    ON dou.assigned_user_id = b.assigned_user_id
GROUP BY
    b.assigned_user_id,
    b.buyer_fname,
    b.buyer_lname
ORDER BY delivered_opportunity_upsheets DESC;