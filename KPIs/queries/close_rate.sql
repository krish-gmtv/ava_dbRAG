SELECT
    b.assigned_user_id,
    b.buyer_fname,
    b.buyer_lname,
    ROUND(COUNT(DISTINCT CASE
        WHEN o.upsheet_id IS NOT NULL
         AND u.delivered_date IS NOT NULL
        THEN u.upsheet_id
    END)::numeric
    / NULLIF(
        COUNT(DISTINCT CASE
            WHEN o.upsheet_id IS NOT NULL
            THEN u.upsheet_id
        END),
        0
    ),2) * 100 AS close_rate
FROM buyers b
LEFT JOIN upsheets u
    ON u.assigned_user_id = b.assigned_user_id
LEFT JOIN opportunities o
    ON o.upsheet_id = u.upsheet_id
GROUP BY
    b.assigned_user_id,
    b.buyer_fname,
    b.buyer_lname
ORDER BY close_rate DESC NULLS LAST;