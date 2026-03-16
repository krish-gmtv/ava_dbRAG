WITH latest_opportunity_per_upsheet AS (
    SELECT
        o.upsheet_id,
        o.expected_amount,
        ROW_NUMBER() OVER (
            PARTITION BY o.upsheet_id
            ORDER BY o.created_at DESC, o.opportunity_id DESC
        ) AS rn
    FROM opportunities o
)
SELECT
    b.assigned_user_id,
    b.buyer_fname,
    b.buyer_lname,
    COALESCE(
        ROUND(
            AVG(
                ((u.sale_price - lo.expected_amount) / NULLIF(lo.expected_amount, 0)) * 100
            ),
            2
        ),
        0
    ) AS realization_percent
FROM buyers b
LEFT JOIN upsheets u
    ON u.assigned_user_id = b.assigned_user_id
   AND u.sold_date IS NOT NULL
LEFT JOIN latest_opportunity_per_upsheet lo
    ON lo.upsheet_id = u.upsheet_id
   AND lo.rn = 1
GROUP BY
    b.assigned_user_id,
    b.buyer_fname,
    b.buyer_lname
ORDER BY realization_percent DESC;