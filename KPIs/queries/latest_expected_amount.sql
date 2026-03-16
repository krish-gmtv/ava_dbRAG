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
    SUM(lo.expected_amount) AS latest_expected_amount
FROM buyers b
LEFT JOIN upsheets u
    ON u.assigned_user_id = b.assigned_user_id
LEFT JOIN latest_opportunity_per_upsheet lo
    ON lo.upsheet_id = u.upsheet_id
   AND lo.rn = 1
GROUP BY
    b.assigned_user_id,
    b.buyer_fname,
    b.buyer_lname
ORDER BY latest_expected_amount DESC NULLS LAST;