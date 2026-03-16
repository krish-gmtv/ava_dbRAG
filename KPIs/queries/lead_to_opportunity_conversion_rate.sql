WITH total_leads_cte AS (
    SELECT
        assigned_user_id,
        COUNT(DISTINCT lead_id) AS total_leads
    FROM leads
    GROUP BY assigned_user_id
),
leads_with_opportunity_cte AS (
    SELECT
        l.assigned_user_id,
        COUNT(DISTINCT l.lead_id) AS leads_with_opportunity
    FROM leads l
    JOIN upsheets u
        ON u.lead_id = l.lead_id
    JOIN opportunities o
        ON o.upsheet_id = u.upsheet_id
    GROUP BY l.assigned_user_id
)
SELECT
    b.assigned_user_id,
    b.buyer_fname,
    b.buyer_lname,
    COALESCE(t.total_leads, 0) AS total_leads,
    COALESCE(lo.leads_with_opportunity, 0) AS leads_with_opportunity,
    ROUND(
        COALESCE(lo.leads_with_opportunity, 0)::numeric
        / NULLIF(t.total_leads, 0) * 100,
        2
    ) AS lead_to_opportunity_conversion_rate
FROM buyers b
LEFT JOIN total_leads_cte t
    ON t.assigned_user_id = b.assigned_user_id
LEFT JOIN leads_with_opportunity_cte lo
    ON lo.assigned_user_id = b.assigned_user_id
ORDER BY lead_to_opportunity_conversion_rate DESC NULLS LAST;