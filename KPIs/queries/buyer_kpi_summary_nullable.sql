WITH total_leads_cte AS (
    SELECT
        b.assigned_user_id,
        COUNT(DISTINCT l.lead_id) AS total_leads
    FROM buyers b
    LEFT JOIN leads l
        ON l.assigned_user_id = b.assigned_user_id
    GROUP BY
        b.assigned_user_id
),
total_upsheets_cte AS (
    SELECT
        b.assigned_user_id,
        COUNT(DISTINCT u.upsheet_id) AS total_upsheets
    FROM buyers b
    LEFT JOIN upsheets u
        ON u.assigned_user_id = b.assigned_user_id
    GROUP BY
        b.assigned_user_id
),
total_opportunities_cte AS (
    SELECT
        b.assigned_user_id,
        COUNT(o.opportunity_id) AS total_opportunities
    FROM buyers b
    LEFT JOIN upsheets u
        ON u.assigned_user_id = b.assigned_user_id
    LEFT JOIN opportunities o
        ON o.upsheet_id = u.upsheet_id
    GROUP BY
        b.assigned_user_id
),
delivered_upsheets_cte AS (
    SELECT
        b.assigned_user_id,
        COUNT(u.upsheet_id) AS delivered_upsheets
    FROM buyers b
    LEFT JOIN upsheets u
        ON u.assigned_user_id = b.assigned_user_id
       AND u.delivered_date IS NOT NULL
    GROUP BY
        b.assigned_user_id
),
sold_upsheets_cte AS (
    SELECT
        b.assigned_user_id,
        COUNT(u.upsheet_id) AS sold_upsheets
    FROM buyers b
    LEFT JOIN upsheets u
        ON u.assigned_user_id = b.assigned_user_id
       AND u.sold_date IS NOT NULL
    GROUP BY
        b.assigned_user_id
),
opportunity_upsheets_cte AS (
    SELECT
        b.assigned_user_id,
        COUNT(DISTINCT u.upsheet_id) AS opportunity_upsheets
    FROM buyers b
    LEFT JOIN upsheets u
        ON u.assigned_user_id = b.assigned_user_id
    LEFT JOIN opportunities o
        ON o.upsheet_id = u.upsheet_id
    WHERE o.opportunity_id IS NOT NULL
    GROUP BY
        b.assigned_user_id
),
delivered_opportunity_upsheet_cte AS (
    SELECT DISTINCT
        u.upsheet_id,
        u.assigned_user_id
    FROM upsheets u
    JOIN opportunities o
        ON o.upsheet_id = u.upsheet_id
    WHERE u.delivered_date IS NOT NULL
),
delivered_opportunity_upsheets_cte AS (
    SELECT
        b.assigned_user_id,
        COUNT(dou.upsheet_id) AS delivered_opportunity_upsheets
    FROM buyers b
    LEFT JOIN delivered_opportunity_upsheet_cte dou
        ON dou.assigned_user_id = b.assigned_user_id
    GROUP BY
        b.assigned_user_id
),
close_rate_cte AS (
    SELECT
        b.assigned_user_id,
        ROUND(
            (
                COUNT(DISTINCT CASE
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
                )
            ) * 100,
            2
        ) AS close_rate
    FROM buyers b
    LEFT JOIN upsheets u
        ON u.assigned_user_id = b.assigned_user_id
    LEFT JOIN opportunities o
        ON o.upsheet_id = u.upsheet_id
    GROUP BY
        b.assigned_user_id
),
latest_opportunity_per_upsheet AS (
    SELECT
        o.upsheet_id,
        o.expected_amount,
        ROW_NUMBER() OVER (
            PARTITION BY o.upsheet_id
            ORDER BY o.created_at DESC, o.opportunity_id DESC
        ) AS rn
    FROM opportunities o
),
latest_expected_amount_cte AS (
    SELECT
        b.assigned_user_id,
        SUM(lo.expected_amount) AS latest_expected_amount
    FROM buyers b
    LEFT JOIN upsheets u
        ON u.assigned_user_id = b.assigned_user_id
    LEFT JOIN latest_opportunity_per_upsheet lo
        ON lo.upsheet_id = u.upsheet_id
       AND lo.rn = 1
    GROUP BY
        b.assigned_user_id
),
total_expected_amount_cte AS (
    SELECT
        b.assigned_user_id,
        SUM(o.expected_amount) AS total_expected_amount
    FROM buyers b
    LEFT JOIN upsheets u
        ON u.assigned_user_id = b.assigned_user_id
    LEFT JOIN opportunities o
        ON o.upsheet_id = u.upsheet_id
    GROUP BY
        b.assigned_user_id
),
total_sale_value_cte AS (
    SELECT
        b.assigned_user_id,
        COALESCE(SUM(u.sale_price), 0) AS total_sale_value
    FROM buyers b
    LEFT JOIN upsheets u
        ON u.assigned_user_id = b.assigned_user_id
       AND u.sold_date IS NOT NULL
    GROUP BY
        b.assigned_user_id
),
avg_sale_value_cte AS (
    SELECT
        b.assigned_user_id,
        ROUND(AVG(u.sale_price), 2) AS avg_sale_value
    FROM buyers b
    LEFT JOIN upsheets u
        ON u.assigned_user_id = b.assigned_user_id
       AND u.sold_date IS NOT NULL
    GROUP BY
        b.assigned_user_id
),
realization_amount_cte AS (
    SELECT
        b.assigned_user_id,
        COALESCE(
            ROUND(SUM(u.sale_price - lo.expected_amount), 2),
            0
        ) AS realization_amount
    FROM buyers b
    LEFT JOIN upsheets u
        ON u.assigned_user_id = b.assigned_user_id
       AND u.sold_date IS NOT NULL
    LEFT JOIN latest_opportunity_per_upsheet lo
        ON lo.upsheet_id = u.upsheet_id
       AND lo.rn = 1
    GROUP BY
        b.assigned_user_id
),
realization_percent_cte AS (
    SELECT
        b.assigned_user_id,
        ROUND(
            AVG(
                ((u.sale_price - lo.expected_amount) / NULLIF(lo.expected_amount, 0)) * 100
            ),
            2
        ) AS realization_percent
    FROM buyers b
    LEFT JOIN upsheets u
        ON u.assigned_user_id = b.assigned_user_id
       AND u.sold_date IS NOT NULL
    LEFT JOIN latest_opportunity_per_upsheet lo
        ON lo.upsheet_id = u.upsheet_id
       AND lo.rn = 1
    GROUP BY
        b.assigned_user_id
),
lead_to_opportunity_cte AS (
    WITH total_leads_inner_cte AS (
        SELECT
            assigned_user_id,
            COUNT(DISTINCT lead_id) AS total_leads
        FROM leads
        GROUP BY assigned_user_id
    ),
    leads_with_opportunity_inner_cte AS (
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
        COALESCE(t.total_leads, 0) AS total_leads_for_conv,
        COALESCE(lo.leads_with_opportunity, 0) AS leads_with_opportunity,
        ROUND(
            (
                COALESCE(lo.leads_with_opportunity, 0)::numeric
                / NULLIF(t.total_leads, 0)
            ) * 100,
            2
        ) AS lead_to_opportunity_conversion_rate
    FROM buyers b
    LEFT JOIN total_leads_inner_cte t
        ON t.assigned_user_id = b.assigned_user_id
    LEFT JOIN leads_with_opportunity_inner_cte lo
        ON lo.assigned_user_id = b.assigned_user_id
)
SELECT
    b.assigned_user_id,
    b.buyer_fname,
    b.buyer_lname,
    COALESCE(tl.total_leads, 0) AS total_leads,
    COALESCE(tu.total_upsheets, 0) AS total_upsheets,
    COALESCE(to_cte.total_opportunities, 0) AS total_opportunities,
    COALESCE(ou.opportunity_upsheets, 0) AS opportunity_upsheets,
    COALESCE(du.delivered_upsheets, 0) AS delivered_upsheets,
    COALESCE(su.sold_upsheets, 0) AS sold_upsheets,
    COALESCE(dou.delivered_opportunity_upsheets, 0) AS delivered_opportunity_upsheets,
    close_rate_cte.close_rate AS close_rate,
    COALESCE(lea.latest_expected_amount, 0) AS latest_expected_amount,
    COALESCE(tea.total_expected_amount, 0) AS total_expected_amount,
    COALESCE(tsv.total_sale_value, 0) AS total_sale_value,
    avg_sale_value_cte.avg_sale_value AS avg_sale_value,
    COALESCE(ra.realization_amount, 0) AS realization_amount,
    realization_percent_cte.realization_percent AS realization_percent,
    COALESCE(lto.total_leads_for_conv, 0) AS total_leads_for_conversion,
    COALESCE(lto.leads_with_opportunity, 0) AS leads_with_opportunity,
    lto.lead_to_opportunity_conversion_rate AS lead_to_opportunity_conversion_rate
FROM buyers b
LEFT JOIN total_leads_cte tl
    ON tl.assigned_user_id = b.assigned_user_id
LEFT JOIN total_upsheets_cte tu
    ON tu.assigned_user_id = b.assigned_user_id
LEFT JOIN total_opportunities_cte to_cte
    ON to_cte.assigned_user_id = b.assigned_user_id
LEFT JOIN opportunity_upsheets_cte ou
    ON ou.assigned_user_id = b.assigned_user_id
LEFT JOIN delivered_upsheets_cte du
    ON du.assigned_user_id = b.assigned_user_id
LEFT JOIN sold_upsheets_cte su
    ON su.assigned_user_id = b.assigned_user_id
LEFT JOIN delivered_opportunity_upsheets_cte dou
    ON dou.assigned_user_id = b.assigned_user_id
LEFT JOIN close_rate_cte
    ON close_rate_cte.assigned_user_id = b.assigned_user_id
LEFT JOIN latest_expected_amount_cte lea
    ON lea.assigned_user_id = b.assigned_user_id
LEFT JOIN total_expected_amount_cte tea
    ON tea.assigned_user_id = b.assigned_user_id
LEFT JOIN total_sale_value_cte tsv
    ON tsv.assigned_user_id = b.assigned_user_id
LEFT JOIN avg_sale_value_cte
    ON avg_sale_value_cte.assigned_user_id = b.assigned_user_id
LEFT JOIN realization_amount_cte ra
    ON ra.assigned_user_id = b.assigned_user_id
LEFT JOIN realization_percent_cte
    ON realization_percent_cte.assigned_user_id = b.assigned_user_id
LEFT JOIN lead_to_opportunity_cte lto
    ON lto.assigned_user_id = b.assigned_user_id
ORDER BY b.assigned_user_id;

