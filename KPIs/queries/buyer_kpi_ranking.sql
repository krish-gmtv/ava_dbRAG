WITH buyer_kpi_summary AS (
    WITH total_leads_cte AS (
        SELECT
            l.assigned_user_id,
            COUNT(DISTINCT l.lead_id) AS total_leads
        FROM leads l
        GROUP BY l.assigned_user_id
    ),
    total_upsheets_cte AS (
        SELECT
            u.assigned_user_id,
            COUNT(DISTINCT u.upsheet_id) AS total_upsheets
        FROM upsheets u
        GROUP BY u.assigned_user_id
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
    ),
    lead_to_opportunity_conversion_rate_cte AS (
        SELECT
            b.assigned_user_id,
            COALESCE(lwo.leads_with_opportunity, 0) AS leads_with_opportunity,
            ROUND(
                (
                    COALESCE(lwo.leads_with_opportunity, 0)::numeric
                    / NULLIF(tl.total_leads, 0)
                ) * 100,
                2
            ) AS lead_to_opportunity_conversion_rate
        FROM buyers b
        LEFT JOIN total_leads_cte tl
            ON tl.assigned_user_id = b.assigned_user_id
        LEFT JOIN leads_with_opportunity_cte lwo
            ON lwo.assigned_user_id = b.assigned_user_id
    ),
    total_opportunities_cte AS (
        SELECT
            u.assigned_user_id,
            COUNT(o.opportunity_id) AS total_opportunities
        FROM upsheets u
        JOIN opportunities o
            ON o.upsheet_id = u.upsheet_id
        GROUP BY u.assigned_user_id
    ),
    opportunity_upsheets_cte AS (
        SELECT
            u.assigned_user_id,
            COUNT(DISTINCT u.upsheet_id) AS opportunity_upsheets
        FROM upsheets u
        JOIN opportunities o
            ON o.upsheet_id = u.upsheet_id
        GROUP BY u.assigned_user_id
    ),
    sold_upsheets_cte AS (
        SELECT
            u.assigned_user_id,
            COUNT(DISTINCT u.upsheet_id) AS sold_upsheets
        FROM upsheets u
        WHERE u.sold_date IS NOT NULL
        GROUP BY u.assigned_user_id
    ),
    delivered_upsheets_cte AS (
        SELECT
            u.assigned_user_id,
            COUNT(DISTINCT u.upsheet_id) AS delivered_upsheets
        FROM upsheets u
        WHERE u.delivered_date IS NOT NULL
        GROUP BY u.assigned_user_id
    ),
    delivered_opportunity_upsheet_base_cte AS (
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
            dou.assigned_user_id,
            COUNT(dou.upsheet_id) AS delivered_opportunity_upsheets
        FROM delivered_opportunity_upsheet_base_cte dou
        GROUP BY dou.assigned_user_id
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
        GROUP BY b.assigned_user_id
    ),
    latest_opportunity_per_upsheet_cte AS (
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
            u.assigned_user_id,
            SUM(lo.expected_amount) AS latest_expected_amount
        FROM upsheets u
        LEFT JOIN latest_opportunity_per_upsheet_cte lo
            ON lo.upsheet_id = u.upsheet_id
           AND lo.rn = 1
        GROUP BY u.assigned_user_id
    ),
    total_expected_amount_cte AS (
        SELECT
            u.assigned_user_id,
            SUM(o.expected_amount) AS total_expected_amount
        FROM upsheets u
        JOIN opportunities o
            ON o.upsheet_id = u.upsheet_id
        GROUP BY u.assigned_user_id
    ),
    total_sale_value_cte AS (
        SELECT
            u.assigned_user_id,
            COALESCE(SUM(u.sale_price), 0) AS total_sale_value
        FROM upsheets u
        WHERE u.sold_date IS NOT NULL
        GROUP BY u.assigned_user_id
    ),
    avg_sale_value_cte AS (
        SELECT
            u.assigned_user_id,
            COALESCE(ROUND(AVG(u.sale_price), 2), 0) AS avg_sale_value
        FROM upsheets u
        WHERE u.sold_date IS NOT NULL
        GROUP BY u.assigned_user_id
    ),
    realization_amount_cte AS (
        SELECT
            u.assigned_user_id,
            COALESCE(
                ROUND(SUM(u.sale_price - lo.expected_amount), 2),
                0
            ) AS realization_amount
        FROM upsheets u
        LEFT JOIN latest_opportunity_per_upsheet_cte lo
            ON lo.upsheet_id = u.upsheet_id
           AND lo.rn = 1
        WHERE u.sold_date IS NOT NULL
        GROUP BY u.assigned_user_id
    ),
    realization_percent_cte AS (
        SELECT
            u.assigned_user_id,
            COALESCE(
                ROUND(
                    AVG(
                        ((u.sale_price - lo.expected_amount) / NULLIF(lo.expected_amount, 0)) * 100
                    ),
                    2
                ),
                0
            ) AS realization_percent
        FROM upsheets u
        LEFT JOIN latest_opportunity_per_upsheet_cte lo
            ON lo.upsheet_id = u.upsheet_id
           AND lo.rn = 1
        WHERE u.sold_date IS NOT NULL
        GROUP BY u.assigned_user_id
    )
    SELECT
        b.assigned_user_id,
        b.buyer_fname,
        b.buyer_lname,
        COALESCE(tl.total_leads, 0) AS total_leads,
        COALESCE(tu.total_upsheets, 0) AS total_upsheets,
        COALESCE(lwo.leads_with_opportunity, 0) AS leads_with_opportunity,
        COALESCE(lto.lead_to_opportunity_conversion_rate, 0) AS lead_to_opportunity_conversion_rate,
        COALESCE(topps.total_opportunities, 0) AS total_opportunities,
        COALESCE(ou.opportunity_upsheets, 0) AS opportunity_upsheets,
        COALESCE(su.sold_upsheets, 0) AS sold_upsheets,
        COALESCE(du.delivered_upsheets, 0) AS delivered_upsheets,
        COALESCE(dou.delivered_opportunity_upsheets, 0) AS delivered_opportunity_upsheets,
        COALESCE(cr.close_rate, 0) AS close_rate,
        COALESCE(tea.total_expected_amount, 0) AS total_expected_amount,
        COALESCE(lea.latest_expected_amount, 0) AS latest_expected_amount,
        COALESCE(tsv.total_sale_value, 0) AS total_sale_value,
        COALESCE(asv.avg_sale_value, 0) AS avg_sale_value,
        COALESCE(ra.realization_amount, 0) AS realization_amount,
        COALESCE(rp.realization_percent, 0) AS realization_percent
    FROM buyers b
    LEFT JOIN total_leads_cte tl
        ON tl.assigned_user_id = b.assigned_user_id
    LEFT JOIN total_upsheets_cte tu
        ON tu.assigned_user_id = b.assigned_user_id
    LEFT JOIN leads_with_opportunity_cte lwo
        ON lwo.assigned_user_id = b.assigned_user_id
    LEFT JOIN lead_to_opportunity_conversion_rate_cte lto
        ON lto.assigned_user_id = b.assigned_user_id
    LEFT JOIN total_opportunities_cte topps
        ON topps.assigned_user_id = b.assigned_user_id
    LEFT JOIN opportunity_upsheets_cte ou
        ON ou.assigned_user_id = b.assigned_user_id
    LEFT JOIN sold_upsheets_cte su
        ON su.assigned_user_id = b.assigned_user_id
    LEFT JOIN delivered_upsheets_cte du
        ON du.assigned_user_id = b.assigned_user_id
    LEFT JOIN delivered_opportunity_upsheets_cte dou
        ON dou.assigned_user_id = b.assigned_user_id
    LEFT JOIN close_rate_cte cr
        ON cr.assigned_user_id = b.assigned_user_id
    LEFT JOIN total_expected_amount_cte tea
        ON tea.assigned_user_id = b.assigned_user_id
    LEFT JOIN latest_expected_amount_cte lea
        ON lea.assigned_user_id = b.assigned_user_id
    LEFT JOIN total_sale_value_cte tsv
        ON tsv.assigned_user_id = b.assigned_user_id
    LEFT JOIN avg_sale_value_cte asv
        ON asv.assigned_user_id = b.assigned_user_id
    LEFT JOIN realization_amount_cte ra
        ON ra.assigned_user_id = b.assigned_user_id
    LEFT JOIN realization_percent_cte rp
        ON rp.assigned_user_id = b.assigned_user_id
)
SELECT
    bks.*,
    RANK() OVER (
        ORDER BY bks.total_leads DESC
    ) AS buyer_workload_rank,
    RANK() OVER (
        ORDER BY bks.lead_to_opportunity_conversion_rate DESC NULLS LAST
    ) AS buyer_conversion_rank,
    RANK() OVER (
        ORDER BY bks.close_rate DESC NULLS LAST
    ) AS buyer_execution_rank,
    RANK() OVER (
        ORDER BY bks.total_sale_value DESC
    ) AS buyer_value_rank
FROM buyer_kpi_summary bks
ORDER BY bks.assigned_user_id;

