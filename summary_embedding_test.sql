WITH
/* ---------------------------
Quarterly leads
--------------------------- */
leads_quarterly_cte AS (
    SELECT
        l.assigned_user_id,
        date_trunc('quarter', l.date_entered)::date AS period_start,
        COUNT(DISTINCT l.lead_id) AS total_leads
    FROM leads l
    GROUP BY
        l.assigned_user_id,
        period_start
),

/* ---------------------------
Quarterly upsheets
--------------------------- */
upsheets_quarterly_cte AS (
    SELECT
        u.assigned_user_id,
        date_trunc('quarter', u.insert_date)::date AS period_start,
        COUNT(DISTINCT u.upsheet_id) AS total_upsheets
    FROM upsheets u
    GROUP BY
        u.assigned_user_id,
        period_start
),

/* ---------------------------
Quarterly leads with opportunity
--------------------------- */
leads_with_opportunity_quarterly_cte AS (
    SELECT
        l.assigned_user_id,
        date_trunc('quarter', l.date_entered)::date AS period_start,
        COUNT(DISTINCT l.lead_id) AS leads_with_opportunity
    FROM leads l
    JOIN upsheets u
        ON u.lead_id = l.lead_id
    JOIN opportunities o
        ON o.upsheet_id = u.upsheet_id
    GROUP BY
        l.assigned_user_id,
        period_start
),

/* ---------------------------
Quarterly opportunities
--------------------------- */
opportunities_quarterly_cte AS (
    SELECT
        u.assigned_user_id,
        date_trunc('quarter', o.created_at)::date AS period_start,
        COUNT(o.opportunity_id) AS total_opportunities
    FROM opportunities o
    JOIN upsheets u
        ON o.upsheet_id = u.upsheet_id
    GROUP BY
        u.assigned_user_id,
        period_start
),

/* ---------------------------
Quarterly opportunity upsheets
--------------------------- */
opportunity_upsheets_quarterly_cte AS (
    SELECT
        u.assigned_user_id,
        date_trunc('quarter', o.created_at)::date AS period_start,
        COUNT(DISTINCT u.upsheet_id) AS opportunity_upsheets
    FROM opportunities o
    JOIN upsheets u
        ON o.upsheet_id = u.upsheet_id
    GROUP BY
        u.assigned_user_id,
        period_start
),

/* ---------------------------
Delivered upsheets
--------------------------- */
delivered_upsheets_quarterly_cte AS (
    SELECT
        u.assigned_user_id,
        date_trunc('quarter', u.delivered_date)::date AS period_start,
        COUNT(DISTINCT u.upsheet_id) AS delivered_upsheets
    FROM upsheets u
    WHERE u.delivered_date IS NOT NULL
    GROUP BY
        u.assigned_user_id,
        period_start
),

/* ---------------------------
Sold upsheets
--------------------------- */
sold_upsheets_quarterly_cte AS (
    SELECT
        u.assigned_user_id,
        date_trunc('quarter', u.sold_date)::date AS period_start,
        COUNT(DISTINCT u.upsheet_id) AS sold_upsheets
    FROM upsheets u
    WHERE u.sold_date IS NOT NULL
    GROUP BY
        u.assigned_user_id,
        period_start
),

/* ---------------------------
Delivered upsheets with opportunities
--------------------------- */
delivered_opportunity_upsheets_quarterly_cte AS (
    SELECT
        u.assigned_user_id,
        date_trunc('quarter', u.delivered_date)::date AS period_start,
        COUNT(DISTINCT u.upsheet_id) AS delivered_opportunity_upsheets
    FROM upsheets u
    JOIN opportunities o
        ON o.upsheet_id = u.upsheet_id
    WHERE u.delivered_date IS NOT NULL
    GROUP BY
        u.assigned_user_id,
        period_start
),

/* ---------------------------
Sale value (total & average)
--------------------------- */
sale_value_quarterly_cte AS (
    SELECT
        u.assigned_user_id,
        date_trunc('quarter', u.sold_date)::date AS period_start,
        SUM(u.sale_price) AS total_sale_value,
        ROUND(AVG(u.sale_price), 2) AS avg_sale_value
    FROM upsheets u
    WHERE u.sold_date IS NOT NULL
    GROUP BY
        u.assigned_user_id,
        period_start
),

/* ---------------------------
Latest opportunity per upsheet per quarter (for expected amount)
--------------------------- */
latest_opportunity_per_upsheet_quarterly_cte AS (
    SELECT
        u.assigned_user_id,
        date_trunc('quarter', o.created_at)::date AS period_start,
        u.upsheet_id,
        o.expected_amount,
        ROW_NUMBER() OVER (
            PARTITION BY u.upsheet_id, date_trunc('quarter', o.created_at)::date
            ORDER BY o.created_at DESC, o.opportunity_id DESC
        ) AS rn
    FROM upsheets u
    JOIN opportunities o
        ON o.upsheet_id = u.upsheet_id
),

/* ---------------------------
Latest expected amount per quarter
--------------------------- */
latest_expected_amount_quarterly_cte AS (
    SELECT
        lo.assigned_user_id,
        lo.period_start,
        SUM(lo.expected_amount) AS latest_expected_amount
    FROM latest_opportunity_per_upsheet_quarterly_cte lo
    WHERE lo.rn = 1
    GROUP BY
        lo.assigned_user_id,
        lo.period_start
),

/* ---------------------------
Total expected amount per quarter
--------------------------- */
total_expected_amount_quarterly_cte AS (
    SELECT
        u.assigned_user_id,
        date_trunc('quarter', o.created_at)::date AS period_start,
        SUM(o.expected_amount) AS total_expected_amount
    FROM upsheets u
    JOIN opportunities o
        ON o.upsheet_id = u.upsheet_id
    GROUP BY
        u.assigned_user_id,
        period_start
),

/* ---------------------------
Realization (amount & percent) per quarter (by sold_date)
--------------------------- */
latest_opportunity_per_upsheet_global_cte AS (
    SELECT
        o.upsheet_id,
        o.expected_amount,
        ROW_NUMBER() OVER (
            PARTITION BY o.upsheet_id
            ORDER BY o.created_at DESC, o.opportunity_id DESC
        ) AS rn
    FROM opportunities o
),
realization_quarterly_cte AS (
    SELECT
        u.assigned_user_id,
        date_trunc('quarter', u.sold_date)::date AS period_start,
        ROUND(SUM(u.sale_price - lo.expected_amount), 2) AS realization_amount,
        ROUND(
            AVG(
                ((u.sale_price - lo.expected_amount) / NULLIF(lo.expected_amount, 0)) * 100
            ),
            2
        ) AS realization_percent
    FROM upsheets u
    LEFT JOIN latest_opportunity_per_upsheet_global_cte lo
        ON lo.upsheet_id = u.upsheet_id
       AND lo.rn = 1
    WHERE u.sold_date IS NOT NULL
    GROUP BY
        u.assigned_user_id,
        period_start
),

/* ---------------------------
Buyer-period spine (all buyer+quarter combinations with any activity)
--------------------------- */
buyer_periods_cte AS (
    SELECT assigned_user_id, period_start FROM leads_quarterly_cte
    UNION
    SELECT assigned_user_id, period_start FROM upsheets_quarterly_cte
    UNION
    SELECT assigned_user_id, period_start FROM leads_with_opportunity_quarterly_cte
    UNION
    SELECT assigned_user_id, period_start FROM opportunities_quarterly_cte
    UNION
    SELECT assigned_user_id, period_start FROM opportunity_upsheets_quarterly_cte
    UNION
    SELECT assigned_user_id, period_start FROM delivered_upsheets_quarterly_cte
    UNION
    SELECT assigned_user_id, period_start FROM sold_upsheets_quarterly_cte
    UNION
    SELECT assigned_user_id, period_start FROM delivered_opportunity_upsheets_quarterly_cte
    UNION
    SELECT assigned_user_id, period_start FROM total_expected_amount_quarterly_cte
    UNION
    SELECT assigned_user_id, period_start FROM latest_expected_amount_quarterly_cte
    UNION
    SELECT assigned_user_id, period_start FROM sale_value_quarterly_cte
    UNION
    SELECT assigned_user_id, period_start FROM realization_quarterly_cte
)

SELECT
    bp.assigned_user_id,
    b.buyer_fname,
    b.buyer_lname,

    bp.period_start,
    (bp.period_start + interval '3 months - 1 day')::date AS period_end,
    EXTRACT(year FROM bp.period_start) AS period_year,
    EXTRACT(quarter FROM bp.period_start) AS period_quarter,

    COALESCE(lq.total_leads, 0) AS total_leads,
    COALESCE(uq.total_upsheets, 0) AS total_upsheets,
    COALESCE(lwoq.leads_with_opportunity, 0) AS leads_with_opportunity,
    ROUND(
        (
            COALESCE(lwoq.leads_with_opportunity, 0)::numeric
            / NULLIF(lq.total_leads, 0)
        ) * 100,
        2
    ) AS lead_to_opportunity_conversion_rate,

    COALESCE(oq.total_opportunities, 0) AS total_opportunities,
    COALESCE(ouq.opportunity_upsheets, 0) AS opportunity_upsheets,

    COALESCE(duq.delivered_upsheets, 0) AS delivered_upsheets,
    COALESCE(suq.sold_upsheets, 0) AS sold_upsheets,
    COALESCE(douq.delivered_opportunity_upsheets, 0) AS delivered_opportunity_upsheets,

    ROUND(
        (
            COALESCE(douq.delivered_opportunity_upsheets, 0)::numeric
            / NULLIF(ouq.opportunity_upsheets, 0)
        ) * 100,
        2
    ) AS close_rate,

    COALESCE(tea.total_expected_amount, 0) AS total_expected_amount,
    COALESCE(lea.latest_expected_amount, 0) AS latest_expected_amount,

    COALESCE(svq.total_sale_value, 0) AS total_sale_value,
    svq.avg_sale_value AS avg_sale_value,

    COALESCE(rq.realization_amount, 0) AS realization_amount,
    rq.realization_percent AS realization_percent

FROM buyers b
JOIN buyer_periods_cte bp
    ON bp.assigned_user_id = b.assigned_user_id
LEFT JOIN leads_quarterly_cte lq
    ON lq.assigned_user_id = bp.assigned_user_id
   AND lq.period_start = bp.period_start
LEFT JOIN upsheets_quarterly_cte uq
    ON uq.assigned_user_id = bp.assigned_user_id
   AND uq.period_start = bp.period_start
LEFT JOIN leads_with_opportunity_quarterly_cte lwoq
    ON lwoq.assigned_user_id = bp.assigned_user_id
   AND lwoq.period_start = bp.period_start
LEFT JOIN opportunities_quarterly_cte oq
    ON oq.assigned_user_id = bp.assigned_user_id
   AND oq.period_start = bp.period_start
LEFT JOIN opportunity_upsheets_quarterly_cte ouq
    ON ouq.assigned_user_id = bp.assigned_user_id
   AND ouq.period_start = bp.period_start
LEFT JOIN delivered_upsheets_quarterly_cte duq
    ON duq.assigned_user_id = bp.assigned_user_id
   AND duq.period_start = bp.period_start
LEFT JOIN sold_upsheets_quarterly_cte suq
    ON suq.assigned_user_id = bp.assigned_user_id
   AND suq.period_start = bp.period_start
LEFT JOIN delivered_opportunity_upsheets_quarterly_cte douq
    ON douq.assigned_user_id = bp.assigned_user_id
   AND douq.period_start = bp.period_start
LEFT JOIN sale_value_quarterly_cte svq
    ON svq.assigned_user_id = bp.assigned_user_id
   AND svq.period_start = bp.period_start
LEFT JOIN total_expected_amount_quarterly_cte tea
    ON tea.assigned_user_id = bp.assigned_user_id
   AND tea.period_start = bp.period_start
LEFT JOIN latest_expected_amount_quarterly_cte lea
    ON lea.assigned_user_id = bp.assigned_user_id
   AND lea.period_start = bp.period_start
LEFT JOIN realization_quarterly_cte rq
    ON rq.assigned_user_id = bp.assigned_user_id
   AND rq.period_start = bp.period_start

ORDER BY
    bp.assigned_user_id,
    bp.period_start;

