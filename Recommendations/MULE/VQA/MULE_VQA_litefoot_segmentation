DECLARE month_start DATE DEFAULT DATE '2026-01-01';
DECLARE month_end   DATE DEFAULT DATE '2026-01-31';

CREATE or replace  TABLE  `etsy-data-warehouse-dev.tsebastian.MULE_VQA_litefoot_seg`  AS
WITH
-- ------------------------------------------------------------------
-- 1) Jan 2026 BOE homescreen feed impressions from MULE, signed-in only
-- ------------------------------------------------------------------
jan_mule_feed_imps AS (
  SELECT
    rdl.visit_id,
    v.user_id,
    rdl._date AS visit_date,
    ANY_VALUE(rdl.buyer_segment) as legacy_segment,
    -- Per-visit feed metrics for this visit (MULE-only feed recs)
    SUM(rdl.seen) AS seen_imps,
    SUM(rdl.clicked) AS clicked_imps,
    SUM(rdl.purchased_after_view) AS purchases,
    SUM(COALESCE(rdl.transactions_gms, 0.0)) AS gms,

  FROM `etsy-data-warehouse-prod.rollups.recsys_delivered_listings` rdl
  JOIN `etsy-data-warehouse-prod.weblog.visits` v
    ON v.visit_id = rdl.visit_id
   AND v._date    = rdl._date

  WHERE rdl._date BETWEEN month_start AND month_end
    AND rdl.module_placement = 'boe_homescreen_feed'
    AND rdl.seen = 1
    AND rdl.candidate_set LIKE '%MULE%'
    AND v.user_id IS NOT NULL            -- signed-in only
    and v._date between '2026-01-01' and '2026-01-31'

  GROUP BY 1,2,3
),

-- ------------------------------------------------------------------
-- 2) First Jan-2026 visit per user with a MULE feed rec seen
-- ------------------------------------------------------------------
first_jan_visit AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY user_id
      ORDER BY visit_date, visit_id
    ) AS rn
  FROM jan_mule_feed_imps
),
first_users AS (
  SELECT
    user_id,
    legacy_segment,
    visit_id,
    visit_date,
    seen_imps,
    clicked_imps,
    purchases,
    gms
  FROM first_jan_visit
  WHERE rn = 1
),

-- ------------------------------------------------------------------
-- 3) Visit history in prior 365 days; pre-compute days_before_anchor
-- ------------------------------------------------------------------
visit_history AS (
  SELECT
    fu.user_id,
    fu.visit_date AS anchor_visit_date,
    v._date       AS visit_date,
    DATE_DIFF(fu.visit_date, v._date, DAY) AS days_before_anchor
  FROM first_users fu
  JOIN `etsy-data-warehouse-prod.weblog.visits` v
    ON v.user_id = fu.user_id
   AND v._date BETWEEN DATE_SUB(fu.visit_date, INTERVAL 365 DAY)
                   AND DATE_SUB(fu.visit_date, INTERVAL 1 DAY)
      and v._date >= '2025-01-01'
),

visit_agg AS (
  SELECT
    user_id,
    MAX(anchor_visit_date) AS first_visit_date,
    SUM(CASE WHEN days_before_anchor BETWEEN 1 AND 90 THEN 1 ELSE 0 END) AS visits_0_90,
    SUM(CASE WHEN days_before_anchor > 90 THEN 1 ELSE 0 END)           AS visits_91_plus
  FROM visit_history
  GROUP BY user_id
),

-- ------------------------------------------------------------------
-- 4) Feed rec clicks in the prior 90 days (all feed recs, not just MULE)
-- ------------------------------------------------------------------
feed_clicks_0_90 AS (
  SELECT
    fu.user_id,
    SUM(CASE WHEN rdl.clicked = 1 THEN 1 ELSE 0 END) AS feed_clicks_0_90
  FROM first_users fu
  JOIN `etsy-data-warehouse-prod.weblog.visits` v
    ON v.user_id = fu.user_id
   AND v._date BETWEEN DATE_SUB(fu.visit_date, INTERVAL 90 DAY)
                   AND DATE_SUB(fu.visit_date, INTERVAL 1 DAY)
  JOIN `etsy-data-warehouse-prod.rollups.recsys_delivered_listings` rdl
    ON rdl.visit_id = v.visit_id
   AND rdl._date   = v._date
  WHERE rdl.module_placement = 'boe_homescreen_feed'
    AND rdl.seen = 1
    AND v._date >= '2025-10-01'
  GROUP BY fu.user_id
)
 
-- ------------------------------------------------------------------
-- 5) Assign MECE engagement segments (signed-in only)
-- ------------------------------------------------------------------
SELECT
  fu.user_id,
  fu.legacy_segment,
  fu.visit_id AS anchor_visit_id,
  fu.visit_date AS anchor_visit_date,
  fu.seen_imps,
  fu.clicked_imps,
  fu.purchases,
  fu.gms,
  COALESCE(va.visits_0_90, 0) AS visits_0_90,
  COALESCE(va.visits_91_plus, 0) AS visits_91_plus,
  COALESCE(fc.feed_clicks_0_90, 0) AS feed_clicks_0_90,

  CASE
    WHEN COALESCE(va.visits_0_90,0) = 0
     AND COALESCE(va.visits_91_plus,0) = 0
      THEN 'No visit in last year'
    WHEN COALESCE(va.visits_0_90, 0) = 0
     AND COALESCE(va.visits_91_plus, 0) > 0
      THEN '90d Lapsed'
    WHEN COALESCE(va.visits_0_90, 0) = 1
      THEN '90d Single Visit'
    WHEN COALESCE(va.visits_0_90, 0) > 1
     AND COALESCE(fc.feed_clicks_0_90, 0) < 4
      THEN 'Multi Visit, Low Clicks'
    ELSE 'Most Active'
  END AS segment
FROM first_users fu
LEFT JOIN visit_agg va
  ON va.user_id = fu.user_id
LEFT JOIN feed_clicks_0_90 fc
  ON fc.user_id = fu.user_id

  ;

-- ------------------------------------------------------------------
-- 6) aggregate metrics by segment
-- ------------------------------------------------------------------
WITH
segment_metrics AS (
  SELECT
    segment,

    COUNT(DISTINCT user_id) AS users,
    SUM(gms) AS total_gms,
    SAFE_DIVIDE(SUM(gms), COUNT(DISTINCT user_id)) AS gms_per_user,
    SAFE_DIVIDE(SUM(purchases), NULLIF(SUM(clicked_imps), 0)) AS post_click_conversion_rate,
    SAFE_DIVIDE(SUM(clicked_imps), NULLIF(SUM(seen_imps), 0)) AS feed_click_rate
  FROM  `etsy-data-warehouse-dev.tsebastian.MULE_VQA_litefoot_seg` 
  GROUP BY 1
)

SELECT
  segment,
  users,
  gms_per_user,
  total_gms,
  post_click_conversion_rate,
  feed_click_rate
FROM segment_metrics
ORDER BY 1;


-- ------------------------------------------------------------------
-- 7) Multi visit low clicks percentiles
-- ------------------------------------------------------------------

with click_to_visit_by_user AS (
  SELECT
    user_id,
    legacy_segment,
    segment,             
    anchor_visit_id,
    anchor_visit_date,
    seen_imps,
    clicked_imps,
    purchases,
    gms,
    visits_0_90,
    visits_91_plus,
    feed_clicks_0_90,
    -- Define click-to-visit as prior-90d feed clicks / prior-90d visits
    SAFE_DIVIDE(feed_clicks_0_90, NULLIF(visits_0_90, 0)) AS click_to_visit
  FROM  `etsy-data-warehouse-dev.tsebastian.MULE_VQA_litefoot_seg`  
),

percentile_buckets AS (
  SELECT
    *,
    -- 100 = percentiles; change to 20 for ventiles, 10 for deciles, etc.
    NTILE(10) OVER (ORDER BY click_to_visit) AS click_to_visit_percentile,
    NTILE(10) OVER (ORDER BY visits_0_90) AS visit_90d_percentile,
    NTILE(10) OVER (ORDER BY feed_clicks_0_90) AS click_90d_percentile
  FROM click_to_visit_by_user
  WHERE click_to_visit IS NOT NULL
)

-- -- visit percentile
--   SELECT 'visit_percentile' as metric,
--     visit_90d_percentile,
--     COUNT(DISTINCT user_id) AS users,
--     AVG(visits_0_90) AS avg_visits_0_90,
--     SUM(gms) AS total_gms,
--     SAFE_DIVIDE(SUM(gms), COUNT(DISTINCT user_id)) AS gms_per_user,
--     SAFE_DIVIDE(SUM(purchases), NULLIF(SUM(clicked_imps), 0)) AS post_click_conversion_rate,
--     SAFE_DIVIDE(SUM(clicked_imps), NULLIF(SUM(seen_imps), 0)) AS feed_click_rate
--   FROM percentile_buckets
--   GROUP BY 1,2

-- -- click percentile
  -- SELECT 'click_percentile' as metric,
  --   click_90d_percentile,
  --   COUNT(DISTINCT user_id) AS users,
  --   AVG(feed_clicks_0_90) AS avg_click_to_visit,
  --   SUM(gms) AS total_gms,
  --   SAFE_DIVIDE(SUM(gms), COUNT(DISTINCT user_id)) AS gms_per_user,
  --   SAFE_DIVIDE(SUM(purchases), NULLIF(SUM(clicked_imps), 0)) AS post_click_conversion_rate,
  --   SAFE_DIVIDE(SUM(clicked_imps), NULLIF(SUM(seen_imps), 0)) AS feed_click_rate
  -- FROM percentile_buckets
  -- GROUP BY 1,2
