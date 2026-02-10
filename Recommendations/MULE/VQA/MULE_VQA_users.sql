##### 30 days lookback #####

-- DECLARE lookback_days INT64 DEFAULT 30;
DECLARE start_date DATE DEFAULT '2025-12-29';
DECLARE end_date   DATE DEFAULT '2026-01-27';

-- 1) Base rows: MULE feed recs on BOE homescreen feed
WITH mule_feed_rows AS (
  SELECT
    v.user_id,
    v.browser_id,
    rdl.visit_id,
    rdl._date,
    rdl.candidate_set,
    rdl.clicked,
    rdl.favorited,
    1 AS rec_delivered,
    transactions_gms,
    buyer_segment
  FROM `etsy-data-warehouse-prod.rollups.recsys_delivered_listings` rdl
  JOIN `etsy-data-warehouse-prod.weblog.visits` v
    ON rdl.visit_id = v.visit_id
   AND rdl._date    = v._date          
  WHERE
    rdl._date BETWEEN start_date AND end_date
    and v._date BETWEEN start_date AND end_date
    AND rdl.module_placement = 'boe_homescreen_feed'
    AND rdl.candidate_set LIKE '%MULE%'     
),

-- 2) Per-"user" aggregates among those served MULE

per_user_mule AS (
  SELECT

    CASE
      WHEN user_id IS NOT NULL THEN CAST(user_id AS STRING)
      ELSE browser_id
    END AS user_key,
    buyer_segment,
    user_id,         
    COUNT(DISTINCT visit_id)                 AS feed_mule_visits,
    SUM(rec_delivered)                       AS feed_mule_recs_delivered,
    SUM(clicked)                             AS feed_mule_clicks,
    SUM(favorited)                           AS feed_mule_favorites,
    sum(transactions_gms)                    as feed_mule_transactions_gms

  FROM mule_feed_rows
  WHERE
    -- ignore rows where we somehow have neither user_id nor browser_id
    (user_id IS NOT NULL OR browser_id IS NOT NULL)
  GROUP BY 1, 2,3
),

/* ------------------------------------------------------------------
   1) Signed-in vs Signed-out share among users served MULE
-------------------------------------------------------------------*/
signed_in_split AS (
  SELECT
    CASE
      WHEN user_id IS NULL THEN 'signed_out'
      ELSE 'signed_in'
    END AS sign_in_status,
    COUNT(DISTINCT user_key) AS user_count
  FROM per_user_mule
  GROUP BY 1
),

buyer_segment_split AS (
  SELECT
    CASE
      WHEN user_id IS NULL THEN 'Signed_Out'
      WHEN buyer_segment IS NULL THEN 'Unknown'
      ELSE buyer_segment
    END AS buyer_segment,
    COUNT(DISTINCT user_key) AS user_count,
    avg(feed_mule_recs_delivered) as avg_feed_mule_recs_delivered,
    avg(feed_mule_clicks) as avg_feed_mule_clicks,
    avg(feed_mule_favorites) as avg_feed_mule_favorites,
    avg(feed_mule_transactions_gms) as avg_feed_mule_transactions_gms
    
  FROM per_user_mule
  GROUP BY 1
),


/* ------------------------------------------------------------------
   3) Quartiles (and mean) for per-user engagement metrics,
      restricted to users with ≥1 of that metric
-------------------------------------------------------------------*/

click_quartiles AS (
  WITH filtered AS (
    SELECT feed_mule_clicks AS val
    FROM per_user_mule
    -- WHERE feed_mule_clicks >= 1
  )
  SELECT
    'feed_mule_clicks' AS metric,
    q[OFFSET(0)] AS min,
    q[OFFSET(1)] AS q1,
    q[OFFSET(2)] AS median,
    q[OFFSET(3)] AS q3,
    q[OFFSET(4)] AS max,
    mean
  FROM (SELECT APPROX_QUANTILES(val, 4) AS q FROM filtered),
       (SELECT AVG(val) AS mean FROM filtered)
),

favorite_quartiles AS (
  WITH filtered AS (
    SELECT feed_mule_favorites AS val
    FROM per_user_mule
    -- WHERE feed_mule_favorites >= 1
  )
  SELECT
    'feed_mule_favorites' AS metric,
    q[OFFSET(0)] AS min,
    q[OFFSET(1)] AS q1,
    q[OFFSET(2)] AS median,
    q[OFFSET(3)] AS q3,
    q[OFFSET(4)] AS max,
    mean
  FROM (SELECT APPROX_QUANTILES(val, 4) AS q FROM filtered),
       (SELECT AVG(val) AS mean FROM filtered)
),

visit_quartiles AS (
  WITH filtered AS (
    SELECT feed_mule_visits AS val
    FROM per_user_mule
    -- WHERE feed_mule_visits >= 1
  )
  SELECT
    'feed_mule_visits' AS metric,
    q[OFFSET(0)] AS min,
    q[OFFSET(1)] AS q1,
    q[OFFSET(2)] AS median,
    q[OFFSET(3)] AS q3,
    q[OFFSET(4)] AS max,
    mean
  FROM (SELECT APPROX_QUANTILES(val, 4) AS q FROM filtered),
       (SELECT AVG(val) AS mean FROM filtered)
),

delivered_quartiles AS (
  WITH filtered AS (
    SELECT feed_mule_recs_delivered AS val
    FROM per_user_mule
    -- WHERE feed_mule_recs_delivered >= 1
  )
  SELECT
    'feed_mule_recs_delivered' AS metric,
    q[OFFSET(0)] AS min,
    q[OFFSET(1)] AS q1,
    q[OFFSET(2)] AS median,
    q[OFFSET(3)] AS q3,
    q[OFFSET(4)] AS max,
    mean
  FROM (SELECT APPROX_QUANTILES(val, 4) AS q FROM filtered),
       (SELECT AVG(val) AS mean FROM filtered)
)
,
gms_quartile AS (
  WITH filtered AS (
    SELECT feed_mule_transactions_gms AS val
    FROM per_user_mule
    -- WHERE feed_mule_transactions_gms > 0
  )
  SELECT
    'feed_mule_transaction_gms' AS metric,
    q[OFFSET(0)] AS min,
    q[OFFSET(1)] AS q1,
    q[OFFSET(2)] AS median,
    q[OFFSET(3)] AS q3,
    q[OFFSET(4)] AS max,
    mean
  FROM (SELECT APPROX_QUANTILES(val, 4) AS q FROM filtered),
       (SELECT AVG(val) AS mean FROM filtered)
)
##-- ===========================
##-- FINAL SELECTS
##-- ===========================

##-- Final quartiles + means
SELECT * FROM click_quartiles
UNION ALL
SELECT * FROM favorite_quartiles
UNION ALL
SELECT * FROM visit_quartiles
UNION ALL
SELECT * FROM delivered_quartiles
UNION ALL
SELECT * FROM gms_quartile
ORDER BY metric;

-- 1) % Signed-in vs Signed-out (users served MULE)
-- SELECT
--   'sign_in_split' AS table_name,
--   sign_in_status,
--   user_count,
--   ROUND(100 * user_count / SUM(user_count) OVER (), 2) AS pct_of_users
-- FROM signed_in_split
-- ORDER BY sign_in_status;

-- 2) Buyer segment breakdown (users served MULE)
SELECT
  'buyer_segment_split' AS table_name,
  a.*,
  ROUND(100 * user_count / SUM(user_count) OVER (), 2) AS pct_of_users

FROM buyer_segment_split a
ORDER BY user_count DESC;


--- by candidate set

DECLARE lookback_days INT64 DEFAULT 30;
DECLARE start_date DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY);
DECLARE end_date   DATE DEFAULT CURRENT_DATE();

  SELECT 
    rdl.candidate_set, 
    count(distinct user_id),
     count(distinct rdl.visit_id) as visits,
    sum(clicked)/sum(seen) as listing_ctr,
    sum(favorited)/sum(seen) as listing_fav_rate,
    sum(purchased_after_view)/sum(seen) as listing_cvr,
    count(distinct case when purchased_after_view>0 then rdl.visit_id end)/count(distinct rdl.visit_id) as visit_cvr,
    sum(transactions_gms)/count(distinct user_id) as gms_per_user,
    sum(transactions_gms)/count(distinct rdl.visit_id) as gms_per_visit,
    
  FROM `etsy-data-warehouse-prod.rollups.recsys_delivered_listings` rdl
  JOIN `etsy-data-warehouse-prod.weblog.visits` v
    ON rdl.visit_id = v.visit_id
   AND rdl._date    = v._date   
  WHERE
    rdl._date BETWEEN start_date AND end_date
    and v._date BETWEEN start_date AND end_date
    AND rdl.module_placement = 'boe_homescreen_feed'
group by 1 order by 1
;



##### 90 days lookback #######


-- Parameters
DECLARE start_date DATE DEFAULT '2025-12-31';
DECLARE end_date   DATE DEFAULT '2026-01-29';

--------------------------------------------------------------------------------
-- 0. Base users: users with ≥1 visit in the Dec 31 2025 – Jan 29 2026 window
--    and an "anchor date" = first visit date in that window.
--------------------------------------------------------------------------------

CREATE or replace  TABLE  `etsy-data-warehouse-dev.tsebastian.base_users`  AS
SELECT
  user_id,
  MIN(_date) AS anchor_date
FROM `etsy-data-warehouse-prod.visit_mart.visits`
WHERE
  user_id IS NOT NULL
  AND _date BETWEEN start_date AND end_date
GROUP BY user_id;

CREATE or replace  TABLE `etsy-data-warehouse-dev.tsebastian.user_lookback_bounds` AS
SELECT
  user_id,
  anchor_date,
  DATE_SUB(anchor_date, INTERVAL 90 DAY) AS lb_start,
  DATE_SUB(anchor_date, INTERVAL 1 DAY)  AS lb_end
FROM `etsy-data-warehouse-dev.tsebastian.base_users`;

--------------------------------------------------------------------------------
-- 1. Prior 90 days per-user features (feed clicks/favs/delivered/purchases,
--    searches, and feed-attributed GMS).
--------------------------------------------------------------------------------

-- 1a. Feed recs metrics over prior 90 days (recsys_delivered_listings)
CREATE or replace TABLE `etsy-data-warehouse-dev.tsebastian.recs_90d`  AS
SELECT
  u.user_id,
  -- Delivered feed recs = seen feed impressions
  SUM(CASE WHEN rdl.seen = 1 THEN 1 ELSE 0 END)                    AS feed_recs_delivered_90d,
  -- Feed clicks
  SUM(CASE WHEN rdl.clicked = 1 THEN 1 ELSE 0 END)                 AS feed_clicks_90d,
  -- Feed favorites
  SUM(CASE WHEN rdl.favorited = 1 THEN 1 ELSE 0 END)               AS feed_favs_90d,
  -- Recs-attributed purchases from feed
  SUM(CASE WHEN rdl.purchased_after_view = 1 THEN 1 ELSE 0 END)    AS feed_purchases_90d,
  -- If you ever want quantity, it's here (not used directly in stats)
  SUM(COALESCE(rdl.transactions_quantity, 0))                       AS feed_transactions_qty_90d,
  -- Recs-attributed GMS from feed
  SUM(COALESCE(rdl.transactions_gms, 0.0))                          AS feed_gms_90d
FROM `etsy-data-warehouse-dev.tsebastian.user_lookback_bounds`  u
JOIN `etsy-data-warehouse-prod.weblog.visits` v
  ON v.user_id = u.user_id
 AND v._date BETWEEN u.lb_start AND u.lb_end
JOIN `etsy-data-warehouse-prod.rollups.recsys_delivered_listings` rdl
  ON rdl.visit_id = v.visit_id
 AND rdl._date BETWEEN u.lb_start AND u.lb_end
WHERE v._date >= '2025-09-01'
  and rdl.module_placement = 'boe_homescreen_feed'
  AND rdl.seen = 1

GROUP BY u.user_id;

-- 1b. Search count over prior 90 days
CREATE or replace  TABLE `etsy-data-warehouse-dev.tsebastian.searches_90d` AS
SELECT
  u.user_id,
  COUNTIF(
    DATE(TIMESTAMP_SECONDS(s.run_date)) BETWEEN u.lb_start AND u.lb_end
  ) AS searches_90d
FROM `etsy-data-warehouse-dev.tsebastian.user_lookback_bounds` u
JOIN `etsy-data-warehouse-prod.search.events` s
  ON CAST(s.user_id AS INT64) = u.user_id
 AND DATE(s._date) BETWEEN u.lb_start AND u.lb_end   -- partition pruning
 and s._date >= '2025-09-01'
GROUP BY u.user_id;

-- 1c. Combine into per-user feature table, coalescing NULLs to 0
CREATE or replace  TABLE `etsy-data-warehouse-dev.tsebastian.user_90d_features` AS
SELECT
  u.user_id,
  COALESCE(r.feed_clicks_90d,          0) AS feed_clicks_90d,
  COALESCE(r.feed_favs_90d,            0) AS feed_favs_90d,
  COALESCE(r.feed_recs_delivered_90d,  0) AS feed_recs_delivered_90d,
  COALESCE(r.feed_purchases_90d,       0) AS feed_purchases_90d,
  COALESCE(s.searches_90d,             0) AS searches_90d,
  COALESCE(r.feed_gms_90d,             0.0) AS feed_gms_90d,
  COALESCE(feed_clicks_90d,0)+COALESCE(feed_favs_90d,0)+COALESCE(feed_purchases_90d,0)+COALESCE(searches_90d,0) AS interactions_90d
FROM `etsy-data-warehouse-dev.tsebastian.user_lookback_bounds` u
LEFT JOIN `etsy-data-warehouse-dev.tsebastian.recs_90d`     r USING (user_id)
LEFT JOIN `etsy-data-warehouse-dev.tsebastian.searches_90d` s USING (user_id);



-- 1d. Distribution stats across users:
--     min, q1, median, q3, max, mean for each metric
WITH metric_qs AS (
  SELECT
    APPROX_QUANTILES(feed_clicks_90d,         4) AS q_feed_clicks,
    APPROX_QUANTILES(feed_favs_90d,           4) AS q_feed_favs,
    APPROX_QUANTILES(feed_recs_delivered_90d, 4) AS q_feed_recs_delivered,
    APPROX_QUANTILES(feed_purchases_90d,      4) AS q_feed_purchases,
    APPROX_QUANTILES(searches_90d,            4) AS q_searches,
    APPROX_QUANTILES(feed_gms_90d,            4) AS q_feed_gms,
    APPROX_QUANTILES(interactions_90d, 4) AS q_interactions
  FROM `etsy-data-warehouse-dev.tsebastian.user_90d_features`
),
user_90d_stats AS (
  SELECT
    'feed_clicks_90d' AS metric,
    MIN(feed_clicks_90d)                              AS min,
     ANY_VALUE(q_feed_clicks)[OFFSET(1)]                          AS q1,
     ANY_VALUE(q_feed_clicks)[OFFSET(2)]                          AS median,
     ANY_VALUE(q_feed_clicks)[OFFSET(3)]                          AS q3,
    MAX(feed_clicks_90d)                              AS max,
    AVG(feed_clicks_90d)                              AS mean
  FROM `etsy-data-warehouse-dev.tsebastian.user_90d_features`, metric_qs

  UNION ALL
  SELECT
    'feed_favs_90d',
    MIN(feed_favs_90d),
     ANY_VALUE(q_feed_favs)[OFFSET(1)],
     ANY_VALUE(q_feed_favs)[OFFSET(2)],
     ANY_VALUE(q_feed_favs)[OFFSET(3)],
    MAX(feed_favs_90d),
    AVG(feed_favs_90d)
  FROM `etsy-data-warehouse-dev.tsebastian.user_90d_features`, metric_qs

  UNION ALL
  SELECT
    'feed_recs_delivered_90d',
    MIN(feed_recs_delivered_90d),
     ANY_VALUE(q_feed_recs_delivered)[OFFSET(1)],
     ANY_VALUE(q_feed_recs_delivered)[OFFSET(2)],
     ANY_VALUE(q_feed_recs_delivered)[OFFSET(3)],
    MAX(feed_recs_delivered_90d),
    AVG(feed_recs_delivered_90d)
  FROM `etsy-data-warehouse-dev.tsebastian.user_90d_features`, metric_qs

  UNION ALL
  SELECT
    'feed_purchases_90d',
    MIN(feed_purchases_90d),
     ANY_VALUE(q_feed_purchases)[OFFSET(1)],
     ANY_VALUE(q_feed_purchases)[OFFSET(2)],
     ANY_VALUE(q_feed_purchases)[OFFSET(3)],
    MAX(feed_purchases_90d),
    AVG(feed_purchases_90d)
  FROM `etsy-data-warehouse-dev.tsebastian.user_90d_features`, metric_qs

  UNION ALL
  SELECT
    'searches_90d',
    MIN(searches_90d),
     ANY_VALUE(q_searches)[OFFSET(1)],
     ANY_VALUE(q_searches)[OFFSET(2)],
     ANY_VALUE(q_searches)[OFFSET(3)],
    MAX(searches_90d),
    AVG(searches_90d)
  FROM `etsy-data-warehouse-dev.tsebastian.user_90d_features`, metric_qs

  UNION ALL
  SELECT
    'feed_gms_90d',
    MIN(feed_gms_90d),
     ANY_VALUE(q_feed_gms)[OFFSET(1)],
     ANY_VALUE(q_feed_gms)[OFFSET(2)],
     ANY_VALUE(q_feed_gms)[OFFSET(3)],
    MAX(feed_gms_90d),
    AVG(feed_gms_90d)
  FROM `etsy-data-warehouse-dev.tsebastian.user_90d_features`, metric_qs

    UNION ALL
  SELECT
    'interactions_90d',
    MIN(interactions_90d),
     ANY_VALUE(q_interactions)[OFFSET(1)],
     ANY_VALUE(q_interactions)[OFFSET(2)],
     ANY_VALUE(q_interactions)[OFFSET(3)],
    MAX(interactions_90d),
    AVG(interactions_90d)
  FROM `etsy-data-warehouse-dev.tsebastian.user_90d_features`, metric_qs
)

SELECT *
FROM user_90d_stats
ORDER BY metric;
 
--------------------------------------------------------------------------------
-- 1d. Distribution stats across users (EXCLUDING 0s)
--------------------------------------------------------------------------------

WITH aggregated_stats AS (
  SELECT
    -- Feed Clicks (Cast to FLOAT64 to match GMS type)
    MIN(CAST(NULLIF(feed_clicks_90d, 0) AS FLOAT64)) AS min_feed_clicks_90d,
    APPROX_QUANTILES(CAST(NULLIF(feed_clicks_90d, 0) AS FLOAT64), 4) AS q_feed_clicks_90d,
    MAX(CAST(NULLIF(feed_clicks_90d, 0) AS FLOAT64)) AS max_feed_clicks_90d,
    AVG(CAST(NULLIF(feed_clicks_90d, 0) AS FLOAT64)) AS mean_feed_clicks_90d,

    -- Feed Favs
    MIN(CAST(NULLIF(feed_favs_90d, 0) AS FLOAT64)) AS min_feed_favs_90d,
    APPROX_QUANTILES(CAST(NULLIF(feed_favs_90d, 0) AS FLOAT64), 4) AS q_feed_favs_90d,
    MAX(CAST(NULLIF(feed_favs_90d, 0) AS FLOAT64)) AS max_feed_favs_90d,
    AVG(CAST(NULLIF(feed_favs_90d, 0) AS FLOAT64)) AS mean_feed_favs_90d,

    -- Feed Delivered
    MIN(CAST(NULLIF(feed_recs_delivered_90d, 0) AS FLOAT64)) AS min_feed_recs_delivered_90d,
    APPROX_QUANTILES(CAST(NULLIF(feed_recs_delivered_90d, 0) AS FLOAT64), 4) AS q_feed_recs_delivered_90d,
    MAX(CAST(NULLIF(feed_recs_delivered_90d, 0) AS FLOAT64)) AS max_feed_recs_delivered_90d,
    AVG(CAST(NULLIF(feed_recs_delivered_90d, 0) AS FLOAT64)) AS mean_feed_recs_delivered_90d,

    -- Feed Purchases
    MIN(CAST(NULLIF(feed_purchases_90d, 0) AS FLOAT64)) AS min_feed_purchases_90d,
    APPROX_QUANTILES(CAST(NULLIF(feed_purchases_90d, 0) AS FLOAT64), 4) AS q_feed_purchases_90d,
    MAX(CAST(NULLIF(feed_purchases_90d, 0) AS FLOAT64)) AS max_feed_purchases_90d,
    AVG(CAST(NULLIF(feed_purchases_90d, 0) AS FLOAT64)) AS mean_feed_purchases_90d,

    -- Searches
    MIN(CAST(NULLIF(searches_90d, 0) AS FLOAT64)) AS min_searches_90d,
    APPROX_QUANTILES(CAST(NULLIF(searches_90d, 0) AS FLOAT64), 4) AS q_searches_90d,
    MAX(CAST(NULLIF(searches_90d, 0) AS FLOAT64)) AS max_searches_90d,
    AVG(CAST(NULLIF(searches_90d, 0) AS FLOAT64)) AS mean_searches_90d,

    -- Feed GMS (Already likely FLOAT/NUMERIC, but cast to FLOAT64 for consistency)
    MIN(CAST(NULLIF(feed_gms_90d, 0) AS FLOAT64)) AS min_feed_gms_90d,
    APPROX_QUANTILES(CAST(NULLIF(feed_gms_90d, 0) AS FLOAT64), 4) AS q_feed_gms_90d,
    MAX(CAST(NULLIF(feed_gms_90d, 0) AS FLOAT64)) AS max_feed_gms_90d,
    AVG(CAST(NULLIF(feed_gms_90d, 0) AS FLOAT64)) AS mean_feed_gms_90d,

    -- Total Interactions
    MIN(CAST(NULLIF(interactions_90d, 0) AS FLOAT64)) AS min_interactions_90d,
    APPROX_QUANTILES(CAST(NULLIF(interactions_90d, 0) AS FLOAT64), 4) AS q_interactions_90d,
    MAX(CAST(NULLIF(interactions_90d, 0) AS FLOAT64)) AS max_interactions_90d,
    AVG(CAST(NULLIF(interactions_90d, 0) AS FLOAT64)) AS mean_interactions_90d
  FROM `etsy-data-warehouse-dev.tsebastian.user_90d_features`
)
SELECT
  metric,
  min,
  q[OFFSET(1)] AS q1,
  q[OFFSET(2)] AS median,
  q[OFFSET(3)] AS q3,
  max,
  mean
FROM aggregated_stats,
UNNEST([
  STRUCT('feed_clicks_90d' AS metric, min_feed_clicks_90d AS min, q_feed_clicks_90d AS q, max_feed_clicks_90d AS max, mean_feed_clicks_90d AS mean),
  STRUCT('feed_favs_90d', min_feed_favs_90d, q_feed_favs_90d, max_feed_favs_90d, mean_feed_favs_90d),
  STRUCT('feed_recs_delivered_90d', min_feed_recs_delivered_90d, q_feed_recs_delivered_90d, max_feed_recs_delivered_90d, mean_feed_recs_delivered_90d),
  STRUCT('feed_purchases_90d', min_feed_purchases_90d, q_feed_purchases_90d, max_feed_purchases_90d, mean_feed_purchases_90d),
  STRUCT('searches_90d', min_searches_90d, q_searches_90d, max_searches_90d, mean_searches_90d),
  STRUCT('feed_gms_90d', min_feed_gms_90d, q_feed_gms_90d, max_feed_gms_90d, mean_feed_gms_90d),
  STRUCT('interactions_90d', min_interactions_90d, q_interactions_90d, max_interactions_90d, mean_interactions_90d)
])
ORDER BY metric;


--------- metrics by engagement amount ---------

  
DECLARE start_date DATE DEFAULT '2025-12-31';
DECLARE end_date   DATE DEFAULT '2026-01-29';
 

WITH
-- 1) Users bucketed by 90‑day prior engagement
user_buckets AS (
  SELECT
    f.user_id,
    f.feed_clicks_90d,
    f.feed_favs_90d,
    f.feed_purchases_90d,
    f.searches_90d,
    f.interactions_90d,

    CASE
      WHEN COALESCE(f.interactions_90d, 0) = 0  THEN '0'
      WHEN f.interactions_90d BETWEEN 1  AND 4   THEN '1-4'
      WHEN f.interactions_90d BETWEEN 5  AND 11  THEN '5-11'
      WHEN f.interactions_90d BETWEEN 12 AND 30  THEN '12-30'
      WHEN f.interactions_90d >= 31              THEN '31+'
      ELSE 'other'
    END AS engagement_bucket
  FROM `etsy-data-warehouse-dev.tsebastian.user_90d_features` f
),

-- 2) All MULE feed delivered listings in the window, with user_id joined in
mule_feed_rows AS (
  SELECT
    v.user_id,
    rdl.visit_id,
    rdl.seen,
    rdl.clicked,
    rdl.favorited_directly_from_feed,
    COALESCE(rdl.transactions_gms, 0.0)      AS transactions_gms,
    COALESCE(rdl.transactions_quantity, 0.0) AS transactions_quantity
  FROM `etsy-data-warehouse-prod.rollups.recsys_delivered_listings` rdl
  JOIN `etsy-data-warehouse-prod.weblog.visits` v
    ON rdl.visit_id = v.visit_id
  WHERE
    rdl._date BETWEEN start_date AND end_date
    AND rdl.module_placement = 'boe_homescreen_feed'
    AND rdl.candidate_set LIKE '%MULE%'   -- MULE content on feed
    and v._date >= start_date
),

-- 3) Aggregate MULE feed stats per user (only users who actually saw MULE feed in window)
mule_user_stats AS (
  SELECT
    user_id,
    SUM(CASE WHEN seen = 1 THEN 1 ELSE 0 END) AS feed_impressions,
    SUM(CASE WHEN clicked = 1 THEN 1 ELSE 0 END) AS feed_clicks,
    sum(CASE WHEN favorited_directly_from_feed = 1 THEN 1 ELSE 0 END) AS feed_favorites,
    SUM(transactions_gms)                       AS transactions_gms,
    SUM(transactions_quantity)                  AS transactions_quantity
  FROM mule_feed_rows
  GROUP BY user_id
),

-- 4) Join buckets with MULE stats, so we only keep bucketed users
--    who had at least one MULE feed exposure in the window
bucketed_mule_users AS (
  SELECT
    b.engagement_bucket,
    b.user_id,
    m.feed_impressions,
    m.feed_clicks,
    m.feed_favorites,
    m.transactions_gms,
    m.transactions_quantity
  FROM user_buckets b
  JOIN mule_user_stats m
    ON b.user_id = m.user_id
  WHERE
    b.engagement_bucket IN ('0','1-4','5-11','12-30','31+')
)

-- 5) Final aggregation by bucket
SELECT
  engagement_bucket,
  COUNT(DISTINCT user_id)                                          AS users,
  SUM(transactions_gms)                                            AS total_transactions_gms,
  SUM(transactions_quantity)                                       AS total_transactions_quantity,
  SAFE_DIVIDE(SUM(feed_clicks), SUM(feed_impressions))             AS feed_listing_ctr,
  SAFE_DIVIDE(SUM(feed_favorites), SUM(feed_impressions))             AS feed_favorite_rate
FROM bucketed_mule_users
GROUP BY engagement_bucket
ORDER BY
  CASE engagement_bucket
    WHEN '0'     THEN 1
    WHEN '1-4'   THEN 2
    WHEN '5-11'  THEN 3
    WHEN '12-30' THEN 4
    WHEN '31+'   THEN 5
    ELSE 99
    
  END;


  --------- Signed in vs out users ---------
-- % of signed-in vs signed-out “users” on BOE home feed in the last 30 days
-- Unit = user_id when signed in, browser_id when signed out

WITH boe_feed_impressions AS (
  SELECT
    v._date,
    v.visit_id,
    IF(v.user_id IS NULL OR v.user_id = 0, "signed_out", "signed_in") AS auth_status

  FROM `etsy-data-warehouse-prod.rollups.recsys_delivered_listings` AS rdl
  JOIN `etsy-data-warehouse-prod.visit_mart.visits` AS v
    USING (visit_id)
  WHERE

    rdl._date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
                 AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)

    and v._date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
                 AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)

    AND rdl.module_placement = 'boe_homescreen_feed'

    AND rdl.seen = 1

),

per_auth_status AS (
  SELECT
    auth_status,
    COUNT(DISTINCT visit_id) AS visits
  FROM boe_feed_impressions
  GROUP BY auth_status
)

SELECT
  auth_status,
  visits,
  ROUND(100 * visits / SUM(visits) OVER (),2 ) AS pct_visits
FROM per_auth_status
ORDER BY auth_status;
