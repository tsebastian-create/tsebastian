---- Listing classification

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.tsebastian.listing_classification`
AS (
  SELECT
    listing_id,
    COALESCE(MAX(top_click_query), MAX(normalized_occasion)) AS top_click_query
  FROM (
    -- top_click_query from Feature Bank
    SELECT
      CAST(key AS INT64) AS listing_id,
      topQuery_clickTopQuery.list[SAFE_OFFSET(0)].element AS top_click_query,
      NULL AS normalized_occasion
    FROM `etsy-ml-systems-prod.feature_bank_v2.listing_feature_bank_most_recent`
    WHERE topQuery_clickTopQuery.list IS NOT NULL

    UNION ALL

    -- normalized_occasion[0] from listing entities
    SELECT
      listing_id,
      NULL AS top_click_query,
      normalized_occasion[SAFE_OFFSET(0)] AS normalized_occasion
    FROM `etsy-data-warehouse-prod.inventory_ml.listing_entities_normalized_latest`
  )
  GROUP BY listing_id
);


-- 1 month window;
DECLARE start_date DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 31 DAY);
DECLARE end_date   DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.tsebastian.mule_candidates_boe_feed`
PARTITION BY _date AS
WITH mule_requests AS (
  SELECT
    TIMESTAMP(timestamp) AS event_ts,
    DATE(timestamp)      AS _date,
    uuid                 AS mmx_request_uuid,
    rankingContext,
    rankedCandidates
  FROM `etsy-recsys-ml-prod.kafka_sink_recsys.recsys-mmx-ranking-response`
  WHERE DATE(timestamp) BETWEEN start_date AND end_date
    -- filter  MULE 
    AND rankedCandidates LIKE '%MULE%'
      AND MOD(ABS(FARM_FINGERPRINT(uuid)), 10) = 0  -- ~10% sample
) 
  SELECT
    _date,
    event_ts,
    mmx_request_uuid,
    JSON_EXTRACT(rankingContext, '$.userId')                          AS user_id, 
    SAFE_CAST(JSON_VALUE(cand, '$.listingId') AS INT64)              AS listing_id,
    JSON_VALUE(cand, '$.candidate_source')                           AS candidate_set,
    SAFE_CAST(JSON_VALUE(cand, '$.score') AS FLOAT64)                AS candidate_score
  FROM mule_requests
  CROSS JOIN UNNEST(JSON_EXTRACT_ARRAY(rankedCandidates)) AS cand
  WHERE JSON_VALUE(cand, '$.candidate_source') LIKE '%MULE%'  -- true pre‑rank MULE membership
;

----- Backfill for extra days

DECLARE MAX_DATE DATE DEFAULT (SELECT MAX(_date) FROM `etsy-data-warehouse-dev.tsebastian.mule_candidates_boe_feed`);

INSERT INTO `etsy-data-warehouse-dev.tsebastian.mule_candidates_boe_feed` (
  _date,
  event_ts,
  mmx_request_uuid,
  user_id,
  listing_id,
  candidate_set,
  candidate_score,
  top_click_query 
)
WITH mule_requests AS (
  SELECT
    TIMESTAMP(timestamp) AS event_ts,
    DATE(timestamp)      AS _date,
    uuid                 AS mmx_request_uuid,
    rankingContext,
    rankedCandidates
  FROM `etsy-recsys-ml-prod.kafka_sink_recsys.recsys-mmx-ranking-response`
  WHERE DATE(timestamp) BETWEEN MAX_DATE + 1 AND CURRENT_DATE() - 1
    AND rankedCandidates LIKE '%MULE%'
    AND MOD(ABS(FARM_FINGERPRINT(uuid)), 10) = 0
),
unnested_candidates AS (
  SELECT
    _date,
    event_ts,
    mmx_request_uuid,
    JSON_VALUE(rankingContext, '$.userId')              AS user_id,
    SAFE_CAST(JSON_VALUE(cand, '$.listingId') AS INT64) AS listing_id,
    JSON_VALUE(cand, '$.candidate_source')             AS candidate_set,
    SAFE_CAST(JSON_VALUE(cand, '$.score') AS FLOAT64)  AS candidate_score
  FROM mule_requests
  CROSS JOIN UNNEST(JSON_EXTRACT_ARRAY(rankedCandidates)) AS cand
  WHERE JSON_VALUE(cand, '$.candidate_source') LIKE '%MULE%'
)
SELECT
  t.*,
  src.top_click_query
FROM unnested_candidates AS t
LEFT JOIN `etsy-data-warehouse-dev.tsebastian.listing_classification` AS src
  ON t.listing_id = src.listing_id;

-- select _date, count(*) from  `etsy-data-warehouse-dev.tsebastian.mule_candidates_boe_feed`   group by 1 order by 1


----- Recs listings seen vs clicked

DECLARE start_date DATE DEFAULT '2025-12-01';
DECLARE end_date   DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.tsebastian.mule_feed_delivered_boe`
PARTITION BY _date AS
WITH feed_delivered AS (
  SELECT
    _date,
    delivery_timestamp,
    visit_id,
    rec_listing_id AS listing_id,
    candidate_set,
    clicked,
    seen,
    purchased_after_unified_impression AS purchased_after_view
  FROM `etsy-data-warehouse-prod.rollups.boe_home_feed_delivered_listings`
  WHERE _date BETWEEN start_date AND end_date
    -- keep if you only care about MULE; drop if you want all feed recs
    AND candidate_set LIKE '%MULE%'
    and module_placement = 'boe_homescreen_feed'
) 

SELECT
  f._date,
  f.delivery_timestamp,
  f.visit_id,
  f.listing_id,
  f.candidate_set,
  f.clicked,
  f.purchased_after_view,
  seen,
  l.top_click_query  -- label: query if available, else occasion
FROM feed_delivered f
LEFT JOIN  `etsy-data-warehouse-dev.tsebastian.listing_classification` l
  USING (listing_id);



----- Backfill for extra days
DECLARE MAX_DATE DATE DEFAULT (select max(_date) from `etsy-data-warehouse-dev.tsebastian.mule_feed_delivered_boe`) ;
INSERT INTO `etsy-data-warehouse-dev.tsebastian.mule_feed_delivered_boe` (
    _date,
    delivery_timestamp,
    visit_id,
    listing_id,
    candidate_set,
    clicked,
    seen,
    purchased_after_view,
    top_click_query 
)
WITH feed_delivered AS (
  SELECT
    _date,
    delivery_timestamp,
    visit_id,
    rec_listing_id AS listing_id,
    candidate_set,
    clicked,
    seen,
    purchased_after_unified_impression AS purchased_after_view
  FROM `etsy-data-warehouse-prod.rollups.boe_home_feed_delivered_listings`
  WHERE _date  between MAX_DATE+1 AND CURRENT_DATE()-1

    AND candidate_set LIKE '%MULE%'
    and module_placement = 'boe_homescreen_feed'
)
SELECT
  f._date,
  f.delivery_timestamp,
  f.visit_id,
  f.listing_id,
  f.candidate_set,
  f.clicked,
  seen,
  f.purchased_after_view,
  l.top_click_query  

FROM feed_delivered f
LEFT JOIN  `etsy-data-warehouse-dev.tsebastian.listing_classification` l
  USING (listing_id);

-- select _date, count(*) from `etsy-data-warehouse-dev.tsebastian.mule_feed_delivered_boe`  group by 1 order by 1


-------- -------- -------- -------- 
-------- Q&A. -------- -------- -------- 
-------- -------- -------- -------- 


--Q1 What % of impressions, % of clicks, and CTR for MULE candidate sets, on BOE feed day by day from 1 month ago to today is from Holiday content?
-- Q2 What % of impressions, % of clicks, and CTR  in MULE candidate sets, on BOE feed day by day from 1 month ago to today is from Valentine’s day content?

DECLARE start_date DATE DEFAULT '2026-01-01';
DECLARE end_date   DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
WITH base AS (
  SELECT
    _date,
    clicked,
    purchased_after_view,
    seen,
    -- Define the logic once here
    REGEXP_CONTAINS(LOWER(top_click_query), r'x.mas|holiday|christmas|hann?ukah|new year|kwanza|wonderland|winter|end of year|yule|santa|white.elephant|boxing.day') AS is_holiday
    -- REGEXP_CONTAINS(LOWER(top_click_query), r'valentine') AS is_holiday
  FROM `etsy-data-warehouse-dev.tsebastian.mule_feed_delivered_boe`
  where _date between start_date and end_date
),
searches as (
SELECT
   _date,
   count(*) as total_searches,
   count(case when REGEXP_CONTAINS(LOWER(query), 
  --  r'x.mas|holiday|christmas|hann?ukah|new year|kwanza|wonderland|winter|end of year|yule|santa|white.elephant|boxing.day') 
      r'valentine')
   then 1 end) as holiday_searches,
 
  FROM `etsy-data-warehouse-prod.search.query_sessions_new`
  WHERE _date Between start_date and end_date 
    -- WHERE _date Between start_date and '2026-02-18' 
  and platform = 'boe'
  group by 1
  ) ,
purchases_boe AS (
  SELECT
    t.date AS _date,
    COUNT(*) AS total_purchases_boe,
    COUNTIF(
      REGEXP_CONTAINS(
        LOWER(ll.top_click_query),
        -- r'x.mas|holiday|christmas|hann?ukah|new year|kwanza|wonderland|winter|end of year|yule|santa|white.elephant|boxing.day'
        r'valentine' )
        ) AS holiday_purchases_boe
  FROM `etsy-data-warehouse-prod.transaction_mart.all_transactions` t
  JOIN `etsy-data-warehouse-prod.transaction_mart.transactions_visits` tv     USING (transaction_id)
  LEFT JOIN  `etsy-data-warehouse-dev.tsebastian.listing_classification` ll     ON t.listing_id = ll.listing_id

  WHERE t.date BETWEEN start_date AND end_date and tv._date BETWEEN start_date AND end_date
  AND tv.platform_app = 'boe'   -- BOE only

  GROUP BY 1
)

SELECT
  a._date,
  -- all impressions/clicks etc
  COUNT(*) AS total_delivered,
  sum(seen) AS total_impressions,
  SUM(clicked) AS total_clicks,
  sum(purchased_after_view) as total_feed_purchases,
  max(p.total_purchases_boe) AS total_boe_purchases,
  max(b.total_searches) as total_Searches,

   -- Holiday impressions/ clicks / purchases /searches
  SUM(IF(is_holiday, seen, 0)) AS holiday_impressions,
  SUM(IF(is_holiday, clicked, 0)) AS holiday_clicks,
  SUM(IF(is_holiday, purchased_after_view, 0)) AS holiday_feed_purchases,
  max(p.holiday_purchases_boe) AS holiday_boe_purchases,
  max(b.holiday_searches) as holiday_searches,
  
  -- Percentage of IMPRESSIONS/ clicks / purchases /searches
  SUM(IF(is_holiday, seen, 0))/SUM(seen) AS pct_impressions_holiday,
  SAFE_DIVIDE(SUM(IF(is_holiday, clicked, 0)), SUM(clicked)) AS pct_clicks_holiday,
  SAFE_DIVIDE(SUM(IF(is_holiday, purchased_after_view, 0)), SUM(purchased_after_view)) AS pct_feed_purchases_holiday,
  SAFE_DIVIDE(max(p.holiday_purchases_boe), max(p.total_purchases_boe)) AS pct_boe_purchases_holiday,
  SAFE_DIVIDE(max(b.holiday_searches), max(b.total_searches)) AS pct_searches_holiday,

  --CTR
  SAFE_DIVIDE(SUM(IF(is_holiday, clicked, 0)), SUM(IF(is_holiday, seen, 0))) AS holiday_ctr,
  SAFE_DIVIDE(SUM(IF(is_holiday, 0, clicked)), SUM(IF(is_holiday, 0, seen))) AS non_holiday_ctr,
  -- CVR
  SAFE_DIVIDE(SUM(IF(is_holiday, purchased_after_view, 0)), SUM(IF(is_holiday, seen, 0))) AS holiday_cvr,
  SAFE_DIVIDE(SUM(IF(is_holiday, 0, purchased_after_view)), SUM(IF(is_holiday, 0, seen))) AS non_holiday_cvr,

FROM base a
left join searches b on a._date = b._date
LEFT JOIN purchases_boe p ON a._date = p._date
GROUP BY 1
ORDER BY 1;




-- Q3 What % of requests for MULE candidate sets day by day from 1 month ago to today is from Holiday content?
-- Q4 What % of requests for MULE candidate sets day by day from 1 month ago to today is from Valentine’s day content?

-- SELECT _date,
--      COUNT(CASE WHEN LOWER(top_click_query)

--     --  like '%valentine%' 
--     LIKE ANY (
--         '%x%mas%', '%holiday%', '%christmas%', '%hannukah%', '%new year%',
--         '%kwanza%', '%wonderland%', '%winter%', '%end of year%', '%yule%',
--         '%santa%', '%white%elephant%', '%chanukah%', '%boxing%day%')
    
--      THEN 1 END)/count(*) as reqs_pct_holiday, 

--     --  COUNT(DISTINCT CASE WHEN top_click_query like '%valentine%' THEN user_id END)/count(distinct user_id) as users_pct_valentine,
--     --  COUNT(DISTINCT CASE WHEN top_click_query like '%valentine%' THEN listing_id END)/count(distinct listing_id) as listings_pct_valentine,

-- FROM `etsy-data-warehouse-dev.tsebastian.mule_candidates_boe_feed` 
-- GROUP BY _date
-- ORDER BY 1;


--------- funnel
DECLARE start_date DATE DEFAULT '2026-01-15';
DECLARE end_date   DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

WITH 
-- 1) Process Candidates 
candidates AS (
  SELECT
    _date,
    COUNT(*) AS total_candidates,
    COUNTIF(REGEXP_CONTAINS(LOWER(top_click_query), r'x.mas|holiday|christmas|hann?ukah|new year|kwanza|wonderland|winter|end of year|yule|santa|white.elephant|boxing.day')) AS valentines_candidates,
    
    -- COUNTIF(REGEXP_CONTAINS(LOWER(top_click_query), r'valentine')) AS valentines_candidates
  FROM `etsy-data-warehouse-dev.tsebastian.mule_candidates_boe_feed`
  WHERE _date BETWEEN start_date AND end_date
  GROUP BY 1
),

-- 2) Process all "Delivered" metrics 
delivered_metrics AS (
  SELECT
    _date,
    COUNT(*) AS total_delivered,
    COUNTIF(is_holiday) AS valentines_delivered,
    SUM(seen) AS total_seen,
    SUM(IF(is_holiday, seen, 0)) AS valentines_seen,
    SUM(clicked) AS total_clicked,
    SUM(IF(is_holiday, clicked, 0)) AS valentines_clicked,
    SUM(purchased_after_view) AS total_purchases,
    SUM(IF(is_holiday, purchased_after_view, 0)) AS valentines_purchases
  FROM (
    SELECT
      _date,
      seen,
      clicked,
      purchased_after_view,
      REGEXP_CONTAINS(LOWER(top_click_query), r'valentine' ) AS is_holiday
    FROM `etsy-data-warehouse-dev.tsebastian.mule_feed_delivered_boe`
    WHERE _date BETWEEN start_date AND end_date
  )
  GROUP BY 1
)

-- 3) Join the two aggregated sets
SELECT
  c._date,
  SAFE_DIVIDE(c.valentines_candidates, c.total_candidates) AS share_candidates_valentines,
  SAFE_DIVIDE(m.valentines_delivered, m.total_delivered) AS share_delivered_valentines,
  SAFE_DIVIDE(m.valentines_seen, m.total_seen) AS share_seen_valentines,
  SAFE_DIVIDE(m.valentines_clicked, m.total_clicked) AS share_clicked_valentines,
  SAFE_DIVIDE(m.valentines_purchases, m.total_purchases) AS share_of_purchases
FROM candidates c
JOIN delivered_metrics m USING (_date)
ORDER BY _date;


------------------------------------------------
-------- USER BASED segmenatation
------------------------------------------------


DECLARE start_date DATE DEFAULT '2026-01-01';
DECLARE end_date DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
DECLARE lookback_window INT64 DEFAULT 90;

-- Switch this between holiday and Valentine's regex as needed
-- e.g. r'x.mas|holiday|christmas|hann?ukah|new year|kwanza|wonderland|winter|end of year|yule|santa|white.elephant|boxing.day'
DECLARE holiday_pattern STRING DEFAULT r'valentine';

-- 1) BOE feed deliveries 
WITH user_mapping AS (
  SELECT DISTINCT visit_id, user_id
  FROM `etsy-data-warehouse-prod.weblog.visits`
  WHERE _date BETWEEN start_date AND end_date
    AND platform = 'boe'
    AND user_id IS NOT NULL
),

feed_delivered AS (
  SELECT
    a._date,
    a.delivery_timestamp,
    a.visit_id,
    b.user_id,
    a.rec_listing_id AS listing_id,
    a.candidate_set,
    a.clicked,
    a.seen,
    a.purchased_after_unified_impression AS purchased_after_view
  FROM `etsy-data-warehouse-prod.rollups.boe_home_feed_delivered_listings` a
  JOIN user_mapping b
    ON a.visit_id = b.visit_id
  WHERE a._date BETWEEN start_date AND end_date
    AND a.candidate_set LIKE '%MULE%'
    AND a.module_placement = 'boe_homescreen_feed'
),

feed_with_labels AS (
  SELECT
    f.*,
    REGEXP_CONTAINS(LOWER(l.top_click_query), holiday_pattern) AS is_holiday
  FROM feed_delivered f
  LEFT JOIN `etsy-data-warehouse-dev.tsebastian.listing_classification` l
    USING (listing_id)
),

-- 2) Distinct users seen on feed by day
feed_users AS (
  SELECT DISTINCT _date, user_id
  FROM feed_with_labels
  WHERE user_id IS NOT NULL
),

-- 3) Per-(date, user) 90d activity + holiday/Valentine activity flags  
user_activity AS (
  SELECT
    fu._date,
    fu.user_id,

    -- Any activity in last 90d 
    EXISTS (
      SELECT 1
      FROM `etsy-data-warehouse-prod.visit_mart.visits` v
      WHERE v.user_id = fu.user_id
        AND v._date BETWEEN DATE_SUB(fu._date, INTERVAL lookback_window DAY)
                        AND DATE_SUB(fu._date, INTERVAL 1 DAY)
        AND (
          v.engaged_visit_5mins = 1
          OR v.cart_adds > 0
          OR v.fav_item_count > 0
          OR v.fav_shop_count > 0
          OR v.orders > 0
        )
    ) AS has_any_activity_90d,

    -- Holiday search activity in last 90d  
    EXISTS (
      SELECT 1
      FROM `etsy-data-warehouse-prod.search.query_sessions_new` q
      JOIN `etsy-data-warehouse-prod.weblog.visits` v
        ON q.visit_id = v.visit_id
      WHERE v.user_id = fu.user_id
        and q._date >= '2025-09-01'
        AND v._date >= '2025-09-01'
        AND q._date BETWEEN DATE_SUB(fu._date, INTERVAL lookback_window DAY)
                        AND DATE_SUB(fu._date, INTERVAL 1 DAY)
        AND V._date BETWEEN DATE_SUB(fu._date, INTERVAL lookback_window DAY)
                        AND DATE_SUB(fu._date, INTERVAL 1 DAY)
        AND REGEXP_CONTAINS(LOWER(q.query), holiday_pattern)
    ) AS has_holiday_search_90d,

    -- Holiday listing views/clicks/etc in last 90d 
    EXISTS (
      SELECT 1
      FROM `etsy-data-warehouse-prod.analytics.listing_views` lv
      JOIN `etsy-data-warehouse-dev.tsebastian.listing_classification` l
        ON lv.listing_id = l.listing_id
      JOIN `etsy-data-warehouse-prod.weblog.visits` v_lv   ON SPLIT(lv.visit_id, ".")[SAFE_OFFSET(0)] = v_lv.browser_id AND lv._date = v_lv._date

      WHERE v_lv.user_id = fu.user_id
        AND lv._date >= '2025-09-01'
        AND v_lv._date >= '2025-09-01'
        AND lv._date BETWEEN DATE_SUB(fu._date, INTERVAL lookback_window DAY)
                        AND DATE_SUB(fu._date, INTERVAL 1 DAY)

        AND REGEXP_CONTAINS(LOWER(l.top_click_query), holiday_pattern)
        AND v_lv._date BETWEEN DATE_SUB(fu._date, INTERVAL lookback_window DAY)
                        AND DATE_SUB(fu._date, INTERVAL 1 DAY)

    ) AS has_holiday_listing_engagement_90d,

    -- Holiday purchases in last 90d 
    EXISTS (
      SELECT 1
      FROM `etsy-data-warehouse-prod.user_mart.user_mapping` um
      JOIN `etsy-data-warehouse-prod.transaction_mart.all_transactions` t
        ON t.mapped_user_id = um.mapped_user_id
      JOIN `etsy-data-warehouse-dev.tsebastian.listing_classification` l
        ON t.listing_id = l.listing_id
      WHERE um.user_id = fu.user_id
        AND t.date BETWEEN DATE_SUB(fu._date, INTERVAL lookback_window DAY)
                       AND DATE_SUB(fu._date, INTERVAL 1 DAY)
        AND REGEXP_CONTAINS(LOWER(l.top_click_query), holiday_pattern)
    ) AS has_holiday_purchase_90d

  FROM feed_users fu
),

-- 4) Segment users into 3 groups per day
segmented_users AS (
  SELECT
    _date,
    user_id,
    CASE
      WHEN NOT has_any_activity_90d
        THEN 'seg1_no_activity_90d'
      WHEN
        (
          has_holiday_search_90d
          OR has_holiday_listing_engagement_90d
          OR has_holiday_purchase_90d
        )
        THEN 'seg2_holiday_activity_90d'
      ELSE 'seg3_other_activity_90d'
    END AS user_segment
  FROM user_activity
),

-- 5) Feed-level base table, joined to user segment
base AS (
  SELECT
    f._date,
    s.user_segment,
    f.clicked,
    f.purchased_after_view,
    f.seen,
    f.is_holiday
  FROM feed_with_labels f
  JOIN segmented_users s
    USING (_date, user_id)
),

-- 6) BOE searches by segment
searches AS (
  SELECT
    q._date,
    s.user_segment,
    COUNT(*) AS total_searches,
    COUNTIF(REGEXP_CONTAINS(LOWER(q.query), holiday_pattern)) AS holiday_searches
  FROM `etsy-data-warehouse-prod.search.query_sessions_new` q
  LEFT JOIN user_mapping b
    ON q.visit_id = b.visit_id
  JOIN segmented_users s
    ON q._date = s._date
   AND b.user_id = s.user_id -- adjust if field is shopper_id
  WHERE q._date BETWEEN start_date AND end_date
  and q._date >= '2025-09-01'
    AND q.platform = 'boe'
  GROUP BY 1, 2
),

-- 7) BOE purchases by segment
purchases_boe AS (
  SELECT
    t.date AS _date,
    s.user_segment,
    COUNT(*) AS total_purchases_boe,
    COUNTIF(
      REGEXP_CONTAINS(LOWER(ll.top_click_query), holiday_pattern)
    ) AS holiday_purchases_boe
  FROM `etsy-data-warehouse-prod.transaction_mart.all_transactions` t
  JOIN `etsy-data-warehouse-prod.transaction_mart.transactions_visits` tv
    USING (transaction_id)
  JOIN segmented_users s
    ON tv._date = s._date
   AND tv.user_id = s.user_id -- adjust if needed
  LEFT JOIN `etsy-data-warehouse-dev.tsebastian.listing_classification` ll
    ON t.listing_id = ll.listing_id
  WHERE t.date BETWEEN start_date AND end_date
    AND tv._date BETWEEN start_date AND end_date
    AND tv.platform_app = 'boe'
  GROUP BY 1, 2
)

-- 8) Final per-day, per-segment metrics
SELECT
  a._date,
  a.user_segment,

  -- share-of metrics
  SAFE_DIVIDE(SUM(IF(is_holiday, seen, 0)), SUM(seen)) AS pct_impressions_holiday,
  SAFE_DIVIDE(SUM(IF(is_holiday, clicked, 0)), SUM(clicked)) AS pct_clicks_holiday,
  SAFE_DIVIDE(SUM(IF(is_holiday, purchased_after_view, 0)), SUM(purchased_after_view)) AS pct_feed_purchases_holiday,
  SAFE_DIVIDE(MAX(p.holiday_purchases_boe), MAX(p.total_purchases_boe)) AS pct_boe_purchases_holiday,
  SAFE_DIVIDE(MAX(b.holiday_searches), MAX(b.total_searches)) AS pct_searches_holiday,

  -- feed CTR
  SAFE_DIVIDE(SUM(IF(is_holiday, clicked, 0)), SUM(IF(is_holiday, seen, 0))) AS holiday_ctr,
  SAFE_DIVIDE(SUM(IF(NOT is_holiday, clicked, 0)), SUM(IF(NOT is_holiday, seen, 0))) AS non_holiday_ctr,

  -- feed CVR (purchases_after_view / impressions)
  SAFE_DIVIDE(SUM(IF(is_holiday, purchased_after_view, 0)), SUM(IF(is_holiday, seen, 0))) AS holiday_cvr,
  SAFE_DIVIDE(SUM(IF(NOT is_holiday, purchased_after_view, 0)), SUM(IF(NOT is_holiday, seen, 0))) AS non_holiday_cvr

FROM base a
LEFT JOIN searches b
  ON a._date = b._date
 AND a.user_segment = b.user_segment
LEFT JOIN purchases_boe p
  ON a._date = p._date
 AND a.user_segment = p.user_segment
GROUP BY 1, 2
ORDER BY 1, 2;

-------- funnel -----------
DECLARE start_date DATE DEFAULT '2026-01-01';
DECLARE end_date   DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
DECLARE lookback_window INT64 DEFAULT 90;

DECLARE holiday_pattern STRING DEFAULT
--  r'valentine'
 r'x.mas|holiday|christmas|hann?ukah|new year|kwanza|wonderland|winter|end of year|yule|santa|white.elephant|boxing.day'
;

WITH user_mapping AS (
  SELECT DISTINCT visit_id, cast(user_id as STRING) AS user_id
  FROM `etsy-data-warehouse-prod.weblog.visits`
  WHERE _date BETWEEN start_date AND end_date
    AND platform = 'boe'
    AND user_id IS NOT NULL
),

feed_delivered AS (
  SELECT
    _date,
    user_id,
    seen,
    clicked,
    purchased_after_view,
    top_click_query
  FROM `etsy-data-warehouse-dev.tsebastian.mule_feed_delivered_boe` a
  join user_mapping b on a.visit_id = b.visit_id
  WHERE _date BETWEEN start_date AND end_date
),

-- B) Distinct users seen on feed by day
feed_users AS (
  SELECT DISTINCT _date, user_id
  FROM feed_delivered
  WHERE user_id IS NOT NULL
),

-- C) Per-(date, user) 90d activity + Valentine activity flags 
user_activity AS (
  SELECT
    fu._date,
    fu.user_id,

    -- Any activity in last 90d  
    EXISTS (
      SELECT 1
      FROM `etsy-data-warehouse-prod.visit_mart.visits` v90
      WHERE cast(v90.user_id as STRING) = fu.user_id
        AND v90._date BETWEEN DATE_SUB(fu._date, INTERVAL lookback_window DAY)
                          AND DATE_SUB(fu._date, INTERVAL 1 DAY)
        AND v90._date >= DATE '2025-09-01'  -- partition pruning
        AND (
          v90.engaged_visit_5mins = 1
          OR v90.cart_adds       > 0
          OR v90.fav_item_count  > 0
          OR v90.fav_shop_count  > 0
          OR v90.orders          > 0
        )
    ) AS has_any_activity_90d,

    -- Valentine search activity in last 90d 
    EXISTS (
      SELECT 1
      FROM `etsy-data-warehouse-prod.search.query_sessions_new` q
      JOIN `etsy-data-warehouse-prod.weblog.visits` v
        ON q.visit_id = v.visit_id
      WHERE cast(v.user_id as STRING) = fu.user_id
        AND q._date BETWEEN DATE_SUB(fu._date, INTERVAL lookback_window DAY)
                        AND DATE_SUB(fu._date, INTERVAL 1 DAY)
        AND q._date >= DATE '2025-09-01'      -- partition pruning
        AND v._date >= DATE '2025-09-01'      -- partition pruning
        AND REGEXP_CONTAINS(LOWER(q.query), holiday_pattern)
    ) AS has_valentine_search_90d,

    -- Valentine listing engagement in last 90d 
    EXISTS (
      SELECT 1
      FROM `etsy-data-warehouse-prod.analytics.listing_views` lv
      JOIN `etsy-data-warehouse-dev.tsebastian.listing_classification` l
        ON lv.listing_id = l.listing_id
      JOIN `etsy-data-warehouse-prod.weblog.visits` v_lv
        ON SPLIT(lv.visit_id, ".")[SAFE_OFFSET(0)] = v_lv.browser_id
       AND lv._date = v_lv._date
      WHERE cast(v_lv.user_id as STRING)= fu.user_id
        AND lv._date BETWEEN DATE_SUB(fu._date, INTERVAL lookback_window DAY)
                        AND DATE_SUB(fu._date, INTERVAL 1 DAY)
        AND lv._date >= DATE '2025-09-01'     -- partition pruning
        AND v_lv._date >= DATE '2025-09-01'   -- partition pruning
        AND REGEXP_CONTAINS(LOWER(l.top_click_query), holiday_pattern)
    ) AS has_valentine_listing_engagement_90d,

    -- Valentine purchases in last 90d 
    EXISTS (
      SELECT 1
      FROM `etsy-data-warehouse-prod.user_mart.user_mapping` um
      JOIN `etsy-data-warehouse-prod.transaction_mart.all_transactions` t
        ON CAST(t.mapped_user_id AS STRING) = CAST(um.mapped_user_id AS STRING)
      JOIN `etsy-data-warehouse-dev.tsebastian.listing_classification` l
        ON t.listing_id = l.listing_id
      WHERE CAST(um.user_id AS STRING) = fu.user_id
        AND t.date BETWEEN DATE_SUB(fu._date, INTERVAL lookback_window DAY)
                       AND DATE_SUB(fu._date, INTERVAL 1 DAY)
        AND t.date >= DATE '2025-09-01'       -- partition pruning
        AND REGEXP_CONTAINS(LOWER(l.top_click_query), holiday_pattern)
    ) AS has_valentine_purchase_90d

  FROM feed_users fu
),

-- D) Segment users into 3 groups per day
segmented_users AS (
  SELECT
    _date,
    cast(user_id as STRING) AS user_id,
    CASE
      WHEN NOT has_any_activity_90d
        THEN 'seg1_no_activity_90d'
      WHEN
        has_valentine_search_90d
        OR has_valentine_listing_engagement_90d
        OR has_valentine_purchase_90d
        THEN 'seg2_holiday_activity_90d'
      ELSE 'seg3_other_activity_90d'
    END AS user_segment
  FROM user_activity
),

-- E) Candidates by segment
candidates_by_segment AS (
  SELECT
    c._date,
    s.user_segment,
    COUNT(*) AS total_candidates,
    COUNTIF(
      REGEXP_CONTAINS(
        LOWER(c.top_click_query),
      holiday_pattern
      )
    ) AS valentines_candidates
 
  FROM `etsy-data-warehouse-dev.tsebastian.mule_candidates_boe_feed` c
  JOIN segmented_users s
    USING (_date, user_id)
  WHERE c._date BETWEEN start_date AND end_date
  GROUP BY 1, 2
),

-- F) Delivered metrics by segment
delivered_by_segment AS (
  SELECT
    fd._date,
    s.user_segment,
    COUNT(*) AS total_delivered,
    COUNTIF(is_valentine) AS valentines_delivered,
    SUM(seen) AS total_seen,
    SUM(IF(is_valentine, seen, 0)) AS valentines_seen,
    SUM(clicked) AS total_clicked,
    SUM(IF(is_valentine, clicked, 0)) AS valentines_clicked,
    SUM(purchased_after_view) AS total_purchases,
    SUM(IF(is_valentine, purchased_after_view, 0)) AS valentines_purchases
  FROM (
    SELECT
      _date,
      user_id,
      seen,
      clicked,
      purchased_after_view,
      REGEXP_CONTAINS(LOWER(top_click_query), holiday_pattern) AS is_valentine
    FROM `etsy-data-warehouse-dev.tsebastian.mule_feed_delivered_boe` a
  join user_mapping b on a.visit_id = b.visit_id
    WHERE _date BETWEEN start_date AND end_date
  ) fd
  JOIN segmented_users s
    USING (_date, user_id)
  GROUP BY 1, 2
)

-- G) Final per-date, per-segment funnel shares
SELECT
  c._date,
  c.user_segment,
  SAFE_DIVIDE(c.valentines_candidates, c.total_candidates) AS share_candidates_valentines,
  SAFE_DIVIDE(d.valentines_delivered, d.total_delivered)   AS share_delivered_valentines,
  SAFE_DIVIDE(d.valentines_seen, d.total_seen)             AS share_seen_valentines,
  SAFE_DIVIDE(d.valentines_clicked, d.total_clicked)       AS share_clicked_valentines,
  SAFE_DIVIDE(d.valentines_purchases, d.total_purchases)   AS share_of_purchases
FROM candidates_by_segment c
JOIN delivered_by_segment d
  ON c._date = d._date
 AND c.user_segment = d.user_segment
ORDER BY c._date, c.user_segment;


---------- user look by day

DECLARE start_date DATE DEFAULT '2025-12-01';
DECLARE end_date   DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
DECLARE holiday_pattern STRING DEFAULT
--  r'valentine'/
 r'x.mas|holiday|christmas|hann?ukah|new year|kwanza|wonderland|winter|end of year|yule|santa|white.elephant|boxing.day'
;


WITH  user_mapping AS (
  SELECT DISTINCT visit_id, user_id
  FROM `etsy-data-warehouse-prod.weblog.visits`
  WHERE _date BETWEEN start_date AND end_date
    AND platform = 'boe'
    AND user_id IS NOT NULL
),
feed_daily_user_flags AS (
  SELECT
    _date,
    user_id,
    -- Flags at the (date,user) level
    MAX(
      IF(
        REGEXP_CONTAINS(LOWER(top_click_query), holiday_pattern) AND seen = 1,
        1, 0
      )
    ) AS saw_valentines,
    MAX(
      IF(
        REGEXP_CONTAINS(LOWER(top_click_query), holiday_pattern) AND clicked = 1,
        1, 0
      )
    ) AS clicked_valentines,
    MAX(
      IF(
        REGEXP_CONTAINS(LOWER(top_click_query), holiday_pattern) AND purchased_after_view = 1,
        1, 0
      )
    ) AS purchased_valentines
  FROM `etsy-data-warehouse-dev.tsebastian.mule_feed_delivered_boe` a
  JOIN user_mapping b
    ON a.visit_id = b.visit_id
  WHERE _date BETWEEN start_date AND end_date
  GROUP BY 1, 2
),

daily_user_rates AS (
  SELECT
    _date,
    COUNT(DISTINCT user_id) AS total_users,
    COUNTIF(saw_valentines = 1) AS users_saw_valentines,
    COUNTIF(clicked_valentines = 1) AS users_clicked_valentines,
    COUNTIF(purchased_valentines = 1) AS users_purchased_valentines
  FROM feed_daily_user_flags
  GROUP BY 1
)

SELECT
  _date,
  SAFE_DIVIDE(users_saw_valentines, total_users)       AS pct_users_saw_valentines,
  SAFE_DIVIDE(users_clicked_valentines, total_users)   AS pct_users_clicked_valentines,
  SAFE_DIVIDE(users_purchased_valentines, total_users) AS pct_users_purchased_valentines,
  total_users,
  -- users_saw_valentines,
  -- users_clicked_valentines,
  -- users_purchased_valentines
FROM daily_user_rates
ORDER BY _date;



-- Post-Christmas 2025 staleness impact with separate AOVs

DECLARE start_date DATE DEFAULT DATE '2025-12-26';
DECLARE end_date   DATE DEFAULT DATE '2026-01-25';

DECLARE holiday_pattern STRING DEFAULT
  r'x.mas|holiday|christmas|hann?ukah|new year|kwanza|wonderland|winter|end of year|yule|santa|white.elephant|boxing.day';

-- A) Feed-level impressions and purchases, tagged as stale vs evergreen
WITH feed_base AS (
  SELECT
    _date,
    seen,
    purchased_after_view,
    REGEXP_CONTAINS(LOWER(top_click_query), holiday_pattern) AS is_holiday
  FROM `etsy-data-warehouse-dev.tsebastian.mule_feed_delivered_boe`
  WHERE _date BETWEEN start_date AND end_date
),

agg AS (
  SELECT
    SUM(seen) AS impr_total,
    SUM(IF(is_holiday, seen, 0)) AS impr_stale,
    SUM(IF(NOT is_holiday, seen, 0)) AS impr_evergreen,
    SUM(purchased_after_view) AS purch_total,
    SUM(IF(is_holiday, purchased_after_view, 0)) AS purch_stale,
    SUM(IF(NOT is_holiday, purchased_after_view, 0)) AS purch_evergreen
  FROM feed_base
),

cvr AS (
  SELECT
    impr_stale,
    impr_evergreen,
    purch_stale,
    purch_evergreen,
    SAFE_DIVIDE(purch_stale, impr_stale)         AS cvr_stale,
    SAFE_DIVIDE(purch_evergreen, impr_evergreen) AS cvr_evergreen
  FROM agg
),

delta_purchases AS (
  SELECT
    impr_stale,
    impr_evergreen,
    purch_stale,
    purch_evergreen,
    cvr_stale,
    cvr_evergreen,
    impr_stale * cvr_evergreen                         AS purch_cf_if_evergreen,
    GREATEST(impr_stale * cvr_evergreen - purch_stale, 0) AS delta_purchases
  FROM cvr
),

-- B) BOE GMS + AOV for holiday vs evergreen purchases in the same window
boe_purchases AS (
  SELECT
    REGEXP_CONTAINS(LOWER(lc.top_click_query), holiday_pattern) AS is_holiday,
    tg.gms_net
  FROM `etsy-data-warehouse-prod.transaction_mart.all_transactions` t
  JOIN `etsy-data-warehouse-prod.transaction_mart.transactions_visits` tv
    USING (transaction_id)
  JOIN `etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans` tg
    USING (transaction_id)
  LEFT JOIN `etsy-data-warehouse-dev.tsebastian.listing_classification` lc
    ON t.listing_id = lc.listing_id
  WHERE t.date BETWEEN start_date AND end_date
    AND tv._date BETWEEN start_date AND end_date
    AND tv.platform_app = 'boe'
    AND tg.is_test_seller = 0
    AND tg.is_cash = 0
),

aovs AS (
  SELECT
    SAFE_DIVIDE(SUM(IF(is_holiday, gms_net, 0.0)), NULLIF(COUNTIF(is_holiday), 0))        AS aov_holiday,
    SAFE_DIVIDE(SUM(IF(NOT is_holiday, gms_net, 0.0)), NULLIF(COUNTIF(NOT is_holiday), 0)) AS aov_evergreen
  FROM boe_purchases
)

SELECT
  'post_christmas_2025_2026' AS period_name,
  impr_stale,
  purch_stale,
  cvr_stale,
  cvr_evergreen,
  aov_holiday,
  aov_evergreen,
  purch_cf_if_evergreen,
  a.delta_purchases,
  -- Observed GMS from stale feed purchases, using holiday AOV
  purch_stale * aov_holiday                        AS gms_obs_stale,
  -- Counterfactual GMS if these impressions had evergreen-like CVR and evergreen AOV
  (impr_stale * cvr_evergreen) * aov_evergreen     AS gms_cf_if_evergreen,
  -- Incremental GMS opportunity
  (impr_stale * cvr_evergreen) * aov_evergreen
    - purch_stale * aov_holiday                    AS delta_gms
FROM delta_purchases a
CROSS JOIN aovs;




-- Post-Valentine's 2026 staleness impact:
-- 1-week window with separate AOVs, then x4 extrapolation to ~1 month

DECLARE start_date DATE DEFAULT DATE '2026-02-15';
DECLARE end_date   DATE DEFAULT DATE '2026-02-21';

DECLARE holiday_pattern STRING DEFAULT r'valentine';

-- A) Feed-level impressions and purchases
WITH feed_base AS (
  SELECT
    _date,
    seen,
    purchased_after_view,
    REGEXP_CONTAINS(LOWER(top_click_query), holiday_pattern) AS is_valentine
  FROM `etsy-data-warehouse-dev.tsebastian.mule_feed_delivered_boe`
  WHERE _date BETWEEN start_date AND end_date
),

agg AS (
  SELECT
    SUM(seen) AS impr_total,
    SUM(IF(is_valentine, seen, 0)) AS impr_stale,
    SUM(IF(NOT is_valentine, seen, 0)) AS impr_evergreen,
    SUM(purchased_after_view) AS purch_total,
    SUM(IF(is_valentine, purchased_after_view, 0)) AS purch_stale,
    SUM(IF(NOT is_valentine, purchased_after_view, 0)) AS purch_evergreen
  FROM feed_base
),

cvr AS (
  SELECT
    impr_stale,
    impr_evergreen,
    purch_stale,
    purch_evergreen,
    SAFE_DIVIDE(purch_stale, impr_stale)         AS cvr_stale,
    SAFE_DIVIDE(purch_evergreen, impr_evergreen) AS cvr_evergreen
  FROM agg
),

delta_purchases AS (
  SELECT
    impr_stale,
    impr_evergreen,
    purch_stale,
    purch_evergreen,
    cvr_stale,
    cvr_evergreen,
    impr_stale * cvr_evergreen                           AS purch_cf_if_evergreen,
    GREATEST(impr_stale * cvr_evergreen - purch_stale, 0) AS delta_purchases_week
  FROM cvr
),

-- B) BOE GMS + AOVs in the same 1-week window
boe_purchases AS (
  SELECT
    REGEXP_CONTAINS(LOWER(lc.top_click_query), holiday_pattern) AS is_holiday,
    tg.gms_net
  FROM `etsy-data-warehouse-prod.transaction_mart.all_transactions` t
  JOIN `etsy-data-warehouse-prod.transaction_mart.transactions_visits` tv
    USING (transaction_id)
  JOIN `etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans` tg
    USING (transaction_id)
  LEFT JOIN `etsy-data-warehouse-dev.tsebastian.listing_classification` lc
    ON t.listing_id = lc.listing_id
  WHERE t.date BETWEEN start_date AND end_date
    AND tv._date BETWEEN start_date AND end_date
    AND tv.platform_app = 'boe'
    AND tg.is_test_seller = 0
    AND tg.is_cash = 0
),

aovs AS (
  SELECT
    SAFE_DIVIDE(SUM(IF(is_holiday, gms_net, 0.0)), NULLIF(COUNTIF(is_holiday), 0))        AS aov_valentine,
    SAFE_DIVIDE(SUM(IF(NOT is_holiday, gms_net, 0.0)), NULLIF(COUNTIF(NOT is_holiday), 0)) AS aov_evergreen
  FROM boe_purchases
)

SELECT
  'post_valentines_2026' AS period_name,
  impr_stale,
  purch_stale,
  cvr_stale,
  cvr_evergreen,
  aov_valentine,
  aov_evergreen,
  purch_cf_if_evergreen,
  delta_purchases_week,
  -- Observed GMS from stale V-day feed purchases in this week
  purch_stale * aov_valentine                    AS gms_obs_stale_week,
  -- Counterfactual GMS if these impressions had evergreen CVR & AOV
  (impr_stale * cvr_evergreen) * aov_evergreen   AS gms_cf_if_evergreen_week,
  (impr_stale * cvr_evergreen) * aov_evergreen
    - purch_stale * aov_valentine                AS delta_gms_week,
  -- Simple x4 extrapolation to approximate a full month
  delta_purchases_week * 4                       AS delta_purchases_month_est,
    ((impr_stale * cvr_evergreen) * aov_evergreen
    - purch_stale * aov_valentine ) * 4                             AS delta_gms_month_est
FROM delta_purchases a
CROSS JOIN aovs;
