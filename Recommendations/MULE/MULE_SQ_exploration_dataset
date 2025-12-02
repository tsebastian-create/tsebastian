DECLARE my_experiment STRING default 'boe_home/app_home.feed.mule_sq';
DECLARE start_date DATE default '2025-11-07';
DECLARE end_date DATE default '2025-11-13';
DECLARE module_placement_input STRING default 'boe_homescreen_feed';
DECLARE bucketing_id_type INT64;


SET bucketing_id_type = (
  SELECT bucketing_id_type
  FROM `etsy-data-warehouse-prod.catapult_unified.experiment`
  WHERE _date = end_date
    AND experiment_id = my_experiment
);

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.tsebastian.bound_reset_ab_first_bucket_initial` 
OPTIONS(expiration_timestamp =  current_timestamp + interval 1 day )
AS  
  SELECT
    bucketing_id,
    bucketing_id_type,
    variant_id,
    MIN(bucketing_ts) AS bucketing_ts,
  FROM `etsy-data-warehouse-prod.catapult_unified.bucketing`
  WHERE _date BETWEEN start_date AND end_date
    AND experiment_id = my_experiment
  GROUP BY bucketing_id, bucketing_id_type, variant_id
;

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.tsebastian.bound_reset_ab_first_bucket` 
OPTIONS(expiration_timestamp =  current_timestamp + interval 1 day )
AS
  SELECT
    bucketing_id_type,
    b.bucketing_id,
    b.variant_id,
    COALESCE(MIN(f.event_ts), b.bucketing_ts) AS bucketing_ts
  FROM  `etsy-data-warehouse-dev.tsebastian.bound_reset_ab_first_bucket_initial` b
  LEFT JOIN `etsy-data-warehouse-prod.catapult_unified.filtering_event` f    ## qn about this step / catapult will have flag if filtered event (qualifying action)
    ON f.bucketing_id = b.bucketing_id
    AND f._date BETWEEN start_date AND end_date
    AND f.experiment_id = my_experiment
    AND f.event_ts >= f.boundary_start_ts
    AND f.event_ts >= b.bucketing_ts
  GROUP BY 
    bucketing_id_type,b.bucketing_id, b.variant_id, b.bucketing_ts
;


CREATE OR REPLACE TABLE`etsy-data-warehouse-dev.tsebastian.bound_reset_subsequent_visits` 
OPTIONS(expiration_timestamp =  current_timestamp + interval 1 day )
AS
 
  -- Browser-based experiments (bucketing_id_type = 1)
  SELECT
    b.bucketing_id,
    b.variant_id,
    b.bucketing_ts,
    v.visit_id,
    v.user_id,
    v.items_purchased, 
    v.total_gms, 
    v.pages_seen, 
    v.bounced, 
    v.referring_domain, 
    v.canonical_region,
    v.cart_adds,
    v.search_info,
    v._date,
  
    
  FROM `etsy-data-warehouse-dev.tsebastian.bound_reset_ab_first_bucket`  b
  JOIN `etsy-data-warehouse-prod.weblog.visits` v
    ON bucketing_id_type = 1 
    AND b.bucketing_id = v.browser_id
    AND TIMESTAMP_TRUNC(b.bucketing_ts, SECOND) <= v.end_datetime
  WHERE v._date BETWEEN start_date AND end_date
  
  UNION ALL
  
  -- User-based experiments (bucketing_id_type = 2)
  SELECT
    b.bucketing_id,
    b.variant_id,
    b.bucketing_ts,
    v.visit_id,
    v.user_id,
    v.items_purchased, 
    v.total_gms, 
    v.pages_seen, 
    v.bounced, 
    v.referring_domain, 
    v.canonical_region,
    v.cart_adds,
    v.search_info,
    v._date

  FROM `etsy-data-warehouse-dev.tsebastian.bound_reset_ab_first_bucket`  b
  JOIN `etsy-data-warehouse-prod.weblog.visits` v
    ON bucketing_id_type = 2
    AND b.bucketing_id = CAST(v.user_id AS STRING)
    AND TIMESTAMP_TRUNC(b.bucketing_ts, SECOND) <= v.end_datetime
  WHERE v._date BETWEEN start_date AND end_date    
;




CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.tsebastian.bound_reset_visit_user_rows` 
OPTIONS(expiration_timestamp =  current_timestamp + interval 1 day )
 AS 
  SELECT 
    visit.user_id,
    rdl._date,
    visit.variant_id, 
    rdl.candidate_set,
    rdl.visit_id, 
    rdl.listing_id,
    rdl.sequence_number,

    visit.items_purchased as visit_items_purchased, 
    CAST(visit.total_gms AS FLOAT64) as visit_total_gms, 
    visit.pages_seen as visit_pages_seen, 
    visit.bounced as visit_bounced, 
    visit.referring_domain as visit_referring_domain, 
    visit.canonical_region as visit_canonical_region,
    visit.cart_adds as visit_cart_adds,

    rdl.top_channel as visit_channel,
    rdl.seen as rec_seen,
    rdl.rec_price,      #usd
    rdl.clicked as rec_clicked,
    rdl.added_to_cart as rec_added_to_cart,
    rdl.favorited as rec_favorited,
    rdl.purchased_after_view as rec_purchased_after_view,     #CR on catapult supersets this since its sitewide CR
    rdl.rec_top_category,

  FROM `etsy-data-warehouse-dev.tsebastian.bound_reset_subsequent_visits`  visit
  JOIN `etsy-data-warehouse-prod.rollups.recsys_delivered_listings` rdl 
    USING(visit_id)
  WHERE module_placement =  module_placement_input

;

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.tsebastian.bound_reset_user_profile_add` 
OPTIONS(expiration_timestamp =  current_timestamp + interval 1 day )

 AS 
 WITH  user_searches as 
    (SELECT
        v.user_id,           
        SUM(v.search_info.queries_count) AS buyer_searches_60d,
        SUM(v.search_info.queries_count)/COUNT(Distinct visit_id) AS buyer_search_intensity_60d,
      FROM `etsy-data-warehouse-prod.weblog.visits` v
        WHERE v._date BETWEEN DATE_SUB(start_date, INTERVAL 60 DAY) AND start_date-1    #start date could be changed to per user bucketing_ts for more accuracy
        GROUP BY 1)

  SELECT a.*,
    b.buyer_segment,
    date_diff(a._date, DATE(TIMESTAMP_SECONDS(c.first_user_create_date)), DAY) as buyer_tenure,
    c.is_seller as buyer_is_seller,
    c.is_admin as buyer_is_admin,

    CAST(d.gms_last_12m AS FLOAT64) as buyer_gms_last_12m,
    d.is_top_buyer,
    d.top_buyer_type,
    e.buyer_searches_60d,
    e.buyer_search_intensity_60d,

  FROM  `etsy-data-warehouse-dev.tsebastian.bound_reset_visit_user_rows` a
  LEFT JOIN  `etsy-data-warehouse-prod.catapult.catapult_daily_buyer_segments`  b      USING(user_id,_date) 

  LEFT JOIN `etsy-data-warehouse-prod.user_mart.mapped_user_profile` c on a.user_id = c.user_id
  LEFT JOIN `etsy-data-warehouse-prod.buyer360_topbuyer.buyer_status_metrics_daily` d on a.user_id = d.mapped_user_id and a._date = d._date
  LEFT JOIN user_searches
            e on a.user_id = e.user_id

;


CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.tsebastian.bound_reset_listing_add` 
OPTIONS(expiration_timestamp =  current_timestamp + interval 1 day )
 AS 

with listing_reviews as (
  SELECT listing_id
      ,AVG(rating) as listing_avg_12m_rating
      ,COUNT(rating) as listing_nbr_12m_rating

  FROM `etsy-data-warehouse-prod.etsy_shard.shop_transaction_review` str        ##rollup for ratings exists
  WHERE DATE(TIMESTAMP_SECONDS(str.create_date)) BETWEEN DATE_SUB(start_date, INTERVAL 1 YEAR) AND start_date
    AND str.is_deleted = 0
    GROUP BY 1
)

,cr AS (
  SELECT
    lv.listing_id,
    SAFE_DIVIDE(SUM(CAST(lv.purchased_after_view AS INT64)),
                COUNT(1)) AS listing_12m_cvr
  FROM `etsy-data-warehouse-prod.analytics.listing_views` lv
  WHERE lv._date BETWEEN DATE_SUB(start_date, INTERVAL 1 YEAR) AND start_date
  GROUP BY 1
)

SELECT a.*,
    date_diff(a._date, DATE(TIMESTAMP_SECONDS(b.original_create_date)), DAY) as listing_age,
    b.shop_id,
    b.user_id AS seller_id,
    c.quality_score.quality_score as listing_quality_score,
    d.is_digital as listing_is_digital,
    -- d.top_category as listing_top_category,
    d.full_path as listing_taxonomy_full_path, #1:1 with taxo id
    e.listing_avg_12m_rating,
    e.listing_nbr_12m_rating,
    f.listing_12m_cvr,
    

 FROM `etsy-data-warehouse-dev.tsebastian.bound_reset_user_profile_add`  
          a
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listings` 
          b ON a.listing_id = b.listing_id and b.is_active = 1
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.derived_listing_indicators`     
          c on a.listing_id = c.listing_id and c.is_active = 1
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_attributes`
          d on a.listing_id = d.listing_id
LEFT JOIN listing_reviews
          e on a.listing_id = e.listing_id
LEFT JOIN cr
          f on a.listing_id = f.listing_id

;


CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.tsebastian.bound_reset_seller_add` 
-- OPTIONS(expiration_timestamp =  current_timestamp + interval 1 day )
 AS 
with seller_reviews as (
  SELECT shop_id
      ,AVG(rating) as seller_avg_12m_rating
      ,COUNT(rating) as seller_nbr_12m_rating

  FROM `etsy-data-warehouse-prod.etsy_shard.shop_transaction_review` str
  WHERE DATE(TIMESTAMP_SECONDS(str.create_date)) BETWEEN DATE_SUB(start_date, INTERVAL 1 YEAR) AND start_date
    AND str.is_deleted = 0
    GROUP BY 1
)

SELECT a.*,
      case when e.listing_id is not null  and e.is_paused = 0 and f.shop_id is not null then 1 else 0 end as listing_is_ad_eligible,
      -- case when e.listing_id is not null then 1 else 0 end as listing_is_in_prolist,
      date_diff(a._date, b.open_date, DAY) as seller_shop_open_tenure,
      cast(b.avg_active_listing_price_usd AS FLOAT64) as seller_avg_listing_price_usd,
      b.gms_percentile as seller_12m_gms_percentile,
      c.seller_tier_new,
      d.seller_avg_12m_rating,
      d.seller_nbr_12m_rating,


FROM `etsy-data-warehouse-dev.tsebastian.bound_reset_listing_add` 
          a
LEFT JOIN `etsy-data-warehouse-prod.rollups.seller_basics` 
          b on a.seller_id = b.user_id
LEFT JOIN `etsy-data-warehouse-prod.rollups.seller_tier_new_daily_historical` 
          c on a.seller_id = c.user_id and a._date = c.date
LEFT JOIN seller_reviews
          d on a.shop_id = d.shop_id
LEFT JOIN `etsy-data-warehouse-prod.etsy_shard.prolist_listing` 
          e on a.listing_id = e.listing_id and DATE(TIMESTAMP_SECONDS(e.create_date)) <= a._date
LEFT JOIN `etsy-data-warehouse-prod.rollups.prolist_daily_shop_data`
          f ON a.shop_id = f.shop_id and f.date = a._date

;


-- -- -- -- -- -- -- -- -- 
-- adding past 90 day top 3 search terms
-- -- -- -- -- -- -- -- -- 
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.tsebastian.bound_reset_sq_add` 
AS

WITH  
-- Visits for each user within [impression_date - 60d, impression_date]
visits AS ( SELECT
    s.user_id,
    s._date,
    vm.visit_id,
    vm._date AS visit_date
  FROM (SELECT DISTINCT user_id,_date FROM `etsy-data-warehouse-dev.tsebastian.bound_reset_seller_add` )s
  JOIN `etsy-data-warehouse-prod.visit_mart.visit_mapping` vm
    ON vm.mapped_user_id = s.user_id
   AND vm._date BETWEEN DATE_SUB(s._date, INTERVAL 90 DAY) AND s._date-1
   WHERE vm._date >= '2025-08-01'
),

-- Classified search sessions in the same window
user_searches AS (
  SELECT
    v.user_id,
    v._date,
    q.classified_taxonomy_id,
    q.start_epoch_ms
  FROM `etsy-data-warehouse-prod.search.query_sessions_new` q
  JOIN visits v
    ON q.visit_id = v.visit_id
   AND q._date BETWEEN DATE_SUB(v._date, INTERVAL 90 DAY) AND v._date-1
  WHERE q.classified_taxonomy_id IS NOT NULL
  and q._date >='2025-08-01'
),

-- Bring taxonomy; derive L0 category
taxo AS (
  SELECT taxonomy_id, full_path
  FROM `etsy-data-warehouse-prod.structured_data.taxonomy`
),

l0_counts AS (
  SELECT
    us.user_id,
    us._date,
    SPLIT(t.full_path, '.')[SAFE_OFFSET(0)] AS l0_category,
    COUNT(*) AS searches,
    MAX(TIMESTAMP_MILLIS(us.start_epoch_ms)) AS last_searched_at
  FROM user_searches us
  LEFT JOIN taxo t
    ON t.taxonomy_id = us.classified_taxonomy_id
  GROUP BY 1, 2, 3
)
, top3 as (
SELECT
  user_id,
  _date,
  -- Top 3 categories as columns (break ties by most recent activity)
  ARRAY_AGG(l0_category ORDER BY searches DESC, last_searched_at DESC)[SAFE_OFFSET(0)] AS l0_cat_1,
  ARRAY_AGG(l0_category ORDER BY searches DESC, last_searched_at DESC)[SAFE_OFFSET(1)] AS l0_cat_2,
  ARRAY_AGG(l0_category ORDER BY searches DESC, last_searched_at DESC)[SAFE_OFFSET(2)] AS l0_cat_3
FROM l0_counts a
GROUP BY 1, 2
)

select a.* 
,b.l0_cat_1
, b.l0_cat_2
, b.l0_cat_3
,CASE
      WHEN COALESCE(rec_clicked, 0) = 1
           AND (
             COALESCE(rec_added_to_cart, 0) = 1 OR
             COALESCE(rec_favorited,     0) = 1 OR
             COALESCE(rec_purchased_after_view, 0) = 1
           )
      THEN 1 ELSE 0 end as recs_engaged_click_proxy

from `etsy-data-warehouse-dev.tsebastian.bound_reset_seller_add`  a
LEFT JOIN top3 b on a._date = b._date and a.user_id = b.user_id
;


 


-- 1% stable user-level sample using a hash of user_id

-- CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.tsebastian.mule_sq_1pct_bound_reset` 
-- AS

-- WITH user_samp AS (
--   SELECT user_id
--   FROM `etsy-data-warehouse-dev.tsebastian.bound_reset_seller_add`
--   GROUP BY user_id
  
--   HAVING MOD(ABS(FARM_FINGERPRINT(CAST(user_id AS STRING))), 100) = 0      
-- )
-- SELECT b.*
-- FROM user_samp a
-- JOIN `etsy-data-warehouse-dev.tsebastian.bound_reset_seller_add` b
-- USING (user_id);


### adding proxy recs engaged clicks

-- DECLARE start_date DATE default '2025-11-07';
-- DECLARE end_date DATE default '2025-11-13';
-- ALTER TABLE  `etsy-data-warehouse-dev.tsebastian.bound_reset_sq_add` 
-- ADD COLUMN recs_engaged_click INT64;

-- UPDATE  `etsy-data-warehouse-dev.tsebastian.bound_reset_sq_add` 
-- SET recs_engaged_click =
--   CAST(
--     CASE
--       WHEN COALESCE(rec_clicked, 0) = 1
--            AND (
--              COALESCE(rec_added_to_cart, 0) = 1 OR
--              COALESCE(rec_favorited,     0) = 1 OR
--              COALESCE(rec_purchased_after_view, 0) = 1
--            )
--       THEN 1 ELSE 0
--     END AS INT64
--   )
-- WHERE _date BETWEEN start_date AND end_date;



--- Non ad listings

-- CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.tsebastian.bound_reset_nonad_listings` 
-- OPTIONS(expiration_timestamp =  current_timestamp + interval 2 day ) AS 

-- select distinct listing_id,_date 
--  from `etsy-data-warehouse-dev.tsebastian.bound_reset_seller_add`  
--  where listing_is_ad_eligible = 0 and variant_id != 'off'
-- ;


-- select listing_is_ad_eligible, 
-- count(distinct listing_id) 
-- from `etsy-data-warehouse-dev.tsebastian.bound_reset_seller_add` 
--  WHERE variant_id != 'off' group by 1
-- ;


-- SELECT  is_paused
-- ,case when c.shop_id is not null then 1 else 0 end as shop_has_budget
-- ,count(distinct a.listing_id) as listings

-- FROM  `etsy-data-warehouse-dev.tsebastian.bound_reset_nonad_listings`  A

-- left join `etsy-data-warehouse-prod.etsy_shard.prolist_listing`  B ON A.listing_id = B.listing_id  and DATE(TIMESTAMP_SECONDS(b.create_date)) <= a._date
-- LEFT JOIN `etsy-data-warehouse-prod.rollups.prolist_daily_shop_data` 
--           c ON b.shop_id = c.shop_id and a._date = c.date

-- group by 1,2
-- order by 1,2



