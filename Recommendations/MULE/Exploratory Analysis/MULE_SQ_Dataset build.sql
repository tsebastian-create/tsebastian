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
    v._date,
    v.event_source,

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
    visit.event_source,

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
        max(_date) as last_visit_date
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
    CASE WHEN e.last_visit_date IS NULL THEN 'Unknown or >60'
        WHEN date_diff(a._date,e.last_visit_date, DAY) <=1 THEN 'a.0-1'
        WHEN date_diff(a._date,e.last_visit_date, DAY) <=7 THEN 'b.1-7'
        WHEN date_diff(a._date,e.last_visit_date, DAY) <=14 THEN 'c.7-14'
        WHEN date_diff(a._date,e.last_visit_date, DAY) <=30 THEN 'd.14-30'
        WHEN date_diff(a._date,e.last_visit_date, DAY) <=60 THEN 'e.30-60'
        END AS days_since_last_visit

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
                COUNT(1)) AS listing_12m_cvr,
    
    SAFE_DIVIDE(SUM(CAST(CASE WHEN lv._date BETWEEN DATE_SUB(start_date, INTERVAL 7 DAY) AND start_date THEN lv.purchased_after_view END AS INT64)),
                COUNT(CASE WHEN lv._date BETWEEN DATE_SUB(start_date, INTERVAL 7 DAY) AND start_date THEN 1 END )) AS listing_7d_cvr
  FROM `etsy-data-warehouse-prod.analytics.listing_views` lv
  WHERE lv._date BETWEEN DATE_SUB(start_date, INTERVAL 1 YEAR) AND start_date-1
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
    f.listing_7d_cvr
    

 FROM `etsy-data-warehouse-dev.tsebastian.bound_reset_user_profile_add`  
          a
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listings` 
          b ON a.listing_id = b.listing_id # and b.is_active = 1
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.derived_listing_indicators`     
          c on a.listing_id = c.listing_id # and c.is_active = 1
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_attributes`
          d on a.listing_id = d.listing_id
LEFT JOIN listing_reviews
          e on a.listing_id = e.listing_id
LEFT JOIN cr
          f on a.listing_id = f.listing_id

;


CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.tsebastian.bound_reset_seller_add` 
OPTIONS(expiration_timestamp =  current_timestamp + interval 1 day )
 AS 
with seller_reviews as (
  SELECT shop_id
      ,AVG(rating) as seller_avg_12m_rating
      ,COUNT(rating) as seller_nbr_12m_rating

  FROM `etsy-data-warehouse-prod.etsy_shard.shop_transaction_review` str
  WHERE DATE(TIMESTAMP_SECONDS(str.create_date)) BETWEEN DATE_SUB(start_date, INTERVAL 1 YEAR) AND start_date-1
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
-- adding past 90 day top 3 search terms, HOME engagement
-- -- -- -- -- -- -- -- -- 
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.tsebastian.bound_reset_sq_add` 
AS

WITH  
-- Visits for each user within [impression_date - 90d, impression_date]
visits AS ( SELECT
    s.user_id,
    s._date,
    vm.visit_id,
      vm._date AS visit_date
  FROM (SELECT DISTINCT user_id,_date FROM `etsy-data-warehouse-dev.tsebastian.bound_reset_seller_add` )s
  JOIN `etsy-data-warehouse-prod.visit_mart.visit_mapping` vm
    ON vm.mapped_user_id = s.user_id
   AND vm._date BETWEEN DATE_SUB(s._date, INTERVAL 90 DAY) AND s._date-1  ## MULE has inference delay of hours-1day, so safe to take day prior
   WHERE vm._date >= '2025-08-01'
),
-- Engagement on BOE Home modules in the  90d window
home_engagement AS (
   SELECT
    v.user_id,
    v._date,

    -- 90d totals 
    SUM(r.clicked)                         AS home_clicks_90d,
    SUM(r.favorited)                       AS home_favs_90d,
    SUM(r.added_to_cart)                   AS home_cart_adds_90d,

    -- 30d subset
    SUM(IF(r._date BETWEEN DATE_SUB(v._date, INTERVAL 30 DAY) AND DATE_SUB(v._date, INTERVAL 1 DAY), r.clicked, 0))       AS home_clicks_30d,
    SUM(IF(r._date BETWEEN DATE_SUB(v._date, INTERVAL 30 DAY) AND DATE_SUB(v._date, INTERVAL 1 DAY), r.favorited, 0))     AS home_favs_30d,
    SUM(IF(r._date BETWEEN DATE_SUB(v._date, INTERVAL 30 DAY) AND DATE_SUB(v._date, INTERVAL 1 DAY), r.added_to_cart, 0)) AS home_cart_adds_30d,

    -- 7d subset
    SUM(IF(r._date BETWEEN DATE_SUB(v._date, INTERVAL 7 DAY) AND DATE_SUB(v._date, INTERVAL 1 DAY), r.clicked, 0))       AS home_clicks_7d,
    SUM(IF(r._date BETWEEN DATE_SUB(v._date, INTERVAL 7 DAY) AND DATE_SUB(v._date, INTERVAL 1 DAY), r.favorited, 0))     AS home_favs_7d,
    SUM(IF(r._date BETWEEN DATE_SUB(v._date, INTERVAL 7 DAY) AND DATE_SUB(v._date, INTERVAL 1 DAY), r.added_to_cart, 0)) AS home_cart_adds_7d

  FROM visits v
  JOIN `etsy-data-warehouse-prod.rollups.recsys_delivered_listings` r
    ON r.visit_id = v.visit_id
   AND r._date    = v.visit_date
  WHERE (r.module_page = 'home' or r.module_page = 'homescreen') ## correct?
     AND r.platform = 'boe'
     
  GROUP BY 1, 2
),
qis_counts AS (
  SELECT
    v.user_id,
    v._date,

    -- 90d totals (window: [v._date - 90d, v._date - 1])
    SUM(IF(s.prediction = 0, 1, 0)) AS qis_broad_90d,
    SUM(IF(s.prediction = 1, 1, 0)) AS qis_direct_unspecified_90d,
    SUM(IF(s.prediction = 2, 1, 0)) AS qis_direct_specified_90d,

    -- 30d subset of the same 90d window
    SUM(IF(q._date >= DATE_SUB(v._date, INTERVAL 30 DAY) AND s.prediction = 0, 1, 0)) AS qis_broad_30d,
    SUM(IF(q._date >= DATE_SUB(v._date, INTERVAL 30 DAY) AND s.prediction = 1, 1, 0)) AS qis_direct_unspecified_30d,
    SUM(IF(q._date >= DATE_SUB(v._date, INTERVAL 30 DAY) AND s.prediction = 2, 1, 0)) AS qis_direct_specified_30d,

    -- 7d subset of the same 90d window
    SUM(IF(q._date >= DATE_SUB(v._date, INTERVAL 7 DAY)  AND s.prediction = 0, 1, 0)) AS qis_broad_7d,
    SUM(IF(q._date >= DATE_SUB(v._date, INTERVAL 7 DAY)  AND s.prediction = 1, 1, 0)) AS qis_direct_unspecified_7d,
    SUM(IF(q._date >= DATE_SUB(v._date, INTERVAL 7 DAY)  AND s.prediction = 2, 1, 0)) AS qis_direct_specified_7d

  FROM visits v
  JOIN `etsy-data-warehouse-prod.search.query_sessions_new` q
    ON q.visit_id = v.visit_id
   AND q._date BETWEEN DATE_SUB(v._date, INTERVAL 90 DAY) AND v._date-1
  LEFT JOIN `etsy-search-ml-prod.mission_understanding.qis_scores_v3` s
    ON s.query_raw = q.query

  WHERE q._date >= '2025-08-01'
  GROUP BY 1, 2
),
-- Classified search sessions in the same window
user_searches AS (
  SELECT
    v.user_id,
    v._date,
    q.classified_taxonomy_id,
    q.start_epoch_ms
  FROM visits v
  LEFT JOIN `etsy-data-warehouse-prod.search.query_sessions_new` q     ON q.visit_id = v.visit_id  AND q._date BETWEEN DATE_SUB(v._date, INTERVAL 90 DAY) AND v._date-1
  and q.classified_taxonomy_id IS NOT NULL
  and q._date >='2025-08-01' #to prune
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
  -- Top 3 categories as columns 
  ARRAY_AGG(l0_category ORDER BY searches DESC, last_searched_at DESC)[SAFE_OFFSET(0)] AS l0_cat_1,
  ARRAY_AGG(l0_category ORDER BY searches DESC, last_searched_at DESC)[SAFE_OFFSET(1)] AS l0_cat_2,
  ARRAY_AGG(l0_category ORDER BY searches DESC, last_searched_at DESC)[SAFE_OFFSET(2)] AS l0_cat_3
FROM l0_counts a
GROUP BY 1, 2
)

select a.* ,

  --- top 3 searched categories in 90 days
    b.l0_cat_1,
    b.l0_cat_2,
    b.l0_cat_3,

  -- BOE Home listing engagement
    he.home_clicks_90d,
    he.home_favs_90d,
    he.home_cart_adds_90d,
    he.home_clicks_30d,
    he.home_favs_30d,
    he.home_cart_adds_30d,
    he.home_clicks_7d,
    he.home_favs_7d,
    he.home_cart_adds_7d,
    -- search specificity
    qc.qis_broad_90d,
    qc.qis_direct_unspecified_90d,
    qc.qis_direct_specified_90d,
    qc.qis_broad_30d,
    qc.qis_direct_unspecified_30d,
    qc.qis_direct_specified_30d,
    qc.qis_broad_7d,
    qc.qis_direct_unspecified_7d,
    qc.qis_direct_specified_7d,

  --- recs engaged click proxy
    CASE
      WHEN COALESCE(rec_clicked, 0) = 1
           AND (
             COALESCE(rec_added_to_cart, 0) = 1 OR
             COALESCE(rec_favorited,     0) = 1 OR
             COALESCE(rec_purchased_after_view, 0) = 1
           )
      THEN 1 ELSE 0 end as recs_engaged_click_proxy

from `etsy-data-warehouse-dev.tsebastian.bound_reset_seller_add`  a
LEFT JOIN top3 b on a._date = b._date and a.user_id = b.user_id
LEFT JOIN home_engagement he
  ON a.user_id = he.user_id AND a._date = he._date
LEFT JOIN qis_counts qc
  ON a.user_id = qc.user_id AND a._date = qc._date

;
 




--  stable user-level sample using a hash of user_id

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.tsebastian.mule_sq_1pct_bound_reset` 
AS

WITH user_samp AS (
  SELECT user_id
  FROM `etsy-data-warehouse-dev.tsebastian.bound_reset_sq_add`
  GROUP BY user_id
  
  HAVING MOD(ABS(FARM_FINGERPRINT(CAST(user_id AS STRING))), 100) <10  #   % sample  
)
SELECT b.*
FROM user_samp a
JOIN `etsy-data-warehouse-dev.tsebastian.bound_reset_sq_add` b
USING (user_id);


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



