DECLARE my_experiment STRING default 'boe_home/app_home.feed.mule_sq';
DECLARE start_date DATE default '2025-10-27';
DECLARE end_date DATE default '2025-11-06';
DECLARE module_placement_input STRING default 'boe_homescreen_feed';

DECLARE bucketing_id_type INT64;


SET bucketing_id_type = (
  SELECT bucketing_id_type
  FROM `etsy-data-warehouse-prod.catapult_unified.experiment`
  WHERE _date = end_date
    AND experiment_id = my_experiment
);

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.tsebastian.ab_first_bucket_initial` 
OPTIONS(expiration_timestamp =  current_timestamp + interval 7 day )
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

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.tsebastian.ab_first_bucket` 
OPTIONS(expiration_timestamp =  current_timestamp + interval 7 day )
AS
  SELECT
    b.bucketing_id,
    b.variant_id,
    COALESCE(MIN(f.event_ts), b.bucketing_ts) AS bucketing_ts
  FROM  `etsy-data-warehouse-dev.tsebastian.ab_first_bucket_initial` b
  LEFT JOIN `etsy-data-warehouse-prod.catapult_unified.filtering_event` f
    ON f.bucketing_id = b.bucketing_id
    AND f._date BETWEEN start_date AND end_date
    AND f.experiment_id = my_experiment
    AND f.event_ts >= f.boundary_start_ts
    AND f.event_ts >= b.bucketing_ts
  GROUP BY 
    b.bucketing_id, b.variant_id, b.bucketing_ts
;


CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.tsebastian.subsequent_visits` 
OPTIONS(expiration_timestamp =  current_timestamp + interval 7 day )
AS
 
  -- Browser-based experiments (bucketing_id_type = 1)
  SELECT
    b.bucketing_id,
    b.variant_id,
    v.visit_id,
    v.user_id
  FROM `etsy-data-warehouse-dev.tsebastian.ab_first_bucket`  b
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
    v.visit_id,
    v.user_id
  FROM `etsy-data-warehouse-dev.tsebastian.ab_first_bucket`  b
  JOIN `etsy-data-warehouse-prod.weblog.visits` v
    ON bucketing_id_type = 2
    AND b.bucketing_id = CAST(v.user_id AS STRING)
    AND TIMESTAMP_TRUNC(b.bucketing_ts, SECOND) <= v.end_datetime
  WHERE v._date BETWEEN start_date AND end_date
;

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.tsebastian.aggregated_results` 
OPTIONS(expiration_timestamp =  current_timestamp + interval 7 day )
 AS 
  SELECT 
    visit.variant_id, 
    candidate_set,
    COUNT(rdl.visit_id) AS count_recs_delivered,
    ROUND(COUNT(rdl.visit_id) / SUM(COUNT(rdl.visit_id)) OVER (PARTITION BY variant_id) * 100, 2) AS pct_of_variant_recs,
      round(avg(rdl.rec_price), 2)  as avg_seen_price,
  round(avg(rdl.clicked), 5) * 100  as avg_clicked,
  round(avg(rdl.added_to_cart), 5) * 100 as avg_add_to_cart,
  round(avg(rdl.favorited), 5) * 100 as avg_favorited,
  round(avg(rdl.purchased_after_view), 5) * 100  as avg_purchased_after_view
  FROM `etsy-data-warehouse-dev.tsebastian.subsequent_visits`  visit
  JOIN `etsy-data-warehouse-prod.rollups.recsys_delivered_listings` rdl 
    USING(visit_id)
  WHERE module_placement = module_placement_input
  GROUP BY 1, 2
;

SELECT *
FROM `etsy-data-warehouse-dev.tsebastian.aggregated_results` 
-- WHERE pct_of_variant_recs > 1

ORDER BY 1, 2;
