DECLARE my_experiment STRING default @MY_EXPERIMENT;
DECLARE start_date DATE default @START_DATE;
DECLARE end_date DATE default @END_DATE;
DECLARE lookback_window INT64 DEFAULT 60;
DECLARE module_placement_input STRING default @MODULE_PLACEMENT_INPUT;
DECLARE bucketing_id_type INT64;

SET bucketing_id_type = (
  SELECT bucketing_id_type
  FROM `etsy-data-warehouse-prod.catapult_unified.experiment`
  WHERE _date = end_date
    AND experiment_id = my_experiment
);

WITH ab_first_bucket_initial AS (
  SELECT
    bucketing_id,
    bucketing_id_type,
    variant_id,
    MIN(bucketing_ts) AS bucketing_ts,
  FROM `etsy-data-warehouse-prod.catapult_unified.bucketing`
  WHERE _date BETWEEN start_date AND end_date
    AND experiment_id = my_experiment
  GROUP BY bucketing_id, bucketing_id_type, variant_id
),

ab_first_bucket AS (
  SELECT
    b.bucketing_id,
    b.variant_id,
    COALESCE(MIN(f.event_ts), b.bucketing_ts) AS bucketing_ts
  FROM ab_first_bucket_initial b
  LEFT JOIN `etsy-data-warehouse-prod.catapult_unified.filtering_event` f
    ON f.bucketing_id = b.bucketing_id
    AND f._date BETWEEN start_date AND end_date
    AND f.experiment_id = my_experiment
    AND f.event_ts >= f.boundary_start_ts
    AND f.event_ts >= b.bucketing_ts
  GROUP BY 
    b.bucketing_id, b.variant_id, b.bucketing_ts
),

subsequent_visits AS (
  -- Browser-based experiments (bucketing_id_type = 1)
  SELECT
    b.bucketing_id,
    b.variant_id,
    v.visit_id,
    v.user_id
  FROM ab_first_bucket b
  JOIN `etsy-data-warehouse-prod.weblog.visits` v
    ON bucketing_id_type = 1 
    AND b.bucketing_id = v.browser_id
    AND TIMESTAMP_TRUNC(b.bucketing_ts, SECOND) <= v.end_datetime
    and v._date BETWEEN start_date AND end_date
  
  UNION ALL
  
  -- User-based experiments (bucketing_id_type = 2)
  SELECT
    b.bucketing_id,
    b.variant_id,
    v.visit_id,
    v.user_id
  FROM ab_first_bucket b
  JOIN `etsy-data-warehouse-prod.weblog.visits` v
    ON bucketing_id_type = 2
    AND b.bucketing_id = CAST(v.user_id AS STRING)
    AND TIMESTAMP_TRUNC(b.bucketing_ts, SECOND) <= v.end_datetime
    and v._date BETWEEN start_date AND end_date
),

precomputed_taxonomy AS (
  SELECT
    listing_id,
    full_path,
    REGEXP_EXTRACT(full_path, r'^[^.]*') AS l1,
    REGEXP_EXTRACT(full_path, r'^[^.]*\.[^.]*') AS l2,
    REGEXP_EXTRACT(full_path, r'^[^.]*\.[^.]*\.[^.]*') AS l3
  FROM `etsy-data-warehouse-prod.materialized.listing_taxonomy`
),

recent_user_taxos_seen AS (
  SELECT DISTINCT
    v.user_id,
    pt.l1,
    pt.l2,
    pt.l3,
    pt.full_path
  FROM `etsy-data-warehouse-prod.analytics.listing_views` lv
  JOIN `etsy-data-warehouse-prod.visit_mart.visits` v2 
    ON v2.visit_id = lv.visit_id
  JOIN subsequent_visits v
    ON v2.user_id = v.user_id
  LEFT JOIN precomputed_taxonomy pt
    ON pt.listing_id = lv.listing_id
  WHERE lv._date > DATE_SUB(start_date, INTERVAL lookback_window DAY)
    AND lv._date <= start_date
),

final_per_user AS (
  SELECT 
    v.variant_id,
    v.user_id,
    COUNT(DISTINCT CASE WHEN recent.full_path IS NULL AND rdl.clicked = 1 THEN pt.full_path END) AS distinct_full_path_new_clicked,
    COUNT(DISTINCT CASE WHEN recent.full_path IS NULL AND rdl.seen = 1 THEN pt.full_path END) AS distinct_full_path_new_seen,
    SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN recent.full_path IS NULL AND rdl.clicked = 1 THEN pt.full_path END), 
                COUNT(DISTINCT CASE WHEN recent.full_path IS NULL AND rdl.seen = 1 THEN pt.full_path END)) AS distinct_full_path_new_ctr,

  FROM subsequent_visits v 
  LEFT JOIN `etsy-data-warehouse-prod.rollups.recsys_delivered_listings` rdl
    ON rdl.visit_id = v.visit_id
    AND rdl._date > start_date
    AND rdl._date <= end_date 
  LEFT JOIN precomputed_taxonomy pt
    ON pt.listing_id = rdl.listing_id
  JOIN recent_user_taxos_seen recent_active
    ON recent_active.user_id = v.user_id
  LEFT JOIN recent_user_taxos_seen recent
    ON recent.user_id = v.user_id
    AND recent.full_path = pt.full_path
  --WHERE rdl.module_placement = module_placement_input
  GROUP BY 1, 2
)

SELECT
  variant_id,
  COUNT(*) AS n_users,
  ROUND(AVG(distinct_full_path_new_clicked), 4) AS avg_distinct_new_full_path_clicked_per_user,
  ROUND(SAFE_DIVIDE(SUM(distinct_full_path_new_clicked), SUM(distinct_full_path_new_seen)),4) AS avg_distinct_new_full_path_ctr_per_variant,
  ROUND(AVG(distinct_full_path_new_ctr), 4) AS avg_distinct_new_full_path_ctr_per_user,
FROM final_per_user
GROUP BY 1
ORDER BY 1 DESC;