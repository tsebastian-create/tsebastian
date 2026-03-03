DECLARE my_experiment STRING DEFAULT 'boe_home/app_home.feed.genie_v2_filtered_nir';
DECLARE start_date DATE DEFAULT '2026-02-26';
DECLARE end_date DATE DEFAULT '2026-03-06';
DECLARE lookback_window INT64 DEFAULT 60;
DECLARE module_placement_input STRING DEFAULT 'boe_homescreen_feed';
DECLARE bucketing_id_type INT64;

SET bucketing_id_type = (
  SELECT bucketing_id_type
  FROM `etsy-data-warehouse-prod.catapult_unified.experiment`
  WHERE _date = '2026-02-27'
    AND experiment_id = my_experiment
);


CREATE or replace  TABLE  `etsy-data-warehouse-dev.tsebastian.genie_v2_newmissions`  AS
WITH ab_first_bucket_initial AS (
  SELECT
    bucketing_id,
    bucketing_id_type,
    variant_id,
    MIN(bucketing_ts) AS bucketing_ts
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
   AND v._date BETWEEN start_date AND end_date

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
   AND v._date BETWEEN start_date AND end_date
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

    -- L2 "new missions" (v1.5)
    COUNT(DISTINCT CASE
      WHEN pt.l2 IS NOT NULL
        AND recent_l2.l2 IS NULL
        AND rdl.clicked = 1
      THEN pt.l2 END
    ) AS distinct_l2_new_clicked,

    COUNT(DISTINCT CASE
      WHEN pt.l2 IS NOT NULL
        AND recent_l2.l2 IS NULL
        AND rdl.seen = 1
      THEN pt.l2 END
    ) AS distinct_l2_new_seen,

    SAFE_DIVIDE(
      COUNT(DISTINCT CASE
        WHEN pt.l2 IS NOT NULL
          AND recent_l2.l2 IS NULL
          AND rdl.clicked = 1
        THEN pt.l2 END
      ),
      COUNT(DISTINCT CASE
        WHEN pt.l2 IS NOT NULL
          AND recent_l2.l2 IS NULL
          AND rdl.seen = 1
        THEN pt.l2 END
      )
    ) AS distinct_l2_new_ctr,

    -- All L2 missions (not just new) – optional diagnostics
    COUNT(DISTINCT CASE
      WHEN rdl.clicked = 1 THEN pt.l2 END
    ) AS distinct_l2_all_clicked,

    COUNT(DISTINCT CASE
      WHEN rdl.seen = 1 THEN pt.l2 END
    ) AS distinct_l2_all_seen,

    SAFE_DIVIDE(
      COUNT(DISTINCT CASE
        WHEN rdl.clicked = 1 THEN pt.l2 END
      ),
      COUNT(DISTINCT CASE
        WHEN rdl.seen = 1 THEN pt.l2 END
      )
    ) AS distinct_l2_all_ctr

  FROM subsequent_visits v
  LEFT JOIN `etsy-data-warehouse-prod.rollups.recsys_delivered_listings` rdl
    ON rdl.visit_id = v.visit_id
   AND rdl._date >= start_date
   AND rdl._date <= end_date
  LEFT JOIN precomputed_taxonomy pt
    ON pt.listing_id = rdl.listing_id
  -- enforce "has listing view in past 60 days"
  JOIN recent_user_taxos_seen recent_active
    ON recent_active.user_id = v.user_id
  -- taxonomy-level history for "new" test
  LEFT JOIN recent_user_taxos_seen recent_l2
    ON recent_l2.user_id = v.user_id
   AND recent_l2.l2 = pt.l2
  WHERE rdl.module_placement = module_placement_input
  GROUP BY 1, 2
),

per_variant AS (
  SELECT
    variant_id,
    COUNT(*) AS n_users,

    -- L2 new missions (v1.5)
    AVG(distinct_l2_new_seen)    AS avg_new_seen_per_user,
    AVG(distinct_l2_new_clicked) AS avg_new_clicked_per_user,
    AVG(distinct_l2_new_ctr)     AS avg_new_ctr_per_user,
    STDDEV(distinct_l2_new_seen)    AS sd_new_seen_per_user,
    STDDEV(distinct_l2_new_clicked) AS sd_new_clicked_per_user,
    STDDEV(distinct_l2_new_ctr)     AS sd_new_ctr_per_user,

    -- All L2 missions (diagnostics)
    AVG(distinct_l2_all_seen)    AS avg_all_seen_per_user,
    AVG(distinct_l2_all_clicked) AS avg_all_clicked_per_user,
    AVG(distinct_l2_all_ctr)     AS avg_all_ctr_per_user,
    STDDEV(distinct_l2_all_seen)    AS sd_all_seen_per_user,
    STDDEV(distinct_l2_all_clicked) AS sd_all_clicked_per_user,
    STDDEV(distinct_l2_all_ctr)     AS sd_all_ctr_per_user

  FROM final_per_user
  GROUP BY variant_id
),

control AS (
  SELECT *
  FROM per_variant
  WHERE variant_id = 'off'
),

variants AS (
  SELECT *
  FROM per_variant
  WHERE variant_id != 'off'
)

SELECT
  v.variant_id,

  -- counts
  c.n_users AS control_users,
  v.n_users AS variant_users,

  -- avg distinct new L2 missions clicked per user (v1.5)
  c.avg_new_clicked_per_user AS control_avg_new_clicked_per_user,
  v.avg_new_clicked_per_user AS variant_avg_new_clicked_per_user,
  c.avg_new_seen_per_user    AS control_avg_new_seen_per_user,
  v.avg_new_seen_per_user    AS variant_avg_new_seen_per_user,

  SAFE_DIVIDE(
    v.avg_new_clicked_per_user - c.avg_new_clicked_per_user,
    c.avg_new_clicked_per_user
  ) AS lift_new_clicked_per_user,

  `etsy-data-warehouse-prod.functions.t_test_agg`(
    v.n_users,
    c.n_users,
    v.avg_new_clicked_per_user,
    c.avg_new_clicked_per_user,
    v.sd_new_clicked_per_user,
    c.sd_new_clicked_per_user
  ).p_value AS pval_new_clicked_per_user,

  `etsy-data-warehouse-prod.functions.power_two_means`(
    v.n_users,
    c.n_users,
    v.avg_new_clicked_per_user - c.avg_new_clicked_per_user,
    c.sd_new_clicked_per_user
  ) AS power_new_clicked_per_user,

  -- estimated additional days to reach 80% power on new-missions-clicks
  (
    SELECT
      MIN(k * (DATE_DIFF(end_date, start_date, DAY) + 1))
      - (DATE_DIFF(end_date, start_date, DAY) + 1)
    FROM UNNEST(GENERATE_ARRAY(1, 20)) AS k
    WHERE `etsy-data-warehouse-prod.functions.power_two_means`(
            k * v.n_users,
            k * c.n_users,
            v.avg_new_clicked_per_user - c.avg_new_clicked_per_user,
            c.sd_new_clicked_per_user
          ) >= 0.8
  ) AS est_additional_days_to_power_new_clicked_per_user,

  -- avg distinct new L2 missions CTR per user
  c.avg_new_ctr_per_user AS control_avg_new_ctr_per_user,
  v.avg_new_ctr_per_user AS variant_avg_new_ctr_per_user,

  SAFE_DIVIDE(
    v.avg_new_ctr_per_user - c.avg_new_ctr_per_user,
    c.avg_new_ctr_per_user
  ) AS lift_new_ctr_per_user,

  `etsy-data-warehouse-prod.functions.t_test_agg`(
    v.n_users,
    c.n_users,
    v.avg_new_ctr_per_user,
    c.avg_new_ctr_per_user,
    v.sd_new_ctr_per_user,
    c.sd_new_ctr_per_user
  ).p_value AS pval_new_ctr_per_user,

  `etsy-data-warehouse-prod.functions.power_two_means`(
    v.n_users,
    c.n_users,
    v.avg_new_ctr_per_user - c.avg_new_ctr_per_user,
    c.sd_new_ctr_per_user
  ) AS power_new_ctr_per_user

FROM variants v
CROSS JOIN control c
ORDER BY v.variant_id;