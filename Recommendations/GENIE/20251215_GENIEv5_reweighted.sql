DECLARE config_flag_param STRING DEFAULT "boe_home/app_home.feed.genie_mvp.experiment_v5";

DECLARE start_date DATE DEFAULT '2025-12-06';  
DECLARE end_date   DATE DEFAULT '2025-12-11'; 

DECLARE is_event_filtered BOOL;  
DECLARE bucketing_id_type INT64;

----- START -----
IF start_date IS NULL OR end_date IS NULL THEN
    SET (start_date, end_date) = (
        SELECT AS STRUCT
            MAX(DATE(boundary_start_ts)) AS start_date,
            MAX(_date) AS end_date,
        FROM
            `etsy-data-warehouse-prod.catapult_unified.experiment`
        WHERE
            experiment_id = config_flag_param
    );
END IF;

IF is_event_filtered IS NULL THEN
    SET (is_event_filtered, bucketing_id_type) = (
        SELECT AS STRUCT
            is_filtered,
            bucketing_id_type,
        FROM
            `etsy-data-warehouse-prod.catapult_unified.experiment`
        WHERE
            _date = end_date
            AND experiment_id = config_flag_param
    );
ELSE
    SET bucketing_id_type = (
        SELECT
            bucketing_id_type,
        FROM
            `etsy-data-warehouse-prod.catapult_unified.experiment`
        WHERE
            _date = end_date
            AND experiment_id = config_flag_param
    );
END IF;


-------------------------------------------------------------------------------------------
-- BUCKETING DATA
-------------------------------------------------------------------------------------------
-- Get the first bucketing moment for each experimental unit (e.g. browser or user).
-- If is_event_filtered is true, then only select experimental unit whose `filtered_bucketing_ts` is defined.
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.tsebastian.ab_first_bucket` 
OPTIONS(expiration_timestamp =  current_timestamp + interval 7 day )AS (
    SELECT
        bucketing_id,
        bucketing_id_type,
        variant_id,
        IF(is_event_filtered, filtered_bucketing_ts, bucketing_ts) AS bucketing_ts,
    FROM
        `etsy-data-warehouse-prod.catapult_unified.bucketing_period`
    WHERE
        _date = end_date
        AND experiment_id = config_flag_param
        AND ((NOT is_event_filtered) OR (filtered_bucketing_ts IS NOT NULL))
          AND DATE(
          IF(is_event_filtered, filtered_bucketing_ts, bucketing_ts)
        ) NOT IN (DATE '2025-12-09', DATE '2025-12-10') --  filter out buyers who were bucketed on affected dates
);

-------------------------------------------------------------------------------------------
-- SEGMENT DATA
-------------------------------------------------------------------------------------------
-- For each bucketing_id and variant_id, output one row with their segment assignments.
-- Each additional column will be a different segmentation, and the value will be the segment for each
-- bucketing_id at the time they were first bucketed into the experiment date range being
-- analyzed.
-- Example output (using the same example data above):
-- bucketing_id | variant_id | buyer_segment | canonical_region
-- 123          | off        | New           | FR
-- 456          | on         | Habitual      | US
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.tsebastian.first_bucket_segments` 
OPTIONS(expiration_timestamp =  current_timestamp + interval 7 day )AS (
    WITH first_bucket_segments_unpivoted AS (
        SELECT
            bucketing_id,
            variant_id,
            event_id,
            IF(is_event_filtered, filtered_event_value, event_value) AS event_value
        FROM
            `etsy-data-warehouse-prod.catapult_unified.aggregated_segment_event`
        WHERE
            _date = end_date
            AND experiment_id = config_flag_param
            -- <SEGMENTATION> Here you can specify whatever segmentations you'd like to analyze.
            -- !!! Please keep this in sync with the PIVOT statement below !!!
            -- For all supported segmentations, see go/catapult-unified-docs.
            AND event_id IN (
                "buyer_segment"
            )
            AND ((NOT is_event_filtered) OR (filtered_bucketing_ts IS NOT NULL))
    )
    SELECT
        *
    FROM
        first_bucket_segments_unpivoted
    PIVOT(
        MAX(event_value)
        FOR event_id IN (
            "buyer_segment"
        )
    )
);

-------------------------------------------------------------------------------------------
-- EVENT AND GMS DATA
-------------------------------------------------------------------------------------------


CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.tsebastian.events`
OPTIONS(expiration_timestamp = current_timestamp + interval 7 day ) AS (
  SELECT *
  FROM UNNEST([
            "total_winsorized_gms", -- winsorized acbv
            "feedRecListingviewEngagement", 
            "gms",                   --  gms data is in cents
            "backend_cart_payment", -- conversion rate
            "backend_add_to_cart", -- add to cart rate
            "backend_favorite_item2", -- favorites
            "search",
            "engaged_visit",
            "visits",
            'total_search_effort'
  ]) AS event_id
);
-- Get all the bucketed units with the events of interest.
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.tsebastian.events_per_unit`
OPTIONS(expiration_timestamp = current_timestamp + interval 7 day ) AS (
  WITH events_raw AS (
    -- First leg: from start_date up to 2025‑12‑08 (inclusive)
    SELECT
      bucketing_id,
      variant_id,
      event_id,
      IF(is_event_filtered, filtered_event_value, event_value) AS event_value
    FROM
      `etsy-data-warehouse-prod.catapult_unified.aggregated_event_func`(start_date, DATE '2025-12-08')
    WHERE
      experiment_id = config_flag_param
      AND ((NOT is_event_filtered) OR (filtered_bucketing_ts IS NOT NULL))

    UNION ALL

    -- Second leg: from 2025‑12‑11 to end_date (inclusive)
    SELECT
      bucketing_id,
      variant_id,
      event_id,
      IF(is_event_filtered, filtered_event_value, event_value) AS event_value
    FROM
      `etsy-data-warehouse-prod.catapult_unified.aggregated_event_func`(DATE '2025-12-11', end_date)
    WHERE
      experiment_id = config_flag_param
      AND ((NOT is_event_filtered) OR (filtered_bucketing_ts IS NOT NULL))
  )
  SELECT
    e.bucketing_id,
    e.variant_id,
    e.event_id,
    SUM(e.event_value) AS event_value
  FROM
    events_raw e
  JOIN
    `etsy-data-warehouse-dev.tsebastian.events` ev
  USING (event_id)
  JOIN `etsy-data-warehouse-dev.tsebastian.ab_first_bucket` b   --  limit to non‑9/10 bucketed units
      USING (bucketing_id, variant_id)
  GROUP BY
    bucketing_id, variant_id, event_id
);
-------------------------------------------------------------------------------------------
-- VISIT COUNT
-------------------------------------------------------------------------------------------

-- Get all post-bucketing visits for each experimental unit
IF bucketing_id_type = 1 THEN -- browser data 
  CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.tsebastian.subsequent_visits` AS (
    SELECT
      b.bucketing_id,
      b.variant_id,
      v.visit_id,
    FROM
      `etsy-data-warehouse-dev.tsebastian.ab_first_bucket` b
    JOIN
      `etsy-data-warehouse-prod.weblog.visits` v
      ON b.bucketing_id = v.browser_id
      AND TIMESTAMP_TRUNC(bucketing_ts, SECOND) <= v.end_datetime
    WHERE
      v._date BETWEEN start_date AND end_date
      AND v._date NOT IN (DATE '2025-12-09', DATE '2025-12-10')     --- excluding affected dates
  );
ELSEIF bucketing_id_type = 2 THEN -- user data
  CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.tsebastian.subsequent_visits` 
  OPTIONS(expiration_timestamp = current_timestamp + interval 7 day )AS (
    SELECT
      b.bucketing_id,
      b.variant_id,
      v.visit_id,
    FROM
      `etsy-data-warehouse-dev.tsebastian.ab_first_bucket` b
    JOIN
      `etsy-data-warehouse-prod.weblog.visits` v
      ON b.bucketing_id = CAST(v.user_id AS STRING)
      AND TIMESTAMP_TRUNC(bucketing_ts, SECOND) <= v.end_datetime
    WHERE
      v._date BETWEEN start_date AND end_date
      AND v._date NOT IN (DATE '2025-12-09', DATE '2025-12-10')     --- excluding affected dates
  );
END IF;

-- Get visit count per experimental unit
-- CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.tsebastian.visits_per_unit` 
-- OPTIONS(expiration_timestamp =  current_timestamp + interval 7 day )AS (
--     SELECT
--         bucketing_id,
--         variant_id,
--         COUNT(*) AS visit_count,
--     FROM
--         `etsy-data-warehouse-dev.tsebastian.subsequent_visits`
--     GROUP BY
--         bucketing_id, variant_id
-- );

-------------------------------------------------------------------------------------------
-- COMBINE BUCKETING, EVENT & SEGMENT DATA (PER UNIT)
-------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.tsebastian.all_units_events_segments`
OPTIONS(expiration_timestamp = current_timestamp + interval 7 day ) AS (
  SELECT
    b.bucketing_id,
    b.variant_id,
    ev.event_id,
    COALESCE(e.event_value, 0) AS event_count,  -- total per unit over boundary
    s.buyer_segment
  FROM
    `etsy-data-warehouse-dev.tsebastian.ab_first_bucket` b
  -- JOIN
  --   `etsy-data-warehouse-dev.tsebastian.visits_per_unit` v  -- filter >=1 visit on good days
  --   USING (bucketing_id, variant_id)
  CROSS JOIN
    `etsy-data-warehouse-dev.tsebastian.events` ev
  LEFT JOIN
    `etsy-data-warehouse-dev.tsebastian.events_per_unit` e
    ON b.bucketing_id = e.bucketing_id
   AND b.variant_id    = e.variant_id
   AND ev.event_id     = e.event_id
  JOIN
    `etsy-data-warehouse-dev.tsebastian.first_bucket_segments` s
        ON b.bucketing_id = s.bucketing_id
   AND b.variant_id    = s.variant_id
);
-------------------------------------------------------------------------------------------
-- RECREATE CATAPULT RESULTS
-------------------------------------------------------------------------------------------

-- Per-variant summary (no buyer_segment split)
-- RECREATE CATAPULT RESULTS: control vs genie variants, with p-values and power
WITH per_variant AS (
  SELECT
    event_id,
    variant_id,

    -- counts
    COUNT(*) AS n_total_units,
    SUM(IF(event_count > 0, 1, 0)) AS n_units_with_event,

    -- metrics
    AVG(IF(event_count = 0, 0, 1)) AS percent_units_with_event,
    AVG(event_count) AS avg_events_per_unit,
    AVG(IF(event_count = 0, NULL, event_count)) AS avg_events_per_unit_with_event,

    -- std devs for t-tests / power on means
    STDDEV_POP(event_count) AS sd_events_per_unit,
    STDDEV_POP(NULLIF(event_count, 0)) AS sd_events_per_unit_with_event
  FROM
    `etsy-data-warehouse-dev.tsebastian.all_units_events_segments`
  GROUP BY
    event_id, variant_id
),

control AS (
  SELECT *
  FROM per_variant
  WHERE variant_id = 'off'
),

genie_variants AS (
  SELECT *
  FROM per_variant
  WHERE variant_id IN ('50_genie_flash_50_mule', '50_genie_lite_50_mule')
)

SELECT
  v.event_id,
  v.variant_id,

  -- counts
  c.n_total_units AS control_units,
  v.n_total_units AS variant_units,

  -- 1) % units with ≥1 event
  c.percent_units_with_event AS control_pct_units_with_event,
  v.percent_units_with_event AS variant_pct_units_with_event,
  SAFE_DIVIDE(
    v.percent_units_with_event - c.percent_units_with_event,
    c.percent_units_with_event
  ) AS lift_pct_units_with_event,

  `etsy-data-warehouse-prod.functions.prop_test_agg`(
    c.n_total_units,            -- n_control
    v.n_total_units,            -- n_variant
    c.n_units_with_event,       -- successes_control
    v.n_units_with_event        -- successes_variant
  ).p_value AS pval_pct_units_with_event,

  `etsy-data-warehouse-prod.functions.power_two_proportions`(
    c.n_total_units,
    v.n_total_units,
    c.percent_units_with_event,
    v.percent_units_with_event
  ) AS power_pct_units_with_event,

  -- 2) Avg events per unit
  c.avg_events_per_unit AS control_avg_events_per_unit,
  v.avg_events_per_unit AS variant_avg_events_per_unit,
  SAFE_DIVIDE(
    v.avg_events_per_unit - c.avg_events_per_unit,
    c.avg_events_per_unit
  ) AS lift_avg_events_per_unit,

  `etsy-data-warehouse-prod.functions.t_test_agg`(
    v.n_total_units,            -- n_variant
    c.n_total_units,            -- n_control
    v.avg_events_per_unit,      -- mean_variant
    c.avg_events_per_unit,      -- mean_control
    v.sd_events_per_unit,       -- sd_variant
    c.sd_events_per_unit        -- sd_control
  ).p_value AS pval_avg_events_per_unit,

  `etsy-data-warehouse-prod.functions.power_two_means`(
    v.n_total_units,
    c.n_total_units,
    v.avg_events_per_unit - c.avg_events_per_unit,
    c.sd_events_per_unit
  ) AS power_avg_events_per_unit,

  -- 3) Avg events per unit (conditional on having event)
  c.avg_events_per_unit_with_event AS control_avg_events_per_unit_with_event,
  v.avg_events_per_unit_with_event AS variant_avg_events_per_unit_with_event,
  SAFE_DIVIDE(
    v.avg_events_per_unit_with_event - c.avg_events_per_unit_with_event,
    c.avg_events_per_unit_with_event
  ) AS lift_avg_events_per_unit_with_event,

  `etsy-data-warehouse-prod.functions.t_test_agg`(
    v.n_units_with_event,                 -- n_variant (only units with event)
    c.n_units_with_event,                 -- n_control
    v.avg_events_per_unit_with_event,     -- mean_variant
    c.avg_events_per_unit_with_event,     -- mean_control
    v.sd_events_per_unit_with_event,      -- sd_variant
    c.sd_events_per_unit_with_event       -- sd_control
  ).p_value AS pval_avg_events_per_unit_with_event,

  `etsy-data-warehouse-prod.functions.power_two_means`(
    v.n_units_with_event,
    c.n_units_with_event,
    v.avg_events_per_unit_with_event - c.avg_events_per_unit_with_event,
    c.sd_events_per_unit_with_event
  ) AS power_avg_events_per_unit_with_event

FROM
  genie_variants v
JOIN
  control c
USING (event_id)
ORDER BY
  event_id, variant_id;


-- split by buyer segment
  -- RECREATE CATAPULT RESULTS BY BUYER SEGMENT: control vs genie variants
WITH per_variant_seg AS (
  SELECT
    event_id,
    variant_id,
    buyer_segment,

    -- counts
    COUNT(*) AS n_total_units,
    SUM(IF(event_count > 0, 1, 0)) AS n_units_with_event,

    -- metrics
    AVG(IF(event_count = 0, 0, 1)) AS percent_units_with_event,
    AVG(event_count) AS avg_events_per_unit,
    AVG(IF(event_count = 0, NULL, event_count)) AS avg_events_per_unit_with_event,

    -- std devs
    STDDEV_POP(event_count) AS sd_events_per_unit,
    STDDEV_POP(NULLIF(event_count, 0)) AS sd_events_per_unit_with_event
  FROM
    `etsy-data-warehouse-dev.tsebastian.all_units_events_segments`
  GROUP BY
    event_id, variant_id, buyer_segment
),

control_seg AS (
  SELECT *
  FROM per_variant_seg
  WHERE variant_id = 'off'
),

genie_seg AS (
  SELECT *
  FROM per_variant_seg
  WHERE variant_id IN ('50_genie_flash_50_mule', '50_genie_lite_50_mule')
)

SELECT
  v.event_id,
  v.buyer_segment,
  v.variant_id,

  -- counts
  c.n_total_units AS control_units,
  v.n_total_units AS variant_units,

  -- 1) % units with ≥1 event
  c.percent_units_with_event AS control_pct_units_with_event,
  v.percent_units_with_event AS variant_pct_units_with_event,
  SAFE_DIVIDE(
    v.percent_units_with_event - c.percent_units_with_event,
    c.percent_units_with_event
  ) AS lift_pct_units_with_event,

  `etsy-data-warehouse-prod.functions.prop_test_agg`(
    c.n_total_units,          -- n_control
    v.n_total_units,          -- n_variant
    c.n_units_with_event,     -- successes_control
    v.n_units_with_event      -- successes_variant
  ).p_value AS pval_pct_units_with_event,

  `etsy-data-warehouse-prod.functions.power_two_proportions`(
    c.n_total_units,
    v.n_total_units,
    c.percent_units_with_event,
    v.percent_units_with_event
  ) AS power_pct_units_with_event,

  -- 2) Avg events per unit
  c.avg_events_per_unit AS control_avg_events_per_unit,
  v.avg_events_per_unit AS variant_avg_events_per_unit,
  SAFE_DIVIDE(
    v.avg_events_per_unit - c.avg_events_per_unit,
    c.avg_events_per_unit
  ) AS lift_avg_events_per_unit,

  `etsy-data-warehouse-prod.functions.t_test_agg`(
    v.n_total_units,          -- n_variant
    c.n_total_units,          -- n_control
    v.avg_events_per_unit,    -- mean_variant
    c.avg_events_per_unit,    -- mean_control
    v.sd_events_per_unit,     -- sd_variant
    c.sd_events_per_unit      -- sd_control
  ).p_value AS pval_avg_events_per_unit,

  `etsy-data-warehouse-prod.functions.power_two_means`(
    v.n_total_units,
    c.n_total_units,
    v.avg_events_per_unit - c.avg_events_per_unit,
    c.sd_events_per_unit
  ) AS power_avg_events_per_unit,

  -- 3) Avg events per unit (conditional on having event)
  c.avg_events_per_unit_with_event AS control_avg_events_per_unit_with_event,
  v.avg_events_per_unit_with_event AS variant_avg_events_per_unit_with_event,
  SAFE_DIVIDE(
    v.avg_events_per_unit_with_event - c.avg_events_per_unit_with_event,
    c.avg_events_per_unit_with_event
  ) AS lift_avg_events_per_unit_with_event,

  `etsy-data-warehouse-prod.functions.t_test_agg`(
    v.n_units_with_event,                 -- n_variant (only units with event)
    c.n_units_with_event,                 -- n_control
    v.avg_events_per_unit_with_event,     -- mean_variant
    c.avg_events_per_unit_with_event,     -- mean_control
    v.sd_events_per_unit_with_event,      -- sd_variant
    c.sd_events_per_unit_with_event       -- sd_control
  ).p_value AS pval_avg_events_per_unit_with_event,

  `etsy-data-warehouse-prod.functions.power_two_means`(
    v.n_units_with_event,
    c.n_units_with_event,
    v.avg_events_per_unit_with_event - c.avg_events_per_unit_with_event,
    c.sd_events_per_unit_with_event
  ) AS power_avg_events_per_unit_with_event

FROM
  genie_seg v
JOIN
  control_seg c
USING (event_id, buyer_segment)
ORDER BY
  event_id, buyer_segment, variant_id;




-------------------------------------------------------------------------------------------
-- PER-UNIT METRICS BY SEGMENT (INPUTS FOR REWEIGHTING)
-------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.tsebastian.segment_metrics_per_unit`
OPTIONS(expiration_timestamp = current_timestamp + interval 7 day ) AS (
  SELECT
    event_id,
    variant_id,
    buyer_segment,
    COUNT(*) AS num_units,
    AVG(event_count) AS avg_events_per_unit,  --  per-unit rate
    AVG(IF(event_count = 0, 0, 1)) AS pct_units_with_event
  FROM
    `etsy-data-warehouse-dev.tsebastian.all_units_events_segments`
  WHERE 1=1
  --  and event_id IN ("gms", "feedRecListingviewEngagement", "backend_cart_payment")
    -- and variant_id = '50_genie_flash_50_mule'
  GROUP BY
    ALL
    order by 1,2,3
);



-------------------------------------------------------------------------------------------
-- GENERAL FEED MAKEUP
-------------------------------------------------------------------------------------------

SELECT
  rdl.buyer_segment,
  COUNT(DISTINCT v.user_id) AS buyers_with_feed_seen
FROM `etsy-data-warehouse-prod.rollups.recsys_delivered_listings` AS rdl
JOIN `etsy-data-warehouse-prod.visit_mart.visits` AS v
  ON v.visit_id = rdl.visit_id 
  and v._date  BETWEEN '2025-12-06' AND '2025-12-11'
  and v._date not in (DATE '2025-12-09', DATE '2025-12-10') 
WHERE
  rdl._date BETWEEN '2025-12-06' AND '2025-12-11' and rdl._date not in (DATE '2025-12-09', DATE '2025-12-10') 
  AND rdl.module_placement = 'boe_homescreen_feed'   -- BOE home feed module
  AND rdl.platform = 'boe'                           -- BOE only
--   AND rdl.seen = 1                                   -- listing-level impression
  AND v.user_id IS NOT NULL                          -- signed-in buyers
GROUP BY 1
ORDER BY buyer_segment ;


-- 1) Create a table with feed buyer segment proportions

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.tsebastian.feed_segment_mix` AS
SELECT
  buyer_segment,
  feed_share
FROM UNNEST([
  STRUCT('Active'       AS buyer_segment, 0.18 AS feed_share),
  STRUCT('Habitual'     AS buyer_segment, 0.18 AS feed_share),
  STRUCT('High_Potential' AS buyer_segment, 0.03 AS feed_share),
  STRUCT('Not_Active'          AS buyer_segment, 0.18 AS feed_share),
  STRUCT('New'          AS buyer_segment, 0.01 AS feed_share),
  STRUCT('Repeat'       AS buyer_segment, 0.42 AS feed_share)
 
]);

-- 2) Reweight  per-unit segment metrics to the feed mix

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.tsebastian.reweighted_metrics_per_unit` AS
SELECT
  m.event_id,
  m.variant_id,   
  SUM(m.avg_events_per_unit * f.feed_share) AS reweighted_events_per_unit
FROM
  `etsy-data-warehouse-dev.tsebastian.segment_metrics_per_unit` m
JOIN
  `etsy-data-warehouse-dev.tsebastian.feed_segment_mix` f
USING (buyer_segment)
GROUP BY
  m.event_id,
  m.variant_id;




-------------------------------------------------------------------------------------------
-- GMS PER UNIT STATS BY BUYER SEGMENT (FOR POWER CALCS)
-------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.tsebastian.gms_per_unit_stats_by_segment`
OPTIONS(expiration_timestamp = current_timestamp + interval 7 day ) AS (
  SELECT
    buyer_segment,
    variant_id,
    COUNT(*) AS n_units,                             -- sample size for this segment + variant
    AVG(event_count) AS mean_gms_per_unit_cents,     -- mean GMS per unit over boundary 
    STDDEV_SAMP(event_count) AS sd_gms_per_unit_cents,
    -- convenience in dollars
    AVG(event_count) / 100.0 AS mean_gms_per_unit_dollars,
    STDDEV_SAMP(event_count) / 100.0 AS sd_gms_per_unit_dollars,
    SAFE_DIVIDE(
      STDDEV_SAMP(event_count),
      NULLIF(AVG(event_count), 0)
    ) AS cv_gms_per_unit_cents                       -- coefficient of variation
  FROM
    `etsy-data-warehouse-dev.tsebastian.all_units_events_segments`
  WHERE
    event_id = 'gms' 
    AND variant_id = 'off'
  GROUP BY
    buyer_segment,
    variant_id
);



-- select * From `etsy-data-warehouse-dev.tsebastian.gms_power_requirements_by_segment` 
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.tsebastian.gms_power_requirements_by_segment` AS
WITH power_inputs AS (
  SELECT
    buyer_segment,
    cv_gms_per_unit_cents AS cv,
    0.01 AS rel_mde,         -- 1% relative lift
    1.96 AS z_alpha_over_2,  -- two-sided alpha=0.05
    0.84 AS z_beta           -- power = 0.8
  FROM `etsy-data-warehouse-dev.tsebastian.gms_per_unit_stats_by_segment`
  WHERE variant_id = 'off'
),
per_segment_n AS (
  SELECT
    buyer_segment,
    cv,
    -- n per arm required to detect 1% lift in GMS / unit
    CEIL(
      2 * POW(z_alpha_over_2 + z_beta, 2)
      * POW(cv, 2)
      / POW(rel_mde, 2)
    ) AS min_n_per_arm_1pct_lift
  FROM power_inputs
)
SELECT *
FROM per_segment_n;



-- “minimum total N” for the experiment
WITH per_segment_n AS (
  SELECT
    buyer_segment,
    cv_gms_per_unit_cents AS cv,
    CEIL(
      2 * POW(1.96 + 0.84, 2)  -- z_alpha/2 + z_beta
      * POW(cv_gms_per_unit_cents, 2)
      / POW(0.01, 2)           -- 1% relative MDE
    ) AS min_n_per_arm_1pct_lift
  FROM `etsy-data-warehouse-dev.tsebastian.gms_per_unit_stats_by_segment`
  WHERE variant_id = 'off'
),
audience_checks AS (
  SELECT
    f.buyer_segment,
    n.min_n_per_arm_1pct_lift,

    -- Expected per-arm n if total audience is 1,000,000
    1000000 * 0.5 * f.feed_share AS n_per_arm_if_1M,

    -- Expected per-arm n if total audience is 30,000,000
    30000000 * 0.5 * f.feed_share AS n_per_arm_if_30M
  FROM `etsy-data-warehouse-dev.tsebastian.feed_segment_mix` f
  JOIN per_segment_n n USING (buyer_segment)
)
SELECT
  buyer_segment,
  min_n_per_arm_1pct_lift,
  n_per_arm_if_1M,
  n_per_arm_if_30M,
  -- flags for readability
  n_per_arm_if_1M  >= min_n_per_arm_1pct_lift AS powered_at_1M,
  n_per_arm_if_30M >= min_n_per_arm_1pct_lift AS powered_at_30M
FROM audience_checks
ORDER BY buyer_segment;