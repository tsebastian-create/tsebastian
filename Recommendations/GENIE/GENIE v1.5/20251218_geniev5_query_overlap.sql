-- ===== PARAMETERS =====
DECLARE my_experiment STRING DEFAULT "boe_home/app_home.feed.genie_mvp.experiment_v5";
DECLARE start_date DATE DEFAULT '2025-12-06';
DECLARE end_date DATE DEFAULT '2025-12-11';
DECLARE module_placement_input STRING DEFAULT 'boe_homescreen_feed';

DECLARE bucketing_id_type INT64;
DECLARE excluded_dates ARRAY<DATE> DEFAULT [DATE '2025-12-09', DATE '2025-12-10'];

-- Get bucketing_id_type from Catapult
SET bucketing_id_type = (
  SELECT bucketing_id_type
  FROM `etsy-data-warehouse-prod.catapult_unified.experiment`
  WHERE _date = end_date
    AND experiment_id = my_experiment
  LIMIT 1
);

-- ===== 1. Experiment units & post-bucketing visits =====
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
    b.bucketing_id_type,
    b.variant_id,
    COALESCE(MIN(f.event_ts), b.bucketing_ts) AS bucketing_ts
  FROM ab_first_bucket_initial b
  LEFT JOIN `etsy-data-warehouse-prod.catapult_unified.filtering_event` f
    ON f.bucketing_id = b.bucketing_id
   AND f._date BETWEEN start_date AND end_date
   AND f.experiment_id = my_experiment
   AND f.event_ts >= f.boundary_start_ts
   AND f.event_ts >= b.bucketing_ts
  GROUP BY b.bucketing_id, b.bucketing_id_type, b.variant_id, b.bucketing_ts
),

subsequent_visits AS (
  -- User-based (bucketing_id_type = 2)
  SELECT
    b.bucketing_id,
    b.variant_id,
    v.visit_id,
    v.user_id,
    v._date,
    v.start_datetime,
    v.end_datetime
  FROM ab_first_bucket b
  JOIN `etsy-data-warehouse-prod.weblog.visits` v
    ON bucketing_id_type = 2
   AND b.bucketing_id = CAST(v.user_id AS STRING)
   AND TIMESTAMP_TRUNC(b.bucketing_ts, SECOND) <= v.end_datetime
  WHERE v._date BETWEEN start_date AND end_date
    AND (v._date NOT IN UNNEST(excluded_dates))
),

-- ===== 2. Feed taxonomies per visit (L0/L1/L2) + feed engagement =====
precomputed_taxonomy AS (
  SELECT
    listing_id,

    SPLIT(full_path, '.')[SAFE_OFFSET(0)] AS l0_category,
    SPLIT(full_path, '.')[SAFE_OFFSET(1)] AS l1_category,
    SPLIT(full_path, '.')[SAFE_OFFSET(2)] AS l2_category,
    full_path
  FROM `etsy-data-warehouse-prod.materialized.listing_taxonomy`
),

feed_taxos_per_visit AS (
  SELECT
    v.visit_id,
    v.variant_id,
    -- Distinct categories seen on the feed in this visit
    ARRAY_AGG(DISTINCT pt.l0_category IGNORE NULLS) AS feed_l0_taxos,
    ARRAY_AGG(DISTINCT pt.l1_category IGNORE NULLS) AS feed_l1_taxos,
    ARRAY_AGG(DISTINCT pt.l2_category IGNORE NULLS) AS feed_l2_taxos,
    -- Any feed click in this visit?
    MAX(CAST(rdl.clicked AS INT64)) AS feed_engaged_flag
  FROM subsequent_visits v
  JOIN `etsy-data-warehouse-prod.rollups.recsys_delivered_listings` rdl
    USING (visit_id)
  LEFT JOIN precomputed_taxonomy pt
    ON pt.listing_id = rdl.listing_id
  WHERE rdl.module_placement = module_placement_input
    AND rdl.seen = 1
    AND rdl._date BETWEEN start_date AND end_date
    AND (rdl._date NOT IN UNNEST(excluded_dates))
  GROUP BY v.visit_id, v.variant_id
),

-- ===== 3. Search queries with taxonomy levels per visit =====
-- Map query classified_taxonomy_id -> L0/L1/L2 via structured taxonomy
query_taxonomy_levels AS (
  SELECT
    taxonomy_id,
    SPLIT(full_path, '.')[SAFE_OFFSET(0)] AS l0_category,
    SPLIT(full_path, '.')[SAFE_OFFSET(1)] AS l1_category,
    SPLIT(full_path, '.')[SAFE_OFFSET(2)] AS l2_category,
    full_path
  FROM `etsy-data-warehouse-prod.structured_data.taxonomy`
),

search_queries AS (
  SELECT
    v.visit_id,
    v.variant_id,
    q.start_epoch_ms,
    TIMESTAMP_MILLIS(q.start_epoch_ms) AS query_ts,
    q.query AS raw_query,
    q.classified_taxonomy_id,
    qt.l0_category AS query_l0_taxo,
    qt.l1_category AS query_l1_taxo,
    qt.l2_category AS query_l2_taxo,
    qt.full_path AS query_full_path_taxo,
    -- performance metrics from query_sessions_new
    q.attributed_gms,
    q.has_click
  FROM subsequent_visits v
  JOIN `etsy-data-warehouse-prod.search.query_sessions_new` q
    ON q.visit_id = v.visit_id
   AND q._date BETWEEN start_date AND end_date
   AND (q._date NOT IN UNNEST(excluded_dates))
   AND q.classified_taxonomy_id IS NOT NULL
  LEFT JOIN query_taxonomy_levels qt
    ON qt.taxonomy_id = q.classified_taxonomy_id
  -- only consider in-visit queries
  WHERE TIMESTAMP_MILLIS(q.start_epoch_ms) BETWEEN v.start_datetime AND v.end_datetime
),

-- ===== 4. Join search queries to feed-taxo summary & compute L0/L1/L2 overlap =====
search_vs_feed AS (
  SELECT
    sq.visit_id,
    sq.variant_id,
    sq.raw_query,
    sq.query_full_path_taxo,
    sq.query_l0_taxo,
    sq.query_l1_taxo,
    sq.query_l2_taxo,
    ft.feed_l0_taxos,
    ft.feed_l1_taxos,
    ft.feed_l2_taxos,
    ft.feed_engaged_flag,
    sq.attributed_gms,
    sq.has_click,

    -- L0 overlap flag
    IF(
      sq.query_l0_taxo IS NOT NULL
      AND ft.feed_l0_taxos IS NOT NULL
      AND sq.query_l0_taxo IN UNNEST(ft.feed_l0_taxos),
      1, 0
    ) AS overlap_flag_l0,

    -- L1 overlap flag
    IF(
      sq.query_l1_taxo IS NOT NULL
      AND ft.feed_l1_taxos IS NOT NULL
      AND sq.query_l1_taxo IN UNNEST(ft.feed_l1_taxos),
      1, 0
    ) AS overlap_flag_l1,

    -- L2 overlap flag
    IF(
      sq.query_l2_taxo IS NOT NULL
      AND ft.feed_l2_taxos IS NOT NULL
      AND sq.query_l2_taxo IN UNNEST(ft.feed_l2_taxos),
      1, 0
    ) AS overlap_flag_l2

  FROM search_queries sq
  LEFT JOIN feed_taxos_per_visit ft
    USING (visit_id, variant_id)
)

===== 5. Aggregate: L0/L1/L2 overlap by variant x feed engagement stratum =====
SELECT
  variant_id,
  -- CASE
  --   WHEN feed_engaged_flag IS NULL THEN 'no_feed_seen'
  --   WHEN feed_engaged_flag = 0 THEN 'feed_seen_no_click'
  --   ELSE 'feed_engaged'
  -- END AS feed_engagement_stratum,

  COUNT(*) AS num_searches,
  COUNT(DISTINCT visit_id) AS visits_with_search,

  -- Overlap rates for each level
  AVG(overlap_flag_l0) AS query_l0_in_feed_l0_rate,
  AVG(overlap_flag_l1) AS query_l1_in_feed_l1_rate,
  AVG(overlap_flag_l2) AS query_l2_in_feed_l2_rate,

  -- Overall query-level performance
  SUM(attributed_gms) AS total_attributed_gms,
  AVG(attributed_gms) AS avg_attributed_gms_per_search,
  SUM(has_click) AS num_searches_with_click,
  AVG(has_click) AS has_click_rate,

  -- L0 overlap metrics
  SUM(CASE WHEN overlap_flag_l0 = 1 THEN attributed_gms ELSE 0 END)
    AS total_attributed_gms_l0_overlap,
  AVG(CASE WHEN overlap_flag_l0 = 1 THEN attributed_gms ELSE NULL END)
    AS avg_attributed_gms_per_search_l0_overlap,
  SUM(CASE WHEN overlap_flag_l0 = 1 THEN has_click ELSE 0 END)
    AS num_searches_with_click_l0_overlap,
  AVG(CASE WHEN overlap_flag_l0 = 1 THEN has_click ELSE NULL END)
    AS has_click_rate_l0_overlap,

  -- L1 overlap metrics
  SUM(CASE WHEN overlap_flag_l1 = 1 THEN attributed_gms ELSE 0 END)
    AS total_attributed_gms_l1_overlap,
  AVG(CASE WHEN overlap_flag_l1 = 1 THEN attributed_gms ELSE NULL END)
    AS avg_attributed_gms_per_search_l1_overlap,
  SUM(CASE WHEN overlap_flag_l1 = 1 THEN has_click ELSE 0 END)
    AS num_searches_with_click_l1_overlap,
  AVG(CASE WHEN overlap_flag_l1 = 1 THEN has_click ELSE NULL END)
    AS has_click_rate_l1_overlap,

  -- L2 overlap metrics
  SUM(CASE WHEN overlap_flag_l2 = 1 THEN attributed_gms ELSE 0 END)
    AS total_attributed_gms_l2_overlap,
  AVG(CASE WHEN overlap_flag_l2 = 1 THEN attributed_gms ELSE NULL END)
    AS avg_attributed_gms_per_search_l2_overlap,
  SUM(CASE WHEN overlap_flag_l2 = 1 THEN has_click ELSE 0 END)
    AS num_searches_with_click_l2_overlap,
  AVG(CASE WHEN overlap_flag_l2 = 1 THEN has_click ELSE NULL END)
    AS has_click_rate_l2_overlap

FROM search_vs_feed
GROUP BY variant_id
-- , feed_engagement_stratum
ORDER BY variant_id
-- , feed_engagement_stratum;


--------- when listings DO NOT overlap

-- SELECT
--   variant_id,
--   -- CASE
--   --   WHEN feed_engaged_flag IS NULL THEN 'no_feed_seen'
--   --   WHEN feed_engaged_flag = 0 THEN 'feed_seen_no_click'
--   --   ELSE 'feed_engaged'
--   -- END AS feed_engagement_stratum,

--   COUNT(*) AS num_searches,
--   COUNT(DISTINCT visit_id) AS visits_with_search,

--   -- Overlap rates for each level
--   AVG(overlap_flag_l0) AS query_l0_in_feed_l0_rate,
--   AVG(overlap_flag_l1) AS query_l1_in_feed_l1_rate,
--   AVG(overlap_flag_l2) AS query_l2_in_feed_l2_rate,

--   -- Overall query-level performance
--   SUM(attributed_gms) AS total_attributed_gms,
--   AVG(attributed_gms) AS avg_attributed_gms_per_search,
--   SUM(has_click) AS num_searches_with_click,
--   AVG(has_click) AS has_click_rate,

--   -- L0 overlap metrics
--   SUM(CASE WHEN overlap_flag_l0 = 1 THEN 0 ELSE attributed_gms END)
--     AS total_attributed_gms_l0_overlap,
--   AVG(CASE WHEN overlap_flag_l0 = 1 THEN NULL ELSE attributed_gms END)
--     AS avg_attributed_gms_per_search_l0_overlap,
--   SUM(CASE WHEN overlap_flag_l0 = 1 THEN 0 ELSE has_click END)
--     AS num_searches_with_click_l0_overlap,
--   AVG(CASE WHEN overlap_flag_l0 = 1 THEN has_click ELSE NULL END)
--     AS has_click_rate_l0_overlap,

--   -- L1 overlap metrics
--   SUM(CASE WHEN overlap_flag_l1 = 1 THEN 0 ELSE attributed_gms END)
--     AS total_attributed_gms_l1_overlap,
--   AVG(CASE WHEN overlap_flag_l1 = 1 THEN NULL ELSE attributed_gms END)
--     AS avg_attributed_gms_per_search_l1_overlap,
--   SUM(CASE WHEN overlap_flag_l1 = 1 THEN 0 ELSE has_click END)
--     AS num_searches_with_click_l1_overlap,
--   AVG(CASE WHEN overlap_flag_l1 = 1 THEN NULL ELSE has_click END)
--     AS has_click_rate_l1_overlap,

--   -- L2 overlap metrics
--   SUM(CASE WHEN overlap_flag_l2 = 1 THEN 0 ELSE attributed_gms END)
--     AS total_attributed_gms_l2_overlap,
--   AVG(CASE WHEN overlap_flag_l2 = 1 THEN NULL ELSE attributed_gms END)
--     AS avg_attributed_gms_per_search_l2_overlap,
--   SUM(CASE WHEN overlap_flag_l2 = 1 THEN 0 ELSE has_click END)
--     AS num_searches_with_click_l2_overlap,
--   AVG(CASE WHEN overlap_flag_l2 = 1 THEN NULL ELSE has_click END)
--     AS has_click_rate_l2_overlap

-- FROM search_vs_feed
-- GROUP BY variant_id
-- -- , feed_engagement_stratum
-- ORDER BY variant_id
-- -- , feed_engagement_stratum; 