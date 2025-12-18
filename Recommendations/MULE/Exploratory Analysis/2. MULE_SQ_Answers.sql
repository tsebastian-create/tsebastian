------------------------------------------------------------
-- Q1. Distribution difference between listing categories delivered
------------------------------------------------------------

-- Distribution difference between categories (one row per category)
-- Control = 'off', Treatment = 'mule_sq_100'
WITH base AS (
  SELECT
    variant_id,
    COALESCE(rec_top_category, 'Unknown') AS category,
    COALESCE(rec_seen, 0) AS rec_seen
  FROM `etsy-data-warehouse-dev.tsebastian.bound_reset_seller_add` 
  WHERE variant_id IN ('off','mule_sq_100')
),
agg AS (
  SELECT
    category,
    variant_id,
    SUM(rec_seen) AS exposures
  FROM base
  GROUP BY 1,2
),
tot AS (
  SELECT variant_id, SUM(exposures) AS total_exposures
  FROM agg
  GROUP BY 1
),
shares AS (
  SELECT
    a.category,
    a.variant_id,
    a.exposures,
    SAFE_DIVIDE(a.exposures, t.total_exposures) AS exposure_share
  FROM agg a
  JOIN tot t USING (variant_id)
)
SELECT
  category,
  COALESCE(MAX(IF(variant_id='off', exposures, NULL)), 0) AS exposures_control,
  COALESCE(MAX(IF(variant_id='off', exposure_share, NULL)), 0) AS share_control,
  COALESCE(MAX(IF(variant_id='mule_sq_100', exposures, NULL)), 0) AS exposures_treatment,
  COALESCE(MAX(IF(variant_id='mule_sq_100', exposure_share, NULL)), 0) AS share_treatment,
  COALESCE(MAX(IF(variant_id='mule_sq_100', exposure_share, NULL)), 0)
    - COALESCE(MAX(IF(variant_id='off', exposure_share, NULL)), 0) AS delta_share,
  SAFE_DIVIDE(
    COALESCE(MAX(IF(variant_id='mule_sq_100', exposure_share, NULL)), 0)
      - COALESCE(MAX(IF(variant_id='off', exposure_share, NULL)), 0),
    NULLIF(COALESCE(MAX(IF(variant_id='off', exposure_share, NULL)), 0), 0)
  ) AS pct_change_vs_control
FROM shares
GROUP BY category
ORDER BY delta_share DESC;

-- Distribution difference between taxonomies (one row per taxonomy)
-- Control = 'off', Treatment = 'mule_sq_100'
WITH base AS (
  SELECT
    variant_id,
    COALESCE(listing_taxonomy_full_path, 'Unknown') AS taxonomy,
    COALESCE(rec_seen, 0) AS rec_seen
  FROM `etsy-data-warehouse-dev.tsebastian.bound_reset_seller_add` 
  WHERE variant_id IN ('off','mule_sq_100')
),
agg AS (
  SELECT
    taxonomy,
    variant_id,
    SUM(rec_seen) AS exposures
  FROM base
  GROUP BY 1,2
),
tot AS (
  SELECT variant_id, SUM(exposures) AS total_exposures
  FROM agg
  GROUP BY 1
),
shares AS (
  SELECT
    a.taxonomy,
    a.variant_id,
    a.exposures,
    SAFE_DIVIDE(a.exposures, t.total_exposures) AS exposure_share
  FROM agg a
  JOIN tot t USING (variant_id)
)
SELECT
  taxonomy,
  COALESCE(MAX(IF(variant_id='off', exposures, NULL)), 0) AS exposures_control,
  COALESCE(MAX(IF(variant_id='off', exposure_share, NULL)), 0) AS share_control,
  COALESCE(MAX(IF(variant_id='mule_sq_100', exposures, NULL)), 0) AS exposures_treatment,
  COALESCE(MAX(IF(variant_id='mule_sq_100', exposure_share, NULL)), 0) AS share_treatment,
  COALESCE(MAX(IF(variant_id='mule_sq_100', exposure_share, NULL)), 0)
    - COALESCE(MAX(IF(variant_id='off', exposure_share, NULL)), 0) AS delta_share,
  SAFE_DIVIDE(
    COALESCE(MAX(IF(variant_id='mule_sq_100', exposure_share, NULL)), 0)
      - COALESCE(MAX(IF(variant_id='off', exposure_share, NULL)), 0),
    NULLIF(COALESCE(MAX(IF(variant_id='off', exposure_share, NULL)), 0), 0)
  ) AS pct_change_vs_control
FROM shares
GROUP BY taxonomy
HAVING exposures_treatment >=100 AND exposures_control >=100
ORDER BY delta_share DESC limit 20;


------------------------------------------------------------
-- Q2. Distribution difference between listing categories delivered
------------------------------------------------------------

WITH base AS (
  SELECT
    variant_id,
    COALESCE(rec_top_category, 'Unknown') AS category,
    COALESCE(rec_seen, 0) AS rec_seen,
    COALESCE(rec_clicked, 0) AS rec_clicked,
    COALESCE(recs_engaged_click, 0) AS recs_engaged_click
  FROM `etsy-data-warehouse-dev.tsebastian.bound_reset_seller_add` 
  WHERE variant_id IN ('off','mule_sq_100')
),
agg AS (
  SELECT
    category,
    variant_id,
    SUM(rec_seen) AS exposures,
    SUM(rec_clicked) AS clicks,
    SUM(recs_engaged_click) AS engaged_clicks
  FROM base
  GROUP BY 1,2
)
SELECT
  category,
  -- Control metrics
  SUM(IF(variant_id='off', exposures, 0)) AS exposures_control,
  SUM(IF(variant_id='off', clicks, 0)) AS clicks_control,
  SUM(IF(variant_id='off', engaged_clicks, 0)) AS engaged_clicks_control,
  SAFE_DIVIDE(SUM(IF(variant_id='off', clicks, 0)), SUM(IF(variant_id='off', exposures, 0))) AS ctr_control,
  SAFE_DIVIDE(SUM(IF(variant_id='off', engaged_clicks, 0)), SUM(IF(variant_id='off', exposures, 0))) AS ecr_control,

  -- Treatment metrics
  SUM(IF(variant_id='mule_sq_100', exposures, 0)) AS exposures_treatment,
  SUM(IF(variant_id='mule_sq_100', clicks, 0)) AS clicks_treatment,
  SUM(IF(variant_id='mule_sq_100', engaged_clicks, 0)) AS engaged_clicks_treatment,
  SAFE_DIVIDE(SUM(IF(variant_id='mule_sq_100', clicks, 0)), SUM(IF(variant_id='mule_sq_100', exposures, 0))) AS ctr_treatment,
  SAFE_DIVIDE(SUM(IF(variant_id='mule_sq_100', engaged_clicks, 0)), SUM(IF(variant_id='mule_sq_100', exposures, 0))) AS ecr_treatment,

  -- Deltas and lifts
  (SAFE_DIVIDE(SUM(IF(variant_id='mule_sq_100', clicks, 0)), SUM(IF(variant_id='mule_sq_100', exposures, 0)))
   - SAFE_DIVIDE(SUM(IF(variant_id='off', clicks, 0)), SUM(IF(variant_id='off', exposures, 0)))) AS ctr_delta,
  SAFE_DIVIDE(
    (SAFE_DIVIDE(SUM(IF(variant_id='mule_sq_100', clicks, 0)), SUM(IF(variant_id='mule_sq_100', exposures, 0)))
     - SAFE_DIVIDE(SUM(IF(variant_id='off', clicks, 0)), SUM(IF(variant_id='off', exposures, 0)))),
    SAFE_DIVIDE(SUM(IF(variant_id='off', clicks, 0)), SUM(IF(variant_id='off', exposures, 0)))
  ) AS ctr_lift_pct,

  (SAFE_DIVIDE(SUM(IF(variant_id='mule_sq_100', engaged_clicks, 0)), SUM(IF(variant_id='mule_sq_100', exposures, 0)))
   - SAFE_DIVIDE(SUM(IF(variant_id='off', engaged_clicks, 0)), SUM(IF(variant_id='off', exposures, 0)))) AS ecr_delta,
  SAFE_DIVIDE(
    (SAFE_DIVIDE(SUM(IF(variant_id='mule_sq_100', engaged_clicks, 0)), SUM(IF(variant_id='mule_sq_100', exposures, 0)))
     - SAFE_DIVIDE(SUM(IF(variant_id='off', engaged_clicks, 0)), SUM(IF(variant_id='off', exposures, 0)))),
    SAFE_DIVIDE(SUM(IF(variant_id='off', engaged_clicks, 0)), SUM(IF(variant_id='off', exposures, 0)))
  ) AS ecr_lift_pct
FROM agg
GROUP BY category
ORDER BY ctr_lift_pct DESC;

--- SAME FOR taxonomy

WITH base AS (
  SELECT
    variant_id,
    COALESCE(listing_taxonomy_full_path, 'Unknown') AS taxonomy,
    COALESCE(rec_seen, 0) AS rec_seen,
    COALESCE(rec_clicked, 0) AS rec_clicked,
    COALESCE(recs_engaged_click, 0) AS recs_engaged_click
  FROM `etsy-data-warehouse-dev.tsebastian.bound_reset_seller_add` 
  WHERE variant_id IN ('off','mule_sq_100')
),
agg AS (
  SELECT
    taxonomy,
    variant_id,
    SUM(rec_seen) AS exposures,
    SUM(rec_clicked) AS clicks,
    SUM(recs_engaged_click) AS engaged_clicks
  FROM base
  GROUP BY 1,2
)
SELECT
  taxonomy,
  SUM(IF(variant_id='off', exposures, 0)) AS exposures_control,
  SUM(IF(variant_id='off', clicks, 0)) AS clicks_control,
  SUM(IF(variant_id='off', engaged_clicks, 0)) AS engaged_clicks_control,
  SAFE_DIVIDE(SUM(IF(variant_id='off', clicks, 0)), SUM(IF(variant_id='off', exposures, 0))) AS ctr_control,
  SAFE_DIVIDE(SUM(IF(variant_id='off', engaged_clicks, 0)), SUM(IF(variant_id='off', exposures, 0))) AS ecr_control,

  SUM(IF(variant_id='mule_sq_100', exposures, 0)) AS exposures_treatment,
  SUM(IF(variant_id='mule_sq_100', clicks, 0)) AS clicks_treatment,
  SUM(IF(variant_id='mule_sq_100', engaged_clicks, 0)) AS engaged_clicks_treatment,
  SAFE_DIVIDE(SUM(IF(variant_id='mule_sq_100', clicks, 0)), SUM(IF(variant_id='mule_sq_100', exposures, 0))) AS ctr_treatment,
  SAFE_DIVIDE(SUM(IF(variant_id='mule_sq_100', engaged_clicks, 0)), SUM(IF(variant_id='mule_sq_100', exposures, 0))) AS ecr_treatment,

  (SAFE_DIVIDE(SUM(IF(variant_id='mule_sq_100', clicks, 0)), SUM(IF(variant_id='mule_sq_100', exposures, 0)))
   - SAFE_DIVIDE(SUM(IF(variant_id='off', clicks, 0)), SUM(IF(variant_id='off', exposures, 0)))) AS ctr_delta,
  SAFE_DIVIDE(
    (SAFE_DIVIDE(SUM(IF(variant_id='mule_sq_100', clicks, 0)), SUM(IF(variant_id='mule_sq_100', exposures, 0)))
     - SAFE_Divide(SUM(IF(variant_id='off', clicks, 0)), SUM(IF(variant_id='off', exposures, 0)))),
    SAFE_DIVIDE(SUM(IF(variant_id='off', clicks, 0)), SUM(IF(variant_id='off', exposures, 0)))
  ) AS ctr_lift_pct,

  (SAFE_DIVIDE(SUM(IF(variant_id='mule_sq_100', engaged_clicks, 0)), SUM(IF(variant_id='mule_sq_100', exposures, 0)))
   - SAFE_DIVIDE(SUM(IF(variant_id='off', engaged_clicks, 0)), SUM(IF(variant_id='off', exposures, 0)))) AS ecr_delta,
  SAFE_DIVIDE(
    (SAFE_DIVIDE(SUM(IF(variant_id='mule_sq_100', engaged_clicks, 0)), SUM(IF(variant_id='mule_sq_100', exposures, 0)))
     - SAFE_DIVIDE(SUM(IF(variant_id='off', engaged_clicks, 0)), SUM(IF(variant_id='off', exposures, 0)))),
    SAFE_DIVIDE(SUM(IF(variant_id='off', engaged_clicks, 0)), SUM(IF(variant_id='off', exposures, 0)))
  ) AS ecr_lift_pct
FROM agg
GROUP BY taxonomy
HAVING clicks_treatment>= 100
ORDER BY ctr_lift_pct DESC limit 20;

------------------------------------------------------------
--- q3 : Difference in prices of listings, including price buckets
------------------------------------------------------------

WITH p AS (
  SELECT
    variant_id,
    APPROX_QUANTILES(rec_price, 100)[OFFSET(99)] AS p99
  FROM (select distinct rec_price, listing_id, variant_id from `etsy-data-warehouse-dev.tsebastian.mule_sq_1pct`   WHERE rec_seen = 1)

  GROUP BY 1
)
SELECT
  t.variant_id,
  AVG(t.rec_price) AS avg_rec_price
FROM (select distinct rec_price, listing_id, variant_id from `etsy-data-warehouse-dev.tsebastian.mule_sq_1pct`   WHERE rec_seen = 1 
## clicked
and rec_clicked = 1
##
) t
JOIN p USING (variant_id)

  WHERE t.rec_price <= p.p99
GROUP BY 1;


 -- buckets

 -- Price bucket comparison with one row per bucket
-- Control = 'off', Treatment = 'mule_sq_100'
WITH base AS (
  SELECT
    variant_id,
    CAST(rec_price AS FLOAT64) AS rec_price,
    COALESCE(rec_seen, 0) AS rec_seen,
    COALESCE(rec_clicked, 0) AS rec_clicked,
    COALESCE(recs_engaged_click, 0) AS recs_engaged_click
  FROM `etsy-data-warehouse-dev.tsebastian.bound_reset_seller_add` 
  WHERE variant_id IN ('off','mule_sq_100')
),
buckets AS (
  SELECT
    variant_id,
    CASE
      WHEN rec_price IS NULL THEN 'Unknown'
      WHEN rec_price < 10 THEN '$0–9.99'
      WHEN rec_price < 25 THEN '$10–24.99'
      WHEN rec_price < 50 THEN '$25–49.99'
      WHEN rec_price < 100 THEN '$50–99.99'
      WHEN rec_price < 200 THEN '$100–199.99'
      WHEN rec_price < 500 THEN '$200–499.99'
      ELSE '$500+'
    END AS price_bucket,
    rec_seen, rec_clicked, recs_engaged_click
  FROM base
),
agg AS (
  SELECT
    price_bucket,
    variant_id,
    SUM(rec_seen) AS exposures,
    SUM(rec_clicked) AS clicks,
    SUM(recs_engaged_click) AS engaged_clicks
  FROM buckets
  GROUP BY 1,2
),
tot AS (
  SELECT
    variant_id,
    SUM(exposures) AS total_exposures
  FROM agg
  GROUP BY 1
),
metrics AS (
  SELECT
    a.price_bucket,
    a.variant_id,
    a.exposures,
    a.clicks,
    a.engaged_clicks,
    SAFE_DIVIDE(a.exposures, t.total_exposures) AS exposure_share,
    SAFE_DIVIDE(a.clicks, NULLIF(a.exposures, 0)) AS ctr,
    SAFE_DIVIDE(a.engaged_clicks, NULLIF(a.exposures, 0)) AS ecr
  FROM agg a
  JOIN tot t USING (variant_id)
)
SELECT
  price_bucket,

  -- Control metrics
  COALESCE(MAX(IF(variant_id = 'off', exposures, NULL)), 0) AS exposures_control,
  COALESCE(MAX(IF(variant_id = 'off', exposure_share, NULL)), 0) AS share_control,
  COALESCE(MAX(IF(variant_id = 'off', clicks, NULL)), 0) AS clicks_control,
  COALESCE(MAX(IF(variant_id = 'off', ctr, NULL)), 0) AS ctr_control,
  COALESCE(MAX(IF(variant_id = 'off', engaged_clicks, NULL)), 0) AS engaged_clicks_control,
  COALESCE(MAX(IF(variant_id = 'off', ecr, NULL)), 0) AS ecr_control,

  -- Treatment metrics
  COALESCE(MAX(IF(variant_id = 'mule_sq_100', exposures, NULL)), 0) AS exposures_treatment,
  COALESCE(MAX(IF(variant_id = 'mule_sq_100', exposure_share, NULL)), 0) AS share_treatment,
  COALESCE(MAX(IF(variant_id = 'mule_sq_100', clicks, NULL)), 0) AS clicks_treatment,
  COALESCE(MAX(IF(variant_id = 'mule_sq_100', ctr, NULL)), 0) AS ctr_treatment,
  COALESCE(MAX(IF(variant_id = 'mule_sq_100', engaged_clicks, NULL)), 0) AS engaged_clicks_treatment,
  COALESCE(MAX(IF(variant_id = 'mule_sq_100', ecr, NULL)), 0) AS ecr_treatment,

  -- Deltas (treatment - control)
  COALESCE(MAX(IF(variant_id = 'mule_sq_100', exposure_share, NULL)), 0)
    - COALESCE(MAX(IF(variant_id = 'off', exposure_share, NULL)), 0) AS delta_share,

  COALESCE(MAX(IF(variant_id = 'mule_sq_100', ctr, NULL)), 0)
    - COALESCE(MAX(IF(variant_id = 'off', ctr, NULL)), 0) AS delta_ctr,

  COALESCE(MAX(IF(variant_id = 'mule_sq_100', ecr, NULL)), 0)
    - COALESCE(MAX(IF(variant_id = 'off', ecr, NULL)), 0) AS delta_ecr,

  -- Percent lifts vs control (useful in Sheets)
  SAFE_DIVIDE(
    COALESCE(MAX(IF(variant_id = 'mule_sq_100', ctr, NULL)), 0)
      - COALESCE(MAX(IF(variant_id = 'off', ctr, NULL)), 0),
    NULLIF(COALESCE(MAX(IF(variant_id = 'off', ctr, NULL)), 0), 0)
  ) AS ctr_lift_pct,

  SAFE_DIVIDE(
    COALESCE(MAX(IF(variant_id = 'mule_sq_100', ecr, NULL)), 0)
      - COALESCE(MAX(IF(variant_id = 'off', ecr, NULL)), 0),
    NULLIF(COALESCE(MAX(IF(variant_id = 'off', ecr, NULL)), 0), 0)
  ) AS ecr_lift_pct

FROM metrics
GROUP BY price_bucket
ORDER BY price_bucket;

------------------------------------------------------------
-- Q4 Listing age buckets: one row per bucket with deltas
------------------------------------------------------------

WITH base AS (
  SELECT
    variant_id,
    CAST(listing_age AS INT64) AS listing_age,
    COALESCE(rec_seen, 0) AS rec_seen,
    COALESCE(rec_clicked, 0) AS rec_clicked,
    COALESCE(recs_engaged_click, 0) AS recs_engaged_click
  FROM `etsy-data-warehouse-dev.tsebastian.bound_reset_seller_add` 
  WHERE variant_id IN ('off','mule_sq_100')
),
buckets AS (
  SELECT
    variant_id,
    CASE
      WHEN listing_age IS NULL THEN 'f. Unknown'
      WHEN listing_age <= 30 THEN 'a. 0–30d'
      WHEN listing_age <= 90 THEN 'b. 31–90d'
      WHEN listing_age <= 180 THEN 'c. 91–180d'
      WHEN listing_age <= 365 THEN 'd. 181–365d'
      ELSE 'e. >365d'
    END AS listing_age_bucket,
    rec_seen, rec_clicked, recs_engaged_click
  FROM base
),
agg AS (
  SELECT
    listing_age_bucket,
    variant_id,
    SUM(rec_seen) AS exposures,
    SUM(rec_clicked) AS clicks,
    SUM(recs_engaged_click) AS engaged_clicks
  FROM buckets
  GROUP BY 1,2
),
tot AS (
  SELECT variant_id, SUM(exposures) AS total_exposures
  FROM agg
  GROUP BY 1
),
metrics AS (
  SELECT
    a.listing_age_bucket,
    a.variant_id,
    a.exposures,
    a.clicks,
    a.engaged_clicks,
    SAFE_DIVIDE(a.exposures, t.total_exposures) AS exposure_share,
    SAFE_DIVIDE(a.clicks, NULLIF(a.exposures, 0)) AS ctr,
    SAFE_DIVIDE(a.engaged_clicks, NULLIF(a.exposures, 0)) AS ecr
  FROM agg a
  JOIN tot t USING (variant_id)
)
SELECT
  listing_age_bucket,

  COALESCE(MAX(IF(variant_id='off', exposures, NULL)), 0) AS exposures_control,
  COALESCE(MAX(IF(variant_id='off', exposure_share, NULL)), 0) AS share_control,
  COALESCE(MAX(IF(variant_id='off', clicks, NULL)), 0) AS clicks_control,
  COALESCE(MAX(IF(variant_id='off', ctr, NULL)), 0) AS ctr_control,
  COALESCE(MAX(IF(variant_id='off', engaged_clicks, NULL)), 0) AS engaged_clicks_control,
  COALESCE(MAX(IF(variant_id='off', ecr, NULL)), 0) AS ecr_control,

  COALESCE(MAX(IF(variant_id='mule_sq_100', exposures, NULL)), 0) AS exposures_treatment,
  COALESCE(MAX(IF(variant_id='mule_sq_100', exposure_share, NULL)), 0) AS share_treatment,
  COALESCE(MAX(IF(variant_id='mule_sq_100', clicks, NULL)), 0) AS clicks_treatment,
  COALESCE(MAX(IF(variant_id='mule_sq_100', ctr, NULL)), 0) AS ctr_treatment,
  COALESCE(MAX(IF(variant_id='mule_sq_100', engaged_clicks, NULL)), 0) AS engaged_clicks_treatment,
  COALESCE(MAX(IF(variant_id='mule_sq_100', ecr, NULL)), 0) AS ecr_treatment,

  COALESCE(MAX(IF(variant_id='mule_sq_100', exposure_share, NULL)), 0)
    - COALESCE(MAX(IF(variant_id='off', exposure_share, NULL)), 0) AS delta_share,

  COALESCE(MAX(IF(variant_id='mule_sq_100', ctr, NULL)), 0)
    - COALESCE(MAX(IF(variant_id='off', ctr, NULL)), 0) AS delta_ctr,

  COALESCE(MAX(IF(variant_id='mule_sq_100', ecr, NULL)), 0)
    - COALESCE(MAX(IF(variant_id='off', ecr, NULL)), 0) AS delta_ecr,

  SAFE_DIVIDE(
    COALESCE(MAX(IF(variant_id='mule_sq_100', ctr, NULL)), 0)
      - COALESCE(MAX(IF(variant_id='off', ctr, NULL)), 0),
    NULLIF(COALESCE(MAX(IF(variant_id='off', ctr, NULL)), 0), 0)
  ) AS ctr_lift_pct,

  SAFE_DIVIDE(
    COALESCE(MAX(IF(variant_id='mule_sq_100', ecr, NULL)), 0)
      - COALESCE(MAX(IF(variant_id='off', ecr, NULL)), 0),
    NULLIF(COALESCE(MAX(IF(variant_id='off', ecr, NULL)), 0), 0)
  ) AS ecr_lift_pct

FROM metrics
GROUP BY listing_age_bucket
ORDER BY listing_age_bucket;


------------------------------------------------------------
-- Q5  Seller tenure buckets:
------------------------------------------------------------


WITH base AS (
  SELECT
    variant_id,
    CAST(seller_shop_open_tenure AS INT64) AS tenure_days,
    COALESCE(rec_seen, 0) AS rec_seen,
    COALESCE(rec_clicked, 0) AS rec_clicked,
    COALESCE(recs_engaged_click, 0) AS recs_engaged_click
  FROM `etsy-data-warehouse-dev.tsebastian.bound_reset_seller_add` 
  WHERE variant_id IN ('off','mule_sq_100')
),
buckets AS (
  SELECT
    variant_id,
    CASE
      WHEN tenure_days IS NULL THEN 'f. Unknown'
      WHEN tenure_days < 180 THEN 'a. <6m'
      WHEN tenure_days < 365 THEN 'b. 6–12m'
      WHEN tenure_days < 1095 THEN 'c. 1–3y'
      WHEN tenure_days < 1825 THEN 'd. 3–5y'
      ELSE 'e. 5y+'
    END AS seller_tenure_bucket,
    rec_seen, rec_clicked, recs_engaged_click
  FROM base
),
agg AS (
  SELECT
    seller_tenure_bucket,
    variant_id,
    SUM(rec_seen) AS exposures,
    SUM(rec_clicked) AS clicks,
    SUM(recs_engaged_click) AS engaged_clicks
  FROM buckets
  GROUP BY 1,2
),
tot AS (
  SELECT variant_id, SUM(exposures) AS total_exposures
  FROM agg
  GROUP BY 1
),
metrics AS (
  SELECT
    a.seller_tenure_bucket,
    a.variant_id,
    a.exposures,
    a.clicks,
    a.engaged_clicks,
    SAFE_DIVIDE(a.exposures, t.total_exposures) AS exposure_share,
    SAFE_DIVIDE(a.clicks, NULLIF(a.exposures, 0)) AS ctr,
    SAFE_DIVIDE(a.engaged_clicks, NULLIF(a.exposures, 0)) AS ecr
  FROM agg a
  JOIN tot t USING (variant_id)
)
SELECT
  seller_tenure_bucket,

  COALESCE(MAX(IF(variant_id='off', exposures, NULL)), 0) AS exposures_control,
  COALESCE(MAX(IF(variant_id='off', exposure_share, NULL)), 0) AS share_control,
  COALESCE(MAX(IF(variant_id='off', clicks, NULL)), 0) AS clicks_control,
  COALESCE(MAX(IF(variant_id='off', ctr, NULL)), 0) AS ctr_control,
  COALESCE(MAX(IF(variant_id='off', engaged_clicks, NULL)), 0) AS engaged_clicks_control,
  COALESCE(MAX(IF(variant_id='off', ecr, NULL)), 0) AS ecr_control,

  COALESCE(MAX(IF(variant_id='mule_sq_100', exposures, NULL)), 0) AS exposures_treatment,
  COALESCE(MAX(IF(variant_id='mule_sq_100', exposure_share, NULL)), 0) AS share_treatment,
  COALESCE(MAX(IF(variant_id='mule_sq_100', clicks, NULL)), 0) AS clicks_treatment,
  COALESCE(MAX(IF(variant_id='mule_sq_100', ctr, NULL)), 0) AS ctr_treatment,
  COALESCE(MAX(IF(variant_id='mule_sq_100', engaged_clicks, NULL)), 0) AS engaged_clicks_treatment,
  COALESCE(MAX(IF(variant_id='mule_sq_100', ecr, NULL)), 0) AS ecr_treatment,

  COALESCE(MAX(IF(variant_id='mule_sq_100', exposure_share, NULL)), 0)
    - COALESCE(MAX(IF(variant_id='off', exposure_share, NULL)), 0) AS delta_share,

  COALESCE(MAX(IF(variant_id='mule_sq_100', ctr, NULL)), 0)
    - COALESCE(MAX(IF(variant_id='off', ctr, NULL)), 0) AS delta_ctr,

  COALESCE(MAX(IF(variant_id='mule_sq_100', ecr, NULL)), 0)
    - COALESCE(MAX(IF(variant_id='off', ecr, NULL)), 0) AS delta_ecr,

  SAFE_DIVIDE(
    COALESCE(MAX(IF(variant_id='mule_sq_100', ctr, NULL)), 0)
      - COALESCE(MAX(IF(variant_id='off', ctr, NULL)), 0),
    NULLIF(COALESCE(MAX(IF(variant_id='off', ctr, NULL)), 0), 0)
  ) AS ctr_lift_pct,

  SAFE_DIVIDE(
    COALESCE(MAX(IF(variant_id='mule_sq_100', ecr, NULL)), 0)
      - COALESCE(MAX(IF(variant_id='off', ecr, NULL)), 0),
    NULLIF(COALESCE(MAX(IF(variant_id='off', ecr, NULL)), 0), 0)
  ) AS ecr_lift_pct

FROM metrics
GROUP BY seller_tenure_bucket
ORDER BY seller_tenure_bucket;


------------------------------------------------------------
-- Q6 Listing quality score deciles:
------------------------------------------------------------

-- Control = 'off', Treatment = 'mule_sq_100'
WITH base AS (
  SELECT
    variant_id,
    CAST(listing_quality_score AS FLOAT64) AS quality_score,
    COALESCE(rec_seen, 0) AS rec_seen,
    COALESCE(rec_clicked, 0) AS rec_clicked,
    COALESCE(recs_engaged_click, 0) AS recs_engaged_click
  FROM `etsy-data-warehouse-dev.tsebastian.bound_reset_seller_add` 
  WHERE variant_id IN ('off','mule_sq_100')
    AND listing_quality_score IS NOT NULL
),
-- 11 cut points for 10 deciles (min through max)
edges AS (
  SELECT APPROX_QUANTILES(quality_score, 10) AS qs
  FROM base
),
decile_bins AS (
  SELECT
    OFFSET + 1 AS decile,
    qs[OFFSET] AS lower_bound,
    qs[OFFSET + 1] AS upper_bound
  FROM edges, UNNEST(qs) AS val WITH OFFSET
  WHERE OFFSET < 10
),
-- Assign each row to exactly one decile via range join
assigned AS (
  SELECT
    b.variant_id,
    db.decile AS quality_decile,
    b.rec_seen,
    b.rec_clicked,
    b.recs_engaged_click
  FROM base b
  JOIN decile_bins db
    ON b.quality_score >= db.lower_bound
   AND (b.quality_score < db.upper_bound OR (db.decile = 10 AND b.quality_score <= db.upper_bound))
),
agg AS (
  SELECT
    quality_decile,
    variant_id,
    SUM(rec_seen) AS exposures,
    SUM(rec_clicked) AS clicks,
    SUM(recs_engaged_click) AS engaged_clicks
  FROM assigned
  GROUP BY 1,2
),
tot AS (
  SELECT variant_id, SUM(exposures) AS total_exposures
  FROM agg
  GROUP BY 1
),
metrics AS (
  SELECT
    a.quality_decile,
    a.variant_id,
    a.exposures,
    a.clicks,
    a.engaged_clicks,
    SAFE_DIVIDE(a.exposures, t.total_exposures) AS exposure_share,
    SAFE_DIVIDE(a.clicks, NULLIF(a.exposures, 0)) AS ctr,
    SAFE_DIVIDE(a.engaged_clicks, NULLIF(a.exposures, 0)) AS ecr
  FROM agg a
  JOIN tot t USING (variant_id)
)
SELECT
  m.quality_decile,

  -- Decile bounds
  MAX(db.lower_bound) AS quality_decile_lower,
  MAX(db.upper_bound) AS quality_decile_upper,

  -- Control
  COALESCE(MAX(IF(m.variant_id='off', m.exposures, NULL)), 0) AS exposures_control,
  COALESCE(MAX(IF(m.variant_id='off', m.exposure_share, NULL)), 0) AS share_control,
  COALESCE(MAX(IF(m.variant_id='off', m.clicks, NULL)), 0) AS clicks_control,
  COALESCE(MAX(IF(m.variant_id='off', m.ctr, NULL)), 0) AS ctr_control,
  COALESCE(MAX(IF(m.variant_id='off', m.engaged_clicks, NULL)), 0) AS engaged_clicks_control,
  COALESCE(MAX(IF(m.variant_id='off', m.ecr, NULL)), 0) AS ecr_control,

  -- Treatment
  COALESCE(MAX(IF(m.variant_id='mule_sq_100', m.exposures, NULL)), 0) AS exposures_treatment,
  COALESCE(MAX(IF(m.variant_id='mule_sq_100', m.exposure_share, NULL)), 0) AS share_treatment,
  COALESCE(MAX(IF(m.variant_id='mule_sq_100', m.clicks, NULL)), 0) AS clicks_treatment,
  COALESCE(MAX(IF(m.variant_id='mule_sq_100', m.ctr, NULL)), 0) AS ctr_treatment,
  COALESCE(MAX(IF(m.variant_id='mule_sq_100', m.engaged_clicks, NULL)), 0) AS engaged_clicks_treatment,
  COALESCE(MAX(IF(m.variant_id='mule_sq_100', m.ecr, NULL)), 0) AS ecr_treatment,

  -- Deltas (treatment - control)
  COALESCE(MAX(IF(m.variant_id='mule_sq_100', m.exposure_share, NULL)), 0)
    - COALESCE(MAX(IF(m.variant_id='off', m.exposure_share, NULL)), 0) AS delta_share,

  COALESCE(MAX(IF(m.variant_id='mule_sq_100', m.ctr, NULL)), 0)
    - COALESCE(MAX(IF(m.variant_id='off', m.ctr, NULL)), 0) AS delta_ctr,

  COALESCE(MAX(IF(m.variant_id='mule_sq_100', m.ecr, NULL)), 0)
    - COALESCE(MAX(IF(m.variant_id='off', m.ecr, NULL)), 0) AS delta_ecr,

  -- Percent lifts
  SAFE_DIVIDE(
    COALESCE(MAX(IF(m.variant_id='mule_sq_100', m.ctr, NULL)), 0)
      - COALESCE(MAX(IF(m.variant_id='off', m.ctr, NULL)), 0),
    NULLIF(COALESCE(MAX(IF(m.variant_id='off', m.ctr, NULL)), 0), 0)
  ) AS ctr_lift_pct,

  SAFE_DIVIDE(
    COALESCE(MAX(IF(m.variant_id='mule_sq_100', m.ecr, NULL)), 0)
      - COALESCE(MAX(IF(m.variant_id='off', m.ecr, NULL)), 0),
    NULLIF(COALESCE(MAX(IF(m.variant_id='off', m.ecr, NULL)), 0), 0)
  ) AS ecr_lift_pct

FROM metrics m
JOIN decile_bins db
  ON db.decile = m.quality_decile
GROUP BY m.quality_decile
ORDER BY m.quality_decile;



------------------------------------------------------------
-- Q7 Diversity metrics: single row with control vs. treatment and deltas
------------------------------------------------------------

-- Control = 'off', Treatment = 'mule_sq_100'
WITH base AS (
  SELECT
    variant_id,
    COALESCE(listing_taxonomy_full_path, 'Unknown') AS taxonomy,
    COALESCE(rec_seen, 0) AS rec_seen
  FROM `etsy-data-warehouse-dev.tsebastian.bound_reset_seller_add` 
  WHERE variant_id IN ('off','mule_sq_100')
),
by_tax AS (
  SELECT variant_id, taxonomy, SUM(rec_seen) AS exposures
  FROM base
  GROUP BY 1,2
),
tot AS (
  SELECT variant_id, SUM(exposures) AS total_exposures
  FROM by_tax
  GROUP BY 1
),
shares AS (
  SELECT
    b.variant_id,
    b.taxonomy,
    b.exposures,
    SAFE_DIVIDE(b.exposures, t.total_exposures) AS p
  FROM by_tax b
  JOIN tot t USING (variant_id)
),
per_variant AS (
  SELECT
    variant_id,
    COUNT(DISTINCT taxonomy) AS distinct_taxonomies,
    SUM(p * p) AS hhi,
    (1 - SUM(p * p)) AS simpson_diversity
  FROM shares
  GROUP BY variant_id
)
SELECT
  COALESCE(MAX(IF(variant_id='off', distinct_taxonomies, NULL)), 0)  AS distinct_taxonomies_control,
  COALESCE(MAX(IF(variant_id='mule_sq_100', distinct_taxonomies, NULL)), 0) AS distinct_taxonomies_treatment,
  COALESCE(MAX(IF(variant_id='mule_sq_100', distinct_taxonomies, NULL)), 0)
    - COALESCE(MAX(IF(variant_id='off', distinct_taxonomies, NULL)), 0) AS delta_distinct_taxonomies,

  COALESCE(MAX(IF(variant_id='off', hhi, NULL)), 0)  AS hhi_control,
  COALESCE(MAX(IF(variant_id='mule_sq_100', hhi, NULL)), 0) AS hhi_treatment,
  COALESCE(MAX(IF(variant_id='mule_sq_100', hhi, NULL)), 0)
    - COALESCE(MAX(IF(variant_id='off', hhi, NULL)), 0) AS delta_hhi,

  COALESCE(MAX(IF(variant_id='off', simpson_diversity, NULL)), 0)  AS simpson_control,
  COALESCE(MAX(IF(variant_id='mule_sq_100', simpson_diversity, NULL)), 0) AS simpson_treatment,
  COALESCE(MAX(IF(variant_id='mule_sq_100', simpson_diversity, NULL)), 0)
    - COALESCE(MAX(IF(variant_id='off', simpson_diversity, NULL)), 0) AS delta_simpson

FROM per_variant;




------------------------------------------------------------
-- Q8) Seller rating buckets (min 10 ratings)
------------------------------------------------------------

-- Control = 'off', Treatment = 'mule_sq_100'
-- Control = 'off', Treatment = 'mule_sq_100'
WITH base AS (
  SELECT
    variant_id,
    CAST(seller_avg_12m_rating AS FLOAT64) AS seller_rating,
    CAST(seller_nbr_12m_rating AS INT64) AS seller_rating_count,
    COALESCE(rec_seen, 0) AS rec_seen,
    COALESCE(rec_clicked, 0) AS rec_clicked,
    COALESCE(recs_engaged_click, 0) AS recs_engaged_click
  FROM `etsy-data-warehouse-dev.tsebastian.bound_reset_seller_add` 
  WHERE variant_id IN ('off','mule_sq_100')
    AND seller_nbr_12m_rating IS NOT NULL
    AND seller_nbr_12m_rating >= 10
),
buckets AS (
  SELECT
    variant_id,
    CASE
      WHEN seller_rating IS NULL THEN 'Unknown'
      WHEN seller_rating < 4.6 THEN '<4.6'
      WHEN seller_rating < 4.8 THEN '4.6–4.79'
      WHEN seller_rating < 4.9 THEN '4.8–4.89'
      WHEN seller_rating <= 5.0 THEN '4.9–5.0'
      ELSE 'Other'
    END AS seller_rating_bucket,
    rec_seen, rec_clicked, recs_engaged_click
  FROM base
),
agg AS (
  SELECT
    seller_rating_bucket,
    variant_id,
    SUM(rec_seen) AS exposures,
    SUM(rec_clicked) AS clicks,
    SUM(recs_engaged_click) AS engaged_clicks
  FROM buckets
  GROUP BY 1,2
),
tot AS (
  SELECT variant_id, SUM(exposures) AS total_exposures
  FROM agg
  GROUP BY 1
),
metrics AS (
  SELECT
    a.seller_rating_bucket,
    a.variant_id,
    a.exposures,
    a.clicks,
    a.engaged_clicks,
    SAFE_DIVIDE(a.exposures, t.total_exposures) AS exposure_share,
    SAFE_DIVIDE(a.clicks, NULLIF(a.exposures, 0)) AS ctr,
    SAFE_DIVIDE(a.engaged_clicks, NULLIF(a.exposures, 0)) AS ecr
  FROM agg a
  JOIN tot t USING (variant_id)
)
SELECT
  seller_rating_bucket,

  COALESCE(MAX(IF(variant_id='off', exposures, NULL)), 0) AS exposures_control,
  COALESCE(MAX(IF(variant_id='off', exposure_share, NULL)), 0) AS share_control,
  COALESCE(MAX(IF(variant_id='off', clicks, NULL)), 0) AS clicks_control,
  COALESCE(MAX(IF(variant_id='off', ctr, NULL)), 0) AS ctr_control,
  COALESCE(MAX(IF(variant_id='off', engaged_clicks, NULL)), 0) AS engaged_clicks_control,
  COALESCE(MAX(IF(variant_id='off', ecr, NULL)), 0) AS ecr_control,

  COALESCE(MAX(IF(variant_id='mule_sq_100', exposures, NULL)), 0) AS exposures_treatment,
  COALESCE(MAX(IF(variant_id='mule_sq_100', exposure_share, NULL)), 0) AS share_treatment,
  COALESCE(MAX(IF(variant_id='mule_sq_100', clicks, NULL)), 0) AS clicks_treatment,
  COALESCE(MAX(IF(variant_id='mule_sq_100', ctr, NULL)), 0) AS ctr_treatment,
  COALESCE(MAX(IF(variant_id='mule_sq_100', engaged_clicks, NULL)), 0) AS engaged_clicks_treatment,
  COALESCE(MAX(IF(variant_id='mule_sq_100', ecr, NULL)), 0) AS ecr_treatment,

  COALESCE(MAX(IF(variant_id='mule_sq_100', exposure_share, NULL)), 0)
    - COALESCE(MAX(IF(variant_id='off', exposure_share, NULL)), 0) AS delta_share,

  COALESCE(MAX(IF(variant_id='mule_sq_100', ctr, NULL)), 0)
    - COALESCE(MAX(IF(variant_id='off', ctr, NULL)), 0) AS delta_ctr,

  COALESCE(MAX(IF(variant_id='mule_sq_100', ecr, NULL)), 0)
    - COALESCE(MAX(IF(variant_id='off', ecr, NULL)), 0) AS delta_ecr,

  SAFE_DIVIDE(
    COALESCE(MAX(IF(variant_id='mule_sq_100', ctr, NULL)), 0)
      - COALESCE(MAX(IF(variant_id='off', ctr, NULL)), 0),
    NULLIF(COALESCE(MAX(IF(variant_id='off', ctr, NULL)), 0), 0)
  ) AS ctr_lift_pct,

  SAFE_DIVIDE(
    COALESCE(MAX(IF(variant_id='mule_sq_100', ecr, NULL)), 0)
      - COALESCE(MAX(IF(variant_id='off', ecr, NULL)), 0),
    NULLIF(COALESCE(MAX(IF(variant_id='off', ecr, NULL)), 0), 0)
  ) AS ecr_lift_pct

FROM metrics
GROUP BY seller_rating_bucket
ORDER BY seller_rating_bucket;


------------------------------------------------------------
-- Q9) Buyer search intensity quantiles:
------------------------------------------------------------

-- Control = 'off', Treatment = 'mule_sq_100'
WITH base AS (
  SELECT
    variant_id,
    CAST(buyer_search_intensity_60d AS FLOAT64) AS search_intensity,
    COALESCE(rec_seen, 0) AS rec_seen,
    COALESCE(rec_clicked, 0) AS rec_clicked,
    COALESCE(recs_engaged_click, 0) AS recs_engaged_click
  FROM `etsy-data-warehouse-dev.tsebastian.bound_reset_seller_add` 
  WHERE variant_id IN ('off','mule_sq_100')
    AND buyer_search_intensity_60d IS NOT NULL
),
-- 6 cut points for 5 quintiles (min through max)
edges AS (
  SELECT APPROX_QUANTILES(search_intensity, 5) AS qs
  FROM base
),
q_bins AS (
  SELECT
    OFFSET + 1 AS quintile,
    qs[OFFSET] AS lower_bound,
    qs[OFFSET + 1] AS upper_bound
  FROM edges, UNNEST(qs) AS val WITH OFFSET
  WHERE OFFSET < 5
),
-- Assign rows to quintiles via non-correlated range join
assigned AS (
  SELECT
    b.variant_id,
    qb.quintile AS search_intensity_quintile,
    qb.lower_bound AS quintile_lower_bound,
    qb.upper_bound AS quintile_upper_bound,
    b.rec_seen,
    b.rec_clicked,
    b.recs_engaged_click
  FROM base b
  JOIN q_bins qb
    ON b.search_intensity >= qb.lower_bound
   AND (b.search_intensity < qb.upper_bound OR (qb.quintile = 5 AND b.search_intensity <= qb.upper_bound))
),
agg AS (
  SELECT
    search_intensity_quintile,
    MIN(quintile_lower_bound) AS quintile_lower_bound,
    MAX(quintile_upper_bound) AS quintile_upper_bound,
    variant_id,
    SUM(rec_seen) AS exposures,
    SUM(rec_clicked) AS clicks,
    SUM(recs_engaged_click) AS engaged_clicks
  FROM assigned
  GROUP BY 1, 4
),
tot AS (
  SELECT variant_id, SUM(exposures) AS total_exposures
  FROM agg
  GROUP BY 1
),
metrics AS (
  SELECT
    a.search_intensity_quintile,
    a.quintile_lower_bound,
    a.quintile_upper_bound,
    a.variant_id,
    a.exposures,
    a.clicks,
    a.engaged_clicks,
    SAFE_DIVIDE(a.exposures, t.total_exposures) AS exposure_share,
    SAFE_DIVIDE(a.clicks, NULLIF(a.exposures, 0)) AS ctr,
    SAFE_DIVIDE(a.engaged_clicks, NULLIF(a.exposures, 0)) AS ecr
  FROM agg a
  JOIN tot t USING (variant_id)
)
SELECT
  m.search_intensity_quintile,
  MAX(m.quintile_lower_bound) AS search_intensity_lower,
  MAX(m.quintile_upper_bound) AS search_intensity_upper,

  -- Control
  COALESCE(MAX(IF(m.variant_id='off', m.exposures, NULL)), 0) AS exposures_control,
  COALESCE(MAX(IF(m.variant_id='off', m.exposure_share, NULL)), 0) AS share_control,
  COALESCE(MAX(IF(m.variant_id='off', m.clicks, NULL)), 0) AS clicks_control,
  COALESCE(MAX(IF(m.variant_id='off', m.ctr, NULL)), 0) AS ctr_control,
  COALESCE(MAX(IF(m.variant_id='off', m.engaged_clicks, NULL)), 0) AS engaged_clicks_control,
  COALESCE(MAX(IF(m.variant_id='off', m.ecr, NULL)), 0) AS ecr_control,

  -- Treatment
  COALESCE(MAX(IF(m.variant_id='mule_sq_100', m.exposures, NULL)), 0) AS exposures_treatment,
  COALESCE(MAX(IF(m.variant_id='mule_sq_100', m.exposure_share, NULL)), 0) AS share_treatment,
  COALESCE(MAX(IF(m.variant_id='mule_sq_100', m.clicks, NULL)), 0) AS clicks_treatment,
  COALESCE(MAX(IF(m.variant_id='mule_sq_100', m.ctr, NULL)), 0) AS ctr_treatment,
  COALESCE(MAX(IF(m.variant_id='mule_sq_100', m.engaged_clicks, NULL)), 0) AS engaged_clicks_treatment,
  COALESCE(MAX(IF(m.variant_id='mule_sq_100', m.ecr, NULL)), 0) AS ecr_treatment,

  -- Deltas (treatment - control)
  COALESCE(MAX(IF(m.variant_id='mule_sq_100', m.exposure_share, NULL)), 0)
    - COALESCE(MAX(IF(m.variant_id='off', m.exposure_share, NULL)), 0) AS delta_share,

  COALESCE(MAX(IF(m.variant_id='mule_sq_100', m.ctr, NULL)), 0)
    - COALESCE(MAX(IF(m.variant_id='off', m.ctr, NULL)), 0) AS delta_ctr,

  COALESCE(MAX(IF(m.variant_id='mule_sq_100', m.ecr, NULL)), 0)
    - COALESCE(MAX(IF(m.variant_id='off', m.ecr, NULL)), 0) AS delta_ecr,

  -- Percent lifts 
  SAFE_DIVIDE(
    COALESCE(MAX(IF(m.variant_id='mule_sq_100', m.ctr, NULL)), 0)
      - COALESCE(MAX(IF(m.variant_id='off', m.ctr, NULL)), 0),
    NULLIF(COALESCE(MAX(IF(m.variant_id='off', m.ctr, NULL)), 0), 0)
  ) AS ctr_lift_pct,

  SAFE_DIVIDE(
    COALESCE(MAX(IF(m.variant_id='mule_sq_100', m.ecr, NULL)), 0)
      - COALESCE(MAX(IF(m.variant_id='off', m.ecr, NULL)), 0),
    NULLIF(COALESCE(MAX(IF(m.variant_id='off', m.ecr, NULL)), 0), 0)
  ) AS ecr_lift_pct

FROM metrics m
GROUP BY m.search_intensity_quintile
ORDER BY m.search_intensity_quintile;



------------------------------------------------------------
-- Q10) Buyer searches buckets
------------------------------------------------------------

-- Control = 'off', Treatment = 'mule_sq_100'
WITH base AS (
  SELECT
    variant_id,
    CAST(buyer_searches_60d AS INT64) AS buyer_searches_60d,
    COALESCE(rec_seen, 0) AS rec_seen,
    COALESCE(rec_clicked, 0) AS rec_clicked,
    COALESCE(recs_engaged_click, 0) AS recs_engaged_click
  FROM `etsy-data-warehouse-dev.tsebastian.bound_reset_seller_add` 
  WHERE variant_id IN ('off','mule_sq_100')
),
buckets AS (
  SELECT
    variant_id,
    CASE
      WHEN buyer_searches_60d IS NULL THEN 'f. Unknown'
      WHEN buyer_searches_60d = 0 THEN 'a. 0'
      WHEN buyer_searches_60d <= 5 THEN 'b. 1–5'
      WHEN buyer_searches_60d <= 20 THEN 'c. 6–20'
      WHEN buyer_searches_60d <= 50 THEN 'd. 21–50'
      ELSE 'e. 51+'
    END AS searches_60d_bucket,
    rec_seen, rec_clicked, recs_engaged_click
  FROM base
),
agg AS (
  SELECT
    searches_60d_bucket,
    variant_id,
    SUM(rec_seen) AS exposures,
    SUM(rec_clicked) AS clicks,
    SUM(recs_engaged_click) AS engaged_clicks
  FROM buckets
  GROUP BY 1,2
),
tot AS (
  SELECT variant_id, SUM(exposures) AS total_exposures
  FROM agg
  GROUP BY 1
),
metrics AS (
  SELECT
    a.searches_60d_bucket,
    a.variant_id,
    a.exposures,
    a.clicks,
    a.engaged_clicks,
    SAFE_DIVIDE(a.exposures, t.total_exposures) AS exposure_share,
    SAFE_DIVIDE(a.clicks, NULLIF(a.exposures, 0)) AS ctr,
    SAFE_DIVIDE(a.engaged_clicks, NULLIF(a.exposures, 0)) AS ecr
  FROM agg a
  JOIN tot t USING (variant_id)
)
SELECT
  searches_60d_bucket,

  COALESCE(MAX(IF(variant_id='off', exposures, NULL)), 0) AS exposures_control,
  COALESCE(MAX(IF(variant_id='off', exposure_share, NULL)), 0) AS share_control,
  COALESCE(MAX(IF(variant_id='off', clicks, NULL)), 0) AS clicks_control,
  COALESCE(MAX(IF(variant_id='off', ctr, NULL)), 0) AS ctr_control,
  COALESCE(MAX(IF(variant_id='off', engaged_clicks, NULL)), 0) AS engaged_clicks_control,
  COALESCE(MAX(IF(variant_id='off', ecr, NULL)), 0) AS ecr_control,

  COALESCE(MAX(IF(variant_id='mule_sq_100', exposures, NULL)), 0) AS exposures_treatment,
  COALESCE(MAX(IF(variant_id='mule_sq_100', exposure_share, NULL)), 0) AS share_treatment,
  COALESCE(MAX(IF(variant_id='mule_sq_100', clicks, NULL)), 0) AS clicks_treatment,
  COALESCE(MAX(IF(variant_id='mule_sq_100', ctr, NULL)), 0) AS ctr_treatment,
  COALESCE(MAX(IF(variant_id='mule_sq_100', engaged_clicks, NULL)), 0) AS engaged_clicks_treatment,
  COALESCE(MAX(IF(variant_id='mule_sq_100', ecr, NULL)), 0) AS ecr_treatment,

  COALESCE(MAX(IF(variant_id='mule_sq_100', exposure_share, NULL)), 0)
    - COALESCE(MAX(IF(variant_id='off', exposure_share, NULL)), 0) AS delta_share,

  COALESCE(MAX(IF(variant_id='mule_sq_100', ctr, NULL)), 0)
    - COALESCE(MAX(IF(variant_id='off', ctr, NULL)), 0) AS delta_ctr,

  COALESCE(MAX(IF(variant_id='mule_sq_100', ecr, NULL)), 0)
    - COALESCE(MAX(IF(variant_id='off', ecr, NULL)), 0) AS delta_ecr,

  SAFE_DIVIDE(
    COALESCE(MAX(IF(variant_id='mule_sq_100', ctr, NULL)), 0)
      - COALESCE(MAX(IF(variant_id='off', ctr, NULL)), 0),
    NULLIF(COALESCE(MAX(IF(variant_id='off', ctr, NULL)), 0), 0)
  ) AS ctr_lift_pct,

  SAFE_DIVIDE(
    COALESCE(MAX(IF(variant_id='mule_sq_100', ecr, NULL)), 0)
      - COALESCE(MAX(IF(variant_id='off', ecr, NULL)), 0),
    NULLIF(COALESCE(MAX(IF(variant_id='off', ecr, NULL)), 0), 0)
  ) AS ecr_lift_pct

FROM metrics
GROUP BY searches_60d_bucket
ORDER BY searches_60d_bucket;

------------------------------------------------------------
-- Q11) Lift by candidate set
------------------------------------------------------------



 -- Control = 'off', Treatment = 'mule_sq_100'
-- One row per candidate_set with exposure_lift_pct, ctr_lift_pct, ecr_lift_pct
WITH base AS (
  SELECT
    candidate_set,
    variant_id,
    COALESCE(rec_seen, 0) AS rec_seen,
    COALESCE(rec_clicked, 0) AS rec_clicked,
    COALESCE(recs_engaged_click, 0) AS recs_engaged_click
  FROM `etsy-data-warehouse-dev.tsebastian.bound_reset_seller_add` 
  WHERE variant_id IN ('off','mule_sq_100')
 
),
agg AS (
  SELECT
    candidate_set,
    variant_id,
    SUM(rec_seen) AS exposures,
    SUM(rec_clicked) AS clicks,
    SUM(recs_engaged_click) AS engaged_clicks
  FROM base
  GROUP BY 1,2
),
tot AS (
  SELECT
    variant_id,
    SUM(exposures) AS total_exposures
  FROM agg
  GROUP BY 1
),
metrics AS (
  SELECT
    a.candidate_set,
    a.variant_id,
    a.exposures,
    a.clicks,
    a.engaged_clicks,
    SAFE_DIVIDE(a.clicks, NULLIF(a.exposures, 0)) AS ctr,
    SAFE_DIVIDE(a.engaged_clicks, NULLIF(a.exposures, 0)) AS ecr,
    SAFE_DIVIDE(a.exposures, t.total_exposures) AS exposure_share
  FROM agg a
  JOIN tot t USING (variant_id)
)
SELECT
  candidate_set,

  -- Raw metrics (optional but useful in Sheets)
  COALESCE(MAX(IF(variant_id='off', exposures, NULL)), 0) AS exposures_control,
  COALESCE(MAX(IF(variant_id='mule_sq_100', exposures, NULL)), 0) AS exposures_treatment,

  COALESCE(MAX(IF(variant_id='off', ctr, NULL)), 0) AS ctr_control,
  COALESCE(MAX(IF(variant_id='mule_sq_100', ctr, NULL)), 0) AS ctr_treatment,

  COALESCE(MAX(IF(variant_id='off', ecr, NULL)), 0) AS ecr_control,
  COALESCE(MAX(IF(variant_id='mule_sq_100', ecr, NULL)), 0) AS ecr_treatment,

  -- Optional: exposure share to see mix shift across candidate sets
  COALESCE(MAX(IF(variant_id='off', exposure_share, NULL)), 0) AS share_control,
  COALESCE(MAX(IF(variant_id='mule_sq_100', exposure_share, NULL)), 0) AS share_treatment,

  -- Lifts (treatment vs control)
  SAFE_DIVIDE(
    COALESCE(MAX(IF(variant_id='mule_sq_100', exposures, NULL)), 0)
      - COALESCE(MAX(IF(variant_id='off', exposures, NULL)), 0),
    NULLIF(COALESCE(MAX(IF(variant_id='off', exposures, NULL)), 0), 0)
  ) AS exposure_lift_pct,

  SAFE_DIVIDE(
    COALESCE(MAX(IF(variant_id='mule_sq_100', ctr, NULL)), 0)
      - COALESCE(MAX(IF(variant_id='off', ctr, NULL)), 0),
    NULLIF(COALESCE(MAX(IF(variant_id='off', ctr, NULL)), 0), 0)
  ) AS ctr_lift_pct,

  SAFE_DIVIDE(
    COALESCE(MAX(IF(variant_id='mule_sq_100', ecr, NULL)), 0)
      - COALESCE(MAX(IF(variant_id='off', ecr, NULL)), 0),
    NULLIF(COALESCE(MAX(IF(variant_id='off', ecr, NULL)), 0), 0)
  ) AS ecr_lift_pct

FROM metrics
GROUP BY candidate_set
ORDER BY ctr_lift_pct DESC;


------------------------------------------------------------
-- Q12) Lift by seller tier
------------------------------------------------------------

-- Control = 'off', Treatment = 'mule_sq_100'
WITH base AS (
  SELECT
    COALESCE(seller_tier_new, 'Unknown') AS seller_tier_new,
    variant_id,
    COALESCE(rec_seen, 0) AS rec_seen,
    COALESCE(rec_clicked, 0) AS rec_clicked
  FROM `etsy-data-warehouse-dev.tsebastian.bound_reset_sq_add` 
  WHERE variant_id IN ('off','mule_sq_100')
),
agg AS (
  SELECT
    seller_tier_new,
    variant_id,
    SUM(rec_seen) AS exposures,
    SUM(rec_clicked) AS clicks
  FROM base
  GROUP BY 1,2
),
tot AS (
  SELECT variant_id, SUM(exposures) AS total_exposures
  FROM agg
  GROUP BY 1
),
metrics AS (
  SELECT
    a.seller_tier_new,
    a.variant_id,
    a.exposures,
    a.clicks,
    SAFE_DIVIDE(a.clicks, NULLIF(a.exposures, 0)) AS ctr,
    SAFE_DIVIDE(a.exposures, t.total_exposures) AS exposure_share
  FROM agg a
  JOIN tot t USING (variant_id)
)
SELECT
  seller_tier_new,

  -- Raw metrics by variant (optional to chart)
  COALESCE(MAX(IF(variant_id='off', exposures, NULL)), 0) AS exposures_control,
  COALESCE(MAX(IF(variant_id='mule_sq_100', exposures, NULL)), 0) AS exposures_treatment,
  COALESCE(MAX(IF(variant_id='off', ctr, NULL)), 0) AS ctr_control,
  COALESCE(MAX(IF(variant_id='mule_sq_100', ctr, NULL)), 0) AS ctr_treatment,

  -- Exposure share (mix) by variant (optional)
  COALESCE(MAX(IF(variant_id='off', exposure_share, NULL)), 0) AS share_control,
  COALESCE(MAX(IF(variant_id='mule_sq_100', exposure_share, NULL)), 0) AS share_treatment,

  -- Lifts (treatment vs control)
  SAFE_DIVIDE(
    COALESCE(MAX(IF(variant_id='mule_sq_100', exposures, NULL)), 0)
      - COALESCE(MAX(IF(variant_id='off', exposures, NULL)), 0),
    NULLIF(COALESCE(MAX(IF(variant_id='off', exposures, NULL)), 0), 0)
  ) AS exposure_lift_pct,

  SAFE_DIVIDE(
    COALESCE(MAX(IF(variant_id='mule_sq_100', ctr, NULL)), 0)
      - COALESCE(MAX(IF(variant_id='off', ctr, NULL)), 0),
    NULLIF(COALESCE(MAX(IF(variant_id='off', ctr, NULL)), 0), 0)
  ) AS ctr_lift_pct

FROM metrics
GROUP BY seller_tier_new
ORDER BY ctr_lift_pct DESC;

------------------------------------------------------------
-- Q13) Lift by buyer segment
------------------------------------------------------------
-- Control = 'off', Treatment = 'mule_sq_100'
WITH base AS (
  SELECT
    COALESCE(buyer_segment, 'Unknown') AS buyer_segment,
    variant_id,
    COALESCE(rec_seen, 0) AS rec_seen,
    COALESCE(rec_clicked, 0) AS rec_clicked
  FROM `etsy-data-warehouse-dev.tsebastian.bound_reset_sq_add` 
  WHERE variant_id IN ('off','mule_sq_100')
),
agg AS (
  SELECT
    buyer_segment,
    variant_id,
    SUM(rec_seen) AS exposures,
    SUM(rec_clicked) AS clicks
  FROM base
  GROUP BY 1,2
),
tot AS (
  SELECT variant_id, SUM(exposures) AS total_exposures
  FROM agg
  GROUP BY 1
),
metrics AS (
  SELECT
    a.buyer_segment,
    a.variant_id,
    a.exposures,
    a.clicks,
    SAFE_DIVIDE(a.clicks, NULLIF(a.exposures, 0)) AS ctr,
    SAFE_DIVIDE(a.exposures, t.total_exposures) AS exposure_share
  FROM agg a
  JOIN tot t USING (variant_id)
)
SELECT
  buyer_segment,

  COALESCE(MAX(IF(variant_id='off', exposures, NULL)), 0) AS exposures_control,
  COALESCE(MAX(IF(variant_id='mule_sq_100', exposures, NULL)), 0) AS exposures_treatment,
  COALESCE(MAX(IF(variant_id='off', ctr, NULL)), 0) AS ctr_control,
  COALESCE(MAX(IF(variant_id='mule_sq_100', ctr, NULL)), 0) AS ctr_treatment,

  COALESCE(MAX(IF(variant_id='off', exposure_share, NULL)), 0) AS share_control,
  COALESCE(MAX(IF(variant_id='mule_sq_100', exposure_share, NULL)), 0) AS share_treatment,

  SAFE_DIVIDE(
    COALESCE(MAX(IF(variant_id='mule_sq_100', exposures, NULL)), 0)
      - COALESCE(MAX(IF(variant_id='off', exposures, NULL)), 0),
    NULLIF(COALESCE(MAX(IF(variant_id='off', exposures, NULL)), 0), 0)
  ) AS exposure_lift_pct,

  SAFE_DIVIDE(
    COALESCE(MAX(IF(variant_id='mule_sq_100', ctr, NULL)), 0)
      - COALESCE(MAX(IF(variant_id='off', ctr, NULL)), 0),
    NULLIF(COALESCE(MAX(IF(variant_id='off', ctr, NULL)), 0), 0)
  ) AS ctr_lift_pct

FROM metrics
GROUP BY buyer_segment
ORDER BY ctr_lift_pct DESC;


------------------------------------------------------------
-- Q14) Lift by visit channel
------------------------------------------------------------
-- Control = 'off', Treatment = 'mule_sq_100'
WITH base AS (
  SELECT
    COALESCE(visit_channel, 'Unknown') AS visit_channel,
    variant_id,
    COALESCE(rec_seen, 0) AS rec_seen,
    COALESCE(rec_clicked, 0) AS rec_clicked
  FROM `etsy-data-warehouse-dev.tsebastian.bound_reset_sq_add` 
  WHERE variant_id IN ('off','mule_sq_100')
),
agg AS (
  SELECT
    visit_channel,
    variant_id,
    SUM(rec_seen) AS exposures,
    SUM(rec_clicked) AS clicks
  FROM base
  GROUP BY 1,2
),
tot AS (
  SELECT variant_id, SUM(exposures) AS total_exposures
  FROM agg
  GROUP BY 1
),
metrics AS (
  SELECT
    a.visit_channel,
    a.variant_id,
    a.exposures,
    a.clicks,
    SAFE_DIVIDE(a.clicks, NULLIF(a.exposures, 0)) AS ctr,
    SAFE_DIVIDE(a.exposures, t.total_exposures) AS exposure_share
  FROM agg a
  JOIN tot t USING (variant_id)
)
SELECT
  visit_channel,

  COALESCE(MAX(IF(variant_id='off', exposures, NULL)), 0) AS exposures_control,
  COALESCE(MAX(IF(variant_id='mule_sq_100', exposures, NULL)), 0) AS exposures_treatment,
  COALESCE(MAX(IF(variant_id='off', ctr, NULL)), 0) AS ctr_control,
  COALESCE(MAX(IF(variant_id='mule_sq_100', ctr, NULL)), 0) AS ctr_treatment,

  COALESCE(MAX(IF(variant_id='off', exposure_share, NULL)), 0) AS share_control,
  COALESCE(MAX(IF(variant_id='mule_sq_100', exposure_share, NULL)), 0) AS share_treatment,

  SAFE_DIVIDE(
    COALESCE(MAX(IF(variant_id='mule_sq_100', exposures, NULL)), 0)
      - COALESCE(MAX(IF(variant_id='off', exposures, NULL)), 0),
    NULLIF(COALESCE(MAX(IF(variant_id='off', exposures, NULL)), 0), 0)
  ) AS exposure_lift_pct,

  SAFE_DIVIDE(
    COALESCE(MAX(IF(variant_id='mule_sq_100', ctr, NULL)), 0)
      - COALESCE(MAX(IF(variant_id='off', ctr, NULL)), 0),
    NULLIF(COALESCE(MAX(IF(variant_id='off', ctr, NULL)), 0), 0)
  ) AS ctr_lift_pct

FROM metrics
GROUP BY visit_channel
ORDER BY ctr_lift_pct DESC;


------------------------------------------------------------
-- Q15) Lift by buyer tenure
------------------------------------------------------------
-- Control = 'off', Treatment = 'mule_sq_100'
WITH base AS (
  SELECT
    CAST(buyer_tenure AS INT64) AS buyer_tenure_days,
    variant_id,
    COALESCE(rec_seen, 0) AS rec_seen,
    COALESCE(rec_clicked, 0) AS rec_clicked
  FROM `etsy-data-warehouse-dev.tsebastian.bound_reset_sq_add` 
  WHERE variant_id IN ('off','mule_sq_100')
),
buckets AS (
  SELECT
    variant_id,
    CASE
      WHEN buyer_tenure_days IS NULL THEN 'Unknown'
      WHEN buyer_tenure_days <= 30 THEN '0–30d'
      WHEN buyer_tenure_days <= 90 THEN '31–90d'
      WHEN buyer_tenure_days <= 180 THEN '91–180d'
      WHEN buyer_tenure_days <= 365 THEN '181–365d'
      ELSE '>365d'
    END AS buyer_tenure_bucket,
    rec_seen,
    rec_clicked
  FROM base
),
agg AS (
  SELECT
    buyer_tenure_bucket,
    variant_id,
    SUM(rec_seen) AS exposures,
    SUM(rec_clicked) AS clicks
  FROM buckets
  GROUP BY 1,2
),
tot AS (
  SELECT variant_id, SUM(exposures) AS total_exposures
  FROM agg
  GROUP BY 1
),
metrics AS (
  SELECT
    a.buyer_tenure_bucket,
    a.variant_id,
    a.exposures,
    a.clicks,
    SAFE_DIVIDE(a.clicks, NULLIF(a.exposures, 0)) AS ctr,
    SAFE_DIVIDE(a.exposures, t.total_exposures) AS exposure_share
  FROM agg a
  JOIN tot t USING (variant_id)
)
SELECT
  buyer_tenure_bucket,

  COALESCE(MAX(IF(variant_id='off', exposures, NULL)), 0) AS exposures_control,
  COALESCE(MAX(IF(variant_id='mule_sq_100', exposures, NULL)), 0) AS exposures_treatment,
  COALESCE(MAX(IF(variant_id='off', ctr, NULL)), 0) AS ctr_control,
  COALESCE(MAX(IF(variant_id='mule_sq_100', ctr, NULL)), 0) AS ctr_treatment,

  COALESCE(MAX(IF(variant_id='off', exposure_share, NULL)), 0) AS share_control,
  COALESCE(MAX(IF(variant_id='mule_sq_100', exposure_share, NULL)), 0) AS share_treatment,

  SAFE_DIVIDE(
    COALESCE(MAX(IF(variant_id='mule_sq_100', exposures, NULL)), 0)
      - COALESCE(MAX(IF(variant_id='off', exposures, NULL)), 0),
    NULLIF(COALESCE(MAX(IF(variant_id='off', exposures, NULL)), 0), 0)
  ) AS exposure_lift_pct,

  SAFE_DIVIDE(
    COALESCE(MAX(IF(variant_id='mule_sq_100', ctr, NULL)), 0)
      - COALESCE(MAX(IF(variant_id='off', ctr, NULL)), 0),
    NULLIF(COALESCE(MAX(IF(variant_id='off', ctr, NULL)), 0), 0)
  ) AS ctr_lift_pct

FROM metrics
GROUP BY buyer_tenure_bucket
ORDER BY buyer_tenure_bucket;


------------------------------------------------------------
-- Q16) Lift by buyer searched top category
------------------------------------------------------------
-- One row per l0_cat_1 with control vs. treatment metrics and lifts
WITH base AS (
  SELECT
    COALESCE(LOWER(l0_cat_1), 'unknown') AS l0_cat_1,
    variant_id,
    COALESCE(rec_seen, 0) AS rec_seen,
    COALESCE(rec_clicked, 0) AS rec_clicked
  FROM `etsy-data-warehouse-dev.tsebastian.bound_reset_sq_add` 
  WHERE variant_id IN ('off','mule_sq_100')

),
agg AS (
  SELECT
    l0_cat_1,
    variant_id,
    SUM(rec_seen)   AS exposures,
    SUM(rec_clicked) AS clicks
  FROM base
  GROUP BY 1,2
),
tot AS (
  SELECT
    variant_id,
    SUM(exposures) AS total_exposures
  FROM agg
  GROUP BY 1
),
metrics AS (
  SELECT
    a.l0_cat_1,
    a.variant_id,
    a.exposures,
    a.clicks,
    SAFE_DIVIDE(a.clicks, NULLIF(a.exposures, 0)) AS ctr,                 -- CTR
    SAFE_DIVIDE(a.exposures, t.total_exposures)   AS exposure_share        -- Share of exposures within variant
  FROM agg a
  JOIN tot t USING (variant_id)
)
SELECT
  l0_cat_1,

  -- Control metrics
  COALESCE(MAX(IF(variant_id='off', exposures, NULL)), 0) AS exposures_control,
  COALESCE(MAX(IF(variant_id='off', exposure_share, NULL)), 0) AS share_control,
  COALESCE(MAX(IF(variant_id='off', ctr, NULL)), 0) AS ctr_control,

  -- Treatment metrics
  COALESCE(MAX(IF(variant_id='mule_sq_100', exposures, NULL)), 0) AS exposures_treatment,
  COALESCE(MAX(IF(variant_id='mule_sq_100', exposure_share, NULL)), 0) AS share_treatment,
  COALESCE(MAX(IF(variant_id='mule_sq_100', ctr, NULL)), 0) AS ctr_treatment,

  -- Lifts (treatment vs. control)
  SAFE_DIVIDE(
    COALESCE(MAX(IF(variant_id='mule_sq_100', exposures, NULL)), 0)
    - COALESCE(MAX(IF(variant_id='off', exposures, NULL)), 0),
    NULLIF(COALESCE(MAX(IF(variant_id='off', exposures, NULL)), 0), 0)
  ) AS exposure_lift_pct,

  SAFE_DIVIDE(
    COALESCE(MAX(IF(variant_id='mule_sq_100', ctr, NULL)), 0)
    - COALESCE(MAX(IF(variant_id='off', ctr, NULL)), 0),
    NULLIF(COALESCE(MAX(IF(variant_id='off', ctr, NULL)), 0), 0)
  ) AS ctr_lift_pct

FROM metrics
GROUP BY l0_cat_1
ORDER BY ctr_lift_pct DESC;


------------------------------------------------------------
-- Q17) Lift by buyer segment x total searches
------------------------------------------------------------

-- Control = 'off', Treatment = 'mule_sq_100'
-- 3 buckets for buyer_searches_60d: 0, 1–5, 6+
-- 3 combined buyer segments: New/Not active, Active/High potential, Repeat/Habitual

WITH base AS (
  SELECT
    variant_id,
    LOWER(COALESCE(buyer_segment, 'unknown')) AS buyer_segment_lc,
    CAST(buyer_searches_60d AS INT64) AS buyer_searches_60d,
    COALESCE(rec_seen, 0) AS rec_seen,
    COALESCE(rec_clicked, 0) AS rec_clicked,
  FROM `etsy-data-warehouse-dev.tsebastian.bound_reset_sq_add`
  WHERE variant_id IN ('off', 'mule_sq_100')
),

-- Map to 3 combined buyer segment groups (adjust synonyms as needed)
segment_map AS (
  SELECT
    variant_id,
    CASE
      WHEN lower(buyer_segment_lc) IN ('new', 'not active') THEN 'New/Not active'
      WHEN lower(buyer_segment_lc) IN ('active', 'high potential') THEN 'Active/High potential'
      WHEN lower(buyer_segment_lc) IN ('repeat', 'habitual') THEN 'Repeat/Habitual'
      ELSE 'Other'
    END AS combined_segment,
    CASE
      WHEN coalesce(buyer_searches_60d,0) = 0 THEN 'a.0'
      WHEN buyer_searches_60d BETWEEN 1 AND 5 THEN 'b.1–5'
      ELSE 'c.6+'
    END AS searches_60d_bucket,
    rec_seen,
    rec_clicked,
  FROM base
),
-- Keep only the 3×3 cells
filtered AS (
  SELECT *
  FROM segment_map
),

-- Aggregate per cell and variant
agg AS (
  SELECT
    combined_segment,
    searches_60d_bucket,
    variant_id,
    SUM(rec_seen) AS exposures,
    SUM(rec_clicked) AS clicks,
  FROM filtered
  GROUP BY 1,2,3
),

-- Pivot control vs treatment
pivoted AS (
  SELECT
    combined_segment,
    searches_60d_bucket,
    SUM(IF(variant_id = 'off', exposures, 0)) AS exposures_control,
    SUM(IF(variant_id = 'off', clicks, 0)) AS clicks_control,

    SUM(IF(variant_id = 'mule_sq_100', exposures, 0)) AS exposures_treatment,
    SUM(IF(variant_id = 'mule_sq_100', clicks, 0)) AS clicks_treatment,

  FROM agg
  GROUP BY 1,2
)

SELECT
  combined_segment,
  searches_60d_bucket,

  -- Control metrics
  exposures_control,
  SAFE_DIVIDE(clicks_control, NULLIF(exposures_control, 0)) AS ctr_control,

  -- Treatment metrics
  exposures_treatment,
  SAFE_DIVIDE(clicks_treatment, NULLIF(exposures_treatment, 0)) AS ctr_treatment,


  -- Lifts (treatment vs control)
  SAFE_DIVIDE(
    SAFE_DIVIDE(clicks_treatment, NULLIF(exposures_treatment, 0))
      - SAFE_DIVIDE(clicks_control, NULLIF(exposures_control, 0)),
    NULLIF(SAFE_DIVIDE(clicks_control, NULLIF(exposures_control, 0)), 0)
  ) AS ctr_lift_pct,


FROM pivoted
ORDER BY
  CASE combined_segment
    WHEN 'New/Not active' THEN 1
    WHEN 'Active/High potential' THEN 2
    WHEN 'Repeat/Habitual' THEN 3
    ELSE 9
  END,
  CASE searches_60d_bucket
    WHEN '0' THEN 1
    WHEN '1–5' THEN 2
    WHEN '6+' THEN 3
    ELSE 9
  END;



-- -- -- -- -- -- -- -- -- 
-- Q18) Exposure and CTR lift by days since last visit
-- -- -- -- -- -- -- -- -- 

WITH 
agg AS (
  SELECT
    coalesce(days_since_last_visit,'Unknown or >60') as days_since_last_visit,
    variant_id,
    SUM(rec_seen) AS exposures,
    SUM(rec_clicked) AS clicks
  FROM  `etsy-data-warehouse-dev.tsebastian.bound_reset_sq_add`
    WHERE variant_id IN ('off', 'mule_sq_100')
  GROUP BY 1, 2
),
pivoted AS (
  SELECT
    days_since_last_visit,
    SUM(IF(variant_id = 'off',        exposures, 0)) AS exposures_control,
    SUM(IF(variant_id = 'off',        clicks,    0)) AS clicks_control,
    SUM(IF(variant_id = 'mule_sq_100', exposures, 0)) AS exposures_treatment,
    SUM(IF(variant_id = 'mule_sq_100', clicks,    0)) AS clicks_treatment
  FROM agg
  GROUP BY 1
)
SELECT
  days_since_last_visit,

  -- Control metrics
  exposures_control,
  SAFE_DIVIDE(clicks_control, NULLIF(exposures_control, 0)) AS ctr_control,

  -- Treatment metrics
  exposures_treatment,
  SAFE_DIVIDE(clicks_treatment, NULLIF(exposures_treatment, 0)) AS ctr_treatment,

  -- Lifts (treatment vs. control)
  SAFE_DIVIDE(exposures_treatment - exposures_control, NULLIF(exposures_control, 0)) AS exposure_lift_pct,
  SAFE_DIVIDE(
    SAFE_DIVIDE(clicks_treatment, NULLIF(exposures_treatment, 0))
      - SAFE_DIVIDE(clicks_control, NULLIF(exposures_control, 0)),
    NULLIF(SAFE_DIVIDE(clicks_control, NULLIF(exposures_control, 0)), 0)
  ) AS ctr_lift_pct

FROM pivoted
ORDER BY days_since_last_visit;


-- -- -- -- -- -- -- -- -- 
-- Q19) xposure and CTR lift by composite home engagement (30d) buckets
-- -- -- -- -- -- -- -- -- 

-- Control = 'off', Treatment = 'mule_sq_100'
-- One row per composite 30d home engagement bucket with exposure and CTR lifts

WITH base AS (
  SELECT
    variant_id,
    -- Composite engagement over last 30d from home rec modules
    COALESCE(CAST(home_clicks_30 AS INT64), 0)
    + COALESCE(CAST(home_favs_30 AS INT64), 0)
    + COALESCE(CAST(home_cart_adds_30 AS INT64), 0) AS home_eng_30,
    COALESCE(rec_seen, 0) AS rec_seen,
    COALESCE(rec_clicked, 0) AS rec_clicked
  FROM `etsy-data-warehouse-dev.tsebastian.bound_reset_sq_add`
  WHERE variant_id IN ('off', 'mule_sq_100')
),
buckets AS (
  SELECT
    variant_id,
    CASE
      WHEN home_eng_30 IS NULL THEN 'Unknown'
      WHEN home_eng_30 = 0 THEN '0'
      WHEN home_eng_30 BETWEEN 1 AND 5 THEN '1–5'
      WHEN home_eng_30 BETWEEN 6 AND 20 THEN '6–20'
      WHEN home_eng_30 BETWEEN 21 AND 50 THEN '21–50'
      ELSE '51+'
    END AS home_eng_30_bucket,
    rec_seen,
    rec_clicked
  FROM base
),
agg AS (
  SELECT
    home_eng_30_bucket,
    variant_id,
    SUM(rec_seen) AS exposures,
    SUM(rec_clicked) AS clicks
  FROM buckets
  GROUP BY 1, 2
),
pivoted AS (
  SELECT
    home_eng_30_bucket,
    SUM(IF(variant_id = 'off',        exposures, 0)) AS exposures_control,
    SUM(IF(variant_id = 'off',        clicks,    0)) AS clicks_control,
    SUM(IF(variant_id = 'mule_sq_100', exposures, 0)) AS exposures_treatment,
    SUM(IF(variant_id = 'mule_sq_100', clicks,    0)) AS clicks_treatment
  FROM agg
  GROUP BY 1
)
SELECT
  home_eng_30_bucket,

  -- Control metrics
  exposures_control,
  SAFE_DIVIDE(clicks_control, NULLIF(exposures_control, 0)) AS ctr_control,

  -- Treatment metrics
  exposures_treatment,
  SAFE_DIVIDE(clicks_treatment, NULLIF(exposures_treatment, 0)) AS ctr_treatment,

  -- Lifts (treatment vs. control)
  SAFE_DIVIDE(exposures_treatment - exposures_control, NULLIF(exposures_control, 0)) AS exposure_lift_pct,
  SAFE_DIVIDE(
    SAFE_DIVIDE(clicks_treatment, NULLIF(exposures_treatment, 0))
      - SAFE_DIVIDE(clicks_control, NULLIF(exposures_control, 0)),
    NULLIF(SAFE_DIVIDE(clicks_control, NULLIF(exposures_control, 0)), 0)
  ) AS ctr_lift_pct

FROM pivoted
ORDER BY
  CASE home_eng_30_bucket
    WHEN '0' THEN 1
    WHEN '1–5' THEN 2
    WHEN '6–20' THEN 3
    WHEN '21–50' THEN 4
    WHEN '51+' THEN 5
    ELSE 9
  END;


-- -- -- -- -- -- -- -- -- 
-- Q19) xposure and CTR lift by composite home engagement (30d) buckets
-- -- -- -- -- -- -- -- -- 


  -- Inputs 
DECLARE lookback_window INT64 DEFAULT 60;   
DECLARE start_date DATE default '2025-11-07';
DECLARE end_date DATE default '2025-11-13';    

-- 1) Experiment rows from your table (test window)
WITH exp AS (
  SELECT
    user_id,
    variant_id,
    listing_taxonomy_full_path,
    rec_clicked,
    buyer_searches_60d,
    home_clicks_7, home_clicks_30, home_clicks_90
  FROM `etsy-data-warehouse-dev.tsebastian.bound_reset_sq_add`
  WHERE _date BETWEEN start_date AND end_date
    AND variant_id IN ('off','mule_sq_100')
),

-- 2) Pre-test click-to-search ratio per user (approximate ~60d from available windows)
pre_ratio AS (
  SELECT
    user_id,
    COALESCE(
      0.6667 * MAX(home_clicks_90),  -- ~60d from 90d
      0
    ) AS clicks_60d_approx,
    COALESCE(MAX(buyer_searches_60d), 0) AS searches_60d,
    SAFE_DIVIDE(
      COALESCE(
        0.6667 * MAX(home_clicks_90),
        0
      ),
      NULLIF(COALESCE(MAX(buyer_searches_60d), 0), 0)
    ) AS ratio
  FROM exp
  GROUP BY user_id
),

-- 3) Separate positive-ratio users and assign NTILE(4) only to positives
pos AS (
  SELECT user_id, ratio
  FROM pre_ratio
  WHERE searches_60d > 0 AND ratio > 0
),
pos_with_quartile AS (
  SELECT
    user_id,
    CONCAT('Q', CAST(NTILE(4) OVER (ORDER BY ratio) AS STRING)) AS pos_quartile,
    NTILE(4) OVER (ORDER BY ratio) AS order_key
  FROM pos
),

-- 4) Final ratio buckets:
--    "No searches" (searches_60d=0), "Zero ratio" (ratio=0 with searches>0), and Q1..Q4 for positives
ratio_buckets AS (
  SELECT
    pr.user_id,
    CASE
      WHEN pr.searches_60d = 0 THEN 'No searches'
      WHEN pr.ratio = 0 THEN 'Zero ratio'
      ELSE pwq.pos_quartile
    END AS ratio_bucket,
    CASE
      WHEN pr.searches_60d = 0 THEN 0
      WHEN pr.ratio = 0 THEN 1
      ELSE 100 + pwq.order_key  -- positives sort after zero
    END AS sort_key
  FROM pre_ratio pr
  LEFT JOIN pos_with_quartile pwq
    ON pr.user_id = pwq.user_id
),

-- 5) Pre-period taxonomies seen per user (to define "new" during test)
users_in_exp AS (SELECT DISTINCT user_id FROM exp),
recent_user_taxos_seen AS (
  SELECT DISTINCT
    v.user_id,
    t.full_path
  FROM `etsy-data-warehouse-prod.analytics.listing_views` lv
  JOIN `etsy-data-warehouse-prod.visit_mart.visits` v
    ON v.visit_id = lv.visit_id
  LEFT JOIN `etsy-data-warehouse-prod.materialized.listing_taxonomy` t
    ON t.listing_id = lv.listing_id
  JOIN users_in_exp u
    ON u.user_id = v.user_id
  WHERE lv._date > DATE_SUB(start_date, INTERVAL lookback_window DAY)
    AND lv._date <= start_date
),

-- 6) Per-user, per-variant true "new missions" during test:
--    distinct clicked taxonomy full_paths not seen in pre-period
per_user_variant AS (
  SELECT
    e.user_id,
    e.variant_id,
    COUNT(DISTINCT CASE
      WHEN e.rec_clicked = 1
       AND e.listing_taxonomy_full_path IS NOT NULL
       AND NOT EXISTS (
         SELECT 1
         FROM recent_user_taxos_seen r
         WHERE r.user_id = e.user_id
           AND r.full_path = e.listing_taxonomy_full_path
       )
      THEN e.listing_taxonomy_full_path
    END) AS new_missions
  FROM exp e
  GROUP BY 1,2
),

-- 7) Aggregate by ratio bucket x variant
per_bucket_variant AS (
  SELECT
    b.ratio_bucket,
    b.sort_key,
    p.variant_id,
    COUNT(*) AS users_in_bucket_variant,
    AVG(p.new_missions) AS avg_new_missions
  FROM per_user_variant p
  JOIN ratio_buckets b USING (user_id)
  GROUP BY 1,2,3
)

-- 8) Final: control vs treatment averages and lift per bucket
SELECT
  ratio_bucket,
  COALESCE(MAX(IF(variant_id='off',         users_in_bucket_variant, NULL)), 0) AS users_control,
  COALESCE(MAX(IF(variant_id='mule_sq_100', users_in_bucket_variant, NULL)), 0) AS users_treatment,

  COALESCE(MAX(IF(variant_id='off',         avg_new_missions, NULL)), 0) AS avg_new_missions_control,
  COALESCE(MAX(IF(variant_id='mule_sq_100', avg_new_missions, NULL)), 0) AS avg_new_missions_treatment,

  COALESCE(MAX(IF(variant_id='mule_sq_100', avg_new_missions, NULL)), 0)
    - COALESCE(MAX(IF(variant_id='off',     avg_new_missions, NULL)), 0) AS delta_avg_new_missions,

  SAFE_DIVIDE(
    COALESCE(MAX(IF(variant_id='mule_sq_100', avg_new_missions, NULL)), 0)
      - COALESCE(MAX(IF(variant_id='off',     avg_new_missions, NULL)), 0),
    NULLIF(COALESCE(MAX(IF(variant_id='off',  avg_new_missions, NULL)), 0), 0)
  ) AS lift_pct_vs_control
FROM per_bucket_variant
GROUP BY ratio_bucket, sort_key
ORDER BY sort_key, ratio_bucket;



-- -- -- -- -- -- -- -- -- 
-- Q20) Exposure and CTR lift by visit_canonical_region
-- -- -- -- -- -- -- -- -- 

WITH 
agg AS (
  SELECT
    coalesce(visit_canonical_region,'Unknown') as visit_canonical_region,
    variant_id,
    SUM(rec_seen) AS exposures,
    SUM(rec_clicked) AS clicks
  FROM  `etsy-data-warehouse-dev.tsebastian.bound_reset_sq_add`
    WHERE variant_id IN ('off', 'mule_sq_100')
  GROUP BY 1, 2
),
pivoted AS (
  SELECT
    visit_canonical_region,
    SUM(IF(variant_id = 'off',        exposures, 0)) AS exposures_control,
    SUM(IF(variant_id = 'off',        clicks,    0)) AS clicks_control,
    SUM(IF(variant_id = 'mule_sq_100', exposures, 0)) AS exposures_treatment,
    SUM(IF(variant_id = 'mule_sq_100', clicks,    0)) AS clicks_treatment
  FROM agg
  GROUP BY 1
)
SELECT
  visit_canonical_region,

  -- Control metrics
  exposures_control,
  SAFE_DIVIDE(clicks_control, NULLIF(exposures_control, 0)) AS ctr_control,

  -- Treatment metrics
  exposures_treatment,
  SAFE_DIVIDE(clicks_treatment, NULLIF(exposures_treatment, 0)) AS ctr_treatment,

  -- Lifts (treatment vs. control)
  SAFE_DIVIDE(exposures_treatment - exposures_control, NULLIF(exposures_control, 0)) AS exposure_lift_pct,
  SAFE_DIVIDE(
    SAFE_DIVIDE(clicks_treatment, NULLIF(exposures_treatment, 0))
      - SAFE_DIVIDE(clicks_control, NULLIF(exposures_control, 0)),
    NULLIF(SAFE_DIVIDE(clicks_control, NULLIF(exposures_control, 0)), 0)
  ) AS ctr_lift_pct

FROM pivoted
ORDER BY visit_canonical_region;



-- -- -- -- -- -- -- -- -- 
-- Q21) Exposure and CTR lift by >50% of each type
-- -- -- -- -- -- -- -- -- 

-- WITH base AS (
--   SELECT
--     variant_id,
--     COALESCE(CAST(rec_seen AS INT64), 0) AS rec_seen,
--     COALESCE(CAST(rec_clicked AS INT64), 0) AS rec_clicked,

--     -- 90d query counts (replace names here if yours differ)
--     COALESCE(CAST(qis_broad_90d AS INT64), 0)               AS q_broad_90d,
--     COALESCE(CAST(qis_direct_unspecified_90d AS INT64), 0)  AS q_direct_unspec_90d,
--     COALESCE(CAST(qis_direct_specified_90d AS INT64), 0)    AS q_direct_spec_90d
--   FROM `etsy-data-warehouse-dev.tsebastian.bound_reset_sq_add`
--   WHERE variant_id IN ('off', 'mule_sq_100')
-- ),

-- cohorts AS (
--   SELECT
--     variant_id,
--     rec_seen,
--     rec_clicked,
--     q_broad_90d,
--     q_direct_unspec_90d,
--     q_direct_spec_90d,
--     (q_broad_90d + q_direct_unspec_90d + q_direct_spec_90d) AS q_total_90d,
--     CASE
--       WHEN (q_broad_90d + q_direct_unspec_90d + q_direct_spec_90d) > 0
--            AND SAFE_DIVIDE(q_broad_90d, (q_broad_90d + q_direct_unspec_90d + q_direct_spec_90d)) >= 0.5
--         THEN 'Broad ≥50%'
--       WHEN (q_broad_90d + q_direct_unspec_90d + q_direct_spec_90d) > 0
--            AND SAFE_DIVIDE(q_direct_unspec_90d, (q_broad_90d + q_direct_unspec_90d + q_direct_spec_90d)) >= 0.5
--         THEN 'Direct (unspecified) ≥50%'
--       WHEN (q_broad_90d + q_direct_unspec_90d + q_direct_spec_90d) > 0
--            AND SAFE_DIVIDE(q_direct_spec_90d, (q_broad_90d + q_direct_unspec_90d + q_direct_spec_90d)) >= 0.5
--         THEN 'Direct (specified) ≥50%'
--       ELSE 'Other'
--     END AS query_mix_group
--   FROM base
-- ), filtered AS (
--   SELECT *
--   FROM cohorts
--   WHERE q_total_90d > 0
--     AND query_mix_group IN ('Broad ≥50%', 'Direct (unspecified) ≥50%', 'Direct (specified) ≥50%')
-- ),

-- -- Aggregate per cohort and variant
-- agg AS (
--   SELECT
--     query_mix_group,
--     variant_id,
--     SUM(rec_seen) AS exposures,
--     SUM(rec_clicked) AS clicks
--   FROM filtered
--   GROUP BY 1, 2
-- ),

-- -- Pivot control vs treatment
-- pivoted AS (
--   SELECT
--     query_mix_group,
--     SUM(IF(variant_id = 'off',        exposures, 0)) AS exposures_control,
--     SUM(IF(variant_id = 'off',        clicks,    0)) AS clicks_control,
--     SUM(IF(variant_id = 'mule_sq_100', exposures, 0)) AS exposures_treatment,
--     SUM(IF(variant_id = 'mule_sq_100', clicks,    0)) AS clicks_treatment
--   FROM agg
--   GROUP BY 1
-- )

-- SELECT
--   query_mix_group,

--   -- Control metrics
--   exposures_control,
--   SAFE_DIVIDE(clicks_control, NULLIF(exposures_control, 0)) AS ctr_control,

--   -- Treatment metrics
--   exposures_treatment,
--   SAFE_DIVIDE(clicks_treatment, NULLIF(exposures_treatment, 0)) AS ctr_treatment,

--   -- CTR lift (treatment vs control)
--   SAFE_DIVIDE(
--     SAFE_DIVIDE(clicks_treatment, NULLIF(exposures_treatment, 0))
--       - SAFE_DIVIDE(clicks_control, NULLIF(exposures_control, 0)),
--     NULLIF(SAFE_DIVIDE(clicks_control, NULLIF(exposures_control, 0)), 0)
--   ) AS ctr_lift_pct

-- FROM pivoted
-- ORDER BY
--   CASE query_mix_group
--     WHEN 'Broad ≥50%' THEN 1
--     WHEN 'Direct (unspecified) ≥50%' THEN 2
--     WHEN 'Direct (specified) ≥50%' THEN 3
--     ELSE 9
--   END;


-- -- -- -- -- -- -- -- -- 
-- Q22) Exposure and CTR lift by predominant search type
-- -- -- -- -- -- -- -- -- 

WITH user_searches AS (
  SELECT
    user_id,
    -- Use MAX to capture the user-level 90d counts without multiplying by impressions
    MAX(COALESCE(CAST(qis_broad_90d AS INT64), 0))              AS q_broad_90d,
    MAX(COALESCE(CAST(qis_direct_unspecified_90d AS INT64), 0)) AS q_direct_unspec_90d,
    MAX(COALESCE(CAST(qis_direct_specified_90d AS INT64), 0))   AS q_direct_spec_90d
  FROM `etsy-data-warehouse-dev.tsebastian.bound_reset_sq_add`
  GROUP BY user_id
),

user_cohort AS (
  SELECT
    user_id,
    q_broad_90d,
    q_direct_unspec_90d,
    q_direct_spec_90d,
    (q_broad_90d + q_direct_unspec_90d + q_direct_spec_90d) AS q_total_90d,
    CASE
      WHEN q_broad_90d > q_direct_unspec_90d AND q_broad_90d > q_direct_spec_90d
        THEN 'Broad (predominant)'
      WHEN q_direct_unspec_90d > q_broad_90d AND q_direct_unspec_90d > q_direct_spec_90d
        THEN 'Direct unspecified (predominant)'
      WHEN q_direct_spec_90d > q_broad_90d AND q_direct_spec_90d > q_direct_unspec_90d
        THEN 'Direct specified (predominant)'
      ELSE 'No predominant / tie'
    END AS predominant_search_type
  FROM user_searches
),

-- Keep only users with any searches and a clear predominant type
eligible_users AS (
  SELECT user_id, predominant_search_type
  FROM user_cohort
  WHERE q_total_90d > 0
    AND predominant_search_type IN (
      'Broad (predominant)',
      'Direct unspecified (predominant)',
      'Direct specified (predominant)'
    )
),

-- Join cohorts to impressions and aggregate by cohort × variant
agg AS (
  SELECT
    eu.predominant_search_type,
    t.variant_id,
    SUM(COALESCE(t.rec_seen, 0))    AS exposures,
    SUM(COALESCE(t.rec_clicked, 0)) AS clicks
  FROM `etsy-data-warehouse-dev.tsebastian.bound_reset_sq_add` t
  JOIN eligible_users eu
    ON eu.user_id = t.user_id
  WHERE t.variant_id IN ('off','mule_sq_100')
  GROUP BY 1,2
),

-- Pivot control vs. treatment
pivoted AS (
  SELECT
    predominant_search_type,
    SUM(IF(variant_id = 'off',        exposures, 0)) AS exposures_control,
    SUM(IF(variant_id = 'off',        clicks,    0)) AS clicks_control,
    SUM(IF(variant_id = 'mule_sq_100', exposures, 0)) AS exposures_treatment,
    SUM(IF(variant_id = 'mule_sq_100', clicks,    0)) AS clicks_treatment
  FROM agg
  GROUP BY 1
)

SELECT
  predominant_search_type,

  -- Control metrics
  exposures_control,
  SAFE_DIVIDE(clicks_control, NULLIF(exposures_control, 0)) AS ctr_control,

  -- Treatment metrics
  exposures_treatment,
  SAFE_DIVIDE(clicks_treatment, NULLIF(exposures_treatment, 0)) AS ctr_treatment,

  -- CTR lift (treatment vs. control)
  SAFE_DIVIDE(
    SAFE_DIVIDE(clicks_treatment, NULLIF(exposures_treatment, 0))
      - SAFE_DIVIDE(clicks_control, NULLIF(exposures_control, 0)),
    NULLIF(SAFE_DIVIDE(clicks_control, NULLIF(exposures_control, 0)), 0)
  ) AS ctr_lift_pct

FROM pivoted
ORDER BY
  CASE predominant_search_type
    WHEN 'Broad (predominant)' THEN 1
    WHEN 'Direct unspecified (predominant)' THEN 2
    WHEN 'Direct specified (predominant)' THEN 3
    ELSE 9
  END;


-- -- -- -- -- -- -- -- -- 
-- Q23) New Missions started
-- -- -- -- -- -- -- -- -- 

  -- PARAMETERS
DECLARE my_experiment STRING DEFAULT 'boe_home/app_home.feed.mule_sq';
DECLARE start_date DATE DEFAULT '2025-11-07';
DECLARE end_date   DATE DEFAULT '2025-11-13';
DECLARE module_placement_input STRING DEFAULT 'boe_homescreen_feed';
DECLARE lookback_window INT64 DEFAULT 60;

-- VARIANTS
DECLARE variants ARRAY<STRING> DEFAULT ['off','mule_sq_100'];

-- Bucketing type for the experiment
DECLARE bucketing_id_type INT64;
SET bucketing_id_type = (
  SELECT bucketing_id_type
  FROM `etsy-data-warehouse-prod.catapult_unified.experiment`
  WHERE _date = end_date
    AND experiment_id = my_experiment
);

-- 0) Experiment users universe
WITH exp_users AS (
  SELECT DISTINCT user_id
  FROM `etsy-data-warehouse-dev.tsebastian.bound_reset_sq_add`
  WHERE variant_id IN UNNEST(variants)
),

-- 1) 60d pre-experiment search CTR (platform=boe)
search_60d AS (
  SELECT
    si.user_id,
    SAFE_DIVIDE(
      COUNT(DISTINCT IF(si.clicked > 0, si.listing_id, NULL)),
      NULLIF(COUNT(DISTINCT si.listing_id), 0)
    ) AS search_ctr_60d,
    COUNT(DISTINCT si.mmx_request_uuid) AS searches_60d 
  FROM `etsy-data-warehouse-prod.rollups.search_impressions` si
  JOIN exp_users u
    ON u.user_id = si.user_id
  WHERE si._date BETWEEN DATE_SUB(start_date, INTERVAL lookback_window DAY)
                     AND DATE_SUB(start_date, INTERVAL 1 DAY)
    AND si.platform = 'boe'
  GROUP BY 1
),

-- 2) 60d pre-experiment feed CTR for the feed module placement
feed_60d AS (
  SELECT
    v.user_id,
    SAFE_DIVIDE(SUM(CAST(rdl.clicked AS INT64)), NULLIF(COUNT(1), 0)) AS feed_ctr_60d
  FROM `etsy-data-warehouse-prod.rollups.recsys_delivered_listings` rdl
  JOIN `etsy-data-warehouse-prod.visit_mart.visits` v
    ON v.visit_id = rdl.visit_id
  WHERE rdl._date BETWEEN DATE_SUB(start_date, INTERVAL lookback_window DAY)
                      AND DATE_SUB(start_date, INTERVAL 1 DAY)
    AND rdl.module_placement = module_placement_input
  GROUP BY 1
),

-- 3) Build deciles for search CTR and terciles for feed CTR
search_ranked AS (
  SELECT
    u.user_id,
    s.search_ctr_60d,
    CASE WHEN s.search_ctr_60d IS NULL THEN NULL
         ELSE NTILE(10) OVER (ORDER BY s.search_ctr_60d) END AS search_decile,
    s.searches_60d,  
  FROM exp_users u
  LEFT JOIN search_60d s
    ON s.user_id = u.user_id
),
feed_ranked AS (
  SELECT
    u.user_id,
    f.feed_ctr_60d,
    CASE WHEN f.feed_ctr_60d IS NULL THEN NULL
         ELSE NTILE(3) OVER (ORDER BY f.feed_ctr_60d) END AS feed_tercile
  FROM exp_users u
  LEFT JOIN feed_60d f
    ON f.user_id = u.user_id
),

user_strata AS (
  SELECT
    sr.user_id,
    IFNULL(FORMAT('d%02d', sr.search_decile), 'd00_no_search_data') AS search_decile_label,
    IFNULL(FORMAT('t%d', fr.feed_tercile),   't0_no_feed_data')     AS feed_tercile_label
  FROM search_ranked sr
  JOIN feed_ranked fr USING (user_id)
),
searches_60d_agg AS (
  SELECT
    us.search_decile_label,
    us.feed_tercile_label,
    COUNT(DISTINCT us.user_id) AS users_60d,
    SUM(sr.searches_60d) AS sum_searches_60d
  FROM user_strata us
  JOIN search_ranked sr
    ON sr.user_id = us.user_id
  GROUP BY 1, 2
),
-- 4) CTR metrics from bound reset table (experiment window)
ctr_agg AS (
  SELECT
    us.search_decile_label,
    us.feed_tercile_label,
    b.variant_id,
    SUM(b.rec_seen)    AS exposures,
    SUM(b.rec_clicked) AS clicks,
    COUNT(DISTINCT b.user_id) AS n_users
  FROM `etsy-data-warehouse-dev.tsebastian.bound_reset_sq_add` b
  JOIN user_strata us
    ON us.user_id = b.user_id
  WHERE b.variant_id IN UNNEST(variants)
  GROUP BY 1, 2, 3
),
ctr_pivoted AS (
  SELECT
    search_decile_label,
    feed_tercile_label,
    -- Control
    SUM(IF(variant_id = 'off', exposures, 0))   AS exposures_control,
    SUM(IF(variant_id = 'off', clicks, 0))      AS clicks_control,
    SUM(IF(variant_id = 'off', n_users, 0))     AS users_control,
    -- Treatment
    SUM(IF(variant_id = 'mule_sq_100', exposures, 0))   AS exposures_treatment,
    SUM(IF(variant_id = 'mule_sq_100', clicks, 0))      AS clicks_treatment,
    SUM(IF(variant_id = 'mule_sq_100', n_users, 0))     AS users_treatment
  FROM ctr_agg
  GROUP BY 1, 2
),

-- 5) New missions started metric during experiment window

ab_first_bucket_initial AS (
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
  GROUP BY b.bucketing_id, b.variant_id, b.bucketing_ts
),
subsequent_visits AS (
  -- Browser-based experiments (bucketing_id_type = 1)
  SELECT b.bucketing_id, b.variant_id, v.visit_id, v.user_id
  FROM ab_first_bucket b
  JOIN `etsy-data-warehouse-prod.weblog.visits` v
    ON bucketing_id_type = 1
   AND b.bucketing_id = v.browser_id
   AND TIMESTAMP_TRUNC(b.bucketing_ts, SECOND) <= v.end_datetime
   AND v._date BETWEEN start_date AND end_date
  UNION ALL
  -- User-based experiments (bucketing_id_type = 2)
  SELECT b.bucketing_id, b.variant_id, v.visit_id, v.user_id
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
    REGEXP_EXTRACT(full_path, r'^[^.]*.[^.]*') AS l2,
    REGEXP_EXTRACT(full_path, r'^[^.]*.[^.]*.[^.]*') AS l3
  FROM `etsy-data-warehouse-prod.materialized.listing_taxonomy`
),
recent_user_taxos_seen AS (
  SELECT DISTINCT v.user_id, pt.l1, pt.l2, pt.l3, pt.full_path
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
new_missions_per_user AS (
  SELECT
    v.variant_id,
    v.user_id,
    COUNT(DISTINCT CASE
      WHEN recent.full_path IS NULL
       AND rdl.clicked = 1
      THEN pt.full_path
    END) AS new_missions_started
  FROM subsequent_visits v
  -- experiment-period feed deliveries for the visits
  LEFT JOIN `etsy-data-warehouse-prod.rollups.recsys_delivered_listings` rdl
    ON rdl.visit_id = v.visit_id
   AND rdl._date > start_date
   AND rdl._date <= end_date
   AND rdl.module_placement = module_placement_input
  LEFT JOIN precomputed_taxonomy pt
    ON pt.listing_id = rdl.listing_id
  -- ensure user has recent activity window baked in
  JOIN recent_user_taxos_seen recent_active
    ON recent_active.user_id = v.user_id
  LEFT JOIN recent_user_taxos_seen recent
    ON recent.user_id = v.user_id
   AND recent.full_path = pt.full_path
  GROUP BY 1, 2
),
-- Limit to experiment users we bucketed 
new_missions_stratified AS (
  SELECT
    us.search_decile_label,
    us.feed_tercile_label,
    nm.variant_id,
    nm.user_id,
    nm.new_missions_started
  FROM new_missions_per_user nm
  JOIN user_strata us
    ON us.user_id = nm.user_id
  WHERE nm.variant_id IN UNNEST(variants)
),
new_missions_agg AS (
  SELECT
    search_decile_label,
    feed_tercile_label,
    variant_id,
    SUM(new_missions_started) AS sum_new_missions,
    COUNT(DISTINCT user_id)    AS users_nm
  FROM new_missions_stratified
  GROUP BY 1, 2, 3
),
new_missions_pivoted AS (
  SELECT
    search_decile_label,
    feed_tercile_label,
    -- Control
    SUM(IF(variant_id = 'off',        sum_new_missions, 0)) AS sum_nm_control,
    SUM(IF(variant_id = 'off',        users_nm,        0)) AS users_nm_control,
    -- Treatment
    SUM(IF(variant_id = 'mule_sq_100', sum_new_missions, 0)) AS sum_nm_treatment,
    SUM(IF(variant_id = 'mule_sq_100', users_nm,        0)) AS users_nm_treatment
  FROM new_missions_agg
  GROUP BY 1, 2
)

-- 6) Final output: CTRs + CTR lift and New Missions + New Missions lift
SELECT
  p.feed_tercile_label,
  p.search_decile_label,

  -- CTR metrics
  p.users_control,
  p.exposures_control,
  SAFE_DIVIDE(p.clicks_control, NULLIF(p.exposures_control, 0)) AS ctr_control,

  p.users_treatment,
  p.exposures_treatment,
  SAFE_DIVIDE(p.clicks_treatment, NULLIF(p.exposures_treatment, 0)) AS ctr_treatment,

  SAFE_DIVIDE(
    SAFE_DIVIDE(p.clicks_treatment, NULLIF(p.exposures_treatment, 0))
    - SAFE_DIVIDE(p.clicks_control,   NULLIF(p.exposures_control,   0)),
    NULLIF(SAFE_DIVIDE(p.clicks_control, NULLIF(p.exposures_control, 0)), 0)
  ) AS ctr_lift_pct,

  -- New missions metrics (per-user averages) and lifts
  nm.users_nm_control,
  SAFE_DIVIDE(nm.sum_nm_control, NULLIF(nm.users_nm_control, 0)) AS avg_new_missions_control,

  nm.users_nm_treatment,
  SAFE_DIVIDE(nm.sum_nm_treatment, NULLIF(nm.users_nm_treatment, 0)) AS avg_new_missions_treatment,

  SAFE_DIVIDE(
    SAFE_DIVIDE(nm.sum_nm_treatment, NULLIF(nm.users_nm_treatment, 0))
    - SAFE_DIVIDE(nm.sum_nm_control,   NULLIF(nm.users_nm_control,   0)),
    NULLIF(SAFE_DIVIDE(nm.sum_nm_control, NULLIF(nm.users_nm_control, 0)), 0)
  ) AS new_missions_lift_pct,
   SAFE_DIVIDE(s60.sum_searches_60d, NULLIF(s60.users_60d, 0)) AS avg_searches_60d_per_user

FROM ctr_pivoted p
LEFT JOIN new_missions_pivoted nm
  USING (feed_tercile_label, search_decile_label)
LEFT JOIN searches_60d_agg s60
  USING (feed_tercile_label, search_decile_label)
ORDER BY
  feed_tercile_label, 
  search_decile_label; 