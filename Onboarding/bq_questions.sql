

---------------- Q1 What percent of visits have at least 1 recs module delivered? How does this differ by module, region, platform, and buyer segment?

SELECT
         recs_module_coverage_bq._date,
         buyer_segment,
         platform,

    COALESCE(SUM(recs_module_coverage_bq.visits_w_delivery ) / NULLIF(SUM(recs_module_coverage_bq.total_segment_visits ), 0), 0) AS recs_module_coverage_bq_delivered_visit_coverage,

SUM(recs_module_coverage_bq.total_segment_visits ),

FROM `etsy-data-warehouse-prod.rollups.recs_module_coverage`  AS recs_module_coverage_bq

WHERE  recs_module_coverage_bq._date  = CURRENT_DATE('UTC') -5
 
  AND (recs_module_coverage_bq.module_placement ) = 'Any'
   AND (recs_module_coverage_bq.module_page ) = 'Any'
    AND (recs_module_coverage_bq.ranker ) = 'Any'
GROUP BY
    ALL
ORDER BY
    1,2,3;


--Q2 On average, how many recs listings get delivered per visit with at least 1 module delivery? How does this differ by module, region, platform, and buyer segment?

SELECT
  t0.module_placement,
  t0.platform,
  t0.buyer_segment,
  AVG(t0.total_delivered_listings) AS average_delivered_listings_per_visit
FROM `etsy-data-warehouse-prod.rollups.recs_visit_metrics` AS t0
WHERE t0.total_delivered_modules >= 1 and _date = current_date() -1
  GROUP BY t0.module_placement, t0.platform, t0.buyer_segment
  ORDER BY t0.module_placement, t0.platform, t0.buyer_segment;
## join with visit mart

--q3 What percent of delivered modules are seen by the user? How does this differ by module, page, and platform?
SELECT
  module_placement,
  module_page,
  platform,
  SUM(seen_module) AS total_seen_modules,
  SUM(total_delivered_modules) AS total_delivered_modules,
  SAFE_DIVIDE(SUM(seen_module), SUM(total_delivered_modules)) * 100 AS percent_modules_seen
FROM
  `etsy-data-warehouse-prod.rollups.recs_visit_metrics`
WHERE
   _date = current_date() -1 
GROUP BY
  module_placement,
  module_page,
  platform
ORDER BY
  percent_modules_seen DESC;

--q4 -- What is the aggregate click-through rate of recommendation modules? How does this differ by module, region, platform, and buyer segment?
SELECT
  module_placement,
  top_channel,
  platform,
  buyer_segment,
  SUM(visits_w_click) AS total_visits_with_click,
  SUM(visits_w_seen) AS total_visits_with_seen,
  SAFE_DIVIDE(SUM(visits_w_click), SUM(visits_w_seen)) AS click_through_rate
FROM
  `etsy-data-warehouse-prod.rollups.recs_module_metrics`
WHERE
  _date = current_date() -1 
  --where xxx = 'any'
GROUP BY
  module_placement,
  top_channel,
  platform,
  buyer_segment
ORDER BY
  click_through_rate DESC;

-- `etsy-data-warehouse-prod.rollups.recs_delivered_listings`
-- `etsy-data-warehouse-prod.rollups.recs_visit_metrics` (rolled up to visit ID level  )
-- `etsy-data-warehouse-prod.rollups.recs_module_metrics` (rolled up least granular) )

--q5 -- What is the aggregate click-through rate of recommendation modules? How does this differ by module, region, platform, and buyer segment?
