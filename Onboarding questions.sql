---Q1---
-- What percent of visits have at least 1 recs module delivered? 
-- How does this differ by module, region, platform, and buyer segment?

SELECT
         recs_module_coverage_bq._date,
         buyer_segment,
         platform,

    COALESCE(SUM(recs_module_coverage_bq.visits_w_delivery ) 
    / NULLIF(SUM(recs_module_coverage_bq.total_segment_visits ), 0), 0) AS recs_module_coverage_bq_delivered_visit_coverage,

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