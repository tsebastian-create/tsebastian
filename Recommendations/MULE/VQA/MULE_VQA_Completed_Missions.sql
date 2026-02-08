
---------------------------------------- 
##  30 Day Lookback Recs Engagement Performance
---------------------------------------- 

DECLARE start_date DATE DEFAULT '2025-12-31';
DECLARE end_date   DATE DEFAULT '2026-01-29';
DECLARE lookback_window INT64 DEFAULT 30;

WITH
precomputed_taxonomy AS (
  SELECT
    listing_id,
    full_path,
    REGEXP_EXTRACT(full_path, r'^[^.]*') AS l1,
    REGEXP_EXTRACT(full_path, r'^[^.]*\.[^.]*') AS l2,
    REGEXP_EXTRACT(full_path, r'^[^.]*\.[^.]*\.[^.]*') AS l3
  FROM `etsy-data-warehouse-prod.materialized.listing_taxonomy`
),

recent_listings_purchased AS (
  SELECT DISTINCT
    rsb.buyer_user_id AS user_id,
    pt.l1,
    pt.l2,
    pt.l3,
    pt.full_path,
    rsb.shop_id,
    SAFE_CAST(listing_id_str AS INT64) AS listing_id
  FROM `etsy-data-warehouse-prod.rollups.receipt_shipping_basics` rsb
  CROSS JOIN UNNEST(SPLIT(REGEXP_REPLACE(full_listing_ids, r'[\[\]\s]', ''), ',')) AS listing_id_str
  LEFT JOIN precomputed_taxonomy pt
    ON pt.listing_id = SAFE_CAST(listing_id_str AS INT64)
  WHERE rsb.order_date > DATE_SUB(start_date, INTERVAL lookback_window DAY)
    AND rsb.order_date <= start_date
),

-- user feed next 7 days
user_feed_next_7_days AS (
  SELECT
  distinct
    case when rlp.listing_id is not null then 1 else 0 end as delivered_listing_recently_purchased,
    case when rlp_fp.listing_id is not null then 1 else 0 end as full_path_recently_purchased,
    case when rlp_same_shop.listing_id is not null then 1 else 0 end as same_shop_recently_purchased,
    rdl.*,
    rlp.full_path

    from `etsy-data-warehouse-prod.rollups.recsys_delivered_listings` rdl

    LEFT JOIN precomputed_taxonomy pt
    ON pt.listing_id = rdl.listing_id

    LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listings` l
    ON l.listing_id = rdl.listing_id


    join `etsy-data-warehouse-prod.weblog.visits` v
    on v.visit_id = rdl.visit_id
    and v._date > start_date
    AND v._date <= current_date()

    left join recent_listings_purchased rlp
    on rlp.user_id = v.user_id
    and rlp.listing_id = rdl.listing_id

    left join recent_listings_purchased rlp_fp
    on rlp_fp.user_id = v.user_id
    and rlp_fp.full_path = pt.full_path


    left join recent_listings_purchased rlp_same_shop
    on rlp_same_shop.user_id = v.user_id
    and rlp_same_shop.shop_id = l.shop_id


# recent 7 day window
     WHERE rdl._date > start_date
    AND rdl._date <= end_date
    and rdl.module_placement = 'boe_homescreen_feed'
        and rdl.buyer_segment = 'Habitual'

)
select 
delivered_listing_recently_purchased,
full_path_recently_purchased,
same_shop_recently_purchased,
count(*) as count_delivered_recs,
#sum(case when seen = 1 then purchased_after_view else 0 end) as count_purchased,
avg(seen) as avg_seen_rec,
avg(case when seen = 1 then clicked else null end) as avg_clicked_rec,

avg(case when seen = 1 then added_to_cart else null end) as avg_added_to_cart_rec,
avg(case when seen = 1 then favorited else null end) as avg_favorited_rec,
avg(case when seen = 1 then purchased_after_view else null end) as avg_purchased_rec,
from user_feed_next_7_days
group by 1,2,3
order by 1,2,3 ;

------------------------------------------------
## Listing Purchases Compared to Past 30D Purchases 
------------------------------------------------

DECLARE start_date DATE DEFAULT '2025-12-31';
DECLARE end_date   DATE DEFAULT '2026-01-29';
DECLARE lookback_window INT64 DEFAULT 30;

WITH
precomputed_taxonomy AS (
  SELECT
    listing_id,
    full_path,
    REGEXP_EXTRACT(full_path, r'^[^.]*') AS l1,
    REGEXP_EXTRACT(full_path, r'^[^.]*\.[^.]*') AS l2,
    REGEXP_EXTRACT(full_path, r'^[^.]*\.[^.]*\.[^.]*') AS l3
  FROM `etsy-data-warehouse-prod.materialized.listing_taxonomy`
),

recent_listings_purchased AS (
  SELECT DISTINCT
    rsb.buyer_user_id AS user_id,
    pt.l1,
    pt.l2,
    pt.l3,
    pt.full_path,
    rsb.shop_id,
    SAFE_CAST(listing_id_str AS INT64) AS listing_id
  FROM `etsy-data-warehouse-prod.rollups.receipt_shipping_basics` rsb
  CROSS JOIN UNNEST(SPLIT(REGEXP_REPLACE(full_listing_ids, r'[\[\]\s]', ''), ',')) AS listing_id_str
  LEFT JOIN precomputed_taxonomy pt
    ON pt.listing_id = SAFE_CAST(listing_id_str AS INT64)
  WHERE rsb.order_date > DATE_SUB(start_date, INTERVAL lookback_window DAY)
    AND rsb.order_date <= start_date
),

current_purchases_unnested AS (
  SELECT
    rsb.buyer_user_id,
    rsb.receipt_id,
    rsb.shop_id,
    rsb.order_date,
    SAFE_CAST(listing_id_str AS INT64) AS listing_id
  FROM `etsy-data-warehouse-prod.rollups.receipt_shipping_basics` rsb
  CROSS JOIN UNNEST(SPLIT(REGEXP_REPLACE(full_listing_ids, r'[\[\]\s]', ''), ',')) AS listing_id_str
  WHERE rsb.order_date > start_date
    AND rsb.order_date <= end_date
),

purchases AS (
  SELECT
    CASE WHEN rlp.listing_id IS NOT NULL THEN 1 ELSE 0 END AS purchased_listing_recently_purchased,
    CASE WHEN rlp_fp.listing_id IS NOT NULL THEN 1 ELSE 0 END AS purchased_full_path_recently_purchased,
    CASE WHEN rlp_same_shop.listing_id IS NOT NULL THEN 1 ELSE 0 END AS purchased_same_shop_recently_purchased,
    cpu.buyer_user_id,
    cpu.receipt_id
  FROM current_purchases_unnested cpu
  LEFT JOIN precomputed_taxonomy pt
    ON pt.listing_id = cpu.listing_id
  LEFT JOIN recent_listings_purchased rlp
    ON rlp.user_id = cpu.buyer_user_id
    AND rlp.listing_id = cpu.listing_id
  LEFT JOIN recent_listings_purchased rlp_fp
    ON rlp_fp.user_id = cpu.buyer_user_id
    AND rlp_fp.full_path = pt.full_path
  LEFT JOIN recent_listings_purchased rlp_same_shop
    ON rlp_same_shop.user_id = cpu.buyer_user_id
    AND rlp_same_shop.shop_id = cpu.shop_id
)

SELECT 
  purchased_listing_recently_purchased,
  purchased_full_path_recently_purchased,
  purchased_same_shop_recently_purchased,
  COUNT(distinct receipt_id) AS n_receipts
FROM purchases
GROUP BY 1, 2, 3
order by 1,2,3;

------------------------------------------------
## Taxonomy-View of Listing Purchases Compared to Past 30D Purchases
------------------------------------------------

DECLARE start_date DATE DEFAULT '2025-12-31';
DECLARE end_date   DATE DEFAULT '2026-01-29';
DECLARE lookback_window INT64 DEFAULT 30;

WITH
-- Only get taxonomy for listings we actually need
relevant_listings AS (
  SELECT DISTINCT SAFE_CAST(listing_id_str AS INT64) AS listing_id
  FROM `etsy-data-warehouse-prod.rollups.receipt_shipping_basics` rsb
  CROSS JOIN UNNEST(SPLIT(REGEXP_REPLACE(full_listing_ids, r'[\[\]\s]', ''), ',')) AS listing_id_str
  WHERE rsb.order_date > DATE_SUB(start_date, INTERVAL lookback_window DAY)
    AND rsb.order_date <= end_date
),

filled_taxonomy AS (
  SELECT
    rl.listing_id,
    REGEXP_EXTRACT(pt.full_path, r'^[^.]*') AS l1,
    COALESCE(
      REGEXP_EXTRACT(pt.full_path, r'^[^.]*\.[^.]*'), 
      REGEXP_EXTRACT(pt.full_path, r'^[^.]*')
    ) AS l2,
    COALESCE(
      REGEXP_EXTRACT(pt.full_path, r'^[^.]*\.[^.]*\.[^.]*'), 
      REGEXP_EXTRACT(pt.full_path, r'^[^.]*\.[^.]*'),
      REGEXP_EXTRACT(pt.full_path, r'^[^.]*')
    ) AS l3,
    COALESCE(
      REGEXP_EXTRACT(pt.full_path, r'^[^.]*\.[^.]*\.[^.]*\.[^.]*'),
      REGEXP_EXTRACT(pt.full_path, r'^[^.]*\.[^.]*\.[^.]*'), 
      REGEXP_EXTRACT(pt.full_path, r'^[^.]*\.[^.]*'),
      REGEXP_EXTRACT(pt.full_path, r'^[^.]*')
    ) AS l4
  FROM relevant_listings rl
  LEFT JOIN `etsy-data-warehouse-prod.materialized.listing_taxonomy` pt
    ON pt.listing_id = rl.listing_id
),

-- Combine current and recent purchases in one pass
all_purchases AS (
  SELECT
    rsb.buyer_user_id,
    rsb.receipt_id,
    rsb.order_date,
    SAFE_CAST(listing_id_str AS INT64) AS listing_id,
    ft.l1,
    ft.l2,
    ft.l3,
    ft.l4,
    CASE 
      WHEN rsb.order_date > start_date AND rsb.order_date <= end_date THEN 'current'
      WHEN rsb.order_date > DATE_SUB(start_date, INTERVAL lookback_window DAY) AND rsb.order_date <= start_date THEN 'recent'
    END AS purchase_type
  FROM `etsy-data-warehouse-prod.rollups.receipt_shipping_basics` rsb
  CROSS JOIN UNNEST(SPLIT(REGEXP_REPLACE(full_listing_ids, r'[\[\]\s]', ''), ',')) AS listing_id_str
  LEFT JOIN filled_taxonomy ft ON ft.listing_id = SAFE_CAST(listing_id_str AS INT64)
  WHERE rsb.order_date > DATE_SUB(start_date, INTERVAL lookback_window DAY)
    AND rsb.order_date <= end_date
),

-- Use window functions to check for matches efficiently
purchases_with_flags AS (
  SELECT
    curr.buyer_user_id,
    curr.receipt_id,
    curr.listing_id,
    curr.l1 AS curr_l1,
    curr.l2 AS curr_l2,
    curr.l3 AS curr_l3,
    curr.l4 AS curr_l4,
    -- Check if user had any recent activity
    CASE WHEN COUNT(CASE WHEN recent.purchase_type = 'recent' THEN 1 END) OVER (PARTITION BY curr.buyer_user_id) = 0 THEN 1 ELSE 0 END AS no_previous_activity,
    -- Check for exact listing match
    CASE WHEN COUNT(CASE WHEN recent.purchase_type = 'recent' AND recent.listing_id = curr.listing_id THEN 1 END) OVER (PARTITION BY curr.buyer_user_id, curr.listing_id) > 0 THEN 1 ELSE 0 END AS purchased_same_listing,
    -- Check for L4 match (different listing)
    CASE WHEN COUNT(CASE WHEN recent.purchase_type = 'recent' AND recent.l4 = curr.l4 AND recent.listing_id != curr.listing_id THEN 1 END) OVER (PARTITION BY curr.buyer_user_id, curr.l4) > 0 THEN 1 ELSE 0 END AS purchased_same_l4,
    -- Check for L3 match (different L4)
    CASE WHEN COUNT(CASE WHEN recent.purchase_type = 'recent' AND recent.l3 = curr.l3 AND recent.l4 != curr.l4 THEN 1 END) OVER (PARTITION BY curr.buyer_user_id, curr.l3) > 0 THEN 1 ELSE 0 END AS purchased_same_l3,
    -- Check for L2 match (different L3)
    CASE WHEN COUNT(CASE WHEN recent.purchase_type = 'recent' AND recent.l2 = curr.l2 AND recent.l3 != curr.l3 THEN 1 END) OVER (PARTITION BY curr.buyer_user_id, curr.l2) > 0 THEN 1 ELSE 0 END AS purchased_same_l2,
    -- Check for L1 match (different L2)
    CASE WHEN COUNT(CASE WHEN recent.purchase_type = 'recent' AND recent.l1 = curr.l1 AND recent.l2 != curr.l2 THEN 1 END) OVER (PARTITION BY curr.buyer_user_id, curr.l1) > 0 THEN 1 ELSE 0 END AS purchased_same_l1
  FROM all_purchases curr
  LEFT JOIN all_purchases recent ON recent.buyer_user_id = curr.buyer_user_id
  WHERE curr.purchase_type = 'current'
)

SELECT 
  purchased_same_listing,
  purchased_same_l4,
  purchased_same_l3,
  purchased_same_l2,
  purchased_same_l1,
  no_previous_activity,
  COUNT(DISTINCT receipt_id) AS n_receipts
FROM purchases_with_flags
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY 1, 2, 3, 4, 5, 6;