WITH test AS (
SELECT raw.*,
dd.hf_running_week, -- as sendout_hf_running_week
dd.hf_week,         -- as sendout_hf_week
ROW_NUMBER() OVER(PARTITION BY test, country, customer_id ORDER BY dd.sk_date ASC) AS order_of_communication_points
FROM uploads.outbound_welcome_abtesting AS raw
LEFT JOIN dimensions.date_dimension AS dd
ON sent_date = date_string
WHERE test = 'Mealchoice Education'
)

-- per individual customer, gives the first communication of a certain campaign
, earliest_sent_date_per_customer AS (
SELECT test, country, locale, customer_id, segment, medium
, sent_date AS earliest_sent_date
, hf_running_week AS earliest_sent_hf_running_week
, hf_week AS earliest_sent_hf_week
, 1 as index
FROM test
WHERE order_of_communication_points = 1
)

, current_week AS (
SELECT DISTINCT
hf_running_week AS current_running_hf_week
, 1 AS index
FROM dimensions.date_dimension
WHERE date_string_backwards = from_unixtime(unix_timestamp(now()), 'yyyy-MM-dd')
)

, boxes_shipped AS (
SELECT
  bs.revenue_excluding_vat_local_currency
, bs.voucher_discount_amount_local_currency
, bs.hf_delivery_week
, bs.country
, cd.customer_id
, dd.hf_running_week             -- as delivery_hf_running_week
FROM fact_tables.boxes_shipped AS bs
LEFT JOIN dimensions.customer_dimension AS cd
ON bs.fk_customer = cd.sk_customer
LEFT JOIN dimensions.date_dimension AS dd
ON dd.sk_date = bs.fk_delivery_date
WHERE bs.country NOT IN ('ML', 'US') AND bs.hf_delivery_week >= '2018-W25'
)

, orders AS (
SELECT earliest.test
, boxes.country
, boxes.customer_id
, ISNULL(SUM (boxes.revenue_excluding_vat_local_currency), 0) AS 10w_revenue_excluding_vat_local_currency
, ISNULL(SUM (boxes.voucher_discount_amount_local_currency), 0) AS 10w_voucher_discount_amount_local_currency
, COUNT (DISTINCT hf_delivery_week) AS 10w_deliveries
-- , CASE WHEN COUNT (DISTINCT hf_delivery_week) >= 5 THEN 1 ELSE 0 END AS 5in10
-- no 5in10 here because we don't start counting from the first delivery but from the moment they receive the first communication
-- column with names of vouchers used within those 10 weeks concatenated
FROM earliest_sent_date_per_customer AS earliest
LEFT JOIN boxes_shipped AS boxes
ON boxes.country = earliest.country AND boxes.customer_id = earliest.customer_id
LEFT JOIN current_week
ON current_week.index = earliest.index
WHERE hf_running_week BETWEEN earliest_sent_hf_running_week + 1 AND earliest_sent_hf_running_week + 9
-- 10 weeks of boxes shipped
AND earliest_sent_hf_running_week + 9 <= current_running_hf_week
-- only customers who have had the chance to reach 10 weeks of boxes
GROUP BY 1, 2, 3
)

, most_active_sub_status AS (
SELECT cd.customer_id, cd.country
, dd.hf_running_week
, MIN(CASE ss.status
               WHEN 'canceled' THEN 3
               WHEN 'user_paused' THEN 2
               WHEN 'active' THEN 1
               WHEN 'interval_paused' THEN 2
               WHEN 'unknown' THEN 4
               ELSE 4 -- else is null
               END)   AS most_active_subscription
FROM fact_tables.subscription_statuses AS ss
   LEFT JOIN dimensions.customer_dimension AS cd
      ON ss.fk_customer = cd.sk_customer
   LEFT JOIN (
               SELECT DISTINCT hf_running_week
                 , hf_week
               FROM dimensions.date_dimension
             ) AS dd
      ON dd.hf_week = ss.hf_week
WHERE ss.country NOT IN ('ML', 'US') AND ss.hf_week >= '2018-W25'
GROUP BY 1,2,3
)
-- one row per country per customer id per week

, cancellations AS (
SELECT
earliest.test
, earliest.country
, earliest.customer_id
, ISNULL(SUM(CASE WHEN mass.most_active_subscription = 3 THEN 1 ELSE 0 END), 0) AS 10w_cancelled_weeks
FROM earliest_sent_date_per_customer AS earliest
LEFT JOIN most_active_sub_status AS mass
ON mass.country = earliest.country AND mass.customer_id = earliest.customer_id
LEFT JOIN current_week
ON current_week.index = earliest.index
WHERE hf_running_week BETWEEN earliest_sent_hf_running_week + 1 AND earliest_sent_hf_running_week + 9
-- 10 weeks of boxes shipped
AND earliest_sent_hf_running_week + 9 <= current_running_hf_week
-- only customers who have had the chance to reach 10 weeks of boxes
GROUP BY 1, 2, 3
)

-- cancellations 1 row per customer id per country per test ok
SELECT DISTINCT one_row_customer.test
							, one_row_customer.country
							, one_row_customer.locale
							, one_row_customer.customer_id
							, one_row_customer.segment
							, one_row_customer.earliest_sent_hf_week
							, one_row_customer.earliest_sent_date
                                                        , nps.score AS nps_score
                                                        , ns.channel
							, one_row_customer.earliest_sent_hf_running_week
							, ISNULL(10w_revenue_excluding_vat_local_currency, 0)   AS 10w_revenue_excluding_vat_local_currency
							, ISNULL(10w_voucher_discount_amount_local_currency, 0) AS 10w_voucher_discount_amount_local_currency
							, ISNULL(10w_deliveries, 0)                             AS 10w_deliveries
							, ISNULL(10w_cancelled_weeks, 0)                        AS 10w_cancelled_weeks
FROM earliest_sent_date_per_customer AS one_row_customer
		 LEFT JOIN orders AS boxes
				 ON boxes.customer_id = one_row_customer.customer_id
				  AND boxes.country = one_row_customer.country
					AND boxes.test = one_row_customer.test
		 LEFT JOIN cancellations AS cancellations
		     ON cancellations.customer_id = one_row_customer.customer_id
		      AND cancellations.country = one_row_customer.country
		      AND cancellations.test = one_row_customer.test
                 LEFT JOIN views_analysts.nps_all AS nps
                        ON nps.country = one_row_customer.country
                       AND nps.customer_id = one_row_customer.customer_id
                 LEFT JOIN fact_tables.new_subscriptions ns
                        ON ns.country = one_row_customer.country
                       AND ns.customer_id = one_row_customer.customer_id