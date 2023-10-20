CREATE OR REPLACE TABLE FUNCTION analytics_1234567.ga4_lookback_window(lookback_window_days INT64, conversion_event STRING)

AS

WITH converters as (
  SELECT user_pseudo_id, event_timestamp, ecommerce.purchase_revenue, ecommerce.transaction_id FROM `analytics_1234567.events_20231018` 
  WHERE event_name = conversion_event),

user_pageviews as (
SELECT user_pseudo_id, 
      event_timestamp, 
      (SELECT value.string_value FROM unnest(event_params) WHERE key = "page_location") as first_page_location
FROM `analytics_1234567.events_202310*` 
WHERE event_name = "page_view")


SELECT user_pseudo_id,
      purchase_revenue, 
    TIMESTAMP_MICROS(converters.event_timestamp) as purchase_time, 
    TIMESTAMP_MICROS(user_pageviews.event_timestamp) as first_page_time,  first_page_location FROM 
  converters
  LEFT OUTER JOIN
  user_pageviews
  USING(user_pseudo_id)
  WHERE (converters.event_timestamp-user_pageviews.event_timestamp)/(1000000*60*60*24) BETWEEN 0 AND lookback_window_days
  QUALIFY ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY user_pageviews.event_timestamp) = 1


  -- You can now trigger the table function with the following command
  -- (14 day lookback window and conversion event = "purchase")
  -- SELECT * FROM `analytics1234567.ga4_lookback_window`(14, "purchase");
