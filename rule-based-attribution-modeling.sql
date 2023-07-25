WITH conversions AS 
  ( 
    SELECT 
      user_pseudo_id, 
      (SELECT value.int_value FROM unnest(event_params) WHERE key = "ga_session_id") as ga_session_id,
      timestamp_micros(min(event_timestamp)) as session_start_timestamp,
      sum(ecommerce.purchase_revenue) as revenue,
      max(ecommerce.transaction_id) as transaction_id
    FROM `analytics_1234.events_*` 
    WHERE event_name = "purchase" AND _table_suffix BETWEEN "20230710" AND "20230716"
    GROUP BY 1,2
  ),

   currentSessions 
AS 
(SELECT user_pseudo_id, ga_session_id, IF(gclid is not null, "cpc", medium) medium, min(session_start_timestamp) session_start_timestamp FROM 
    (SELECT  user_pseudo_id,  
      (SELECT value.int_value FROM unnest(event_params) WHERE key = 'ga_session_id') as ga_session_id,  
      first_value(collected_traffic_source.manual_medium)  OVER (PARTITION BY user_pseudo_id, (SELECT value.int_value FROM unnest(event_params) WHERE key = 'ga_session_id') ORDER BY event_timestamp) as medium,
      first_value(collected_traffic_source.gclid)  OVER (PARTITION BY user_pseudo_id, (SELECT value.int_value FROM unnest(event_params) WHERE key = 'ga_session_id') ORDER BY event_timestamp) as gclid,
      timestamp_micros(event_timestamp) as session_start_timestamp
      FROM `analytics_1234.events_*` WHERE event_name NOT IN ("session_start", "first_visit") AND _table_suffix BETWEEN "20230610" AND "20230716") 
      GROUP BY 1,2,3
),

precedingSessions AS 
(SELECT DISTINCT user_pseudo_id, old_ga_session_id, IF(gclid is not null, "cpc", old_medium) old_medium  FROM 
    (SELECT  user_pseudo_id,  
      (SELECT value.int_value FROM unnest(event_params) WHERE key = 'ga_session_id') as old_ga_session_id,  
      first_value(collected_traffic_source.manual_medium)  OVER (PARTITION BY user_pseudo_id, (SELECT value.int_value FROM unnest(event_params) WHERE key = 'ga_session_id') ORDER BY event_timestamp) as old_medium,
      first_value(collected_traffic_source.gclid)  OVER (PARTITION BY user_pseudo_id, (SELECT value.int_value FROM unnest(event_params) WHERE key = 'ga_session_id') ORDER BY event_timestamp) as gclid
      FROM `analytics_1234.events_*` WHERE event_name NOT IN ("session_start", "first_visit") AND _table_suffix BETWEEN "20230510" AND "20230723")
),

interactions AS (
  SELECT  user_pseudo_id, ga_session_id, if(medium is null, (SELECT old_medium FROM unnest(old_sessions) WHERE old_medium is not null ORDER BY old_ga_session_id DESC LIMIT 1 ) ,medium) as medium,session_start_timestamp FROM (
    SELECT user_pseudo_id, ga_session_id, medium, session_start_timestamp, ARRAY_AGG(struct(old_ga_session_id, old_medium)) as old_sessions FROM 
      currentSessions 
      LEFT OUTER JOIN
      precedingSessions
      USING(user_pseudo_id)
      WHERE old_ga_session_id <= ga_session_id
      GROUP BY 1,2,3,4)
),



  base AS (
    SELECT interactions,  conversions.session_start_timestamp as conversion_timestamp, conversions.revenue, conversions.transaction_id,
      count(*) OVER (PARTITION BY conversions.transaction_id) as totalInteractions,
      ROW_NUMBER() OVER (PARTITION BY conversions.transaction_id ORDER BY interactions.session_start_timestamp) interactionNumber,
      ROW_NUMBER() OVER (PARTITION BY  conversions.transaction_id  ORDER BY interactions.session_start_timestamp DESC) interactionNumber_DESC,
     FROM 
      conversions
      LEFT OUTER JOIN
      interactions
      USING (user_pseudo_id)
    WHERE interactions.session_start_timestamp <= conversions.session_start_timestamp AND interactions.session_start_timestamp > TIMESTAMP_SUB(conversions.session_start_timestamp, INTERVAL 30 DAY)
  )

  SELECT * FROM base
