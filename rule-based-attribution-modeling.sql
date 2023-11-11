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
      collected_traffic_source.manual_medium as medium,
      collected_traffic_source.gclid as gclid,
      timestamp_micros(event_timestamp) as session_start_timestamp
      FROM `analytics_1234.events_*` WHERE event_name = "session_start" AND _table_suffix BETWEEN "20231105" AND "20231110") 
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
  ),
  
decayAttr AS (
SELECT interactions.medium, sum(revenue*decayShare) as revenueDecay, sum(1*decayShare) as conversionsDecay  FROM (
  SELECT 
    *, decayShare_PN/sum(decayShare_PN) OVER (PARTITION BY transaction_id) decayShare
  FROM 
  (SELECT *,
    POW(0.5, (TIMESTAMP_DIFF(conversion_timestamp, interactions.session_start_timestamp, MINUTE)/(7*24*60))) decayShare_PN
    FROM  base)

)
GROUP BY 1), 

positionBasedAttr AS (

SELECT 
  interactions.medium, sum(revenue*positionShare) as revenuePosition, sum(1*positionShare) as conversionsPosition 
FROM 
 (SELECT *,
 CASE
    WHEN totalInteractions = 1 THEN 1
    WHEN totalInteractions = 2 THEN 0.5
    WHEN interactionNumber = 1 THEN 0.4
    WHEN interactionNumber_Desc = 1 THEN 0.4
    ELSE
      0.2/(totalInteractions-2)
 END as positionShare
  FROM  base)

GROUP BY 1), 

linearAttr AS (
  SELECT 
    interactions.medium, 
    sum(revenue/totalInteractions) as revenueLinear, 
    sum(1/totalInteractions) as conversionsLinear
  FROM 
    base

  GROUP BY 1
),

lastTouchAttr AS (

SELECT 
  interactions.medium, sum(revenue) as revenueLastTouch, count(distinct transaction_id) as conversionsLastTouch
FROM base 
WHERE interactionNumber_DESC=1
GROUP BY 1
),

firstTouchAttr AS (
(SELECT 
  interactions.medium, sum(revenue) as revenueFirstTouch, count(distinct transaction_id) as conversionsFirstTouch
FROM base 
WHERE interactionNumber=1
GROUP BY 1)
)

SELECT * FROM 
  firstTouchAttr
  JOIN 
  lastTouchAttr
  using(medium)
  JOIN
  linearAttr
  using(medium)
  JOIN
  positionBasedAttr
  using(medium)
  JOIN
  decayAttr
  using(medium)
