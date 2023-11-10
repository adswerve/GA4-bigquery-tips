DECLARE start_date STRING DEFAULT "20230723";
DECLARE end_date STRING DEFAULT "20230723";

#### OBSERVATIONS #### 
#Entrances = 1 is unreliable, sometimes it shows up, sometimes it doesn't
#Session_start has no sessions traffic source information
#Going with the first non-null traffic source of the session could be wrong (since it could have started as Direct)

#### CONFIRM ####
#What stream and events are exported in the BigQuery Linking in Admin

#Grab the first non-systemic (session_start, first_visit) event and use the medium as session medium
#If the medium is null, I find the closest non-null preceding session and use that as the session medium
  
#With this method I see an average absolute error of around 3%, more for some media (Direct and organic)



WITH currentSessions AS 
  #Get the medium of the sessions that are currently provided
(SELECT DISTINCT user_pseudo_id, ga_session_id, IF(gclid is not null, "cpc", medium) medium, src FROM 
    (SELECT  user_pseudo_id,  
      (SELECT value.int_value FROM unnest(event_params) WHERE key = 'ga_session_id') as ga_session_id,  
      collected_traffic_source.manual_medium as medium,
      collected_traffic_source.manual_source as src,
      collected_traffic_source.gclid as gclid
      FROM `analytics_1234.events_*` WHERE event_name= "session_start" AND _table_suffix BETWEEN start_date AND end_date) 
),

precedingSessions AS 
(SELECT DISTINCT user_pseudo_id, old_ga_session_id, IF(gclid is not null, "cpc", old_medium) old_medium, old_src  FROM 
    (SELECT  user_pseudo_id,  
      (SELECT value.int_value FROM unnest(event_params) WHERE key = 'ga_session_id') as old_ga_session_id,  
      first_value(collected_traffic_source.manual_medium  IGNORE NULLS)  OVER (PARTITION BY user_pseudo_id, (SELECT value.int_value FROM unnest(event_params) WHERE key = 'ga_session_id') ORDER BY event_timestamp DESC) as old_medium,
      first_value(collected_traffic_source.manual_source  IGNORE NULLS)  OVER (PARTITION BY user_pseudo_id, (SELECT value.int_value FROM unnest(event_params) WHERE key = 'ga_session_id') ORDER BY event_timestamp DESC) as old_src,
      first_value(collected_traffic_source.gclid)  OVER (PARTITION BY user_pseudo_id, (SELECT value.int_value FROM unnest(event_params) WHERE key = 'ga_session_id') ORDER BY event_timestamp) as gclid
      FROM `analytics_1234.events_*` WHERE event_name NOT IN ("session_start", "first_visit") AND _table_suffix BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(PARSE_DATE("%Y%m%d", start_date), INTERVAL 90 DAY)) AND end_date)
),

lastNonDirect AS (
  SELECT  user_pseudo_id, ga_session_id, if(medium is null, (SELECT old_medium FROM unnest(old_sessions) WHERE old_medium is not null ORDER BY old_ga_session_id DESC LIMIT 1 ) ,medium) as medium
  , IF(src is null, (SELECT old_src FROM unnest(old_sessions) WHERE old_src is not null ORDER BY old_ga_session_id DESC LIMIT 1 ) ,src) as src
   FROM (
    SELECT user_pseudo_id, ga_session_id, medium, src, ARRAY_AGG(struct(old_ga_session_id, old_medium, old_src)) as old_sessions FROM 
      currentSessions 
      LEFT OUTER JOIN
      precedingSessions
      USING(user_pseudo_id)
      WHERE old_ga_session_id <= ga_session_id
      GROUP BY 1,2,3,4)
)

SELECT medium, count(distinct concat( user_pseudo_id, " ", ga_session_id)) as sessions 
FROM lastNonDirect
GROUP BY 1 
ORDER BY 2 DESC
