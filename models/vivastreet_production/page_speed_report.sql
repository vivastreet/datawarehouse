    with speed_metrics as (
  select
    date,
    (select cd.value from unnest(hits.customDimensions) as cd where index = 2) as country,
    device.deviceCategory,
    device.operatingSystem,
    device.browser,
    if(totals.visits = 1, concat(fullVisitorId, visitId, date), null) as session_id,
    hits.page.pagePath,
    hits.type,
    hits.latencyTracking.pageLoadSample, 
    hits.latencyTracking.pageLoadTime,
    hits.latencyTracking.domInteractiveTime,
    hits.latencyTracking.domContentLoadedTime
  from `massive-period-517.78355533.ga_sessions_*`, unnest(hits) as hits
  where _table_suffix between '20220712' and '20220712'
  -- from `massive-period-517.78355533.ga_sessions_20*`, unnest(hits) as hits_
  -- where (parse_date('%y%m%d', _table_suffix) between date_sub(current_date(), interval 1 day) and date_sub(current_date(), interval 1 day)) 
  )

select 
  country, 
  date,
  deviceCategory,
  operatingSystem,
  browser,
  pagePath,
  count(if(type = 'PAGE', type, null)) as total_pageviews,
  count(distinct session_id) as total_sessions,
  round(count(if(type = 'PAGE', type, null))/count(distinct session_id), 2) as avg_pageviews_per_session,
  sum(pageLoadSample) as speed_metrics_page_load_sample,
  round(avg(if(pageLoadSample is not null, pageLoadTime, null))/1000, 2) as avg_page_load_time_sec,
  round(avg(if(pageLoadSample is not null, domInteractiveTime, null))/1000, 2) as avg_dom_interactive_time_sec,
  round(avg(if(pageLoadSample is not null, domContentLoadedTime, null))/1000, 2) as avg_dom_content_loaded_time_sec,
from speed_metrics
where country in ('UK', 'BR', 'FR', 'BE')
group by country, date, pagePath, deviceCategory, operatingSystem, browser
having total_sessions != 0
order by country desc, date, total_pageviews desc