WITH ao_and_budgets as (
SELECT 
     DATE(ao.Date) as date,
     ao.country,
    EXTRACT(YEAR FROM ao.date) AS year,
    CONCAT(CAST(EXTRACT(YEAR FROM ao.Date) AS STRING), LPAD(CAST(EXTRACT(MONTH FROM ao.Date) AS STRING), 2, '0')) AS yearmonth,
    FORMAT_DATE('%B', ao.Date) AS month,
    EXTRACT(ISOWEEK FROM ao.Date) AS iso_week,
    EXTRACT(DAY FROM ao.Date) AS day,
    COUNT(DISTINCT Classified_ID) AS ads_live,
    COUNT(DISTINCT Order_ID) AS NB_order_id,
    Sum(Total*er.currency_rate) as gross_rev,
    SUM(IF(ao.country in ('GB', 'FR', 'BE', 'IT', 'PT', 'ES', 'IE'), total_net*er.currency_rate, ao.total*er.currency_rate)) as net_rev,
    (SUM(IF(ao.country in ('GB', 'FR', 'BE', 'IT', 'PT', 'ES', 'IE'), total_net*er.currency_rate, ao.total*er.currency_rate))) / COUNT(Order_ID) as AOV,
    MAX(IF(bt.type = 'budget', Budget_ex_VAT, 0)) budget_net,
    MAX(IF(bt.type = 'budget', Daily_run_rate_revenue_, 0)) Daily_run_rate_revenue_budget,
    MAX(IF(bt.type = 'budget', Daily_Run_Rate__orders, 0)) Daily_run_rate_orders_budget,
    MAX(IF(bt.type = 'budget', AOV, 0)) aov_budget,
    MAX(IF(bt.type = 'target', Budget_ex_VAT, 0)) target_net,
    MAX(IF(bt.type = 'target', Daily_run_rate_revenue_, 0)) Daily_run_rate_revenue_target,
    MAX(IF(bt.type = 'target', Daily_Run_Rate__orders, 0)) Daily_run_rate_orders_target,
    MAX(IF(bt.type = 'target', AOV, 0)) aov_target,
    MAX(IF(bt.type = 'LYbudget', Budget_ex_VAT, 0)) LYbudget_net,
    MAX(IF(bt.type = 'LYbudget', Daily_run_rate_revenue_, 0)) Daily_run_rate_revenue_LYbudget,
    MAX(IF(bt.type = 'LYbudget', Daily_Run_Rate__orders, 0)) Daily_run_rate_orders_LYbudget,
    MAX(IF(bt.type = 'LYbudget', AOV, 0)) aov_LYbudget
FROM `data-warehouse-326816.vivastreet_production.ads_orders_report` ao
LEFT JOIN `data-warehouse-326816.vivastreet_production.budgets_and_targets` bt
ON (lower(bt.Country) = LOWER(ao.country) and DATE_TRUNC(DATE(ao.date), MONTH) = DATE_TRUNC(DATE(bt.date),MONTH) and bt.type in ('budget','target','LYbudget'))
LEFT JOIN `data-warehouse-326816.vivastreet_production.exchange_rates` er
ON er.country = ao.country and
DATE_TRUNC(DATE(ao.date), MONTH) = DATE_TRUNC(DATE(er.date), MONTH)
WHERE DATE(ao.Date) > '2019-01-01' 
GROUP BY date, year, yearmonth, month, iso_week, day,country
order by Day asc
)

SELECT  
 *,
 round((previous_year_revenue-net_rev)/net_rev, 2) previous_rev_change,
 round((previous_year_revenue-MTD)/net_rev, 2) budget_rev_change
FROM (
SELECT 
    SUM(net_rev) OVER (Partition by yearmonth, year, country ORDER BY date) as RTD , 
    (SUM(net_rev) OVER (Partition by yearmonth, year, country ORDER BY date))/day as MTD,
    (SELECT ANY_VALUE(net_rev) from ao_and_budgets ao2 WHERE ao.country = ao2.country and ao2.date = DATE_SUB(ao.date, INTERVAL 1 Year)) previous_year_revenue,
    DATE_SUB(date, INTERVAL 1 YEAR) previous_year, 
    IF(budget_net > net_rev, 1, 0) budget_quick_positive,
    IF(budget_net > net_rev, 0, 1) budget_quick_negative
    ,*
    FROM ao_and_budgets ao
)
WHERE country = 'gb' and date = '2022-05-06'
