WITH ao_and_budgets as (
    SELECT 
        DATE(ao.Date) as date,
        ao.country,
        EXTRACT(YEAR FROM ao.date) AS year,
        CONCAT(CAST(EXTRACT(YEAR FROM ao.Date) AS STRING), LPAD(CAST(EXTRACT(MONTH FROM ao.Date) AS STRING), 2, '0')) AS yearmonth,
        FORMAT_DATE('%B', ao.Date) AS month,
        EXTRACT(ISOWEEK FROM ao.Date) AS iso_week,
        EXTRACT(DAY FROM ao.Date) AS day,
        COUNT(DISTINCT Order_ID) AS NB_order_id,
        Sum(Total*er.currency_rate) as gross_rev,
        SUM(IF(ao.country in ('gb', 'fr', 'be', 'it', 'pt', 'es', 'ie'), total_net*er.currency_rate, ao.total*er.currency_rate)) as net_rev,
        SUM(IF(ao.country in ('gb', 'fr', 'be', 'it', 'pt', 'es', 'ie'), total_net, ao.total)) as total_net_local,
        SUM(ao.total) as total_gross_local,
        SUM(ao.total) as total_gross_GBP,
        (SUM(IF(ao.country in ('gb', 'fr', 'be', 'it', 'pt', 'es', 'ie'), total_net*er.currency_rate, ao.total*er.currency_rate))) / COUNT(Order_ID) as AOV,
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
        MAX(IF(bt.type = 'LYbudget', AOV, 0)) aov_LYbudget,
        ANY_VALUE(er.currency_rate) currency_rate
    FROM `data-warehouse-326816.vivastreet_production.ads_orders_report` ao
    LEFT JOIN `data-warehouse-326816.vivastreet_production.budgets_and_targets` bt
    ON (lower(bt.Country) = LOWER(ao.country) and DATE_TRUNC(DATE(ao.date), MONTH) = DATE_TRUNC(DATE(bt.date),MONTH) and bt.type in ('budget','target','LYbudget'))
    LEFT JOIN `data-warehouse-326816.vivastreet_production.exchange_rates` er
    ON er.country = ao.country and
    DATE_TRUNC(DATE(ao.date), MONTH) = DATE_TRUNC(DATE(er.date), MONTH)
    WHERE DATE(ao.Date) > '2019-01-01' 
    GROUP BY date, year, yearmonth, month, iso_week, day,country
    order by Day asc
),

im_daily as (
SELECT 
*,
round(SAFE_DIVIDE(revenue_run_rate-previous_year_revenue,previous_year_revenue), 2) previous_rev_change,
round(SAFE_DIVIDE(revenue_run_rate-budget_net,budget_net), 2) budget_rev_change,
IF(net_rev >= budget_net, 1, 0) budget_quick_positive,
IF(budget_net > net_rev, -1, 0) budget_quick_negative
FROM
(
    SELECT 
    date,
    IF(country not in ('gb','fr','be','br'), 'ROW', country) country,
    year,
    yearmonth,
    month,
    iso_week,
    day,
    SUM(NB_order_id) as NB_order_id,
    SUM(gross_rev) as gross_rev,
    SUM(net_rev) as net_rev,
    AVG(AOV) as AOV,
    SUM(budget_net) as budget_net,
    SUM(Daily_run_rate_revenue_budget) as Daily_run_rate_revenue_budget,
    SUM(Daily_run_rate_orders_budget) as Daily_run_rate_orders_budget,
    SUM(aov_budget) as aov_budget,
    SUM(target_net) as target_net,
    SUM(Daily_run_rate_revenue_target) as Daily_run_rate_revenue_target,
    SUM(Daily_run_rate_orders_target) as Daily_run_rate_orders_target,
    SUM(aov_target) as aov_target,
    SUM(LYbudget_net) as LYbudget_net,
    SUM(Daily_run_rate_revenue_LYbudget) as Daily_run_rate_revenue_LYbudget,
    SUM(Daily_run_rate_orders_LYbudget) as Daily_run_rate_orders_LYbudget,
    SUM(aov_LYbudget) as aov_LYbudget,
    SUM(RTD_NET) RTD,
    SUM(MTD_NET) MTD,
    SUM(RTD_GROSS_LOCAL) RTD_GROSS_LOCAL,
    SUM(MTD_GROSS_LOCAL) MTD_GROSS_LOCAL,
    SUM(RTD_GROSS_GBP) RTD_GROSS_GBP,
    SUM(MTD_GROSS_GBP) MTD_GROSS_GBP,
    SUM(previous_year_revenue) previous_year_revenue,
    AVG(previous_year_AOV)previous_year_AOV,
    SUM(previous_year_total_orders) previous_year_orders,
    (SUM(RTD_NET)/SUM(day))*SUM(EXTRACT(DAY FROM LAST_DAY(date))) revenue_run_rate,
    (SUM(RTD_GROSS_GBP)/SUM(day))*SUM(EXTRACT(DAY FROM LAST_DAY(date))) revenue_run_rate_gross,
    (SUM(RTD_NET_LOCAL)/SUM(day))*SUM(EXTRACT(DAY FROM LAST_DAY(date))) revenue_run_rate_local,
    (SUM(RTD_GROSS_LOCAL)/SUM(day))*SUM(EXTRACT(DAY FROM LAST_DAY(date))) revenue_run_rate_gross,
    FROM (
    SELECT 
        SUM(net_rev) OVER (Partition by yearmonth, year, country ORDER BY date) as RTD_NET , 
        (SUM(net_rev) OVER (Partition by yearmonth, year, country ORDER BY date))/day as MTD_NET,
        SUM(total_net_local) OVER (Partition by yearmonth, year, country ORDER BY date) as RTD_NET_LOCAL , 
        (SUM(total_net_local) OVER (Partition by yearmonth, year, country ORDER BY date))/day as MTD_NET_LOCAL,
        SUM(total_gross_GBP) OVER (Partition by yearmonth, year, country ORDER BY date) as RTD_GROSS_GBP, 
        (SUM(total_gross_GBP) OVER (Partition by yearmonth, year, country ORDER BY date))/day as MTD_GROSS_GBP,
        SUM(total_gross_LOCAL) OVER (Partition by yearmonth, year, country ORDER BY date) as RTD_GROSS_LOCAL, 
        (SUM(total_gross_LOCAL) OVER (Partition by yearmonth, year, country ORDER BY date))/day as MTD_GROSS_LOCAL,
        (SELECT SUM(net_rev) from ao_and_budgets ao2 WHERE ao.country = ao2.country and DATE_TRUNC(ao2.date, MONTH) = DATE_TRUNC(DATE_SUB(ao.date, INTERVAL 1 Year),MONTH)) previous_year_revenue,
        (SELECT SUM(total_gross_gbp) from ao_and_budgets ao2 WHERE ao.country = ao2.country and DATE_TRUNC(ao2.date, MONTH) = DATE_TRUNC(DATE_SUB(ao.date, INTERVAL 1 Year),MONTH)) previous_year_revenue_gross_gbp,
        (SELECT SUM(total_gross_local) from ao_and_budgets ao2 WHERE ao.country = ao2.country and DATE_TRUNC(ao2.date, MONTH) = DATE_TRUNC(DATE_SUB(ao.date, INTERVAL 1 Year),MONTH)) previous_year_revenue_gross_local,
        (SELECT COUNT(DISTINCT ao2.Order_ID) from `data-warehouse-326816.vivastreet_production.ads_orders_report` ao2 WHERE ao.country = ao2.country and date(ao2.date) = DATE_SUB(ao.date, INTERVAL 1 Year)) previous_year_total_orders,
        (SELECT SUM(AOV) from ao_and_budgets ao2 WHERE ao.country = ao2.country and date(ao2.date) = DATE_SUB(ao.date, INTERVAL 1 Year)) previous_year_AOV,
        *
        FROM ao_and_budgets ao
    )
    group by country, day, date, 
    date,
    IF(country not in ('gb','fr','be','br'), 'ROW', country),
    year,
    yearmonth,
    month,
    iso_week,
    day
    
)
)

SELECT * FROM im_daily