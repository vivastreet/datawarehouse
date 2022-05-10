
with source_data as (
    SELECT * FROM data_lake.budgets_and_targets_sheet
    WHERE date is not null
)

SELECT k.*
FROM ( SELECT ARRAY_AGG(x LIMIT 1)[OFFSET(0)] k  FROM source_data x GROUP BY Type,Date,Month_,Country,Budget_ex_VAT,Daily_run_rate_revenue_,NB_of_Orders,Daily_Run_Rate__orders,AOV)