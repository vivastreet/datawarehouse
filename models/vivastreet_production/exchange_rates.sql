
with source_data as (
    SELECT * FROM data_lake.exchange_rates_sheet
    where date is not null
)

SELECT k.*
FROM ( SELECT ARRAY_AGG(x LIMIT 1)[OFFSET(0)] k  FROM source_data x GROUP BY date,country,currency_name,currency_rate)

date,country,currency_name,currency_rate