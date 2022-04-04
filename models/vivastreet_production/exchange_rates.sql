
with source_data as (
    SELECT * FROM data_lake.exchange_rates_sheet
    where date is not null
)

SELECT * FROM source_data