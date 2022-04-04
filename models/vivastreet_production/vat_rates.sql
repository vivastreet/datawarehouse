
with source_data as (
    SELECT * FROM data_lake.vat_rates_sheet
    WHERE date is not null
)

SELECT * FROM source_data