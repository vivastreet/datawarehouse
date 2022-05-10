
with source_data as (
    SELECT * FROM data_lake.vat_rates_sheet
    WHERE date is not null
)


SELECT k.*
FROM ( SELECT ARRAY_AGG(x LIMIT 1)[OFFSET(0)] k  FROM source_data x GROUP BY date, country, vat )