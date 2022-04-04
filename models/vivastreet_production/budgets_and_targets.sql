
with source_data as (
    SELECT * FROM data_lake.budgets_and_targets_sheet
    WHERE date is not null
)

SELECT * FROM source_data