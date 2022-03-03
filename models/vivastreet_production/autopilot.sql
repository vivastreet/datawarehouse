
{{ config(
    unique_key='_airbyte_ab_id'
    )
}}

with source_data as (
    select _airbyte_ab_id, JSON_EXTRACT_SCALAR(noiredata, '$.id') as noire_id, cast(id as integer) as id, 'gb' as country, cast(date as timestamp) as date, flag, cast(amount as numeric) as amount, method, noiredata
    FROM vivastreet_production.autopilot_gb_payments
)

SELECT * FROM source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
