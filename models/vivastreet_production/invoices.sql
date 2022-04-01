
{{ config(
    unique_key='_airbyte_ab_id',
    cluster_by=["order_id"]
    )
}}

with source_data as (
    select _airbyte_ab_id, cast(id as integer) as id, cast(sent as timestamp) as sent, email, items, LOWER(country) as country, cast(created as timestamp) as created, category, order_id, processor_code, cast(REPLACE(REPLACE (subtotal, ',', '.'), '.00', '') as numeric) as subtotal, cast(REPLACE(REPLACE (total, ',', '.'), '.00', '') as numeric) as total, cast(REPLACE (vat, ',', '.') as numeric) as vat, vat_percentage
    from data_lake.vivastreet_common_invoices
    where created > '2019-01-01'
)

SELECT k.*
FROM ( SELECT ARRAY_AGG(x LIMIT 1)[OFFSET(0)] k  FROM source_data x GROUP BY id, country, order_id)

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
