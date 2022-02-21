
{{ config(
    unique_key='_airbyte_ab_id'
    )
}}

with source_data as (

    select _airbyte_ab_id, cast(id as integer) as id, cast(REPLACE (vat, ',', '.') as numeric) as vat, cast(sent as timestamp) as sent, email, items, cast(REPLACE (total, ',', '.') as numeric) as total, country, cast(created as timestamp) as created, category, order_id, cast(REPLACE (subtotal, ',', '.') as numeric) as subtotal, processor_code, cast(REPLACE (vat_percentage, ',', '.') as numeric) as vat_percentage
    from vivastreet_production.common_invoices
    where created > '2019-01-01'
)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
