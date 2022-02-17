
{{ config(
    unique_key='_airbyte_ab_id'
    )
}}

with source_data as (

    select _airbyte_ab_id, cast(id as integer) as id, cast(vat as numeric) as vat, cast(sent as datetime) as sent, email, items, cast(total as numeric) as total, country, cast(created as datetime) as created, category, cast(order_id as integer) as order_id, cast(subtotal as numeric) as subtotal, processor_code, cast(vat_percentage as numeric) as vat_percentage
    from vivastreet_production.common_invoices
    where created > '2019-01-01'
)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
