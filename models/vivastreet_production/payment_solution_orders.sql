
{{ config(
    unique_key='_airbyte_ab_id'
    )
}}

with source_data as (

    select _airbyte_ab_id, cast(id as integer) as id, 'gb' as country, user_id, classified_id, plan_ids, plan_types, cast(transaction_id as integer) as transaction_id, provider_id, result_code, refunded, processed_at, created_at, updated_at, cast(amount as NUMERIC) as amount, currency, status, denialid, cast(service_fee as NUMERIC) as service_fee, payment_method, boleto_url
    from vivastreet_production.gb_payment_solution_orders

)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
