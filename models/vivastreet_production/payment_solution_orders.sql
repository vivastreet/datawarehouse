
{{ config(
    unique_key='_airbyte_ab_id'
    )
}}

with source_data as (

    select _airbyte_ab_id, 'gb' as country, cast(id as integer) as id, cast(user_id as integer) as user_id, cast(classified_id as integer) as classified_id, plan_ids, plan_types, cast(transaction_id as integer) as transaction_id, provider_id, result_code, refunded, PARSE_DATETIME('%Y-%m-%d %H:%M:%S UTC', processed_at) as processed_at, PARSE_DATETIME('%Y-%m-%d %H:%M:%S UTC', created_at) as created_at, PARSE_DATETIME('%Y-%m-%d %H:%M:%S UTC', updated_at) as updated_at, cast(amount as numeric) as amount, currency, status, denialid, service_fee, payment_method, boleto_url
    from vivastreet_production.gb_payment_solution_orders
    where created_at > '2019-01-01'
)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
