
{{ config(
    unique_key='_airbyte_ab_id'
    )
}}

with source_data as (

    select _airbyte_ab_id, cast(id as integer) as id, type, cast(amount as numeric) as amount, client, reason, status, gateway, cast(customer as integer) as customer, cast(created_at as timestamp) as created_at, token_hash, cast(updated_at as timestamp) as updated_at, description, return_data, cast(payment_card as integer) as payment_card, client_confirm, cast(transaction_at as timestamp) as transaction_at, transaction_id
    from vivastreet_production.payment_solution_payments
    where created_at > '2019-01-01'
)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
