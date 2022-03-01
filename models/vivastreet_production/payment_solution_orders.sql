
{{ config(
    unique_key='_airbyte_ab_id',
    cluster_by=["transaction_id", "id"]
    )
}}

with source_data as (

    select _airbyte_ab_id, 'gb' as country, cast(id as integer) as id, cast(user_id as integer) as user_id, cast(classified_id as integer) as classified_id, plan_ids, plan_types, cast(transaction_id as integer) as transaction_id, provider_id, result_code, refunded, cast(processed_at as timestamp) as processed_at, cast(created_at as timestamp) as created_at, cast(updated_at as timestamp) as updated_at, cast(amount as numeric) as amount, currency, status, denialid, service_fee, payment_method, boleto_url
    from vivastreet_production.gb_payment_solution_orders
    where created_at > '2019-01-01'
)

SELECT k.*
FROM ( SELECT ARRAY_AGG(x LIMIT 1)[OFFSET(0)] k  FROM source_data x GROUP BY id, user_id, transaction_id )

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
