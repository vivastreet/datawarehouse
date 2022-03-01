
{{ config(
    unique_key='_airbyte_ab_id',
    partition_by={
       "field": "transaction_id",
       "data_type": "string"
    }
    )
}}

with source_data as (

    select _airbyte_ab_id,
    CASE
        WHEN ps.description = 'vivastreet.co.uk' THEN 'gb'
        WHEN ps.description = 'vivastreet.co.in' THEN 'in'
        WHEN ps.description = 'vivastreet.com' THEN 'fr'
        WHEN ps.description = 'vivavisos.com.ar' THEN 'ar'
        WHEN ps.description = 'vivastreet.cl' THEN 'cl'
        WHEN ps.description = 'vivalocal.com' THEN 'br'
        WHEN ps.description = 'Let Pay Simple Boleto' THEN 'br'
        WHEN ps.description = 'Let Pay Simple Pix' THEN 'br'
        WHEN ps.description = 'vivastreet.co' THEN 'co'
        WHEN ps.description = 'vivastreet.it' THEN 'it'
        WHEN ps.description = 'Volt Bank Transfer' THEN 'gb'
        WHEN ps.description = 'vivastreet.be' THEN 'be'
        WHEN ps.description = 'vivastreet.ma' THEN 'ma'
        WHEN ps.description = 'vivastreet.ie' THEN 'ie'
        WHEN ps.description = 'vivalocal.pt' THEN 'pt'
    END country,
    cast(id as integer) as id, type, cast(amount as numeric) as amount, client, reason, status, gateway, cast(customer as integer) as customer, cast(created_at as timestamp) as created_at, token_hash, cast(updated_at as timestamp) as updated_at, description, return_data, cast(payment_card as integer) as payment_card, client_confirm, cast(transaction_at as timestamp) as transaction_at, transaction_id
    FROM vivastreet_production.payment_solution_payments ps
    WHERE created_at > '2019-01-01'
)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
