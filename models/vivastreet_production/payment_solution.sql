
{{ config(
    unique_key='_airbyte_ab_id'
    )
}}

-- with source_data as (

--     select _airbyte_ab_id, cast(id as integer) as id, geo, cast(date as timestamp) as date, brand, cast(user_id as integer) as user_id, boleto_id, paid_date, client_cpf, plans_sold, client_name, sales_agent, client_email, cast(boleto_amount as numeric) as boleto_amount, cast(boleto_status as integer) as boleto_status, client_surname, sales_agent_email
--     from vivastreet_production.payment_solution_payments
--     where date > '2019-01-01'
-- )

-- select *
-- from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
