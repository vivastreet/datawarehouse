
{{ config(
    unique_key='_airbyte_ab_id',
     cluster_by=["id"]
    )
}}

with source_data as (

    select _airbyte_ab_id,
    CASE
        WHEN brand = 'vivastreet.co.uk' THEN 'gb'
        WHEN brand = 'vivalocal.com'    THEN 'br'
        WHEN brand = 'vivastreet.cl'    THEN 'cl'
        WHEN brand = 'vivastreet.com'   THEN 'fr'
        WHEN brand = 'vivastreet.be'    THEN 'be'
        WHEN brand = 'vivastreet.it'    THEN 'it'
    END country,
    cast(id as integer) as id, geo, cast(date as timestamp) as date, brand, cast(user_id as integer) as user_id, boleto_id, paid_date, client_cpf, plans_sold, client_name, sales_agent, client_email, cast(boleto_amount as numeric) as boleto_amount, cast(boleto_status as integer) as boleto_status, client_surname, sales_agent_email,
    from vivastreet_production.payment_suite_brcs_boleto_log
    where date > '2019-01-01'
)

SELECT k.*
FROM ( SELECT ARRAY_AGG(x LIMIT 1)[OFFSET(0)] k  FROM source_data x GROUP BY id, user_id, country )

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
