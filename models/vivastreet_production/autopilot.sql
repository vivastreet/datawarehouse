
{{ config(
    unique_key='_airbyte_ab_id'
    )
}}

with source_data as (
    select _airbyte_ab_id, 
    cast(id as integer) as id, 
    'gb' as country, 
    cast(date as timestamp) as date, flag, 
    cast(amount as numeric) as amount, 
    method, 
    noiredata,
    JSON_EXTRACT_SCALAR(noiredata, '$.id') as noire_id, 
    JSON_EXTRACT_SCALAR(noiredata, "$.customParameters.SHOPPER_plan") as plan,
    JSON_EXTRACT_SCALAR(noiredata, "$.customParameters.SHOPPER_userId") as userId,
    JSON_EXTRACT_SCALAR(noiredata, "$.customParameters.SHOPPER_EndToEndIdentity") as EndToEndIdentity,
    JSON_EXTRACT_SCALAR(noiredata, "$.customParameters.SHOPPER_location") as location,
    JSON_EXTRACT_SCALAR(noiredata, "$.customParameters.SHOPPER_userName") as userName,
    JSON_EXTRACT_SCALAR(noiredata, "$.customParameters.SHOPPER_category") as category,
    JSON_EXTRACT_SCALAR(noiredata, "$.customParameters.SHOPPER_email") as email,
    JSON_EXTRACT_SCALAR(noiredata, "$.customParameters.SHOPPER_token") as token,
    JSON_EXTRACT_SCALAR(noiredata, "$.customParameters.SHOPPER_adId") as adId,
    FROM vivastreet_production.autopilot_gb_payments
)

SELECT * FROM source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
