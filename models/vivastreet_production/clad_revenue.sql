
{{ config(
    unique_key='_airbyte_ab_id',
    partition_by={
       "field": "order_id",
       "data_type": "integer"
    }
    )
}}

with source_data as (

    select _airbyte_ab_id, 'gb' as country, email, cast(amount as numeric) as amount, cast(clad_id as integer) as clad_id, init_by, plan_id, cast(user_id as integer) as user_id, zipcode, cast(order_id as integer) as order_id, platform, cast(processed as timestamp) as processed, proxy_type, renew_type, cast(location_id as integer) as location_id, subcat_code, partner_name, customer_name, discount_type, posting_status, proxy_username, cast(clad_revenue_id as integer) as clad_revenue_id, existing_status, individual_type, transaction_type, cast(clad_affiliate_id as integer) as clad_affiliate_id, cast(user_affiliate_id as integer) as user_affiliate_id
    from vivastreet_production.gb_clad_revenue
    where processed > '2019-01-01'
)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
