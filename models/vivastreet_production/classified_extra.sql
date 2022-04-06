
{{ config(
    unique_key='_airbyte_ab_id',
    cluster_by=["classified_id"]
    )
}}

with source_data as (
    select _airbyte_ab_id, 'ar' as country, email, cast(amount as numeric) as amount, cast(clad_id as integer) as clad_id, init_by, plan_id, cast(user_id as integer) as user_id, zipcode, cast(order_id as integer) as order_id, platform, cast(processed as timestamp) as processed, proxy_type, renew_type, cast(location_id as integer) as location_id, subcat_code, partner_name, customer_name, discount_type, posting_status, proxy_username, cast(clad_revenue_id as integer) as clad_revenue_id, existing_status, individual_type, transaction_type, cast(clad_affiliate_id as integer) as clad_affiliate_id, cast(user_affiliate_id as integer) as user_affiliate_id
            from data_lake.ar_clad_revenue
            where processed > '2019-01-01'
            UNION ALL
    select _airbyte_ab_id, 'be' as country, email, cast(amount as numeric) as amount, cast(clad_id as integer) as clad_id, init_by, plan_id, cast(user_id as integer) as user_id, zipcode, cast(order_id as integer) as order_id, platform, cast(processed as timestamp) as processed, proxy_type, renew_type, cast(location_id as integer) as location_id, subcat_code, partner_name, customer_name, discount_type, posting_status, proxy_username, cast(clad_revenue_id as integer) as clad_revenue_id, existing_status, individual_type, transaction_type, cast(clad_affiliate_id as integer) as clad_affiliate_id, cast(user_affiliate_id as integer) as user_affiliate_id
            from data_lake.be_clad_revenue
            where processed > '2019-01-01'
            UNION ALL
    select _airbyte_ab_id, 'br' as country, email, cast(amount as numeric) as amount, cast(clad_id as integer) as clad_id, init_by, plan_id, cast(user_id as integer) as user_id, zipcode, cast(order_id as integer) as order_id, platform, cast(processed as timestamp) as processed, proxy_type, renew_type, cast(location_id as integer) as location_id, subcat_code, partner_name, customer_name, discount_type, posting_status, proxy_username, cast(clad_revenue_id as integer) as clad_revenue_id, existing_status, individual_type, transaction_type, cast(clad_affiliate_id as integer) as clad_affiliate_id, cast(user_affiliate_id as integer) as user_affiliate_id
            from data_lake.br_clad_revenue
            where processed > '2019-01-01'
            UNION ALL
    select _airbyte_ab_id, 'cl' as country, email, cast(amount as numeric) as amount, cast(clad_id as integer) as clad_id, init_by, plan_id, cast(user_id as integer) as user_id, zipcode, cast(order_id as integer) as order_id, platform, cast(processed as timestamp) as processed, proxy_type, renew_type, cast(location_id as integer) as location_id, subcat_code, partner_name, customer_name, discount_type, posting_status, proxy_username, cast(clad_revenue_id as integer) as clad_revenue_id, existing_status, individual_type, transaction_type, cast(clad_affiliate_id as integer) as clad_affiliate_id, cast(user_affiliate_id as integer) as user_affiliate_id
            from data_lake.cl_clad_revenue
            where processed > '2019-01-01'
            UNION ALL
    select _airbyte_ab_id, 'co' as country, email, cast(amount as numeric) as amount, cast(clad_id as integer) as clad_id, init_by, plan_id, cast(user_id as integer) as user_id, zipcode, cast(order_id as integer) as order_id, platform, cast(processed as timestamp) as processed, proxy_type, renew_type, cast(location_id as integer) as location_id, subcat_code, partner_name, customer_name, discount_type, posting_status, proxy_username, cast(clad_revenue_id as integer) as clad_revenue_id, existing_status, individual_type, transaction_type, cast(clad_affiliate_id as integer) as clad_affiliate_id, cast(user_affiliate_id as integer) as user_affiliate_id
            from data_lake.co_clad_revenue
            where processed > '2019-01-01'
            UNION ALL
    select _airbyte_ab_id, 'fr' as country, email, cast(amount as numeric) as amount, cast(clad_id as integer) as clad_id, init_by, plan_id, cast(user_id as integer) as user_id, zipcode, cast(order_id as integer) as order_id, platform, cast(processed as timestamp) as processed, proxy_type, renew_type, cast(location_id as integer) as location_id, subcat_code, partner_name, customer_name, discount_type, posting_status, proxy_username, cast(clad_revenue_id as integer) as clad_revenue_id, existing_status, individual_type, transaction_type, cast(clad_affiliate_id as integer) as clad_affiliate_id, cast(user_affiliate_id as integer) as user_affiliate_id
            from data_lake.fr_clad_revenue
            where processed > '2019-01-01'
            UNION ALL
    select _airbyte_ab_id, 'gb' as country, email, cast(amount as numeric) as amount, cast(clad_id as integer) as clad_id, init_by, plan_id, cast(user_id as integer) as user_id, zipcode, cast(order_id as integer) as order_id, platform, cast(processed as timestamp) as processed, proxy_type, renew_type, cast(location_id as integer) as location_id, subcat_code, partner_name, customer_name, discount_type, posting_status, proxy_username, cast(clad_revenue_id as integer) as clad_revenue_id, existing_status, individual_type, transaction_type, cast(clad_affiliate_id as integer) as clad_affiliate_id, cast(user_affiliate_id as integer) as user_affiliate_id
            from data_lake.gb_clad_revenue
            where processed > '2019-01-01'
            UNION ALL
    select _airbyte_ab_id, 'ie' as country, email, cast(amount as numeric) as amount, cast(clad_id as integer) as clad_id, init_by, plan_id, cast(user_id as integer) as user_id, zipcode, cast(order_id as integer) as order_id, platform, cast(processed as timestamp) as processed, proxy_type, renew_type, cast(location_id as integer) as location_id, subcat_code, partner_name, customer_name, discount_type, posting_status, proxy_username, cast(clad_revenue_id as integer) as clad_revenue_id, existing_status, individual_type, transaction_type, cast(clad_affiliate_id as integer) as clad_affiliate_id, cast(user_affiliate_id as integer) as user_affiliate_id
            from data_lake.ie_clad_revenue
            where processed > '2019-01-01'
            UNION ALL
    select _airbyte_ab_id, 'in' as country, email, cast(amount as numeric) as amount, cast(clad_id as integer) as clad_id, init_by, plan_id, cast(user_id as integer) as user_id, zipcode, cast(order_id as integer) as order_id, platform, cast(processed as timestamp) as processed, proxy_type, renew_type, cast(location_id as integer) as location_id, subcat_code, partner_name, customer_name, discount_type, posting_status, proxy_username, cast(clad_revenue_id as integer) as clad_revenue_id, existing_status, individual_type, transaction_type, cast(clad_affiliate_id as integer) as clad_affiliate_id, cast(user_affiliate_id as integer) as user_affiliate_id
            from data_lake.in_clad_revenue
            where processed > '2019-01-01'
            UNION ALL
    select _airbyte_ab_id, 'it' as country, email, cast(amount as numeric) as amount, cast(clad_id as integer) as clad_id, init_by, plan_id, cast(user_id as integer) as user_id, zipcode, cast(order_id as integer) as order_id, platform, cast(processed as timestamp) as processed, proxy_type, renew_type, cast(location_id as integer) as location_id, subcat_code, partner_name, customer_name, discount_type, posting_status, proxy_username, cast(clad_revenue_id as integer) as clad_revenue_id, existing_status, individual_type, transaction_type, cast(clad_affiliate_id as integer) as clad_affiliate_id, cast(user_affiliate_id as integer) as user_affiliate_id
            from data_lake.it_clad_revenue
            where processed > '2019-01-01'
            UNION ALL
    select _airbyte_ab_id, 'ma' as country, email, cast(amount as numeric) as amount, cast(clad_id as integer) as clad_id, init_by, plan_id, cast(user_id as integer) as user_id, zipcode, cast(order_id as integer) as order_id, platform, cast(processed as timestamp) as processed, proxy_type, renew_type, cast(location_id as integer) as location_id, subcat_code, partner_name, customer_name, discount_type, posting_status, proxy_username, cast(clad_revenue_id as integer) as clad_revenue_id, existing_status, individual_type, transaction_type, cast(clad_affiliate_id as integer) as clad_affiliate_id, cast(user_affiliate_id as integer) as user_affiliate_id
            from data_lake.ma_clad_revenue
            where processed > '2019-01-01'
            UNION ALL
    select _airbyte_ab_id, 'pt' as country, email, cast(amount as numeric) as amount, cast(clad_id as integer) as clad_id, init_by, plan_id, cast(user_id as integer) as user_id, zipcode, cast(order_id as integer) as order_id, platform, cast(processed as timestamp) as processed, proxy_type, renew_type, cast(location_id as integer) as location_id, subcat_code, partner_name, customer_name, discount_type, posting_status, proxy_username, cast(clad_revenue_id as integer) as clad_revenue_id, existing_status, individual_type, transaction_type, cast(clad_affiliate_id as integer) as clad_affiliate_id, cast(user_affiliate_id as integer) as user_affiliate_id
            from data_lake.pt_clad_revenue
            where processed > '2019-01-01'
            UNION ALL
    select _airbyte_ab_id, 'us' as country, email, cast(amount as numeric) as amount, cast(clad_id as integer) as clad_id, init_by, plan_id, cast(user_id as integer) as user_id, zipcode, cast(order_id as integer) as order_id, platform, cast(processed as timestamp) as processed, proxy_type, renew_type, cast(location_id as integer) as location_id, subcat_code, partner_name, customer_name, discount_type, posting_status, proxy_username, cast(clad_revenue_id as integer) as clad_revenue_id, existing_status, individual_type, transaction_type, cast(clad_affiliate_id as integer) as clad_affiliate_id, cast(user_affiliate_id as integer) as user_affiliate_id
            from data_lake.us_clad_revenue
            where processed > '2019-01-01'
            UNION ALL
)

SELECT k.*
FROM ( SELECT ARRAY_AGG(x LIMIT 1)[OFFSET(0)] k  FROM source_data x GROUP BY country, user_id)

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
