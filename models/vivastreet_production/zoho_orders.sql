{{ config(
    unique_key='_airbyte_ab_id'
    )
}}

WITH source_data as (
    SELECT
    'gb' as country, 
    JSON_EXTRACT_SCALAR(r, '$.Tier_Group') as tier_group,
    JSON_EXTRACT_SCALAR(r, '$.Grand_Total') as grand_total,
    JSON_EXTRACT_SCALAR(r, '$.Modified_Time') as modified_time,
    JSON_EXTRACT_SCALAR(r, '$.No_of_Ads_P2P') as no_of_ads_p2p,
    JSON_EXTRACT_SCALAR(r, '$.Owner.id') as ownser_id,
    JSON_EXTRACT_SCALAR(r, '$.Description') as description,
    JSON_EXTRACT_SCALAR(r, '$.User_ID_payment_email') as user_id_payment_email,
    JSON_EXTRACT_SCALAR(r, '$.Created_Time') as created_time,
    JSON_EXTRACT_SCALAR(r, '$.Payment_Received_Date') as payment_received_date,
    JSON_EXTRACT_SCALAR(r, '$.Sub_Total') as sub_total,
    JSON_EXTRACT_SCALAR(r, '$.Contract_Start') as contract_start,
    JSON_EXTRACT_SCALAR(r, '$.Duration') as duration,
    JSON_EXTRACT_SCALAR(r, '$.Payment_Method') as payment_method,
    JSON_EXTRACT_SCALAR(r, '$.Contract_Expiry') as contract_expiry,
    JSON_EXTRACT_SCALAR(r, '$.Subject') as subject,
    JSON_EXTRACT_SCALAR(r, '$.AD_ID1') as ad_id,
    JSON_EXTRACT_SCALAR(r, '$.Package_Type') as package_type,
    JSON_EXTRACT_SCALAR(r, '$.Modified_By.id') as modified_by_id,
    JSON_EXTRACT_SCALAR(r, '$.SO_Number') as so_number,
    JSON_EXTRACT_SCALAR(r, '$.id') as id
    FROM data_lake._airbyte_raw_zoho_sales_orders, 
    UNNEST(JSON_QUERY_ARRAY(JSON_EXTRACT_SCALAR(_airbyte_data, "$.details.output"), '$.data')) as r
)

SELECT * FROM source_data