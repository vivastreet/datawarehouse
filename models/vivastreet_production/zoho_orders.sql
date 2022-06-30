WITH source_data_historical as (
    SELECT 'gb' as country,
        SALESORDERID, 
        Ad_IDs,
        CAST(PARSE_DATE('%d %b, %Y',Payment_Received_Date) as DATE) Payment_Received_Date,
        Payment_Method,
        Sales_Order_Owner,
        SAFE_CAST(REGEXP_EXTRACT(REGEXP_REPLACE(Grand_Total, ',',''), r'([0-9,.]+)') as NUMERIC) Grand_Total,
        CAST(User_ID__payment_ as STRING) as User_ID,
        Description
    FROM data_lake.zoho_adhoc_gb
    UNION ALL
    SELECT 'fr' as country,
        Record_Id as SALESORDERID,
        '' as Ad_IDs,
        Date_du_paiement as Payment_Received_Date,        
        Type_paiement as Payment_Method,
        Gestionnaire_de_Produit as Sales_Order_Owner,       
        Total_G__n__ral as Grand_Total,
        CAST(N___de_client as STRING) as User_ID,
        Description__Articles_command__s_ as Description
        FROM data_lake.zoho_adhoc_fr
    UNION ALL
    SELECT 'be' as country,
        SALESORDERID,
        Ad_IDs,
        CAST(PARSE_DATE('%d %b, %Y',Payment_Received_Date) as DATE) Payment_Received_Date,
        Payment_Method,
        Account_Owner as Sales_Order_Owner,
        SAFE_CAST(REGEXP_EXTRACT(REGEXP_REPLACE(Grand_Total, ',',''), r'([0-9,.]+)') as NUMERIC) Grand_Total,
        CAST(User_ID__payment_ as STRING) as User_ID,
        Description
        FROM data_lake.zoho_adhoc_be
    UNION ALL
    SELECT 'br' as country,
        SALESORDERID,
        Ad_IDs,
        CAST(PARSE_DATE('%d %b, %Y',Payment_Received_Date) as DATE) Payment_Received_Date,
        Payment_Method,
        Account_Owner as Sales_Order_Owner,
        SAFE_CAST(REGEXP_EXTRACT(REGEXP_REPLACE(Grand_Total, ',',''), r'([0-9,.]+)') as NUMERIC) Grand_Total,
        CAST(User_ID__payment_ as STRING) as User_ID,
        Description
        FROM data_lake.zoho_adhoc_br
),

source_data_automated as (
    SELECT
        'gb' as country,
        JSON_EXTRACT_SCALAR(r, '$.id') as ID,
        CAST(JSON_EXTRACT_SCALAR(r, '$.Grand_Total') as NUMERIC ) as Grand_Total,  
        JSON_EXTRACT_SCALAR(r, '$.Description') as description, 
        JSON_EXTRACT_SCALAR(r, '$.User_ID_payment_email') as User_ID,  
        DATE(CAST(JSON_EXTRACT_SCALAR(r, '$.Payment_Received_Date') as TIMESTAMP)) as payment_received_date,  
        JSON_EXTRACT_SCALAR(r, '$.Payment_Method') as payment_method
    FROM data_lake._airbyte_raw_zoho_gb_sales_orders, 
    UNNEST(JSON_QUERY_ARRAY(JSON_EXTRACT_SCALAR(_airbyte_data, "$.details.output"), '$.data')) as r
    UNION ALL
    SELECT 
        'be' as country,
        JSON_EXTRACT_SCALAR(r, '$.id') as ID,
        CAST(JSON_EXTRACT_SCALAR(r, '$.Grand_Total') as NUMERIC ) as Grand_Total,
        JSON_EXTRACT_SCALAR(r, '$.Description') as Description,
        JSON_EXTRACT_SCALAR(r, '$.User_ID_payment_email') as User_ID,
        DATE(CAST(JSON_EXTRACT_SCALAR(r, '$.Payment_Received_Date') AS TIMESTAMP)) as Payment_Received_Date,
        JSON_EXTRACT_SCALAR(r, '$.Payment_Method') as Payment_Method,
        FROM data_lake._airbyte_raw_zoho_be_sales_orders, 
        UNNEST(JSON_QUERY_ARRAY(JSON_EXTRACT_SCALAR(_airbyte_data, "$.details.output"), '$.data')) as r
        UNION ALL
    SELECT 
        'fr' as country,
        JSON_EXTRACT_SCALAR(r, '$.id') as ID,
        GREATEST(SAFE_CAST(JSON_EXTRACT_SCALAR(r, '$.Grand_Total') as NUMERIC), SAFE_CAST(JSON_EXTRACT_SCALAR(r, '$.Valeur_du_contrat') as NUMERIC)/SAFE_CAST(JSON_EXTRACT_SCALAR(r, '$.Dur_e_du_contrat_mois') as NUMERIC))  as Grand_Total,
        JSON_EXTRACT_SCALAR(r, '$.Description') as Description,
        JSON_EXTRACT_SCALAR(r, '$.Customer_No') as User_ID,
        DATE(CAST(JSON_EXTRACT_SCALAR(r, '$.Date_1er_paiement') as TIMESTAMP)) as Payment_Received_Date,
        JSON_EXTRACT_SCALAR(r, '$.Type_Paiement') as Payment_Method,
        FROM data_lake._airbyte_raw_zoho_fr_sales_orders, 
        UNNEST(JSON_QUERY_ARRAY(JSON_EXTRACT_SCALAR(_airbyte_data, "$.details.output"), '$.data')) as r
),

source_data_union as (
        
    SELECT country, SALESORDERID as ID, Payment_Received_Date, Description, Payment_Method, Grand_Total, User_ID
    FROM source_data_historical
    UNION ALL
    SELECT country, ID, Payment_Received_Date, Description, Payment_Method, Grand_Total, User_ID
    FROM source_data_automated
),

source_data as (
            SELECT * FROM (
                SELECT k.*
                FROM ( SELECT ARRAY_AGG(x LIMIT 1)[OFFSET(0)] k  FROM source_data_union x GROUP BY country, ID, Payment_Received_Date)
            )
        )

SELECT * FROM source_data