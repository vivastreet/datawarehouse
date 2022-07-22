WITH daily_data as (
    SELECT
        'br' as country,
        JSON_EXTRACT_SCALAR(r, '$.paymentMethodCode') as paymentMethodCode,
        JSON_EXTRACT_SCALAR(r, '$.amount') as amount,
        JSON_EXTRACT_SCALAR(r, '$.idDeposit') as idDeposit,
        JSON_EXTRACT_SCALAR(r, '$.fee') as fee,
        JSON_EXTRACT_SCALAR(r, '$.lastChangeDate') as lastChangeDate,
        JSON_EXTRACT_SCALAR(r, '$.flags') as flags,
        JSON_EXTRACT_SCALAR(r, '$.externalId') as externalId,
        JSON_EXTRACT_SCALAR(r, '$.paymentMethodName') as paymentMethodName,
        JSON_EXTRACT_SCALAR(r, '$.creationDate') as creationDate,
        JSON_EXTRACT_SCALAR(r, '$.clientDocument') as clientDocument,
        JSON_EXTRACT_SCALAR(r, '$.refundAttempted') as refundAttempted,
        JSON_EXTRACT_SCALAR(r, '$.currency') as currency,
        JSON_EXTRACT_SCALAR(r, '$.requestedAmount') as requestedAmount,
        JSON_EXTRACT_SCALAR(r, '$.countryName') as countryName,
        JSON_EXTRACT_SCALAR(r, '$.requestedCurrency') as requestedCurrency,
        JSON_EXTRACT_SCALAR(r, '$.status') as status,
    FROM data_lake._airbyte_raw_directa24_deposits, 
    UNNEST(JSON_QUERY_ARRAY(_airbyte_data, '$.data')) as r 
),

historical_and_daily_data as (
    SELECT Creation_Date,	
            Reference,	
            Invoice,	
            'br' as Country,	
            Payment_Method,
            CASE WHEN Payment_Method = 'IX' THEN 'Pix'
                 WHEN Payment_Method = 'BL' THEN 'Boleto'
                 WHEN Payment_Method = 'WP' THEN 'WebPay'
                 WHEN Payment_Method = 'N/A' THEN 'N/A'
            END Payment_Method_Name,
            Status,	
            Currency,	
            User_Amount__local_,	
            Last_Change_Date,		
            Client_Document
            FROM data_lake.directa24_historical
    UNION ALL 
    SELECT  SAFE_CAST(creationDate as TIMESTAMP) as Creation_Date,
            SAFE_CAST(idDeposit as INTEGER) as Reference,
            SAFE_CAST(externalId as STRING) as Invoice,
            SAFE_CAST(country as STRING) as Country,
            SAFE_CAST(paymentMethodCode as STRING) as Payment_Method,
            SAFE_CAST(paymentMethodName as STRING) as Payment_Method_Name,
            SAFE_CAST(status as STRING) as Status,
            SAFE_CAST(requestedCurrency as STRING) as  Currency,
            SAFE_CAST(requestedAmount as FLOAT64) as User_Amount__local_,
            SAFE_CAST(lastChangeDate as TIMESTAMP) as Last_Change_Date,
            SAFE_CAST(clientDocument as INTEGER) as Client_Document
        FROM daily_data dd
)

SELECT k.*
FROM ( SELECT ARRAY_AGG(x LIMIT 1)[OFFSET(0)] k  FROM historical_and_daily_data x GROUP BY Reference, Invoice, Last_Change_Date)