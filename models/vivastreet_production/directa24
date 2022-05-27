with source_data as (
    SELECT * FROM data_lake.directa24_sheet
)


SELECT k.*
FROM ( SELECT ARRAY_AGG(x LIMIT 1)[OFFSET(0)] k  FROM source_data x GROUP BY reference,Invoice, Last_Change_Date)