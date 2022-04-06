
{{ config(
    unique_key='_airbyte_ab_id'
    )
}}

with source_data as (
    SELECT _airbyte_ab_id, 'ar' as country, CAST(id as INTEGER) id, name, label, CAST(display_priority as INTEGER) as display_priority FROM data_lake.ar_featured_plan_type
        UNION ALL
    SELECT _airbyte_ab_id, 'be' as country, CAST(id as INTEGER) id, name, label, CAST(display_priority as INTEGER) as display_priority FROM data_lake.be_featured_plan_type
        UNION ALL
    SELECT _airbyte_ab_id, 'br' as country, CAST(id as INTEGER) id, name, label, CAST(display_priority as INTEGER) as display_priority FROM data_lake.br_featured_plan_type
        UNION ALL
    SELECT _airbyte_ab_id, 'cl' as country, CAST(id as INTEGER) id, name, label, CAST(display_priority as INTEGER) as display_priority FROM data_lake.cl_featured_plan_type
        UNION ALL
    SELECT _airbyte_ab_id, 'co' as country, CAST(id as INTEGER) id, name, label, CAST(display_priority as INTEGER) as display_priority FROM data_lake.co_featured_plan_type
        UNION ALL
    SELECT _airbyte_ab_id, 'fr' as country, CAST(id as INTEGER) id, name, label, CAST(display_priority as INTEGER) as display_priority FROM data_lake.fr_featured_plan_type
        UNION ALL
    SELECT _airbyte_ab_id, 'gb' as country, CAST(id as INTEGER) id, name, label, CAST(display_priority as INTEGER) as display_priority FROM data_lake.gb_featured_plan_type
        UNION ALL
    SELECT _airbyte_ab_id, 'ie' as country, CAST(id as INTEGER) id, name, label, CAST(display_priority as INTEGER) as display_priority FROM data_lake.ie_featured_plan_type
        UNION ALL
    SELECT _airbyte_ab_id, 'in' as country, CAST(id as INTEGER) id, name, label, CAST(display_priority as INTEGER) as display_priority FROM data_lake.in_featured_plan_type
        UNION ALL
    SELECT _airbyte_ab_id, 'it' as country, CAST(id as INTEGER) id, name, label, CAST(display_priority as INTEGER) as display_priority FROM data_lake.it_featured_plan_type
        UNION ALL
    SELECT _airbyte_ab_id, 'ma' as country, CAST(id as INTEGER) id, name, label, CAST(display_priority as INTEGER) as display_priority FROM data_lake.ma_featured_plan_type
        UNION ALL
    SELECT _airbyte_ab_id, 'pt' as country, CAST(id as INTEGER) id, name, label, CAST(display_priority as INTEGER) as display_priority FROM data_lake.pt_featured_plan_type
        UNION ALL
    SELECT _airbyte_ab_id, 'us' as country, CAST(id as INTEGER) id, name, label, CAST(display_priority as INTEGER) as display_priority FROM data_lake.us_featured_plan_type
)

SELECT k.*
FROM ( SELECT ARRAY_AGG(x LIMIT 1)[OFFSET(0)] k  FROM source_data x GROUP BY country, id )

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
