
{{ config(
    unique_key='_airbyte_ab_id'
    )
}}

with source_data as (
    SELECT _airbyte_ab_id, 'gb' as country, CAST(id as INTEGER) id, name, label, CAST(display_priority as INTEGER) as display_priority FROM vivastreet_production.gb_featured_plan_type
)

SELECT k.*
FROM ( SELECT ARRAY_AGG(x LIMIT 1)[OFFSET(0)] k  FROM source_data x GROUP BY country, id )

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
