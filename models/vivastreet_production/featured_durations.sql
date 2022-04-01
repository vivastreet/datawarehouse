
{{ config(
    unique_key='_airbyte_ab_id'
    )
}}

with source_data as (
    SELECT _airbyte_ab_id, 'gb' as country, label, CAST(seconds as INTEGER) as seconds, CAST(duration_id as INTEGER) as duration_id FROM data_lake.gb_featured_durations
)

SELECT k.*
FROM ( SELECT ARRAY_AGG(x LIMIT 1)[OFFSET(0)] k  FROM source_data x GROUP BY country, duration_id )

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
