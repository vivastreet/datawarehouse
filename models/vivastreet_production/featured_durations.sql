
{{ config(
    unique_key='_airbyte_ab_id'
    )
}}

with source_data as (
    SELECT _airbyte_ab_id, 'ar' as country, label, CAST(seconds as INTEGER) as seconds, CAST(duration_id as INTEGER) as duration_id FROM data_lake.ar_featured_durations
    UNION ALL   
    SELECT _airbyte_ab_id, 'be' as country, label, CAST(seconds as INTEGER) as seconds, CAST(duration_id as INTEGER) as duration_id FROM data_lake.be_featured_durations
    UNION ALL
    SELECT _airbyte_ab_id, 'br' as country, label, CAST(seconds as INTEGER) as seconds, CAST(duration_id as INTEGER) as duration_id FROM data_lake.br_featured_durations
    UNION ALL
    SELECT _airbyte_ab_id, 'cl' as country, label, CAST(seconds as INTEGER) as seconds, CAST(duration_id as INTEGER) as duration_id FROM data_lake.cl_featured_durations
    UNION ALL
    SELECT _airbyte_ab_id, 'co' as country, label, CAST(seconds as INTEGER) as seconds, CAST(duration_id as INTEGER) as duration_id FROM data_lake.co_featured_durations
    UNION ALL
    SELECT _airbyte_ab_id, 'fr' as country, label, CAST(seconds as INTEGER) as seconds, CAST(duration_id as INTEGER) as duration_id FROM data_lake.fr_featured_durations
    UNION ALL
    SELECT _airbyte_ab_id, 'gb' as country, label, CAST(seconds as INTEGER) as seconds, CAST(duration_id as INTEGER) as duration_id FROM data_lake.gb_featured_durations
    UNION ALL
    SELECT _airbyte_ab_id, 'ie' as country, label, CAST(seconds as INTEGER) as seconds, CAST(duration_id as INTEGER) as duration_id FROM data_lake.ie_featured_durations
    UNION ALL
    SELECT _airbyte_ab_id, 'in' as country, label, CAST(seconds as INTEGER) as seconds, CAST(duration_id as INTEGER) as duration_id FROM data_lake.in_featured_durations
    UNION ALL
    SELECT _airbyte_ab_id, 'it' as country, label, CAST(seconds as INTEGER) as seconds, CAST(duration_id as INTEGER) as duration_id FROM data_lake.it_featured_durations
    UNION ALL
    SELECT _airbyte_ab_id, 'ma' as country, label, CAST(seconds as INTEGER) as seconds, CAST(duration_id as INTEGER) as duration_id FROM data_lake.ma_featured_durations
    UNION ALL
    SELECT _airbyte_ab_id, 'pt' as country, label, CAST(seconds as INTEGER) as seconds, CAST(duration_id as INTEGER) as duration_id FROM data_lake.pt_featured_durations
    UNION ALL
    SELECT _airbyte_ab_id, 'us' as country, label, CAST(seconds as INTEGER) as seconds, CAST(duration_id as INTEGER) as duration_id FROM data_lake.us_featured_durations
)

SELECT k.*
FROM ( SELECT ARRAY_AGG(x LIMIT 1)[OFFSET(0)] k  FROM source_data x GROUP BY country, duration_id )

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
