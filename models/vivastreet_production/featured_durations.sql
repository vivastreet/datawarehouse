
{{ config(
    unique_key='_airbyte_ab_id'
    )
}}

with source_data as (
    SELECT _airbyte_ab_id, 'gb' as country, label, CAST(seconds as INTEGER) as seconds, CAST(duration_id as INTEGER) as duration_id FROM vivastreet_production.gb_featured_durations
)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
