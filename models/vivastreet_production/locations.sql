
{{ config(
    unique_key='_airbyte_ab_id'
    )
}}

with source_data as (
    SELECT _airbyte_ab_id, 'gb' as country, CAST(id as INT64) id, code, name, CAST(depth as INT64) depth, label, CAST(`order` as INT64) `order`, enable, CAST(parent as INT64) parent, CAST(counter as INT64) counter, zipcode, latitude, longitude, subdomain, updated_at, CAST(l1_ancestor as INT64) l1_ancestor, region_code, main_location, footer_display, CAST(parent_subdomain as INT64) parent_subdomain
    FROM data_lake.gb_locations
)

SELECT k.*
FROM ( SELECT ARRAY_AGG(x LIMIT 1)[OFFSET(0)] k  FROM source_data x GROUP BY country, id )

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
