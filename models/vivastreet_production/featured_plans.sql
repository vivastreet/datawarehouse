
{{ config(
    unique_key='_airbyte_ab_id'
    )
}}

with source_data as (

    select _airbyte_ab_id, 'gb' as country, cast(plan_id as integer) as plan_id, geo, type, cast(`order` as integer) as  `order`, cast(price as numeric) as price, status, category, flag_new, parent_id, deprecated, cast(duration_id as integer) as duration_id, preselected, is_highlight, main_plan_id, version_number, individual_type, category_meta_id, cast(discount_level_1 as numeric) as discount_level_1, cast(discount_level_2 as numeric) as discount_level_2, cast(discount_level_3 as numeric) as discount_level_3, available_for_free, display_standalone, display_posting_form, cast(featured_plan_type_id as integer) as featured_plan_type_id
    from data_lake.gb_featured_plans
)


SELECT k.*
FROM ( SELECT ARRAY_AGG(x LIMIT 1)[OFFSET(0)] k  FROM source_data x GROUP BY country, plan_id )

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
