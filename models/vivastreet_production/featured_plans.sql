
{{ config(
    unique_key='_airbyte_ab_id'
    )
}}

with source_data as (

    select _airbyte_ab_id, 'ar' as country, cast(plan_id as integer) as plan_id, geo, type, cast(`order` as integer) as  `order`, cast(price as numeric) as price, status, category, flag_new, parent_id, deprecated, cast(duration_id as integer) as duration_id, preselected, is_highlight, main_plan_id, version_number, individual_type, category_meta_id, cast(discount_level_1 as numeric) as discount_level_1, cast(discount_level_2 as numeric) as discount_level_2, cast(discount_level_3 as numeric) as discount_level_3, available_for_free, display_standalone, display_posting_form, cast(featured_plan_type_id as integer) as featured_plan_type_id
        from data_lake.ar_featured_plans
        UNION ALL
    select _airbyte_ab_id, 'be' as country, cast(plan_id as integer) as plan_id, geo, type, cast(`order` as integer) as  `order`, cast(price as numeric) as price, status, category, flag_new, parent_id, deprecated, cast(duration_id as integer) as duration_id, preselected, is_highlight, main_plan_id, version_number, individual_type, category_meta_id, cast(discount_level_1 as numeric) as discount_level_1, cast(discount_level_2 as numeric) as discount_level_2, cast(discount_level_3 as numeric) as discount_level_3, available_for_free, display_standalone, display_posting_form, cast(featured_plan_type_id as integer) as featured_plan_type_id
        from data_lake.be_featured_plans
        UNION ALL
    select _airbyte_ab_id, 'br' as country, cast(plan_id as integer) as plan_id, geo, type, cast(`order` as integer) as  `order`, cast(price as numeric) as price, status, category, flag_new, parent_id, deprecated, cast(duration_id as integer) as duration_id, preselected, is_highlight, main_plan_id, version_number, individual_type, category_meta_id, cast(discount_level_1 as numeric) as discount_level_1, cast(discount_level_2 as numeric) as discount_level_2, cast(discount_level_3 as numeric) as discount_level_3, available_for_free, display_standalone, display_posting_form, cast(featured_plan_type_id as integer) as featured_plan_type_id
        from data_lake.br_featured_plans
        UNION ALL
    select _airbyte_ab_id, 'cl' as country, cast(plan_id as integer) as plan_id, geo, type, cast(`order` as integer) as  `order`, cast(price as numeric) as price, status, category, flag_new, parent_id, deprecated, cast(duration_id as integer) as duration_id, preselected, is_highlight, main_plan_id, version_number, individual_type, category_meta_id, cast(discount_level_1 as numeric) as discount_level_1, cast(discount_level_2 as numeric) as discount_level_2, cast(discount_level_3 as numeric) as discount_level_3, available_for_free, display_standalone, display_posting_form, cast(featured_plan_type_id as integer) as featured_plan_type_id
        from data_lake.cl_featured_plans
        UNION ALL
    select _airbyte_ab_id, 'co' as country, cast(plan_id as integer) as plan_id, geo, type, cast(`order` as integer) as  `order`, cast(price as numeric) as price, status, category, flag_new, parent_id, deprecated, cast(duration_id as integer) as duration_id, preselected, is_highlight, main_plan_id, version_number, individual_type, category_meta_id, cast(discount_level_1 as numeric) as discount_level_1, cast(discount_level_2 as numeric) as discount_level_2, cast(discount_level_3 as numeric) as discount_level_3, available_for_free, display_standalone, display_posting_form, cast(featured_plan_type_id as integer) as featured_plan_type_id
        from data_lake.co_featured_plans
        UNION ALL
    select _airbyte_ab_id, 'fr' as country, cast(plan_id as integer) as plan_id, geo, type, cast(`order` as integer) as  `order`, cast(price as numeric) as price, status, category, flag_new, parent_id, deprecated, cast(duration_id as integer) as duration_id, preselected, is_highlight, main_plan_id, version_number, individual_type, category_meta_id, cast(discount_level_1 as numeric) as discount_level_1, cast(discount_level_2 as numeric) as discount_level_2, cast(discount_level_3 as numeric) as discount_level_3, available_for_free, display_standalone, display_posting_form, cast(featured_plan_type_id as integer) as featured_plan_type_id
        from data_lake.fr_featured_plans
        UNION ALL
    select _airbyte_ab_id, 'gb' as country, cast(plan_id as integer) as plan_id, geo, type, cast(`order` as integer) as  `order`, cast(price as numeric) as price, status, category, flag_new, parent_id, deprecated, cast(duration_id as integer) as duration_id, preselected, is_highlight, main_plan_id, version_number, individual_type, category_meta_id, cast(discount_level_1 as numeric) as discount_level_1, cast(discount_level_2 as numeric) as discount_level_2, cast(discount_level_3 as numeric) as discount_level_3, available_for_free, display_standalone, display_posting_form, cast(featured_plan_type_id as integer) as featured_plan_type_id
        from data_lake.gb_featured_plans
        UNION ALL
    select _airbyte_ab_id, 'ie' as country, cast(plan_id as integer) as plan_id, geo, type, cast(`order` as integer) as  `order`, cast(price as numeric) as price, status, category, flag_new, parent_id, deprecated, cast(duration_id as integer) as duration_id, preselected, is_highlight, main_plan_id, version_number, individual_type, category_meta_id, cast(discount_level_1 as numeric) as discount_level_1, cast(discount_level_2 as numeric) as discount_level_2, cast(discount_level_3 as numeric) as discount_level_3, available_for_free, display_standalone, display_posting_form, cast(featured_plan_type_id as integer) as featured_plan_type_id
        from data_lake.ie_featured_plans
        UNION ALL
    select _airbyte_ab_id, 'in' as country, cast(plan_id as integer) as plan_id, geo, type, cast(`order` as integer) as  `order`, cast(price as numeric) as price, status, category, flag_new, parent_id, deprecated, cast(duration_id as integer) as duration_id, preselected, is_highlight, main_plan_id, version_number, individual_type, category_meta_id, cast(discount_level_1 as numeric) as discount_level_1, cast(discount_level_2 as numeric) as discount_level_2, cast(discount_level_3 as numeric) as discount_level_3, available_for_free, display_standalone, display_posting_form, cast(featured_plan_type_id as integer) as featured_plan_type_id
        from data_lake.in_featured_plans
        UNION ALL
    select _airbyte_ab_id, 'it' as country, cast(plan_id as integer) as plan_id, geo, type, cast(`order` as integer) as  `order`, cast(price as numeric) as price, status, category, flag_new, parent_id, deprecated, cast(duration_id as integer) as duration_id, preselected, is_highlight, main_plan_id, version_number, individual_type, category_meta_id, cast(discount_level_1 as numeric) as discount_level_1, cast(discount_level_2 as numeric) as discount_level_2, cast(discount_level_3 as numeric) as discount_level_3, available_for_free, display_standalone, display_posting_form, cast(featured_plan_type_id as integer) as featured_plan_type_id
        from data_lake.it_featured_plans
        UNION ALL
    select _airbyte_ab_id, 'ma' as country, cast(plan_id as integer) as plan_id, geo, type, cast(`order` as integer) as  `order`, cast(price as numeric) as price, status, category, flag_new, parent_id, deprecated, cast(duration_id as integer) as duration_id, preselected, is_highlight, main_plan_id, version_number, individual_type, category_meta_id, cast(discount_level_1 as numeric) as discount_level_1, cast(discount_level_2 as numeric) as discount_level_2, cast(discount_level_3 as numeric) as discount_level_3, available_for_free, display_standalone, display_posting_form, cast(featured_plan_type_id as integer) as featured_plan_type_id
        from data_lake.ma_featured_plans
        UNION ALL
    select _airbyte_ab_id, 'pt' as country, cast(plan_id as integer) as plan_id, geo, type, cast(`order` as integer) as  `order`, cast(price as numeric) as price, status, category, flag_new, parent_id, deprecated, cast(duration_id as integer) as duration_id, preselected, is_highlight, main_plan_id, version_number, individual_type, category_meta_id, cast(discount_level_1 as numeric) as discount_level_1, cast(discount_level_2 as numeric) as discount_level_2, cast(discount_level_3 as numeric) as discount_level_3, available_for_free, display_standalone, display_posting_form, cast(featured_plan_type_id as integer) as featured_plan_type_id
        from data_lake.pt_featured_plans
        UNION ALL
    select _airbyte_ab_id, 'us' as country, cast(plan_id as integer) as plan_id, geo, type, cast(`order` as integer) as  `order`, cast(price as numeric) as price, status, category, flag_new, parent_id, deprecated, cast(duration_id as integer) as duration_id, preselected, is_highlight, main_plan_id, version_number, individual_type, category_meta_id, cast(discount_level_1 as numeric) as discount_level_1, cast(discount_level_2 as numeric) as discount_level_2, cast(discount_level_3 as numeric) as discount_level_3, available_for_free, display_standalone, display_posting_form, cast(featured_plan_type_id as integer) as featured_plan_type_id
        from data_lake.us_featured_plans
)


SELECT k.*
FROM ( SELECT ARRAY_AGG(x LIMIT 1)[OFFSET(0)] k  FROM source_data x GROUP BY country, plan_id )

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
