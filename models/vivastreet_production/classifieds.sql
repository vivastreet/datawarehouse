
{{ config(
    unique_key='_airbyte_ab_id',
    cluster_by=["id"]
    )
}}

with source_data as (
    select _airbyte_ab_id, 'gb' as country, cast(id as integer) as id, link, cast(cat_1 as integer) as cat_1, cast(cat_2 as integer) as cat_2, cast(cat_3 as integer) as cat_3, cast(cat_4 as integer) as cat_4, cast(cat_5 as integer) as cat_5, active, cast(posted as timestamp) as posted, status, cast(created as timestamp) as created, phone_nbr
    from vivastreet_production.gb_classifieds
    UNION ALL
    select _airbyte_ab_id, 'gb' as country, cast(id as integer) as id, link, cast(cat_1 as integer) as cat_1, cast(cat_2 as integer) as cat_2, cast(cat_3 as integer) as cat_3, cast(cat_4 as integer) as cat_4, cast(cat_5 as integer) as cat_5, active, cast(posted as timestamp) as posted, status, cast(created as timestamp) as created, phone_nbr
    from vivastreet_production.archive_gb
)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
