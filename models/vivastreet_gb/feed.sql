
{{ config(
    materialized='incremental',
    unique_key='_airbyte_ab_id'
    )
}}

with source_data as (

    select _airbyte_ab_id, cast(id as integer) as id, 'gb' as country, notes, enabled, ad_status, created_at, updated_at, description, repost_only, template_name, upload_credential_id
    from vivastreet_production.gb_feed

)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
