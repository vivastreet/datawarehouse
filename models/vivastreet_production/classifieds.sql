
{{ config(
    unique_key='_airbyte_ab_id'
    )
}}

with source_data as (
       select _airbyte_ab_id, 'ar' as country, cast(id as integer) as id, CAST(user_id as integer) user_id, link, cast(cat_1 as integer) as cat_1, cast(cat_2 as integer) as cat_2, cast(cat_3 as integer) as cat_3, cast(cat_4 as integer) as cat_4, cast(cat_5 as integer) as cat_5, active, cast(posted as timestamp) as posted, status, cast(created as timestamp) as created, phone_nbr
        from data_lake.ar_classifieds
        UNION ALL
select _airbyte_ab_id, 'ar' as country, cast(id as integer) as id, CAST(user_id as integer) user_id, link, cast(cat_1 as integer) as cat_1, cast(cat_2 as integer) as cat_2, cast(cat_3 as integer) as cat_3, cast(cat_4 as integer) as cat_4, cast(cat_5 as integer) as cat_5, active, cast(posted as timestamp) as posted, status, cast(created as timestamp) as created, phone_nbr
        from data_lake.archive_ar
        UNION ALL
select _airbyte_ab_id, 'ar' as country, cast(id as integer) as id, CAST(user_id as integer) user_id, link, cast(cat_1 as integer) as cat_1, cast(cat_2 as integer) as cat_2, cast(cat_3 as integer) as cat_3, cast(cat_4 as integer) as cat_4, cast(cat_5 as integer) as cat_5, active, cast(posted as timestamp) as posted, status, cast(created as timestamp) as created, phone_nbr
        from data_lake.vivastreet_common_classifieds_new WHERE LOWER(country) = 'ar'
        UNION ALL
select _airbyte_ab_id, 'be' as country, cast(id as integer) as id, CAST(user_id as integer) user_id, link, cast(cat_1 as integer) as cat_1, cast(cat_2 as integer) as cat_2, cast(cat_3 as integer) as cat_3, cast(cat_4 as integer) as cat_4, cast(cat_5 as integer) as cat_5, active, cast(posted as timestamp) as posted, status, cast(created as timestamp) as created, phone_nbr
        from data_lake.be_classifieds
        UNION ALL
select _airbyte_ab_id, 'be' as country, cast(id as integer) as id, CAST(user_id as integer) user_id, link, cast(cat_1 as integer) as cat_1, cast(cat_2 as integer) as cat_2, cast(cat_3 as integer) as cat_3, cast(cat_4 as integer) as cat_4, cast(cat_5 as integer) as cat_5, active, cast(posted as timestamp) as posted, status, cast(created as timestamp) as created, phone_nbr
        from data_lake.archive_be
        UNION ALL
select _airbyte_ab_id, 'be' as country, cast(id as integer) as id, CAST(user_id as integer) user_id, link, cast(cat_1 as integer) as cat_1, cast(cat_2 as integer) as cat_2, cast(cat_3 as integer) as cat_3, cast(cat_4 as integer) as cat_4, cast(cat_5 as integer) as cat_5, active, cast(posted as timestamp) as posted, status, cast(created as timestamp) as created, phone_nbr
        from data_lake.vivastreet_common_classifieds_new WHERE LOWER(country) = 'be'
        UNION ALL
select _airbyte_ab_id, 'br' as country, cast(id as integer) as id, CAST(user_id as integer) user_id, link, cast(cat_1 as integer) as cat_1, cast(cat_2 as integer) as cat_2, cast(cat_3 as integer) as cat_3, cast(cat_4 as integer) as cat_4, cast(cat_5 as integer) as cat_5, active, cast(posted as timestamp) as posted, status, cast(created as timestamp) as created, phone_nbr
        from data_lake.br_classifieds
        UNION ALL
select _airbyte_ab_id, 'br' as country, cast(id as integer) as id, CAST(user_id as integer) user_id, link, cast(cat_1 as integer) as cat_1, cast(cat_2 as integer) as cat_2, cast(cat_3 as integer) as cat_3, cast(cat_4 as integer) as cat_4, cast(cat_5 as integer) as cat_5, active, cast(posted as timestamp) as posted, status, cast(created as timestamp) as created, phone_nbr
        from data_lake.archive_br
        UNION ALL
select _airbyte_ab_id, 'br' as country, cast(id as integer) as id, CAST(user_id as integer) user_id, link, cast(cat_1 as integer) as cat_1, cast(cat_2 as integer) as cat_2, cast(cat_3 as integer) as cat_3, cast(cat_4 as integer) as cat_4, cast(cat_5 as integer) as cat_5, active, cast(posted as timestamp) as posted, status, cast(created as timestamp) as created, phone_nbr
        from data_lake.vivastreet_common_classifieds_new WHERE LOWER(country) = 'br'
        UNION ALL
select _airbyte_ab_id, 'cl' as country, cast(id as integer) as id, CAST(user_id as integer) user_id, link, cast(cat_1 as integer) as cat_1, cast(cat_2 as integer) as cat_2, cast(cat_3 as integer) as cat_3, cast(cat_4 as integer) as cat_4, cast(cat_5 as integer) as cat_5, active, cast(posted as timestamp) as posted, status, cast(created as timestamp) as created, phone_nbr
        from data_lake.cl_classifieds
        UNION ALL
select _airbyte_ab_id, 'cl' as country, cast(id as integer) as id, CAST(user_id as integer) user_id, link, cast(cat_1 as integer) as cat_1, cast(cat_2 as integer) as cat_2, cast(cat_3 as integer) as cat_3, cast(cat_4 as integer) as cat_4, cast(cat_5 as integer) as cat_5, active, cast(posted as timestamp) as posted, status, cast(created as timestamp) as created, phone_nbr
        from data_lake.archive_cl
        UNION ALL
select _airbyte_ab_id, 'cl' as country, cast(id as integer) as id, CAST(user_id as integer) user_id, link, cast(cat_1 as integer) as cat_1, cast(cat_2 as integer) as cat_2, cast(cat_3 as integer) as cat_3, cast(cat_4 as integer) as cat_4, cast(cat_5 as integer) as cat_5, active, cast(posted as timestamp) as posted, status, cast(created as timestamp) as created, phone_nbr
        from data_lake.vivastreet_common_classifieds_new WHERE LOWER(country) = 'cl'
        UNION ALL
select _airbyte_ab_id, 'co' as country, cast(id as integer) as id, CAST(user_id as integer) user_id, link, cast(cat_1 as integer) as cat_1, cast(cat_2 as integer) as cat_2, cast(cat_3 as integer) as cat_3, cast(cat_4 as integer) as cat_4, cast(cat_5 as integer) as cat_5, active, cast(posted as timestamp) as posted, status, cast(created as timestamp) as created, phone_nbr
        from data_lake.co_classifieds
        UNION ALL
select _airbyte_ab_id, 'co' as country, cast(id as integer) as id, CAST(user_id as integer) user_id, link, cast(cat_1 as integer) as cat_1, cast(cat_2 as integer) as cat_2, cast(cat_3 as integer) as cat_3, cast(cat_4 as integer) as cat_4, cast(cat_5 as integer) as cat_5, active, cast(posted as timestamp) as posted, status, cast(created as timestamp) as created, phone_nbr
        from data_lake.archive_co
        UNION ALL
select _airbyte_ab_id, 'co' as country, cast(id as integer) as id, CAST(user_id as integer) user_id, link, cast(cat_1 as integer) as cat_1, cast(cat_2 as integer) as cat_2, cast(cat_3 as integer) as cat_3, cast(cat_4 as integer) as cat_4, cast(cat_5 as integer) as cat_5, active, cast(posted as timestamp) as posted, status, cast(created as timestamp) as created, phone_nbr
        from data_lake.vivastreet_common_classifieds_new WHERE LOWER(country) = 'co'
        UNION ALL
select _airbyte_ab_id, 'fr' as country, cast(id as integer) as id, CAST(user_id as integer) user_id, link, cast(cat_1 as integer) as cat_1, cast(cat_2 as integer) as cat_2, cast(cat_3 as integer) as cat_3, cast(cat_4 as integer) as cat_4, cast(cat_5 as integer) as cat_5, active, cast(posted as timestamp) as posted, status, cast(created as timestamp) as created, phone_nbr
        from data_lake.fr_classifieds
        UNION ALL
select _airbyte_ab_id, 'fr' as country, cast(id as integer) as id, CAST(user_id as integer) user_id, link, cast(cat_1 as integer) as cat_1, cast(cat_2 as integer) as cat_2, cast(cat_3 as integer) as cat_3, cast(cat_4 as integer) as cat_4, cast(cat_5 as integer) as cat_5, active, cast(posted as timestamp) as posted, status, cast(created as timestamp) as created, phone_nbr
        from data_lake.archive_fr
        UNION ALL
select _airbyte_ab_id, 'fr' as country, cast(id as integer) as id, CAST(user_id as integer) user_id, link, cast(cat_1 as integer) as cat_1, cast(cat_2 as integer) as cat_2, cast(cat_3 as integer) as cat_3, cast(cat_4 as integer) as cat_4, cast(cat_5 as integer) as cat_5, active, cast(posted as timestamp) as posted, status, cast(created as timestamp) as created, phone_nbr
        from data_lake.vivastreet_common_classifieds_new WHERE LOWER(country) = 'fr'
        UNION ALL
select _airbyte_ab_id, 'gb' as country, cast(id as integer) as id, CAST(user_id as integer) user_id, link, cast(cat_1 as integer) as cat_1, cast(cat_2 as integer) as cat_2, cast(cat_3 as integer) as cat_3, cast(cat_4 as integer) as cat_4, cast(cat_5 as integer) as cat_5, active, cast(posted as timestamp) as posted, status, cast(created as timestamp) as created, phone_nbr
        from data_lake.gb_classifieds
        UNION ALL
select _airbyte_ab_id, 'gb' as country, cast(id as integer) as id, CAST(user_id as integer) user_id, link, cast(cat_1 as integer) as cat_1, cast(cat_2 as integer) as cat_2, cast(cat_3 as integer) as cat_3, cast(cat_4 as integer) as cat_4, cast(cat_5 as integer) as cat_5, active, cast(posted as timestamp) as posted, status, cast(created as timestamp) as created, phone_nbr
        from data_lake.archive_gb
        UNION ALL
select _airbyte_ab_id, 'gb' as country, cast(id as integer) as id, CAST(user_id as integer) user_id, link, cast(cat_1 as integer) as cat_1, cast(cat_2 as integer) as cat_2, cast(cat_3 as integer) as cat_3, cast(cat_4 as integer) as cat_4, cast(cat_5 as integer) as cat_5, active, cast(posted as timestamp) as posted, status, cast(created as timestamp) as created, phone_nbr
        from data_lake.vivastreet_common_classifieds_new WHERE LOWER(country) = 'gb'
        UNION ALL
select _airbyte_ab_id, 'ie' as country, cast(id as integer) as id, CAST(user_id as integer) user_id, link, cast(cat_1 as integer) as cat_1, cast(cat_2 as integer) as cat_2, cast(cat_3 as integer) as cat_3, cast(cat_4 as integer) as cat_4, cast(cat_5 as integer) as cat_5, active, cast(posted as timestamp) as posted, status, cast(created as timestamp) as created, phone_nbr
        from data_lake.ie_classifieds
        UNION ALL
select _airbyte_ab_id, 'ie' as country, cast(id as integer) as id, CAST(user_id as integer) user_id, link, cast(cat_1 as integer) as cat_1, cast(cat_2 as integer) as cat_2, cast(cat_3 as integer) as cat_3, cast(cat_4 as integer) as cat_4, cast(cat_5 as integer) as cat_5, active, cast(posted as timestamp) as posted, status, cast(created as timestamp) as created, phone_nbr
        from data_lake.archive_ie
        UNION ALL
select _airbyte_ab_id, 'ie' as country, cast(id as integer) as id, CAST(user_id as integer) user_id, link, cast(cat_1 as integer) as cat_1, cast(cat_2 as integer) as cat_2, cast(cat_3 as integer) as cat_3, cast(cat_4 as integer) as cat_4, cast(cat_5 as integer) as cat_5, active, cast(posted as timestamp) as posted, status, cast(created as timestamp) as created, phone_nbr
        from data_lake.vivastreet_common_classifieds_new WHERE LOWER(country) = 'ie'
        UNION ALL
select _airbyte_ab_id, 'in' as country, cast(id as integer) as id, CAST(user_id as integer) user_id, link, cast(cat_1 as integer) as cat_1, cast(cat_2 as integer) as cat_2, cast(cat_3 as integer) as cat_3, cast(cat_4 as integer) as cat_4, cast(cat_5 as integer) as cat_5, active, cast(posted as timestamp) as posted, status, cast(created as timestamp) as created, phone_nbr
        from data_lake.in_classifieds
        UNION ALL
select _airbyte_ab_id, 'in' as country, cast(id as integer) as id, CAST(user_id as integer) user_id, link, cast(cat_1 as integer) as cat_1, cast(cat_2 as integer) as cat_2, cast(cat_3 as integer) as cat_3, cast(cat_4 as integer) as cat_4, cast(cat_5 as integer) as cat_5, active, cast(posted as timestamp) as posted, status, cast(created as timestamp) as created, phone_nbr
        from data_lake.archive_in
        UNION ALL
select _airbyte_ab_id, 'in' as country, cast(id as integer) as id, CAST(user_id as integer) user_id, link, cast(cat_1 as integer) as cat_1, cast(cat_2 as integer) as cat_2, cast(cat_3 as integer) as cat_3, cast(cat_4 as integer) as cat_4, cast(cat_5 as integer) as cat_5, active, cast(posted as timestamp) as posted, status, cast(created as timestamp) as created, phone_nbr
        from data_lake.vivastreet_common_classifieds_new WHERE LOWER(country) = 'in'
        UNION ALL
select _airbyte_ab_id, 'it' as country, cast(id as integer) as id, CAST(user_id as integer) user_id, link, cast(cat_1 as integer) as cat_1, cast(cat_2 as integer) as cat_2, cast(cat_3 as integer) as cat_3, cast(cat_4 as integer) as cat_4, cast(cat_5 as integer) as cat_5, active, cast(posted as timestamp) as posted, status, cast(created as timestamp) as created, phone_nbr
        from data_lake.it_classifieds
        UNION ALL
select _airbyte_ab_id, 'it' as country, cast(id as integer) as id, CAST(user_id as integer) user_id, link, cast(cat_1 as integer) as cat_1, cast(cat_2 as integer) as cat_2, cast(cat_3 as integer) as cat_3, cast(cat_4 as integer) as cat_4, cast(cat_5 as integer) as cat_5, active, cast(posted as timestamp) as posted, status, cast(created as timestamp) as created, phone_nbr
        from data_lake.archive_it
        UNION ALL
select _airbyte_ab_id, 'it' as country, cast(id as integer) as id, CAST(user_id as integer) user_id, link, cast(cat_1 as integer) as cat_1, cast(cat_2 as integer) as cat_2, cast(cat_3 as integer) as cat_3, cast(cat_4 as integer) as cat_4, cast(cat_5 as integer) as cat_5, active, cast(posted as timestamp) as posted, status, cast(created as timestamp) as created, phone_nbr
        from data_lake.vivastreet_common_classifieds_new WHERE LOWER(country) = 'it'
        UNION ALL
select _airbyte_ab_id, 'ma' as country, cast(id as integer) as id, CAST(user_id as integer) user_id, link, cast(cat_1 as integer) as cat_1, cast(cat_2 as integer) as cat_2, cast(cat_3 as integer) as cat_3, cast(cat_4 as integer) as cat_4, cast(cat_5 as integer) as cat_5, active, cast(posted as timestamp) as posted, status, cast(created as timestamp) as created, phone_nbr
        from data_lake.ma_classifieds
        UNION ALL
select _airbyte_ab_id, 'ma' as country, cast(id as integer) as id, CAST(user_id as integer) user_id, link, cast(cat_1 as integer) as cat_1, cast(cat_2 as integer) as cat_2, cast(cat_3 as integer) as cat_3, cast(cat_4 as integer) as cat_4, cast(cat_5 as integer) as cat_5, active, cast(posted as timestamp) as posted, status, cast(created as timestamp) as created, phone_nbr
        from data_lake.archive_ma
        UNION ALL
select _airbyte_ab_id, 'ma' as country, cast(id as integer) as id, CAST(user_id as integer) user_id, link, cast(cat_1 as integer) as cat_1, cast(cat_2 as integer) as cat_2, cast(cat_3 as integer) as cat_3, cast(cat_4 as integer) as cat_4, cast(cat_5 as integer) as cat_5, active, cast(posted as timestamp) as posted, status, cast(created as timestamp) as created, phone_nbr
        from data_lake.vivastreet_common_classifieds_new WHERE LOWER(country) = 'ma'
        UNION ALL
select _airbyte_ab_id, 'pt' as country, cast(id as integer) as id, CAST(user_id as integer) user_id, link, cast(cat_1 as integer) as cat_1, cast(cat_2 as integer) as cat_2, cast(cat_3 as integer) as cat_3, cast(cat_4 as integer) as cat_4, cast(cat_5 as integer) as cat_5, active, cast(posted as timestamp) as posted, status, cast(created as timestamp) as created, phone_nbr
        from data_lake.pt_classifieds
        UNION ALL
select _airbyte_ab_id, 'pt' as country, cast(id as integer) as id, CAST(user_id as integer) user_id, link, cast(cat_1 as integer) as cat_1, cast(cat_2 as integer) as cat_2, cast(cat_3 as integer) as cat_3, cast(cat_4 as integer) as cat_4, cast(cat_5 as integer) as cat_5, active, cast(posted as timestamp) as posted, status, cast(created as timestamp) as created, phone_nbr
        from data_lake.archive_pt
        UNION ALL
select _airbyte_ab_id, 'pt' as country, cast(id as integer) as id, CAST(user_id as integer) user_id, link, cast(cat_1 as integer) as cat_1, cast(cat_2 as integer) as cat_2, cast(cat_3 as integer) as cat_3, cast(cat_4 as integer) as cat_4, cast(cat_5 as integer) as cat_5, active, cast(posted as timestamp) as posted, status, cast(created as timestamp) as created, phone_nbr
        from data_lake.vivastreet_common_classifieds_new WHERE LOWER(country) = 'pt'
        UNION ALL
select _airbyte_ab_id, 'us' as country, cast(id as integer) as id, CAST(user_id as integer) user_id, link, cast(cat_1 as integer) as cat_1, cast(cat_2 as integer) as cat_2, cast(cat_3 as integer) as cat_3, cast(cat_4 as integer) as cat_4, cast(cat_5 as integer) as cat_5, active, cast(posted as timestamp) as posted, status, cast(created as timestamp) as created, phone_nbr
        from data_lake.us_classifieds
        UNION ALL
select _airbyte_ab_id, 'us' as country, cast(id as integer) as id, CAST(user_id as integer) user_id, link, cast(cat_1 as integer) as cat_1, cast(cat_2 as integer) as cat_2, cast(cat_3 as integer) as cat_3, cast(cat_4 as integer) as cat_4, cast(cat_5 as integer) as cat_5, active, cast(posted as timestamp) as posted, status, cast(created as timestamp) as created, phone_nbr
        from data_lake.archive_us
        UNION ALL
select _airbyte_ab_id, 'us' as country, cast(id as integer) as id, CAST(user_id as integer) user_id, link, cast(cat_1 as integer) as cat_1, cast(cat_2 as integer) as cat_2, cast(cat_3 as integer) as cat_3, cast(cat_4 as integer) as cat_4, cast(cat_5 as integer) as cat_5, active, cast(posted as timestamp) as posted, status, cast(created as timestamp) as created, phone_nbr
        from data_lake.vivastreet_common_classifieds_new WHERE LOWER(country) = 'us'
)

SELECT k.*
FROM ( SELECT ARRAY_AGG(x LIMIT 1)[OFFSET(0)] k  FROM source_data x GROUP BY country, id, user_id )

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
