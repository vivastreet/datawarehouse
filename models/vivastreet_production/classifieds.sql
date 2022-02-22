
{{ config(
    unique_key='_airbyte_ab_id'
    )
}}

with source_data as (

    select _airbyte_ab_id, 'gb' as country, cast(id as integer) as id, link, cast(cat_1 as integer) as cat_1, cast(cat_2 as integer) as cat_2, cast(cat_3 as integer) as cat_3, cast(cat_4 as integer) as cat_4, cast(cat_5 as integer) as cat_5, active, posted, status, cast(created as timestamp) as created, cast(deleted as timestamp) as deleted, cast(expired as timestamp) as expired, cast(updated as timestamp) as updated, cast(user_id as integer) as user_id, zipcode, cat_tags, cast(disabled as timestamp) as disabled, geo_info, platform, cast(reposted as timestamp) as reposted, phone_nbr, remote_ip, cast(repost_on as timestamp) as repost_on, sp_text_0, sp_text_1, cast(expires_on as timestamp) as expires_on, cast(location_1 as integer) as location_1, cast(location_2 as integer) as location_2, cast(location_3 as integer) as location_3, cast(location_4 as integer) as location_4, cast(location_5 as integer) as location_5, offer_type, cast(updated_at as timestamp) as updated_at, cast(website_id as integer) as website_id, cast(category_id as integer) as category_id, cast(external_id as integer) as external_id, cast(featured_on as timestamp) as featured_on, filter_type, cast(image_total as integer) as image_total, cast(location_id as integer) as location_id, cast(p2v_plan_id as integer) as p2v_plan_id, resume_size, resume_type, sp_pets_age, sp_string_0, sp_string_1, sp_string_2, sp_string_3, subcat_code, affiliate_id, company_name, full_zipcode, geo_latitude, cast(lock_version as integer) as lock_version, media_origin, num_reposted, web_location, currency_code, geo_longitude, geo_transport, location_tags, logo_attached, modify_action, cast(nb_user_flags as integer) as nb_user_flags, cast(p2url_plan_id as integer) as p2url_plan_id, cast(p2vip_plan_id as integer) as p2vip_plan_id, sp_jobs_tasks, company_number, main_lang_desc, nationality_id, nb_admin_flags, cast(p2v_expires_on as timestamp) as p2v_expires_on, repost_plan_id, sp_common_tags, sp_common_used, sp_common_year, _ab_cdc_log_pos, display_company, foreign_country, individual_type, main_lang_title, p2label_plan_id, premium_plan_id, reposted_action, resume_attached, sp_common_price, sp_for_sale_era, sp_housing_fees, _ab_cdc_log_file, display_priority, featured_plan_id, p2url_expires_on, p2v_auto_renewal, p2vip_expires_on, sp_common_tags_2, sp_for_sale_size, sp_housing_sq_ft, sp_personals_age, highlight_plan_id, image_upload_path, interm_expires_on, repost_expires_on, sp_common_date_to, sp_vehicules_year, _ab_cdc_deleted_at, _ab_cdc_updated_at, p2label_expires_on, p2url_auto_renewal, p2vip_auto_renewal, repost_notify_sent, content_platform_id, expire_warning_sent, featured_expires_on, nb_user_flags_total, repost_auto_renewal, repost_notify_optin, sp_common_date_from, sp_common_main_type, sp_common_price_alt, sp_housing_location, sp_housing_nb_baths, sp_housing_nb_bedrs, sp_housing_nb_rooms, sp_vehicules_energy, sp_vehicules_length, highlight_expires_on, main_lang_title_code, main_lang_title_full, p2label_auto_renewal, phone_nbr_searchable, premium_auto_renewal, sp_housing_nb_sleeps, sp_vehicules_mileage, featured_auto_renewal, sp_common_designation, sp_common_second_type, sp_vehicules_cylinder, highlight_auto_renewal, sp_housing_annual_rent, sp_housing_weekly_rent, sp_housing_monthly_rent, sp_housing_weekly_rent_alt, sp_housing_monthly_rent_alt, featured_expire_warning_sent, sp_communities_rideshare_detail
    from vivastreet_production.gb_classifieds
)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
