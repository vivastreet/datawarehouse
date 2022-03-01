
{{ config(
    unique_key='_airbyte_ab_id',
    cluster_by=["user_id"]
    )
}}

with source_data as (

    select _airbyte_ab_id, 'gb' as src_country, cast(user_id as integer) as user_id, zip, email, phone, extras, source, status, country, cast(created as timestamp) as created, last_ip, cast(updated as timestamp) as updated, cast(admin_id as integer) as admin_id, civility, lastname, password, username, usertype, firstname, watchlist, cast(year_born as integer) as year_born, cast(last_login as timestamp) as last_login, posted_ads, session_id, vip_profile, repost_optin, web_usertype, admin_comment, temp_password, nb_admin_flags, nb_classifieds, partners_optin, profile_enable, report_sent_dt, mandatory_phone, report_optout_dt, repost_optout_dt, usertype_options, vivastreet_optin, cast(from_affiliate_id as integer) as from_affiliate_id, whitelist_plan_id, _ab_cdc_deleted_at, _ab_cdc_updated_at, content_platform_id, whitelist_expires_on, nb_pending_classifieds, nb_inactive_classifieds
    from vivastreet_production.gb_user
    where created > '2019-01-01'
)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
