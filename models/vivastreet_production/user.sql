
{{ config(
    unique_key='_airbyte_ab_id'
    )
}}

with source_data as (
    SELECT _airbyte_ab_id, 'ar' as src_country, cast(user_id as integer) as user_id, zip, email, phone, extras, source, status, country, cast(created as timestamp) as created, last_ip, cast(updated as timestamp) as updated, cast(admin_id as integer) as admin_id, civility, lastname, password, username, usertype, firstname, watchlist, cast(year_born as integer) as year_born, cast(last_login as timestamp) as last_login, posted_ads, session_id, vip_profile, repost_optin, web_usertype, admin_comment, temp_password, nb_admin_flags, nb_classifieds, partners_optin, profile_enable, report_sent_dt, mandatory_phone, report_optout_dt, repost_optout_dt, usertype_options, vivastreet_optin, cast(from_affiliate_id as integer) as from_affiliate_id, whitelist_plan_id, content_platform_id, whitelist_expires_on, nb_pending_classifieds, nb_inactive_classifieds
    FROM data_lake.ar_user
    WHERE created > '2019-01-01'
    UNION ALL
    SELECT _airbyte_ab_id, 'be' as src_country, cast(user_id as integer) as user_id, zip, email, phone, extras, source, status, country, cast(created as timestamp) as created, last_ip, cast(updated as timestamp) as updated, cast(admin_id as integer) as admin_id, civility, lastname, password, username, usertype, firstname, watchlist, cast(year_born as integer) as year_born, cast(last_login as timestamp) as last_login, posted_ads, session_id, vip_profile, repost_optin, web_usertype, admin_comment, temp_password, nb_admin_flags, nb_classifieds, partners_optin, profile_enable, report_sent_dt, mandatory_phone, report_optout_dt, repost_optout_dt, usertype_options, vivastreet_optin, cast(from_affiliate_id as integer) as from_affiliate_id, whitelist_plan_id, content_platform_id, whitelist_expires_on, nb_pending_classifieds, nb_inactive_classifieds
        FROM data_lake.be_user
    WHERE created > '2019-01-01'
    UNION ALL
    SELECT _airbyte_ab_id, 'br' as src_country, cast(user_id as integer) as user_id, zip, email, phone, extras, source, status, country, cast(created as timestamp) as created, last_ip, cast(updated as timestamp) as updated, cast(admin_id as integer) as admin_id, civility, lastname, password, username, usertype, firstname, watchlist, cast(year_born as integer) as year_born, cast(last_login as timestamp) as last_login, posted_ads, session_id, vip_profile, repost_optin, web_usertype, admin_comment, temp_password, nb_admin_flags, nb_classifieds, partners_optin, profile_enable, report_sent_dt, mandatory_phone, report_optout_dt, repost_optout_dt, usertype_options, vivastreet_optin, cast(from_affiliate_id as integer) as from_affiliate_id, whitelist_plan_id, content_platform_id, whitelist_expires_on, nb_pending_classifieds, nb_inactive_classifieds
        FROM data_lake.br_user
    WHERE created > '2019-01-01'
    UNION ALL
    SELECT _airbyte_ab_id, 'cl' as src_country, cast(user_id as integer) as user_id, zip, email, phone, extras, source, status, country, cast(created as timestamp) as created, last_ip, cast(updated as timestamp) as updated, cast(admin_id as integer) as admin_id, civility, lastname, password, username, usertype, firstname, watchlist, cast(year_born as integer) as year_born, cast(last_login as timestamp) as last_login, posted_ads, session_id, vip_profile, repost_optin, web_usertype, admin_comment, temp_password, nb_admin_flags, nb_classifieds, partners_optin, profile_enable, report_sent_dt, mandatory_phone, report_optout_dt, repost_optout_dt, usertype_options, vivastreet_optin, cast(from_affiliate_id as integer) as from_affiliate_id, whitelist_plan_id, content_platform_id, whitelist_expires_on, nb_pending_classifieds, nb_inactive_classifieds
        FROM data_lake.cl_user
    WHERE created > '2019-01-01'
    UNION ALL
    SELECT _airbyte_ab_id, 'co' as src_country, cast(user_id as integer) as user_id, zip, email, phone, extras, source, status, country, cast(created as timestamp) as created, last_ip, cast(updated as timestamp) as updated, cast(admin_id as integer) as admin_id, civility, lastname, password, username, usertype, firstname, watchlist, cast(year_born as integer) as year_born, cast(last_login as timestamp) as last_login, posted_ads, session_id, vip_profile, repost_optin, web_usertype, admin_comment, temp_password, nb_admin_flags, nb_classifieds, partners_optin, profile_enable, report_sent_dt, mandatory_phone, report_optout_dt, repost_optout_dt, usertype_options, vivastreet_optin, cast(from_affiliate_id as integer) as from_affiliate_id, whitelist_plan_id, content_platform_id, whitelist_expires_on, nb_pending_classifieds, nb_inactive_classifieds
        FROM data_lake.co_user
    WHERE created > '2019-01-01'
    UNION ALL
    SELECT _airbyte_ab_id, 'fr' as src_country, cast(user_id as integer) as user_id, zip, email, phone, extras, source, status, country, cast(created as timestamp) as created, last_ip, cast(updated as timestamp) as updated, cast(admin_id as integer) as admin_id, civility, lastname, password, username, usertype, firstname, watchlist, cast(year_born as integer) as year_born, cast(last_login as timestamp) as last_login, posted_ads, session_id, vip_profile, repost_optin, web_usertype, admin_comment, temp_password, nb_admin_flags, nb_classifieds, partners_optin, profile_enable, report_sent_dt, mandatory_phone, report_optout_dt, repost_optout_dt, usertype_options, vivastreet_optin, cast(from_affiliate_id as integer) as from_affiliate_id, whitelist_plan_id, content_platform_id, whitelist_expires_on, nb_pending_classifieds, nb_inactive_classifieds
        FROM data_lake.fr_user
    WHERE created > '2019-01-01'
    UNION ALL
    SELECT _airbyte_ab_id, 'gb' as src_country, cast(user_id as integer) as user_id, zip, email, phone, extras, source, status, country, cast(created as timestamp) as created, last_ip, cast(updated as timestamp) as updated, cast(admin_id as integer) as admin_id, civility, lastname, password, username, usertype, firstname, watchlist, cast(year_born as integer) as year_born, cast(last_login as timestamp) as last_login, posted_ads, session_id, vip_profile, repost_optin, web_usertype, admin_comment, temp_password, nb_admin_flags, nb_classifieds, partners_optin, profile_enable, report_sent_dt, mandatory_phone, report_optout_dt, repost_optout_dt, usertype_options, vivastreet_optin, cast(from_affiliate_id as integer) as from_affiliate_id, whitelist_plan_id, content_platform_id, whitelist_expires_on, nb_pending_classifieds, nb_inactive_classifieds
        FROM data_lake.gb_user
    WHERE created > '2019-01-01'
    UNION ALL
    SELECT _airbyte_ab_id, 'ie' as src_country, cast(user_id as integer) as user_id, zip, email, phone, extras, source, status, country, cast(created as timestamp) as created, last_ip, cast(updated as timestamp) as updated, cast(admin_id as integer) as admin_id, civility, lastname, password, username, usertype, firstname, watchlist, cast(year_born as integer) as year_born, cast(last_login as timestamp) as last_login, posted_ads, session_id, vip_profile, repost_optin, web_usertype, admin_comment, temp_password, nb_admin_flags, nb_classifieds, partners_optin, profile_enable, report_sent_dt, mandatory_phone, report_optout_dt, repost_optout_dt, usertype_options, vivastreet_optin, cast(from_affiliate_id as integer) as from_affiliate_id, whitelist_plan_id, content_platform_id, whitelist_expires_on, nb_pending_classifieds, nb_inactive_classifieds
        FROM data_lake.ie_user
    WHERE created > '2019-01-01'
    UNION ALL
    SELECT _airbyte_ab_id, 'in' as src_country, cast(user_id as integer) as user_id, zip, email, phone, extras, source, status, country, cast(created as timestamp) as created, last_ip, cast(updated as timestamp) as updated, cast(admin_id as integer) as admin_id, civility, lastname, password, username, usertype, firstname, watchlist, cast(year_born as integer) as year_born, cast(last_login as timestamp) as last_login, posted_ads, session_id, vip_profile, repost_optin, web_usertype, admin_comment, temp_password, nb_admin_flags, nb_classifieds, partners_optin, profile_enable, report_sent_dt, mandatory_phone, report_optout_dt, repost_optout_dt, usertype_options, vivastreet_optin, cast(from_affiliate_id as integer) as from_affiliate_id, whitelist_plan_id, content_platform_id, whitelist_expires_on, nb_pending_classifieds, nb_inactive_classifieds
        FROM data_lake.in_user
    WHERE created > '2019-01-01'
    UNION ALL
    SELECT _airbyte_ab_id, 'it' as src_country, cast(user_id as integer) as user_id, zip, email, phone, extras, source, status, country, cast(created as timestamp) as created, last_ip, cast(updated as timestamp) as updated, cast(admin_id as integer) as admin_id, civility, lastname, password, username, usertype, firstname, watchlist, cast(year_born as integer) as year_born, cast(last_login as timestamp) as last_login, posted_ads, session_id, vip_profile, repost_optin, web_usertype, admin_comment, temp_password, nb_admin_flags, nb_classifieds, partners_optin, profile_enable, report_sent_dt, mandatory_phone, report_optout_dt, repost_optout_dt, usertype_options, vivastreet_optin, cast(from_affiliate_id as integer) as from_affiliate_id, whitelist_plan_id, content_platform_id, whitelist_expires_on, nb_pending_classifieds, nb_inactive_classifieds
        FROM data_lake.it_user
    WHERE created > '2019-01-01'
    UNION ALL
    SELECT _airbyte_ab_id, 'ma' as src_country, cast(user_id as integer) as user_id, zip, email, phone, extras, source, status, country, cast(created as timestamp) as created, last_ip, cast(updated as timestamp) as updated, cast(admin_id as integer) as admin_id, civility, lastname, password, username, usertype, firstname, watchlist, cast(year_born as integer) as year_born, cast(last_login as timestamp) as last_login, posted_ads, session_id, vip_profile, repost_optin, web_usertype, admin_comment, temp_password, nb_admin_flags, nb_classifieds, partners_optin, profile_enable, report_sent_dt, mandatory_phone, report_optout_dt, repost_optout_dt, usertype_options, vivastreet_optin, cast(from_affiliate_id as integer) as from_affiliate_id, whitelist_plan_id, content_platform_id, whitelist_expires_on, nb_pending_classifieds, nb_inactive_classifieds
        FROM data_lake.ma_user
    WHERE created > '2019-01-01'
    UNION ALL
    SELECT _airbyte_ab_id, 'pt' as src_country, cast(user_id as integer) as user_id, zip, email, phone, extras, source, status, country, cast(created as timestamp) as created, last_ip, cast(updated as timestamp) as updated, cast(admin_id as integer) as admin_id, civility, lastname, password, username, usertype, firstname, watchlist, cast(year_born as integer) as year_born, cast(last_login as timestamp) as last_login, posted_ads, session_id, vip_profile, repost_optin, web_usertype, admin_comment, temp_password, nb_admin_flags, nb_classifieds, partners_optin, profile_enable, report_sent_dt, mandatory_phone, report_optout_dt, repost_optout_dt, usertype_options, vivastreet_optin, cast(from_affiliate_id as integer) as from_affiliate_id, whitelist_plan_id, content_platform_id, whitelist_expires_on, nb_pending_classifieds, nb_inactive_classifieds
        FROM data_lake.pt_user
    WHERE created > '2019-01-01'
    UNION ALL
    SELECT _airbyte_ab_id, 'us' as src_country, cast(user_id as integer) as user_id, zip, email, phone, extras, source, status, country, cast(created as timestamp) as created, last_ip, cast(updated as timestamp) as updated, cast(admin_id as integer) as admin_id, civility, lastname, password, username, usertype, firstname, watchlist, cast(year_born as integer) as year_born, cast(last_login as timestamp) as last_login, posted_ads, session_id, vip_profile, repost_optin, web_usertype, admin_comment, temp_password, nb_admin_flags, nb_classifieds, partners_optin, profile_enable, report_sent_dt, mandatory_phone, report_optout_dt, repost_optout_dt, usertype_options, vivastreet_optin, cast(from_affiliate_id as integer) as from_affiliate_id, whitelist_plan_id, content_platform_id, whitelist_expires_on, nb_pending_classifieds, nb_inactive_classifieds
        FROM data_lake.us_user
    WHERE created > '2019-01-01'
)

SELECT k.*
FROM ( SELECT ARRAY_AGG(x LIMIT 1)[OFFSET(0)] k  FROM source_data x GROUP BY src_country, user_id, country )

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
