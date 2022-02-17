
{{ config(
    unique_key='_airbyte_ab_id'
    )
}}

with source_data as (

    select _airbyte_ab_id, cast(id as integer) as id, 'gb' as country, code, cast(meta as integer) as meta, type, color, label, logos, cast(`order` as integer) as `order`, param, cast(column as integer) as column, cast(parent as integer) as parent, photos, status, videos, cast(counter as integer) as counter, meta_h1, new_cat, resumes, roll_up, ad_label, homepage, old_code, adult_cat, meta_code, subdomain, vivaphone, meta_title, short_code, PARSE_DATETIME('%Y-%m-%dT%H:%M:%SZ', updated_at) as updated_at, premium_ads, cast(seo_columns as integer) as seo_columns, afs_keywords, detail_title, foreign_only, meta_h1_spec, search_label, spec_options, carousel_mode, forced_search, hide_children, meta_keywords, old_subdomain, posting_label, premium_price, sold_out_spec, enable_profile, cast(sold_out_level as integer) as sold_out_level, carousel_source, footer_seo_spec, meta_breadcrumb, meta_title_spec, repost_interval, adsense_keywords, image_moderation, meta_description, profile_vip_mode, alternative_label, search_only_title, subcats_on_search, default_expiration, evergreen_interval, meta_keywords_spec, photo_verification, summary_text_label, detail_normal_label, similar_ads_options, profile_vip_carousel, similar_ads_options2, meta_description_spec
    from vivastreet_production.gb_categories

    UNION ALL

    select _airbyte_ab_id, cast(id as integer) as id, 'pt' as country, code, cast(meta as integer) as meta, type, color, label, logos, cast(`order` as integer) as `order`, param, cast(column as integer) as column, cast(parent as integer) as parent, photos, status, videos, cast(counter as integer) as counter, meta_h1, new_cat, resumes, roll_up, ad_label, homepage, old_code, adult_cat, meta_code, subdomain, vivaphone, meta_title, short_code, PARSE_DATETIME('%Y-%m-%dT%H:%M:%SZ', updated_at) as updated_at, premium_ads, cast(seo_columns as integer) as seo_columns, afs_keywords, detail_title, foreign_only, meta_h1_spec, search_label, spec_options, carousel_mode, forced_search, hide_children, meta_keywords, old_subdomain, posting_label, premium_price, sold_out_spec, enable_profile, cast(sold_out_level as integer) as sold_out_level, carousel_source, footer_seo_spec, meta_breadcrumb, meta_title_spec, repost_interval, adsense_keywords, image_moderation, meta_description, profile_vip_mode, alternative_label, search_only_title, subcats_on_search, default_expiration, evergreen_interval, meta_keywords_spec, photo_verification, summary_text_label, detail_normal_label, similar_ads_options, profile_vip_carousel, similar_ads_options2, meta_description_spec
    from vivastreet_production.pt_categories

    UNION ALL

    select _airbyte_ab_id, cast(id as integer) as id, 'es' as country, code, cast(meta as integer) as meta, type, color, label, logos, cast(`order` as integer) as `order`, param, cast(column as integer) as column, cast(parent as integer) as parent, photos, status, videos, cast(counter as integer) as counter, meta_h1, new_cat, resumes, roll_up, ad_label, homepage, old_code, adult_cat, meta_code, subdomain, vivaphone, meta_title, short_code, PARSE_DATETIME('%Y-%m-%dT%H:%M:%SZ', updated_at) as updated_at, premium_ads, cast(seo_columns as integer) as seo_columns, afs_keywords, detail_title, foreign_only, meta_h1_spec, search_label, spec_options, carousel_mode, forced_search, hide_children, meta_keywords, old_subdomain, posting_label, premium_price, sold_out_spec, enable_profile, cast(sold_out_level as integer) as sold_out_level, carousel_source, footer_seo_spec, meta_breadcrumb, meta_title_spec, repost_interval, adsense_keywords, image_moderation, meta_description, profile_vip_mode, alternative_label, search_only_title, subcats_on_search, default_expiration, evergreen_interval, meta_keywords_spec, photo_verification, summary_text_label, detail_normal_label, similar_ads_options, profile_vip_carousel, similar_ads_options2, meta_description_spec
    from vivastreet_production.es_categories

)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
