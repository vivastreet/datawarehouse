
{{ config(
    unique_key='_airbyte_ab_id',
    cluster_by=["classified_id"]
    )
}}

with source_data as (

    select _airbyte_ab_id, 'gb' as country, cast(user_id as integer) as user_id, cast(created_at as timestamp) as created_at, cast(updated_at as timestamp) as  updated_at, cast(classified_id as integer) as classified_id, cast(photo_verified as integer) as photo_verified, cast(account_sort_dt as timestamp) as account_sort_dt, cast(teaser_interval as integer) as teaser_interval, cast(vivamail_sent_on as timestamp) as vivamail_sent_on, cast(account_status_id as integer) as account_status_id, cast(discount_interval as integer) as discount_interval, cast(p2outstand_plan_id as integer) as p2outstand_plan_id, cast(repost_notify_sent as timestamp) as repost_notify_sent, cast(p2outstand_expires_on as timestamp) as p2outstand_expires_on, cast(p2outstand_auto_renewal as timestamp) as p2outstand_auto_renewal, cast(last_pre_expire_email_sent as timestamp) as last_pre_expire_email_sent, cast(last_post_expire_email_sent as timestamp) as last_post_expire_email_sent
    from vivastreet_production.gb_classified_extra
    where created_at > '2019-01-01'
)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
