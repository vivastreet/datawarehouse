WITH source_data as (
    SELECT c.id,
           c.created            as classified_created,
           c.country            as country,
           phone_nbr,
           u.user_id,
           u.created               user_created,
           ce.teaser_interval   as ce_teaser_interval,
           ce.discount_interval as ce_discount_interval,
    FROM {{ ref('classifieds') }} c
    LEFT JOIN {{ ref ('classifieds_extra') }} ce
ON ce.classified_id = c.id
    and ce.country = c.country
    LEFT JOIN {{ ref ('user') }} u ON
    CAST (c.user_id as STRING) = CAST (u.user_id as STRING)
    )


SELECT k.*
FROM ( SELECT ARRAY_AGG(x LIMIT 1)[OFFSET(0)] k  FROM source_data x GROUP BY country id, user_id, created )