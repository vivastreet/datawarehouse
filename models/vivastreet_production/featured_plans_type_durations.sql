
with source_data as (
SELECT plan.country, price, type, plan.plan_id, duration.seconds, plan_type.name FROM {{ ref('featured_plans') }} plan
    LEFT JOIN {{ ref('featured_plan_type') }} plan_type ON (plan.featured_plan_type_id = plan_type.id AND plan.country = plan_type.country)
    LEFT JOIN {{ ref('featured_durations') }} duration ON (duration.duration_id = plan.duration_id AND duration.country = plan.country)
)

SELECT k.*
FROM ( SELECT ARRAY_AGG(x LIMIT 1)[OFFSET(0)] k  FROM source_data x GROUP BY country, plan_id)

/*
Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
