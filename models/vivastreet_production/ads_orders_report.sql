WITH categories as (
            SELECT
                c1.country ,c1.id c1_id, c1.meta_code c1_meta_code, c2.id c2_id, c2.meta_code c2_meta_code, c3.id c3_id, c3.meta_code c3_meta_code,
                c1.label as Category,
                IF(c3.label IS NULL, NULL, c2.label) as Umbrella,
                COALESCE(c3.label, c2.label) as Subcategory
            FROM `vivastreet_production.categories` c1
            LEFT JOIN  `vivastreet_production.categories` c2 ON (c1.id = c2.parent and c1.country = c2.country)
            LEFT JOIN `vivastreet_production.categories` c3 ON (c2.id = c3.parent and c2.country = c3.country)
        ),

        featured_plans as (
            SELECT plan.country, price, type, plan.plan_id, duration.seconds, plan_type.name FROM vivastreet_production.featured_plans plan
            LEFT JOIN vivastreet_production.featured_plan_type plan_type ON (plan.featured_plan_type_id = plan_type.id AND plan.country = plan_type.country)
            LEFT JOIN vivastreet_production.featured_durations duration ON (duration.duration_id = plan.duration_id AND duration.country = plan.country)
        ),

        vat_rates as (
            SELECT * FROM `data-warehouse-326816.vivastreet_production.vat_rates`
        ),

        plans_orders as (
            SELECT country, provider_id, user_id,
                    SUM(IF(fpo.type = 'featured' , duration, 0)) as featured_duration,
                    SUM(IF(fpo.type = 'featured' , price, 0)) as featured_price,
                    SUM(IF(fpo.type = 'repost' , duration, 0)) as repost_duration,
                    SUM(IF(fpo.type = 'repost' , price, 0)) as repost_price,
                    SUM(IF(fpo.type = 'p2label_new' , duration, 0)) as p2label_new_duration,
                    SUM(IF(fpo.type = 'p2label_new' , price, 0)) as p2label_new_price,
                    SUM(IF(fpo.type = 'single_repost' , duration, 0)) as single_repost_duration,
                    SUM(IF(fpo.type = 'single_repost' , price, 0)) as single_repost_price,
                    SUM(IF(fpo.type = 'p2vip' , duration, 0)) as p2vip_duration,
                    SUM(IF(fpo.type = 'p2vip' , price, 0)) as p2vip_price,
                    SUM(IF(fpo.type = 'p2url' , duration, 0)) as p2url_duration,
                    SUM(IF(fpo.type = 'p2url' , price, 0)) as p2url_price,
                    SUM(IF(fpo.type = 'premium' , duration, 0)) as premium_duration,
                    SUM(IF(fpo.type = 'premium' , price, 0)) as premium_price,
                    SUM(IF(fpo.type = 'repost_unlimited' , duration, 0)) as repost_unlimited_duration,
                    SUM(IF(fpo.type = 'repost_unlimited' , price, 0)) as repost_unlimited_price,
                    SUM(IF(fpo.type = 'highlight' , duration, 0)) as highlight_duration,
                    SUM(IF(fpo.type = 'highlight' , price, 0)) as highlight_price,
                    SUM(IF(fpo.type = 'p2v' , duration, 0)) as p2v_duration,
                    SUM(IF(fpo.type = 'p2v' , price, 0)) as p2v_price,
                    SUM(IF(fpo.type = 'p2label_urgent' , duration, 0)) as p2label_urgent_duration,
                    SUM(IF(fpo.type = 'p2label_urgent' , price, 0)) as p2label_urgent_price,
            FROM (
                    SELECT pso.country, pso.processed_at, provider_id, price, type, user_id, CAST(ROUND(fp.seconds/(3600*24), 0) as INT64) duration,
                    FROM vivastreet_production.payment_solution_orders pso, UNNEST(SPLIT(plan_ids)) plan_ids
                    LEFT JOIN featured_plans fp
                    ON fp.country = pso.country
                    AND CAST(fp.plan_id as STRING) = plan_ids
                ) fpo
            GROUP BY country, provider_id, user_id, processed_at
        ),

        plans_invoices as (
            SELECT country, order_id, sent, email, ARRAY_AGG(plans IGNORE NULLS) plans,
                    SUM(IF(fpo.type = 'featured' , duration, 0)) as featured_duration,
                    SUM(IF(fpo.type = 'featured' , price, 0)) as featured_price,
                    SUM(IF(fpo.type = 'repost' , duration, 0)) as repost_duration,
                    SUM(IF(fpo.type = 'repost' , price, 0)) as repost_price,
                    SUM(IF(fpo.type = 'p2label_new' , duration, 0)) as p2label_new_duration,
                    SUM(IF(fpo.type = 'p2label_new' , price, 0)) as p2label_new_price,
                    SUM(IF(fpo.type = 'single_repost' , duration, 0)) as single_repost_duration,
                    SUM(IF(fpo.type = 'single_repost' , price, 0)) as single_repost_price,
                    SUM(IF(fpo.type = 'p2vip' , duration, 0)) as p2vip_duration,
                    SUM(IF(fpo.type = 'p2vip' , price, 0)) as p2vip_price,
                    SUM(IF(fpo.type = 'p2url' , duration, 0)) as p2url_duration,
                    SUM(IF(fpo.type = 'p2url' , price, 0)) as p2url_price,
                    SUM(IF(fpo.type = 'premium' , duration, 0)) as premium_duration,
                    SUM(IF(fpo.type = 'premium' , price, 0)) as premium_price,
                    SUM(IF(fpo.type = 'repost_unlimited' , duration, 0)) as repost_unlimited_duration,
                    SUM(IF(fpo.type = 'repost_unlimited' , price, 0)) as repost_unlimited_price,
                    SUM(IF(fpo.type = 'highlight' , duration, 0)) as highlight_duration,
                    SUM(IF(fpo.type = 'highlight' , price, 0)) as highlight_price,
                    SUM(IF(fpo.type = 'p2v' , duration, 0)) as p2v_duration,
                    SUM(IF(fpo.type = 'p2v' , price, 0)) as p2v_price,
                    SUM(IF(fpo.type = 'p2label_urgent' , duration, NULL)) as p2label_urgent_duration,
                    SUM(IF(fpo.type = 'p2label_urgent' , price, NULL)) as p2label_urgent_price,
            FROM (
                    SELECT i.country, i,email, i.sent, order_id, price, type, CAST(ROUND(fp.seconds/(3600*24), 0) as INT64) duration, JSON_EXTRACT_SCALAR(plans, '$.id') plans
                    FROM vivastreet_production.invoices i, UNNEST(JSON_EXTRACT_ARRAY(items)) plans
                    LEFT JOIN featured_plans fp
                    ON LOWER(fp.country) = LOWER(i.country)
                    and CAST(fp.plan_id as STRING) = JSON_EXTRACT_SCALAR(plans, '$.id')
                ) fpo
            GROUP BY country, order_id, sent, email
        ),

        locations as (
            SELECT
                l.country,
                l3.id l3_id,
                l4.id l4_id,
                l.label as geo_1,
                l2.label as geo_2,
                l3.label as geo_3,
                l4.label as geo_4
            FROM `vivastreet_production.locations` l
                    LEFT JOIN `vivastreet_production.locations` l2
                                ON l2.parent = l.id and l2.country = l.country
                    LEFT JOIN `vivastreet_production.locations` l3
                                ON l3.parent = l2.id and l3.country = l2.country
                    LEFT JOIN `vivastreet_production.locations` l4
                                ON l4.parent = l3.id and l4.country = l3.country
            WHERE l.depth = 1
        ),

        payment_solution_payments as (
            SELECT
                ps.transaction_id,
                ps.id,
                ps.country,
                ps.customer as user_id,
                ps.status,
                ps.amount,
                ps.client_confirm,
                ps.transaction_at
            FROM `vivastreet_production.payments` ps
        ),

        classifieds as (
            SELECT c.id,
                    c.created as classified_created,
                    c.country as country,
                    phone_nbr,
                    u.user_id,
                    u.created user_created,
                    ce.teaser_interval as ce_teaser_interval,
                    ce.discount_interval as ce_discount_interval,
            FROM `vivastreet_production.classifieds` c
                    LEFT JOIN `vivastreet_production.classified_extra` ce
                    ON ce.classified_id = c.id
                    and ce.country = c.country
                    LEFT JOIN `data-warehouse-326816.vivastreet_production.user` u ON
                    CAST(c.user_id as STRING) = CAST(u.user_id as STRING)
                    and c.country = u.src_country
        ),

        payment_suite_orders as (
            SELECT
                country,
                boleto_id,
                safe_cast(date as TIMESTAMP) date,
                safe_cast(paid_date as TIMESTAMP) paid_date,
                user_id,
                client_cpf,
                client_email,
                boleto_amount,
                client_name,
                boleto_status,
                id,
                plans_sold
            FROM `vivastreet_production.payment_suite`
            WHERE boleto_status = 1
        ),

        payments as (
            SELECT * FROM
                (SELECT
                ps.country as ps_country,ps.transaction_id as ps_transaction_id,ps.id as ps_id,ps.user_id as ps_user_id,ps.status as ps_status,ps.amount as ps_amount,ps.client_confirm as ps_client_confirm,ps.transaction_at as ps_transaction_at,
                pso.id as pso_id, pso.classified_id as pso_classified_id,pso.plan_ids as pso_plan_ids,pso.plan_types as pso_plan_types,pso.transaction_id as pso_transaction_id,pso.provider_id as pso_provider_id,pso.result_code as pso_result_code,pso.refunded as pso_refunded,pso.processed_at as pso_processed_at,pso.created_at as pso_created_at,pso.updated_at as pso_updated_at,pso.amount as pso_amount,pso.currency as pso_currency,pso.status as pso_status,pso.denialid as pso_denialid,pso.service_fee as pso_service_fee,pso.payment_method as pso_payment_method,pso.boleto_url as pso_boleto_url,
                cr.amount as cr_amount, cr.email as cr_email,cr.clad_id as cr_clad_id,cr.init_by as cr_init_by,cr.plan_id as cr_plan_id,cr.user_id as cr_user_id,cr.zipcode as cr_zipcode,cr.order_id as cr_order_id,cr.platform as cr_platform,cr.processed as cr_processed,cr.proxy_type as cr_proxy_type,cr.renew_type as cr_renew_type,cr.location_id as cr_location_id,cr.subcat_code as cr_subcat_code,cr.partner_name as cr_partner_name,cr.customer_name as cr_customer_name,cr.discount_type as cr_discount,cr.posting_status as cr_posting_status,cr.proxy_username as cr_proxy_username,cr.clad_revenue_id as cr_clad_revenue_id,cr.existing_status as cr_existing_status,cr.individual_type as cr_individual_type,'payment_solution' as cr_transaction_type,cr.clad_affiliate_id as cr_clad_affiliate_id,cr.user_affiliate_id as cr_user_affiliate_id,
                i.id as i_id,i.sent as i_sent,i.email as i_email,i.items as i_items,i.country as i_country,i.created as i_created,i.category as i_category,i.order_id as i_order_id,i.processor_code as i_processor_code,i.subtotal as i_subtotal,i.total as i_total,i.vat as i_vat, i.vat_percentage,
                (SELECT ANY_VALUE(vat) from vat_rates vr WHERE vr.country = ps.country and DATE_TRUNC(DATE(ps.transaction_at), MONTH) = DATE_TRUNC(DATE(vr.date), MONTH)) vr_vat_percentage
                FROM payment_solution_payments ps
                LEFT JOIN `vivastreet_production.payment_solution_orders` pso
                ON pso.user_id = ps.user_id
                AND pso.provider_id = ps.transaction_id
                AND LOWER(ps.country) = LOWER(pso.country)
                LEFT JOIN `vivastreet_production.clad_revenue` cr
                ON cr.order_id = ps.id
                and LOWER(ps.country) = LOWER(cr.country)
                and cr.user_id = ps.user_id
                LEFT JOIN `vivastreet_production.invoices` i
                ON  i.email = cr.email
                and i.total = cr.amount
                and i.order_id = CAST( pso.id as STRING)
                and date(i.sent) = date(cr.processed)
                and LOWER(i.country) = LOWER(ps.country)
                WHERE (ps.status = 'completed' or ((ps.status = 'forwarding' or ps.status = 'refunded')  and ps.client_confirm = 'confirmed'))
                UNION ALL
                SELECT
                country as ps_country,boleto_id as ps_transaction_id,null as ps_id,user_id as ps_user_id,null as ps_status,boleto_amount as ps_amount,null as ps_client_confirm,IF(paid_date>date,paid_date, date) as ps_transaction_at,
                null as pso_id, null as pso_classified_id,null as pso_plan_ids, plans_sold as pso_plan_types,null as pso_transaction_id,null as pso_provider_id,null as pso_result_code,null as pso_refunded,null as pso_processed_at,null as pso_created_at,null as pso_updated_at,null as pso_amount,null as pso_currency,null as pso_status,null as pso_denialid,null as pso_service_fee,null as pso_payment_method,null as pso_boleto_url,
                null as cr_amount, null as cr_email,null as cr_clad_id,null as cr_init_by,null as cr_plan_id,null as cr_user_id,null as cr_zipcode,id as cr_order_id,null as cr_platform,null as cr_processed,null as cr_proxy_type,null as cr_renew_type,null as cr_location_id,null as cr_subcat_code,null as cr_partner_name,null as cr_customer_name,null as cr_discount_type,null as cr_posting_status,null as cr_proxy_username,null as cr_clad_revenue_id,null as cr_existing_status,null as cr_individual_type,'payment_suite' as cr_transaction_type,null as cr_clad_affiliate_id,null as cr_user_affiliate_id,
                null as i_id,null as i_sent,null as i_email,null as i_items,null as i_country,null as i_created,null as i_category,null as i_order_id,null as i_processor_code,null as i_subtotal,null as i_total,null as i_vat, null as i_vat_percentage,
                (SELECT ANY_VALUE(vat) from vat_rates vr WHERE LOWER(vr.country) = LOWER(ps.country) and DATE_TRUNC(DATE(IF(ps.paid_date>ps.date,ps.paid_date, ps.date)), MONTH) = DATE_TRUNC(vr.date, MONTH)) vr_vat_percentage
                FROM payment_suite_orders ps
                UNION ALL
                SELECT
                country as ps_country,noire_id as ps_transaction_id,null as ps_id,SAFE_CAST(userId as INT64) as ps_user_id,null as ps_status,amount as ps_amount,null as ps_client_confirm,date as ps_transaction_at,
                null as pso_id, null as pso_classified_id,null as pso_plan_ids, plan as pso_plan_types,null as pso_transaction_id,null as pso_provider_id,null as pso_result_code,null as pso_refunded,null as pso_processed_at,null as pso_created_at,null as pso_updated_at,null as pso_amount,null as pso_currency,null as pso_status,null as pso_denialid,null as pso_service_fee,null as pso_payment_method,null as pso_boleto_url,
                null as cr_amount, null as cr_email,SAFE_CAST(adId as integer) as cr_clad_id,null as cr_init_by,null as cr_plan_id,null as cr_user_id,null as cr_zipcode,id as cr_order_id,null as cr_platform,null as cr_processed,null as cr_proxy_type,null as cr_renew_type,null as cr_location_id,null as cr_subcat_code,null as cr_partner_name,null as cr_customer_name,null as cr_discount_type,null as cr_posting_status,null as cr_proxy_username,null as cr_clad_revenue_id,null as cr_existing_status,null as cr_individual_type, 'autopilot' as cr_transaction_type,null as cr_clad_affiliate_id,null as cr_user_affiliate_id,
                null as i_id,null as i_sent,null as i_email,null as i_items,null as i_country,null as i_created,null as i_category,null as i_order_id,null as i_processor_code,null as i_subtotal,null as i_total,null as i_vat, null as i_vat_percentage,
                (SELECT ANY_VALUE(vat) from vat_rates vr WHERE LOWER(vr.country) = LOWER(ap.country) and DATE_TRUNC(date, MONTH) = DATE_TRUNC(vr.date, MONTH)) vr_vat_percentage
                FROM `data-warehouse-326816.vivastreet_production.autopilot` ap
                UNION ALL
                SELECT
                country as ps_country,zo.ID as ps_transaction_id,null as ps_id,SAFE_CAST(zo.User_ID as INT64) as ps_user_id,null as ps_status,SAFE_CAST(zo.Grand_Total as NUMERIC) as ps_amount,null as ps_client_confirm,SAFE_CAST(zo.payment_Received_Date as TIMESTAMP)ps_transaction_at,
                null as pso_id, null as pso_classified_id,null as pso_plan_ids, description as pso_plan_types,null as pso_transaction_id,null as pso_provider_id,null as pso_result_code,null as pso_refunded,null as pso_processed_at,null as pso_created_at,null as pso_updated_at,null as pso_amount,null as pso_currency,null as pso_status,null as pso_denialid,null as pso_service_fee,'Bank/Online Transfer' as pso_payment_method,null as pso_boleto_url,
                null as cr_amount, null as cr_email,null as cr_clad_id,null as cr_init_by,null as cr_plan_id,null as cr_user_id,null as cr_zipcode,SAFE_CAST(id as NUMERIC) as cr_order_id,null as cr_platform,null as cr_processed,null as cr_proxy_type,null as cr_renew_type,null as cr_location_id,null as cr_subcat_code,null as cr_partner_name,null as cr_customer_name,null as cr_discount_type,null as cr_posting_status,null as cr_proxy_username,null as cr_clad_revenue_id,null as cr_existing_status,null as cr_individual_type, 'zoho' as cr_transaction_type,null as cr_clad_affiliate_id,null as cr_user_affiliate_id,
                null as i_id,null as i_sent,null as i_email,null as i_items,null as i_country,null as i_created,null as i_category,null as i_order_id,null as i_processor_code,null as i_subtotal,null as i_total,null as i_vat, null as i_vat_percentage,
                (SELECT ANY_VALUE(vat) from vat_rates vr WHERE LOWER(vr.country) = LOWER(zo.country) and DATE_TRUNC(DATE(SAFE_CAST(zo.payment_received_date as TIMESTAMP)), MONTH) = DATE_TRUNC(DATE(vr.date), MONTH)) vr_vat_percentage
                FROM `data-warehouse-326816.vivastreet_production.zoho_orders` zo
                WHERE (payment_method = 'Bank/Online Transfer' and country = 'gb')
                UNION ALL
                SELECT
                country as ps_country,zo.ID as ps_transaction_id,null as ps_id,SAFE_CAST(zo.User_ID as INT64) as ps_user_id,null as ps_status,SAFE_CAST(zo.Grand_Total as NUMERIC) as ps_amount,null as ps_client_confirm,SAFE_CAST(zo.payment_Received_Date as TIMESTAMP)ps_transaction_at,
                null as pso_id, null as pso_classified_id,null as pso_plan_ids, description as pso_plan_types,null as pso_transaction_id,null as pso_provider_id,null as pso_result_code,null as pso_refunded,null as pso_processed_at,null as pso_created_at,null as pso_updated_at,null as pso_amount,null as pso_currency,null as pso_status,null as pso_denialid,null as pso_service_fee,zo.Payment_Method as pso_payment_method,null as pso_boleto_url,
                null as cr_amount, null as cr_email,null as cr_clad_id,null as cr_init_by,null as cr_plan_id,null as cr_user_id,null as cr_zipcode,SAFE_CAST(id as NUMERIC) as cr_order_id,null as cr_platform,null as cr_processed,null as cr_proxy_type,null as cr_renew_type,null as cr_location_id,null as cr_subcat_code,null as cr_partner_name,null as cr_customer_name,null as cr_discount_type,null as cr_posting_status,null as cr_proxy_username,null as cr_clad_revenue_id,null as cr_existing_status,null as cr_individual_type, 'zoho' as cr_transaction_type,null as cr_clad_affiliate_id,null as cr_user_affiliate_id,
                null as i_id,null as i_sent,null as i_email,null as i_items,null as i_country,null as i_created,null as i_category,null as i_order_id,null as i_processor_code,null as i_subtotal,null as i_total,null as i_vat, null as i_vat_percentage,
                (SELECT ANY_VALUE(vat) from vat_rates vr WHERE LOWER(vr.country) = LOWER(zo.country) and DATE_TRUNC(DATE(SAFE_CAST(zo.payment_received_date as TIMESTAMP)), MONTH) = DATE_TRUNC(DATE(vr.date), MONTH)) vr_vat_percentage
                FROM `data-warehouse-326816.vivastreet_production.zoho_orders` zo
                WHERE ((zo.payment_method = 'Prelevement (Direct Debit)' and zo.country = 'fr') or (zo.payment_method = 'Bank/Online Transfer' and zo.country = 'be') or (zo.country != 'gb' AND zo.country != 'fr' AND zo.country != 'be' AND zo.country != 'br'))
                UNION ALL
                SELECT
                'br' as ps_country,d24.Invoice as ps_transaction_id,null as ps_id,null as ps_user_id,d24.Status as ps_status,SAFE_CAST(d24.User_Amount__local_ as NUMERIC) as ps_amount,null as ps_client_confirm,SAFE_CAST(d24.Last_Change_Date as TIMESTAMP)ps_transaction_at,
                null as pso_id, null as pso_classified_id,null as pso_plan_ids, null as pso_plan_types,null as pso_transaction_id,null as pso_provider_id,null as pso_result_code,null as pso_refunded,null as pso_processed_at,null as pso_created_at,null as pso_updated_at,null as pso_amount,null as pso_currency,null as pso_status,null as pso_denialid,SAFE_CAST(d24.Fee__local_ as STRING) as pso_service_fee,d24.Payment_Method as pso_payment_method,null as pso_boleto_url,
                null as cr_amount, null as cr_email,null as cr_clad_id,null as cr_init_by,null as cr_plan_id,null as cr_user_id,null as cr_zipcode,SAFE_CAST(reference as NUMERIC) as cr_order_id,null as cr_platform,null as cr_processed,null as cr_proxy_type,null as cr_renew_type,null as cr_location_id,null as cr_subcat_code,null as cr_partner_name,null as cr_customer_name,null as cr_discount_type,null as cr_posting_status,null as cr_proxy_username,null as cr_clad_revenue_id,null as cr_existing_status,null as cr_individual_type, 'directa24' as cr_transaction_type,null as cr_clad_affiliate_id,null as cr_user_affiliate_id,
                null as i_id,null as i_sent,null as i_email,null as i_items,null as i_country,null as i_created,null as i_category,null as i_order_id,null as i_processor_code,null as i_subtotal,null as i_total,null as i_vat, null as i_vat_percentage,
                (SELECT ANY_VALUE(vat) from vat_rates vr WHERE LOWER(vr.country) = LOWER('br') and DATE_TRUNC(DATE(SAFE_CAST(d24.Last_Change_Date as TIMESTAMP)), MONTH) = DATE_TRUNC(DATE(vr.date), MONTH)) vr_vat_percentage
                FROM `data-warehouse-326816.vivastreet_production.directa24` d24
                UNION ALL
                SELECT
                country as ps_country,zo.ID as ps_transaction_id,null as ps_id,SAFE_CAST(zo.User_ID as INT64) as ps_user_id,null as ps_status,SAFE_CAST(zo.Grand_Total as NUMERIC) as ps_amount,null as ps_client_confirm,SAFE_CAST(zo.payment_Received_Date as TIMESTAMP)ps_transaction_at,
                null as pso_id, null as pso_classified_id,null as pso_plan_ids, description as pso_plan_types,null as pso_transaction_id,null as pso_provider_id,null as pso_result_code,null as pso_refunded,null as pso_processed_at,null as pso_created_at,null as pso_updated_at,null as pso_amount,null as pso_currency,null as pso_status,null as pso_denialid,null as pso_service_fee,zo.Payment_Method as pso_payment_method,null as pso_boleto_url,
                null as cr_amount, null as cr_email,null as cr_clad_id,null as cr_init_by,null as cr_plan_id,null as cr_user_id,null as cr_zipcode,SAFE_CAST(id as NUMERIC) as cr_order_id,null as cr_platform,null as cr_processed,null as cr_proxy_type,null as cr_renew_type,null as cr_location_id,null as cr_subcat_code,null as cr_partner_name,null as cr_customer_name,null as cr_discount_type,null as cr_posting_status,null as cr_proxy_username,null as cr_clad_revenue_id,null as cr_existing_status,null as cr_individual_type, 'zoho' as cr_transaction_type,null as cr_clad_affiliate_id,null as cr_user_affiliate_id,
                null as i_id,null as i_sent,null as i_email,null as i_items,null as i_country,null as i_created,null as i_category,null as i_order_id,null as i_processor_code,null as i_subtotal,null as i_total,null as i_vat, null as i_vat_percentage,
                (SELECT ANY_VALUE(vat) from vat_rates vr WHERE LOWER(vr.country) = LOWER(zo.country) and DATE_TRUNC(DATE(SAFE_CAST(zo.payment_received_date as TIMESTAMP)), MONTH) = DATE_TRUNC(DATE(vr.date), MONTH)) vr_vat_percentage
                FROM `data-warehouse-326816.vivastreet_production.zoho_orders` zo
                WHERE ((zo.country = 'br')) and SAFE_CAST(zo.payment_Received_Date as TIMESTAMP) < '2022-02-01'
            )
        ),

        ads_orders as (
            SELECT
                ps_transaction_id as `Order_ID`,
                cr_order_id as  `Local_Order_Id`,
                COALESCE(pso_processed_at, ps_transaction_at) as `Date`,
                ps_user_id as `User_ID`,
                cl.phone_nbr as `User_Phone_Number`,
                cl.user_created  as `Account_Creation_Date`,
                cr_customer_name as `Name`,
                cr_existing_status as `Existing_New`,
                cr_clad_id as `Classified_ID`,
                ce_teaser_interval  as `Received_Teaser_on_Day`,
                ce_discount_interval  as `Received_Discount_on_Day`,
                cr_posting_status  as `Posting_Modify`,
                c.Category  as `Category`,
                c.Umbrella  as `Umbrella`,
                c.Subcategory  as `Subcategory`,
                pso_payment_method as `Source`,
                p.ps_amount  as `Total`,
                p.ps_amount/(1+vr_vat_percentage)  as `Total_NET`,
                CAST(cr_amount as FLOAT64)-CAST(pso_service_fee as FLOAT64) as `Subtotal`,
                IFNULL(pso_service_fee, '0.0') as `Service_Charge`,            
                vr_vat_percentage as `VAT_PERCENTAGE`,
                pso_plan_types as `Plans`,
                pi.p2v_duration  as `P2V_Length`,
                pi.p2v_price  as `P2v_Price`,
                pi.p2vip_duration  as `P2VIP_Length`,
                pi.p2vip_price  as `P2VIP_Price`,
                pi.premium_price  as `P2P_Price`,
                pi.premium_duration  as `P2P_Length`,
                pi.featured_price  as `FA_Price`,
                pi.featured_duration  as `FA_Length`,
                pi.highlight_price  as `H_Price`,
                pi.highlight_duration  as `H_Length`,
                pi.repost_price  as `P2R_Price`,
                pi.repost_duration  as `P2R_Length`,
                pi.single_repost_price  as `P2R1_Price`,
                pi.single_repost_duration  as `P2R1_Length`,
                pi.p2label_new_price  as `P2L_Price`,
                pi.p2label_new_duration  as `P2L_Length`,
                pi.p2url_duration  as `P2URL_Length`,
                pi.p2url_price  as `P2URL_Price`,
                pi.repost_unlimited_duration  as `P2RU_Length`,
                pi.repost_unlimited_price  as `P2RU_Price`,
                ps_country  as `Country`,
                geo_1 as `Geo1`,
                geo_2 as `Geo2`,
                geo_3 as `Geo3`,
                geo_4 as `Geo4`,
                cr_zipcode as `Post_Code`,
                cr_discount as `Discount`,
                cr_renew_type as `Type`,
                cr_individual_type as `Social_Status`,
                cr_init_by  as `Page_Type`,
                cr_platform as `Platform`,
                cr_transaction_type as `Data_Source`
            FROM payments p
                LEFT JOIN categories c ON
                COALESCE(c3_meta_code, c2_meta_code) = cr_subcat_code AND
                c.country = ps_country
                LEFT JOIN locations l ON
                p.cr_location_id = COALESCE(l4_id,l3_id) AND
                l.country = p.ps_country
                LEFT JOIN classifieds cl ON
                p.cr_clad_id = cl.id and
                cl.country = p.ps_country
                LEFT JOIN plans_orders po ON
                LOWER(po.country) = LOWER(p.ps_country) AND
                po.provider_id = p.pso_provider_id AND
                po.user_id = p.ps_user_id
                LEFT JOIN plans_invoices pi ON
                LOWER(pi.country) = LOWER(p.ps_country) AND
                pi.order_id = CAST(p.pso_id as STRING) AND
                pi.email = p.cr_email
        ),

        source_data as (
            SELECT * FROM (
                SELECT k.*
                FROM ( SELECT ARRAY_AGG(x LIMIT 1)[OFFSET(0)] k  FROM ads_orders x GROUP BY Order_Id, User_ID, country)
            )
            where country is not null
        )
        
        SELECT * FROM source_data