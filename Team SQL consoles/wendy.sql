

select *, p.policy_reference,
       json_extract_path_text(json_args,'business_name',true) as business_name
       from dwh.quotes_policies_mlob qpm
        join nimi_svc_prod.policies p
       on qpm.highest_policy_id = p.policy_id
where lob_policy = 'GL' and qpm. creation_time>='2021-12-01'
and cob_group = 'Auto Service and Repair'

select * from dwh.quotes_policies_mlob qpm where highest_policy_id = '7666236'

select DISTINCT highest_policy_status,highest_status_name
from dwh.quotes_policies_mlob ORDER BY highest_policy_status

select * from dwh.all_activities_table
where final_quote_status = 'decline' limit 10

select * from nimi_svc_prod.application_params
where param_name ilike '10072_rev_perc'

select * from nimi_svc_prod.application_params
where policy_application_id = '6266877'

select *,
       json_extract_path_text(json_args,'1300000_rev_perc',true) as appliances,
       json_extract_path_text(json_args,'1300001_rev_perc',true) as automotive_powersports,
       json_extract_path_text(json_args,'1300002_rev_perc',true) as baby_products,
       json_extract_path_text(json_args,'1300003_rev_perc',true) as books_music_movies_video_games,
       json_extract_path_text(json_args,'1300004_rev_perc',true) as clothing_jewelry_accessories,
       json_extract_path_text(json_args,'1300005_rev_perc',true) as collectibles,
       json_extract_path_text(json_args,'1300006_rev_perc',true) as computers_electronics,
       json_extract_path_text(json_args,'1300007_rev_perc',true) as food_beverage,
       json_extract_path_text(json_args,'1300008_rev_perc',true) as health_beauty,
       json_extract_path_text(json_args,'1300009_rev_perc',true) as home_and_garden,
       json_extract_path_text(json_args,'1300010_rev_perc',true) as office_products,
       json_extract_path_text(json_args,'1300011_rev_perc',true) as other,
       json_extract_path_text(json_args,'1300012_rev_perc',true) as pet_supplies,
       json_extract_path_text(json_args,'1300013_rev_perc',true) as software,
       json_extract_path_text(json_args,'1300014_rev_perc',true) as sports_outdoors,
       json_extract_path_text(json_args,'1300015_rev_perc',true) as toys_games,
       json_extract_path_text(json_args,'businessname',true) as business_name

from dwh.quotes_policies_mlob
where cob in ('E-Commerce','Retail Stores') and lob_policy = 'GL' and creation_time >= '2022-1-25'

select *
from dwh.quotes_policies_mlob
where cob = 'E-Commerce' and highest_policy_status >=3 and lob_policy = 'GL'

select *,
       json_extract_path_text(json_args,'retail_product_category_appliances',true) as appliances,
       json_extract_path_text(json_args,'retail_product_category_automotive_powersports',true) as automotive_powersports,
       json_extract_path_text(json_args,'retail_product_category_baby_products',true) as baby_products,
       json_extract_path_text(json_args,'retail_product_category_books_music_movies_video_games',true) as books_music_movies_video_games,
       json_extract_path_text(json_args,'retail_product_category_clothing_jewelry_accessories',true) as clothing_jewelry_accessories,
       json_extract_path_text(json_args,'retail_product_category_collectibles',true) as collectibles,
       json_extract_path_text(json_args,'retail_product_category_computers_electronics',true) as computers_electronics,
       json_extract_path_text(json_args,'retail_product_category_food_beverage',true) as food_beverage,
       json_extract_path_text(json_args,'retail_product_category_health_beauty',true) as health_beauty,
       json_extract_path_text(json_args,'retail_product_category_home_and_garden',true) as home_and_garden,
       json_extract_path_text(json_args,'retail_product_category_office_products',true) as office_products,
       json_extract_path_text(json_args,'retail_product_category_other',true) as other,
       json_extract_path_text(json_args,'retail_product_category_pet_supplies',true) as pet_supplies,
       json_extract_path_text(json_args,'retail_product_category_software',true) as software,
       json_extract_path_text(json_args,'retail_product_category_sports_outdoors',true) as sports_outdoors,
       json_extract_path_text(json_args,'retail_product_category_toys_games',true) as toys_games

from dwh.quotes_policies_mlob
where cob = 'E-Commerce' and highest_policy_status =4 and lob_policy = 'GL' and creation_time >= '2021-8-10' and creation_time <= '2022-1-17'

select distinct funnelphase from dwh.all_activities_table

select * from underwriting_svc_prod.lob_applications
where lob = 'GL' limit 100

select * from underwriting_svc_prod.data_collection_responses limit 10

select *,json_extract_path_text(json_args,'businessname',true) as business_name
from dwh.quotes_policies_mlob
where creation_time >= '2021-12-1' and cob_group = 'Auto Service and Repair' and lob_policy = 'GL' and highest_policy_status >= 3

select count(json_extract_path_text(json_args,'businessname',true))
from dwh.quotes_policies_mlob
where creation_time >= '2021-12-1' and cob_group = 'Auto Service and Repair' and lob_policy = 'GL' and highest_policy_status >= 3

select distinct cob
from dwh.quotes_policies_mlob
where cob_group = 'Retail' and lob_policy = 'GL'

select related_business_id, highest_policy_id, distribution_channel,cob, cob_group, highest_status_name, highest_yearly_premium,revenue_in_12_months,
       json_extract_path_text(json_args,'vehicle_services_coverage_hired_non_owned_auto',true) as HNOA,
       json_extract_path_text(json_args,'auto_service_repair_faulty_work_professional_liability',true) as Faulty,
       json_extract_path_text(json_args,'hnoa_deliver_customer_vehicles',true) as deliver_customer_vehicles,
       json_extract_path_text(json_args,'node_driving_experience',true) as driving_experience,
       json_extract_path_text(json_args,'employees_valid_license',true) as employees_valid_license,
       json_extract_path_text(json_args,'bundle_name',true) as bundle_name,
       json_extract_path_text(json_args,'businessname',true) as business_name
       from dwh.quotes_policies_mlob qpm
where lob_policy = 'GL' and qpm. creation_time>='2021-12-01'
and cob_group = 'Auto Service and Repair' and highest_policy_status >= 3

-- get offer_id from all declines
select * from underwriting_svc_prod.offers
where lob = 'GL' and execution_status ilike '%DECLINE%' limit 100

select offer_id from dwh.quotes_policies_mlob where lob_policy = 'GL' and COB ilike '%commerce%'


select * from dwh.all_activities_table where failurereason = 'You manufacture or private label goods in restricted product categories in excess of 1,500 units per year.'
                                         and json_extract_path_text(interaction_data,'offer_id',true) = 's0pKGKzcbnwRQ28a'

WITH t1 as (select distinct tracking_id, failurereason
from dwh.all_activities_table
    where failurereason in ('You provide roadside assistance.')),
t2 as (select * from dwh.all_activities_table
    where data_domain = 'App User Interactions'
    and funnelphase = 'Answered Question Sequence'
    and question_name in ('businessname'))
select * from t1 left join t2
on t1.tracking_id = t2.tracking_id


select * from dwh.all_activities_table
where tracking_id = '987a271941d3684742995ebc9b5e85d3'


/*    and data_domain = 'App User Interactions'
    and funnelphase = 'Answered Question Sequence'
    and question_name = 'roadside_assistance_exposure' */
ORDER BY eventtime asc

select distinct *
from dwh.all_activities_table
    where failurereason in ('You provide roadside assistance.')

select *,
       json_extract_path_text(json_args,'used_car_sales',true) as used_car_sales,
       json_extract_path_text(json_args,'businessname',true) as business_name
      from dwh.quotes_policies_mlob qpm
where lob_policy = 'GL' and qpm.creation_time>='2021-12-01'
and cob_group = 'Auto Service and Repair'

select * from dwh.all_claims_details claims
left join (select distinct business_id,
       json_extract_path_text(json_args,'business_name',true) as business_name
      from dwh.quotes_policies_mlob) qpm
on qpm.business_id = claims.business_id
where lob = 'WC' and cob_name = 'Restaurant'

select *,
       json_extract_path_text(json_args,'business_name',true) as business_name,
       json_extract_path_text(json_args,'gl_asr_activities_mobile_mechanic_services',true) as mobile_mechanic
      from dwh.quotes_policies_mlob qpm
where lob_policy = 'GL'
and cob_group = 'Auto Service and Repair'

select *,
       json_extract_path_text(json_args,'businessname',true) as business_name
      from dwh.quotes_policies_mlob qpm
where lob_policy = 'GL' and qpm.creation_time>='2021-12-01'
and cob = 'Handyperson' and highest_policy_status >= 3

WITH total_unique_quote AS
(select COUNT (DISTINCT related_business_id) AS num_quotes
      from dwh.quotes_policies_mlob qpm
where lob_policy = 'GL' and qpm.creation_time>='2021-12-01'
and cob = 'Handyperson')

 ,policy_sold AS (select COUNT (DISTINCT related_business_id) AS num_policies
      from dwh.quotes_policies_mlob qpm
where lob_policy = 'GL' and qpm.creation_time>='2021-12-01'
and cob = 'Handyperson' and highest_policy_status >= 3)

SELECT * FROM
(Select *,ROW_NUMBER() OVER (ORDER BY num_policies) as RowNumber from policy_sold) as t1
    FULL OUTER JOIN
    (Select *,ROW_NUMBER() OVER (ORDER BY num_quotes) as RowNumber FROM total_unique_quote) as t2
ON t1.RowNumber = t2.RowNumber

select claim_id, count(distinct loss_cause_type_name)
from dwh.all_claims_details
group by 1
having count(distinct loss_cause_type_name) > 1

select * from s3_operational.rating_interface where lob = 'GL' and cob = 'tree_services'


-- AS&R existing faulty work premium
WITH t1 AS (select pro_plus_quote_job_id,pro_plus_status_name,cob,revenue_in_12_months,
       json_extract_path_text(json_args,'businessname',true) as business_name
       from dwh.quotes_policies_mlob
where lob_policy = 'GL' and cob_group = 'Auto Service and Repair'),
     t2 AS
(select *,
       json_extract_path_text(json_extract_path_text(json_extract_path_text(json_extract_path_text(rating_result,'ratingGroupedByCategories',true),'BASE_PREMIUM_GROUPED_BY_COB_ISO_CODE',true),'10367',true),'premOpsIlf',true) as iso10367_premOpsIlf,
       json_extract_path_text(json_extract_path_text(json_extract_path_text(json_extract_path_text(rating_result,'ratingGroupedByCategories',true),'BASE_PREMIUM_GROUPED_BY_COB_ISO_CODE',true),'10367',true),'prodCOpsIlf',true) as iso10367_prodCOpsIlf,
       json_extract_path_text(json_extract_path_text(json_extract_path_text(json_extract_path_text(rating_result,'ratingGroupedByCategories',true),'BASE_PREMIUM_GROUPED_BY_COB_ISO_CODE',true),'10367',true),'premOps-share-base_premium',true) as iso10367_premOps_share_base_premium,
       json_extract_path_text(json_extract_path_text(json_extract_path_text(json_extract_path_text(rating_result,'ratingGroupedByCategories',true),'BASE_PREMIUM_GROUPED_BY_COB_ISO_CODE',true),'10367',true),'prodCOps-share-base_premium',true) as iso10367_prodCOps_share_base_premium,
       json_extract_path_text(json_extract_path_text(json_extract_path_text(json_extract_path_text(rating_result,'ratingGroupedByCategories',true),'BASE_PREMIUM_GROUPED_BY_COB_ISO_CODE',true),'18616',true),'premOpsIlf',true) as iso18616_premOpsIlf,
       json_extract_path_text(json_extract_path_text(json_extract_path_text(json_extract_path_text(rating_result,'ratingGroupedByCategories',true),'BASE_PREMIUM_GROUPED_BY_COB_ISO_CODE',true),'18616',true),'prodCOpsIlf',true) as iso18616_prodCOpsIlf,
       json_extract_path_text(json_extract_path_text(json_extract_path_text(json_extract_path_text(rating_result,'ratingGroupedByCategories',true),'BASE_PREMIUM_GROUPED_BY_COB_ISO_CODE',true),'18616',true),'premOps-share-base_premium',true) as iso18616_premOps_share_base_premium,
       json_extract_path_text(json_extract_path_text(json_extract_path_text(json_extract_path_text(rating_result,'ratingGroupedByCategories',true),'BASE_PREMIUM_GROUPED_BY_COB_ISO_CODE',true),'18616',true),'prodCOps-share-base_premium',true) as iso18616_prodCOps_share_base_premium,
       json_extract_path_text(json_extract_path_text(json_extract_path_text(json_extract_path_text(rating_result,'ratingGroupedByCategories',true),'BASE_PREMIUM_GROUPED_BY_COB_ISO_CODE',true),'10368',true),'premOpsIlf',true) as iso10368_premOpsIlf,
       json_extract_path_text(json_extract_path_text(json_extract_path_text(json_extract_path_text(rating_result,'ratingGroupedByCategories',true),'BASE_PREMIUM_GROUPED_BY_COB_ISO_CODE',true),'10368',true),'prodCOpsIlf',true) as iso10368_prodCOpsIlf,
       json_extract_path_text(json_extract_path_text(json_extract_path_text(json_extract_path_text(rating_result,'ratingGroupedByCategories',true),'BASE_PREMIUM_GROUPED_BY_COB_ISO_CODE',true),'10368',true),'premOps-share-base_premium',true) as iso10368_premOps_share_base_premium,
       json_extract_path_text(json_extract_path_text(json_extract_path_text(json_extract_path_text(rating_result,'ratingGroupedByCategories',true),'BASE_PREMIUM_GROUPED_BY_COB_ISO_CODE',true),'10368',true),'prodCOps-share-base_premium',true) as iso10368_prodCOps_share_base_premium,
       json_extract_path_text(json_extract_path_text(json_extract_path_text(json_extract_path_text(rating_result,'ratingGroupedByCategories',true),'BASE_PREMIUM_GROUPED_BY_COB_ISO_CODE',true),'10072',true),'premOpsIlf',true) as iso10072_premOpsIlf,
       json_extract_path_text(json_extract_path_text(json_extract_path_text(json_extract_path_text(rating_result,'ratingGroupedByCategories',true),'BASE_PREMIUM_GROUPED_BY_COB_ISO_CODE',true),'10072',true),'prodCOpsIlf',true) as iso10072_prodCOpsIlf,
       json_extract_path_text(json_extract_path_text(json_extract_path_text(json_extract_path_text(rating_result,'ratingGroupedByCategories',true),'BASE_PREMIUM_GROUPED_BY_COB_ISO_CODE',true),'10072',true),'premOps-share-base_premium',true) as iso10072_premOps_share_base_premium,
       json_extract_path_text(json_extract_path_text(json_extract_path_text(json_extract_path_text(rating_result,'ratingGroupedByCategories',true),'BASE_PREMIUM_GROUPED_BY_COB_ISO_CODE',true),'10072',true),'prodCOps-share-base_premium',true) as iso10072_prodCOps_share_base_premium,
       json_extract_path_text(json_extract_path_text(json_extract_path_text(json_extract_path_text(rating_result,'ratingGroupedByCategories',true),'BASE_PREMIUM_GROUPED_BY_COB_ISO_CODE',true),'10073',true),'premOpsIlf',true) as iso10073_premOpsIlf,
       json_extract_path_text(json_extract_path_text(json_extract_path_text(json_extract_path_text(rating_result,'ratingGroupedByCategories',true),'BASE_PREMIUM_GROUPED_BY_COB_ISO_CODE',true),'10073',true),'prodCOpsIlf',true) as iso10073_prodCOpsIlf,
       json_extract_path_text(json_extract_path_text(json_extract_path_text(json_extract_path_text(rating_result,'ratingGroupedByCategories',true),'BASE_PREMIUM_GROUPED_BY_COB_ISO_CODE',true),'10073',true),'premOps-share-base_premium',true) as iso10073_premOps_share_base_premium,
       json_extract_path_text(json_extract_path_text(json_extract_path_text(json_extract_path_text(rating_result,'ratingGroupedByCategories',true),'BASE_PREMIUM_GROUPED_BY_COB_ISO_CODE',true),'10073',true),'prodCOps-share-base_premium',true) as iso10073_prodCOps_share_base_premium
       from s3_operational.rating_svc_prod_calculations where lob = 'GL')
select * from t1 join t2 ON t1.pro_plus_quote_job_id=t2.job_id

WITH t1 AS (select pro_plus_quote_job_id,pro_plus_status_name,cob,revenue_in_12_months,
       json_extract_path_text(json_args,'businessname',true) as business_name
       from dwh.quotes_policies_mlob
where lob_policy = 'GL' and cob_group = 'Auto Service and Repair'),
     t2 AS
(select *,
       json_extract_path_text((regexp_replace(json_extract_path_text(rating_result,'debugInfo',true),'rate bearing endorsement premium breakdown',true),'\\\\.',''),'PROFESSIONAL_LIABILITY_CONSULTING',true) as PL_premium

       from s3_operational.rating_svc_prod_calculations where lob = 'GL')
select * from t1 join t2 ON t1.pro_plus_quote_job_id=t2.job_id

/*    Extract premium data */
WITH t1 AS (select pro_plus_quote_job_id,pro_plus_status_name,cob,revenue_in_12_months,
       json_extract_path_text(json_args,'businessname',true) as business_name
       from dwh.quotes_policies_mlob
where lob_policy = 'GL' and cob_group = 'Auto Service and Repair'),
     t2 AS
(select *,
           json_extract_path_text(
               regexp_replace(json_extract_path_text(
               json_extract_path_text(rating_result,'debugInfo',true),
           'rate bearing endorsement premium breakdown',true),'\\\\'),
               'PROFESSIONAL_LIABILITY_CONSULTING',true) as PL_premium

       from s3_operational.rating_svc_prod_calculations where lob = 'GL')
select * from t1 join t2 ON t1.pro_plus_quote_job_id=t2.job_id

with gaap as (

    select last_day(date) as date,
        policy_id,
         sum(dollar_amount) as earned_premium
    from reporting.gaap_snapshots_ASL
    where trans in ('monthly earned premium', 'monthly earned premium endorsement')
    and test_accounts != 'test' and lob = 'GL'
    and carrier not in (2,3,5)
    group by 1,2
)


    select start_year,
           report_month,
           months_since_start_year,
           policy_id,
           case when months_since_start_year > 12 then 0 else gaap.earned_premium end as earned_premium
    from db_data_science.loss_ratio_date_list_v4 dt_ls
    left join gaap
    on gaap.date = dt_ls.report_month
    order by report_month, start_year




select * from dwh.quotes_policies_mlob where lob_policy = 'PL' limit 100


-- large reserve change investigation
select date,claim_id,exposure_id,loss_paid_today,loss_paid_total_yesterday,loss_reserve_total,loss_reserve_total_yesterday,loss_paid_today-loss_paid_total_yesterday as loss_paid_change,
       loss_reserve_total-loss_reserve_total_yesterday as loss_reserve_change,
       loss_paid_today-loss_paid_total_yesterday+loss_reserve_total-loss_reserve_total_yesterday as total_change
       from dwh.all_claims_financial_changes_ds where total_change > 10000 and lob = 'GL' and marketing_cob_group NOT IN ('Cleaning', 'Construction', 'Artisan contractor')
       order by total_change desc limit 100

select * ,loss_paid_today-loss_paid_total_yesterday as loss_paid_change,
       loss_reserve_total-loss_reserve_total_yesterday as loss_reserve_change from dwh.all_claims_financial_changes_ds where claim_id = 'MdEKG9xp2V9fp0TB' AND exposure_id = 'cNsCYIf1hYjYCAHd' order by date ASC



-- amazon risk data

with review_data as (
        select *,
               split_part(json_extract_path_text(raw_response,'seller_details', 'seller_name',true),'|',1) as seller_name,
               split_part(json_extract_path_text(raw_response,'seller_details', 'seller_description',true),'|',1) as seller_description,
               split_part(json_extract_path_text(raw_response,'seller_details', 'review_summary','review_stars',true),'|',1) as review_stars,
               split_part(json_extract_path_text(raw_response,'seller_details', 'review_summary','review_positivity_pct',true),'|',1) as review_positivity_pct,
               json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(split_part(json_extract_path_text(raw_response,'seller_details', 'review_detail','review_snapshots',true),'|',1),0),'days_30',true) as positive_day_30,
               json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(split_part(json_extract_path_text(raw_response,'seller_details', 'review_detail','review_snapshots',true),'|',1),0),'days_90',true) as positive_day_90,
               json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(split_part(json_extract_path_text(raw_response,'seller_details', 'review_detail','review_snapshots',true),'|',1),0),'days_365',true) as positive_day_365,
               json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(split_part(json_extract_path_text(raw_response,'seller_details', 'review_detail','review_snapshots',true),'|',1),1),'days_30',true) as neutral_day_30,
               json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(split_part(json_extract_path_text(raw_response,'seller_details', 'review_detail','review_snapshots',true),'|',1),1),'days_90',true) as neutral_day_90,
               json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(split_part(json_extract_path_text(raw_response,'seller_details', 'review_detail','review_snapshots',true),'|',1),1),'days_365',true) as neutral_day_365,
               json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(split_part(json_extract_path_text(raw_response,'seller_details', 'review_detail','review_snapshots',true),'|',1),2),'days_30',true) as negative_day_30,
               json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(split_part(json_extract_path_text(raw_response,'seller_details', 'review_detail','review_snapshots',true),'|',1),2),'days_90',true) as negative_day_90,
               json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(split_part(json_extract_path_text(raw_response,'seller_details', 'review_detail','review_snapshots',true),'|',1),2),'days_365',true) as negative_day_365,
               json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(split_part(json_extract_path_text(raw_response,'seller_details', 'review_detail','review_snapshots',true),'|',1),3),'days_30',true) as day_30_count,
               json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(split_part(json_extract_path_text(raw_response,'seller_details', 'review_detail','review_snapshots',true),'|',1),3),'days_90',true) as day_90_count,
               json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(split_part(json_extract_path_text(raw_response,'seller_details', 'review_detail','review_snapshots',true),'|',1),3),'days_365',true) as day_365_count

        from riskmgmt_svc_prod.amazon_seller_result),

     qpm as (select *,json_extract_path_text(json_args,'lob_app_json','retail_market_amazon_seller_id',true) as amazon_seller_id from dwh.quotes_policies_mlob
where cob = 'E-Commerce' and lob_policy = 'GL')

select * from review_data left join qpm on review_data.seller_id=qpm.amazon_seller_id limit 10000



select * from portfolio_svc_prod.permitted_cobs_states_lobs
--     inner join portfolio_svc_prod.cobs using(cob_id) where permitted = 1
--     inner join dwh.sources_test_cobs using(cob_name)

select * from portfolio_svc_prod.permitted_cobs_states_lobs
    left join dwh.sources_test_cobs on portfolio_svc_prod.permitted_cobs_states_lobs.cob_id=dwh.sources_test_cobs.cob_id
where permitted = 1 and marketing_cob_group in ('Auto Service and Repair')

select * from dwh.sources_test_cobs where cob_name

select * from portfolio_svc_prod.permitted_cobs_states_lobs
    left join dwh.sources_test_cobs on portfolio_svc_prod.permitted_cobs_states_lobs.cob_id=dwh.sources_test_cobs.cob_id
where cob_name = 'Environmental Science and Protection Technicians, Including Health' and state_code= 'GA'



select * from dwh.underwriting_quotes_data uqd
        left join dwh.sources_test_cobs stc on uqd.cob = stc.cob_name limit 100





SELECT * FROM information_schema.tables WHERE table_schema = 'riskmgmt_svc_prod'

select *,json_extract_path_text(json_args, 'lob_app_json','square_footage', true) from dwh.quotes_policies_mlob where lob_policy = 'CP' and cob_group = 'Auto Service and Repair'

select * from dwh.all_claims_financial_changes_ds where date is null


select *,
       json_extract_path_text(json_args,'business_name',true) as business_name
      from dwh.quotes_policies_mlob
where cob = 'Poke'

select * from dwh.quotes_policies_mlob where business_id = 'e44fd58bc4057f94d1c287d8ffbce903'

select *,json_extract_path_text(json_args,'business_name',true) as business_name from dwh.quotes_policies_mlob
where highest_policy_status >= 3 and business_id IN
(select business_id from dwh.quotes_policies_mlob where lob_policy = 'PL' and highest_policy_status >= 3 and creation_time>='2021-12-01')

select qpm.agent_id, a.agent_name, a.current_agencytype, a.agency_name, a.agent_email_address, a.agency_aggregator_name
from dwh.quotes_policies_mlob qpm
left join dwh.v_agents a on qpm.agent_id = a.agent_id
left join nimi_svc_prod.policies p on qpm.highest_policy_id = p.policy_id
left join db_data_science.v_all_agents_policies_v2 DS on qpm.business_id = DS.business_id

-- all data
With fcra as (
    select distinct business_id,
           last_value(score) over (partition by business_id order by creation_time rows between unbounded preceding and unbounded following ) as fcra_score
           from riskmgmt_svc_prod.risk_score_result
    order by business_id desc),

     raven_raw as (select creation_time,json_extract_path_text(event_json,'business_id',true) as business_id,
                json_extract_path_text(response_json,'score',true) as score,
                json_extract_path_text(response_json,'bin_num',true) as bin_num
        from prod.risk_model_svc_prod.gl_non_construction_schedule_rate_requests),
      raven as (
    select distinct business_id,
     last_value(score) over (partition by business_id order by creation_time rows between unbounded preceding and unbounded following ) as raven_score,
     last_value(bin_num) over (partition by business_id order by creation_time rows between unbounded preceding and unbounded following ) as raven_bin
        from raven_raw
     order by business_id desc),
     t1 as (select agent_id agentid,agent_name, current_agencytype, agency_name, agent_email_address, agency_aggregator_name from dwh.v_agents),
     t2 as (select *,json_extract_path_text(json_args,'business_name',true) as business_name,
                     json_extract_path_text(json_args,'years_in_business_num',true) as YIB,
                     json_extract_path_text(json_args, 'lob_app_json','liquor_sales_exposure', true) as liquor_sales_exposure
     from dwh.quotes_policies_mlob qpm left join t1 on t1.agentid = qpm.agent_id)
select * from t2
left join fcra on fcra.business_id=t2.business_id
left join raven on raven.business_id=t2.business_id
where cob = 'Restaurant' and highest_policy_status >= 3 and creation_time<='2022-10-31' and creation_time>='2021-01-01' and offer_flow_type in ('APPLICATION','RENEWAL','CANCEL_REWRITE')

-- current period summarized
With t1 as (select agent_id agentid,agent_name, current_agencytype, agency_name, agent_email_address, agency_aggregator_name from dwh.v_agents),
     t2 as (select * from dwh.quotes_policies_mlob qpm left join t1 on t1.agentid = qpm.agent_id),

     t3 as (select agent_id, agent_name,agent_email_address,agency_aggregator_name,count(business_id) as res_policy,sum(highest_yearly_premium) as total_res_premium from t2
     where cob = 'Restaurant' and highest_policy_status >= 3 and creation_time<='2022-10-31' and creation_time>='2021-11-01' and agent_name IS NOT NULL and offer_flow_type in ('APPLICATION','RENEWAL','CANCEL_REWRITE') and current_agencytype not in ('Wholesaler')
     group by 1,2,3,4
     order by total_res_premium desc limit 20),

     t4 as (select agent_id, count(business_id) as total_policy,sum(highest_yearly_premium) as total_premium from t2
     where highest_policy_status >= 3 and creation_time<='2022-10-31' and creation_time>='2021-11-01' and agent_name IS NOT NULL and offer_flow_type in ('APPLICATION','RENEWAL','CANCEL_REWRITE') and current_agencytype not in ('Wholesaler')
     group by 1)

--      t5 as (select agent_id, count(business_id) as prev_res_policy,sum(highest_yearly_premium) as prev_res_premium from t2
--      where cob = 'Restaurant' and highest_policy_status >= 3 and creation_time<='2021-10-31' and creation_time>='2020-11-01' and agent_name IS NOT NULL
--      group by 1),
--
--      t6 as (select agent_id, count(business_id) as total_prev_policy,sum(highest_yearly_premium) as total_prev_premium from t2
--      where highest_policy_status >= 3 and creation_time<='2021-10-31' and creation_time>='2020-11-01' and agent_name IS NOT NULL
--      group by 1)

Select * from t3 left join t4 on t3.agent_id=t4.agent_id
-- left join t5 on t3.agent_id=t5.agent_id
-- left join t6 on t3.agent_id=t6.agent_id
order by total_res_premium desc

With t1 as (select agent_id agentid,agent_name, current_agencytype, agency_name, agent_email_address, agency_aggregator_name from dwh.v_agents),
     t2 as (select * from dwh.quotes_policies_mlob qpm left join t1 on t1.agentid = qpm.agent_id)
select * from t2 where agent_id = 'RElczpbzcB29JrEd'



With t1 as (select agent_id agentid,agent_name, current_agencytype, agency_name, agent_email_address, agency_aggregator_name,state_code from dwh.v_agents),
     t2 as (select * from dwh.quotes_policies_mlob qpm left join t1 on t1.agentid = qpm.agent_id),

     t3 as (select agent_id, agent_name,agent_email_address,agency_aggregator_name,count(business_id) as res_policy,sum(highest_yearly_premium) as total_res_premium from t2
     where cob = 'Restaurant' and highest_policy_status >= 3 and creation_time<='2022-10-31' and creation_time>='2021-11-01' and agent_name IS NOT NULL and offer_flow_type in ('APPLICATION','RENEWAL','CANCEL_REWRITE') and current_agencytype not in ('Wholesaler')
     group by 1,2,3,4
     order by total_res_premium desc limit 20)

-- select * from t2 where highest_policy_status >= 3 and creation_time<='2022-10-31' and creation_time>='2021-11-01' and agent_name IS NOT NULL
-- select * from t3 left join t2 on t3.agent_id = t2.agent_id where highest_policy_status >= 3 and creation_time<='2022-10-31' and creation_time>='2021-11-01' and and offer_flow_type in ('APPLICATION','RENEWAL','CANCEL_REWRITE')
select * from t3

select * from dwh.quotes_policies_mlob where cob = 'Restaurant' and highest_policy_status >= 3 and creation_time<='2022-10-31' and creation_time>='2021-11-01'

SELECT
    PERIODS.date AS month
    , COUNT(DISTINCT EXPOSURES.exposure_id) AS count_exposures
    , SUM(EXPOSURES.loss_paid_total)
        + SUM(EXPOSURES.expense_ao_paid_total)
        + SUM(EXPOSURES.expense_dcc_paid_total)
        + SUM(EXPOSURES.loss_reserve_total)
        + SUM(EXPOSURES.expense_ao_reserve_total)
        + SUM(EXPOSURES.expense_dcc_reserve_total)
        AS sum_total_severity
    , sum_total_severity / count_exposures AS avg_severity
FROM
    sl_prod_dwh.dim_claim_exposure EXPOSURES
JOIN
    bi_workspace.periods PERIODS
    ON PERIODS.date BETWEEN EXPOSURES.effective_time AND EXPOSURES.end_effective_time
    AND PERIODS.monthfirstday = PERIODS.date
    AND PERIODS.date BETWEEN '2018-01-01' AND CURRENT_DATE
GROUP BY
    1
ORDER BY
    1 DESC



With t1 as (select agent_id agentid,agent_name, current_agencytype, agency_name, agent_email_address, agency_aggregator_name,state_code from dwh.v_agents),
     t2 as (select * from dwh.quotes_policies_mlob qpm left join t1 on t1.agentid = qpm.agent_id),

     t3 as (select agent_id, agent_name,agency_aggregator_name,count(business_id) as res_policy,sum(highest_yearly_premium) as total_res_premium from t2
     where cob = 'Restaurant' and highest_policy_status >= 3 and creation_time<='2022-10-31' and creation_time>='2021-11-01' and agent_name IS NOT NULL
     group by 1,2,3
     order by total_res_premium desc limit 15)

select * from t2 where cob = 'Restaurant' and highest_policy_status >= 3 and creation_time<='2022-10-31' and creation_time>='2020-11-01'

With t1 as (select agent_id agentid,agent_name, current_agencytype, agency_name, agent_email_address, agency_aggregator_name,state_code from dwh.v_agents),
     t2 as (select * from dwh.quotes_policies_mlob qpm left join t1 on t1.agentid = qpm.agent_id)

     select agent_id, agent_name,agency_aggregator_name,count(business_id) as res_policy,sum(highest_yearly_premium) as total_res_premium from t2
     where cob = 'Restaurant' and highest_policy_status >= 3 and creation_time<='2022-10-31' and creation_time>='2021-11-01' and agent_name IS NOT NULL and offer_flow_type in ('APPLICATION','RENEWAL','CANCEL_REWRITE')
     group by 1,2,3
     order by total_res_premium desc limit 15

select * from dwh.quotes_policies_mlob qpm where offer_flow_type in ('APPLICATION','RENEWAL','CANCEL_REWRITE')
select * from underwriting_svc_prod.lob_applications where lob_application_id in ('ON1B9B5O0CX2xQ70', 'fXRpbZTxMVMAUkE6')


select * from underwriting_svc_prod.offers where lob_application_id in ('ON1B9B5O0CX2xQ70', 'fXRpbZTxMVMAUkE6')

with FL_Exp as (
select qpm.offer_id, highest_status_name, state, highest_policy_id,highest_policy_reference, cob,
policy_start_date::date, policy_end_date::date,  dateadd(‘Year’,1,qpm.policy_start_date)::date as Exp_Date,
highest_yearly_premium, o.execution_status,
case when pq.original_policy_id is null then 1 else 0 end as NoRenewalOffer
from dwh.quotes_policies_mlob qpm
left join renewal_svc_prod.policies_quotes pq on qpm.highest_policy_id = pq.original_policy_id
left join underwriting_svc_prod.offers o on pq.quote_request_id = o.lob_application_id
where highest_status_name = ‘Active’ and highest_policy_reference is not null
and policy_end_date::date >= ‘2022-9-28’ and policy_end_date::date <= current_date + 55
order by exp_date desc
),
FL_NR as (
select * from FL_EXP where NoRenewalOffer = 1 or execution_status = ‘DECLINE’
)


select *,json_extract_path_text(json_args,'lob_app_json','retail_market_amazon_seller_id',true) as amazon_seller_id from dwh.quotes_policies_mlob
where cob = 'E-Commerce' and lob_policy = 'GL'

select *, p.policy_reference,
       json_extract_path_text(json_args,'business_name',true) as business_name
       from dwh.quotes_policies_mlob qpm
        join nimi_svc_prod.policies p
       on qpm.highest_policy_id = p.policy_id
where lob_policy = 'GL' and qpm. creation_time>='2022-10-01' and cob = 'Restaurant 'limit 50


select * from dwh.quotes_policies_mlob where highest_policy_id = '6782334'

select * from dwh.all_activities_table
    where data_domain = 'App User Interactions'
    and funnelphase = 'Answered Question Sequence'
    and question_name like '%same_mailing_and_business_address%'


select * from dwh.all_activities_table where tracking_id = '007950bedac933df1595dd6dbf08ce57'

select * from s3_operational.rating_svc_prod_calculations where lob = 'GL' limit 10

select *,json_extract_path_text(json_args,'lob_app_json','liquor_sales_yes_no',true) as liquor_sales_yes_no,
       json_extract_path_text(json_args,'lob_app_json','liquor_sales_exposure',true) as liquor_sales_exposure,
       json_extract_path_text(json_args,'lob_app_json','restaurant_type',true) as restaurant_type,
       json_extract_path_text(json_args,'business_name',true) as business_name
       from dwh.quotes_policies_mlob where cob in ('Restaurant','Coffee Shop','Bakery')  and highest_policy_status >=3 and creation_time >= '2022-01-01'

select
    business_id
from dwh.quotes_policies_mlob
where
--     highest_policy_status >=4 and
      offer_flow_type in ('APPLICATION','RENEWAL','CANCEL_REWRITE') and
      creation_time >= '2022-12-20' and
      highest_policy_occurence_limit > 1000000 and
      state in ('AL','AZ','CO','FL','MI','MN','NV','SC','TN','TX','UT') and
      cob in ('Restaurant','E-Commerce','Retail Stores','Grocery Store','Clothing Store','Electronics Store','Florist','Jewelry Store','Sporting Goods Retailer','Tailors, Dressmakers, and Custom Sewers','Nurseries and Gardening Shop','Candle Store','Pet Stores','Paint Stores','Flea Markets','Arts and Crafts Store','Eyewear and Optician Store','Hardware Store','Discount Store','Pawn Shop','Hobby Shop','Beach Equipment Rentals','Furniture Rental','Packing Supplies Store','Horse Equipment Shop','Demonstrators and Product Promoters','Fabric Store','Lighting Store','Luggage Store','Bike Rentals','Bike Shop','Bookstore','Home and Garden Retailer','Newspaper and Magazine Store','Department Stores','Furniture Store','Wholesalers')


-- food&bev large reserve change
select date,business_id,claim_id,exposure_id,loss_paid_today,loss_paid_total_yesterday,loss_reserve_total,loss_reserve_total_yesterday,loss_paid_today-loss_paid_total_yesterday as loss_paid_change,
       loss_reserve_total-loss_reserve_total_yesterday as loss_reserve_change,
       loss_paid_today-loss_paid_total_yesterday+loss_reserve_total-loss_reserve_total_yesterday as total_change,
       abs(loss_paid_today-loss_paid_total_yesterday+loss_reserve_total-loss_reserve_total_yesterday) as abs_total_change
       from dwh.all_claims_financial_changes_ds where abs_total_change > 10000 and lob = 'CP' and marketing_cob_group = 'Food & beverage' and date >= '2022-12-01' and date < '2023-01-01'
       order by abs_total_change desc limit 100

select * ,loss_paid_today-loss_paid_total_yesterday as loss_paid_change,
       loss_reserve_total-loss_reserve_total_yesterday as loss_reserve_change from dwh.all_claims_financial_changes_ds where claim_id = 'vXMMlidxmdwiYpVR' order by date DESC

select * from dwh.all_claims_financial_changes_ds where lob = 'CP' and marketing_cob_group = 'Food & beverage' and date IN ('2022-12-31','2022-11-30')

select business_id,lob_policy,distribution_channel from dwh.quotes_policies_mlob where cob = 'Restaurant'


select
    business_id, policy_start_date, highest_status_name, cob, highest_policy_occurence_limit, highest_policy_aggregate_limit
from dwh.quotes_policies_mlob
where --highest_policy_status >=4 and
      offer_flow_type in ('APPLICATION','RENEWAL','CANCEL_REWRITE') and
      creation_time >= '2022-12-20' and
      (highest_policy_aggregate_limit > 2000000 or highest_policy_occurence_limit > 1000000) and
      state in ('AL','AZ','CO','FL','MI','MN','NV','SC','TN','TX','UT') and
      (cob = 'Restaurant' or cob_group = 'Retail')

select *,json_extract_path_text(json_args,'lob_app_json','liquor_sales_yes_no',true) as liquor_sales_yes_no,
       json_extract_path_text(json_args,'lob_app_json','liquor_sales_exposure',true) as liquor_sales_exposure,
       json_extract_path_text(json_args,'lob_app_json','restaurant_type',true) as restaurant_type,
       json_extract_path_text(json_args,'lob_app_json','liquor_risk_byob_alcohol',true) as BYOB,
       json_extract_path_text(json_args,'business_name',true) as business_name
       from dwh.quotes_policies_mlob where cob in ('Restaurant')  and creation_time >= '2022-01-01' and creation_time <= '2022-12-31' and highest_policy_status >=3 and
      offer_flow_type in ('APPLICATION','RENEWAL','CANCEL_REWRITE')

with t1 as (select
    last_day(creation_time) as month,
    count(distinct related_business_id) as sold_policy_count
from dwh.quotes_policies_mlob
where highest_policy_status >= 3 and
      lob_policy = 'GL' and
      new_reneweal = 'new' and
--       offer_flow_type in ('APPLICATION') and
--       distribution_channel <> 'agents' and
      creation_time >= '2020-01-01' and creation_time <= '2022-12-31'
group by 1
order by month asc),

    t2 as (select
    last_day(creation_time) as month,
    count(distinct related_business_id) as quote_count
from dwh.quotes_policies_mlob
where lob_policy = 'GL' and
      new_reneweal = 'new' and
--       offer_flow_type in ('APPLICATION') and
--       distribution_channel <> 'agents' and
      creation_time >= '2020-01-01' and creation_time <= '2022-12-31'
group by 1
order by month asc)

select *, cast(sold_policy_count*1.0/quote_count*1.0 as decimal(10,4)) as qtp from t1 join t2 on t1.month = t2.month
order by t1.month asc

select distinct failurereason from dwh.all_activities_table


select business_id,
       json_extract_path_text(json_args,'business_name',true) as business_name
       from dwh.quotes_policies_mlob where cob in ('Restaurant')




select qpm.lob_policy,
       qpm.cob_group,
       gaap.affiliate_name,
       extract(year from qpm.creation_time) || '-' || right('00'+convert(varchar,extract(month from qpm.creation_time)),2) as creation_year_month,
       sum(qpm.highest_yearly_premium)
from dwh.quotes_policies_mlob qpm
    inner join reporting.gaap_snapshots_asl gaap on gaap.business_id=qpm.business_id and
                                               qpm.highest_policy_status >= 4 and
                                               qpm.lob_policy = 'WC' and
                                               qpm.distribution_channel = 'partnerships' and
                                               qpm.cob_group = 'Food & beverage' and
                                               gaap.affiliate_name <> '' and
                                               qpm.offer_flow_type in ('APPLICATION')
group by 1,2,3,4
order by creation_year_month asc


select *,json_extract_path_text(json_args,'lob_app_json','liquor_sales_yes_no',true) as liquor_sales_yes_no,
       json_extract_path_text(json_args,'lob_app_json','liquor_sales_exposure',true) as liquor_sales_exposure,
       json_extract_path_text(json_args,'lob_app_json','restaurant_type',true) as restaurant_type,
       json_extract_path_text(json_args,'lob_app_json','liquor_risk_byob_alcohol',true) as BYOB,
       json_extract_path_text(json_args,'business_name',true) as business_name
       from dwh.quotes_policies_mlob where cob in ('Restaurant')  and creation_time >= '2022-01-01' and
      offer_flow_type in ('APPLICATION')

With claims_detail as
 (select cc.date as report_date,
           -- date(date_trunc('year', date_of_loss)) as accident_year,
           -- actuary requested to change to loss_basis_date
           last_day(case
                                       when cc.lob = 'PL' then cc.date_submitted
                                       else cc.date_of_loss end) as accident_month,
           date(date_trunc('year', case
                                       when cc.lob = 'PL' then cc.date_submitted
                                       else cc.date_of_loss end)) as accident_year,
--            ceiling(months_between(cc.date, date_trunc('year', case
--                                                                   when cc.lob = 'PL' then cc.date_submitted
--                                                                   else cc.date_of_loss end))) as months_since_accident,
           p.policy_id,
           cc.tpa,
           case when cc.tpa = 2 then LEFT(cc.claim_id, 13) else cc.claim_id end as claim_id_trunc,
           cc.loss_type,
           location_of_loss,
--            coverage_description,
--            coverage,
           claim_status,
           sum(nvl(loss_paid_total, 0)) as loss_paid_total,
           sum(nvl(loss_reserve_total, 0)) as loss_reserve_total,
           sum(nvl(expense_ao_paid_total, 0)) as expense_ao_paid_total,
           sum(nvl(expense_dcc_paid_total, 0)) as expense_dcc_paid_total,
           sum(nvl(expense_ao_reserve_total, 0)) as expense_ao_reserve_total,
           sum(nvl(expense_dcc_reserve_total, 0)) as expense_dcc_reserve_total,
           sum(nvl(recovery_salvage_collected_total, 0)) as recovery_salvage_collected_total,
           sum(nvl(recovery_salvage_reserve_total, 0)) as recovery_salvage_reserve_total,
           sum(nvl(recovery_subrogation_collected_total, 0)) as recovery_subrogation_collected_total,
           sum(nvl(recovery_subrogation_reserve_total, 0)) as recovery_subrogation_reserve_total


    from (select *,rank() over (partition by claim_id
                         order by date desc) as date_order
                     from dwh.all_claims_financial_changes_ds where date = ((date_trunc('month', current_date) - interval '1 day')::date)) cc
--                   from dwh.all_claims_financial_changes_ds where date is not null) cc
             left join nimi_svc_prod.policies p
                       on cc.policy_reference = p.policy_reference

    where
--           cc.date = last_day(cc.date) and
          cc.carrier not in (2, 3, 5) and cc.date_order = 1
    group by  1,2,3,4,5,6,7,8,9),

    claims_total as

(select claim_id_trunc,loss_paid_total + expense_ao_paid_total + expense_dcc_paid_total                        as loss_alae_paid,
                loss_reserve_total + expense_ao_reserve_total + expense_dcc_reserve_total                      as loss_alae_reserve,
                loss_alae_paid + loss_alae_reserve + recovery_salvage_collected_total +
                recovery_subrogation_collected_total                                                           as total_loss_alae
from claims_detail),


    claims_sum as

(select claim_id_trunc,sum(loss_alae_paid) as loss_alae_paid, sum(loss_alae_reserve) as loss_alae_reserve,sum(total_loss_alae) as total_loss_alae from claims_total
       group by claim_id_trunc),

    claims_detail_and_sum as

(select claim_id,business_id,policy_reference,lob,cob_name,marketing_cob_group, loss_cause_type_name,claim_status,loss_alae_paid,loss_alae_reserve,total_loss_alae
 from dwh.all_claims_details left join claims_sum on dwh.all_claims_details.claim_id = claims_sum.claim_id_trunc)


select * from claims_detail_and_sum claims
left join (select distinct business_id,
      json_extract_path_text(json_args,'business_name',true) as business_name
     from dwh.quotes_policies_mlob) qpm
on qpm.business_id = claims.business_id
where --lob = 'GL' and
      cob_name = 'Restaurant' and
      total_loss_alae is not null
order by total_loss_alae desc

select * from dwh.all_claims_details where claim_id = '28iJxK6jV88p8U3g'

with t1 as (select agent_id agentid,agent_name, current_agencytype, agency_name, agent_email_address, agency_aggregator_name from dwh.v_agents)
     select *,json_extract_path_text(json_args,'business_name',true) as business_name,
                     json_extract_path_text(json_args,'lob_app_json','liquor_sales_yes_no',true) as liquor_sales_yes_no,
                     json_extract_path_text(json_args, 'lob_app_json','liquor_sales_exposure', true) as liquor_sales_exposure
     from dwh.quotes_policies_mlob qpm left join t1 on t1.agentid = qpm.agent_id
where cob = 'Restaurant' and offer_flow_type in ('APPLICATION') and  creation_time>='2022-01-01' and agent_name IS NOT NULL
order by creation_time desc

select * from prod.albus_prod.search_result where created <= '2023-02-18' and created > '2023-02-17' and domain = 'yelp'

select *,json_extract_path_text(json_args,'lob_app_json','liquor_sales_yes_no',true) as liquor_sales_yes_no,
       json_extract_path_text(json_args,'lob_app_json','liquor_sales_exposure',true) as liquor_sales_exposure,
       json_extract_path_text(json_args,'lob_app_json','restaurant_type',true) as restaurant_type,
       json_extract_path_text(json_args,'business_name',true) as business_name
       from dwh.quotes_policies_mlob where cob in ('Restaurant','Coffee Shop','Bakery')  and highest_policy_status >=3 and creation_time >= '2023-03-26'

WITH t1 AS (select pro_plus_quote_job_id,highest_status_name,cob,revenue_in_12_months,highest_yearly_premium,
       json_extract_path_text(json_args,'business_name',true) as business_name
       from dwh.quotes_policies_mlob
where lob_policy = 'GL' and cob_group = 'Auto Service and Repair' and highest_policy_status >=3 and creation_time>='2022-01-01' and creation_time>='2022-12-31'),
     t2 AS
(select *,
        (json_extract_path_text(json_extract_path_text(json_extract_path_text(rating_result,'lobSpecificResult',true), 'hiredNonOwnedMapResults', true), '_gl_hired_auto_coverage_premium',true)) as hired_auto_premium,
               (json_extract_path_text(json_extract_path_text(json_extract_path_text(rating_result,'lobSpecificResult',true), 'hiredNonOwnedMapResults', true), '_gl_non_owned_auto_coverage_premium',true)) as non_owned_premium
       from s3_operational.rating_svc_prod_calculations where lob = 'GL')
select * from t1 join t2 ON t1.pro_plus_quote_job_id=t2.job_id

WITH t1 AS (select pro_plus_quote_job_id,highest_status_name,cob,revenue_in_12_months,highest_yearly_premium,
       json_extract_path_text(json_args,'business_name',true) as business_name
       from dwh.quotes_policies_mlob
where lob_policy = 'GL' and cob_group = 'Auto Service and Repair' and highest_policy_status >=3 and creation_time>='2022-01-01' and creation_time>='2022-12-31'),
     t2 AS
(select *,
        (json_extract_path_text(json_extract_path_text(json_extract_path_text(rating_result,'lobSpecificResult',true), 'hiredNonOwnedMapResults', true), '_gl_hired_auto_coverage_premium',true)) as hired_auto_premium,
               (json_extract_path_text(json_extract_path_text(json_extract_path_text(rating_result,'lobSpecificResult',true), 'hiredNonOwnedMapResults', true), '_gl_non_owned_auto_coverage_premium',true)) as non_owned_premium
       from s3_operational.rating_svc_prod_calculations where lob = 'GL')
select * from t1 join t2 ON t1.pro_plus_quote_job_id=t2.job_id

WITH t1 as (select distinct business_id as biz_id, affiliate_name from reporting.gaap_snapshots_asl)
select business_id,lob_policy,cob_group,highest_status_name,affiliate_name from dwh.quotes_policies_mlob qpm
join t1 on t1.biz_id = qpm.business_id
where qpm.creation_time >= '2021-01-01' and
      qpm.lob_policy = 'GL' and
      --qpm.cob_group = 'Consulting' and
      --gaap.affiliate_name = 'LegalZoom' and
      qpm.offer_flow_type = 'APPLICATION' and
      qpm.business_id = 'd4c314f008e60c9accb254986b6c98be'
limit 100

select qpm.business_id,
       qpm.lob_policy,
       qpm.cob_group,
       qpm.highest_status_name
       --gaap.affiliate_name
from dwh.quotes_policies_mlob qpm
        inner join reporting.gaap_snapshots_asl gaap
            on gaap.business_id = qpm.business_id
where qpm.creation_time >= '2021-01-01' and
      qpm.lob_policy = 'GL' and
      --qpm.cob_group = 'Consulting' and
      --gaap.affiliate_name = 'LegalZoom' and
      qpm.offer_flow_type = 'APPLICATION' and
      qpm.business_id = 'd4c314f008e60c9accb254986b6c98be'
limit 100

select business_id, affiliate_name from reporting.gaap_snapshots_asl where business_id = 'd4c314f008e60c9accb254986b6c98be'

select distinct distribution_channel from dwh.quotes_policies_mlob

SELECT *
FROM dwh.underwriting_quotes_data
WHERE execution_status='DECLINE'
LIMIT 10

select * FROM ap_intego_db.ap_salesforce.account limit 30

select distinct offer_creation_time::date,business_name, business_id, cob, execution_status, answers,policy_status_name, decline_reasons,
                json_extract_path_text(answers,'restaurant_type',true) as restaurant_type,
                json_extract_path_text(answers,'liquor_sales_exposure',true) as liquor_sales_exposure,
                json_extract_path_text(answers,'liquor_sales_yes_no',true) as liquor_sales_yes_no,
                street,city, state_code, zip_code
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
where uw.lob = 'GL'  and cob = 'Restaurant' and state_code in ('IA', 'IL', 'MA', 'MI', 'MN', 'MO', 'UT') order by offer_creation_time desc limit 500


select distinct offer_creation_time::date,business_name, business_id, cob, execution_status, policy_status_name, decline_reasons,answers,
                street,city, state_code, zip_code,affiliate_id,agent_id,
                (CASE WHEN (affiliate_id = 'N/A' and  agent_id = 'N/A') then 'direct'
                    WHEN (affiliate_id <> 'N/A' and  agent_id = 'N/A') then 'affiliate'
                    else 'agent' end) as channel
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
where uw.lob = 'BP' and execution_status = 'DECLINE' and decline_reasons like '%claim%' and cob = 'Restaurant' order by offer_creation_time desc

select distinct offer_creation_time::date,business_name, business_id, cob, execution_status, policy_status_name, decline_reasons,answers,
                street,city, state_code, zip_code,affiliate_id,agent_id,
                (CASE WHEN (affiliate_id = 'N/A' and  agent_id = 'N/A') then 'direct'
                    WHEN (affiliate_id <> 'N/A' and  agent_id = 'N/A') then 'affiliate'
                    else 'agent' end) as channel
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
where uw.lob = 'GL' and execution_status = 'DECLINE' and decline_reasons like '%floor%' and cob = 'Restaurant' order by offer_creation_time desc

select affiliate_id,agent_id,(CASE WHEN (affiliate_id = 'N/A' and  agent_id = 'N/A') then 'direct'
    WHEN (affiliate_id <> 'N/A' and  agent_id = 'N/A') then 'affiliate'
                    else 'agent' end) as channel
from dwh.underwriting_quotes_data uw limit 500

select * from underwriting_svc_prod.lob_applications limit 10

select distinct offer_creation_time::date,business_name, business_id, cob, execution_status, policy_status_name, decline_reasons,
                street,city, state_code, zip_code
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
where cob in ('Restaurant') and execution_status = 'DECLINE' and decline_reasons like '%hurricane%' order by offer_creation_time desc



select * from dwh.underwriting_quotes_data limit 10

select distinct business_id
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
where uw.lob = 'CP' and execution_status = 'DECLINE' and decline_reasons like '%seasonally%' order by offer_creation_time desc

select *,
                json_extract_path_text(answers,'restaurant_type',true) as restaurant_type,
                json_extract_path_text(answers,'liquor_sales_exposure',true) as liquor_sales_exposure,
                json_extract_path_text(answers,'liquor_sales_yes_no',true) as liquor_sales_yes_no
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
where uw.lob = 'GL' and cob in ('Bakery', 'Caterer','Coffee Shop','Food Truck','Grocery Store','Restaurant') and policy_status >=3 order by offer_creation_time desc limit 50

    select distinct business_id,business_name, policy_status_name, revenue_in_12_months,uw.state_code, cob,  uw.city, uw.zip_code, uw.street,agent_name,agency_aggregator_name,current_agencytype,
                    json_extract_path_text(answers,'restaurant_type',true) as restaurant_type,
                    json_extract_path_text(answers,'liquor_sales_exposure',true) as liquor_sales_exposure,
                    json_extract_path_text(answers,'liquor_sales_yes_no',true) as liquor_sales_yes_no
    from dwh.underwriting_quotes_data uw
    join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
    join dwh.v_agents a on uw.agent_id = a.agent_id
    where uw.lob = 'GL' and uw.cob = 'Restaurant' and agent_name like '%Kyle%' and policy_status_name = 'Active' order by offer_creation_time

    select * from dwh.underwriting_quotes_data uw
    join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
    join dwh.v_agents a on uw.agent_id = a.agent_id
    where uw.lob = 'GL' and uw.cob = 'Restaurant' and agent_name like '%Kyle%' and policy_status_name = 'Active' and business_id = '9b6627c936f5b86e1bc16296ceb085ac'
    order by offer_creation_time


select * from riskmgmt_svc_prod.exposure_base_revenue_results limit 10

select *
FROM underwriting_svc_prod.applicant_data
WHERE prospect_id = '154b5b8f83737064729657d5e0c8e106'

select distinct business_id,business_name, policy_status_name, uw.state_code, cob, uw.city, uw.zip_code, uw.street,agent_name,agency_aggregator_name,current_agencytype
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
left join dwh.v_agents a on uw.agent_id = a.agent_id
where uw.lob != 'CA' and policy_status >= 3 and cob in ('Restaurant') order by offer_creation_time desc

select * from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
left join dwh.v_agents a on uw.agent_id = a.agent_id
where uw.lob = 'CP' and policy_status >= 3 and cob in ('Restaurant') order by offer_creation_time desc limit 50

SELECT DISTINCT
    s.external_id as business_id,
    s.created,
    sr.domain,
    sr.url,
    bo.type,
    bo.description

FROM albus_prod.search s
JOIN albus_prod.search_result sr
ON sr.search_id = s.id
left JOIN albus_prod.business_operation bo
ON bo.search_result_id = sr.id
WHERE s.external_id in ('8d95a8f8c209f3f97bd889518d5e9a11')

select * from albus_prod.search_result where domain = 'google' limit 10
SELECT * FROM information_schema.tables WHERE table_schema = 'albus_prod'
select * from albus_prod.business_feature limit 10

-- hours of operation check
with t1 as (SELECT DISTINCT
    s.external_id as business_id,url,domain
FROM albus_prod.search s
JOIN albus_prod.search_result sr
ON sr.search_id = s.id where domain in ('google','yelp') and business_id is not null)
-- JOIN albus_prod.business_operation bo
-- ON bo.search_result_id = sr.id)

select distinct offer_creation_time::date,uw.business_id,business_name, policy_status_name, uw.state_code, cob, uw.city, uw.zip_code, uw.street,agent_name,agency_aggregator_name,current_agencytype,url,domain
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
left join dwh.v_agents a on uw.agent_id = a.agent_id
left join t1 on uw.business_id = t1.business_id
where uw.lob != 'CA'
--   and policy_status >= 3
  and cob in ('Restaurant')
--   and business_id in (select business_id from t1)
order by offer_creation_time desc limit 1000

select distinct offer_creation_time::date, business_id,business_name, cob,revenue_in_12_months, policy_status,policy_status_name, agent_name from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
join dwh.v_agents a on uw.agent_id = a.agent_id
where uw.lob != 'CA' and agent_name = 'Anthony Cannizzaro'
order by offer_creation_time desc

select distinct offer_creation_time::date, business_id,business_name, cob,revenue_in_12_months, policy_status,policy_status_name, agent_name
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
join dwh.v_agents a on uw.agent_id = a.agent_id
where uw.lob != 'CA' and business_id = '501b0961a471205a4027e2518c79bb3a'
order by offer_creation_time desc

select * from dwh.underwriting_quotes_data where business_id = 'ba4e0266444d6c52729ae09fdc9dfb70'

select distinct business_id,business_name, cob, uw.lob,revenue_in_12_months, policy_status,policy_status_name, agent_name from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
join dwh.v_agents a on uw.agent_id = a.agent_id
where business_id = 'ba4e0266444d6c52729ae09fdc9dfb70'
order by offer_creation_time desc

-- gl5 ni factors and premiums
(select distinct business_id as biz_id, policy_id as v5_policy_id, quote_type as v5_quote_type, start_date as v5_start_date, schedule_rating_applied as v5_schedule_rating_applied, schedule_rating_factor as v5_schedule_rating_factor, gross_sales as v5_gross_sales, lcm as v5_lcm, policy_age as v5_policy_age, state_code, years_loss_free as v5_years_loss_free,final_premium as v5_final_premium, partition_0 as v5_partition_0,
  iso_classes[0].exposure_base_value AS v5_exposure_base_value,
  iso_classes[0].full_exposure_base_value AS v5_full_exposure_base_value,
  iso_classes[0].prem_ops_ilf AS v5_prem_ops_ilf,
  iso_classes[0].prem_ops_iso_loss_cost AS v5_prem_ops_iso_loss_cost,
  iso_classes[0].prem_ops_loss_cost AS v5_prem_ops_loss_cost,
  iso_classes[0].prem_ops_med_ilf AS v5_prem_ops_med_ilf,
  iso_classes[0].prod_cops_ilf AS v5_prod_cops_ilf,
  iso_classes[0].prod_cops_iso_loss_cost AS v5_prod_cops_iso_loss_cost,
  iso_classes[0].prod_cops_loss_cost AS v5_prod_cops_loss_cost,
  iso_classes[0].prod_cops_med_ilf AS v5_prod_cops_med_ilf,
  ni_factors_v5.area_ni_factor AS v5_area_ni_factor,
  ni_factors_v5.area_primary_iso_class_factor AS v5_area_primary_iso_class_factor,
  ni_factors_v5.business_scale_gross_sales_factor AS v5_business_scale_gross_sales_factor,
  ni_factors_v5.business_scale_payroll_factor AS v5_business_scale_payroll_factor,
  ni_factors_v5.existing_insured_factor AS v5_existing_insured_factor,
  ni_factors_v5.franchise_factor AS v5_franchise_factor,
  ni_factors_v5.gross_sales_ni_factor AS v5_gross_sales_ni_factor,
  ni_factors_v5.gross_sales_primary_iso_class_factor AS v5_gross_sales_primary_iso_class_factor,
  ni_factors_v5.heavy_materials_factor AS v5_heavy_materials_factor,
  ni_factors_v5.judgment_factor AS v5_judgment_factor,
  ni_factors_v5.ni_factor AS v5_ni_factor,
  ni_factors_v5.payroll_ni_factor AS v5_payroll_ni_factor,
  ni_factors_v5.payroll_primary_iso_class_factor AS v5_payroll_primary_iso_class_factor,
  ni_factors_v5.policy_age_factor AS v5_policy_age_factor,
  ni_factors_v5.risk_score_factor AS v5_risk_score_factor,
  ni_factors_v5.share_bank_access_factor AS vshare_bank_access_factor,
  ni_factors_v5.sole_proprietor_factor AS v5_sole_proprietor_factor,
  ni_factors_v5.state_primary_iso_class_factor AS v5_state_primary_iso_class_factor,
  ni_factors_v5.sub_cost_ni_factor AS v5_sub_cost_ni_factor,
  ni_factors_v5.subcontractor_cost_primary_iso_class_factor AS v5_subcontractor_cost_primary_iso_class_factor,
  ni_factors_v5.tree_trimming_factor AS v5_tree_trimming_factor,
  ni_factors_v5.units_ni_factor AS v5_units_ni_factor,
  ni_factors_v5.units_primary_iso_class_factor AS v5_units_primary_iso_class_factor,
  ni_factors_v5.years_in_business_factor AS v5_years_in_business_factor,
  ni_factors_v5.years_since_last_claim_factor AS v5_years_since_last_claim_factor,
  ni_factors_v5.percent_liquor_sales_factor AS v5_percent_liquor_sales_factor,
  ni_factors_v5.revenue_per_employee_factor AS v5_revenue_per_employee_factor
from external_dwh.gl_quotes
where cob = 'restaurant' and policy_id is not null and partition_0 = 'generic_gl_ggl_v5' and quote_type = 'RENEWAL')

-- gl4 ni factors and premiums
with bid as (select distinct business_id as biz_id from external_dwh.gl_quotes
where cob = 'restaurant' and policy_id is not null and partition_0 = 'generic_gl_ggl_v5' and quote_type = 'RENEWAL')

select distinct business_id,policy_id as v4_policy_id, quote_type as v4_quote_type, start_date as v4_start_date, schedule_rating_applied as v4_schedule_rating_applied, schedule_rating_factor as v4_schedule_rating_factor, gross_sales as v4_gross_sales, lcm as v4_lcm, policy_age as v4_policy_age, years_loss_free as v4_years_loss_free,final_premium as v4_final_premium, partition_0 as v4_partition_0,
  iso_classes[0].exposure_base_value AS v4_exposure_base_value,
  iso_classes[0].full_exposure_base_value AS v4_full_exposure_base_value,
  iso_classes[0].prem_ops_ilf AS v4_prem_ops_ilf,
  iso_classes[0].prem_ops_iso_loss_cost AS v4_prem_ops_iso_loss_cost,
  iso_classes[0].prem_ops_loss_cost AS v4_prem_ops_loss_cost,
  iso_classes[0].prem_ops_med_ilf AS v4_prem_ops_med_ilf,
  iso_classes[0].prod_cops_ilf AS v4_prod_cops_ilf,
  iso_classes[0].prod_cops_iso_loss_cost AS v4_prod_cops_iso_loss_cost,
  iso_classes[0].prod_cops_loss_cost AS v4_prod_cops_loss_cost,
  iso_classes[0].prod_cops_med_ilf AS v4_prod_cops_med_ilf,
  ni_factors_v4.area_ni_factor AS v4_area_ni_factor,
  ni_factors_v4.area_primary_iso_class_factor AS v4_area_primary_iso_class_factor,
  ni_factors_v4.business_scale_gross_sales_factor AS v4_business_scale_gross_sales_factor,
  ni_factors_v4.business_scale_payroll_factor AS v4_business_scale_payroll_factor,
  ni_factors_v4.existing_insured_factor AS v4_existing_insured_factor,
  ni_factors_v4.franchise_factor AS v4_franchise_factor,
  ni_factors_v4.gross_sales_ni_factor AS v4_gross_sales_ni_factor,
  ni_factors_v4.gross_sales_primary_iso_class_factor AS v4_gross_sales_primary_iso_class_factor,
  ni_factors_v4.heavy_materials_factor AS v4_heavy_materials_factor,
  ni_factors_v4.judgment_factor AS v4_judgment_factor,
  ni_factors_v4.ni_factor AS v4_ni_factor,
  ni_factors_v4.payroll_ni_factor AS v4_payroll_ni_factor,
  ni_factors_v4.payroll_primary_iso_class_factor AS v4_payroll_primary_iso_class_factor,
  ni_factors_v4.policy_age_factor AS v4_policy_age_factor,
  ni_factors_v4.risk_score_factor AS v4_risk_score_factor,
  ni_factors_v4.share_bank_access_factor AS v4_share_bank_access_factor,
  ni_factors_v4.sole_proprietor_factor AS v4_sole_proprietor_factor,
  ni_factors_v4.state_primary_iso_class_factor AS v4_state_primary_iso_class_factor,
  ni_factors_v4.sub_cost_ni_factor AS v4_sub_cost_ni_factor,
  ni_factors_v4.subcontractor_cost_primary_iso_class_factor AS v4_subcontractor_cost_primary_iso_class_factor,
  ni_factors_v4.tree_trimming_factor AS v4_tree_trimming_factor,
  ni_factors_v4.units_ni_factor AS v4_units_ni_factor,
  ni_factors_v4.units_primary_iso_class_factor AS v4_units_primary_iso_class_factor,
  ni_factors_v4.years_in_business_factor AS v4_years_in_business_factor,
  ni_factors_v4.years_since_last_claim_factor AS v4_years_since_last_claim_factor
from external_dwh.gl_quotes glq
right join bid on bid.biz_id = glq.business_id
where cob = 'restaurant' and policy_id is not null and partition_0 = 'generic_gl_ggl_v4' and quote_type in ('APPLICATION', 'RENEWAL')

select distinct business_id,policy_id, gross_sales,iso_classes[0].exposure_base_value AS v4_exposure_base_value,
  iso_classes[0].full_exposure_base_value AS v4_full_exposure_base_value,payroll_in_next_12_months,
  iso_classes[0].exposure_type AS exposure_type,partition_0
from external_dwh.gl_quotes where cob = 'restaurant' and quote_type in ('APPLICATION', 'RENEWAL') and payroll_in_next_12_months is not null

select distinct business_id,policy_id, cob, gross_sales,iso_classes[0].exposure_base_value AS v4_exposure_base_value,
  iso_classes[0].full_exposure_base_value AS v4_full_exposure_base_value,payroll_in_next_12_months,
  iso_classes[0].exposure_type AS exposure_type,partition_0
from external_dwh.gl_quotes where start_date >= '2025-01-01' and quote_type in ('APPLICATION', 'RENEWAL') and payroll_in_next_12_months is not null and policy_id is not null

select * from external_dwh.gl_quotes where cob = 'restaurant' limit 10




  SELECT
    business_id,
    policy_id,
    schedule_rating_adjustments[1].category AS cat_1,
    schedule_rating_adjustments[1].factor AS factor_1,
    schedule_rating_adjustments[2].category AS cat_2,
    schedule_rating_adjustments[2].factor AS factor_2,
    schedule_rating_adjustments[3].category AS cat_3,
    schedule_rating_adjustments[3].factor AS factor_3,
    schedule_rating_adjustments[4].category AS cat_4,
    schedule_rating_adjustments[4].factor AS factor_4,
    schedule_rating_adjustments[5].category AS cat_5,
    schedule_rating_adjustments[5].factor AS factor_5,
    schedule_rating_adjustments[6].category AS cat_6,
    schedule_rating_adjustments[6].factor AS factor_6,
    schedule_rating_adjustments[7].category AS cat_7,
    schedule_rating_adjustments[7].factor AS factor_7,
    schedule_rating_adjustments[8].category AS cat_8,
    schedule_rating_adjustments[8].factor AS factor_8,
    COALESCE(
    CASE WHEN cat_1 = 'exposure_outside_premises' THEN factor_1 END,
    CASE WHEN cat_2 = 'exposure_outside_premises' THEN factor_2 END,
    CASE WHEN cat_3 = 'exposure_outside_premises' THEN factor_3 END,
    CASE WHEN cat_4 = 'exposure_outside_premises' THEN factor_4 END,
    CASE WHEN cat_5 = 'exposure_outside_premises' THEN factor_5 END,
    CASE WHEN cat_6 = 'exposure_outside_premises' THEN factor_6 END,
    CASE WHEN cat_7 = 'exposure_outside_premises' THEN factor_7 END,
    CASE WHEN cat_8 = 'exposure_outside_premises' THEN factor_8 END,
    0  -- default value if not found
  ) AS exposure_factor
  FROM external_dwh.gl_quotes
  WHERE business_id = '9478b606c64d373fd41a158bfeb685c2'
  AND policy_id IS NOT NULL



select * from external_dwh.gl_quotes where business_id = '9478b606c64d373fd41a158bfeb685c2' and policy_id is not null











select distinct business_id,start_date, final_premium from external_dwh.gl_quotes where cob = 'restaurant' and final_premium > 90000 order by start_date desc





with bid as (select distinct business_id as biz_id from external_dwh.gl_quotes
where cob = 'restaurant' and policy_id is not null and partition_0 = 'generic_gl_ggl_v5' and quote_type = 'RENEWAL')
select distinct policy_id, total_premium
from external_dwh.gl_quotes glq
right join bid on bid.biz_id = glq.business_id
where cob = 'restaurant' and policy_id is not null and partition_0 = 'generic_gl_ggl_v4' and quote_type in ('APPLICATION', 'RENEWAL')
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --


with t1 as (SELECT DISTINCT
    s.external_id as business_id,
    s.created,
    sr.domain,
    sr.url,
    bo.type,
    bo.description

FROM albus_prod.search s
JOIN albus_prod.search_result sr
ON sr.search_id = s.id
JOIN albus_prod.business_operation bo
ON bo.search_result_id = sr.id)


select distinct business_id,business_name, policy_status_name, uw.state_code, cob, uw.city, uw.zip_code, uw.street,agent_name,agency_aggregator_name,current_agencytype
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
left join dwh.v_agents a on uw.agent_id = a.agent_id
where uw.lob != 'CA' and policy_status >= 3 and cob in ('Restaurant')
order by offer_creation_time desc



select count (distinct business_id) from dwh.underwriting_quotes_data uw
where  policy_status >= 3 and  cob in ('Restaurant') and lob in ('GL','CP')



SELECT DISTINCT
    s.external_id as business_id,
    s.created,
    sr.domain,
    sr.url,
    bo.type,
    bo.description

FROM albus_prod.search s
JOIN albus_prod.search_result sr
ON sr.search_id = s.id
JOIN albus_prod.business_operation bo
ON bo.search_result_id = sr.id
WHERE s.external_id in ('792808ce56471233a81ef1a1242b0ca5','57713b0c08547d78e0f907fec450edfc','e922431e2c25c3bf1c7d151a9dbf77eb')

-- usual restaurant query
select distinct offer_creation_time::date, business_id,business_name, policy_status_name,revenue_in_12_months,yearly_premium, uw.lob,uw.state_code, cob, uw.city, uw.zip_code, uw.street,agent_name,agency_aggregator_name,current_agencytype,
                json_extract_path_text(answers,'restaurant_type',true) as restaurant_type,
                json_extract_path_text(answers,'liquor_sales_exposure',true) as liquor_sales_exposure,
                json_extract_path_text(answers,'liquor_sales_yes_no',true) as liquor_sales_yes_no
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
left join dwh.v_agents a on uw.agent_id = a.agent_id
where uw.lob != 'CA'  and cob in ('Restaurant')
  and policy_status >= 3 and offer_creation_time >= '2024-01-07' and agent_name is not null and current_agencytype = 'Wholesaler'
order by offer_creation_time desc limit  300


select date_trunc('quarter',createddate) as month, count(distinct opportunity_id__c) as opps, count(distinct case when other_policy_limits__c ilike '%DAMAGE%' then opportunity_id__c end) as demage_limits
from ap_salesforce.opportunity
where other_policy_limits__c!='' and policy_type__c IN ('GL','BOP')
and stagename in ('Closed Dead','Closed Sold','Submitted Proposal','Issuance','Quote Complete','Ready to Quote','Quoting')
group by 1
order by 1
limit 100;

SELECT DISTINCT
    s.external_id as business_id,
    s.created,
    sr.domain,
    sr.url,
    bo.type,
    bo.description

FROM albus_prod.search s
JOIN albus_prod.search_result sr
ON sr.search_id = s.id
JOIN albus_prod.business_operation bo
ON bo.search_result_id = sr.id
WHERE s.external_id = 'bdedce672203aa5567b424ba326f4a55'


select
    cob,
    (CASE WHEN (affiliate_id = 'N/A' and  agent_id = 'N/A') then 'direct'
        WHEN (affiliate_id <> 'N/A' and  agent_id = 'N/A') then 'affiliate'
        else 'agent' end) as channel,
    uw.lob,
    decline_reasons,
    count(distinct(business_id)) as biz_count
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
where execution_status = 'DECLINE'
  and decline_reasons not like '%","%'
  and offer_creation_time >= '2023-01-01'
  and offer_creation_time <= '2023-03-31'
and cob = 'Restaurant' and uw.lob = 'CP'
group by 1,2,3,4
order by biz_count desc

select date_trunc('quarter',createddate) as month, count(distinct opportunity_id__c) as opps, count(distinct case when other_policy_limits__c ilike '%DAMAGE%' then opportunity_id__c end) as demage_limits
from ap_salesforce.opportunity
where other_policy_limits__c!='' and policy_type__c IN ('GL','BOP')
and stagename in ('Closed Dead','Closed Sold','Submitted Proposal','Issuance','Quote Complete','Ready to Quote','Quoting')
group by 1
order by 1
limit 100;


--to get top declines by COB, channel and LOB


select distinct offer_creation_time::date,business_name,revenue_in_12_months, business_id, cob, execution_status, answers,policy_status_name, decline_reasons,agent_name,agency_aggregator_name,current_agencytype,
                json_extract_path_text(answers,'bpp_limit',true) as bpp_limit,
                json_extract_path_text(answers,'bpp_kitchen_equipment',true) as bpp_kitchen_equipment,
                json_extract_path_text(answers,'bpp_tables_and_seating',true)  as bpp_tables_and_seating,
                json_extract_path_text(answers,'bpp_building_improvements_and_remodeling',true)   as bpp_building_improvements_and_remodeling,
                json_extract_path_text(answers,'bpp_pos_system_cash_register',true)   as bpp_pos_system_cash_register,
                json_extract_path_text(answers,'bpp_other',true)  as bpp_other,
        street,uw.city, uw.state_code, uw.zip_code
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
left join dwh.v_agents a on uw.agent_id = a.agent_id
where uw.lob = 'CP' and cob in ('Restaurant') and creation_time > '2023-3-20'
--   and execution_status = 'DECLINE' and decline_reasons ilike '%limits%'
order by offer_creation_time desc

select * from dwh.underwriting_quotes_data limit 10


-- enriching restaurant policies with albus data
WITH t0 as (select distinct offer_creation_time::date,business_id,business_name, policy_status_name, uw.lob,execution_status,decline_reasons,uw.state_code, cob, uw.city, uw.zip_code, uw.street,agent_name,agency_aggregator_name,current_agencytype
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
left join dwh.v_agents a on uw.agent_id = a.agent_id
where uw.lob != 'CA' and cob in ('Restaurant')
order by offer_creation_time desc),

    t1 as (SELECT DISTINCT
    s.external_id as business_id,
    s.created,
    sr.domain,
    sr.url,
    bo.type,
    bo.description

FROM albus_prod.search s
JOIN albus_prod.search_result sr
ON sr.search_id = s.id
JOIN albus_prod.business_operation bo
ON bo.search_result_id = sr.id
),

    t2 as (
select business_id, listagg(description::varchar, ',')
within group (order by description) as business_description from t1
group by business_id
order by business_id)

select distinct offer_creation_time::date,t0.business_id,business_name, lob,policy_status_name,execution_status,decline_reasons, cob, street,city,state_code,zip_code,agent_name,agency_aggregator_name,current_agencytype,business_description
from t0 left join t2 on t0.business_id = t2.business_id
                  where offer_creation_time >= '2023-04-01' and offer_creation_time <= '2023-04-30'
         order by offer_creation_time desc
----------------------------------------------------------------



select distinct offer_creation_time::date,business_id,offer_id,business_name, policy_status_name, uw.lob,execution_status,decline_reasons,uw.state_code, cob, uw.city, uw.zip_code, uw.street,agent_name,agency_aggregator_name,current_agencytype
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
left join dwh.v_agents a on uw.agent_id = a.agent_id
where uw.lob != 'CA' and business_id in ('6719df4283fdbaa0aae0ddc00649d0da')
order by offer_creation_time desc

SELECT DISTINCT
    s.external_id as business_id,
    s.created,
    sr.domain,
    sr.url,
    bo.type,
    bo.description

FROM albus_prod.search s
JOIN albus_prod.search_result sr
ON sr.search_id = s.id
JOIN albus_prod.business_operation bo
ON bo.search_result_id = sr.id limit 500

select * from dwh.v_agents where city = 'Palo Alto'

select * from dwh.ncci_history_reports_cache limit 10




select distinct offer_creation_time, business_id,business_name, policy_status_name,revenue_in_12_months,uw.lob,uw.state_code, cob, uw.city, uw.zip_code, uw.street,agent_name,agency_aggregator_name,current_agencytype,answers,
                 json_extract_path_text(answers,'has_5_or_more_deep_fryers',true) as has_5_or_more_deep_fryers
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
left join dwh.v_agents a on uw.agent_id = a.agent_id
where uw.lob = 'CP'  and cob in ('Restaurant')
order by offer_creation_time desc limit 2000

select distinct offer_creation_time, business_id,business_name, policy_status_name,revenue_in_12_months,uw.lob,uw.state_code, cob, uw.city, uw.zip_code, uw.street,agent_name,agency_aggregator_name,current_agencytype,answers,
                 json_extract_path_text(answers,'has_5_or_more_deep_fryers',true) as has_5_or_more_deep_fryers
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
left join dwh.v_agents a on uw.agent_id = a.agent_id
where  cob in ('Restaurant') and uw.city = 'Palo Alto'
order by offer_creation_time desc limit 2000



select offer_creation_time, business_id,business_name, policy_status_name,revenue_in_12_months,uw.lob,uw.state_code, cob, uw.city, uw.zip_code, uw.street,agent_name,agency_aggregator_name,current_agencytype,answers from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
join dwh.v_agents a on uw.agent_id = a.agent_id
where uw.lob != 'CA' and offer_flow_type = 'APPLICATION' and execution_status = 'SUCCESS' and agent_name = 'Issac Gelbstein' and cob = 'Grocery Store'
order by offer_creation_time desc


select distinct offer_creation_time::date,business_name, business_id, cob, execution_status, policy_status_name, decline_reasons,answers,
                street,city, state_code, zip_code,affiliate_id,agent_id,
                (CASE WHEN (affiliate_id = 'N/A' and  agent_id = 'N/A') then 'direct'
                    WHEN (affiliate_id <> 'N/A' and  agent_id = 'N/A') then 'affiliate'
                    else 'agent' end) as channel
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
where execution_status = 'DECLINE' and uw.lob = 'GL' and cob = 'Grocery Store'

select distinct offer_creation_time,business_id,business_name, policy_status_name,uw.lob,yearly_premium, uw.state_code, cob, uw.city, uw.zip_code, uw.street,agent_name,agency_aggregator_name,current_agencytype,revenue_in_12_months
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
left join dwh.v_agents a on uw.agent_id = a.agent_id
where uw.lob != 'CA' and policy_status >= 3 and cob in ('Grocery Store') and offer_creation_time >=
order by yearly_premium desc

select * from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
left join dwh.v_agents a on uw.agent_id = a.agent_id
where uw.lob != 'CA' and policy_status >= 3 and cob in ('Restaurant') limit 10

select distinct offer_creation_time,business_id,policy_id,business_name, policy_status_name,uw.lob,yearly_premium, uw.state_code, cob, uw.city, uw.zip_code, uw.street,agent_name,agency_aggregator_name,current_agencytype,revenue_in_12_months from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
join dwh.v_agents a on uw.agent_id = a.agent_id
where uw.lob != 'CA' and offer_flow_type in ('APPLICATION','RENEWAL')   and agency_name = 'J.K.Ting & Associates LLC'
order by offer_creation_time desc

select distinct business_id,business_name, policy_status_name, revenue_in_12_months,uw.state_code, cob,  uw.city, uw.zip_code, uw.street,agent_name,agency_aggregator_name,current_agencytype,
                    json_extract_path_text(answers,'restaurant_type',true) as restaurant_type,
                    json_extract_path_text(answers,'liquor_sales_exposure',true) as liquor_sales_exposure,
                    json_extract_path_text(answers,'liquor_sales_yes_no',true) as liquor_sales_yes_no
    from dwh.underwriting_quotes_data uw
    join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
    left join dwh.v_agents a on uw.agent_id = a.agent_id
    where uw.lob = 'GL' and policy_status >= 3 and cob in ('Restaurant') and uw.state_code = 'IA'

select * from riskmgmt_svc_prod.exposure_base_revenue_results where business_id = '06980d083cd8b51961d77022295ef831'


--avg price and standard deviation by COB group
with subquery as (
        SELECT lob_policy, cob_group, highest_yearly_premium, business_id,
               AVG(highest_yearly_premium) OVER (PARTITION BY lob_policy, cob_group) AS avg_premium
        FROM dwh.quotes_policies_mlob
        WHERE highest_policy_status = 4 and
              offer_flow_type in ('APPLICATION','RENEWAL','CANCEL_REWRITE') and
              policy_start_date >= '2023-06-01' and
              lob_policy = 'GL')

--     ,subquery2 as
        (SELECT lob_policy, cob_group,
           STDDEV(highest_yearly_premium),
--                 SQRT(SUM(POW(highest_yearly_premium - avg_premium, 2)) / COUNT(business_id)) AS premium_std_dev,
           SUM(highest_yearly_premium) / COUNT(business_id) AS avg_premium,
           SUM(highest_yearly_premium) AS total_premium_in_force
         from subquery
         group by 1,2)


select distinct offer_creation_time::date,business_id,offer_id,business_name, policy_status_name, uw.lob,execution_status,decline_reasons,uw.state_code, cob, uw.city, uw.zip_code, uw.street,agent_name,agency_aggregator_name,current_agencytype
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
left join dwh.v_agents a on uw.agent_id = a.agent_id
where uw.lob != 'CA' and business_id in ('cfbab9b8122520b4daf925cad3b93f33')
order by offer_creation_time desc

select offer_creation_time, business_id,business_name, policy_status_name,revenue_in_12_months,uw.lob,uw.state_code, cob, uw.city, uw.zip_code, uw.street,agent_name,agency_aggregator_name,current_agencytype,answers from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
join dwh.v_agents a on uw.agent_id = a.agent_id
where uw.lob != 'CA' and offer_flow_type = 'APPLICATION' and execution_status = 'SUCCESS' and agent_name = 'Issac Gelbstein' and cob = 'Grocery Store'
order by offer_creation_time desc

select distinct offer_creation_time::date,business_id,offer_id,business_name, policy_status_name, uw.lob,yearly_premium,execution_status,decline_reasons,uw.state_code, cob, uw.city, uw.zip_code, uw.street,agent_name,agency_name,agency_aggregator_name,current_agencytype
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
left join dwh.v_agents a on uw.agent_id = a.agent_id
where uw.lob != 'CA'  and policy_status >= 3 and agent_name = 'Joo King Ting'
order by offer_creation_time desc

select * from riskmgmt_svc_prod.exposure_base_revenue_results where business_id = '8b82a34b14700294049ab9bf1f62e9ce'



select offer_creation_time, business_id,business_name, policy_status_name,revenue_in_12_months,uw.lob,uw.state_code, cob, uw.city, uw.zip_code, uw.street,agent_name,agency_aggregator_name,current_agencytype,answers from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
join dwh.v_agents a on uw.agent_id = a.agent_id
where uw.lob != 'CA' and  execution_status = 'DECLINE' and business_id = '7ae07bfcda3c6e3b452fa37782ba86ed'
order by offer_creation_time desc

select * from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
join dwh.v_agents a on uw.agent_id = a.agent_id
where uw.lob != 'CA' and  execution_status = 'DECLINE' and business_id = 'ae29bb36cd6856758bab0708e1afab41'
order by offer_creation_time desc

select distinct
    tracking_id,
    data_domain,
    eventtime,
    funnelphase,
    funnelseqorder,
    cob_id,
    cob_name,
    app_mode,
    user_type,
    payg_ind,
    updated_at,
    placement
from dwh.all_activities_table
where data_domain = 'App User Interactions' and eventtime > '07-06-2023' and funnelphase = 'Lead'
order by eventtime desc
limit 100

select * from dwh.all_activities_table
where data_domain = 'App User Interactions' and eventtime > '07-06-2023' and funnelphase = 'Lead'
order by eventtime desc
limit 100

select distinct cob_name from dwh.sources_attributed_table where marketing_cob_group = 'Unsupported COB'



select distinct marketing_cob_group from dwh.sources_attributed_table

with tiv_decline as (select business_id,
       tracking_id,
       date(offer_creation_time) as ds,
       cob
from dwh.underwriting_quotes_data
where lob = 'CP'
and offer_creation_time >= '2022-12-01'
  and execution_status = 'DECLINE'
and decline_reasons like '%The limits you require are too high per our risk guidelines%'),
    falty_calls as (
          SELECT business_id,
         date(creation_time) ds,
         first_name,
         city,
         state,
         street_address1
  FROM riskmgmt_svc_prod.risk_score_result
  WHERE creation_time BETWEEN '2022-10-01' AND '2023-09-07'
      and last_name = first_name
  and score is null
    ),
    restaurants as (select a.business_id
from tiv_decline a
left join falty_calls b
on a.business_id = b.business_id
and a.ds = b.ds
left join dwh.policy_transactions c
on a.business_id = c.business_id
and c.tx_effective_date >= '2022-10-01'
and c.transaction_type = 'BIND'
and c.prev_policy_id is null
where b.business_id is not null
and c.business_id is null
and (lower(a.cob) like '%restaurant%' ))

select distinct transaction_type from dwh.policy_transactions

select distinct a.business_id, state,city,street,zip_code,json_extract_path_text(json_args, 'business_name', true) biz_name
from dwh.quotes_policies_mlob_dec a
left join nimi_svc_prod.businesses b
                           on a.business_id = b.business_id
where a.business_id in (select * from restaurants)
                                           and a.creation_time >= '2022-10-01'
                                               and lob_policy = 'CP'
                                               limit 1000

select distinct offer_creation_time, business_id,business_name, policy_status_name,revenue_in_12_months,uw.lob,decline_reasons,uw.state_code, cob, uw.city, uw.zip_code, uw.street,agent_name,agency_aggregator_name,current_agencytype,
                json_extract_path_text(answers,'restaurant_type',true) as restaurant_type,
                json_extract_path_text(answers,'liquor_sales_exposure',true) as liquor_sales_exposure,
                json_extract_path_text(answers,'liquor_sales_yes_no',true) as liquor_sales_yes_no
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
left join dwh.v_agents a on uw.agent_id = a.agent_id
where uw.lob = 'GL'  and cob in ('Restaurant')
 order by revenue_in_12_months desc limit  300

-- check decline_reasons
select decline_reasons, count (distinct business_id) as ct
from dwh.underwriting_quotes_data uw where uw.lob = 'GL'  and cob in ('Restaurant')
group by decline_reasons
order by ct desc

select business_id, decline_reasons
from dwh.underwriting_quotes_data uw
where decline_reasons = '["This policy is not suitable for your business operations given the type or status of your liquor license."]'
order by offer_creation_time desc

select cob_name,
    avg(sqft)  avg_sqft,
       median(sqft)  median_sqft

from(select a.policy_id,
       a.tx_effective_date,
       a.policy_start_date,
       a.policy_actual_end_date,
       a.prev_policy_id,
       a.bind_quote_id,
       a.business_id,
       a.business_name,
       a.cob_name,
       b.marketing_cob_group cob_group,
       a.channel,
       a.policy_reference,
       a.original_yearly_premium,
       cast(nullif(json_extract_path_text(c.data_points,
           'dataPointsWithoutPackageData' , 'squareFootage', true), '') as int) sqft

from dwh.policy_transactions a
left join dwh.sources_test_cobs b
on a.cob_id = b.cob_id
left join s3_operational.rating_svc_prod_calculations c
on a.bind_quote_id = c.job_id
where a.lob = 'CP'
and a.tx_effective_date >= '2023-01-01'
and a.transaction_type = 'BIND'
and a.prev_policy_id is null --new policy, no renewal
and c.creation_time >= '2023-01-01'
and c.lob = 'CP'
and b.marketing_cob_group = 'Food & beverage') a
group by 1

-- toast data
select distinct offer_creation_time::date, business_id,business_name, policy_status_name,revenue_in_12_months,uw.lob,decline_reasons,uw.state_code, cob, uw.city, uw.zip_code, uw.street,yearly_premium, affiliate_id
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
where affiliate_id in ('5100') and offer_creation_time >= '2023-09-26'
 order by offer_creation_time desc



select distinct offer_creation_time::date, business_id,business_name, policy_status_name,revenue_in_12_months,uw.lob,decline_reasons,uw.state_code, cob, uw.city, uw.zip_code, uw.street,yearly_premium, affiliate_id
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
where uw.zip_code in ('33132') and offer_creation_time >= '2025-02-04'
 order by offer_creation_time desc

select distinct offer_creation_time::date,opportunity_id, business_id,business_name, policy_status_name,revenue_in_12_months,uw.lob,decline_reasons,uw.state_code, cob, uw.city, uw.zip_code, uw.street,yearly_premium, affiliate_id
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
where affiliate_id in ('31093') and offer_creation_time >= '2024-01-28'
 order by offer_creation_time desc



select distinct offer_creation_time::date,affiliate_id,opportunity_id, business_id,business_name, policy_status_name,revenue_in_12_months,uw.lob,decline_reasons,uw.state_code, cob, uw.city, uw.zip_code, uw.street,yearly_premium, affiliate_id
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
where business_id = '1655581f0f82e45c37c17604721b88b5'

select * from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
where affiliate_id in ('31093') limit 10

select avg(revenue_in_12_months)
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
where cob = 'Restaurant' and offer_creation_time >= '2023-09-26'  and affiliate_id not in ('5100') and offer_flow_type = 'APPLICATION' and execution_status = 'SUCCESS' and affiliate_id = 'N/A' and agent_id = 'N/A'

SELECT AVG(revenue_next_12_months) as avg_revenue_restaurants_2024
FROM dwh.company_level_metrics_ds
WHERE cob_name IN ('Restaurant')
AND EXTRACT(YEAR FROM eventtime) = 2024 and channel IN ('Direct', 'Affiliate') and affiliate_id not in ('5100')


select distinct offer_creation_time::date, business_id,business_name, policy_status_name,revenue_in_12_months,uw.lob,decline_reasons,uw.state_code, cob, uw.city, uw.zip_code, uw.street,yearly_premium, affiliate_id
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
where business_id = 'ba4e0266444d6c52729ae09fdc9dfb70' and policy_status >3
 order by offer_creation_time desc


select * from dwh.company_level_metrics_ds where business_id = 'ba4e0266444d6c52729ae09fdc9dfb70'

select * from dwh.company_level_metrics_ds where policy_reference = 'NXTJWXK37L-00-GL'

select * from dwh.company_level_metrics_ds where eventtime = (select max(eventtime) from dwh.company_level_metrics_ds)

select * from dwh.company_level_metrics_ds where agency_name = 'Eddie Brown Agency' and cob_name = 'Restaurant'



select * from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
left join dwh.v_agents a on uw.agent_id = a.agent_id
where business_id = '318584d2f733886ace1e858a0a67ac81'

select email_address, business_id from nimi_svc_prod.contacts where business_id in ('cd047063a7d5099e853f616d959f4dab','90777d67ebe72793b36cbde193ba4abc','bb5eb71d9c116c3c982c3fba109bb748','eff658879e982f49a3e2f0c14805fac3','9963ecaad7870645ccde1d49b0a31f7b','7b257f925eb7b4bfbf7394d278168964','d541b39bfac23507e238c9b13b3b2f3e','4869b4cdc31367278fc6891e395c8f11','518249b9efc3f49cc8519983ae86f656','608f09569157e51d930f57f7f2faaca7','59f464df050d3c075b69bab716fcbb0a','8c69314c4f39a3d196e392ba70f85dec','50e3a7143e488913919551681faa8c0e','b7b1e239e430cfd0c8c2d9e6119660ac','5242b63285bfd5f954f205d0a04e27ee','e7139100e44a72914a397d39ebbbd257','d3ad19754e23434d64fdd8f16e88f84a','3b072c1ddfceb37022ce42fad8b604c6','b89db1f87cd0dfba581a65eeaa91af2b','8e757efd0d765a2cab1234eb2e539237','13fc557a23cb74c76ba6ac10017ecea0')
select * from nimi_svc_prod.contacts where email_address = '2575jaybees@gmail.com'

select distinct offer_creation_time, business_id,business_name, policy_status_name,revenue_in_12_months,uw.lob,decline_reasons,uw.state_code, cob, uw.city, uw.zip_code, uw.street,agent_name,agency_aggregator_name,current_agencytype,
                json_extract_path_text(answers,'restaurant_type',true) as restaurant_type,
                json_extract_path_text(answers,'liquor_sales_exposure',true) as liquor_sales_exposure,
                json_extract_path_text(answers,'liquor_sales_yes_no',true) as liquor_sales_yes_no, affiliate_id
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
join dwh.v_agents a on uw.agent_id = a.agent_id
where cob in ('Restaurant') and decline_reasons like '%suspended%'
 order by offer_creation_time desc limit  100

select distinct offer_creation_time, business_id,business_name, policy_status_name,revenue_in_12_months,uw.lob,decline_reasons,answers,uw.state_code, cob, uw.city, uw.zip_code, uw.street,agent_name,agency_aggregator_name,current_agencytype,
                 json_extract_path_text(answers,'restaurant_type',true) as restaurant_type,
                json_extract_path_text(answers,'liquor_sales_exposure',true) as liquor_sales_exposure,
                json_extract_path_text(answers,'liquor_sales_yes_no',true) as liquor_sales_yes_no
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
join dwh.v_agents a on uw.agent_id = a.agent_id
where cob in ('Restaurant') and uw.state_code = 'GA' and current_agencytype = 'Wholesaler'
 order by offer_creation_time desc limit  100

select *
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
join dwh.v_agents a on uw.agent_id = a.agent_id
where cob in ('Restaurant') and uw.state_code = 'GA' and current_agencytype = 'Wholesaler'
 order by offer_creation_time desc limit  100




select * from dwh.v_agents where agent_email_address = 'lorenagonzalez_16@hotmail.com'

select distinct offer_creation_time, business_id,business_name, policy_status_name,revenue_in_12_months,uw.lob,decline_reasons,uw.state_code, cob, uw.city, uw.zip_code, uw.street,yearly_premium
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
where agent_id = 'S5JNlTWdlUVfLXZS'


select * from dwh.underwriting_quotes_data limit 10
select * from db_data_science.gl_premium_loss_by_cov_type limit 10
select * from underwriting_svc_prod.lob_applications limit 10
select * from s3_operational.rating_interface where lob = 'GL' limit 10
select * from s3_operational.rating_svc_prod_calculations where lob = 'GL' limit 10


with t1 as (select distinct offer_creation_time, uw.business_id,business_name, policy_status_name,uw.lob,uw.state_code, uw.cob, uw.city, uw.zip_code, uw.street,
                json_extract_path_text(answers,'asked_for_umbrella_excess_liability',true) as umbrella_yes_no,
                json_extract_path_text(answers,'umbrella_excess_liability_limit',true) as requested_umbrella_limit,
   (CASE WHEN (uw.affiliate_id = 'N/A' and  uw.agent_id = 'N/A') then 'direct'
                    WHEN (uw.affiliate_id <> 'N/A' and  uw.agent_id = 'N/A') then 'affiliate'
                    else 'agent' end) as channel, purchased_quote_job_id
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
join (select distinct business_id, purchased_quote_job_id from dwh.quotes_policies_mlob qpm where lob_policy = 'GL') a on uw.business_id = a.business_id
where channel in ('direct','affiliate') and uw.lob = 'GL'
  and policy_status >=3
  and umbrella_yes_no = 'Yes'
  and purchased_quote_job_id is not null
 order by offer_creation_time desc),

   t2 as (select job_id,json_extract_path_text(json_extract_path_text(data_points,'packageData',true), 'coverages', true) as coveragesJSON,
       json_extract_path_text(json_extract_path_text(data_points,'packageData',true), 'version', true) as packageDataVersion
from s3_operational.rating_svc_prod_calculations where lob = 'GL'
                                                   and (coveragesJSON like '%UMBRELLA%' or coveragesJSON like '%EXCESS_LIABILITY%') and
                                                       creation_time > '2023-08-01')

select * from t1 left join t2 on t1.purchased_quote_job_id = t2.job_id limit 10

select distinct offer_creation_time, business_id,business_name, policy_status_name,revenue_in_12_months,uw.lob,decline_reasons,uw.state_code, cob, uw.city, uw.zip_code, uw.street,agent_name,agency_aggregator_name,current_agencytype,agent_email_address,
                json_extract_path_text(answers,'restaurant_type',true) as restaurant_type,
                json_extract_path_text(answers,'liquor_sales_exposure',true) as liquor_sales_exposure,
                json_extract_path_text(answers,'liquor_sales_yes_no',true) as liquor_sales_yes_no, affiliate_id
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
join dwh.v_agents a on uw.agent_id = a.agent_id
where cob in ('Restaurant') and agent_name = 'Jeffrey Beaver'
 order by offer_creation_time desc limit  100




select * from s3_operational.rating_svc_prod_calculations where lob = 'CP' and creation_time > '2023-08-01'

-- CP TIV decline
with declines as (
select a.*,
date(offer_creation_time) as creation_ds,
(CASE WHEN (affiliate_id = 'N/A' and  agent_id = 'N/A') then 'direct'
WHEN (affiliate_id <> 'N/A' and  agent_id = 'N/A') then 'affiliate'
else 'agent' end) as channel,
b.marketing_cob_group
from dwh.underwriting_quotes_data a
left join dwh.sources_test_cobs b
on a.cob = b.cob_name

where execution_status = 'DECLINE'
and decline_reasons not like '%","%'
and decline_reasons in ('["The limits you require are too high per our risk guidelines."]',
'["We can not accept your business per our retail underwriting guideline."]')
and offer_creation_time >= '2022-10-01'
and lob = 'CP'
),

crime as (
select distinct street,
zip_code_5digit,
nullif(json_extract_path_text(verisk_json_response, 'Ms3', 'Crime', 'AggregateCrimesAgainstProperty',
'IndexValuesUpto10', 'Current', true), '') as crime_score
from (select street,
cast(right('00000' + zip_code, 5) as varchar(5)) as zip_code_5digit,
creation_time,
verisk_json_response,
rank() over (partition by street, zip_code_5digit order by creation_time desc) as rnk
from riskmgmt_svc_prod.verisk_property_risk_request_response
) rank_table
where rnk = 1
),

credit as (
select business_id,
score as credit_score
from (select business_id,
creation_time,
score,
rank() over (partition by business_id order by creation_time desc) as rnk
from riskmgmt_svc_prod.risk_score_result where score is not null
) rank_table
where rnk = 1
)


select a.cob,
a.offer_id,
a.business_id,
creation_time,
channel,
decline_reasons,
b.state,
b.street,
b.city,
b.zip_code,
crime_score,
credit_score


from declines a
left join dwh.quotes_policies_mlob_dec b
on a.offer_id = b.offer_id
left join crime c
on b.zip_code = c.zip_code_5digit
and b.street = c.street
left join credit d
on a.business_id = d.business_id

where b.creation_time >= '2022-10-01' and d.business_id = '52e07ab66786c1bcbdcf7cbbfd00e5bb'
and b.lob_policy = 'CP'


select * from riskmgmt_svc_prod.exposure_base_revenue_results where business_id = '069bc78fd26c92540e2ab978046537f1'

select distinct offer_creation_time, business_id,business_name, policy_status_name,revenue_in_12_months,uw.lob,decline_reasons,uw.state_code, cob, uw.city, uw.zip_code, uw.street,agent_name,agency_aggregator_name,current_agencytype,
                (CASE WHEN (affiliate_id = 'N/A' and  uw.agent_id = 'N/A') then 'direct'
                    WHEN (affiliate_id <> 'N/A' and  uw.agent_id = 'N/A') then 'affiliate'
                    else 'agent' end) as channel
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
left join dwh.v_agents a on uw.agent_id = a.agent_id
where business_id = 'b89db1f87cd0dfba581a65eeaa91af2b'
 order by offer_creation_time desc

select distinct offer_creation_time::date, business_id,business_name, policy_status_name,revenue_in_12_months,uw.lob,decline_reasons,uw.state_code, cob, uw.city, uw.zip_code, uw.street,
                agent_name,agency_aggregator_name,current_agencytype,
                (CASE WHEN (affiliate_id = 'N/A' and  uw.agent_id = 'N/A') then 'direct'
                    WHEN (affiliate_id <> 'N/A' and  uw.agent_id = 'N/A') then 'affiliate'
                    else 'agent' end) as channel
from dwh.underwriting_quotes_data uw
-- join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
left join dwh.v_agents a on uw.agent_id = a.agent_id
where business_id = 'b89db1f87cd0dfba581a65eeaa91af2b'
 order by offer_creation_time desc

select * from dwh.quotes_policies_mlob qpm where business_id = 'b89db1f87cd0dfba581a65eeaa91af2b'
select * from dwh.underwriting_quotes_data uw where business_id = 'b89db1f87cd0dfba581a65eeaa91af2b'
select * from underwriting_svc_prod.lob_applications where lob_application_id in ('rT8GQ4bJjOc219M8','ZoaMcK4ZwNYngcOP')

select roles, agent_name, agent_id from db_data_science.v_all_agents_policies where agent_id not in (select agent_id from dwh.v_agents)

select distinct cob, count(*)
from dwh.underwriting_quotes_data
where policy_status >=3 and lob = 'GL'
group by cob

select * from dwh.underwriting_quotes_data
where policy_status >=3 and lob = 'GL' and cob = 'Pet Insurance Agent'





select distinct uw.business_id,business_name, agent_name
from dwh.underwriting_quotes_data uw
left join dwh.v_agents a on uw.agent_id = a.agent_id
where business_id = 'bb9fcafb0946336a67803dcabf103f9f'

select distinct offer_creation_time::date, uw.business_id,business_name, policy_status_name,revenue_in_12_months,uw.lob,decline_reasons,uw.state_code, cob, uw.city, uw.zip_code, uw.street, verisk_json_response,answers,
                nullif(json_extract_path_text(verisk_json_response, 'Ms3', 'Crime', 'AggregateCrimesAgainstProperty','IndexValuesUpto10', 'Current', true), '') as crime_score,
                json_extract_path_text(verisk_json_response, 'Ms1', 'Ppc', true) as PPC
from dwh.underwriting_quotes_data uw
left join riskmgmt_svc_prod.verisk_property_risk_request_response vr on vr.street = uw.street and vr.business_city = uw.city
left join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
where uw.lob = 'CP' and uw.cob = 'Restaurant' and offer_flow_type = 'APPLICATION'
order by offer_creation_time desc
limit 100

            select *,
                   json_extract_path_text(verisk_json_response, 'Ms1', 'GpsCoordinates', 'Latitude', true) original_Latitude,
       nullif(json_extract_path_text(verisk_json_response,'Ms4','FloorData','SquareFootage',true), '') original_sqft,
    nullif(json_extract_path_text(verisk_json_response,'Ms1','SquareFootage',true), '') original_sqft_str,
    CASE
        WHEN original_sqft_str IS NOT NULL THEN
            cast(REGEXP_SUBSTR(replace(original_sqft_str,',',''),'[0-9]+') as numeric)
        ELSE
            NULL
    END AS original_sqft_numeric,
                cast(nullif(json_extract_path_text(verisk_json_response, 'Ms1','Stories', 'Stories', true),'') as int) original_stories,
                                cast(nullif(json_extract_path_text(verisk_json_response, 'Ms1','Occupancy','Code', true),'') as int) original_occupancy_code,
                nullif(json_extract_path_text(verisk_json_response, 'Ms1','Occupancy','Description', true), '') original_occupancy_desc,
    coalesce(nullif(json_extract_path_text(verisk_json_response,'Ms1','ConstructionType','ReportedConstructionType',true), ''), nullif(json_extract_path_text(verisk_json_response,'Ms1','BuildingFireConstructionCode', 'Description', true), '')) original_construction_type,
    json_extract_path_text(json_extract_array_element_text(json_extract_path_text(verisk_json_response,'Ms1','ConstructionType','ISOConstructionTypes',true),0), 'ConstructionTypeCodeDescription', true) original_iso_construction_type,
    --cast(json_extract_path_text(json_extract_array_element_text(json_extract_path_text(verisk_json_response,'Ms1','ConstructionType','ISOConstructionTypes',true),0), 'ConfidenceScore', true) as numeric) original_iso_cctype_confidence,
    nullif(json_extract_path_text(verisk_json_response,'Ms3','Sprinklered',true), '') original_sprinklered,
    cast(nullif(json_extract_path_text(verisk_json_response,'Ms1','YearBuilt',true), '') as numeric) original_year_built,
    cast(nullif(json_extract_path_text(verisk_json_response,'Ms4','EffectiveYearBuilt',true), '') as numeric) original_eff_year_built,
    nullif(json_extract_path_text(verisk_json_response,'Ms1','Ppc',true), '') original_ms1_ppc,
    --cast(nullif(json_extract_path_text(verisk_json_response,'Ms4','Ppc','Ppc',true), '') as int) ms4_ppc,
    --cast(nullif(json_extract_path_text(verisk_json_response,'Ms3','Bcegs',true), '') as int) bcegs,
    cast(nullif(json_extract_path_text(verisk_json_response,'Ms3','Crime','AggregateCrimesAgainstProperty','IndexValuesUpto10','Current',true), '') as int) original_current_crime_score,
    cast(nullif(json_extract_path_text(verisk_json_response,'Ms3','Crime','AggregateCrimesAgainstProperty','IndexValuesUpto10','Past',true), '') as int)  original_past_crime_score,
    cast(nullif(json_extract_path_text(verisk_json_response,'Ms3','Crime','AggregateCrimesAgainstProperty','IndexValuesUpto10','Forecasted',true), '') as int)  original_forecast_crime_score,
    json_extract_path_text(verisk_json_response,'Address','StreetAddress1',true) original_StreetAddress1,
    json_extract_path_text(verisk_json_response,'Address','City',true) original_City,
    json_extract_path_text(verisk_json_response,'Address','Zip',true) original_Zip
    from riskmgmt_svc_prod.verisk_property_risk_request_response
limit 100


select
 app.lob_application_id,
  app.prospect_id,
  app.opportunity_id,
  pros.business_id,
  app.lob,
  json_extract(app.metadata, "$.generationReason"),
  json_unquote(json_extract(apd.data_points,json_unquote(replace(json_search(apd.data_points,'one', 'revenue_next_12_months'),'dataPointId','value')))) AS revenue,
  app.creation_time,
 elig.execution_status,
  elig.risk_required_input_overrides,
  elig.risk_input_override_id,
  elig.checks
 from underwriting_svc_prod.lob_applications app
 join prospects pros on app.prospect_id = pros.prospect_id
 join underwriting_svc_prod.applicant_data apd on apd.applicant_data_id = app.applicant_data_id
 left join offers on app.lob_application_id = offers.application_id
 left join eligibility_checks elig on offers.offer_id = elig.offer_id
 where pros.business_id = '069bc78fd26c92540e2ab978046537f1'
 order BY app.creation_time

select * from underwriting_svc_prod.lob_applications app
 join underwriting_svc_prod.prospects pros on app.prospect_id = pros.prospect_id
 join underwriting_svc_prod.applicant_data apd on apd.applicant_data_id = app.applicant_data_id
left join underwriting_svc_prod.offers on app.lob_application_id = offers.application_id
where business_id = '4185d7162da8ad633920b2f6b731780c' and app.lob = 'GL'


select * from riskmgmt_svc_prod.verisk_property_risk_request_response where zip_code = '97227' limit 100

select * from dwh.underwriting_quotes_data limit 10

with credit as (
select business_id,
score as credit_score
from (select business_id,
creation_time,
score,
rank() over (partition by business_id order by creation_time desc) as rnk
from riskmgmt_svc_prod.risk_score_result where score is not null
) rank_table
where rnk = 1
)

select * from credit where business_id = 'a79bf50131a8908597d5d82bb1abd443'

with credit as (
select business_id,
score as credit_score
from (select business_id,
creation_time,
score,
rank() over (partition by business_id order by creation_time desc) as rnk
from riskmgmt_svc_prod.risk_score_result where score is not null
) rank_table
where rnk = 1
)
select distinct offer_creation_time::date, uw.business_id,business_name, policy_status_name,revenue_in_12_months,uw.lob,decline_reasons,uw.state_code, cob, uw.city, uw.zip_code, uw.street, verisk_json_response,answers,
                json_extract_path_text(answers,'bpp_limit',true) as bpp_limit,
                json_extract_path_text(answers,'bpp_kitchen_equipment',true) as bpp_kitchen_equipment,
                json_extract_path_text(answers,'bpp_tables_and_seating',true)  as bpp_tables_and_seating,
                json_extract_path_text(answers,'bpp_building_improvements_and_remodeling',true)   as bpp_building_improvements_and_remodeling,
                json_extract_path_text(answers,'bpp_pos_system_cash_register',true)   as bpp_pos_system_cash_register,
                json_extract_path_text(answers,'bpp_other',true)  as bpp_other,
                nullif(json_extract_path_text(verisk_json_response, 'Ms3', 'Crime', 'AggregateCrimesAgainstProperty','IndexValuesUpto10', 'Current', true), '') as crime_score,
                json_extract_path_text(verisk_json_response, 'Ms1', 'Ppc', true) as PPC,credit_score
from dwh.underwriting_quotes_data uw
left join riskmgmt_svc_prod.verisk_property_risk_request_response vr on vr.street = uw.street and vr.business_city = uw.city
left join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
left join credit on uw.business_id = credit.business_id
where uw.lob = 'CP' and uw.cob = 'Restaurant' and offer_flow_type = 'APPLICATION'
  and decline_reasons ilike '%protection%' and offer_creation_time >= '2023-9-1'
order by offer_creation_time desc

-- tiv query from Seb
SET json_serialization_enable TO true;

with base as (
select distinct
       a.business_id,
       a.policy_status,
       a.policy_status_name,
       --a.yearly_premium,
       a.state_code,
       a.cob,
       a.offer_creation_time,
       a.execution_status,
       a.decline_reasons,
       a.street,
       a.city,
       a.zip_code,
       a.offer_id,
       case when (decline_reasons like '%We cannot currently offer property insurance for this building''s Public Protection Classification%'
           or decline_reasons like '%Your limits are too high per our risk guidelines%'
           or decline_reasons like '%The limits you require are too high per our risk guidelines%')
           then 1 else 0 end decline_reason_from_q2uw,
       b.credit_score,
       b.crime_score,
       b.construction_type,
       b.building_limit,
       b.bpp_limit,
       b.revenue_in_12_months,
       b.sprinklers,
       b.dateid
from dwh.underwriting_quotes_data a
left join cortex_prod.dwh__submissions_cp b
on (a.offer_id = b.first_offer_id
or a.offer_id = b.last_offer_id)
left join dwh.sources_test_cobs c
        on a.cob = c.cob_name

where a.lob = 'CP'
and a.offer_creation_time >= '2023-08-28'
and a.offer_creation_time <= '2023-10-28'
and a.offer_flow_type = 'APPLICATION'
and c.marketing_cob_group = 'Food & beverage'


)

select *
from base
;

select transaction_type, transaction_sub_type, tx_effective_date, policy_start_date,channel, agent_id
from dwh.policy_transactions
where business_id = '31440355a0be47883385e0435157568a'
and lob = 'GL'
order by tx_effective_date

select job_id, total_premium
from
        s3_operational.rating_svc_prod_calculations
where creation_time >= 2023-10-31
limit 10

select total_premium
from
        s3_operational.rating_svc_prod_calculations
where creation_time >= 2023-10-31 and job_id = '109118989'
limit 10

with renewal as (
    select *--, case when agent_id = 'N/A' then 1 else 0 end agent_removed
    from dwh.quotes_policies_mlob
    where offer_flow_type = 'RENEWAL'
      and highest_policy_status >= 4 --and agent_id = 'N/A'
      and creation_time >= '2022-01-01')
        ,
     agent_renewal as (
         select *--, case when agent_id = 'N/A' then 1 else 0 end agent_removed
         from db_data_science.v_all_agents_policies
         where policy_category = 'renewal_active'
           and eventtime >= '2022-01-01')
        ,
     agent_sales as (
         select *
         from db_data_science.v_all_agents_policies
         where policy_category = 'new'
           and eventtime >= '2022-01-01')
select count(distinct s.business_id + s.lob) as total_policies,
       count(distinct case when r.business_id + r.lob_policy is not null then s.business_id + s.lob end) as all_renewals,
       count(distinct case when re.business_id + re.lob is null and r.business_id + r.lob_policy is not null
                              then s.business_id + s.lob end) as renewals_no_agent
from agent_sales s
         left join agent_renewal re on re.business_id = s.business_id and re.lob = s.lob
         left join renewal r on r.business_id = s.business_id and r.lob_policy = s.lob

select * from dwh.underwriting_quotes_data where business_id = 'ffee4876f40a1aafd7f64e40828b0591' and lob = 'GL' and policy_status >3


select transaction_type, transaction_sub_type, tx_effective_date, policy_start_date,channel, agent_id
from dwh.policy_transactions
where business_id = 'ffee4876f40a1aafd7f64e40828b0591'
and lob = 'GL'
order by tx_effective_date


with agent_policies as (
select business_id from dwh.policy_transactions where channel = 'Agent' and lob = 'GL' and cob_name in  ('Restaurant')),

t1 as (select pt.business_id, count (distinct pt.channel) as unique_channel, count (pt.channel) as p_count
       from dwh.policy_transactions pt
where pt.lob = 'GL' and pt.business_id in (select business_id from agent_policies) and transaction_type = 'BIND'
group by pt.business_id)

select * from t1 where p_count >= 2 and unique_channel > 1



select policy_id, transaction_type, transaction_sub_type, tx_effective_date, policy_start_date,channel, agent_id
from dwh.policy_transactions
where business_id = '837f6e860e986ff04851b2094c850242'
and lob = 'GL'
order by tx_effective_date

select *
from dwh.policy_transactions
where business_id = '2940e8cda18a2b9e119ea34d110228d1'
and lob = 'GL'
order by tx_effective_date

select distinct offer_creation_time, business_id,business_name, policy_status_name,revenue_in_12_months,uw.lob,uw.state_code, cob, decline_reasons,uw.city, uw.zip_code, uw.street,agent_name,agency_aggregator_name,current_agencytype,
                json_extract_path_text(answers,'restaurant_type',true) as restaurant_type,
                json_extract_path_text(answers,'liquor_sales_exposure',true) as liquor_sales_exposure,
                json_extract_path_text(answers,'liquor_sales_yes_no',true) as liquor_sales_yes_no
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
left join dwh.v_agents a on uw.agent_id = a.agent_id
where uw.lob != 'CA'  and cob in ('Restaurant')
  and business_id = '318584d2f733886ace1e858a0a67ac81'
order by offer_creation_time desc limit  300

select distinct offer_creation_time::date, uw.business_id,business_name, policy_status_name,revenue_in_12_months,uw.lob,decline_reasons,uw.state_code, cob, uw.city, uw.zip_code, uw.street, verisk_json_response,answers,
                json_extract_path_text(answers,'bpp_limit',true) as bpp_limit,
                json_extract_path_text(answers,'bpp_kitchen_equipment',true) as bpp_kitchen_equipment,
                json_extract_path_text(answers,'bpp_tables_and_seating',true)  as bpp_tables_and_seating,
                json_extract_path_text(answers,'bpp_building_improvements_and_remodeling',true)   as bpp_building_improvements_and_remodeling,
                json_extract_path_text(answers,'bpp_pos_system_cash_register',true)   as bpp_pos_system_cash_register,
                json_extract_path_text(answers,'bpp_other',true)  as bpp_other,
                nullif(json_extract_path_text(verisk_json_response, 'Ms3', 'Crime', 'AggregateCrimesAgainstProperty','IndexValuesUpto10', 'Current', true), '') as crime_score,
                json_extract_path_text(verisk_json_response, 'Ms1', 'Ppc', true) as PPC
from dwh.underwriting_quotes_data uw
left join riskmgmt_svc_prod.verisk_property_risk_request_response vr on vr.street = uw.street and vr.business_city = uw.city
left join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
where uw.lob = 'CP' and uw.cob = 'Restaurant' and offer_flow_type = 'APPLICATION'
  and uw.business_id = '318584d2f733886ace1e858a0a67ac81'
order by offer_creation_time desc
limit 50

SET json_serialization_enable TO true;

with base as (
select distinct
       a.business_id,
       a.policy_status,
       a.policy_status_name,
       --a.yearly_premium,
       a.state_code,
       a.cob,
       a.offer_creation_time,
       a.execution_status,
       a.decline_reasons,
       a.street,
       a.city,
       a.zip_code,
       a.offer_id,
       case when (decline_reasons like '%We cannot currently offer property insurance for this building''s Public Protection Classification%'
           or decline_reasons like '%Your limits are too high per our risk guidelines%'
           or decline_reasons like '%The limits you require are too high per our risk guidelines%')
           then 1 else 0 end decline_reason_from_q2uw,
       b.credit_score,
       b.crime_score,
       b.construction_type,
       b.building_limit,
       b.bpp_limit,
       b.revenue_in_12_months,
       b.sprinklers,
       b.dateid
from dwh.underwriting_quotes_data a
left join cortex_prod.dwh__submissions_cp b
on (a.offer_id = b.first_offer_id
or a.offer_id = b.last_offer_id)
left join dwh.sources_test_cobs c
        on a.cob = c.cob_name

where a.lob = 'CP'
and a.offer_creation_time >= '2023-08-28'
and a.offer_creation_time <= '2023-10-28'
and a.offer_flow_type = 'APPLICATION'
and c.marketing_cob_group = 'Food & beverage'

),
    crime as (
         select distinct street,
                         zip_code_5digit,
                         nullif(json_extract_path_text(verisk_json_response, 'Ms3', 'Crime', 'AggregateCrimesAgainstProperty',
                                                       'IndexValuesUpto10', 'Current', true), '') as crime_score
         from (select street,
                      cast(right('00000' + zip_code, 5) as varchar(5))                               as zip_code_5digit,
                      creation_time,
                      verisk_json_response,
                      rank() over (partition by street, zip_code_5digit order by creation_time desc) as rnk
               from riskmgmt_svc_prod.verisk_property_risk_request_response
              ) rank_table
         where rnk = 1
     ),

     credit as (
         select business_id,
                score as credit_score
         from (select business_id,
                      creation_time,
                      score,
                      rank() over (partition by business_id order by creation_time desc) as rnk
               from riskmgmt_svc_prod.risk_score_result where score is not null
              ) rank_table
         where rnk = 1
     )

select a.business_id,
       policy_status,
       policy_status_name,
       a.state_code,
       cob,
       offer_creation_time,
       execution_status,
       decline_reasons,
       a.street,
       a.city,
       a.zip_code,
       offer_id,
       decline_reason_from_q2uw,
       coalesce(a.credit_score, b.credit_score) credit_score,
       coalesce(a.crime_score, cast(c.crime_score as int)) crime_score,
       a.construction_type,
       a.building_limit,
       a.bpp_limit,
       a.revenue_in_12_months,
       a.sprinklers,
       a.dateid
from base a
left join credit b
on a.business_id = b.business_id
left join crime c
on lower(a.street) = lower(c.street)

with contacts as (select p.business_id,
json_extract_path_text(p.business_details, 'applicantfirstname') as first_name,
json_extract_path_text(p.business_details, 'applicantlastname') as last_name,
json_extract_path_text(p.business_details, 'telephonenumber') as phone_number,
json_extract_path_text(p.business_details, 'emailaddress') as business_email
from underwriting_svc_prod.prospects p)

select distinct offer_creation_time::date, uw.business_id,business_name, policy_status_name,revenue_in_12_months,uw.lob,decline_reasons,uw.state_code, cob, uw.city, uw.zip_code, uw.street,business_email,
         affiliate_id
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
left join contacts on contacts.business_id = uw.business_id
where affiliate_id in ('5100','7400','6497') and offer_creation_time >= '2024-05-17'
 order by offer_creation_time desc

select distinct offer_creation_time, business_id,business_name, policy_status_name,revenue_in_12_months,uw.lob,uw.state_code, cob, yearly_premium,uw.city, uw.zip_code, uw.street,
                json_extract_path_text(answers,'restaurant_type',true) as restaurant_type,
                json_extract_path_text(answers,'liquor_sales_exposure',true) as liquor_sales_exposure,
                json_extract_path_text(answers,'liquor_sales_yes_no',true) as liquor_sales_yes_no
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
where json_extract_path_text(metadata, 'createdFromOneClickFlowType') like '%one_click%' and creation_time >= '2023-11-01' and policy_status >= 3
order by offer_creation_time asc

select * from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
where business_id = '2e204fd23d1e3f626cd89df58c58261e'

select decline_reasons,COUNT (DISTINCT business_id) as count, avg(revenue_in_12_months)
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
where uw.lob = 'CP' and cob = 'Caterer' and offer_creation_time >= '2023-01-01'
group by decline_reasons
order by count desc

select distinct offer_creation_time, business_id,business_name, policy_status_name,revenue_in_12_months,uw.lob,uw.state_code, cob, decline_reasons,uw.city, uw.zip_code, uw.street
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
where  cob = 'Restaurant' and offer_creation_time >= '2025-01-01' and decline_reasons = '["Your business is not accepted due to high fire hazard"]'
order by offer_creation_time desc

select distinct offer_creation_time, business_id,business_name, policy_status_name,revenue_in_12_months,uw.lob,uw.state_code, cob, yearly_premium,uw.city, uw.zip_code, uw.street,agent_name,agency_name,agency_aggregator_name,current_agencytype
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
left join dwh.v_agents a on uw.agent_id = a.agent_id
where cob in ('Bakery') and uw.state_code = 'CA' and offer_creation_time >= '2024-06-01' and policy_status >3
order by offer_creation_time desc

select * from dwh.v_agents where agent_id = 'Fvg8NbdGlfzCJXjB'

select cob, lob, execution_status,decline_reasons, count (distinct business_id) as declines
from dwh.underwriting_quotes_data uw
where cob in ('Restaurant')
  and offer_creation_time >= '2025-01-01'
  and uw.lob != 'CA'
  and execution_status in ('DECLINE')
  and offer_flow_type in ('APPLICATION')
group by cob, lob, execution_status,decline_reasons
order by cob, lob,execution_status,declines desc

select cob, lob, execution_status,decline_reasons, count (distinct business_id) as declines
from dwh.underwriting_quotes_data uw
where cob in ('Clothing Store','Retail Stores','E-Commerce','Electronics Store',
              'Convenience Stores','Pet Groomers','Screen Printing and T Shirt Printing',
              'AV Equipment Rental for Events','Specialty Food','Farmers Market','Pet Training',
              'Furniture Store','Dog Walker','Jewelry Store','Laundromat','Knife Sharpening',
              'Bike Rentals','Bookstore','Signmaking','Arts and Crafts Store','Wholesalers',
              'Candle Store','Pet Boarding','Bike Shop','Medical Supplies Store','Dumpster Rental',
              'Self Storage','Home and Garden Retailer','Hobby Shop','Pet Stores','Dog Training',
              'Games and Concession Rental','Sporting Goods Retailer','Florist','Pet Services',
              'Veterinarians','Grill Services','Nurseries and Gardening Shop','Furniture Rental',
              'Vitamins and Supplements Store','Candy Stores','Lighting Store','Flea Markets','Animal Trainers',
              'Department Stores','Toy Store','Popcorn Shops','Auction House','Discount Store','Eyewear and Optician Store',
              'Fabric Store','Hardware Store','Horse Equipment Shop','Luggage Store','Packing Supplies Store',
              'Paint Stores','Party Equipment Rentals','Shopping Center')
  and cob not in  ('Party Equipment Rentals', 'Clothing Store', 'Retail Stores', 'E-Commerce', 'Electronics Store', 'Convenience Stores', 'Pet Groomers', 'Screen Printing and T Shirt Printing', 'AV Equipment Rental for Events', 'Specialty Food', 'Farmers Market', 'Pet Training', 'Furniture Store', 'Dog Walker', 'Jewelry Store', 'Laundromat', 'Knife Sharpening', 'Bike Rentals', 'Bookstore', 'Signmaking', 'Arts and Crafts Store', 'Wholesalers', 'Candle Store', 'Pet Boarding', 'Bike Shop', 'Medical Supplies Store', 'Dumpster Rental', 'Self Storage', 'Home and Garden Retailer', 'Hobby Shop', 'Pet Stores', 'Dog Training', 'Games and Concession Rental', 'Sporting Goods Retailer', 'Florist', 'Pet Services', 'Veterinarians', 'Grill Services', 'Nurseries and Gardening Shop', 'Furniture Rental', 'Vitamins and Supplements Store', 'Candy Stores', 'Lighting Store', 'Flea Markets', 'Animal Trainers', 'Department Stores', 'Toy Store', 'Popcorn Shops')
  and offer_creation_time >= '2023-01-01'
  and uw.lob != 'CA'
  and execution_status in ('DECLINE')
  and offer_flow_type in ('APPLICATION')
group by cob, lob, execution_status,decline_reasons
order by cob, lob,execution_status,declines desc

select distinct offer_creation_time, business_id,business_name, policy_status_name,revenue_in_12_months,yearly_premium, uw.lob,uw.state_code, cob, yearly_premium,uw.city, uw.zip_code, uw.street,agent_name,agency_name,agency_aggregator_name,current_agencytype
    from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
left join dwh.v_agents a on uw.agent_id = a.agent_id
where offer_creation_time >= '2023-11-29' and uw.lob = 'CP' and  cob in ('Bakery', 'Caterer','Coffee Shop','Restaurant')

select distinct business_id, json_extract_path_text(package_data,'basic','coverages','BUSINESS_PERSONAL_PROPERTY','limits','OCCURRENCE') as bpp_limit
from underwriting_svc_prod.lob_applications la
         join dwh.quotes_policies_mlob m on m.lob_application_id = la.lob_application_id
         left join underwriting_svc_prod.offers o on o.offer_id = m.offer_id
where lob_policy = 'CP' and m. creation_time>='2023-11-30'
and cob_group = 'Food & beverage'


select distinct business_id, cob,highest_yearly_premium,revenue_in_12_months,
    nullif(json_extract_path_text(package_data,'pro','coverages','DAMAGE_TO_RENTED_PREMISES','limits','PER_PREMISE'), '') as dtrp_limit
from underwriting_svc_prod.lob_applications la
         join dwh.quotes_policies_mlob m on m.lob_application_id = la.lob_application_id
         left join underwriting_svc_prod.offers o on o.offer_id = m.offer_id
where highest_policy_status >=3 and lob_policy = 'GL'and m. creation_time>='2023-12-06'




select distinct business_id, package_data
from underwriting_svc_prod.lob_applications la
         join dwh.quotes_policies_mlob m on m.lob_application_id = la.lob_application_id
         left join underwriting_svc_prod.offers o on o.offer_id = m.offer_id
where lob_policy = 'GL' and m. creation_time>='2023-12-26'
and cob_group = 'Food & beverage' limit 10




with t1 as (select distinct offer_creation_time::date, uw.business_id,quote_job_id,business_name, policy_status_name,yearly_premium,uw.lob,uw.state_code, uw.cob, uw.city, uw.zip_code, uw.street,
   (CASE WHEN (uw.affiliate_id = 'N/A' and  uw.agent_id = 'N/A') then 'direct'
                    WHEN (uw.affiliate_id <> 'N/A' and  uw.agent_id = 'N/A') then 'affiliate'
                    else 'agent' end) as channel
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
-- join (select distinct business_id, purchased_quote_job_id from dwh.quotes_policies_mlob qpm where lob_policy = 'CP') a on uw.business_id = a.business_id
where channel in ('agent') and uw.lob = 'CP' and uw.cob in ('Restaurant','Bakery','Caterer','Coffee Shop')
   and offer_creation_time > '2023-11-30'
 order by offer_creation_time desc),

   t2 as (select job_id,json_extract_path_text(data_points,'packageData','coverages','BUSINESS_PERSONAL_PROPERTY','limits','OCCURRENCE') as bpp_limit
from s3_operational.rating_svc_prod_calculations where lob = 'CP')

select distinct offer_creation_time::date, business_id,business_name, policy_status_name,yearly_premium,lob,state_code, cob, city, zip_code, street, bpp_limit
from t1 left join t2 on t1.quote_job_id = t2.job_id
 order by offer_creation_time desc

with t1 as (select distinct offer_creation_time::date, uw.business_id,quote_job_id,business_name, policy_status_name,yearly_premium,uw.lob,uw.state_code, uw.cob, uw.city, uw.zip_code, uw.street,
   (CASE WHEN (uw.affiliate_id = 'N/A' and  uw.agent_id = 'N/A') then 'direct'
                    WHEN (uw.affiliate_id <> 'N/A' and  uw.agent_id = 'N/A') then 'affiliate'
                    else 'agent' end) as channel
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
-- join (select distinct business_id, purchased_quote_job_id from dwh.quotes_policies_mlob qpm where lob_policy = 'CP') a on uw.business_id = a.business_id
where channel in ('agent') and uw.lob = 'GL'
   and offer_creation_time >= '2024-03-03' and policy_status >=1 and uw.cob = 'Restaurant'
 order by offer_creation_time desc),

   t2 as (select job_id,json_extract_path_text(data_points,'packageData','coverages','DAMAGE_TO_RENTED_PREMISES','limits','PER_PREMISE') as dtrp_limit
from s3_operational.rating_svc_prod_calculations where lob = 'GL')

select distinct offer_creation_time::date, business_id,business_name, policy_status_name,yearly_premium, cob, dtrp_limit
from t1 left join t2 on t1.quote_job_id = t2.job_id
where dtrp_limit > 100000
 order by offer_creation_time desc



select job_id,json_extract_path_text(data_points,'packageData','coverages','BUSINESS_PERSONAL_PROPERTY','limits','OCCURRENCE') as bpp_limit
from s3_operational.rating_svc_prod_calculations where lob = 'CP' and job_id = '114157007'

select cob, lob, execution_status, count (distinct business_id)
from dwh.underwriting_quotes_data uw
where cob in ('Bakery', 'Caterer','Coffee Shop','Food Truck','Grocery Store','Restaurant',
              'Clothing Store','Retail Stores','E-Commerce','Electronics Store',
              'Convenience Stores','Pet Groomers','Screen Printing and T Shirt Printing',
              'AV Equipment Rental for Events','Specialty Food','Farmers Market','Pet Training',
              'Furniture Store','Dog Walker','Jewelry Store','Laundromat','Knife Sharpening',
              'Bike Rentals','Bookstore','Signmaking','Arts and Crafts Store','Wholesalers',
              'Candle Store','Pet Boarding','Bike Shop','Medical Supplies Store','Dumpster Rental',
              'Self Storage','Home and Garden Retailer','Hobby Shop','Pet Stores','Dog Training',
              'Games and Concession Rental','Sporting Goods Retailer','Florist','Pet Services',
              'Veterinarians','Grill Services','Nurseries and Gardening Shop','Furniture Rental',
              'Vitamins and Supplements Store','Candy Stores','Lighting Store','Flea Markets','Animal Trainers',
              'Department Stores','Toy Store','Popcorn Shops','Auction House','Discount Store','Eyewear and Optician Store',
              'Fabric Store','Hardware Store','Horse Equipment Shop','Luggage Store','Packing Supplies Store',
              'Paint Stores','Party Equipment Rentals','Shopping Center',
             'Insurance Agent','Business Consulting','Photographer','IT Consulting or Programming',
              'Accountant','Other Consulting','Property Manager','Marketing',
              'Home Inspectors','Salesperson','Real Estate Agent','Real Estate Brokers',
              'Engineer','Audio and Video Equipment Technicians','Legal Service','Interior Designer',
              'Travel Guides','Videographers','Computer Programmers','Travel Agency','Graphic Designers',
              'Architect','Training and Development Specialists','Administrative Services Managers',
              'Writer','Computer and Information Systems Managers','Claims Adjuster')
  and offer_creation_time >= '2023-01-01' and uw.lob != 'CA'
  and execution_status in ('DECLINE','SUCCESS')
  and offer_flow_type in ('APPLICATION')
group by cob, lob, execution_status
order by cob, lob,execution_status


select distinct offer_creation_time::date, uw.business_id,business_name, policy_status_name,uw.lob,uw.state_code, uw.cob, uw.city, uw.zip_code, uw.street
from dwh.underwriting_quotes_data uw where agent_id in (select agent_id  from dwh.underwriting_quotes_data uw
                 where business_id = '52a30c35fea6ba1e02fe46ed588b48ef')





with t1 as (select distinct offer_creation_time::date, uw.business_id,quote_job_id,business_name, policy_status_name,uw.lob,uw.state_code, uw.cob, uw.city, uw.zip_code, uw.street
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
-- join (select distinct business_id, purchased_quote_job_id from dwh.quotes_policies_mlob qpm where lob_policy = 'CP') a on uw.business_id = a.business_id
where json_extract_path_text(metadata, 'createdFromOneClickFlowType') like '%one_click%' and creation_time >= '2023-11-01'
 and uw.lob = 'GL' and uw.cob in ('Restaurant','Bakery','Caterer','Coffee Shop')
  order by offer_creation_time desc),

   t2 as (select job_id, json_extract_path_text(data_points,'packageData','coverages','LIQUOR_LIABILITY_COVERAGE','limits','AGGREGATE', true) as gl_liquor_liability_aggregate
FROM s3_operational.rating_svc_prod_calculations)

select offer_creation_time, business_id,business_name, policy_status_name,lob,state_code, cob, city, zip_code, street,gl_liquor_liability_aggregate
from t1 left join t2 on t1.quote_job_id = t2.job_id
 order by offer_creation_time asc


select distinct offer_creation_time::date, uw.business_id,business_name, policy_status_name,revenue_in_12_months,yearly_premium,answers,uw.lob,uw.state_code, uw.cob, uw.city, uw.zip_code, uw.street
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
where business_id = '0b6b049b2431244459aecd58f8c56ab2'
 order by offer_creation_time desc



select * from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
where business_id = '0b6b049b2431244459aecd58f8c56ab2'
 order by offer_creation_time desc

select distinct offer_creation_time::date, uw.business_id,business_name, policy_status_name,revenue_in_12_months,uw.lob,decline_reasons,uw.state_code, cob, uw.city, uw.zip_code, uw.street, verisk_json_response,answers,
                nullif(json_extract_path_text(verisk_json_response, 'Ms3', 'Crime', 'AggregateCrimesAgainstProperty','IndexValuesUpto10', 'Current', true), '') as crime_score,
                json_extract_path_text(verisk_json_response, 'Ms1', 'Ppc', true) as PPC
from dwh.underwriting_quotes_data uw
left join riskmgmt_svc_prod.verisk_property_risk_request_response vr on vr.street = uw.street and vr.business_city = uw.city
left join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
where uw.business_id = '13e626a35c7d95855e3befcad80eeda3'
order by offer_creation_time desc
limit 50


with base as (select *
from dwh.underwriting_quotes_data
where policy_reference is not null
-- and policy_status>=4
and offer_creation_time>='2024-01-10'),
     rating_join as (select
     json_extract_path_text(calculation_summary, 'lob specific', '_gl_hired_auto_coverage_premium', true) hired_auto_coverage_premium,
    json_extract_path_text(calculation_summary, 'lob specific', '_gl_non_owned_auto_coverage_premium', true) non_owned_auto_coverage_premium,
        b.*
         from  base b
         join s3_operational.rating_svc_prod_calculations r
     on b.quote_job_id = r.job_id)
select *
from rating_join;

select
date_trunc('day', offer_creation_time),
       count(distinct policy_reference),
              count(distinct case when non_owned_auto_coverage_premium <> '0.0' and non_owned_auto_coverage_premium <> '' then policy_reference end )
from db_data_science.gl_hnoa_base
where lob = 'GL'
group by 1
order by 1

with base as (select *
from dwh.underwriting_quotes_data
where policy_reference is not null
-- and policy_status>=4
and offer_creation_time>='2024-01-10'),

    rating_join as (select
     json_extract_path_text(calculation_summary, 'lob specific', '_gl_hired_auto_coverage_premium', true) hired_auto_coverage_premium,
    json_extract_path_text(calculation_summary, 'lob specific', '_gl_non_owned_auto_coverage_premium', true) non_owned_auto_coverage_premium,
        b.*
         from  base b
         join s3_operational.rating_svc_prod_calculations r
     on b.quote_job_id = r.job_id)

select distinct business_id, business_name, cob, yearly_premium, non_owned_auto_coverage_premium, hired_auto_coverage_premium
from rating_join where non_owned_auto_coverage_premium <> '0.0' and non_owned_auto_coverage_premium <> ''

select count (distinct business_id), sum(yearly_premium)
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
left join dwh.v_agents a on uw.agent_id = a.agent_id
where uw.lob = 'GL'  and cob in ('Restaurant') and uw.state_code not in ('CA','MI')
  and policy_status >= 4 and offer_creation_time >= '2024-01-10'  and agent_name is not null

select distinct business_id
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
left join dwh.v_agents a on uw.agent_id = a.agent_id
where uw.lob = 'GL'  and cob in ('Restaurant') and uw.state_code not in ('CA','MI')
  and policy_status >= 4 and offer_creation_time >= '2024-01-10'  and agent_name is not null

select * from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
where business_id = '59dd33187360fd388e3195add6e3ac91'



select distinct offer_creation_time::date, uw.business_id,business_name, policy_status_name,revenue_in_12_months,uw.lob,decline_reasons,uw.state_code, cob, uw.city, uw.zip_code, uw.street, answers
from dwh.underwriting_quotes_data uw
left join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
where uw.lob = 'GL' and uw.cob = 'Grocery Store' and offer_flow_type = 'APPLICATION' and decline_reasons = '["You sell products that cannot be covered by this policy."]'
order by offer_creation_time desc
limit 100

with base as (select *
from dwh.underwriting_quotes_data
where policy_reference is not null
and policy_status>=4
and offer_creation_time>='2024-01-10'),
     rating_join as (select
     json_extract_path_text(calculation_summary, 'lob specific', '_gl_hired_auto_coverage_premium', true) hired_auto_coverage_premium,
    json_extract_path_text(calculation_summary, 'lob specific', '_gl_non_owned_auto_coverage_premium', true) non_owned_auto_coverage_premium,
        b.*
         from  base b
         join s3_operational.rating_svc_prod_calculations r
     on b.quote_job_id = r.job_id)
select *
from rating_join;


select * from dwh.quotes_policies_mlob qpm where business_id = 'e84c8d2e37b0c83f638af36f9898ec65'



with base as (select *
from dwh.underwriting_quotes_data
where policy_reference is not null
and policy_status>=4
and offer_creation_time>='2024-01-08'),

    rating_join as (select
     json_extract_path_text(calculation_summary, 'lob specific', '_gl_hired_auto_coverage_premium', true) hired_auto_coverage_premium,
    json_extract_path_text(calculation_summary, 'lob specific', '_gl_non_owned_auto_coverage_premium', true) non_owned_auto_coverage_premium,
        b.*
         from  base b
         join s3_operational.rating_svc_prod_calculations r
     on b.quote_job_id = r.job_id)

  select business_id, business_name, cob, yearly_premium, non_owned_auto_coverage_premium, hired_auto_coverage_premium
from rating_join where non_owned_auto_coverage_premium <> '0.0' and non_owned_auto_coverage_premium <> ''



select *,
       json_extract_path_text(json_args,'lob_app_json','location.catering_delivery_by_employee',true) as employee_cater,
       json_extract_path_text(json_args,'lob_app_json','location.catering_delivery_by_owner',true) as owner_cater,
       json_extract_path_text(json_args,'lob_app_json','location.food_delivery_by_employee',true) as employee_deliver,
       json_extract_path_text(json_args,'lob_app_json','location.food_delivery_by_owner',true) as owner_deliver,
       json_extract_path_text(json_args,'lob_app_json','mvr_driver_violation_monitoring',true) as mvr_monitoring,
       json_extract_path_text(json_args,'lob_app_json','have_commercial_auto_policy',true) as have_commercial_auto_policy,
      json_extract_path_text(json_args,'lob_app_json','driver_age_baseline',true) as driver_age_baseline,
       json_extract_path_text(json_args,'lob_app_json','drivers_personal_coverage_baseline',true) as drivers_personal_coverage_baseline
from dwh.quotes_policies_mlob qpm where lob_policy = 'GL' and employee_cater is not null and highest_policy_status >=3 and creation_time >= '2024-01-10' limit 10






select distinct json_args from dwh.quotes_policies_mlob qpm where business_id = '243ccce2eb48bb7bfc61db3073c28b7c' and lob_policy = 'CP'

select business_id as biz_id, json_args,json_extract_path_text(json_args,'location.catering_delivery_by_employee',true) as employee_cater,
       json_extract_path_text(json_args,'lob_app_json','location.catering_delivery_by_owner',true) as owner_cater,
       json_extract_path_text(json_args,'lob_app_json','location.food_delivery_by_employee',true) as employee_deliver,
       json_extract_path_text(json_args,'lob_app_json','location.food_delivery_by_owner',true) as owner_deliver,
       json_extract_path_text(json_args,'lob_app_json','mvr_driver_violation_monitoring',true) as mvr_monitoring,
       json_extract_path_text(json_args,'lob_app_json','have_commercial_auto_policy',true) as have_commercial_auto_policy,
      json_extract_path_text(json_args,'lob_app_json','driver_age_baseline',true) as driver_age_baseline,
       json_extract_path_text(json_args,'lob_app_json','drivers_personal_coverage_baseline',true) as drivers_personal_coverage_baseline
from dwh.quotes_policies_mlob qpm where lob_policy = 'GL' and cob = 'Restaurant' and creation_time>='2024-01-10'  and biz_id = '2ac4354a94ce7f707daaec668fe41534'

select * from dwh.quotes_policies_mlob qpm where lob_policy = 'GL'  and business_id = '2ac4354a94ce7f707daaec668fe41534'
select * from rating

select * from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
where business_id = '652c9c0200c1c29ab09c31a07454df12' and uw.lob = 'GL'

select cob, lob, execution_status,decline_reasons, count (distinct business_id) as declines
from dwh.underwriting_quotes_data uw
where cob in ('Restaurant')
  and offer_creation_time >= '2023-01-01'
  and uw.lob = 'GL'
  and execution_status in ('DECLINE')
  and offer_flow_type in ('APPLICATION')
--   and affiliate_id = 'N/A' and  agent_id = 'N/A'
 and agent_id != 'N/A'
group by cob, lob, execution_status,decline_reasons
order by cob, lob,execution_status,declines desc

select distinct business_id,business_name, policy_status_name, revenue_in_12_months,uw.state_code, cob,  uw.city, uw.zip_code, uw.street
                       from dwh.underwriting_quotes_data uw
    join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
    where cob in ('Grocery Store')
  and offer_creation_time >= '2023-01-01'
  and uw.lob = 'CP'
  and execution_status in ('DECLINE')
  and offer_flow_type in ('APPLICATION')
and decline_reasons = '["Your business is not accepted due to high fire hazard"]'

select distinct offer_creation_time::date,business_id, revenue_in_12_months, json_extract_path_text(answers,'started_operations',true) as started_operations, agent_id,decline_reasons
                  from dwh.underwriting_quotes_data uw
    join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
  where  uw.lob = 'GL'
    and offer_flow_type in ('APPLICATION')
    and offer_creation_time >= '2023-01-01'
    and cob in ('Restaurant')

select * from dwh.v_agents where agency_id = 28569

if ((agent_id != null || requested_limit(coverage_type="DAMAGE_TO_RENTED_PREMISES", limit_type="Per Premise Limit") > $100_000)
    && (sprinklers == true || central_fire_alarm == true) && number_of_deep_fryers <5 && extinguishing_system_maintenance_and_repair_contract == true
    && building_systems_updated_in_last_15_years == true && square_footage <= 7500 && is_undergoing_structural_renovation == false)

with hnoa_uw as (select offer_id, business_id as biz_id, json_args,
       json_extract_path_text(json_args,'lob_app_json','location.catering_delivery_by_employee',true) as employee_cater,
       json_extract_path_text(json_args,'lob_app_json','location.catering_delivery_by_owner',true) as owner_cater,
       json_extract_path_text(json_args,'lob_app_json','location.food_delivery_by_employee',true) as employee_deliver,
       json_extract_path_text(json_args,'lob_app_json','location.food_delivery_by_owner',true) as owner_deliver,
       json_extract_path_text(json_args,'lob_app_json','mvr_driver_violation_monitoring',true) as mvr_monitoring,
       json_extract_path_text(json_args,'lob_app_json','have_commercial_auto_policy',true) as have_commercial_auto_policy,
      json_extract_path_text(json_args,'lob_app_json','driver_age_baseline',true) as driver_age_baseline,
       json_extract_path_text(json_args,'lob_app_json','drivers_personal_coverage_baseline',true) as drivers_personal_coverage_baseline
from dwh.quotes_policies_mlob qpm where lob_policy = 'GL' and employee_cater is not null and highest_policy_status >=3 and creation_time >= '2024-01-10'
)



with hnoa_uw as (select offer_id, business_id as biz_id, purchased_quote_job_id, json_args,portfolio_version,
       json_extract_path_text(json_args,'lob_app_json','location.catering_delivery_by_employee',true) as employee_cater,
       json_extract_path_text(json_args,'lob_app_json','location.catering_delivery_by_owner',true) as owner_cater,
       json_extract_path_text(json_args,'lob_app_json','location.food_delivery_by_employee',true) as employee_deliver,
       json_extract_path_text(json_args,'lob_app_json','location.food_delivery_by_owner',true) as owner_deliver,
       json_extract_path_text(json_args,'lob_app_json','mvr_driver_violation_monitoring',true) as mvr_monitoring,
       json_extract_path_text(json_args,'lob_app_json','have_commercial_auto_policy',true) as have_commercial_auto_policy,
      json_extract_path_text(json_args,'lob_app_json','driver_age_baseline',true) as driver_age_baseline,
       json_extract_path_text(json_args,'lob_app_json','drivers_personal_coverage_baseline',true) as drivers_personal_coverage_baseline
from dwh.quotes_policies_mlob qpm
left join (select job_id, json_extract_path_text(data_points,'packageData','version',true) as portfolio_version from s3_operational.rating_svc_prod_calculations where lob = 'GL') svc
    on qpm.purchased_quote_job_id = svc.job_id
where lob_policy = 'GL' and employee_cater is not null and purchased_quote_job_id is not null and highest_policy_status >=3 and creation_time >= '2024-01-10'
)





select distinct offer_creation_time::date, business_id, employee_cater,owner_cater,employee_deliver,owner_deliver,mvr_monitoring,have_commercial_auto_policy,driver_age_baseline,drivers_personal_coverage_baseline,portfolio_version
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
left join dwh.v_agents a on uw.agent_id = a.agent_id
left join hnoa_uw on uw.business_id = hnoa_uw.biz_id
where uw.lob = 'GL'  and cob in ('Restaurant') and uw.state_code not in ('CA','MI')
  and policy_status >= 4 and offer_creation_time >= '2024-01-10'  and agent_name is not null
order by offer_creation_time desc

select distinct offer_creation_time::date, json_args from dwh.quotes_policies_mlob qpm
join dwh.underwriting_quotes_data uw on uw.business_id = qpm.business_id
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
left join dwh.v_agents a on uw.agent_id = a.agent_id
where uw.lob = 'GL'  and uw.cob in ('Grocery Store','Clothing Store','Retail Stores','E-Commerce','Electronics Store','Convenience Stores','Screen Printing and T Shirt Printing','Specialty Food','Furniture Store','Jewelry Store','Laundromat','Knife Sharpening','Bookstore','Signmaking','Arts and Crafts Store','Candle Store','Medical Supplies Store','Home and Garden Retailer','Hobby Shop','Sporting Goods Retailer','Nurseries and Gardening Shop','Vitamins and Supplements Store','Candy Stores','Lighting Store','Flea Markets','Department Stores','Popcorn Shops') and uw.state_code not in ('CA','MI')
  and policy_status >= 4 and offer_creation_time >= '2025-01-01'  and agent_name is not null
order by offer_creation_time desc

-- hnoa eligibility
select distinct business_id, json_args, distribution_channel from dwh.quotes_policies_mlob qpm where highest_policy_status >= 4 and lob_policy = 'GL'
and cob in ('Grocery Store','Clothing Store','Retail Stores','E-Commerce') and creation_time >= '2025-01-10' and offer_flow_type = 'APPLICATION'

with hnoa_uw as (select offer_id, business_id as biz_id, purchased_quote_job_id, json_args,portfolio_version,
       json_extract_path_text(json_args,'lob_app_json','personal_vehicle_for_business_use',true) as personal_vehicle_for_business_use,
       json_extract_path_text(json_args,'lob_app_json','personal_vehicle_for_deliveries',true) as personal_vehicle_for_deliveries,
       json_extract_path_text(json_args,'lob_app_json','personal_vehicle_for_errands',true) as personal_vehicle_for_errands,
       json_extract_path_text(json_args,'lob_app_json','personal_vehicle_for_driving_to_temp_retail_locations',true) as personal_vehicle_for_driving_to_temp_retail_locations,
       json_extract_path_text(json_args,'lob_app_json','mvr_driver_violation_monitoring',true) as mvr_monitoring,
       json_extract_path_text(json_args,'lob_app_json','have_commercial_auto_policy',true) as have_commercial_auto_policy,
      json_extract_path_text(json_args,'lob_app_json','driver_age_baseline',true) as driver_age_baseline,
       json_extract_path_text(json_args,'lob_app_json','drivers_personal_coverage_baseline',true) as drivers_personal_coverage_baseline
from dwh.quotes_policies_mlob qpm
left join (select job_id, json_extract_path_text(data_points,'packageData','version',true) as portfolio_version from s3_operational.rating_svc_prod_calculations where lob = 'GL') svc
    on qpm.purchased_quote_job_id = svc.job_id
where lob_policy = 'GL' and personal_vehicle_for_business_use is not null and purchased_quote_job_id is not null and highest_policy_status >=3 and creation_time >= '2024-10-23'
)

select distinct offer_creation_time::date, business_id,personal_vehicle_for_business_use,personal_vehicle_for_deliveries,personal_vehicle_for_errands,personal_vehicle_for_driving_to_temp_retail_locations,mvr_monitoring,have_commercial_auto_policy,driver_age_baseline,drivers_personal_coverage_baseline,portfolio_version
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
left join hnoa_uw on uw.business_id = hnoa_uw.biz_id
where uw.lob = 'GL'  and cob in ('Grocery Store','Clothing Store','Retail Stores','E-Commerce','Electronics Store','Convenience Stores','Screen Printing and T Shirt Printing','Specialty Food','Furniture Store','Jewelry Store','Laundromat','Knife Sharpening','Bookstore','Signmaking','Arts and Crafts Store','Candle Store','Medical Supplies Store','Home and Garden Retailer','Hobby Shop','Sporting Goods Retailer','Nurseries and Gardening Shop','Vitamins and Supplements Store','Candy Stores','Lighting Store','Flea Markets','Department Stores','Popcorn Shops')
  and uw.state_code not in ('CA','MI')
  and policy_status >= 4 and offer_creation_time >= '2024-10-23'  and agent_id is not null
order by offer_creation_time desc


select distinct offer_creation_time::date, business_id,revenue_in_12_months, yearly_premium
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
left join dwh.v_agents a on uw.agent_id = a.agent_id
where uw.lob = 'GL'  and cob in ('Restaurant') and uw.state_code not in ('CA','MI')
  and policy_status >= 4 and offer_creation_time >= '2024-01-10'  and agent_name is not null
order by offer_creation_time desc

select offer_id, business_id as biz_id, purchased_quote_job_id, json_args,portfolio_version,
       json_extract_path_text(json_args,'lob_app_json','location.catering_delivery_by_employee',true) as employee_cater,
       json_extract_path_text(json_args,'lob_app_json','location.catering_delivery_by_owner',true) as owner_cater,
       json_extract_path_text(json_args,'lob_app_json','location.food_delivery_by_employee',true) as employee_deliver,
       json_extract_path_text(json_args,'lob_app_json','location.food_delivery_by_owner',true) as owner_deliver,
       json_extract_path_text(json_args,'lob_app_json','mvr_driver_violation_monitoring',true) as mvr_monitoring,
       json_extract_path_text(json_args,'lob_app_json','have_commercial_auto_policy',true) as have_commercial_auto_policy,
      json_extract_path_text(json_args,'lob_app_json','driver_age_baseline',true) as driver_age_baseline,
       json_extract_path_text(json_args,'lob_app_json','drivers_personal_coverage_baseline',true) as drivers_personal_coverage_baseline
from dwh.quotes_policies_mlob qpm
left join (select job_id, json_extract_path_text(data_points,'packageData','version',true) as portfolio_version from s3_operational.rating_svc_prod_calculations where lob = 'GL') svc
    on qpm.purchased_quote_job_id = svc.job_id
where lob_policy = 'GL' and employee_cater is not null and purchased_quote_job_id is not null and highest_policy_status >=3 and creation_time >= '2024-01-10' and business_id = 'fc944a2550055364f1f2acc5e91e3a58'

select distinct offer_creation_time::date, business_id,business_name,cob,uw.lob, decline_reasons
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
where business_name ilike '%lounge%' and cob = 'Restaurant' and uw.lob in ('GL','BP')
order by offer_creation_time desc
limit 500

select business_id, agent_id,
                (CASE WHEN (affiliate_id = 'N/A' and  agent_id = 'N/A') then 'direct'
                    WHEN (affiliate_id <> 'N/A' and  agent_id = 'N/A') then 'affiliate'
                    else 'agent' end) as channel
from dwh.underwriting_quotes_data uw
where uw.lob = 'GL' and uw.cob = 'Restaurant' and offer_flow_type = 'RENEWAL'
order by offer_creation_time desc


select * from dwh.sources_test_cobs where cob_name ilike '%manufacturing%'

select distinct offer_creation_time::date, uw.business_id,business_name, policy_status_name,revenue_in_12_months,uw.lob,decline_reasons,uw.state_code, cob, uw.city, uw.zip_code, uw.street
from dwh.underwriting_quotes_data uw
left join riskmgmt_svc_prod.verisk_property_risk_request_response vr on vr.street = uw.street and vr.business_city = uw.city
left join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
where uw.lob = 'CP' and uw.cob = 'Restaurant' and offer_flow_type = 'APPLICATION'
  and decline_reasons ilike '%contract%'
order by offer_creation_time desc

-- south carolina ll closure
select *
from
(select distinct business_id,
                 distribution_channel,
                 lob_policy,
                 street,
                 qpm.city,
                 qpm.state,
                 highest_yearly_premium,
                 policy_end_date,
                 qpm.agent_id,
                 a.agent_name, a.current_agencytype, a.agency_name, a.agent_email_address, a.agency_aggregator_name,
                 json_extract_path_text(json_args, 'lob_app_json', 'liquor_sales_yes_no', true) as liquor_sales_yes_no,
                 rank() over(partition by business_id order by creation_time desc) rnk
 from dwh.quotes_policies_mlob qpm
 left join dwh.v_agents a on qpm.agent_id = a.agent_id
 where cob in ('Restaurant')
   and highest_policy_status =4
   and state = 'IA'
   and lob_policy = 'GL'
   and liquor_sales_yes_no = 'Yes'

 )
where rnk = 1

select * from dwh.quotes_policies_mlob limit 10






with base as (select business_id, business_name, cob, yearly_premium,quote_job_id,state_code,policy_status
from dwh.underwriting_quotes_data
where policy_reference is not null
and lob = 'GL' and cob = 'Restaurant'
and offer_creation_time>='2023-01-01'),

    rating_join as (select
    json_extract_path_text(rating_result,'ratingGroupedByCategories','STAND_ALONE_ENDORSEMENTS_GROUPED_BY_ENDORSEMENT_ID', 'LIQUOR_LIABILITY_COVERAGE', 'liquorLiabilityAggregateLimit', true) liquor_liability_agg_limit,
    json_extract_path_text(rating_result,'ratingGroupedByCategories','STAND_ALONE_ENDORSEMENTS_GROUPED_BY_ENDORSEMENT_ID', 'LIQUOR_LIABILITY_COVERAGE', 'liquorLiabilityTotalPremiumWithSurcharge', true) liquor_liability_premium,
        b.*
         from  base b
         join s3_operational.rating_svc_prod_calculations r
     on b.quote_job_id = r.job_id)

  select distinct business_id, business_name, cob, yearly_premium,state_code,policy_status,liquor_liability_agg_limit,liquor_liability_premium
from rating_join where liquor_liability_premium <> ''






select distinct qpm.business_id,
                 distribution_channel,
                 lob_policy,
                 street,
                 qpm.city,
                 qpm.state,
                 highest_yearly_premium,
                 policy_end_date,
                 qpm.agent_id,
                 json_extract_path_text(json_args, 'lob_app_json', 'liquor_sales_yes_no', true) as liquor_sales_yes_no
           from dwh.quotes_policies_mlob qpm
left join dwh.policy_transactions c
on  qpm.business_id = c.business_id
where cob in ('Restaurant')
   and highest_policy_status >3
   and lob_policy = 'GL'
   and liquor_sales_yes_no = 'Yes'
and transaction_type = 'CHANGE' and transaction_sub_type in ('COVERAGE_LIMIT_CHANGE','COVERAGE_LIMIT')

select * from dwh.policy_transactions where transaction_type = 'CHANGE' limit 10

select distinct transaction_sub_type from dwh.policy_transactions where transaction_type = 'CHANGE'
select * from dwh.policy_transactions where business_id = '75105cb2e789d6bd71901dbcd196bbce'



with t1 as (select distinct qpm.business_id,
                 distribution_channel,
                 cob,
                 lob_policy,
                 street,
                 qpm.city,
                 qpm.state,
                 highest_yearly_premium,
                 policy_end_date,
                 qpm.agent_id,
                 json_extract_path_text(json_args, 'lob_app_json', 'liquor_sales_yes_no', true) as liquor_sales_yes_no
           from dwh.quotes_policies_mlob qpm
left join dwh.policy_transactions c
on  qpm.business_id = c.business_id
where cob in ('Restaurant')
   and highest_policy_status >3
   and lob_policy = 'GL'
--    and liquor_sales_yes_no = 'Yes'
and transaction_type = 'CHANGE' and transaction_sub_type in ('COVERAGE_LIMIT_CHANGE','COVERAGE_LIMIT') and offer_flow_type in ('APPLICATION','RENEWAL','CANCEL_REWRITE')
)

select distinct pc.business_id,change_id,policy_id, yearly_amount_diff, attribution_diff, description,creation_time::date,cob,liquor_sales_yes_no,highest_yearly_premium,
       json_extract_path_text(json_extract_array_element_text
    (json_extract_path_text(attribution_diff,'policyItems'),0),'itemId') itemID1,
    json_extract_path_text(json_extract_array_element_text
    (json_extract_path_text(attribution_diff,'policyItems'),0),'amounts','premium') Premium1,
    json_extract_path_text(json_extract_array_element_text
    (json_extract_path_text(attribution_diff,'policyItems'),1),'itemId')itemID2,
    json_extract_path_text(json_extract_array_element_text
    (json_extract_path_text(attribution_diff,'policyItems'),1),'amounts','premium') Premium2,
    json_extract_path_text(json_extract_array_element_text
    (json_extract_path_text(attribution_diff,'policyItems'),2),'itemId')itemID3,
    json_extract_path_text(json_extract_array_element_text
    (json_extract_path_text(attribution_diff,'policyItems'),2),'amounts','premium') Premium3,
     json_extract_path_text(json_extract_array_element_text
    (json_extract_path_text(attribution_diff,'policyItems'),3),'itemId')itemID4,
    json_extract_path_text(json_extract_array_element_text
    (json_extract_path_text(attribution_diff,'policyItems'),3),'amounts','premium') Premium4,
     json_extract_path_text(json_extract_array_element_text
    (json_extract_path_text(attribution_diff,'policyItems'),4),'itemId')itemID5,
    json_extract_path_text(json_extract_array_element_text
    (json_extract_path_text(attribution_diff,'policyItems'),4),'amounts','premium') Premium5,
    json_extract_path_text(json_extract_array_element_text
    (json_extract_path_text(attribution_diff,'policyItems'),5),'itemId')itemID6,
    json_extract_path_text(json_extract_array_element_text
    (json_extract_path_text(attribution_diff,'policyItems'),5),'amounts','premium') Premium6
    from nimi_svc_prod.policy_changes pc
    left join t1 on  t1.business_id = pc.business_id
where pc.business_id in (select distinct business_id from t1) and description = '[Policy Change V2] coverage limit'




select * from dwh.quotes_policies_mlob
where offer_flow_type in ('RENEWAL') and
      (cob = 'Restaurant') limit 50

select * from dwh.quotes_policies_mlob where business_id = '2c63e37fe347cba8f461a01faecc5764' and highest_policy_status >3 and cob = 'Restaurant'

select * from dwh.quotes_policies_mlob where offer_flow_type in ('RENEWAL')
                                         and business_id in (select business_id from dwh.quotes_policies_mlob where affiliate_id != 'N/A' and cob = 'Restaurant') limit 50

select business_id,policy_start_date,lob_policy, distribution_channel, offer_flow_type, affiliate_id from dwh.quotes_policies_mlob where business_id = 'a20f4768251e2cb63d1a5c226049a7e9' and highest_policy_status >3 and offer_flow_type in ('APPLICATION','RENEWAL','CANCEL_REWRITE')

select
    nullif(json_extract_path_text(event_payload, 'data', 'riskScreeningId', true), '')   risk_screening_id,
    nullif(json_extract_path_text(event_payload, 'data', 'additionalData', 'businessId', true), '')   bid,
    nullif(json_extract_path_text(event_payload, 'data', 'additionalData', 'score', true), '')   wildfire_score,
    event_payload, creation_time

from prod.riskmgmt_svc_prod.business_events
-- WHERE 1=1
and SPLIT_PART(event_id, '.', 2) = 'RISK_SCREENING'
and bid = '1be02c134985506d1e6e306a39123ec2'
--   and risk_screening_id = 'qkIbmM2nc6G00VLj'
-- and nullif(json_extract_path_text(event_payload, 'data', 'riskScreeningStep', true), '') = 'WILDFIRE_RISK'
limit 100

with contacts as (select p.business_id,
json_extract_path_text(p.business_details, 'applicantfirstname') as first_name,
json_extract_path_text(p.business_details, 'applicantlastname') as last_name,
json_extract_path_text(p.business_details, 'telephonenumber') as phone_number,
json_extract_path_text(p.business_details, 'emailaddress') as business_email
from underwriting_svc_prod.prospects p)

select distinct offer_creation_time::date, uw.business_id,business_name, policy_status_name,revenue_in_12_months,uw.lob,decline_reasons,uw.state_code, cob, uw.city, uw.zip_code, uw.street,business_email,
         affiliate_id
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
left join contacts on contacts.business_id = uw.business_id
where affiliate_id in ('3110', '3120', '2','4600','4070') and offer_creation_time >= '2023-09-26'
 order by offer_creation_time desc

select *, nullif(json_extract_path_text(event_payload, 'data', 'additionalData', 'businessId', true), '') bid
    from prod.riskmgmt_svc_prod.business_events limit 10




with contacts as (select p.business_id,
json_extract_path_text(p.business_details, 'applicantfirstname') as first_name,
json_extract_path_text(p.business_details, 'applicantlastname') as last_name,
json_extract_path_text(p.business_details, 'telephonenumber') as phone_number,
json_extract_path_text(p.business_details, 'emailaddress') as business_email
from underwriting_svc_prod.prospects p)

select *
from
(select distinct creation_time::date,
                 business_id,
                 lob_policy,
                 street,
                 qpm.city,
                 qpm.state,
                 qpm.zip_code,
                  json_extract_path_text(json_args,'business_name',true) as business_name,
                json_extract_path_text(json_args, 'lob_app_json', 'liquor_sales_yes_no', true) as liquor_sales_yes_no,
                 rank() over(partition by business_id order by creation_time desc) rnk
 from dwh.quotes_policies_mlob qpm
 where cob in ('Restaurant')
   and highest_policy_status >=3
   and lob_policy = 'GL'
   and liquor_sales_yes_no = 'Yes'

 ) uw
left join contacts on contacts.business_id = uw.business_id
where rnk = 1

select distinct offer_creation_time::date,uw.business_id,business_name, policy_status_name,cob,uw.lob,uw.street,uw.city,uw.state_code,uw.zip_code,yearly_premium,agent_name,agency_aggregator_name,current_agencytype
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
left join dwh.v_agents a on uw.agent_id = a.agent_id
where uw.lob != 'CA'
  and cob in ('Restaurant') and offer_creation_time >= '2023-01-01' and offer_flow_type = 'APPLICATION'
and uw.state_code in ('AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'FL', 'GA', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MD', 'ME', 'MO', 'MS', 'MT', 'NE', 'NH', 'NM', 'NV', 'OK', 'OR', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VT', 'WV')
and current_agencytype not in ('Wholesaler') and agent_name is not null
order by offer_creation_time desc


with t1 as (select
    -- tpm 2.0 has 3 models trained for different risk/premium buckets
    -- so we would want to have all of those predictions in 1 bin for normalization purposes
    case when model_version like '%gl_v3%'
        then 'models/gl_v3/pipeline_model_2024_04_17' else model_version end,
    avg(predicted_loss_ratio) as avg_predicted_loss_ratio
from
    nidl_loss_ratio_model_prod.model_output_v2
where (silent_run = 'false' or silent_run is null)
group by 1),

t2 as (select distinct qpm.business_id,policy_start_date::date, predicted_loss_ratio,
                case when nidl_loss_ratio_model_prod.model_output_v2.model_version like '%gl_v3%'
        then 'models/gl_v3/pipeline_model_2024_04_17' else nidl_loss_ratio_model_prod.model_output_v2.model_version end
from dwh.quotes_policies_mlob qpm
left join nidl_loss_ratio_model_prod.model_output_v2 on nidl_loss_ratio_model_prod.model_output_v2.business_id = qpm.business_id
where cob in ('Restaurant','Bakery','Coffee Shop','Caterer','Grocery Store','Food Truck')
  and highest_policy_status >3 and offer_flow_type in ('APPLICATION') and lob_policy = 'GL'
 order by policy_start_date desc)

select * from t2 left join t1 on t1.model_version = t2.model_version


select distinct business_id,policy_start_date, policy_end_date,highest_status_name,json_extract_path_text(json_args,'lob_app_json','liquor_sales_yes_no',true) as liquor_sales_yes_no
       from dwh.quotes_policies_mlob where cob in ('Restaurant')  and highest_policy_status =4 and state = 'IA' and lob_policy = 'GL' and liquor_sales_yes_no = 'Yes'



select * from dwh.quotes_policies_mlob limit 10

select distinct business_id, json_extract_path_text(qpm.json_args, 'lob_app_json','liquor_sales_yes_no',true) as liquor_yes_no,
json_extract_path_text(qpm.json_args, 'lob_app_json','liquor_risk_byob_alcohol',true) as liquor_byob,
json_extract_path_text(qpm.json_args, 'lob_app_json','liquor_sales_exposure',true) as liquor_pct
from dwh.quotes_policies_mlob qpm
where liquor_yes_no = 'Yes' and
liquor_byob = 'Yes' and
highest_policy_status in (4,7) and
lob_policy = 'GL' and
offer_flow_type in ('APPLICATION','RENEWAL','CANCEL_REWRITE') and
creation_time >= '2023-01-01' and
cob = 'Restaurant'

select count (distinct business_id) from dwh.quotes_policies_mlob where highest_policy_status =4 and cob in ('Bakery', 'Caterer','Coffee Shop','Food Truck','Grocery Store','Restaurant')

select distinct business_id, business_name, business_name, uw.street,uw.city,uw.state_code,uw.zip_code
from dwh.underwriting_quotes_data uw
                            where uw.lob = 'GL' and policy_status >= 3 and cob in ('Restaurant')


with contacts as (select p.business_id,
json_extract_path_text(p.business_details, 'applicantfirstname') as first_name,
json_extract_path_text(p.business_details, 'applicantlastname') as last_name,
json_extract_path_text(p.business_details, 'telephonenumber') as phone_number,
json_extract_path_text(p.business_details, 'emailaddress') as business_email
from underwriting_svc_prod.prospects p)

select distinct uw.business_id,business_name, uw.street,uw.city,uw.state_code, uw.zip_code, first_name, last_name,business_email, phone_number
from dwh.underwriting_quotes_data uw
left join contacts on contacts.business_id = uw.business_id
where uw.lob = 'GL' and policy_status >= 4 and cob in ('Restaurant')
order by offer_creation_time desc

select distinct policy_status, policy_status_name from dwh.underwriting_quotes_data order by policy_status asc

select * from dwh.all_claims_financial_changes_ds limit 10

select * from db_data_science."F&BLossCause_taggedByClaimBolt_for_Tong" where response = 'UNKNOWN'

with loss as (
select * from (
select
    case when c.tpa = 2 then substring(c.claim_id, 1, 13) else c.claim_id end as Claim_ID
    ,c.claim_number
    , c.exposure_id
    , c.policy_id
    , c.policy_reference
    , c.lob
    , c.date_of_loss
    , c.date_submitted
    , c.cob_name
    , c.loss_cause_type_name
    , c.loss_type
    , c.date
    , p.start_date
    , row_number() over (partition by c.policy_reference, c.claim_id, c.exposure_id order by c.date desc) as rk
    , NVL(loss_reserve_total, 0) as total_reserve
    , NVL(loss_paid_total, 0) as total_paid
    , (total_reserve + total_paid)   as total_at_stake
    , nvl(loss_paid_total, 0) + nvl(loss_reserve_total, 0) +
      nvl(expense_ao_paid_total, 0) + nvl(expense_ao_reserve_total, 0) +
      nvl(expense_dcc_paid_total, 0) + nvl(expense_dcc_reserve_total, 0) +
      nvl(recovery_salvage_collected_total, 0) + nvl(recovery_subrogation_collected_total, 0) as LossALAE
from dwh.all_claims_financial_changes_ds c
left join nimi_svc_prod.policies p on p.policy_reference = c.policy_reference
where c.lob IN ('GL','CP','WC','IM','CA','PL')
and c.date is not null
  and claim_id = 'CX2MRImeRuDxJCjv'
) tt
where rk = 1
)



------ 2601 claims ----------
------- Zhe's model output don't have claims start with 009: Claim_ID like '009%' because API has a different ID--------------
--select
--    count(distinct Claim_ID) as claim_ct,
--    count(distinct case when claim_bot_claim_number is null then Claim_ID end) as claim_ct_not_in_claim_bot,
--    count(distinct case when claim_bot_claim_number is null and Claim_ID not like '009%' then Claim_ID end) as claim_ct_not_in_claim_bot_excl_009,
--    count(distinct case when response = 'UNKNOWN' then Claim_ID end) as claim_ct_UNKNOWN_label
--from (
select distinct clm.business_id as biz_id, clm.lob, clm.revenue_next_12_months,
    Claim_ID,l.claim_number,date_submitted
    --Claim_ID, l.claim_number,exposure_id, date_of_loss, date_submitted, loss_cause_type_name, loss_type
    ,total_reserve, total_paid, total_at_stake, LossALAE
    ,cb.claim_number as claim_bot_claim_number
    ,cb.response
    --,clm.*
from dwh.company_level_metrics_ds clm
join loss l on l.policy_reference = clm.policy_reference
left join db_data_science.fblosscause_taggedbyclaimbolt_tz cb on cb.claim_number = l.claim_number
where clm.lob IN ('GL','CP','WC','IM','CA','PL')
    and clm.marketing_cob_group like '%Food & beverage%'
    and date_submitted::date <= '2024-06-30' and l.claim_id = 'CX2MRImeRuDxJCjv'
 --and l.claim_number = 'NXTC-NYCP-C3VHK9'
    --and Claim_ID not like '009%'
    --and cb.claim_number is null
--) as tmp

 select * from dwh.company_level_metrics_ds limit 10

 with loss as (
select * from (
select
    case when c.tpa = 2 then substring(c.claim_id, 1, 13) else c.claim_id end as Claim_ID
    ,c.claim_number
    , c.exposure_id
    , c.policy_id
    , c.policy_reference
    , c.lob
    , c.date_of_loss
    , c.date_submitted
    , c.cob_name
    , c.loss_cause_type_name
    , c.loss_type
    , c.date
    , p.start_date
    , row_number() over (partition by c.policy_reference, c.claim_id, c.exposure_id order by c.date desc) as rk
    , NVL(loss_reserve_total, 0) as total_reserve
    , NVL(loss_paid_total, 0) as total_paid
    , (total_reserve + total_paid)   as total_at_stake
    , nvl(loss_paid_total, 0) + nvl(loss_reserve_total, 0) +
      nvl(expense_ao_paid_total, 0) + nvl(expense_ao_reserve_total, 0) +
      nvl(expense_dcc_paid_total, 0) + nvl(expense_dcc_reserve_total, 0) +
      nvl(recovery_salvage_collected_total, 0) + nvl(recovery_subrogation_collected_total, 0) as LossALAE
from dwh.all_claims_financial_changes_ds c
left join nimi_svc_prod.policies p on p.policy_reference = c.policy_reference
where c.lob IN ('GL','CP','WC','IM','CA','PL')
and c.date is not null
) tt
where rk = 1
)


------ 2601 claims ----------
------- Zhe's model output don't have claims start with 009: Claim_ID like '009%' because API has a different ID--------------
--select
--    count(distinct Claim_ID) as claim_ct,
--    count(distinct case when claim_bot_claim_number is null then Claim_ID end) as claim_ct_not_in_claim_bot,
--    count(distinct case when claim_bot_claim_number is null and Claim_ID not like '009%' then Claim_ID end) as claim_ct_not_in_claim_bot_excl_009,
--    count(distinct case when response = 'UNKNOWN' then Claim_ID end) as claim_ct_UNKNOWN_label
--from (
select distinct clm.business_id as biz_id, clm.lob,
    Claim_ID,l.claim_number,date_submitted
    --Claim_ID, l.claim_number,exposure_id, date_of_loss, date_submitted, loss_cause_type_name, loss_type
    ,total_reserve, total_paid, total_at_stake, LossALAE
    ,cb.claim_number as claim_bot_claim_number
    ,cb.response
    --,clm.*
from dwh.company_level_metrics_ds clm
left join loss l on l.policy_reference = clm.policy_reference
left join db_data_science.fblosscause_taggedbyclaimbolt_tz cb on cb.claim_number = l.claim_number
where clm.lob IN ('GL','CP','WC','IM','CA','PL')
    and clm.marketing_cob_group like '%Food & beverage%'
    and date_submitted::date <= '2024-06-30'  and claim_id = 'CX2MRImeRuDxJCjv'
 --and l.claim_number = 'NXTC-NYCP-C3VHK9'
    --and Claim_ID not like '009%'
    --and cb.claim_number is null
--) as tmp

 /*
 Creator: Tong Zhang
 Subject: F&B Claims for According to Next Blog post data
 Date: 06/28/2024
 */


with loss as (
select * from (
select
    case when c.tpa = 2 then substring(c.claim_id, 1, 13) else c.claim_id end as Claim_ID
    , c.claim_number
    , c.exposure_id
    , c.policy_id
    , c.policy_reference
    , c.lob
    , c.date_of_loss
    , c.date_submitted
    , c.cob_name
    , c.loss_cause_type_name
    , c.loss_type
    , c.date
    , p.start_date
    , c.coverage
    , row_number() over (partition by c.policy_reference, c.claim_id, c.exposure_id order by c.date desc) as rk
    , NVL(loss_reserve_total, 0) as total_reserve
    , NVL(loss_paid_total, 0) as total_paid
    , (total_reserve + total_paid)   as total_at_stake
    , nvl(loss_paid_total, 0) + nvl(loss_reserve_total, 0) +
      nvl(expense_ao_paid_total, 0) + nvl(expense_ao_reserve_total, 0) +
      nvl(expense_dcc_paid_total, 0) + nvl(expense_dcc_reserve_total, 0) +
      nvl(recovery_salvage_collected_total, 0) + nvl(recovery_subrogation_collected_total, 0) as LossALAE
from dwh.all_claims_financial_changes_ds c
left join nimi_svc_prod.policies p on p.policy_reference = c.policy_reference
where c.lob IN ('GL','CP','WC','IM','CA','PL')
and c.date is not null
) tt
where rk = 1
)

,biz_info as (
select business_id, MAX(nullif(json_extract_path_text(json_args, 'lob_app_json', 'restaurant_type',true),'')) as restaurant_type
from dwh.quotes_policies_mlob qpm
group by 1
)

select
    Claim_ID, l.claim_number,exposure_id, date_of_loss, date_submitted, loss_cause_type_name, loss_type,coverage
    ,total_reserve, total_paid, total_at_stake, LossALAE
    ,cb.claim_number as claim_bolt_claim_number
    ,cb.response
    ,clm.*
    ,bi.restaurant_type
from dwh.company_level_metrics_ds clm
left join loss l on l.policy_reference = clm.policy_reference
left join db_data_science.fblosscause_taggedbyclaimbolt_tz cb on cb.claim_number = l.claim_number
left join biz_info bi on bi.business_id = clm.business_id
where clm.lob = 'CP' and coverage in ('SPOILAGE','RESTAURANTS')

select * from dwh.quotes_policies_mlob where business_id = 'fe64b75ce1d32c23cd7165bd3bfbfcbb'



select distinct offer_creation_time, business_id,business_name, policy_status_name,revenue_in_12_months,uw.lob,uw.state_code, cob, decline_reasons,uw.city, uw.zip_code, uw.street
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
where  uw.lob = 'GL'
   and policy_status >3 and  cob = 'Grocery Store' and business_id = '82a4be8b79b8fc8980ccdcc4f1b6f8b4'
--and decline_reasons = '["Your annual revenue is too large for this insurance policy to cover."]'
order by offer_creation_time desc

select count (distinct business_id) from dwh.underwriting_quotes_data  where lob in ('BP','CP') and policy_status = 4

select *
from
(select distinct business_id,
                 distribution_channel,
                 lob_policy,
                 street,
                 qpm.city,
                 qpm.state,
                 highest_yearly_premium,
                 policy_end_date,
                 qpm.agent_id,
                 a.agent_name, a.current_agencytype, a.agency_name, a.agent_email_address, a.agency_aggregator_name,
                 json_extract_path_text(json_args, 'lob_app_json', 'liquor_sales_yes_no', true) as liquor_sales_yes_no,
                 rank() over(partition by business_id order by creation_time desc) rnk
 from dwh.quotes_policies_mlob qpm
 left join dwh.v_agents a on qpm.agent_id = a.agent_id
 where cob in ('Restaurant')
   and highest_policy_status =4
   and state = 'NY'
   and lob_policy = 'GL'
   and liquor_sales_yes_no = 'Yes'

 )
where rnk = 1
limit 10

select *
from
(select distinct business_id,
                 distribution_channel,
                 lob_policy,
                 street,
                 qpm.city,
                 qpm.state,
                 highest_yearly_premium,
                 policy_end_date,
                 qpm.agent_id, highest_policy_status, highest_policy_status_name,
                 a.agent_name, a.current_agencytype, a.agency_name, a.agent_email_address, a.agency_aggregator_name,
                 rank() over(partition by business_id order by creation_time desc) rnk
 from dwh.quotes_policies_mlob qpm
 left join dwh.v_agents a on qpm.agent_id = a.agent_id
 where cob in ('Grocery Store')
   and highest_policy_status >3
 --  and state = 'MA'
   and lob_policy = 'GL' and business_id = '82a4be8b79b8fc8980ccdcc4f1b6f8b4'
 --  and liquor_sales_yes_no = 'Yes'

 )
where rnk = 1

select distinct offer_creation_time::date,business_id,business_name,  cob, execution_status, policy_status_name, decline_reasons,
                street,city, state_code, zip_code,affiliate_id,agent_id
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
where uw.lob = 'GL' and execution_status = 'DECLINE' and cob = 'Restaurant' and offer_creation_time >= '2024-01-01'
order by offer_creation_time desc



select distinct offer_creation_time, business_id,revenue_in_12_months,uw.agent_id,a.current_agencytype
from dwh.underwriting_quotes_data uw
left join dwh.v_agents a on uw.agent_id = a.agent_id
where uw.lob = 'GL' and cob in ('Restaurant') and policy_status >3 and offer_flow_type in ('APPLICATION')
order by offer_creation_time desc

select distinct offer_creation_time, business_id,revenue_in_12_months,uw.agent_id,a.current_agencytype,lob,cob,policy_status, offer_flow_type
from dwh.underwriting_quotes_data uw
left join dwh.v_agents a on uw.agent_id = a.agent_id
where policy_status >3 and lob = 'GL' and offer_flow_type in ('APPLICATION','RENEWAL','CANCEL_REWRITE') and cob = 'Restaurant'


select distinct creation_time, business_id,revenue_in_12_months,qpm.agent_id,a.current_agencytype,lob_policy,cob,highest_policy_status, offer_flow_type
from  dwh.quotes_policies_mlob qpm
left join dwh.v_agents a on qpm.agent_id = a.agent_id
where qpm.business_id = '266730752f57f04c64143eeb011ecf2c' and offer_flow_type in ('APPLICATION')


select * from db_data_science.ds_decline_monitoring
         where lob = 'WC' and stepstatus = 'DECLINE' and marketing_cob_group is null
         order by offer_creation_time desc


with claims_lit as (select claim_number
                         , case
                               when in_suit_flag = 1 then 1
                               when attorney_authorized_representative_flag = 1 then 1
                               else 0 end as attorney_rep
                          , in_suit_flag


                    from dwh.all_claims_financial_changes_ds
                    where 1
                      and date = (select max(date) from dwh.all_claims_financial_changes_ds)
                    group by 1, 2, 3)

select claim_number
                            , max(attorney_rep) as attorney_rep
                            , max(in_suit_flag) as in_suit_flag

                       from claims_lit group by 1


  loss as (
        select Claim_ID,p.business_id,cob_name,marketing_cob_group,attorney_rep
--     , exposure.policy_reference
--     , claim_id
--     , loss_cause_type_name
--     , p.business_id
--     , lob
             , min(date)           claim_date_min
             , min(date_submitted) claim_submit_date
             , sum(lossalae)   as  lossalae_uncapped
             , sum(TotReserve) as  TotReserve
        from (select case when tpa = 2 then substring(a.claim_id, 1, 13) else a.claim_id end as Claim_ID,
                     date,
                     date_submitted,
                     a.exposure_id,
                     a.policy_reference,
                     a.business_id,
                     lob,
                     cob_name,
                     marketing_cob_group,
                     case
                               when in_suit_flag = 1 then 1
                               when attorney_authorized_representative_flag = 1 then 1
                               else 0 end as attorney_rep,
                     nvl(a.loss_paid_total, 0) +
                     nvl(a.loss_reserve_total, 0) +
                     nvl(a.expense_ao_paid_total, 0) +
                     nvl(a.expense_ao_reserve_total, 0) +
                     nvl(a.expense_dcc_paid_total, 0) +
                     nvl(a.expense_dcc_reserve_total, 0) +
                     nvl(a.recovery_salvage_collected_total, 0) +
                     nvl(a.recovery_subrogation_collected_total, 0)                          as LossALAE,
                     nvl(a.loss_reserve_total, 0) + nvl(a.expense_ao_reserve_total, 0) +
                     nvl(a.expense_dcc_reserve_total, 0)                                     as TotReserve,
                     coalesce(a.loss_cause_type_name, 'UNKNOWN') as loss_cause_type_name
              from dwh.all_claims_financial_changes_ds a
                       join (select policy_reference, claim_id, exposure_id, max(date) as maxdate
                             from dwh.all_claims_financial_changes_ds
                             where lob = 'GL'
                             group by 1, 2, 3) b
                            on a.policy_reference = b.policy_reference and a.claim_id = b.claim_id and
                               a.exposure_id = b.exposure_id and a.date = b.maxdate) exposure
                 left join nimi_svc_prod.policies p on p.policy_reference = exposure.policy_reference
        where lob = 'GL' and marketing_cob_group = 'Food & beverage'
        group by 1,2,3,4,5
    )

select * from dwh.all_claims_financial_changes_ds limit 10

select distinct policy_start_date::date,business_id, cob, highest_policy_status
from dwh.quotes_policies_mlob qpm
where lob_policy = 'GL' and cob = 'Restaurant' and highest_policy_status = 4
order by policy_start_date desc

select distinct policy_start_date::date,business_id, cob, highest_policy_status, distribution_channel, highest_yearly_premium from dwh.quotes_policies_mlob qpm
where lob_policy = 'GL' and cob = 'Restaurant' and highest_policy_status = 4 and offer_flow_type in ('APPLICATION','RENEWAL','CANCEL_REWRITE')
order by policy_start_date desc

select *  from dwh.quotes_policies_mlob qpm
             where business_id = '683df78b2bc783f8a355d4c3409be62d' and lob_policy = 'GL' and cob = 'Restaurant' and offer_flow_type in ('APPLICATION','RENEWAL','CANCEL_REWRITE')
and highest_policy_status >3

select * from dwh.company_level_metrics_ds where business_id = 'fd1483d34d51e063a31341bbb8a4b67d' and lob = 'GL'

select * from external_dwh.cp_quotes where location_type = 'SPACE_OWNED' limit 10



With base as (
    SELECT distinct qpm.creation_time::date as quote_day,
                    qpm.business_id,
                    qpm.highest_policy_id,
                    qpm.highest_policy_status,
                    qpm.highest_yearly_premium,
                    CASE WHEN creation_time < '2023-09-26' THEN 'pre' ELSE 'post' END as pre_post,
                    case when qpm.agent_id <> 'N/A' then 'Agent' when qpm.affiliate_id <> 'N/A' then 'Partnership' else 'Direct' end channel
    FROM dwh.quotes_policies_mlob qpm
    WHERE qpm.lob_policy IN ('GL')
      and qpm.creation_time >= '2023-07-01'
      and qpm.offer_flow_type = 'APPLICATION'
    and qpm.cob in ('Day Care')
)
SELECT
       pre_post,
       --channel,
       average_purchased_premium,
       purchases/quotes::decimal(10,2) as qtp
FROM (
                SELECT pre_post,
                       --channel,
                       AVG(CASE WHEN highest_policy_status >= 3 then highest_yearly_premium END) as average_purchased_premium,
                       count(distinct business_id)                                 as quotes,
                       SUM(CASE WHEN highest_policy_status >= 3 then 1 ELSE 0 END) as purchases
                from base
                group by 1
            )

select * FROM dwh.quotes_policies_mlob qpm WHERE qpm.lob_policy IN ('GL')
      and qpm.creation_time >= '2023-07-01'
      and qpm.offer_flow_type = 'APPLICATION'
    and qpm.cob in ('Day Care')
and business_id = '85580e7656df6458e9d3806e6191359c'

with tmp as
         (select qpm.business_id,
                 json_extract_path_text(qpm.json_args, 'business_name', True) AS business_name,
                 qpm.street,
                 qpm.city,
                 qpm."state",
                 qpm.zip_code,
                 qpm.offer_flow_type,
                 qpm.distribution_channel,
                 qpm.agent_id
          from dwh.quotes_policies_mlob qpm
          where qpm.highest_policy_status = 4
            AND qpm.distribution_channel = 'agents'
            AND qpm.cob = 'Restaurant'
            AND qpm.lob_policy IN ('BP', 'GL', 'CP')
            and qpm.agent_id = 'N/A')
select offer_flow_type
      ,count(1)
from tmp
group by 1

select business_id, cob,lob_policy,highest_policy_status from dwh.quotes_policies_mlob qpm where business_id = '0250f19c4b152a826f1bf6fa4b96c26d'



select * from dwh.quotes_policies_mlob qpm where business_id = '9b6627c936f5b86e1bc16296ceb085ac' and highest_policy_status = 4 and lob_policy = 'GL'

select business_id,
       json_args

from dwh.quotes_policies_mlob qpm where lob_policy = 'CP' and cob = 'Restaurant' and highest_policy_status >=3  limit 100


--get datapoints PDP
select *
from underwriting_svc_prod.bi_applications_data
where data_point_id = 'location.restaurant_operations' limit 100

select business_id, count (distinct cob) as cob_count
from dwh.quotes_policies_mlob qpm
group by business_id
order by cob_count desc

select * from dwh.quotes_policies_mlob qpm
    where business_id = '6719df4283fdbaa0aae0ddc00649d0da'

select * from riskmgmt_svc_prod.exposure_base_revenue_results where business_id = '06980d083cd8b51961d77022295ef831'

select distinct offer_creation_time::date, business_id,business_name, policy_status_name,revenue_in_12_months,uw.lob,decline_reasons,uw.state_code, cob, uw.city, uw.zip_code, uw.street,yearly_premium, affiliate_id
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
where cob = 'Restaurant' and uw.lob ='GL' and offer_creation_time >= '2023-01-01' and offer_creation_time < '2023-06-01' and policy_status =6
 order by offer_creation_time limit 200




with verisk as (select lower(street) street,
       zip_code,
       lower(business_city) business_city ,
       upper(state) state,
       original_iso_construction_type,
       original_sqft_numeric,
       original_stories,
       original_sprinklered,
       original_year_built,
       creation_time,
       row_number() over (partition by lower(street), zip_code order by creation_time desc) rnk

from(select *,
                   json_extract_path_text(verisk_json_response, 'Ms1', 'GpsCoordinates', 'Latitude', true) original_Latitude,
       nullif(json_extract_path_text(verisk_json_response,'Ms4','FloorData','SquareFootage',true), '') original_sqft,
    nullif(json_extract_path_text(verisk_json_response,'Ms1','SquareFootage',true), '') original_sqft_str,
    CASE
        WHEN original_sqft_str IS NOT NULL THEN
            cast(REGEXP_SUBSTR(replace(original_sqft_str,',',''),'[0-9]+') as numeric)
        ELSE
            NULL
    END AS original_sqft_numeric,
                cast(nullif(json_extract_path_text(verisk_json_response, 'Ms1','Stories', 'Stories', true),'') as int) original_stories,
                                cast(nullif(json_extract_path_text(verisk_json_response, 'Ms1','Occupancy','Code', true),'') as int) original_occupancy_code,
                nullif(json_extract_path_text(verisk_json_response, 'Ms1','Occupancy','Description', true), '') original_occupancy_desc,
    coalesce(nullif(json_extract_path_text(verisk_json_response,'Ms1','ConstructionType','ReportedConstructionType',true), ''), nullif(json_extract_path_text(verisk_json_response,'Ms1','BuildingFireConstructionCode', 'Description', true), '')) original_construction_type,
    json_extract_path_text(json_extract_array_element_text(json_extract_path_text(verisk_json_response,'Ms1','ConstructionType','ISOConstructionTypes',true),0), 'ConstructionTypeCodeDescription', true) original_iso_construction_type,
    --cast(json_extract_path_text(json_extract_array_element_text(json_extract_path_text(verisk_json_response,'Ms1','ConstructionType','ISOConstructionTypes',true),0), 'ConfidenceScore', true) as numeric) original_iso_cctype_confidence,
    nullif(json_extract_path_text(verisk_json_response,'Ms3','Sprinklered',true), '') original_sprinklered,
    cast(nullif(json_extract_path_text(verisk_json_response,'Ms1','YearBuilt',true), '') as numeric) original_year_built,
    cast(nullif(json_extract_path_text(verisk_json_response,'Ms4','EffectiveYearBuilt',true), '') as numeric) original_eff_year_built,
    nullif(json_extract_path_text(verisk_json_response,'Ms1','Ppc',true), '') original_ms1_ppc,
    --cast(nullif(json_extract_path_text(verisk_json_response,'Ms4','Ppc','Ppc',true), '') as int) ms4_ppc,
    --cast(nullif(json_extract_path_text(verisk_json_response,'Ms3','Bcegs',true), '') as int) bcegs,
    cast(nullif(json_extract_path_text(verisk_json_response,'Ms3','Crime','AggregateCrimesAgainstProperty','IndexValuesUpto10','Current',true), '') as int) original_current_crime_score,
    cast(nullif(json_extract_path_text(verisk_json_response,'Ms3','Crime','AggregateCrimesAgainstProperty','IndexValuesUpto10','Past',true), '') as int)  original_past_crime_score,
    cast(nullif(json_extract_path_text(verisk_json_response,'Ms3','Crime','AggregateCrimesAgainstProperty','IndexValuesUpto10','Forecasted',true), '') as int)  original_forecast_crime_score,
    json_extract_path_text(verisk_json_response,'Address','StreetAddress1',true) original_StreetAddress1,
    json_extract_path_text(verisk_json_response,'Address','City',true) original_City,
    json_extract_path_text(verisk_json_response,'Address','Zip',true) original_Zip
    from riskmgmt_svc_prod.verisk_property_risk_request_response
    where creation_time >= '2023-06-01'
    ))

select a.*,
       b.*
from dwh.quotes_policies_mlob a
    left join verisk b
on lower(a.street) = b.street
and b.zip_code = a.zip_code
and b.rnk = 1

where lob_policy = 'CP'
and a.creation_time >= '2024-06-01'
and offer_flow_type = 'APPLICATION'
and cob in ('Restaurant', 'Coffee Shop', 'Bakery')

limit 2000             )

-- non-f&b hnoa uw

WITH package_data AS (
    SELECT quote_id,
           json_extract_path_text(quote_package_data, 'version', true) AS packageDataVersion,
           json_extract_path_text(quote_package_data, 'coverages', 'GL_HIRED_NON_OWNED_AUTO_COVERAGE', true) AS gl_hnoa_coverage
    FROM external_dwh.gl_quotes
    WHERE lob = 'GL'
      AND start_date >= '2024-09-15'
      AND gl_hnoa_coverage <> ''
)
SELECT DISTINCT creation_time,
    qpm.business_id,
    cob,
    highest_status_name,
    highest_yearly_premium,
    policy_start_date, json_extract_path_text(json_args,'lob_app_json','personal_vehicle_for_business_use',true) as personal_vehicle_for_business_use
FROM package_data pd
JOIN dwh.quotes_policies_mlob qpm
ON pd.quote_id = qpm.purchased_quote_job_id
--ON pd.quote_id = qpm.pro_plus_quote_job_id
WHERE qpm.highest_policy_status >= 3 and
      cob not in ('Restaurant','Bakery','Coffee Shop')


select * from external_dwh.cp_quotes where cob_name = 'restaurant' limit 10



WITH package_data AS (
    SELECT quote_id,
           json_extract_path_text(quote_package_data, 'version', true) AS packageDataVersion,
           json_extract_path_text(quote_package_data, 'coverages', 'GL_HIRED_NON_OWNED_AUTO_COVERAGE', true) AS gl_hnoa_coverage
    FROM external_dwh.gl_quotes
    WHERE lob = 'GL'
      AND start_date >= '2024-09-15'
      AND gl_hnoa_coverage <> ''
)
SELECT DISTINCT creation_time,
    qpm.business_id,
    cob,
    CASE
        WHEN agent_id <> 'N/A' THEN 'agent'
        WHEN affiliate_id <> 'N/A' THEN 'partnership'
        ELSE 'direct'
    END AS channel,
    highest_status_name, highest_yearly_premium,
    policy_start_date
FROM package_data pd
JOIN dwh.quotes_policies_mlob qpm
ON pd.quote_id = qpm.purchased_quote_job_id
--ON pd.quote_id = qpm.pro_plus_quote_job_id
WHERE qpm.highest_policy_status >= 3 and
      cob <> 'Restaurant'
LIMIT 1000

select
a.offer_id,
a.quote_id,
a.business_id,
a.schedule_rating_factor sr_factor,
a.state_code,
qpm.cob, qpm.cob_group, qpm.highest_yearly_premium
from external_dwh.gl_quotes a
join dwh.quotes_policies_mlob qpm on a.offer_id = qpm.offer_id
where a.dateid >= 20241001
and a.quote_type = 'APPLICATION'
and a.quote_status = 'SUCCESS'
and a.state_code = 'NY'
and sr_factor >0
-- AND qpm.cob = 'Restaurant'

select distinct offer_creation_time::date,business_id,offer_id,business_name, policy_status_name, uw.lob,yearly_premium,execution_status,decline_reasons,uw.state_code, cob, uw.city, uw.zip_code, uw.street,agent_name,agency_name,agency_aggregator_name,current_agencytype
from dwh.underwriting_quotes_data uw
join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
left join dwh.v_agents a on uw.agent_id = a.agent_id
where uw.lob = 'GL'  and policy_status >= 3 and uw.state_code = 'NY' and yearly_premium < 2500 and offer_creation_time > '2024-10-01'
order by offer_creation_time desc




select count (distinct business_id),
       json_extract_path_text(json_args,'lob_app_json','open_broiling_solid_fuel_cooking',true) as high_intensity_cooking
from dwh.quotes_policies_mlob qpm where lob_policy in ('CP','BP') and cob = 'Restaurant' and offer_flow_type = 'APPLICATION' and highest_policy_status >=3 and distribution_channel = 'agents'
group by high_intensity_cooking

select distinct business_id,highest_yearly_premium,
       json_extract_path_text(json_args,'lob_app_json','open_broiling_solid_fuel_cooking',true) as high_intensity_cooking
from dwh.quotes_policies_mlob qpm where lob_policy in ('CP') and cob = 'Restaurant' and offer_flow_type = 'APPLICATION' and highest_policy_status >=3 and distribution_channel = 'agents' and highest_yearly_premium > 2500
               and creation_time >= '2024-01-10' and high_intensity_cooking = 'No'



WITH package_data AS (
    SELECT quote_id,
           json_extract_path_text(quote_package_data, 'version', true) AS packageDataVersion,
--            json_extract_path_text(quote_package_data, 'coverages','RESTAURANTS','subCoverages','SPOILAGE','limits','OCCURRENCE',true) AS restaurant_spoilage
           json_extract_path_text(
           json_extract_path_text(quote_package_data, 'coverages','RESTAURANTS','subCoverages','SPOILAGE','limits', true),'OCCURRENCE',true)  AS restaurant_spoilage
    FROM external_dwh.cp_quotes
    WHERE lob = 'CP'
      AND start_date >= '2024-12-20'
)
SELECT DISTINCT creation_time,
    qpm.business_id,
    cob,
    CASE
        WHEN agent_id <> 'N/A' THEN 'agent'
        WHEN affiliate_id <> 'N/A' THEN 'partnership'
        ELSE 'direct'
    END AS channel,
    highest_status_name, highest_yearly_premium,
    policy_start_date, restaurant_spoilage
FROM package_data pd
JOIN dwh.quotes_policies_mlob qpm
ON pd.quote_id = qpm.purchased_quote_job_id
--ON pd.quote_id = qpm.pro_plus_quote_job_id
WHERE
--     qpm.highest_policy_status >= 3 and
      cob = 'Restaurant'
LIMIT 1000

SELECT
	rq.event_id,
	rq.id,
	rq.operation,
	rq.provider,
	rq.additional_data,
	rp.body,
	json_extract_path_text(rp.body, 'result','types', true) AS types
FROM silver_insurance_data_gateway.third_party_request_v1 AS rq
JOIN silver_insurance_data_gateway.third_party_response_v1 AS rp ON rp.id = rq.id
WHERE rq.provider = 'Google'
	AND rq.operation = 'PlaceDetails'
LIMIT 50


SELECT *,
	json_extract_path_text(rp.body, 'result','types', true) AS types
FROM silver_insurance_data_gateway.third_party_request_v1 AS rq
JOIN silver_insurance_data_gateway.third_party_response_v1 AS rp ON rp.id = rq.id
WHERE rq.provider = 'Google'
	AND rq.operation = 'PlaceDetails'
LIMIT 100


select *,json_extract_path_text(api_response, 'inspections','hazel_score', true) from db_data_science.wt_ecolab limit 10

 select distinct business_id, lob from dwh.underwriting_quotes_data uw
 where offer_creation_time >= '2024-12-01' and
 offer_creation_time < '2025-01-01' and cob in ('Restaurant') and lob in ('GL','BP')

select * from dwh.underwriting_quotes_data uw where business_id = '0c2fe9a3b1987ffd4b44bf8458c10088'

-- manual review
WITH event_data AS (
    SELECT
        dm.business_id,
        dm.screening_id,
        ed.activities,
        ed.creation_time,
        ROW_NUMBER() OVER (PARTITION BY dm.business_id ORDER BY ed.creation_time DESC) AS row_num
    FROM db_data_science.ds_decline_monitoring dm
    LEFT JOIN (
        SELECT
            creation_time,
            JSON_EXTRACT_PATH_TEXT(event_payload, 'data', 'riskScreeningId') AS riskScreeningId,
            JSON_EXTRACT_PATH_TEXT(event_payload, 'data', 'additionalData', 'activities') AS activities
        FROM riskmgmt_svc_prod.business_events
        WHERE JSON_EXTRACT_PATH_TEXT(event_payload, 'data', 'riskScreeningStep') = 'ALBUS_KEYWORDS'
    ) ed ON dm.screening_id = ed.riskScreeningId
    WHERE dm.stepname = 'ALBUS_KEYWORDS'
      AND dm.offer_flow_type = 'APPLICATION'
      AND ed.creation_time > '2025-01-01'
      AND ed.activities IS NOT NULL  -- Exclude NULL activities
)

SELECT
    clm.business_id,
    b.business_name,
    a.street_address AS street,
    a.city,
    a.state_code AS state,
    a.zip_code,
    clm.lob,
    dm_events.screening_id,
    dm_events.activities
FROM dwh.company_level_metrics_ds clm
JOIN nimi_svc_prod.businesses b ON b.business_id = clm.business_id
JOIN nimi_svc_prod.addresses a ON a.business_id = clm.business_id
JOIN event_data dm_events ON clm.business_id = dm_events.business_id
WHERE
    clm.policy_status_name = 'Active'
    AND clm.cob_name = 'Restaurant'
    AND a.is_primary = 1
    AND clm.lob IN ('GL', 'BP')
    AND clm.business_lob = 'GL'
    AND clm.policy_category = 'new'
    AND clm.start_date > '2025-01-01'
    AND dm_events.row_num = 1
    AND dm_events.activities ILIKE '%sports%';

select distinct business_id, uw.lob, policy_id,business_name, yearly_premium,policy_status, agent_name,agency_aggregator_name,current_agencytype,agent_email_address
from dwh.underwriting_quotes_data uw
left join dwh.v_agents a on uw.agent_id = a.agent_id
where cob in ('Restaurant') and uw.lob in ('GL', 'BP','CP') and policy_status in (4,7) and business_id = '8ea4294dcba2e03aa8b415b4b36cc3b3'
 order by offer_creation_time

select distinct business_id, uw.lob, policy_id,business_name, yearly_premium,policy_status, offer_flow_type, agent_name,agency_aggregator_name,current_agencytype,agent_email_address
from dwh.underwriting_quotes_data uw
left join dwh.v_agents a on uw.agent_id = a.agent_id
where cob in ('Restaurant') and uw.lob in ('GL', 'BP','CP') and policy_status in (4,7) and business_id = '6cfd57a0dc985e033d44a428333823b1'
 order by offer_creation_time

--liquor pct
select json_extract_path_text(json_args, 'lob_app_json','liquor_sales_yes_no',true) as liquor_yes_no,
       count(distinct business_id)
from dwh.quotes_policies_mlob qpm
where highest_policy_status = 4 and
      lob_policy = 'GL' and
      cob = 'Restaurant' and
      offer_flow_type in ('APPLICATION','RENEWAL','CANCEL_REWRITE') and
      creation_time >= '2023-06-01' and
      liquor_yes_no <> ''
group by 1

select distinct policy_status, policy_status_name from dwh.underwriting_quotes_data uw
order by policy_status desc


select median (revenue_in_12_months) over () as median
from dwh.underwriting_quotes_data uw
where lob = 'GL' and cob in ('Coffee Shop', 'Bakery', 'Food Truck','Caterer') and policy_status >= 4 and offer_creation_time >= '2024-01-01' limit 1


('Coffee Shop', 'Bakery', 'Food Truck','Caterer')

-- Jacob SC query
SELECT
    policy_id,
    business_id,
    policy_status_name,
    cancellation_reason,
    start_date,
    end_date,
    policy_reference
FROM
    dwh.company_level_metrics_ds
WHERE
    policy_status_name = 'Canceled'
    AND end_date BETWEEN CURRENT_DATE - INTERVAL '30 days' AND CURRENT_DATE
AND business_id IN (
    'e7feea99e4816370930d8dd82d5144d5', '257db1040fdd5da4c62dbd170469029b',
    '6438992d737120caaf69a63981fb25de', '9c14cc5d7db6132b173542f9c2350f35',
    'a46d80c0d81daf82dba36db6d47328c7', '2c8677e22e25b35ca5aeec86980c197e',
    '7ee0ad35ef7cff03c1ace4263c556c8f',
    'd39a3dc0300a936298b1fc73aa3590d9',
    'a05d95205d7550a837273db56f820560', 'cef01e1ea1559830f5fe0f4b4d4bdbff',
    'a7a2b44b89de71c524243a13055e224c',
    'ef18bc8f9769fc9bbef1ad529337497e', '04c886d7bc551e9c873f0b594ad7bcbd',
    '31e2650ef05d7006d0570c9d7fbc0788', '21d7b7bb444c7b6a1db50eff589753c1',
    'f1a7d5066da09d7319bad28bf6e744cc', '9c83d8b14508768e64afd722ab8e4aad',
    'f159fd4513cd566bcf159a1a0199cdba', '1a94e094147fabeea357018455a1ab63',
    '624054c38332ac486567f786063112f2', 'f70c5682847860d0220d1f05440ee35c',
    'f7252ad84c05559296122554ef4ae39a', '01b9fa0f6415cba55d3835ed13c0a31c',
    '7cc1edd741f66ab73b3365be94a03cfe', 'edfdfefff74d7b309b7ebc17246f25b9',
    '681f7fe700651bc41a1a5154747f0141', 'f89a6a8204ebadce01c34a096eead131',
    'e32c45497939815cd6344685e73a948c', 'ddc3b0af022711faff12e5568ca00c69',
    'da6ab3d3068f441129b434646f066d8b', '10868a9839116963af7ff385aa2c1295',
    '146594252e2a38e0a07f93e34cec03dd', 'b5a779a1dfe672dab10420e785b1ff70',
    'ef6ea218f9cce1e0892ce57569a4baf6',
    '1cb92463f4144ada389a11d773b57f4a', 'e620895a751aab1cfda1c40722ebe913',
    '3cb13f4870b4d9c0751b17b9c79ea358', '85ced01c83e4b452623b8a967917e602',
    'b27ca854fc866bec59269fc80f34e7db', '6aa420dc5c4638c81b1a9eacae5e8fb4',
    '40de44a104293baff21837b74832096f',
    '2c7832d30601d025564b0af5a13cfca6', '62821df21ced4a6ff1cdbed81a281d70',
    'dbbd7db01d5c80d440e5607eafa4cef2', '5603e0240f0652c02fb773d41ef312b9',
    '9b06227b7a33fb661dcb7462b551d69c', '013c7ad1093b9f51c7bba850e4177dd6',
    '96c80c5fe4de8a42b1de79d49248689f', 'e94305443783a0bbb74ab830760b8c89',
    'f032a67c2afc302d9eac285b72be68ab', '86c183bf8c64d26c682d414357fec729',
    '22bd491b01165f235f4fff54847566a2', 'c01f5c84f80be857433c65b3f12bf9ec',
    '24a979bdad27d26b06659c537c8c5159', '268d11dc4e62a13d173ba0c3beba80ec',
    'dbdc0214709c932152759166636ba106', 'b060d7f9ec0c1af2f188f97c101a84a6',
    'fecf473a86af7776ceed3bf48b94eeb0', 'c9018894109310bbbc264bca89d4cfac',
    'c51623f11d8e856fc969ff8d325945eb', 'df074ee53fd299b529191c8371c6f20d',
    '0733e40cccdc5c731389a4aa6a41cd85',
    'c54ef28f6fc0d1f79fa5fa7b0fa82a2e', '16112e923c7984bec9dae915a5b7eca2',
    '5249a7d67ec95f21f2365e393d842d4b', '16c3ffb02ced51ef46913796d16b8b64',
    '347a1cb161827278811731144407efb7', '56756af3891e2a524dfaf7cf7d2a4c8d',
    'b6c01462c06d42e8f73eaac227ba6057', '6922b694db55163dc312e8673e7cd700',
    '47618573949b925c8db1dabd9d02f48c', '613f3bfaa36e27a1c9d7ceae6b3c83a8',
    'ec11d9d2c971a83e2de6865b9f6ad1aa', '5e52d233a163c9f7cb7e49e7e728adef',
    '880a79d4af6941cd767d649cf367dbe8', 'a0eb4606849ff2b270638583954e7df5',
    'a55a77193a6ca464f646aa981a100bd4', 'da921b168e0c4b3ed66db8399af3e2e8',
    'ab756a94c883dfdc71b98d5cc36670b4', 'ae02cec8d3f1ac2a6627e26b16693427',
    '074f2c500fa442d6255aac87c1725502', 'ae33e59a3e549807f9e4703b6b0a5d10',
    '61f2df9567a001e1f7fad8dced6a60df', '1fecc362002c2de8d12ef0debd85cb22',
    'a551a8ca7fa531acd4b8c54754c78b25', 'f51132a391e7f7c1fcfc48c64b78986b',
     '02aa68efe023e005c36bd54897ac6a63',
    '292630b6d9478cefdcd628d10b82fe55', '27008f26c37a7a56a484e727b5554456',
    '54688bdb29c839dfc51e71e38204da79', '28e2b8a09d8c569f05b8c3a2b7798183',
    'ed2c8ddead94605902308aaea166f2f8', '0b4aa06ed6a1a7635e7bf55079704d95',
    '5251201056cb3b3e81ce35b6f3d73737', '73350ddfa0e2bba58f7975de85b98287',
    'ae7407603b315fa827c98bfb10e61b92', '770c0a56c302872b7290ddd7a0ac3c7c',
    '9b7025ceaac1594658c16da89d4a8911', 'ce3a920cf2b13cb6284de8271bd0db7b',
    'b262474aed7bb72655db7b943742ff32', '61fd40b797a7acec34a27dad9cdc0c32',
    '6a9ad455e2f54979728a3d559168825b', '8bdd6941ec2e019e914ae39ff8960b90',
    '60b839bb66e66464cf3fa8864754e97a', 'aca5017754868bdb3f50fbd857ac1b44',
    '48e75ed99bc64ea2214db76bf21be8d1', '1ef98da6d2bde0ab527685a97d84bc17',
    '005e335fe103241155b556e20ddc6b90', '357d84b12fef65b2e3b1e9bf8363738b',
    'c04e5a6047659eeaea95dcff2db080fc', 'b97b03147470c087f6d17002e269fd4a',
    'b97b03147470c087f6d17002e269fd4a', 'f2bc6cf27e2b9b7a2fc3a141ed5bef3f',
    '49a6895df95dbd88c618c68bafeaeff1', '1d806eb3a6c6625b8680f2cb6de7ed86',
    '73182748005dcb0373c422050e73a693', '5203218cb88a6c2a9aa737f4706438cc',
    '5203218cb88a6c2a9aa737f4706438cc', 'a5d62a1e79836f9ed98e629503a85935',
    '1f21252fce5594c37733356b112b91dc', '04bc289afb6040074a97c8266b6d560a',
    '6c3a69262a071e03680bfd43aa3530dc', 'f64cebbb918d9064ae60b43b9b87e54a',
    '132d22f47d0d5a179aabd27f84fb9730', '2699c8ba8fd7556a372a57f5dbf75f4a',
    'e124a93cb4654329694e98ab9dc4c0eb', '86f0a547844bb01512aa13978fb4fe7c',
    '0acfbe355b4ad09b7ce5b7cafe38a180', 'c9f84eee7c77568798e9d0b163fdaf87',
    '567d954debc47b494239b2a3226c638a',
    '44e094ca6c1d0f3102d4e92a59baaa0a', '57abb04e84d4ea779a4ab195ee83516e',
    '260d61bfc492e800cacc39c245d9594f', 'fe942873bf3a38183e22592b27d11ec6',
    '0f7b7f0484a64b291d44ffd8f9d909e4', '4f6a726057d5c5bc7ecd2495df95bbc9',
    'cb018acf2f12016808f20a1819492e80', '8aa43394b23be85c6cbbbb324d2a7f1b',
    'ea02267a3acb4ea7df2f33cd9a4b18bf'
)
AND lob = 'GL'

SELECT
    json_extract_path_text(response_data,'Address','Zip',true) as zip_code,
    CAST(AVG(CAST(json_extract_path_text(json_extract_path_text(response_data,'Ms3','Crime','AggregateCrimesAgainstProperty',true), 'IndexValuesUpto10','Current',true) AS DECIMAL(10,2))) AS DECIMAL(10,2)) AS avg_crime_score
FROM db_data_science.GL_historical_crime_data_202502_allpolicies
WHERE json_extract_path_text(json_extract_path_text(response_data,'Ms3','Crime','AggregateCrimesAgainstProperty',true), 'IndexValuesUpto10','Current',true) ~ '^[0-9]+(\.[0-9]*)?$'  -- Ensures we only process numeric values
GROUP BY zip_code;


-- CP TIV decline
with gl_policies as (
select a.*,
date(offer_creation_time) as creation_ds,
(CASE WHEN (affiliate_id = 'N/A' and  agent_id = 'N/A') then 'direct'
WHEN (affiliate_id <> 'N/A' and  agent_id = 'N/A') then 'affiliate'
else 'agent' end) as channel,
b.marketing_cob_group
from dwh.underwriting_quotes_data a
left join dwh.sources_test_cobs b
on a.cob = b.cob_name

where execution_status = 'SUCCESS'
and offer_creation_time >= '2022-10-01'
and lob = 'GL'
and a.cob = 'Restaurant'
),

crime as (
select distinct street,
zip_code_5digit,
nullif(json_extract_path_text(verisk_json_response, 'Ms3', 'Crime', 'AggregateCrimesAgainstProperty',
'IndexValuesUpto10', 'Current', true), '') as crime_score
from (select street,
cast(right('00000' + zip_code, 5) as varchar(5)) as zip_code_5digit,
creation_time,
verisk_json_response,
rank() over (partition by street, zip_code_5digit order by creation_time desc) as rnk
from riskmgmt_svc_prod.verisk_property_risk_request_response
) rank_table
where rnk = 1
)


select a.cob,
a.offer_id,
a.business_id,
creation_time,
channel,
b.state,
b.street,
b.city,
b.zip_code,
crime_score


from declines a
left join dwh.quotes_policies_mlob_dec b
on a.offer_id = b.offer_id
left join crime c
on b.zip_code = c.zip_code_5digit
and b.street = c.street
on a.business_id = d.business_id

where b.creation_time >= '2022-10-01' and d.business_id = '52e07ab66786c1bcbdcf7cbbfd00e5bb'
and b.lob_policy = 'CP'

with crime as (
select distinct street,
zip_code_5digit,
nullif(json_extract_path_text(verisk_json_response, 'Ms3', 'Crime', 'AggregateCrimesAgainstProperty',
'IndexValuesUpto10', 'Current', true), '') as crime_score
from (select street,
             cast(right('00000' + zip_code, 5) as varchar(5))                               as zip_code_5digit,
             creation_time,
             verisk_json_response,
             rank() over (partition by street, zip_code_5digit order by creation_time desc) as rnk
      from riskmgmt_svc_prod.verisk_property_risk_request_response) rank_table
where rnk = 1
)
select distinct business_id, b.street, b.zip_code, cob,crime_score from dwh.quotes_policies_mlob_dec b
left join crime c
on b.zip_code = c.zip_code_5digit
and b.street = c.street
where cob = 'Restaurant' and lob_policy in ('BP','GL') and highest_policy_status >=4



select * from db_data_science.GL_historical_crime_data_202502_allpolicies where business_id = '12835ba392654da0e291efb0595c2325'


select street,
             cast(right('00000' + zip_code, 5) as varchar(5)) as zip_code_5digit,
             creation_time,
             verisk_json_response,
             rank() over (partition by street, zip_code_5digit order by creation_time desc) as rnk
      from riskmgmt_svc_prod.verisk_property_risk_request_response where street = '7013 Melrose Ave' and zip_code_5digit = '90038'

with d1 as (select TRIM(LOWER(json_extract_path_text(response_data, 'Address', 'Zip'))) AS zip_code,
             creation_time,
             response_data,
             TRIM(LOWER(json_extract_path_text(response_data, 'Address', 'StreetAddress1'))) AS street
--              rank() over (partition by street, zip_code order by creation_time desc) as rnk
      from Insurance_data_gateway_svc_prod.third_parties_data)

select * from d1 where street = '7013 melrose ave'

select TRIM(LOWER(json_extract_path_text(response_data, 'Address', 'Zip'))) AS zip_code,
             creation_time,
             response_data,
             TRIM(LOWER(json_extract_path_text(response_data, 'Address', 'StreetAddress1'))) AS street
--              rank() over (partition by street, zip_code order by creation_time desc) as rnk
      from Insurance_data_gateway_svc_prod.third_parties_data  where street = '7013 melrose ave'



select distinct offer_creation_time::date,business_id, business_name, cob, uw.lob,yearly_premium, uw.street,uw.city, uw.state_code,uw.zip_code,
                affiliate_id,agent_id,(CASE WHEN (affiliate_id = 'N/A' and  agent_id = 'N/A') then 'direct'
    WHEN (affiliate_id <> 'N/A' and  agent_id = 'N/A') then 'affiliate'
                    else 'agent' end) as channel
from dwh.underwriting_quotes_data uw
where policy_status = 4 and uw.lob = 'GL'
and cob = 'Restaurant'
order by offer_creation_time desc


with address as (select distinct business_id, uw.street,uw.city, uw.state_code,uw.zip_code
    from dwh.underwriting_quotes_data uw where policy_status = 4 and uw.lob in ('GL','BP') and cob = 'Restaurant')

select distinct clm.business_id,clm.channel,clm.policy_status_name,yearly_premium,street,city, state_code,zip_code
from dwh.company_level_metrics_ds clm
    left join address on clm.business_id = address.business_id
where cob_name = 'Restaurant' and business_lob = 'GL' and policy_status_name = 'Active'

select * from dwh.underwriting_quotes_data where business_id = '412966d8df4faa593314f43a605d6235'
select * from dwh.company_level_metrics_ds clm where business_id = '412966d8df4faa593314f43a605d6235'



select * from dwh.company_level_metrics_ds limit 10

select distinct policy_status_name from dwh.company_level_metrics_ds

-- Albus lounge and sports bars
WITH event_data AS (
    SELECT
        dm.business_id,
        dm.screening_id,
        ed.activities,
        ed.creation_time,
        ROW_NUMBER() OVER (PARTITION BY dm.business_id ORDER BY ed.creation_time DESC) AS row_num
    FROM db_data_science.ds_decline_monitoring dm
    LEFT JOIN (
        SELECT
            creation_time,
            JSON_EXTRACT_PATH_TEXT(event_payload, 'data', 'riskScreeningId') AS riskScreeningId,
            JSON_EXTRACT_PATH_TEXT(event_payload, 'data', 'additionalData', 'activities') AS activities
        FROM riskmgmt_svc_prod.business_events
        WHERE JSON_EXTRACT_PATH_TEXT(event_payload, 'data', 'riskScreeningStep') = 'ALBUS_KEYWORDS'
    ) ed ON dm.screening_id = ed.riskScreeningId
    WHERE dm.stepname = 'ALBUS_KEYWORDS'
      AND dm.offer_flow_type = 'APPLICATION'
      AND ed.creation_time > '2025-01-01'
      AND ed.activities IS NOT NULL  -- Exclude NULL activities
)

SELECT
    clm.business_id,
    clm.start_date,
    b.business_name,
    a.street_address AS street,
    a.city,
    a.state_code AS state,
    a.zip_code,
    clm.lob,
    dm_events.screening_id,
    dm_events.activities
FROM dwh.company_level_metrics_ds clm
JOIN nimi_svc_prod.businesses b ON b.business_id = clm.business_id
JOIN nimi_svc_prod.addresses a ON a.business_id = clm.business_id
JOIN event_data dm_events ON clm.business_id = dm_events.business_id
WHERE
    clm.policy_status_name = 'Active'
    AND clm.cob_name = 'Restaurant'
    AND a.is_primary = 1
    AND clm.lob IN ('GL', 'BP')
    AND clm.business_lob = 'GL'
    AND clm.policy_category = 'new'
    AND clm.start_date > '2025-02-01'
    AND dm_events.row_num = 1
    AND dm_events.activities ILIKE '%sport%';



select * from db_data_science.ultimate_lrd limit 10

select * from db_data_science.ultimate_lrd where cob_group = 'Food & beverage' and accident_month >= '2024-01-01' and accident_month <= '2024-12-01'


select cob, lob, execution_status,decline_reasons, count (distinct business_id) as declines
from dwh.underwriting_quotes_data uw
where cob in ('Grocery Store')
and offer_creation_time >= '2025-01-01'
and uw.lob in ('BP','GL')
and execution_status in ('DECLINE')
and offer_flow_type in ('APPLICATION')
group by cob, lob, execution_status,decline_reasons
order by cob, lob,execution_status,declines desc

select distinct offer_creation_time, business_id, business_name, revenue_in_12_months from dwh.underwriting_quotes_data uw
where cob in ('Grocery Store') and decline_reasons = '["Your annual revenue is too large for this insurance policy to cover."]'

select distinct business_id, business_name from dwh.underwriting_quotes_data uw where cob in ('Restaurant') and policy_status >= 3


select distinct business_id, uw.zip_code
from dwh.underwriting_quotes_data uw
where uw.lob in ('GL','BP') and cob in ('Restaurant')
order by offer_creation_time desc


select *
from "dl-rds-albusgpt-prod".public_business_hours_of_operation hrs
join "dl-rds-albusgpt-prod".public_business b on b.id = hrs.business_id
where hrs.is_reconciled = TRUE and next_insurance_business_id = '94841e4ac43d1880b916a934cb949bc5'

with t1 as (
select distinct qpm.business_id, json_extract_path_text(json_args,'business_name',true) as business_name,
                json_extract_path_text(json_args,'lob_app_json','business_closing_hour',true) as business_closing_hour,
                json_extract_path_text(json_args, 'lob_app_json','location.liquor_sales_exposure', true) as liquor_sales_exposure,
                qpm.state
from dwh.quotes_policies_mlob qpm
where cob = 'Restaurant' and business_closing_hour is not null and offer_flow_type in ('APPLICATION')),

    t2 as (select *
from "dl-rds-albusgpt-prod".public_business_hours_of_operation hrs
join "dl-rds-albusgpt-prod".public_business b on b.id = hrs.business_id
where hrs.is_reconciled = TRUE)

select * from t1 left join t2 on t1.business_id = t2.next_insurance_business_id and t1.business_name = t2.name

select *, json_extract_path_text(json_args, 'lob_app_json','location.liquor_sales_exposure', true) as liquor_sales_exposure
   from dwh.quotes_policies_mlob qpm where business_id = '99289636cc0088d1629fbfb95de6cd24'




select *
from "dl-rds-albusgpt-prod".public_business_hours_of_operation hrs
join "dl-rds-albusgpt-prod".public_business b on b.id = hrs.business_id
where hrs.is_reconciled = TRUE and next_insurance_business_id = '9edaf6c5bed5b32de3dc1abb00cda2aa'


select json_extract_path_text(event_payload, 'data', 'businessId') as business_id
      ,json_extract_path_text(event_payload, 'data', 'screeningId') as screen_id
      ,creation_time
      ,ROW_NUMBER() OVER (PARTITION BY screen_id ORDER BY creation_time DESC) as row_num
      ,case when event_payload like '%"status": "DECLINE", "stepName": "HOURS_OF_OPERATION_LATE_NIGHT%' then 1 else 0 end as declined_hours_of_operation
      ,case when event_payload like '%"businessSearchResult": "No hours of operation"%' then 1 else 0 end as no_hit_hours_of_operation
      ,event_payload
from riskmgmt_svc_prod.business_events
where true
      and creation_time >= '2025-04-26'
      and event_payload like '%"stepName": "HOURS_OF_OPERATION%'
      and len(json_extract_path_text(event_payload, 'data', 'businessId'))>0





select *
from "dl-rds-albusgpt-prod".public_business_hours_of_operation hrs
join "dl-rds-albusgpt-prod".public_business b on b.id = hrs.business_id
where hrs.is_reconciled = TRUE and next_insurance_business_id = '11de82bf7f0c9df98ce84cc94db1ed67'

select * from external_dwh.gl_quotes where business_id = 'abc9e8cb8ee220a2ad31667c4f93e3e1'

select * from db_data_science.gl_monitoring_policy_data limit 10