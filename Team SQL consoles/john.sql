-- joins rating log table (rating_svc_prod) with application info (quotes_policies_mlob)
-- ultimately this query is looking to get schedule rating factor for PL at the policy level, with info on policy characteristics (for sorting)

with qp as (select policy_start_date
                 , policy_end_date
                 , state
                 , highest_policy_id
                 , p.policy_reference
                 , case
                       when highest_status_package = 'basic'
                           then basic_quote_job_id -- these rows are necessary to create the join on job_id?
                       when highest_status_package = 'basicTria' then basic_tria_quote_job_id
                       when highest_status_package = 'pro' then pro_quote_job_id
                       when highest_status_package = 'proTria' then pro_tria_quote_job_id
                       when highest_status_package = 'proPlus' then pro_plus_quote_job_id
                       when highest_status_package = 'proPlusTria' then pro_plus_tria_quote_job_id
                       else pro_quote_job_id
        end as Quote_job_id
            from dwh.quotes_policies_mlob qpm
                     left join nimi_svc_prod.policies p
                               on qpm.highest_policy_id = p.policy_id
            where highest_policy_status >= 3
              and lob_policy = 'PL'
--         and qpm.creation_time >='2019-10-01'
--         and policy_start_date<'2021-01-01'
--         and policy_end_date>='2020-12-31'----- Active As of 2020-12-31
    --and new_reneweal= 'new'
)
   , rc as (with SR_new as (select job_id
                                 , creation_time::date as SR_Crtdate
                                 , calculation
                                 , data_points
                            from s3_operational.rating_svc_prod_calculations -- there are two rating tables; this is the new one, rating_svc_prod is the old one (i think); this code joins them
                            where lob = 'PL'),
                 inoldnotnew as (select job_id
                                 from rating_svc_prod.rating_calculations
                                 where lob = 'PL'
                                 except
                                 select job_id
                                 from s3_operational.rating_svc_prod_calculations
                                 where lob = 'PL'),
                 SR_old as (select a.job_id
                                 , creation_time::date as SR_Crtdate
                                 , calculation
                                 , data_points
                            from rating_svc_prod.rating_calculations a
                                     join inoldnotnew b on a.job_id = b.job_id
                            where lob = 'PL')

            select *
            from SR_new
            union
            select *
            from SR_old)
select
--  qp.purchase_date,
    qp.policy_start_date
     , qp.policy_end_date
--        qp.business_id,
--        ,qp.lob_policy
--        ,json_extract_path_text(json_args, 'carrier',true) as carrier
     , asl.carrier_name
--        concat(json_extract_path_text(json_args,'num_of_employees',true) ,json_extract_path_text(json_args, 'num_of_employees_std',true)) as Number_of_Employee,
--        concat(json_extract_path_text(json_args, 'num_of_owners_std',true), json_extract_path_text(json_args, 'num_of_owners_std_v2',true)) as number_of_owner,
--        qp.cob_group,
--        qp.cob,
     , qp.state
--        qp.new_reneweal,
--        qp.distribution_channel,
--        qp.highest_policy_status,
--        qp.highest_status_name,
     , qp.highest_policy_id
     , qp.policy_reference
     , rc.job_id
--        qp.highest_yearly_premium,
--       json_extract_path_text(rc.calculation_summary, 'Subtotal Final Premium With State Surcharges',true) as Final_premium
--     ,json_extract_path_text(rc.calculation_summary, 'Subtotal Premium Before Minimum',true) as Premium_before_minimum
--     ,json_extract_path_text(rc.calculation_summary, 'Minimum Premium',true) as Minimum_premium
--     ,split_part(json_extract_path_text(rc.calculation,'Subtotal Base Premium','calculated revenue','result',true),'|',1) as calculated_revenue
--     ,json_extract_path_text(rc.calculation,'Subtotal Base Premium','calculated revenue','b2 <- Input revenue from user',true) as user_input_revenue
--     ,split_part(concat(json_extract_path_text(rc.calculation, 'Subtotal Premium Before Minimum','t1 <- Before Minimum Factor','c1 <- Other Prior Acts Factor',true)
--            ,json_extract_path_text(rc.calculation, 'Subtotal Premium Before Minimum','t1 <- Before Minimum Factor','c1 <- Other Prior Acts Factor',true)),'|',1) as Retro_Factor
     , split_part(json_extract_path_text(rc.calculation, 'Subtotal Final Premium', 'z2 <- final Factor',
                                         'q4 <- Schedule Rating Factor', 'result', true), '|', 1) as SR_result

from qp
         left join rc
                   on qp.quote_job_id = rc.job_id
         left join reporting.gaap_snapshots_asl asl
                   on qp.highest_policy_id = asl.policy_id
group by 1, 2, 3, 4, 5, 6, 7, 8

select *,
       p.policy_reference,
       json_extract_path_text(json_args, 'business_name', true) as business_name
from dwh.quotes_policies_mlob qpm
         join nimi_svc_prod.policies p
              on qpm.highest_policy_id = p.policy_id
where lob_policy = 'GL'
  and qpm.creation_time >= '2022-01-01'
  and cob_group = 'Retail'
limit 10

select *
from dwh.quotes_policies_mlob
where cob = 'E-Commerce'
  and highest_policy_status >= 3
  and lob_policy = 'GL'
limit 10

select json_extract_path_text(json_args, 'retail_market_website_link', true) as website
from dwh.quotes_policies_mlob
where cob = 'E-Commerce'
  and highest_policy_status >= 3
  and lob_policy = 'GL'
  and website IS NOT NULL
limit 10

select split_part(split_part(json_args, 'retail_market_website_link', 2), ',', 1) as website
from dwh.quotes_policies_mlob
where cob = 'E-Commerce'
  and highest_policy_status >= 3
  and lob_policy = 'GL'
  and website <> ''
limit 10


select business_id,
       offer_id,
       highest_policy_status,
       lob_policy,
       cob,
       state,
       highest_status_package,
       highest_policy_aggregate_limit,
       highest_yearly_premium
from dwh.quotes_policies_mlob as qpm
where highest_policy_status = 4
  and lob_policy = 'PL'

select business_id,
       split_part(split_part(json_args, 'retail_market_website_link', 2), ',', 1) as website,
       purchase_date
from dwh.quotes_policies_mlob qpm
where cob = 'E-Commerce'
  and highest_policy_status >= 3
  and lob_policy = 'GL'
  and website <> ''

--get list of retail websites
select business_id,
       split_part(split_part(json_args, 'retail_market_website_link', 2), ',', 1) as website,
       purchase_date,
       split_part(split_part(json_args, 'retail_market_website', 2), ',', 1)      as has_website,
       distribution_channel
from dwh.quotes_policies_mlob qpm
where cob = 'E-Commerce'
  and highest_policy_status >= 3
  and lob_policy = 'GL'
  and has_website = '" : "Yes"'
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')

select business_id, qpm.*
from dwh.quotes_policies_mlob qpm
where cob = 'E-Commerce'
  and highest_policy_status >= 3
  and lob_policy = 'GL'
limit 10


select business_id,
       offer_id,
       highest_policy_status,
       lob_policy,
       cob,
       state,
       highest_status_package,
       highest_yearly_premium,
       distribution_channel
from dwh.quotes_policies_mlob as qpm
where highest_policy_status >= 3
  and lob_policy = 'GL'
  and cob_group = 'Food & beverage'
  and purchase_date >= '2022-08-01'
  and purchase_date < '2022-08-08'

select business_id,
       purchase_date,
       highest_policy_status,
       qpm.*
from dwh.quotes_policies_mlob as qpm
where highest_policy_status >= 3
  and lob_policy = 'GL'
limit 10

select *
from db_data_science.v_all_agents_policies_v2
limit 10

select business_id,
       cob,
       state,
       highest_yearly_premium,
       distribution_channel
from dwh.quotes_policies_mlob as qpm
where highest_policy_status >= 3
  and lob_policy = 'GL'
  and purchase_date >= '2022-07-01'
  and highest_yearly_premium > 20000
order by highest_yearly_premium desc

select business_id,
       offer_id,
       purchase_date,
       highest_policy_status,
       lob_policy,
       cob,
       cob_group,
       state,
       highest_status_package,
       highest_yearly_premium,
       distribution_channel,
       new_reneweal
from dwh.quotes_policies_mlob as qpm
where highest_policy_status >= 3
  and purchase_date >= '2021-01-01'
  and purchase_date < '2022-06-30'

select business_id,
       offer_id,
       purchase_date,
       creation_time,
       highest_policy_status,
       highest_status_name,
       lob_policy,
       cob,
       cob_group,
       state,
       highest_status_package,
       highest_yearly_premium,
       distribution_channel,
       new_reneweal
from dwh.quotes_policies_mlob as qpm
where highest_policy_status = 1
  and lob_policy = 'GL'
  and creation_time >= '2022-07-01'
  and creation_time < '2022-08-17'
limit 30

--get business ids for specific decline reasons
select business_id, decline_reasons, cob, lob
from dwh.underwriting_quotes_data as qdata
where start_date >= '2024-01-01'
  and business_id <> ''
--and execution_status = 'DECLINE' and decline_reasons like '%Criminal record%'

--get business ids for specific decline reasons
select distinct business_id
from dwh.underwriting_quotes_data as qdata
where start_date >= '2024-01-01'
  and business_id <> ''

select qdata.cob, qpm.cob_group, count(distinct (qdata.business_id)) as biz_count
from dwh.underwriting_quotes_data as qdata
         join dwh.quotes_policies_mlob qpm on qdata.business_id = qpm.business_id
where start_date >= '2023-01-01'
  and lob = 'IM'
group by 1, 2
order by biz_count desc



select business_id,
       offer_id,
       purchase_date,
       creation_time,
       highest_policy_status,
       highest_status_name,
       lob_policy,
       cob,
       cob_group,
       state,
       highest_status_package,
       highest_yearly_premium,
       distribution_channel,
       new_reneweal
from dwh.quotes_policies_mlob as qpm
where highest_policy_status = 1
  and lob_policy = 'GL'
  and creation_time >= '2022-07-01'
  and creation_time < '2022-08-17'
limit 30

select *
from dwh.all_activities_agents
where eventtime >= '2023-04-17'
limit 10

select business_id, highest_yearly_premium, policy_start_date
from dwh.quotes_policies_mlob
where cob = 'Claims Adjuster'
  and lob_policy = 'PL'

select question_answer, final_quote_status
from dwh.all_activities_table
where cob_name = 'Restaurant'
  and lob = 'GL'
limit 10
    10 aaa866f23ad8a1f789bc84715681cd

select cob_name, count(distinct tracking_id)
from dwh.all_activities_table
where eventtime >= '2023-04-17'
  and cob_name <> ''
group by 1
order by 2 desc

select business_id,
       purchase_date,
       highest_policy_status,
       highest_status_name,
       highest_yearly_premium,
       distribution_channel
from dwh.quotes_policies_mlob as qpm
where highest_policy_status = 4
  and lob_policy = 'GL'
  and cob = 'Medical Supplies Store'

--get top selling amazon product categories
select business_id,
       split_part(split_part(json_args, 'retail_market_website_link', 2), ',', 1) as website,
       purchase_date,
       split_part(split_part(json_args, 'retail_market_website', 2), ',', 1)      as has_website,
       distribution_channel
from dwh.quotes_policies_mlob qpm
where cob = 'E-Commerce'
  and highest_policy_status >= 3
  and lob_policy = 'GL'
  and has_website = '" : "Yes"'

select json_args,
       split_part(split_part(json_args, '1300000_rev_perc', 2), ',', 1) as appliances
from dwh.quotes_policies_mlob
where cob = 'E-Commerce'
  and highest_policy_status >= 3
  and lob_policy = 'GL'
limit 100

Select qpm.highest_policy_id,
       p.policy_reference,
       split_part(split_part(qpm.json_args, '1300000_rev_perc', 2), ',', 1) as appliances,
       json_args,
       qpm.highest_policy_status
from dwh.quotes_policies_mlob qpm
         join nimi_svc_prod.policies p on qpm.highest_policy_id = p.policy_id
where p.policy_reference is not null
  and qpm.policy_end_date > qpm.policy_start_date
  and qpm.lob_policy = 'GL'
  and qpm.cob = 'E-Commerce'
  and appliances <> ''

select qpm.*,
       business_id,
       offer_id,
       highest_policy_status,
       lob_policy,
       cob,
       state,
       highest_status_package,
       highest_yearly_premium,
       distribution_channel
from dwh.quotes_policies_mlob as qpm
where highest_policy_status = 4
  and lob_policy = 'CA'
  and CARRIER_NAME = 'NEXT_CARRIER'


with qp as (select policy_start_date
                 , policy_end_date
                 , state
                 , highest_policy_id
                 , p.policy_reference
                 , case
                       when highest_status_package = 'basic'
                           then basic_quote_job_id -- these rows are necessary to create the join on job_id?
                       when highest_status_package = 'basicTria' then basic_tria_quote_job_id
                       when highest_status_package = 'pro' then pro_quote_job_id
                       when highest_status_package = 'proTria' then pro_tria_quote_job_id
                       when highest_status_package = 'proPlus' then pro_plus_quote_job_id
                       when highest_status_package = 'proPlusTria' then pro_plus_tria_quote_job_id
                       else pro_quote_job_id
        end as Quote_job_id
            from dwh.quotes_policies_mlob qp
                     left join nimi_svc_prod.policies p
                               on qp.highest_policy_id = p.policy_id
            where highest_policy_status >= 3
              and lob_policy = 'GL'
--         and qpm.creation_time >='2019-10-01'
--         and policy_start_date<'2021-01-01'
--         and policy_end_date>='2020-12-31'----- Active As of 2020-12-31
    --and new_reneweal= 'new'
)
   , rc as (with SR_new as (select job_id
                                 , creation_time::date as SR_Crtdate
                                 , calculation
                                 , data_points
                            from s3_operational.rating_svc_prod_calculations -- there are two rating tables; this is the new one, rating_svc_prod is the old one (i think); this code joins them
                            where lob = 'GL'),
                 inoldnotnew as (select job_id
                                 from rating_svc_prod.rating_calculations
                                 where lob = 'GL'
                                 except
                                 select job_id
                                 from s3_operational.rating_svc_prod_calculations
                                 where lob = 'GL'),
                 SR_old as (select a.job_id
                                 , creation_time::date as SR_Crtdate
                                 , calculation
                                 , data_points
                            from rating_svc_prod.rating_calculations a
                                     join inoldnotnew b on a.job_id = b.job_id
                            where lob = 'GL')

            select *
            from SR_new
            union
            select *
            from SR_old)
select
--  qp.purchase_date,
    qp.policy_start_date
     , qp.policy_end_date
--        qp.business_id,
--        ,qp.lob_policy
--        ,json_extract_path_text(json_args, 'carrier',true) as carrier
     , asl.carrier_name
--        concat(json_extract_path_text(json_args,'num_of_employees',true) ,json_extract_path_text(json_args, 'num_of_employees_std',true)) as Number_of_Employee,
--        concat(json_extract_path_text(json_args, 'num_of_owners_std',true), json_extract_path_text(json_args, 'num_of_owners_std_v2',true)) as number_of_owner,
--        qp.cob_group,
--        qp.cob,
     , qp.state
--        qp.new_reneweal,
--        qp.distribution_channel,
--        qp.highest_policy_status,
--        qp.highest_status_name,
     , qp.highest_policy_id
     , qp.policy_reference
     , rc.job_id
--        qp.highest_yearly_premium,
--       json_extract_path_text(rc.calculation_summary, 'Subtotal Final Premium With State Surcharges',true) as Final_premium
--     ,json_extract_path_text(rc.calculation_summary, 'Subtotal Premium Before Minimum',true) as Premium_before_minimum
--     ,json_extract_path_text(rc.calculation_summary, 'Minimum Premium',true) as Minimum_premium
--     ,split_part(json_extract_path_text(rc.calculation,'Subtotal Base Premium','calculated revenue','result',true),'|',1) as calculated_revenue
--     ,json_extract_path_text(rc.calculation,'Subtotal Base Premium','calculated revenue','b2 <- Input revenue from user',true) as user_input_revenue
--     ,split_part(concat(json_extract_path_text(rc.calculation, 'Subtotal Premium Before Minimum','t1 <- Before Minimum Factor','c1 <- Other Prior Acts Factor',true)
--            ,json_extract_path_text(rc.calculation, 'Subtotal Premium Before Minimum','t1 <- Before Minimum Factor','c1 <- Other Prior Acts Factor',true)),'|',1) as Retro_Factor
     , split_part(json_extract_path_text(rc.calculation, 'PremOps premium', true), '|', 1) as premops_premium
limit 10

select business_id,
       offer_id,
       highest_status_name,
       lob_policy,
       cob,
       state,
       highest_status_package,
       highest_yearly_premium,
       distribution_channel
from dwh.quotes_policies_mlob as qpm
where highest_policy_status >= 3
  and highest_policy_status <= 4
  and cob = 'E-Commerce'
  and distribution_channel = 'partnerships'
  and policy_start_date >= '2022-08-01'
  and policy_start_date < '2022-09-01'

--get decline reasons with qre json args
select qdata.business_id, qdata.decline_reasons, qpm.json_args
from dwh.underwriting_quotes_data qdata
         left join dwh.quotes_policies_mlob qpm
                   on qpm.business_id = qdata.business_id
where qdata.start_date >= '2022-01-01'
  and qdata.lob = 'CP'
  and qpm.cob = 'Restaurant'
  and qdata.business_id <> '""'
  and qdata.execution_status = 'DECLINE'
limit 30


select business_id,
       offer_id,
       highest_status_name,
       lob_policy,
       cob,
       state,
       highest_status_package,
       highest_yearly_premium,
       distribution_channel,
       carrier_name,
       policy_end_date
from dwh.quotes_policies_mlob as qpm
where highest_policy_status = 4
  and lob_policy = 'GL'
  and state = 'SC'
  and carrier_name = 'NEXT_INSURANCE'
order by policy_end_date

select business_id,
       offer_id,
       highest_status_name,
       lob_policy,
       cob,
       cob_group,
       state,
       highest_status_package,
       highest_yearly_premium,
       distribution_channel,
       carrier_name,
       policy_start_date,
       policy_end_date,
       new_reneweal
from dwh.quotes_policies_mlob as qpm
where highest_policy_status >= 4
  and lob_policy = 'GL'
  and policy_start_date >= '2022-07-21'
  and new_reneweal = 'new'
order by policy_start_date


select business_id,
       highest_policy_reference,
       lob_policy,
       policy_start_date,
       policy_end_date,
       new_reneweal,
       qpm.*
from dwh.quotes_policies_mlob as qpm
where highest_policy_status = 4
  and lob_policy = 'CA'
limit 10


select *
from dwh.all_activities_agents
--join w/ dwh.all_activities_table on activity_id or policy_id
limit 10

select *
from dwh.quotes_policies_mlob
where highest_policy_status = 6
  and lob_policy = 'GL'
limit 10

select *
from dwh.quotes_policies_mlob
where business_id = '40956263d5cfe2c38856733ff1bca97a'

select business_id,
       lob_policy,
       cob,
       policy_start_date,
       policy_end_date,
       new_reneweal,
       json_args,
       highest_status_package,
       state,
       highest_yearly_premium,
       highest_status_name
from dwh.quotes_policies_mlob as qpm
where highest_policy_status = 4
  and lob_policy = 'GL'
  and cob = 'Restaurant'

select business_id,
       lob_policy,
       cob,
       policy_start_date,
       policy_end_date,
       new_reneweal,
       json_args,
       highest_status_package,
       state,
       highest_yearly_premium,
       highest_status_name
from dwh.quotes_policies_mlob as qpm
where highest_policy_status = 4
  and lob_policy = 'PL'
  and state = 'CT'

select business_id,
       lob_policy,
       cob,
       policy_start_date,
       policy_end_date,
       new_reneweal,
       json_args,
       highest_status_package,
       state,
       highest_yearly_premium,
       highest_status_name
from dwh.quotes_policies_mlob as qpm
where highest_policy_status = 4
  and lob_policy = 'CA'
  and state = 'AZ'
  and CARRIER_NAME = 'NEXT_CARRIER'

select snap_agencytype, current_agencytype, count(distinct (policy_id))
from db_data_science.v_all_agents_policies
where eventtime >= '2023-04-01'
group by 1, 2

select business_id,
       lob_policy,
       cob,
       policy_start_date,
       policy_end_date,
       new_reneweal,
       json_args,
       highest_status_package,
       state,
       highest_yearly_premium,
       highest_status_name
from dwh.quotes_policies_mlob as qpm
where highest_policy_status = 4
  and lob_policy = 'WC'
  and cob = 'Restaurant'
  and offer_flow_type in (‘APPLICATION’, ‘RENEWAL’, ‘CANCEL_REWRITE’)

select *
from reporting.alir2_bob

select highest_policy_reference
from dwh.quotes_policies_mlob as qpm
where highest_policy_status = 4
  and lob_policy = 'CA'
  and highest_policy_reference != ''
  and state in
      ('AL', 'AR', 'AZ', 'CO', 'CT', 'IL', 'KS', 'KY', 'MO', 'MS', 'MT', 'NE', 'NV', 'OK', 'SC', 'TN', 'TX', 'UT', 'WV',
       'WY')
  and CARRIER_NAME = 'NEXT_CARRIER'

select count(highest_policy_reference)
from dwh.quotes_policies_mlob as qpm
where highest_policy_status = 4
  and lob_policy = 'CA'
  and highest_policy_reference != ''
  and state in
      ('AL', 'AR', 'AZ', 'CO', 'CT', 'IL', 'KS', 'KY', 'MO', 'MS', 'MT', 'NE', 'NV', 'OK', 'SC', 'TN', 'TX', 'UT', 'WV',
       'WY')
  and CARRIER_NAME = 'NEXT_CARRIER'

select count(distinct policy_number)
from reporting.alir2_bob

select state, CARRIER_NAME, sum(highest_yearly_premium) as premium_total
from dwh.quotes_policies_mlob as qpm
where lob_policy = 'GL'
  and highest_policy_status = 4
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
group by state, CARRIER_NAME
order by state, CARRIER_NAME

select *
from dwh.quotes_policies_mlob as qpm
where highest_policy_status >= 4
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
limit 10

select count(highest_policy_reference)
from dwh.quotes_policies_mlob as qpm
where highest_policy_status = 4
  and lob_policy = 'CA'
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')

select business_id, policy_end_date, highest_status_name
from dwh.quotes_policies_mlob as qpm
where lob_policy = 'GL'
  and distribution_channel = 'partnerships'
  and highest_policy_status >= 4
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')

select current_amendment,
       sum(highest_yearly_premium) as premium_total,
       month(policy_start_date)    as policy_start_month
from dwh.quotes_policies_mlob
where highest_policy_status >= 4
  and lob_policy = 'GL'
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
group by policy_start_month, current_amendment

select current_amendment, sum(highest_yearly_premium), extract(month from policy_start_date) as policy_start_month
from dwh.quotes_policies_mlob
where highest_policy_status >= 4
  and lob_policy = 'GL'
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and policy_start_date >= '2022-01-01'
  and policy_start_date <= '2022-12-31'
group by policy_start_month, current_amendment
order by policy_start_month

select count(data_domain)
from dwh.all_activities_table
where data_domain = 'App User Interactions'
  and funnelphase = 'Answered Question Sequence'
  and question_name not like '%same_mailing_and_business_address%'

select cob_name,
       extract(month from eventtime)                       as quote_month,
       question_answer,
       count(case when question_answer = 'Yes' then 1 end) as same_mailing,
       count(case when question_answer = 'No' then 1 end)  as different_mailing
from dwh.all_activities_table
where question_name like '%same_mailing_and_business_address%'
  and funnelphase = 'Answered Question Sequence'
  and eventtime >= '2022-06-01'
  and cob_name in
      ('Restaurant', 'Retail Stores', 'Caterer', 'General Contractor', 'Carpentry', 'Handyperson', 'Painting',
       'Personal Trainer', 'IT Consulting or Programming', 'Food Truck', 'Grocery Store', 'Property Manager',
       'Other Consulting', 'Insurance Agent', 'Fitness Instructor', 'E-Commerce', 'Business Consulting')
  and question_answer != 'null'
group by question_answer, cob_name, quote_month
order by cob_name, quote_month, question_answer

--split-mailing frequency for restaurants
select cob_name,
       extract(month from eventtime)                       as quote_month,
       question_answer,
       count(case when question_answer = 'Yes' then 1 end) as same_mailing,
       count(case when question_answer = 'No' then 1 end)  as different_mailing
from dwh.all_activities_table
where question_name like '%same_mailing_and_business_address%'
  and funnelphase = 'Answered Question Sequence'
  and eventtime >= '2022-01-01'
  and cob_name in ('Restaurant')
  and question_answer != 'null'
group by question_answer, cob_name, quote_month
order by cob_name, quote_month, question_answer

select business_id,
       creation_time,
       policy_start_date,
       distribution_channel,
       lob_policy,
       cob,
       state,
       highest_yearly_premium,
       highest_status_name,
       highest_status_package,
       current_amendment
from dwh.quotes_policies_mlob
where highest_policy_status >= 4
  and highest_yearly_premium >= 5000
  and cob in ('Restaurant', 'Caterers', 'Food Truck', 'Coffee Shop', 'Grocery Store')
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and creation_time >= (getdate() - 7)
order by highest_yearly_premium desc


-- query to identify amazon bug
with qpm as (select case
                        when highest_status_package = 'basic'
                            then basic_quote_job_id -- these rows are necessary to create the join on job_id
                        when highest_status_package = 'basicTria' then basic_tria_quote_job_id
                        when highest_status_package = 'pro' then pro_quote_job_id
                        when highest_status_package = 'proTria' then pro_tria_quote_job_id
                        when highest_status_package = 'proPlus' then pro_plus_quote_job_id
                        when highest_status_package = 'proPlusTria' then pro_plus_tria_quote_job_id
                        else pro_quote_job_id
                        end as quote_job_id,
                    business_id,
                    creation_time,
                    policy_start_date,
                    json_args,
                    highest_status_package,
                    current_amendment,
                    cob
             from dwh.quotes_policies_mlob
             where highest_policy_status >= 4
               and lob_policy = 'GL'
               and cob = 'E-Commerce'
               and distribution_channel = 'partnerships'
               and json_args like '%4700%'
               and creation_time >= '2022-06-01'),
     rates as (select *
               from s3_operational.rating_svc_prod_calculations
               where lob = 'GL'
                 and data_points like '%E_COMMERCE%' -- this is only to narrow data / amazon sellers affiliate ID can be any COB, though default and 99% of policies are e-commerce
                 and creation_time >= '2022-06-01')
select qpm.business_id,
       qpm.creation_time,
       qpm.policy_start_date,
       qpm.json_args,
       qpm.current_amendment,
       qpm.cob,
       rates.calculation,
       rates.rating_result
from qpm
         left join rates
                   on qpm.quote_job_id = rates.job_id
group by 1, 2, 3, 4, 5, 6, 7, 8
limit 10000

-- query to pull epli endt premium
with qpm as (select case
                        when highest_status_package = 'basic'
                            then basic_quote_job_id -- these rows are necessary to create the join on job_id
                        when highest_status_package = 'basicTria' then basic_tria_quote_job_id
                        when highest_status_package = 'pro' then pro_quote_job_id
                        when highest_status_package = 'proTria' then pro_tria_quote_job_id
                        when highest_status_package = 'proPlus' then pro_plus_quote_job_id
                        when highest_status_package = 'proPlusTria' then pro_plus_tria_quote_job_id
                        else pro_quote_job_id
                        end as quote_job_id,
                    business_id,
                    creation_time,
                    policy_start_date,
                    json_args,
                    highest_status_package,
                    current_amendment,
                    cob
             from dwh.quotes_policies_mlob
             where highest_policy_status >= 4
               and lob_policy = 'GL'
               and cob = 'Restaurant'
               and creation_time >= '2022-06-01'),
     rates as (select *
               from s3_operational.rating_svc_prod_calculations
               where lob = 'GL'
                 and data_points like '%RESTAURANT%'
                 and creation_time >= '2022-06-01')
select qpm.business_id,
       qpm.creation_time,
       qpm.policy_start_date,
       qpm.json_args,
       qpm.current_amendment,
       qpm.cob,
       rates.calculation,
       rates.rating_result,
       json_extract_path_text(rates.calculation, 'stand alone endorsements', true) as standalone_endts
from qpm
         left join rates
                   on qpm.quote_job_id = rates.job_id
where standalone_endts like '%EPLI_COVERAGE%'
group by 1, 2, 3, 4, 5, 6, 7, 8, 9
limit 10000

--to pull other optional endt premium
with qpm as (select case
                        when highest_status_package = 'basic'
                            then basic_quote_job_id -- these rows are necessary to create the join on job_id
                        when highest_status_package = 'basicTria' then basic_tria_quote_job_id
                        when highest_status_package = 'pro' then pro_quote_job_id
                        when highest_status_package = 'proTria' then pro_tria_quote_job_id
                        when highest_status_package = 'proPlus' then pro_plus_quote_job_id
                        when highest_status_package = 'proPlusTria' then pro_plus_tria_quote_job_id
                        else pro_quote_job_id
                        end as quote_job_id,
                    business_id,
                    creation_time,
                    policy_start_date,
                    json_args,
                    highest_status_package,
                    current_amendment,
                    cob,
                    cob_group
             from dwh.quotes_policies_mlob
             where highest_policy_status >= 4
               and lob_policy = 'GL'
               and cob_group in ('Food & beverage', 'Retail')
               and creation_time >= '2022-06-01'),
     rates as (select *
               from s3_operational.rating_svc_prod_calculations
               where lob = 'GL'
                 and creation_time >= '2022-06-01')

select qpm.business_id,
       qpm.creation_time,
       qpm.policy_start_date,
       qpm.json_args,
       qpm.current_amendment,
       qpm.cob,
       qpm.cob_group,
       rates.calculation,
       rates.rating_result
--       json_extract_path_text(rates.calculation,'stand alone endorsements', true) as standalone_endts
from qpm
         left join rates
                   on qpm.quote_job_id = rates.job_id
--where standalone_endts like '%EPLI_COVERAGE%'
where rates.rating_result like '%expanded_damage_rented_premises_coverage%'
   or rates.rating_result like '%product_withdrawal_expense_coverage%'
group by 1, 2, 3, 4, 5, 6, 7, 8, 9
limit 10000


--to pull liquor liability premium
with qpm as (select case
                        when highest_status_package = 'basic'
                            then basic_quote_job_id -- these rows are necessary to create the join on job_id
                        when highest_status_package = 'basicTria' then basic_tria_quote_job_id
                        when highest_status_package = 'pro' then pro_quote_job_id
                        when highest_status_package = 'proTria' then pro_tria_quote_job_id
                        when highest_status_package = 'proPlus' then pro_plus_quote_job_id
                        when highest_status_package = 'proPlusTria' then pro_plus_tria_quote_job_id
                        else pro_quote_job_id
                        end as quote_job_id,
                    business_id,
                    creation_time,
                    policy_start_date,
                    json_args,
                    highest_status_package,
                    current_amendment,
                    cob,
                    cob_group
             from dwh.quotes_policies_mlob
             where highest_policy_status >= 4
               and lob_policy = 'GL'
               and cob = 'Restaurant'
               and creation_time >= '2021-06-01'),
     rates as (select *
               from s3_operational.rating_svc_prod_calculations
               where lob = 'GL'
                 and creation_time >= '2021-06-01')

select qpm.business_id,
       qpm.creation_time,
       qpm.policy_start_date,
       qpm.json_args,
       qpm.current_amendment,
       qpm.cob,
       qpm.cob_group,
       json_extract_path_text(qpm.json_args, 'lob_app_json', 'liquor_sales_yes_no', true) as liquor_yes_no,
       rates.calculation,
       rates.rating_result
--       json_extract_path_text(rates.calculation,'stand alone endorsements', true) as standalone_endts
from qpm
         left join rates
                   on qpm.quote_job_id = rates.job_id
--where standalone_endts like '%EPLI_COVERAGE%'
where liquor_yes_no = 'Yes'
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
limit 10000

--test to figure out how to isolate liquor policies
select json_args,
       cob,
       policy_start_date,
       json_extract_path_text(json_args, 'offers_json', 'carrier', true)              as carrier,
       json_extract_path_text(json_args, 'lob_app_json', 'liquor_sales_yes_no', true) as liquor_yes_no
from dwh.quotes_policies_mlob
where cob = 'Restaurant'
  and lob_policy = 'GL'
  and liquor_yes_no = 'Yes'
limit 10

--to pull liquor liability separate coverage part premium
-- query to pull epli endt premium, not needed; included above in other epli pull
with qpm as (select case
                        when highest_status_package = 'basic'
                            then basic_quote_job_id -- these rows are necessary to create the join on job_id
                        when highest_status_package = 'basicTria' then basic_tria_quote_job_id
                        when highest_status_package = 'pro' then pro_quote_job_id
                        when highest_status_package = 'proTria' then pro_tria_quote_job_id
                        when highest_status_package = 'proPlus' then pro_plus_quote_job_id
                        when highest_status_package = 'proPlusTria' then pro_plus_tria_quote_job_id
                        else pro_quote_job_id
                        end as quote_job_id,
                    business_id,
                    creation_time,
                    policy_start_date,
                    json_args,
                    highest_status_package,
                    current_amendment,
                    cob
             from dwh.quotes_policies_mlob
             where highest_policy_status >= 4
               and lob_policy = 'GL'
               and cob = 'Restaurant'
               and creation_time >= '2022-06-01'),
     rates as (select *
               from s3_operational.rating_svc_prod_calculations
               where lob = 'GL'
                 and data_points like '%RESTAURANT%'
                 and creation_time >= '2022-06-01')
select qpm.business_id,
       qpm.creation_time,
       qpm.policy_start_date,
       qpm.json_args,
       qpm.current_amendment,
       qpm.cob,
       rates.calculation,
       rates.rating_result,
       json_extract_path_text(rates.calculation, 'stand alone endorsements', true) as standalone_endts
from qpm
         left join rates
                   on qpm.quote_job_id = rates.job_id
where standalone_endts like '%LIQUOR_LIABILITY_COVERAGE%'
group by 1, 2, 3, 4, 5, 6, 7, 8, 9
limit 10

-- all restaurant 2022 YTD premium
with qpm as (select case
                        when highest_status_package = 'basic'
                            then basic_quote_job_id -- these rows are necessary to create the join on job_id with rates
                        when highest_status_package = 'basicTria' then basic_tria_quote_job_id
                        when highest_status_package = 'pro' then pro_quote_job_id
                        when highest_status_package = 'proTria' then pro_tria_quote_job_id
                        when highest_status_package = 'proPlus' then pro_plus_quote_job_id
                        when highest_status_package = 'proPlusTria' then pro_plus_tria_quote_job_id
                        else pro_quote_job_id
                        end as quote_job_id,
                    business_id,
                    creation_time,
                    policy_start_date,
                    json_args,
                    highest_status_package,
                    highest_policy_status,
                    lob_policy,
                    current_amendment,
                    cob
             from dwh.quotes_policies_mlob
             where highest_policy_status >= 4
               and lob_policy = 'GL'
               and cob = 'Restaurant'
               and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
               and creation_time >= '2022-01-01'),
     rates as (select *
               from s3_operational.rating_svc_prod_calculations
               where lob = 'GL'
                 and data_points like '%RESTAURANT%'
                 and creation_time >= '2022-01-01')
select qpm.business_id,
       qpm.creation_time,
       qpm.policy_start_date,
       qpm.json_args,
       qpm.current_amendment,
       qpm.cob,
       rates.calculation,
       rates.rating_result
from qpm
         left join rates
                   on qpm.quote_job_id = rates.job_id
where qpm.highest_policy_status >= 4
  and qpm.lob_policy = 'GL'
  and qpm.cob = 'Restaurant'
  and qpm.creation_time >= '2022-01-01'
group by 1, 2, 3, 4, 5, 6, 7, 8
limit 10000

--to get raven score over time
with tpm as (select business_id,
                    predicted_loss_ratio
             from riskmgmt_svc_prod.adverse_risk_result
             where lob = 'GL'
               and creation_time >= '2022-09-01'),
     raven_score_construction as (select distinct business_id,
                                                  creation_time::date,
                                                  first_quote_time::date,
                                                  current_quote_time::date,
                                                  schedule_rating_factor::float,
                                                  bin_num::float,
                                                  score::float
                                  from prod.risk_model_svc_prod.risk_score_requests
                                  where creation_time >= '2022-09-01'),
     raven_score_non_construction
         as (select distinct json_extract_path_text(event_json, 'business_id', true)                          as business_id,
                             creation_time::date,
                             substring(json_extract_path_text(event_json, 'first_quote_time', true), 1,
                                       10)                                                                    as first_quote_time,
                             substring(JSON_EXTRACT_PATH_text(event_json, 'current_quote_time', true), 1,
                                       10)                                                                    as current_quote_time,
                             JSON_EXTRACT_PATH_text(response_json, 'factor', true)::float                     as schedule_rating_factor,
                             JSON_EXTRACT_PATH_text(response_json, 'bin_num', true)::float                    as bin_num,
                             JSON_EXTRACT_PATH_text(response_json, 'score', true)::float                      as score
             from prod.risk_model_svc_prod.gl_non_construction_schedule_rate_requests
             where creation_time >= '2022-09-01'),
     agent_quotes as (select distinct json_extract_path_text(interaction_data, 'offer_id', true) as offer_id, *
                      from dwh.all_activities_agents
                      where funnelphase = 'Quote'
                        and lob = 'GL')
select tpm.predicted_loss_ratio                                                                             as tpm_score,
       qpm.*,
       (CASE
            WHEN highest_status_package = 'proPlusTria' THEN pro_plus_tria_premium_before_minimum
            WHEN highest_status_package = 'proPlus' THEN pro_plus_premium_before_minimum
            WHEN highest_status_package = 'proTria' THEN pro_tria_premium_before_minimum
            WHEN highest_status_package = 'pro' THEN pro_premium_before_minimum
            WHEN highest_status_package = 'basic' THEN basic_premium_before_minimum
            ELSE basic_tria_premium_before_minimum END)                                                     as premium_before_minimum,
       case when qpm.creation_time::date < '2022-10-25' then 'pre' else 'post' end                          AS pre_post,
       nullif(json_extract_path_text(rating_result, 'debugInfo', 'niFactor', TRUE), '')                     AS niFactor,

       coalesce(rc.score, rn.score)                                                                         as raven_score,
       coalesce(rc.bin_num, rn.bin_num)                                                                     as raven_bin,
       case when (a.offer_id is not null) then 1 else 0 end                                                 as agent_channel,
       nullif(json_extract_path_text(calculation_summary, 'lob specific', 'exposure base type', true),
              '')                                                                                           as exposure_type,
       nullif(json_extract_path_text(calculation_summary, 'lob specific', 'exposure base value', true),
              '')                                                                                           as exposure_base_value
from dwh.quotes_policies_mlob qpm
         left join tpm on qpm.business_id = tpm.business_id
         left join raven_score_construction rc
                   on qpm.business_id = rc.business_id and qpm.creation_time::date = rc.creation_time::date
         left join raven_score_non_construction rn
                   on qpm.business_id = rn.business_id and qpm.creation_time::date = rn.creation_time::date
         left join agent_quotes a on a.offer_id = qpm.offer_id
         LEFT JOIN s3_operational.rating_svc_prod_calculations r ON
    job_id = (CASE
                  WHEN highest_status_package = 'basic' THEN qpm.basic_quote_job_id
                  WHEN highest_status_package = 'basicTria' THEN qpm.basic_tria_quote_job_id
                  WHEN highest_status_package = 'pro' THEN qpm.pro_quote_job_id
                  WHEN highest_status_package = 'proTria' THEN qpm.pro_tria_quote_job_id
                  WHEN highest_status_package = 'proPlus' THEN qpm.pro_plus_quote_job_id
                  WHEN highest_status_package = 'proPlusTria' THEN qpm.pro_plus_tria_quote_job_id
        END)
where lob_policy = 'GL'
  and offer_flow_type = 'APPLICATION'
  and highest_policy_status >= 4
  and qpm.creation_time >= '2022-09-01'

select *
from prod.risk_model_svc_prod.gl_non_construction_schedule_rate_requests
limit 10

--restaurant riskiness query
with qpm as (select business_id,
                    highest_policy_status,
                    highest_yearly_premium
             from dwh.quotes_policies_mlob
             where lob_policy = 'GL'
               and creation_time >= '2022-01-01'
               and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
               and cob = 'Restaurant'
               and highest_policy_status >= 4),
     riskybiz as (select distinct json_extract_path_text(event_json, 'business_id', true)       as business_id,
                                  json_extract_path_text(event_json, 'cob_name', true)          as cob,
                                  json_extract_path_text(event_json, 'num_of_employees', true)  as employees,
                                  json_extract_path_text(event_json, 'yoe', true)               as years_experience,
                                  creation_time::date,
                                  substring(json_extract_path_text(event_json, 'first_quote_time', true), 1,
                                            10)                                                 as first_quote_time,
                                  --substring(json_extract_path_text(event_json, 'current_quote_time', true), 1,
                                  --10)                                                                    as current_quote_time,
                                  json_extract_path_text(response_json, 'factor', true)::float  as schedule_rating_factor,
                                  json_extract_path_text(response_json, 'bin_num', true)::float as bin_num,
                                  json_extract_path_text(response_json, 'score', true)::float   as score
                  from prod.risk_model_svc_prod.gl_non_construction_schedule_rate_requests
                  where creation_time >= '2022-01-01'
                    and cob = 'Restaurant')
select riskybiz.*,
       qpm.*
from riskybiz
         inner join qpm on qpm.business_id = riskybiz.business_id
limit 10000

--aggregate up F&B GL by channel and package
select
    --business_id,
    --creation_time,
    --policy_start_date,
    --month(creation_time) as creation_month,
    extract(week from creation_time) as creation_week,
    distribution_channel,
    --lob_policy,
    --cob,
    --state,
    --count(highest_yearly_premium) as business_count,
    --highest_status_name,
    highest_status_package,
    count(highest_status_package)    as package_count
--current_amendment
from dwh.quotes_policies_mlob
where highest_policy_status >= 4
  and cob in ('Restaurant', 'Caterers', 'Food Truck', 'Coffee Shop', 'Grocery Store')
  and lob_policy = 'GL'
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and creation_time >= '2022-08-01'
group by 1, 2, 3
order by creation_week asc

--to get limits distribution for 2/4M policies following 12/20 release
select count(highest_policy_occurence_limit) as policy_count,
       highest_policy_occurence_limit,
       highest_policy_aggregate_limit
from dwh.quotes_policies_mlob
where highest_policy_status >= 4
  and distribution_channel = 'agents'
  and lob_policy = 'GL'
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and creation_time >= '2022-12-20'
  and state in ('AL', 'AZ', 'CO', 'FL', 'MI', 'MN', 'NV', 'SC', 'TN', 'TX', 'UT')
  and cob in
      ('Restaurant', 'E-Commerce', 'Retail Stores', 'Grocery Store', 'Clothing Store', 'Electronics Store', 'Florist',
       'Jewelry Store', 'Sporting Goods Retailer', 'Tailors, Dressmakers, and Custom Sewers',
       'Nurseries and Gardening Shop', 'Candle Store', 'Pet Stores', 'Paint Stores', 'Flea Markets',
       'Arts and Crafts Store', 'Eyewear and Optician Store', 'Hardware Store', 'Discount Store', 'Pawn Shop',
       'Hobby Shop', 'Beach Equipment Rentals', 'Furniture Rental', 'Packing Supplies Store', 'Horse Equipment Shop',
       'Demonstrators and Product Promoters', 'Fabric Store', 'Lighting Store', 'Luggage Store', 'Bike Rentals',
       'Bike Shop', 'Bookstore', 'Home and Garden Retailer', 'Newspaper and Magazine Store', 'Department Stores',
       'Furniture Store', 'Wholesalers')
group by highest_policy_occurence_limit, highest_policy_aggregate_limit

--to get 2/4M policy list
select business_id,
       policy_start_date,
       highest_status_name,
       cob,
       distribution_channel,
       highest_policy_occurence_limit,
       highest_policy_aggregate_limit,
       highest_yearly_premium
from dwh.quotes_policies_mlob
where highest_policy_status >= 3
  and distribution_channel = 'agents'
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and creation_time >= '2022-12-20'
  and
  --policy_start_date >= '2023-01-01' and policy_start_date < '2023-04-01' and
    (highest_policy_aggregate_limit > 2000000 or highest_policy_occurence_limit > 1000000)
  and state in ('AL', 'AZ', 'CO', 'FL', 'MI', 'MN', 'NV', 'SC', 'TN', 'TX', 'UT')
  and (cob = 'Restaurant' or cob_group = 'Retail')
--cob = 'Restaurant'

-- avg premium by COB
select cob,
       (sum(highest_yearly_premium) / count(highest_yearly_premium)) as avg_premium,
       sum(highest_yearly_premium)                                   as total_premium
from dwh.quotes_policies_mlob
where highest_policy_status >= 4
  and lob_policy = 'GL'
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and creation_time >= '2022-10-01'
  and
group by cob

--get list of amazon.com DBAs
select business_id,
       json_extract_path_text(json_args, 'business_name', true) as biz_name,
       lob_policy,
       cob,
       distribution_channel,
       highest_policy_status,
       policy_end_date
from dwh.quotes_policies_mlob
where biz_name LIKE '%Amazon%'
  and
  --offer_flow_type in ('APPLICATION','RENEWAL','CANCEL_REWRITE') and
    highest_policy_status in ('4', '7')

--get business_id by policy number
select business_id,
       highest_policy_id,
       lob_policy,
       cob,
       distribution_channel
from dwh.quotes_policies_mlob
where highest_policy_id in
      ('6257806', '6253636', '6274335', '6247196', '6234854', '6231963', '6264936', '6268409', '6244432', '6266930',
       '6272848', '6294228', '6269652', '6248177', '6258898', '6195780', '6215367', '6302397')


--get WC business_name by policy number
select business_id,
       json_args,
       json_extract_path_text(json_args, 'business_name', true) as biz_name,
       highest_policy_id,
       lob_policy,
       cob,
       distribution_channel
from dwh.quotes_policies_mlob
where highest_policy_id in
      ('6251172', '6248009', '6274855', '6290112', '6231729', '6168511', '6197659', '6171456', '6172576', '6288686',
       '6261918', '6274246', '6254317', '6234830', '6257482', '6257711', '6275905', '6237344', '6279295', '6226219',
       '6236011', '6245490', '6284905', '6250477', '6187630', '6271371', '6254366', '6282465', '6246485', '6278929',
       '6237433', '6223185', '6231699', '6249646', '6257314', '6284010', '6257748', '6237514', '6230319', '6282270',
       '6212606', '6175456', '6209465', '6309440'
          )

--customSQL query behind high risk keyword declines (from https://tableau.next-insurance.com/#/views/KeyWordDeclines/Details?:iid=2)
with KWdecline as (select business_id,
                          offer_id,
                          offer_creation_time,
                          date_trunc('second', offer_creation_time) as offerCrt_Time,
                          lob,
                          lob_application_id,
                          state_code,
                          cob,
                          marketing_cob_group                       as cob_group
                   from dwh.underwriting_quotes_data uqd
                            left join dwh.sources_test_cobs stc on uqd.cob = stc.cob_name
                   where offer_creation_time::date >>= '2022-05-12'
                     and execution_status = 'DECLINE'
                     and offer_flow_type = 'APPLICATION'
                     and decline_reasons ilike
                         '%We are unable to provide the right coverage and support for your business at this time. Your business is associated with services for which we cannot provide insurance.%'),
     SUCCESS as (select business_id,
                        offer_id,
                        offer_creation_time,
                        lob,
                        policy_status,
                        policy_status_name,
                        max(policy_status) over (partition by offer_id) as HighestStatus
                 from dwh.underwriting_quotes_data
                 where offer_creation_time::date >>= '2022-05-12'
                   and execution_status = 'SUCCESS'
                   and offer_flow_type = 'APPLICATION'),
     Bound as (select distinct business_id, offer_id, offer_creation_time, lob, policy_status_name
               from SUCCESS
               where HighestStatus >>= 3
                 and policy_status = highestStatus),
     SuccessQuote as (select distinct business_id, offer_id, offer_creation_time, lob, policy_status_name
                      from SUCCESS
                      where HighestStatus << 3
                        and policy_status = highestStatus),
     Result as
         (select id,
                 business_id,
                 lob,
                 cob,
                 business_name,
                 email_address,
                 date_trunc('second', creation_time) as creationtime,
                 is_decline_keyword_in_business_name as biznamedecline,
                 is_decline_keyword_in_email         as emaildeccline,
                 decline_keyword
          from riskmgmt_svc_prod.high_risk_keywords_screening_results),
     Merge as (select *
               from (select KW.*,
                            R.ID,
                            R.business_name,
                            R.email_address,
                            R.biznamedecline,
                            R.emaildeccline,
                            R.decline_keyword,
                            creationtime,
                            max(KW.offer_creation_time)
                            over (PARTITION BY KW.business_id, KW.lob)                           as LastDeclineOfferCrtTime,
                            row_number() over (partition by offer_id order by creationtime desc) as closest
                     from KWdecline KW
                              left join Result R on KW.business_id = R.business_id and KW.lob = R.lob
                         and (R.creationtime <<= KW.offerCrt_Time and
                              date_trunc('hour', KW.offerCrt_Time) = date_trunc('hour', R.creationtime)))
               where closest = 1)
select distinct M.*,
                case when SQ.business_id is not null then 'Y' else 'N' end as SuccessquoteAfter,
                case when B.business_id is not null then 'Y' else 'N' end  as BoundAfter
from Merge M
         left join SuccessQuote SQ on M.business_id = SQ.business_id and M.lob = SQ.lob and
                                      SQ.offer_creation_time >> M.LastDeclineOfferCrtTime
         left join Bound B on M.business_id = B.business_id and M.lob = B.lob and
                              B.offer_creation_time >> M.LastDeclineOfferCrtTime

--find high risk declines associated with a list of business IDs
select business_id, decline_keyword, is_decline_keyword_in_business_name, is_decline_keyword_in_email
from riskmgmt_svc_prod.high_risk_keywords_screening_results
where business_id in
      ('6c2f9204e973e7ec0cbe68024cd7f9af', 'f6803cc171ae0d03b12699d181f78b4d', 'f1c78ee3738ba1b8eed1aa016b2673d4',
       '0f3a015d239321ac1517a0bffe904637', '039193a580d6355575868780bad2859e', '61b8691d7499425522d5021919403a9b',
       'ae0aa344147443215abc8baab8e8af44', '5a9e2c7067cacaf2334b08bbbbe78767', 'a769cafe1ead0eabfb6439dbfaf5a7b8',
       '5e96a4ff0be4d11020f0e234b31f4903', 'a127c53c318fd5f0c21126d5f60b9f59', '9328f57fb87fbf21ed7b520836c12eb1',
       'b2aef012496f0839b38e127aa0746b29', '2a77989d015ec90cdd8df56b66b4b1a8', '6469d0adb03ab7644180a3e655d65123',
       '4b40fb3ad885d8d26f67cc4bfca8029f', 'c51ff7b362f8e8b4597c99a534ed47a5', '4cd28a50af0aba738d1d33b110759a2c'
          )

--find high risk declines associated with a list of WC business IDs
select business_id, decline_keyword, is_decline_keyword_in_business_name, is_decline_keyword_in_email
from riskmgmt_svc_prod.high_risk_keywords_screening_results
where lob = 'WC'
  and business_id in
      ('95a1215a89aef57afa8df00aa89dd6eb', '48534cf28480983eff1a0702bf0481b6', 'f3351b7eb8eac768988d3e67b5aaf427',
       'ec01e8be63463fe5c3486137d17e186b', '8496dcf48391aa1f31aa6224e48be395', '9cce4e43a0a79c6110829e2bc3042ee7',
       '35d520429411422083b1583d1a640833', '6c46608ec34556483f253d90530d4e3b', '1e1934aa8d2c3c477a274924a279537e',
       '37e36f32b0a0522ce14fd4ed405528d8', '37e36f32b0a0522ce14fd4ed405528d8', 'be25bdab48e9668d4579a7b47f517a84',
       'd31d9da6af9e6d4c29f6d7c7c5111edc', '443f7672bc5cf1e6d6f18db0c7edd923', '43d4111ad446739a48f6daea88a4c49d',
       '9cce4e43a0a79c6110829e2bc3042ee7', '8878df4844b7a05fdcc116e78c2db8fc', 'b93950bcc5ff48a66d41b398cee7f302',
       '8382da4da9aa9fb3ead2d68044caadeb', 'ec01e8be63463fe5c3486137d17e186b', 'afde76a871dc162828c7e5cbf3983956',
       '8878df4844b7a05fdcc116e78c2db8fc', 'ec01e8be63463fe5c3486137d17e186b', '37e36f32b0a0522ce14fd4ed405528d8',
       '37e36f32b0a0522ce14fd4ed405528d8', '8878df4844b7a05fdcc116e78c2db8fc', '13edd155526870b2b00c4afc63b27e67',
       'edccd78bb7cf8f4274a65c4dc3a31b98', 'd31d9da6af9e6d4c29f6d7c7c5111edc', '0dfa81b457409d5d39564755b827459e',
       '616009d25f866db3f9d671e8708c5926', '2ed7c89b3878023389cf53c69f1c2001', 'ccb9cd34d4ad76469473849285f72010',
       '911ee104a5f9d9de970441cbba3358a7', '8496dcf48391aa1f31aa6224e48be395', 'd9fef9c43640a86774e79eac9d7a906b',
       'd4c5b59f958c470ba1bbcf700b89fa92', 'ee6af984dd0a3fb2093926216c00297d', '9cce4e43a0a79c6110829e2bc3042ee7',
       'f239af58ba1754583c7addd58467da59', 'd9fef9c43640a86774e79eac9d7a906b', '6c4144d4a7af8abd360d2c944bc943d0',
       '1f1946f6c7109db22d20f6a6ebd0bef8', 'acec7d75f1d536825e960ed73ef9fd4e', '5deb3779535a055fb0d93af61d9e0389',
       '443f7672bc5cf1e6d6f18db0c7edd923', 'f239af58ba1754583c7addd58467da59', 'b93950bcc5ff48a66d41b398cee7f302',
       '37e36f32b0a0522ce14fd4ed405528d8', '37e36f32b0a0522ce14fd4ed405528d8', 'f239af58ba1754583c7addd58467da59',
       'f3351b7eb8eac768988d3e67b5aaf427', '95a1215a89aef57afa8df00aa89dd6eb', '5cf4d15372bbe1d2ebe12cfb85142faa',
       'f3351b7eb8eac768988d3e67b5aaf427', 'f3351b7eb8eac768988d3e67b5aaf427', 'd4c5b59f958c470ba1bbcf700b89fa92',
       '620af53abef57914fecb8832077dc438', '37e36f32b0a0522ce14fd4ed405528d8', 'ccb9cd34d4ad76469473849285f72010',
       '1f1946f6c7109db22d20f6a6ebd0bef8', '5deb3779535a055fb0d93af61d9e0389', '35d520429411422083b1583d1a640833',
       '73d10b207fe7f381b3160ccbfef5931f', 'decabb1eea8f9d2637de4ccc502fddd4', '539bfbbb12ef9754b6c93b997ab369ac',
       'edccd78bb7cf8f4274a65c4dc3a31b98', '4338c758e613db6a4ca3e2b86ad83ad5', 'be25bdab48e9668d4579a7b47f517a84',
       '70140fad4e9eba2daec6543de7366bb9', 'd31d9da6af9e6d4c29f6d7c7c5111edc', 'a390e09bf0d044c0d8c129a1aea16814',
       'c1db0f8d3f49561ddd81f0c58c87dd61', 'ce569d86ce531ce372d27a96b490c2b1', '9a8bb3b066667090a4e1771fec0d2d33',
       'd9fef9c43640a86774e79eac9d7a906b', '4338c758e613db6a4ca3e2b86ad83ad5'
          )

select highest_status_name
from dwh.quotes_policies_mlob
where highest_policy_status = 1
limit 10

-- key for policy status numbers
select DISTINCT highest_policy_status, highest_status_name
from dwh.quotes_policies_mlob
ORDER BY highest_policy_status


-- query to identify amazon bug
with qpm as (select case
                        when highest_status_package = 'basic'
                            then basic_quote_job_id -- these rows are necessary to create the join on job_id
                        when highest_status_package = 'basicTria' then basic_tria_quote_job_id
                        when highest_status_package = 'pro' then pro_quote_job_id
                        when highest_status_package = 'proTria' then pro_tria_quote_job_id
                        when highest_status_package = 'proPlus' then pro_plus_quote_job_id
                        when highest_status_package = 'proPlusTria' then pro_plus_tria_quote_job_id
                        else pro_quote_job_id
                        end as quote_job_id,
                    business_id,
                    creation_time,
                    policy_start_date,
                    json_args,
                    highest_status_package,
                    current_amendment,
                    cob
             from dwh.quotes_policies_mlob
             where highest_policy_status >= 4
               and lob_policy = 'GL'
               and cob = 'E-Commerce'
               and distribution_channel = 'partnerships'
               and json_args like '%4700%'
               and creation_time >= '2023-01-01'),
     rates as (select *
               from s3_operational.rating_svc_prod_calculations
               where lob = 'GL'
                 and data_points like '%E_COMMERCE%' -- this is only to narrow data / amazon sellers affiliate ID can be any COB, though default and 99% of policies are e-commerce
                 and creation_time >= '2023-01-01')
select qpm.business_id,
       qpm.creation_time,
       qpm.policy_start_date,
       qpm.json_args,
       qpm.current_amendment,
       qpm.cob,
       rates.calculation,
       rates.rating_result
from qpm
         left join rates
                   on qpm.quote_job_id = rates.job_id
group by 1, 2, 3, 4, 5, 6, 7, 8
limit 10000

select *
from dwh.quotes_policies_mlob
where business_id = '58fe3df2c426f571c52e34d86fa0f543'

--to get limits distribution across GL book after certain date by cob group
select count(highest_policy_occurence_limit) as occ_limit_count,
       count(highest_policy_aggregate_limit) as agg_limit_count,
       highest_policy_occurence_limit,
       highest_policy_aggregate_limit,
       cob_group
from dwh.quotes_policies_mlob
where highest_policy_status >= 4
  and lob_policy = 'GL'
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and creation_time >= '2023-01-01'
group by highest_policy_occurence_limit, highest_policy_aggregate_limit, cob_group

--list of policies bound/active in closed COBs
select business_id, cob, state, cob_group
from dwh.quotes_policies_mlob
where highest_policy_status >= 4
  and lob_policy = 'GL'
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and creation_time >= '2023-01-01'
  and cob in
      ('Tree Services', 'Dance Entertainment', 'Opera and Ballet', 'Barre Classes', 'Boot Camps', 'Cardio Classes',
       'Cycling Classes', 'Qi Gong', 'Spin Instructor', 'Circus Entertainment', 'Kids Activities', 'Astrologers',
       'Clowns', 'Comedy Entertainment', 'Commissioned Artists', 'Impersonating', 'Magician', 'MC and Host Services',
       'Models', 'Muralist', 'Music Directors and Composers', 'Musicians and Singers', 'Mystics', 'Palm Reading',
       'Party Characters', 'Psychic Mediums', 'Psychics', 'Singing Telegram', 'Supernatural Readings', 'Trivia Hosts',
       'Ventriloquist and Puppet Entertainment', 'Jazz and Blues', 'Balloon Services', 'Caricaturing',
       'Recreation Workers', 'Balloon Twisting', 'Salon Owner', 'Acne Treatment', 'Aestheticians', 'Bridal Stylist',
       'Custom Airbrushing', 'Facial Treatments', 'Hair Removal', 'Henna Artists', 'Henna Tattooing',
       'Makeup Artistry Lessons', 'Makeup Artists, Theatrical and Performance', 'Manicurists and Pedicurists',
       'Shampooers', 'Skin Care', 'Skincare Specialists', 'Sugaring', 'Temporary Tattoo Artistry', 'Threading Services',
       'Waxing', 'Wedding and Event Makeup', 'Medical Spas', 'Float Spa', 'Hot Springs', 'Oxygen Bars', 'Reflexology',
       'Tui Na', 'Physical Therapist', 'Wedding Coordination', 'Wedding Planning', 'Balloon Decorations', 'Day Camps',
       'Nanny Services', 'Preschools', 'Literacy Teachers and Instructors', 'Anthropology and Archeology Teachers',
       'Arabic Lessons', 'Architecture Teachers', 'Area, Ethnic, and Cultural Studies Teachers',
       'Art, Drama, and Music Teachers', 'Atmospheric, Earth, Marine, and Space Sciences Teachers',
       'Audio Production Lessons', 'Bass Guitar Lessons', 'Biological Science Teachers', 'Business Teachers',
       'Cello Lessons', 'Cheese Tasting Classes', 'Chemistry Teachers', 'Chess Lessons', 'Communications Teachers',
       'Computer Science Teachers', 'Drawing Lessons', 'Drum Lessons', 'Economics Teachers', 'Education Teachers',
       'Elementary School Teachers', 'Engineering Teachers', 'English Language and Literature Teachers',
       'Environmental Science Teachers', 'ESL, English as a Second Language Lessons', 'Flute Lessons', 'French Lessons',
       'Geography Teachers', 'German Lessons', 'Graduate Teaching Assistants', 'Graphic Design Instruction',
       'Guitar Lessons', 'Health Specialties Teachers', 'High School Teachers', 'History Teachers',
       'Home Economics Teachers', 'Instrument Instructor', 'Italian Lessons', 'Japanese Lessons', 'Korean Lessons',
       'Law Teachers', 'Library Science Teachers', 'Mandarin Lessons', 'Mathematical Science Teachers',
       'Middle School Teachers', 'Music Theory Lessons', 'Nursing Instructors and Teachers', 'Painting Lessons',
       'Philosophy and Religion Teachers', 'Photography Lessons', 'Physics Teachers', 'Piano Lessons',
       'Political Science Teachers', 'Portuguese Lessons', 'Preschool and Kindergarten Teachers', 'Psychology Teachers',
       'Public Speaking Lessons', 'Saxophone Lessons', 'Self Enrichment Education Teachers', 'Sewing Lessons',
       'Sign Language Lessons', 'Singing Lessons', 'Social Work Teachers', 'Sociology Teachers', 'Spanish Lessons',
       'Substitute Teachers', 'Tasting Classes', 'Teacher Assistants', 'Test Prep Services', 'Violin Lessons',
       'Voice Over Lessons', 'Swimming Pool Work', 'Merchandise Displayers and Window Trimmers', 'Home Staging',
       'Feng Shui', 'Holiday Decorating Services', 'Civil Engineer', 'Electrical Engineer', 'Environmental Engineer',
       'Industrial Engineer', 'Process Engineer', 'Transportation Engineer', 'Electronics Engineers, Except Computer',
       'Materials Engineers', 'Mechanical Engineers', 'Engineering and Technical Design',
       'Health and Safety Engineers, Except Mining Safety Engineers and Inspectors', 'Stock Broker',
       'Substance Abuse Counselor', 'Food Tours', 'Child, Family, and School Social Workers', 'Venues and Event Spaces',
       'Medical Billing Agency', 'Tour Guides and Escorts', 'Historical Tours', 'Art Tours', 'Bike tours', 'Bus Tours',
       'Scooter Tours', 'Walking Tours', 'Visitor Centers', 'Court Reporters', 'Legal Secretaries', 'Bailiffs',
       'Court, Municipal, and License Clerks', 'Court Interpreting', 'Legal Document Preparation',
       'Criminal Defense Attorney', 'Disability Attorney', 'Divorce and Family Attorney', 'DUI Attorney',
       'Estate Attorney', 'Intellectual Property Attorney', 'International Law Attorney',
       'Labor and Employment Attorney', 'Real Estate Attorney', 'Tax Attorney', 'Traffic Law Attorney',
       'Wills and Estate Planning', 'Contracts Attorney', 'Family Law Attorney', 'Immigration Attorney',
       'Personal Bankruptcy Attorney', 'Personal Injury Attorney', 'Patent Law', 'Workers Compensation Law',
       'Elder Law', 'Employment Law', 'Entertainment Law', 'General Litigation', 'IP and Internet Law', 'Medical Law',
       'Tenant and Eviction Law', 'Social Security Law', 'Mediators', 'Process Servers', 'Wills, Trusts, and Probates',
       'Bankruptcy Law', 'Business Law', 'Consumer Law', 'Billing Services', 'Bill and Account Collectors',
       'Mortgage Lenders', 'Auto Loan Providers', 'Tellers', 'Installment Loans', 'Banks and Credit Unions',
       'Currency Exchange', 'Fingerprinting', 'Cultural Center', 'Healthcare Social Workers', 'Adoption Services',
       'Passport and Visa Services', 'Vacation Rental Agents', 'Insurance Underwriters', 'Karaoke', 'Donation Center',
       'Substance Abuse and Behavioral Disorder Counselors', 'Tours', 'Halfway Houses', 'Homeless Shelters',
       'Pet Insurance Agent', 'Bail Bondsmen', 'Health Insurance Offices', 'Tax Preparers', 'Bookkeepers',
       'Tax Services', 'Individual Tax Preparation', 'Inspectors, Testers, Sorters, Samplers, and Weighers',
       'Public Adjusters', 'Secretaries and Administrative Assistants, Except Legal, Medical, and Executive',
       'Video Booth Rental', 'Pet Cremation Services', 'Pet Services', 'Holistic Animal Care', 'Animal Shelters',
       'Wind Turbine Service Technicians', 'Games and Concession Rental', 'Photo Booth Rentals',
       'Karaoke Machine Rental', 'Food Servers, Nonrestaurant', 'Cremation Services', 'Embalmers',
       'Morticians, Undertakers, and Funeral Directors', 'Wedding Chapels', 'Building Supplies', 'Animal Trainers',
       'Dog Training', 'Pet Sitting', 'Cat Grooming', 'Dog Grooming', 'Comedy Club Owner', 'Expert Witness',
       'Manufacturer Sales Representative', 'Motion Picture Projectionists',
       'Camera Operators, Television, Video, and Motion Picture', 'Audio Services', 'Cabaret',
       'Video Streaming and Webcasting Services', 'Brokerage Clerks', 'Customs Brokers', 'Budget Analysts',
       'Credit Analysts', 'Credit Counselors', 'Loan Interviewers and Clerks', 'Investing', 'Financial Analysts',
       'Survey Researchers', 'Surveying and Mapping Technicians', 'Cartographers and Photogrammetrists',
       'Procurement Clerks', 'Reiki Lessons', 'Silent Disco', 'Country Dance Halls', 'Billing and Posting Clerks',
       'Music Venues', 'Television Stations', 'Radio Stations', 'Broadcast News Analysts', 'Nutritionists',
       'Executive Secretaries and Executive Administrative Assistants', 'Statistical Assistants', 'Concierges',
       'Receptionists and Information Clerks', 'Residential Advisors', 'Pool Halls', 'Country Clubs', 'Dance Clubs',
       'Nightlife', 'Dishwashers', 'Barbecue and Grill Services', 'Cideries', 'Kombucha', 'Packing Services',
       'Audio Visual and Multimedia Collections Specialists', 'High Fidelity Audio Equipment', 'Military Surplus',
       'Safe Store', 'Safety Equipment Retailer', 'Cashiers', 'Art Restoration', 'Semiconductor Processors',
       'Grill Services', 'Interlock Systems', 'Luggage Storage', 'Bike Sharing', 'Mailbox Centers', 'Piano Tuning',
       'Piano Services', 'Botanical Gardens', 'Computer, Automated Teller, and Office Machine Repairers',
       'Fitness Equipment Assembly', 'Exercise Equipment Repair', 'Stock Clerks and Order Fillers',
       'Counter and Rental Clerks', 'Bicycle Repairers', 'Bike Repair and Maintenance', 'Data Recovery',
       'Mobile Phone Repair', 'Virus Removal Services', 'Shoe Shine', 'Dry Cleaning',
       'Audio and Visual Equipment Rental', 'Motorcycle Gear Store', 'Art Supplies Store', 'Wholesale Store',
       'Tax Examiners and Collectors, and Revenue Agents', 'Caricatures', 'Karaoke Rental', 'Bounce House Rentals',
       'Photo Booth Rental', 'Fire Inspectors and Investigators', 'Opticians, Dispensing', 'Financial Advising',
       'Butchers and Meat Cutters', 'Firewood', 'Lighting Fixtures and Equipment', 'Lock Installation and Repair',
       'Pastry Chef and Cake Making Services', 'Custom Cakes', 'Patisserie and Cake Shop', 'Cupcakes',
       'Party Bike Rentals', 'Funeral Attendants', 'Funeral Services and Cemeteries', 'Circuit Training Gyms',
       'Interval Training Gyms', 'Sports Clubs', 'Meditation Centers', 'Gyms', 'Herbs and Spices',
       'Laundry and Dry Cleaning Workers', 'Musical Instrument Repairers and Tuners', 'Screen Printing',
       'Public Address System and Other Announcers', 'Radio Operators', 'Shoe and Leather Workers and Repairers',
       'Shoe Machine Operators and Tenders', 'Packers and Packagers, Hand', 'Packing and Unpacking',
       'Postal Service Clerks', 'Postal Service Mail Sorters, Processors, and Processing Machine Operators',
       'Personal Chef', 'Sales', 'Watch Repair', 'Apartment Agents', 'Payroll Services', 'Business Tax Preparation',
       'Web Site Designer', 'Employment Service', 'Social Media Marketing', 'Operations Research Analysts',
       'Information Security Analysts', 'Industrial Organizational Psychologists', 'Mathematicians', 'Statisticians',
       'Chief Executives', 'Compliance Officers', 'Telephone Operators',
       'Switchboard Operators, Including Answering Service', 'Instructional Coordinators', 'Librarians',
       'College Admissions Counseling', 'Labor Relations Specialists', 'Compensation and Benefits Managers',
       'Recruiting', 'Career Counseling', 'Talent Agencies', 'Computer Systems Analysts', 'Technical Support',
       'Software Sales Engineer', 'Computer and Information Research Scientists', 'Data Entry Keyers',
       'Database Administrators', 'Network and Computer Systems Administrators', 'Software Developers',
       'Web Developers', 'Computer Operators', 'Computer Hardware Engineers', 'Computer User Support Specialists',
       'Mobile Design', 'Network Support Services', 'Web Hosting', 'Mass Media', 'Elder Care Planning',
       'Buyers and Purchasing Agents', 'Curators', 'Museum Technicians and Conservators', 'Economists', 'Officiants',
       'Ushers, Lobby Attendants, and Ticket Takers', 'Appraisal Services',
       'Compensation, Benefits, and Job Analysis Specialists', 'Management Analysts', 'Statistical Data Analysis',
       'Anthropologists and Archeologists', 'Astronomers', 'Atmospheric and Space Scientists',
       'Biochemists and Biophysicists', 'Conservation Scientists', 'Cost Estimators',
       'Dispatchers, Except Police, Fire, and Ambulance', 'Geographers',
       'Geoscientists, Except Hydrologists and Geographers', 'Hydrologists',
       'Interviewers, Except Eligibility and Loan', 'Political Scientists', 'General and Operations Managers',
       'Social Science Research Assistants', 'Human Resources Assistants, Except Payroll and Timekeeping',
       'Human Resources Managers', 'First Line Supervisors of Non Retail Sales Workers',
       'First Line Supervisors of Office and Administrative Support Workers',
       'First Line Supervisors of Personal Service Workers', 'Microbiologists', 'Office Clerks, General',
       'Office Machine Operators, Except Computer', 'Order Clerks', 'Production, Planning, and Expediting Clerks',
       'Purchasing Managers', 'Archivists', 'Art Directors', 'Materials Scientists', 'Mathematical Technicians',
       'Natural Sciences Managers', 'Project Management', 'Animation', 'Wardrobe Consulting', 'Presentation Design',
       'Geneticists', 'Weighers, Measurers, Checkers, and Samplers, Recordkeeping', 'File Clerks', 'Desktop Publishers',
       'Logo Design', 'Proofreaders and Copy Markers', 'Word Processors and Typists', 'Correspondence Clerks',
       'Editors', 'Songwriting', 'Sociologists', 'Locker Room, Coatroom, and Dressing Room Attendants', 'Advertising',
       'Public Relations and Fundraising Managers', 'College Counseling', 'Training and Development Managers',
       'Firearm Training', 'Flight Instruction', 'Software Development', 'Marketing Managers',
       'Market Research Analysts and Marketing Specialists', 'Library Assistants, Clerical', 'Library Technicians',
       'Payroll and Timekeeping Clerks', 'Graphic Design', 'Web Design', 'Consulting',
       'Veterinary Assistants and Laboratory Animal Caretakers', 'Veterinary Technologists and Technicians',
       'Medical and Clinical Laboratory Technologists', 'Ophthalmic Laboratory Technicians',
       'Ophthalmic Medical Technicians', 'Radiologic Technologists', 'Audiologist', 'Dentists, General',
       'General Dentistry', 'Medicine and Medical Services', 'Neurotologists', 'Nurse Practitioner',
       'Pediatricians, General', 'Respiratory Therapy Technicians', 'Emergency Pet Hospital', 'Behavior Analysts',
       'Counseling and Mental Health', 'Therapy or Mental Health Services', 'Occupational Therapy Assistants',
       'Therapy and Counseling', 'Addiction Medicine', 'Acupressurist', 'Acupuncturist', 'Lice Services',
       'Hair Loss Centers', 'Clinical, Counseling, and School Psychologists', 'Family Counseling', 'Hypnotherapy',
       'Lactation Services', 'Recreational Therapists', 'Sex Therapists', 'Speech Therapists', 'Parenting Classes',
       'Childbirth Education', 'CPR Classes', 'First Aid Classes', 'Forensic Science Technicians', 'Genealogy',
       'Genetic Counselors', 'Halotherapy', 'Medical Secretaries', 'Medical Transcriptionists', 'Memory Care',
       'Undersea/Hyperbaric Medicine', 'Alternative Medicine', 'Naturopathic/Holistic', 'Rehabilitation Center',
       'Dietitians', 'Health Coach', 'Hypnosis/Hypnotherapy', 'Marriage and Relationship Counseling',
       'Occupational Health and Safety Technicians', 'Occupational Therapy', 'Occupational Therapy Aides',
       'Psychiatric Technicians', 'Dietetic Technicians', 'Nutritionist', 'Private Detectives and Investigators',
       'Detectives and Criminal Investigators', 'Private Investigation', 'Transportation Security Screeners',
       'Security Services', 'Tai Chi Instructor', 'Soccer Coach', 'Softball Coach', 'Baseball Coach',
       'Basketball Coach', 'Swim Lessons', 'Volleyball Coach', 'Self-defense Instructor', 'Karate Instructor',
       'Pickleball Coach', 'Kickboxing Trainer', 'Disc Golf Coach', 'Taekwondo Instructor', 'Badminton Coach',
       'Amateur Sports Teams Coach', 'Boxing Trainer', 'Squash Coach', 'Tennis Coach', 'Archery Coach',
       'Capoeira Instructor', 'Athletic Trainer', 'Package Delivery',
       'Transportation, Storage, and Distribution Managers', 'Couriers', 'App Based Delivery Services',
       'Light Truck or Delivery Services Drivers', 'Baggage Porters and Bellhops', 'Cargo and Freight Agents',
       'Heavy and Tractor Trailer Truck Drivers', 'Shipping, Receiving, and Traffic Clerks', 'Shipping Centers',
       'Postmasters and Mail Superintendents', 'Floral Designers', 'Flowers and Gifts Store', 'Online Auctioneer',
       'Computer Store', 'Fashion Retailer', 'Uniform Store', 'Mattress Store', 'Pool and Billiards Store',
       'Antiques Store', 'Comic Book Store', 'Used Bookstore', 'Photography Store', 'Bridal Shop', 'Hydroponics Store',
       'Mobile Phone Accessories Retailer', 'Mobile Phone Retailer', 'Costume Store', 'Rug Store',
       'Holiday Decorations Store', 'Props Store', 'Tabletop Games Store', 'Teacher Supplies Store', 'Home Decor Shop',
       'Trophy Shop', 'Watches Retailer', 'Grilling Equipment Store', 'Sunglasses Shop', 'Framing Store',
       'Vinyl Record Store', 'Golf Equipment Retailer', 'Hockey Equipment Retailer',
       'Hunting and Fishing Supplies Retailer', 'Outdoor Gear Store', 'Skate Shop', 'Ski and Snowboard Shop',
       'Kitchen and Bath Store', 'Kitchen Supplies Shop', 'Tableware Store', 'Knitting Supplies Shop',
       'Office Equipment Retailer', 'Pop up Shop', 'Religious Items Retailer', 'Souvenir Shop', 'Spiritual Shop',
       'Thrift Store', 'Wig Store', 'Books, Mags, Music and Video Store', 'Music and DVDs Store', 'Video Game Store',
       'Videos and Video Game Rental Store', 'Fabric Menders, Except Garment', 'Fabric and Apparel Patternmakers',
       'Embroidery', 'Quilting and Crochet', 'Jewelers and Precious Stone and Metal Workers',
       'Gemstones and Minerals Shop', 'Gold Buyers', 'Guitar Stores', 'Bird Shops', 'Reptile Shops',
       'First Line Supervisors of Retail Sales Workers', 'Embroidery and Crochet Store', 'Gift Shop', 'Battery Store',
       'Cards and Stationery Store', 'Outlet Store', 'Party Supplies', 'Public Markets', 'Perfume', 'Piano Stores',
       'Brewing Supplies Store', 'Retail Salespersons', 'Sewing and Alterations', 'Fashion Designers', 'Jewelry Repair',
       'Clock Repair', 'Tobacco Shop', 'Candy Buffet Services', 'Beverage Store', 'Fruits and Veggies', 'Cheese Shops',
       'Organic Stores', 'Pasta Shops', 'Imported Food', 'Olive Oil', 'Gelato', 'Ice Cream and Frozen Yogurt',
       'Local Fish Stores', 'Shaved Ice', 'Shaved Snow', 'Butcher', 'Herbal Shops', 'Restaurant Supplies', 'Meat Shops',
       'Seafood Markets', 'Chocolatiers and Shops', 'Desserts', 'Personal Chefs', 'Food Truck or Cart Services',
       'Baker', 'Coffee and Tea', 'Juice Bars and Smoothies', 'Local Flavor', 'Chefs and Cooks',
       'First Line Supervisors of Food Preparation and Serving Workers', 'Food Preparation Workers',
       'Food Service Managers', 'Waiters and Waitresses', 'Do It Yourself Food', 'Empanadas', 'Piadina', 'Poke',
       'Smokehouse', 'Eatertainment', 'Food Cart Operator', 'Wedding Cakes', 'Street Vendors', 'Tea Rooms', 'Pretzels',
       'Bagels', 'Donuts', 'Acai Bowls', 'Bubble Tea', 'Chimney Cakes', 'Coffee Roasteries', 'Macarons',
       'Audio Recording', 'Community Book Box', 'Video Production', 'Video Editing', 'Portrait Artistry',
       'Film and Video Editors', 'Film and Video Production', 'Sound Engineering Technicians',
       'Producers and Directors', 'Multimedia Artists and Animators', 'Taxidermy', 'Party Favors', 'Calligraphy',
       'Engraving', 'Illustrating', 'Scrapbooking', 'Bookbinding', 'Duplication Services',
       'Prepress Technicians and Workers', 'Printing Press Operators', 'Digitizing Services', '3D Printing',
       '3D Modeling', 'Photographic Process Workers and Processing Machine Operators', 'Pool Table Repair Services',
       'Sewing Machine Operators', 'Alterations, Tailoring, and Clothing Design', 'Watch Repairers',
       'Restaurants and Bars', 'Welding, Cutting and Metal Frame Erection')
order by cob_group

--excluded PL A&E premium
select sum(highest_yearly_premium),
       json_args like '%pl_aop_elevator_consultants_alloc%'     as elevator,
       json_args like '%pl_aop_agricultural_engineering_alloc%' as agricultural,
       json_args like '%pl_aop_energy_consultants_ae_alloc%'    as energy
from dwh.quotes_policies_mlob qpm
where qpm.lob_policy = 'PL'
  and policy_start_date >= '2022-01-01'
  and policy_start_date < '2023-01-01'
  and cob_group = 'Architects & engineers'
  and highest_policy_status >= 4
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
group by 2, 3, 4

--excluded real estate investor premium
select sum(highest_yearly_premium)
from dwh.quotes_policies_mlob qpm
where qpm.lob_policy = 'GL'
  and policy_start_date >= '2022-01-01'
  and policy_start_date < '2023-01-01'
  and cob = 'Real Estate Investor'
  and highest_policy_status >= 4
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')

--excluded allied health premium GL & PL limited COBs
select sum(highest_yearly_premium)
from dwh.quotes_policies_mlob qpm
where qpm.lob_policy in ('PL', 'GL')
  and policy_start_date >= '2022-01-01'
  and policy_start_date < '2023-01-01'
  and cob in ('Home Health Aides', 'Home Health Care', 'Personal Care Aides', 'Personal Care Services')
  and highest_policy_status >= 4
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')

--excluded allied health premium PL only expanded COBs
select sum(highest_yearly_premium)
from dwh.quotes_policies_mlob qpm
where qpm.lob_policy in ('PL')
  and policy_start_date >= '2022-01-01'
  and policy_start_date < '2023-01-01'
  and cob in ('Speech Language Pathologists', 'Marriage And Family Therapists', 'Mental Health Counselors',
              'Occupational Health And Safety Specialists', 'Occupational Therapists', 'Psychologists')
  and highest_policy_status >= 4
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')

--package distribution by month
select extract(year from creation_time) || '-' ||
       right('00' + convert(varchar, extract(month from creation_time)), 2) as creation_year_month,
       --extract(month from policy_start_date) as creation_year_month,
       --cob_group,
       highest_status_package,
       count(highest_status_package)                                        as package_count
from dwh.quotes_policies_mlob
where highest_policy_status >= 4
  and
  --(cob in ('Restaurant', 'Caterers', 'Food Truck', 'Coffee Shop', 'Grocery Store') or cob_group = 'Retail') and
  --cob_group = 'Food & beverage' and
  --cob_group not in ('Construction', 'Cleaning') and
    lob_policy = 'GL'
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and highest_status_package in ('basic', 'pro', 'proPlus')
  and distribution_channel <> 'agents'
  and creation_time >= '2022-01-01'
  and creation_time <= '2023-02-01'
group by 1, 2
order by creation_year_month asc

--get PL aggregates (PIF, premium, limit) for policies sold w/ cyber coverage (amdt version = base, state = list of 6 where not excluded)
select sum(highest_policy_aggregate_limit) as total_agg_limit,
       count(business_id)                  as policy_count,
       sum(highest_yearly_premium)         as total_premium
from dwh.quotes_policies_mlob
where lob_policy = 'PL'
  and state in ('CA', 'CT', 'HI', 'KS', 'NE', 'NJ')
  and --note: this is an underestimate since we did not start excluding cyber (outside of these states) until several months after launch
    highest_policy_status >= 4
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and current_amendment isnull
--current_amendment in ('{"version": 2.0, "amendmentId": "SNIC_BMPL_2_new_policy"}', '{"version": 2.0, "amendmentId": "SNIC_BMPL_2_renewal"}')

--avg_premium by month
select extract(month from creation_time) as creation_month,
       --distribution_channel,
       --lob_policy,
       --cob,
       --state,
       --count(highest_yearly_premium) as business_count,
       --highest_status_name,
       --highest_status_package,
       median(highest_yearly_premium)    as avg_premium
--current_amendment
from dwh.quotes_policies_mlob
where highest_policy_status >= 4
  and (cob in ('Restaurant', 'Caterers', 'Food Truck', 'Coffee Shop', 'Grocery Store') or cob_group = 'Retail')
  and lob_policy = 'GL'
  and highest_status_package in ('proPlus', 'proPlusTria')
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and creation_time >= '2022-02-01'
group by 1
order by creation_month asc

--to get all GL proPlus policies written ITD through 1/1/23
select count(business_id) as biz_count, cob_group
from dwh.quotes_policies_mlob
where highest_policy_status >= 4
  and highest_status_package in ('proPlus', 'proPlusTria')
  and lob_policy = 'GL'
  and offer_flow_type in ('APPLICATION', 'RENEWAL')
  and creation_time <= '2023-01-01'
  and policy_start_date <= '2023-01-01'
group by 2
order by biz_count desc

--to get GL proPlus policy list w/ premium from ITD through 1/1/23
select highest_yearly_premium
from dwh.quotes_policies_mlob
where highest_policy_status >= 4
  and highest_status_package in ('proPlus', 'proPlusTria')
  and lob_policy = 'GL'
  and offer_flow_type in ('APPLICATION', 'RENEWAL')
  and creation_time <= '2023-01-01'
  and policy_start_date <= '2023-01-01'

--to get limits distribution across in-force GL book
select count(highest_policy_occurence_limit) as occ_limit_count,
       count(highest_policy_aggregate_limit) as agg_limit_count,
       highest_policy_occurence_limit,
       highest_policy_aggregate_limit,
       cob_group
from dwh.quotes_policies_mlob
where highest_policy_status in ('4', '7')
  and lob_policy = 'GL'
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
group by 3, 4, 5

--lob distribution
select lob_policy,
       count(distinct business_id)
from dwh.quotes_policies_mlob
where highest_policy_status in ('4', '7')
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
group by 1

-- list of marketing cob groups
select DISTINCT cob, cob_group
from dwh.quotes_policies_mlob
group by 1, 2

--most popular profession we write for professional services
select cob, sum(highest_yearly_premium) as total_premium, count(business_id) as pif
from dwh.quotes_policies_mlob
where cob_group in ('Architects & Engineers', 'Business & administrative services', 'Consulting', 'Creative services',
                    'Financial services', 'Insurance professional', 'IT and technical services', 'Legal',
                    'Real estate services')
  and highest_policy_status in ('4', '7')
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
group by cob
order by total_premium desc

--PL DOL reinsurance request
select highest_policy_aggregate_limit,
       highest_status_package,
       sum(highest_yearly_premium) as total_premium,
       count(business_id)          as pif
from dwh.quotes_policies_mlob
where lob_policy = 'PL'
  and highest_policy_status in ('4', '7')
  and state in ('AK', 'AR', 'CT', 'NJ', 'NY', 'VT')
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
group by 1, 2
order by total_premium desc

--qtp by month (DO NOT USE)
select extract(month from creation_time) as creation_month,
       extract(year from creation_time)  as creation_year,
       count(distinct business_id)       as biz_count
from dwh.quotes_policies_mlob
where --highest_policy_status >= 4 and --note: add this back to get only policies sold
    lob_policy = 'GL'
  and
  --offer_flow_type in ('APPLICATION','RENEWAL','CANCEL_REWRITE') and
    offer_flow_type in ('APPLICATION')
  and distribution_channel <> 'agents'
  and creation_time >= '2020-01-01'
  and creation_time <= '2022-12-31'
group by 1, 2
order by creation_year, creation_month asc

--qtp by month (consistent with 2-dim QTP)
with t1 as (select last_day(creation_time)             as month,
                   count(distinct related_business_id) as sold_policy_count
            from dwh.quotes_policies_mlob
            where highest_policy_status >= 3
              and lob_policy = 'WC'
              and new_reneweal = 'new'
              and offer_flow_type in ('APPLICATION')
              and distribution_channel <> 'agents'
              and cob_group = 'Food & beverage'
              and creation_time >= '2020-01-01'
              and creation_time <= '2023-05-01'
            group by 1
            order by month asc),

     t2 as (select last_day(creation_time)             as month,
                   count(distinct related_business_id) as quote_count
            from dwh.quotes_policies_mlob
            where lob_policy = 'WC'
              and new_reneweal = 'new'
              and offer_flow_type in ('APPLICATION')
              and distribution_channel <> 'agents'
              and cob_group = 'Food & beverage'
              and creation_time >= '2020-01-01'
              and creation_time <= '2023-05-01'
            group by 1
            order by month asc)

select *, cast(sold_policy_count * 1.0 / quote_count * 1.0 as decimal(10, 4)) as qtp
from t1
         join t2 on t1.month = t2.month
order by t1.month asc

--qtp (consistent with 2-dim QTP)
with t1 as (select min(extract(year from creation_time)) as joiner,
                   count(distinct related_business_id)   as sold_policy_count
            from dwh.quotes_policies_mlob
            where highest_policy_status >= 3
              and lob_policy = 'GL'
              and cob_group = 'Construction'
              and offer_flow_type in ('APPLICATION')
              --and distribution_channel not in ('agents','partnerships')
              and distribution_channel = 'partnerships'
              and affiliate_id = '4070'
              and creation_time >= '2023-07-01'
    --and creation_time <= '2022-12-31'
),
     t2 as (select min(extract(year from creation_time)) as joiner,
                   count(distinct related_business_id)   as quote_count
            from dwh.quotes_policies_mlob
            where lob_policy = 'GL'
              and cob_group = 'Construction'
              and offer_flow_type in ('APPLICATION')
              --and distribution_channel not in ('agents','partnerships')
              and distribution_channel = 'partnerships'
              and affiliate_id = '4070'
              and creation_time >= '2023-07-01'
         --and creation_time <= '2022-12-31'
     )
select sold_policy_count, quote_count, cast(t1.sold_policy_count * 1.0 / t2.quote_count * 1.0 as decimal(10, 4)) as qtp
from t1
         join t2 on t1.joiner = t2.joiner

--all policies written by agent A
select business_id,
       cob_name,
       lob,
       policy_status_name,
       yearly_premium,
       start_date,
       business_state,
       business_name,
       business_address,
       *
from db_data_science.v_all_agents_policies
where agent_email_address like '%edgar@olverains.com%'
  and
  --cob_name = 'Grocery Store' and
    start_date >= '2023-01-01'
  and policy_status_name <> ''

select *
from db_data_science.v_all_agents_policies
where agency_name like '%Lawrence R%'
  and cob_name = 'Grocery Store'
  and policy_status_name = 'Quote'
limit 10

--top X agents by LOB by customers
select agent_name, sum(yearly_premium) as total_premium, count(DISTINCT business_id) as total_customers
from db_data_science.v_all_agents_policies
where --policy_type = 'PL' and
      --cob = 'Insurance Agent' and
      policy_status_name <> ''
group by 1
order by total_customers desc
limit 100

--all policies written by agent A
select business_id,
       cob_name,
       lob,
       policy_status_name,
       yearly_premium,
       *
from db_data_science.v_all_agents_policies
where --agent_email_address = 'greg@traditions-group.com' --and
      agency_name = 'Traditions Insurance Group LLC'
--policy_status_name <> ''

select *
from db_data_science.v_all_agents_policies
limit 10

--to get largest restaurant / retail policies (top 20 / top restaurants)
select distinct business_id,
                policy_start_date,
                lob_policy,
                revenue_in_12_months,
                cob,
                distribution_channel,
                highest_yearly_premium--, highest_status_package
from dwh.quotes_policies_mlob
where highest_policy_status = 4
  and distribution_channel = 'agents'
  and
  --lob_policy = 'GL' and
    offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and creation_time >= '2024-06-01'
  and cob_group = 'Food & beverage'
  and
  --(cob = 'Restaurant' or cob_group = 'Retail') and
    revenue_in_12_months <> ''
order by highest_yearly_premium desc
limit 20

--to get 10 largest restaurant / retail COBs (new biz only)
select cob,
       cob_group,
       count(distinct business_id) as businesses,
       sum(highest_yearly_premium) as total_premium
from dwh.quotes_policies_mlob
where highest_policy_status >= 4
  and
  --distribution_channel = 'agents' and
  --lob_policy = 'GL' and
  --new_reneweal= 'new' and
    creation_time >= '2022-01-01'
  and
  --(cob = 'Restaurant' or cob_group = 'Retail') and
    cob_group in ('Food & beverage', 'Retail')
  and revenue_in_12_months <> ''
group by 1, 2
order by total_premium desc
limit 10

select *
from s3_operational.rating_svc_prod_calculations
where policy_id <> ''
  and policy_id <> -1
  and creation_time > '2022-12-20'
  and lob = 'GL'
limit 10

select *
from nimi_svc_prod.policies
limit 10

select distinct(cob)
from dwh.quotes_policies_mlob
where cob_group = 'Food & beverage'

--to get top 100 COBs (new biz only)
select cob,
       cob_group,
       count(distinct business_id) as businesses,
       sum(highest_yearly_premium) as total_premium
from dwh.quotes_policies_mlob
where highest_policy_status >= 4
  and
  --distribution_channel = 'agents' and
  --lob_policy = 'GL' and
  --new_reneweal= 'new' and
    creation_time >= '2022-01-01'
  --(cob = 'Restaurant' or cob_group = 'Retail') and
  --cob_group in ('Food & beverage', 'Retail') and
  and highest_yearly_premium <> ''
group by 1, 2
order by total_premium desc
limit 100

--counts on PL retail stores
select distribution_channel, count(distinct business_id) as pif, sum(highest_yearly_premium) as premIF
from dwh.quotes_policies_mlob
where lob_policy = 'PL'
  and cob = 'Retail Stores'
  and highest_policy_status in ('4', '7')
group by 1
order by pif desc

--list of PL retail stores
select distinct business_id, highest_status_name, lob_policy, cob, distribution_channel, highest_yearly_premium
from dwh.quotes_policies_mlob
where lob_policy = 'PL'
  and cob = 'Retail Stores'
  and highest_policy_status in ('4', '7')

--list of all $20K+ policies
select distinct business_id, highest_status_name, lob_policy, cob, distribution_channel, highest_yearly_premium
from dwh.quotes_policies_mlob
where highest_policy_status in ('4', '7')
  and highest_yearly_premium >= 20000
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
order by highest_yearly_premium desc


--loss ratio LR claims data
WITH claims as (select cc.date                                                              as report_date,
                       -- date(date_trunc('year', date_of_loss)) as accident_year,
                       -- actuary requested to change to loss_basis_date
                       last_day(case
                                    when cc.lob = 'PL' then cc.date_submitted
                                    else cc.date_of_loss end)                               as accident_month,
                       date(date_trunc('year', case
                                                   when cc.lob = 'PL' then cc.date_submitted
                                                   else cc.date_of_loss end))               as accident_year,
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
                       sum(nvl(loss_paid_total, 0))                                         as loss_paid_total,
                       sum(nvl(loss_reserve_total, 0))                                      as loss_reserve_total,
                       sum(nvl(expense_ao_paid_total, 0))                                   as expense_ao_paid_total,
                       sum(nvl(expense_dcc_paid_total, 0))                                  as expense_dcc_paid_total,
                       sum(nvl(expense_ao_reserve_total, 0))                                as expense_ao_reserve_total,
                       sum(nvl(expense_dcc_reserve_total, 0))                               as expense_dcc_reserve_total,
                       sum(nvl(recovery_salvage_collected_total, 0))                        as recovery_salvage_collected_total,
                       sum(nvl(recovery_salvage_reserve_total, 0))                          as recovery_salvage_reserve_total,
                       sum(nvl(recovery_subrogation_collected_total, 0))                    as recovery_subrogation_collected_total,
                       sum(nvl(recovery_subrogation_reserve_total, 0))                      as recovery_subrogation_reserve_total


                from (select *,
                             rank() over (partition by claim_id
                                 order by date desc) as date_order
                      from dwh.all_claims_financial_changes_ds
                      where date = ((date_trunc('month', current_date) - interval '1 day')::date)) cc
--                   from dwh.all_claims_financial_changes_ds where date is not null) cc
                         left join nimi_svc_prod.policies p
                                   on cc.policy_reference = p.policy_reference

                where
--           cc.date = last_day(cc.date) and
                    cc.carrier not in (2, 3, 5)
                  and cc.date_order = 1
                group by 1, 2, 3, 4, 5, 6, 7, 8, 9

--     limit 10000
),

     claims_cumulative as (select *,
                                  row_number()
                                  over (partition by policy_id, accident_month order by (select null)) as claim_rank
                           from claims),


     gaap as (select last_day(date)     as date,
                     policy_id,
                     sum(dollar_amount) as earned_premium
              from reporting.gaap_snapshots_ASL
              where trans in ('monthly earned premium', 'monthly earned premium endorsement')
                and test_accounts != 'test'
                and carrier not in (2, 3, 5)
              group by 1, 2),


     gaap_all as (select
--            start_year,
report_month,
--            months_since_start_year,
policy_id,
sum(case when months_since_start_year > 12 then 0 else gaap.earned_premium end) as earned_premium
                  from db_data_science.loss_ratio_date_list_v4 dt_ls
                           left join gaap
                                     on gaap.date = dt_ls.report_month
                  group by policy_id, report_month
                  order by policy_id, report_month),

     qp as (select distinct qp.*, policy_reference
            from dwh.quotes_policies_mlob qp
                     left join nimi_svc_prod.policies p on qp.highest_policy_id = p.policy_id
--     where highest_policy_status >= 3
     )
        ,
     output as (select nvl(gaap_all.policy_id, claims_cumulative.policy_id)                          as policy_id,
                       claims_cumulative.claim_id_trunc,
                       claims_cumulative.tpa,
                       claims_cumulative.accident_year                                               as accident_year,
                       claims_cumulative.accident_month                                              as accident_month,
                       nvl(gaap_all.report_month, claims_cumulative.accident_month)                  as report_date,
--        nvl(gaap_all.months_since_start_year,claims_cumulative.months_since_accident) as months_of_development,
                       case
                           when claim_rank = 1 then earned_premium
                           when claim_rank is null then earned_premium
                           else 0 end                                                                as earned_premium, -- prevents duplication
--        loss_paid_total,
--        loss_reserve_total,
--        expense_ao_paid_total,
--        expense_ao_reserve_total,
--        expense_dcc_paid_total,
--        expense_dcc_reserve_total,
--        recovery_salvage_collected_total,
--        recovery_salvage_reserve_total,
--        recovery_subrogation_collected_total,
--        recovery_subrogation_reserve_total,
                       loss_paid_total + expense_ao_paid_total + expense_dcc_paid_total              as loss_alae_paid,
                       loss_reserve_total + expense_ao_reserve_total + expense_dcc_reserve_total     as loss_alae_reserve,
                       loss_alae_paid + loss_alae_reserve + recovery_salvage_collected_total +
                       recovery_subrogation_collected_total                                          as total_loss_alae,
                       loss_type,
                       location_of_loss,
--        coverage_description,
--        coverage,
                       claim_status,
                       qp.policy_reference,
                       qp.business_id,
                       qp.lob_policy,
                       qp.cob,
                       qp.cob_group,
                       qp.state,
                       qp.distribution_channel,
                       qp.policy_start_date,
--        qp.highest_policy_id,
                       payroll_in_next_12_months,
                       revenue_in_12_months,
                       highest_yearly_premium,
                       new_reneweal,
                       current_amendment,
                       purchase_type,
                       highest_status_package,
                       business_ownership_structure,
                       marketing_source,
                       nvl(json_extract_path_text(json_args, 'years_in_business_num', true),
                           '')                                                                       as years_in_business,
                       nvl(json_extract_path_text(json_args, 'year_business_started', true),
                           '')                                                                       as year_business_started,
                       nvl(json_extract_path_text(json_args, 'carrier', true), '')                   as carrier,
                       case
                           when json_extract_path_text(json_args, 'subcontractor_cost_in_12_months', true) = ''
                               then NULL
                           else json_extract_path_text(json_args, 'subcontractor_cost_in_12_months',
                                                       true) end::float                              as subcontractor_cost_in_12_months,
                       case
                           when json_extract_path_text(json_args, 'num_of_employees', true) = ''
                               then NULL
                           else json_extract_path_text(json_args, 'num_of_employees', true) end::int as num_of_employees
                from gaap_all
                         full join claims_cumulative
                                   on (gaap_all.report_month = claims_cumulative.accident_month
                                       and gaap_all.policy_id = claims_cumulative.policy_id)
                         left join qp
                                   on highest_policy_id = nvl(gaap_all.policy_id, claims_cumulative.policy_id)

                where nvl(report_month, accident_month) < current_date
                  --and nvl(report_month, report_date) >= '2019-01-01'
                  and (earned_premium is not null or claim_id_trunc is not null)
                  and cob is not null
                order by policy_id, report_date)

-- select extract(year from report_date) as year, policy_id,sum(earned_premium) as EP, sum(total_loss_alae) as total_loss
--        from output
-- group by year,policy_id
-- having year = 2020

-- select extract(year from report_date) as year, sum(earned_premium) as EP, sum(total_loss_alae) as total_loss, (date_trunc('month', current_date) - interval '1 day')::date as date
-- from output
-- group by year

select *
from output
limit 50

--premium from PL COBs by LOB
select cob, lob_policy, count(distinct business_id) as PIF, sum(highest_yearly_premium) as WP
from dwh.quotes_policies_mlob
where cob in ('Architect', 'Engineer', 'Interior Designer', 'Property Manager', 'Real Estate Agent', 'Insurance Agent',
              'Real Estate Brokers', 'Urban and Regional Planners', 'Music Entertainment', 'Insurance Inspector',
              'Audio and Video Equipment Technicians', 'Music Production Services', 'Home Inspectors',
              'Video and Film Production', 'Community Gardens', 'Printing Services', 'Travel Guides', 'Notary',
              'Social Services', 'Travel Agency', 'Insurance Appraisers', 'Speech Language Pathologists',
              'Home Health Aides', 'Home Health Care', 'Personal Care Aides', 'Personal Care Services',
              'Marriage and Family Therapists', 'Mental Health Counselors',
              'Occupational Health and Safety Specialists', 'Occupational Therapists', 'Psychologists',
              'CPR and First Aid Training', 'Accountant', 'Claims Adjuster', 'Retail Stores')
  and policy_start_date >= '2022-01-01'
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and highest_policy_status >= 4
group by 1, 2
order by WP desc

select count(distinct business_id)
from dwh.quotes_policies_mlob
where cob = 'Video And Film Production'
  and policy_start_date >= '2021-01-01'
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and highest_policy_status >= 4

select distinct cob
from dwh.quotes_policies_mlob
where policy_start_date >= '2021-01-01'
  and highest_policy_status >= 4

select highest_yearly_premium
from dwh.quotes_policies_mlob
where lob_policy = 'GL'
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and highest_policy_status >= 4
  and distribution_channel = 'agents'
  and highest_status_package in ('proPlus', 'proPlusTria')
  and creation_time <= '2023-01-01'
  and policy_start_date <= '2023-01-01'

--to get decline reasons by business_id
select business_id, decline_reasons
from dwh.underwriting_quotes_data
where decline_reasons <> '[]'
limit 10

--to get business_ids associated with affiliate_name
select distinct business_id, channel, affiliate_name
from reporting.gaap_snapshots_asl
where affiliate_name = 'LegalZoom'
  and status_name = 'Active'
limit 10

--get premium in force by commission tier
select commission_tier, sum(dollar_amount) as premium_in_force
from reporting.gaap_snapshots_asl
where status_name = 'Active'
  and trans = 'New'
group by 1
order by 2 desc

--bolt actives by COB
select cob_name, cob_group, affiliate_name, count(distinct business_id)
from reporting.gaap_snapshots_asl
where affiliate_name like '%bolt%'
  and status_name = 'Active'
group by 1, 2, 3

--to get LegalZoom decline reasons
select gaap.business_id,
       dwh.offer_id,
       dwh.lob,
       dwh.cob,
       gaap.affiliate_name,
       dwh.decline_reasons
from reporting.gaap_snapshots_asl gaap
         inner join dwh.underwriting_quotes_data dwh on gaap.business_id = dwh.business_id and
                                                        gaap.affiliate_name = 'LegalZoom' and
                                                        dwh.decline_reasons <> '[]'
group by 1, 2, 3, 4, 5, 6

--to get revenue distribution of retail and F&B GL policies
select distinct business_id,
                revenue_in_12_months,
                cob_group,
                num_of_employees
from dwh.quotes_policies_mlob
where highest_policy_status = 4
  and highest_status_package = 'proPlus'
  and lob_policy = 'GL'
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and creation_time >= '2022-01-01'
  and creation_time < '2023-01-01'
  and (cob = 'Restaurant' or cob_group = 'Retail')
  and revenue_in_12_months <> ''

--cob by cob_group
select distinct(cob), cob_group
from dwh.quotes_policies_mlob

--top cobs by WP in Jan
select cob, sum(highest_yearly_premium) as new_wp, count(distinct business_id) as pif
from dwh.quotes_policies_mlob
where highest_policy_status >= 4
  and offer_flow_type in ('APPLICATION')
  and creation_time >= '2023-01-01'
  and creation_time < '2023-02-01'
group by 1
order by pif desc

--top cobs by WP in Jan
select cob, sum(highest_yearly_premium) as new_wp, count(distinct business_id) as pif
from dwh.quotes_policies_mlob
where highest_policy_status >= 4
  and offer_flow_type in ('APPLICATION')
  and creation_time >= '2023-01-01'
  and creation_time < '2023-02-01'
group by 1
order by pif desc

--quotes by employees over time
select count(distinct business_id)                                              as policies_total,
       num_of_employees,
       extract(year from policy_start_date) || '-' ||
       right('00' + convert(varchar, extract(month from policy_start_date)), 2) as creation_year_month
from dwh.quotes_policies_mlob
where offer_flow_type in ('APPLICATION')
  and lob_policy = 'WC'
  and cob = 'Restaurant'
group by 2, 3
order by 3 asc

--to get largest fitness policies
select distinct business_id,
                policy_start_date,
                lob_policy,
                revenue_in_12_months,
                cob,
                distribution_channel,
                highest_yearly_premium--, highest_status_package
from dwh.quotes_policies_mlob
where highest_policy_status = 4
  and
  --distribution_channel = 'agents' and
  --lob_policy = 'GL' and
    offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and creation_time >= '2023-01-01'
  and cob_group = 'Fitness'
  and
  --(cob = 'Restaurant' or cob_group = 'Retail') and
    revenue_in_12_months <> ''
order by highest_yearly_premium desc
limit 30

--top claims list
select *
from dwh.all_claims_details claims
         left join (select distinct business_id,
                                    json_extract_path_text(json_args, 'business_name', true) as business_name
                    from dwh.quotes_policies_mlob) qpm
                   on qpm.business_id = claims.business_id
where lob = 'GL'
  and cob_name = 'Restaurant'
--missing loss dollars (separate table?)

--scratch looking for drivers of WC F&B
select lob,
       affiliate_name,
       extract(year from transaction_date) || '-' ||
       right('00' + convert(varchar, extract(month from transaction_date)), 2) as creation_year_month,
       sum(annual_premium_accum)                                               as new_wp,
       count(business_id)                                                      as new_pif
from reporting.gaap_snapshots_asl
where affiliate_name = 'Intuit QBOP'
  and status_name = 'Active'
  and transaction_date >= '2021-01-01'
  and
  --transaction_date < '2023-02-01' and
    lob = 'WC'
  and trans = 'Premium collected - New'
  and cob_group = 'Food & beverage'
group by 1, 2, 3
order by 3 asc

select trans, affiliate_name, annual_premium_accum, business_id
from reporting.gaap_snapshots_asl
where affiliate_name <> ''
  and status_name = 'Active'
  and transaction_date >= '2023-01-01'
  and lob = 'WC'
  and trans = 'Premium collected - New'
limit 100

--to get largest f&b WC policies
select distinct business_id,
                policy_start_date,
                lob_policy,
                revenue_in_12_months,
                cob,
                distribution_channel,
                highest_yearly_premium--, highest_status_package
from dwh.quotes_policies_mlob
where highest_policy_status >= 3
  and highest_yearly_premium <> ''
  and
  --distribution_channel = 'agents' and
    lob_policy = 'WC'
  and
  --offer_flow_type in ('APPLICATION','RENEWAL','CANCEL_REWRITE') and
    creation_time >= '2023-01-01'
  and cob_group = 'Food & beverage'
  and
  --(cob = 'Restaurant' or cob_group = 'Retail') and
  --revenue_in_12_months <> '' and
    distribution_channel = 'partnerships'
order by highest_yearly_premium desc
limit 100


select lob, affiliate_id, sum(annual_premium_accum) as new_wp
from reporting.gaap_snapshots_asl
where --affiliate_name <> '' and
    status_name = 'Active'
  and transaction_date >= '2023-01-01'
  and transaction_date < '2023-02-01'
  and lob = 'WC'
  and trans = 'Premium collected - New'
  and cob_group = 'Food & beverage'
group by 1, 2
order by new_wp desc

--to get WC F&B by month by affiliate_name
select qpm.lob_policy,
       qpm.cob_group,
       gaap.affiliate_name,
       extract(year from qpm.creation_time) || '-' ||
       right('00' + convert(varchar, extract(month from qpm.creation_time)), 2) as creation_year_month,
       sum(qpm.highest_yearly_premium)
from dwh.quotes_policies_mlob qpm
         inner join reporting.gaap_snapshots_asl gaap on gaap.business_id = qpm.business_id and
                                                         qpm.highest_policy_status >= 4 and
                                                         qpm.lob_policy = 'WC' and
                                                         qpm.distribution_channel = 'partnerships' and
                                                         qpm.cob_group = 'Food & beverage' and
                                                         gaap.affiliate_name <> '' and
                                                         qpm.offer_flow_type in ('APPLICATION')
group by 1, 2, 3, 4
order by creation_year_month asc

--to get lob mix for F&B by channel
select distribution_channel,
       lob_policy,
       count(distinct business_id) as new_businesses,
       avg(revenue_in_12_months)   as avg_revenue,
       avg(num_of_employees)       as avg_employees
from dwh.quotes_policies_mlob
where highest_policy_status >= 3
  and highest_yearly_premium <> ''
  and distribution_channel in ('agents', 'sem', 'Next Connect')
  and
  --lob_policy = 'WC' and
    offer_flow_type in ('APPLICATION')
  and creation_time >= '2023-01-01'
  and cob_group = 'Food & beverage'
--revenue_in_12_months <> '' and
group by 1, 2

select distinct distribution_channel, count(distinct business_id) as new_businesses
from dwh.quotes_policies_mlob
where highest_policy_status >= 3
  and highest_yearly_premium <> ''
  and
  --distribution_channel in ('agents','sem','partnerships') and
  --lob_policy = 'WC' and
    offer_flow_type in ('APPLICATION')
  and creation_time >= '2023-01-01'
  and cob_group = 'Food & beverage'
--revenue_in_12_months <> '' and
group by 1

--get largest claims (top claims) in a segment
With claims_detail as
         (select cc.date                                                              as report_date,
                 -- date(date_trunc('year', date_of_loss)) as accident_year,
                 -- actuary requested to change to loss_basis_date
                 last_day(case
                              when cc.lob = 'PL' then cc.date_submitted
                              else cc.date_of_loss end)                               as accident_month,
                 date(date_trunc('year', case
                                             when cc.lob = 'PL' then cc.date_submitted
                                             else cc.date_of_loss end))               as accident_year,
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
                 sum(nvl(loss_paid_total, 0))                                         as loss_paid_total,
                 sum(nvl(loss_reserve_total, 0))                                      as loss_reserve_total,
                 sum(nvl(expense_ao_paid_total, 0))                                   as expense_ao_paid_total,
                 sum(nvl(expense_dcc_paid_total, 0))                                  as expense_dcc_paid_total,
                 sum(nvl(expense_ao_reserve_total, 0))                                as expense_ao_reserve_total,
                 sum(nvl(expense_dcc_reserve_total, 0))                               as expense_dcc_reserve_total,
                 sum(nvl(recovery_salvage_collected_total, 0))                        as recovery_salvage_collected_total,
                 sum(nvl(recovery_salvage_reserve_total, 0))                          as recovery_salvage_reserve_total,
                 sum(nvl(recovery_subrogation_collected_total, 0))                    as recovery_subrogation_collected_total,
                 sum(nvl(recovery_subrogation_reserve_total, 0))                      as recovery_subrogation_reserve_total


          from (select *,
                       rank() over (partition by claim_id
                           order by date desc) as date_order
                from dwh.all_claims_financial_changes_ds
                where date = ((date_trunc('month', current_date) - interval '1 day')::date)) cc
--                   from dwh.all_claims_financial_changes_ds where date is not null) cc
                   left join nimi_svc_prod.policies p
                             on cc.policy_reference = p.policy_reference

          where
--           cc.date = last_day(cc.date) and
              cc.carrier not in (2, 3, 5)
            and cc.date_order = 1
          group by 1, 2, 3, 4, 5, 6, 7, 8, 9),

     claims_total as
         (select claim_id_trunc,
                 loss_paid_total + expense_ao_paid_total + expense_dcc_paid_total          as loss_alae_paid,
                 loss_reserve_total + expense_ao_reserve_total + expense_dcc_reserve_total as loss_alae_reserve,
                 loss_alae_paid + loss_alae_reserve + recovery_salvage_collected_total +
                 recovery_subrogation_collected_total                                      as total_loss_alae
          from claims_detail),

     claims_sum as
         (select claim_id_trunc,
                 sum(loss_alae_paid)    as loss_alae_paid,
                 sum(loss_alae_reserve) as loss_alae_reserve,
                 sum(total_loss_alae)   as total_loss_alae
          from claims_total
          group by claim_id_trunc),

     claims_detail_and_sum as
         (select *
          from dwh.all_claims_details
                   left join claims_sum on dwh.all_claims_details.claim_id = claims_sum.claim_id_trunc)

select *
from claims_detail_and_sum claims
         left join (select distinct business_id,
                                    json_extract_path_text(json_args, 'business_name', true) as business_name
                    from dwh.quotes_policies_mlob) qpm
                   on qpm.business_id = claims.business_id
where --lob = 'GL' and
    cob_name = 'Restaurant'
  and total_loss_alae is not null
order by total_loss_alae desc


--to find WC SR factor
select calculation,
       split_part(json_extract_path_text(calculation, 'schedule_rating_premium_calculation', 'schedule_rating_factor',
                                         true), '|', 1) as SR_result
from rating_svc_prod.calculations
where lob = 'WC'
limit 10

--what's in the new cortex table?
select *
from dwh.policy_transactions
limit 10

--pull list of leads by COB by day
select --extract(year from event_date) || '-' || right('00'+convert(varchar,extract(month from event_date)),2) || '-' || right('00'+convert(varchar,extract(day from event_date)),2) as event_year_month_day,
       event_date,
       cob_name,
       count(distinct business_id)
from dwh.daily_activities
where start_date >= '2022-10-01'
  and cob_name <> ''
  and lead = 1
group by 1, 2
order by 1 asc, 3 desc

--custom query for cinet/tivly leads
select eventtime, related_business_id, business_id, funnelphase, placement, list.permitted, list.update_time
from dwh.all_activities_table aa
         join dwh.sources_attributed_table ss on aa.tracking_id = ss.tracking_id
         left join portfolio_svc_prod.permitted_cobs_states_lobs list
                   on list.cob_id = ss.cob_id and list.state_code = ss.state_code and aa.lob = list.lob
where date(eventtime) >= '2024-03-01'
  and (funnelphase in ('Lead', 'Unsupported - View Out of Appetite COBs', 'Unsupported - Get Phone Call',
                       'Unsupported - Get Referral')
    OR funnelphase like '%Unsupported%')

--to get top legalzoom COBs
select cob_name, affiliate_name, count(distinct business_id) as businesses, sum(annual_premium_accum) as total_premium
from reporting.gaap_snapshots_asl
where affiliate_name = 'LegalZoom'
  and status_name = 'Active'
  and trans = 'New'
group by 1, 2
order by 3 desc

--to get legalzoom by LOB
select affiliate_name, lob, count(distinct business_id) as businesses, sum(annual_premium_accum) as total_premium
from reporting.gaap_snapshots_asl
where affiliate_name = 'LegalZoom'
  and status_name = 'Active'
group by 1, 2
order by 3 desc

--to get smartystreets / address validation/verification failures
select all_steps_related_business_id like '%Questionnaire - Address Validation - failure - correction%' as correction,
       all_steps_related_business_id like '%Questionnaire - Address Validation - failure - invalid%'    as invalid,
       all_steps_related_business_id like '%Questionnaire - Address Validation - failure - retryRange%' as retry,
       all_steps_related_business_id like '%Questionnaire - Address Validation - failure - stateError%' as stateError,
       all_steps_related_business_id like '%Questionnaire - Address Validation - success%'              as success,
       all_channels_related_business_id_before_purchase like '%agents%'                                 as agent_channel,
       business_id,
       tracking_id
--marketing_cob_group = 'Food & beverage' as food_bev_lead,
--count(distinct related_business_id)
from dwh.daily_activities
where start_date >= '2023-09-15'
limit 10000

select *
from dwh.daily_activities
where event_date >= '2023-05-01'
limit 10

--QTP and ASP for pre/post (pre post) post launch post-launch analysis (sub out classes and dates)
With base as (SELECT distinct qpm.creation_time::date                                           as quote_day,
                              qpm.business_id,
                              qpm.highest_policy_id,
                              qpm.highest_policy_status,
                              qpm.highest_yearly_premium,
                              CASE
                                  WHEN cob = 'Fitness Studio' THEN 'Fitness Studio'
                                  WHEN cob in
                                       ('Personal Trainer', 'Fitness Instructor', 'Yoga Instructor', 'Dance Instructor',
                                        'Zumba Instructor', 'Pilates Instructor', 'Indoor Cycling Instructor',
                                        'Crossfit Instructor', 'Aerobics Instructor', 'Health and Wellness Coaching')
                                      THEN 'Independent Fitness Contractors COBs'
                                  ELSE 'Other Fitness'
                                  END                                                           as fitness,
                              CASE WHEN creation_time < '2023-02-14' THEN 'pre' ELSE 'post' END as pre_post
              FROM dwh.quotes_policies_mlob qpm
              WHERE qpm.lob_policy IN ('GL')
                and qpm.creation_time between '2023-01-24' and '2023-03-07'
                and qpm.offer_flow_type = 'APPLICATION'
                and qpm.cob_group in ('Fitness', 'Sports & fitness'))
SELECT pre_post,
       fitness,
       average_purchased_premium,
       purchases / quotes::decimal(10, 2) as qtp
FROM (SELECT pre_post,
             fitness,
             AVG(CASE WHEN highest_policy_status >= 3 then highest_yearly_premium END) as average_purchased_premium,
             count(distinct business_id)                                               as quotes,
             SUM(CASE WHEN highest_policy_status >= 3 then 1 ELSE 0 END)               as purchases
      from base
      group by 1, 2)


--get decline reasons with qre json args
select qdata.decline_reasons,
       extract(year from qdata.offer_creation_time) || '-' ||
       right('00' + convert(varchar, extract(month from qdata.offer_creation_time)), 2) as creation_year_month,
       count(distinct qdata.business_id)
from dwh.underwriting_quotes_data qdata
         left join dwh.quotes_policies_mlob qpm
                   on qpm.business_id = qdata.business_id
where qdata.start_date >= '2022-01-01'
  and qdata.lob = 'GL'
  and qpm.cob = 'Restaurant'
  and qdata.business_id <> '""'
  and qdata.execution_status = 'DECLINE' --and qpm.distribution_channel = 'agents'
group by 1, 2

--decline reasons for specific set of restaurant policies
select extract(year from creation_time) || '-' ||
       right('00' + convert(varchar, extract(month from creation_time)), 2) as creation_year_month,
       distribution_channel,
       count(distinct business_id)
from dwh.quotes_policies_mlob
where cob = 'Restaurant'
  and creation_time >= '2024-01-01'
  and lob_policy = 'GL'
  and
  --highest_policy_status >= 4 and
    offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and distribution_channel in ('agents', 'sem')
  and revenue_in_12_months >= 4000000
group by 1, 2

--to get all active legalZoom policies
select distinct business_id, annual_premium_accum, affiliate_name, lob, cob_name, cob_group, state
from reporting.gaap_snapshots_asl
where affiliate_name = 'LegalZoom'
  and status_name = 'Active'
  and lob = 'GL'
  and trans = 'Unearned premium'

--to get all active legalZoom policies with package name
select distinct gaap.business_id,
                qpm.highest_yearly_premium,
                gaap.affiliate_name,
                gaap.cob_name,
                gaap.cob_group,
                gaap.state,
                qpm.highest_status_package,
                qpm.num_of_employees,
                qpm.revenue_in_12_months
from reporting.gaap_snapshots_asl gaap
         join dwh.quotes_policies_mlob qpm on gaap.business_id = qpm.business_id and
                                              gaap.annual_premium_accum = qpm.highest_yearly_premium and
                                              gaap.affiliate_name = 'LegalZoom' and
                                              qpm.highest_policy_status >= 4 and
                                              qpm.offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE') and
                                              gaap.lob = 'GL' and
                                              gaap.trans = 'Unearned premium'

--hartford competitiveness automation restaurant book
select business_id,
       json_extract_path_text(json_args, 'zip_code', true)                                                            as zip,
       revenue_in_12_months                                                                                           as sales,
       json_extract_path_text(json_extract_path_text(json_args, 'lob_app_json', true), 'liquor_sales_exposure',
                              true)                                                                                   as liq_sales_pct,
       json_extract_path_text(json_args, 'years_in_business_num', true)                                               as yib,
       num_of_employees,
       json_extract_path_text(json_args, 'num_of_owners', true)                                                       as num_owners,
       state,
       json_args
from dwh.quotes_policies_mlob
where --highest_policy_status = 4 and
    lob_policy = 'GL'
  and cob = 'Restaurant'
  and state in ('MA', 'FL', 'OR')
  and offer_flow_type in ('APPLICATION')
limit 100

select json_args
from dwh.quotes_policies_mlob
where business_id = 'cfe2bf84841952032f6f94fe4566dcdd'

--median square footage of a MA restaurant on our in-force book
select quotes_policies_mlob.revenue_in_12_months,
       num_of_employees + json_extract_path_text(json_args, 'num_of_owners', true)                             as num_employees_owners,
       json_extract_path_text(json_extract_path_text(json_args, 'lob_app_json', true), 'square_footage', true) as sqft
from dwh.quotes_policies_mlob
where highest_policy_status >= 4
  and lob_policy = 'CP'
  and cob = 'Restaurant'

--in-force accountants book by LOB
select lob_policy, sum(highest_yearly_premium), count(distinct business_id)
from dwh.quotes_policies_mlob
where highest_policy_status >= 4
  and
  --lob_policy = 'CP' and
    cob = 'Accountant'
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
group by 1

--to get largest F&B policies from last week
select distinct business_id,
                policy_start_date,
                lob_policy,
                revenue_in_12_months,
                cob,
                distribution_channel,
                state,
                highest_yearly_premium--, highest_status_package
from dwh.quotes_policies_mlob
where highest_policy_status = 4
  and
  --distribution_channel = 'agents' and
  --lob_policy = 'GL' and
    offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and creation_time >= '2023-04-03'
  and cob_group = 'Food & beverage'
  and
  --(cob = 'Restaurant' or cob_group = 'Retail') and
    revenue_in_12_months <> ''
order by highest_yearly_premium desc
limit 30

--to get monthly distribution of >2M limit in release states/COBs
select
    --extract(year from creation_time) || '-' || right('00'+convert(varchar,extract(week from creation_time)),2) as creation_year_week,
    --creation_time::date,
    date_trunc('month', creation_time),
    count(distinct business_id) as policy_count
from dwh.quotes_policies_mlob
where highest_policy_status >= 4
  and highest_policy_occurence_limit >= 2000000
  and distribution_channel = 'agents'
  and lob_policy = 'GL'
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and creation_time >= '2022-12-20'
  and state in ('AL', 'AZ', 'CO', 'FL', 'MI', 'MN', 'NV', 'SC', 'TN', 'TX', 'UT')
  and cob in
      ('Restaurant', 'E-Commerce', 'Retail Stores', 'Grocery Store', 'Clothing Store', 'Electronics Store', 'Florist',
       'Jewelry Store', 'Sporting Goods Retailer', 'Tailors, Dressmakers, and Custom Sewers',
       'Nurseries and Gardening Shop', 'Candle Store', 'Pet Stores', 'Paint Stores', 'Flea Markets',
       'Arts and Crafts Store', 'Eyewear and Optician Store', 'Hardware Store', 'Discount Store', 'Pawn Shop',
       'Hobby Shop', 'Beach Equipment Rentals', 'Furniture Rental', 'Packing Supplies Store', 'Horse Equipment Shop',
       'Demonstrators and Product Promoters', 'Fabric Store', 'Lighting Store', 'Luggage Store', 'Bike Rentals',
       'Bike Shop', 'Bookstore', 'Home and Garden Retailer', 'Newspaper and Magazine Store', 'Department Stores',
       'Furniture Store', 'Wholesalers')
group by 1
order by 1 asc

--to get active PL policies in financial classes (aggregate)
select count(distinct business_id), sum(highest_yearly_premium)
from dwh.quotes_policies_mlob
where (highest_policy_status = 4 or highest_policy_status = 7)
  and lob_policy = 'PL'
  and cob in ('Securities, Commodities, and Financial Services Sales Agents', 'Financial Adviser')

--to get active PL policies in financial classes
select business_id, highest_yearly_premium, policy_end_date
from dwh.quotes_policies_mlob
where (highest_policy_status = 4 or highest_policy_status = 7)
  and lob_policy = 'PL'
  and cob in ('Securities, Commodities, and Financial Services Sales Agents', 'Financial Adviser')

--nib pre-post
With base as (SELECT distinct qpm.creation_time::date                                           as quote_day,
                              qpm.business_id,
                              qpm.highest_policy_id,
                              qpm.highest_policy_status,
                              qpm.highest_yearly_premium,
                              json_extract_path_text(json_extract_path_text(json_args, 'lob_app_json', true),
                                                     'payroll_in_12_months', true)              as payroll,
                              json_extract_path_text(json_extract_path_text(json_args, 'lob_app_json', true),
                                                     'run_payroll_date', true)                  as payroll_date,
                              json_extract_path_text(json_extract_path_text(json_args, 'lob_app_json', true),
                                                     'started_operations', true)                as started_ops,
                              json_extract_path_text(json_extract_path_text(json_args, 'prospects_json', true),
                                                     'years_in_business', true)                 as yib,
                              revenue_in_12_months,
                              CASE WHEN creation_time < '2022-12-11' THEN 'pre' ELSE 'post' END as pre_post,
                              CASE WHEN payroll = 0 THEN '=0' ELSE '>0' END                     as payroll_category
              FROM dwh.quotes_policies_mlob qpm
              WHERE qpm.lob_policy IN ('GL')
                and qpm.creation_time between '2022-09-04' and '2023-03-19'
                and qpm.offer_flow_type = 'APPLICATION'
                and qpm.cob = 'Restaurant'
                and yib = 'yib_less_than_1'
                and revenue_in_12_months <> ''
                and payroll <> '')
SELECT pre_post,
       --average_purchased_premium,
       quotes,
       payroll_category
--avg_payroll,
--avg_revenue,
--purchases/quotes::decimal(10,2) as qtp
FROM (SELECT pre_post,
             payroll_category,
             --AVG(CASE WHEN highest_policy_status >= 3 then highest_yearly_premium END) as average_purchased_premium,
             count(distinct business_id) as quotes
      --SUM(CASE WHEN highest_policy_status >= 3 then 1 ELSE 0 END) as purchases,
      --avg(CASE WHEN highest_policy_status >= 3 then payroll END) as avg_payroll,
      --avg(CASE WHEN highest_policy_status >= 3 then revenue_in_12_months END) as avg_revenue
      from base
      group by 1, 2)


--WC BDL
select u.data_points,
       qpm.creation_time
from underwriting_svc_prod.applicant_data u
         join dwh.quotes_policies_mlob qpm
              on u.prospect_id = u.prospect_id
where qpm.creation_time >= '2023-03-22'
  and qpm.lob_policy = 'WC'
  and qpm.offer_flow_type = 'APPLICATION'
  and qpm.highest_policy_status >= 3
limit 100

select cob, count(distinct business_id)
from dwh.quotes_policies_mlob
where cob_group = 'Consulting'
  and highest_policy_status >= 4
  and offer_flow_type in ('APPLICATION')
  and lob_policy = 'GL'
  and creation_time >= '2022-06-01'
group by 1
order by 2 desc

--BDL business description for WC test
WITH ranked_data AS (SELECT bad.lob_application_id,
                            la.opportunity_id,
                            p.business_id,
                            bad.creation_time,
                            bad.data_point_value,
                            la.answers,
                            o.lead_answers,
--      qpm.purchase_date,
--		qpm.cob,
--		qpm.cob_group,
--		qpm.json_args,
--		qpm.highest_status_name,
--		qpm.highest_policy_reference,
--      qpm.distribution_channel
                            ROW_NUMBER() OVER (PARTITION BY la.opportunity_id ORDER BY bad.creation_time DESC) AS rn
                     FROM underwriting_svc_prod.bi_applications_data bad
                              JOIN underwriting_svc_prod.lob_applications la
                                   ON la.lob_application_id = bad.lob_application_id
                              JOIN underwriting_svc_prod.prospects p on p.prospect_id = la.prospect_id
                              JOIN underwriting_svc_prod.opportunities o on o.opportunity_id = la.opportunity_id
                              LEFT JOIN dwh.quotes_policies_mlob qpm on qpm.business_id = p.business_id
                     WHERE data_point_id = 'business_description_of_operations'
                       and qpm.lob_policy = 'WC')
SELECT r.*
FROM ranked_data r
WHERE rn = 1;

--throwaway for marketing
select cob, trunc(avg(highest_yearly_premium) / 12, 2) as avg_monthly_premium
from dwh.quotes_policies_mlob
where --highest_policy_status = 4 and
    cob in ('Insurance Agent', 'Real Estate Agent', 'Notary', 'Accountant', 'Home Inspectors')
  and lob_policy = 'PL'
group by 1

--WC BDL text via json_args in qpm
select distinct business_id,
                lob_policy,
                cob,
                json_extract_path_text(json_extract_path_text(json_args, 'lob_app_json', true),
                                       'business_description_of_operations', true) as bdl_ops
from dwh.quotes_policies_mlob
where creation_time >= '2023-05-01'
  and business_id = 'e57516f77d5723d04c3f4f7ee9899fc9'

--hartford competitiveness GL competitor price analysis comparison
with qpm as (select case
                        when highest_status_package = 'basic'
                            then basic_quote_job_id -- these rows are necessary to create the join on job_id with rates
                        when highest_status_package = 'basicTria' then basic_tria_quote_job_id
                        when highest_status_package = 'pro' then pro_quote_job_id
                        when highest_status_package = 'proTria' then pro_tria_quote_job_id
                        when highest_status_package = 'proPlus' then pro_plus_quote_job_id
                        when highest_status_package = 'proPlusTria' then pro_plus_tria_quote_job_id
                        else pro_quote_job_id
                        end as quote_job_id,
                    business_id,
                    creation_time,
                    policy_start_date,
                    json_args,
                    highest_status_package,
                    highest_policy_status,
                    lob_policy,
                    current_amendment,
                    distribution_channel,
                    cob,
                    revenue_in_12_months,
                    num_of_employees,
                    state,
                    highest_yearly_premium
             from dwh.quotes_policies_mlob
             where creation_time >= '2023-01-01'
               and lob_policy = 'GL'
               and state in ('FL', 'MA', 'OR')),
     rates as (select *
               from s3_operational.rating_svc_prod_calculations
               where creation_time >= '2021-03-01'
                 and state in ('FL', 'MA', 'OR')
                 and lob = 'GL')
select distinct qpm.business_id,
                json_extract_path_text(qpm.json_args, 'zip_code', true)              as zip,
                qpm.revenue_in_12_months                                             as sales,
                json_extract_path_text(json_extract_path_text(qpm.json_args, 'lob_app_json', true),
                                       'liquor_sales_exposure', true)                as liq_sales_pct,
                json_extract_path_text(json_extract_path_text(qpm.json_args, 'lob_app_json', true), 'restaurant_type',
                                       true)                                         as rest_type,
                json_extract_path_text(qpm.json_args, 'years_in_business_num', true) as yib,
                qpm.num_of_employees,
                json_extract_path_text(qpm.json_args, 'num_of_owners', true)         as num_owners,
                qpm.state,
                qpm.json_args,
                qpm.creation_time,
                qpm.policy_start_date,
                qpm.current_amendment,
                qpm.cob,
                qpm.highest_policy_status,
                qpm.highest_yearly_premium,
                qpm.distribution_channel,
                rates.calculation,
                rates.rating_result
from qpm
         left join rates
                   on qpm.quote_job_id = rates.job_id
where qpm.lob_policy = 'GL'
  and qpm.cob = 'Restaurant'
  and qpm.creation_time >= '2023-01-01'
  and qpm.state in ('FL', 'MA', 'OR')
  and qpm.highest_status_package = 'proPlus'
  and rates.calculation <> ''
limit 500

--LegalZoom data request
--qtp by month (consistent with 2-dim QTP) for LegalZoom policies
with t1 as (select last_day(creation_time)             as month,
                   count(distinct related_business_id) as sold_policy_count
            from dwh.quotes_policies_mlob
            where highest_policy_status >= 3
              and lob_policy = 'GL'
              and cob_group = 'Consulting'
              and new_reneweal = 'new'
              and creation_time >= '2022-01-01'
              and affiliate_id = 8500
            group by 1
            order by month asc),
     t2 as (select last_day(creation_time)             as month,
                   count(distinct related_business_id) as quote_count
            from dwh.quotes_policies_mlob
            where lob_policy = 'GL'
              and cob_group = 'Consulting'
              and new_reneweal = 'new'
              and creation_time >= '2022-01-01'
              and affiliate_id = 8500
            group by 1
            order by month asc)
select *, cast(sold_policy_count * 1.0 / quote_count * 1.0 as decimal(10, 4)) as qtp
from t1
         join t2 on t1.month = t2.month
order by t1.month asc

--LZ declines legal zoom legalzoom
WITH t1 as (select distinct business_id,
                            decline_reasons
            from dwh.underwriting_quotes_data
            where start_date >= '2022-01-01'
              and lob = 'GL'
    --execution_status = 'DECLINE'
)
select distinct da.business_id, decline_reasons
from dwh.daily_activities da
         join t1
              on t1.business_id = da.business_id
where da.start_date >= '2022-01-01'
  and da.lob = 'GL'
  and da.marketing_cob_group = 'Consulting'
  and da.source_json_first_related_business_id like '%legalzoom%'

--legalzoom quote args
select business_id,
       highest_status_name,
       revenue_in_12_months,
       json_extract_path_text(json_args, 'num_of_owners', true)                                                     as num_owners,
       num_of_employees,
       business_ownership_structure,
       payroll_in_next_12_months,
       policy_start_date,
       creation_time,
       json_extract_path_text(json_extract_path_text(json_args, 'lob_app_json', true), 'claims_last_3_years',
                              true)                                                                                 as prior_claims,
       json_extract_path_text(json_extract_path_text(json_args, 'lob_app_json', true), 'coverage_start',
                              true)                                                                                 as start_date
from dwh.quotes_policies_mlob
where affiliate_id = 8500
  and creation_time >= '2022-01-01'
  and lob_policy = 'GL'
  and cob_group = 'Consulting'
  and highest_status_name = 'Quote'
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
limit 10000


--all policies written by bolt
select cob_name,
       marketing_cob_group,
       sum(yearly_premium),
       count(distinct policy_id)
from db_data_science.v_all_agents_policies
where agency_name = 'Bolt Insurance Agency'
  and lob = 'GL'
  and policy_status_name = 'Active'
--cob_segments = 'Other COB'
group by 1, 2
order by 3 desc

--counts of WC monoline, GL monoline, WC+GL, and other for AP Intego (excl. Next Connect) - MLOB
SELECT COUNT(DISTINCT CASE
                          WHEN lob_policy = 'WC' AND highest_policy_status = 4 AND distribution_channel = 'Next Connect'
                              AND business_id NOT IN (SELECT business_id
                                                      FROM dwh.quotes_policies_mlob
                                                      WHERE lob_policy = 'GL'
                                                        AND highest_policy_status = 4
                                                        AND distribution_channel = 'Next Connect')
                              THEN business_id END) AS wc_only,
       COUNT(DISTINCT CASE
                          WHEN lob_policy = 'GL' AND highest_policy_status = 4 AND distribution_channel LIKE '%ap-%'
                              AND business_id NOT IN (SELECT business_id
                                                      FROM dwh.quotes_policies_mlob
                                                      WHERE lob_policy = 'WC'
                                                        AND highest_policy_status = 4
                                                        AND distribution_channel = 'Next Connect')
                              THEN business_id END) AS gl_only,
       COUNT(DISTINCT CASE
                          WHEN lob_policy IN ('WC', 'GL') AND highest_policy_status = 4 AND
                               distribution_channel = 'Next Connect'
                              AND business_id IN (SELECT business_id
                                                  FROM dwh.quotes_policies_mlob
                                                  WHERE lob_policy = 'WC'
                                                    AND highest_policy_status = 4
                                                    AND distribution_channel = 'Next Connect')
                              AND business_id IN (SELECT business_id
                                                  FROM dwh.quotes_policies_mlob
                                                  WHERE lob_policy = 'GL'
                                                    AND highest_policy_status = 4
                                                    AND distribution_channel = 'Next Connect')
                              THEN business_id END) AS both_wc_and_gl,
       COUNT(DISTINCT business_id)                  AS total_distinct_ids
FROM dwh.quotes_policies_mlob
WHERE highest_policy_status = 4
  AND distribution_channel = 'Next Connect'

--policy list for WC+GL policies for AP Intego - MLOB
SELECT DISTINCT business_id
FROM dwh.quotes_policies_mlob
WHERE lob_policy IN ('WC', 'GL')
  AND highest_policy_status = 4
  AND distribution_channel = 'Next Connect'
  and policy_start_date >= '2023-03-01'
  AND business_id IN (SELECT business_id
                      FROM dwh.quotes_policies_mlob
                      WHERE lob_policy = 'WC'
                        AND highest_policy_status = 4
                        AND distribution_channel = 'Next Connect')
  AND business_id IN (SELECT business_id
                      FROM dwh.quotes_policies_mlob
                      WHERE lob_policy = 'GL'
                        AND highest_policy_status = 4
                        AND distribution_channel = 'Next Connect')

--lists all LOBs on a quote (note: works for MLOB but not cross-sell, which is a separate row)
select business_id, lobs
from dwh.active_users
where business_id = 'b4c79c8ab1600134f3131d09a11931a6'
limit 100

--find businesses with both active WC PAYG and GL policies
SELECT business_id, marketing_source
FROM dwh.quotes_policies_mlob
WHERE ((lob_policy = 'WC' AND
        json_extract_path_text(json_extract_path_text(json_args, 'lob_app_json', true), 'paygo_frequency', true) <>
        '') OR lob_policy = 'GL')
  AND highest_policy_status = 4
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
GROUP BY 1, 2
HAVING COUNT(DISTINCT lob_policy) = 2
limit 1000

--count active WC paygo policies
SELECT count(distinct business_id)
FROM dwh.quotes_policies_mlob
WHERE --(lob_policy = 'WC' AND json_extract_path_text(json_extract_path_text(json_args,'lob_app_json',true),'paygo_frequency',true) <> '') AND
    lob_policy = 'WC'
  and highest_policy_status = 4
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')

--count active WC+GL policies (no paygo filter)
SELECT COUNT(DISTINCT business_id)
FROM dwh.quotes_policies_mlob
WHERE lob_policy = 'GL'
  AND highest_policy_status = 4
  AND business_id IN (SELECT business_id
                      FROM dwh.quotes_policies_mlob
                      WHERE lob_policy = 'PL'
                        AND highest_policy_status = 4);

SELECT COUNT(DISTINCT business_id)
FROM dwh.quotes_policies_mlob
WHERE lob_policy = 'GL'
  AND highest_policy_status = 4

--list of live COBs
select state_code
     , lob
     , cob_name
     , marketing_cob_group
     , cob_industry
     , update_time
     , update_message
from portfolio_svc_prod.permitted_cobs_states_lobs
         inner join portfolio_svc_prod.cobs using (cob_id)
         inner join dwh.sources_test_cobs using (cob_name)
where permitted = 1

--allied health sizing
select cob, sum(highest_yearly_premium), count(distinct (business_id))
from dwh.quotes_policies_mlob
where lob_policy = 'PL'
  and highest_policy_status = 4
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and cob in
      ('Speech Language Pathologists', 'Marriage and Family Therapists', 'Mental Health Counselors', 'Psychologists',
       'CPR and First Aid Training', 'Home Health Aides', 'Home Health Care', 'Personal Care Aides',
       'Personal Care Services', 'Occupational Health and Safety Specialists', 'Occupational Therapists')
group by 1
order by 2 desc

--grab all auto recently expired policies in remainder of states
select distinct business_id, state
from dwh.quotes_policies_mlob
where lob_policy = 'CA'
  and highest_policy_status = 6
  and policy_end_date >= '2023-03-01'
  and state in
      ('IN', 'KS', 'KY', 'MD', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NM', 'NV', 'OH', 'OK', 'OR', 'PA', 'SC', 'TN',
       'TX', 'UT', 'WI', 'WV', 'WY')

--to get TPM exposure updates by business ID
select *
from riskmgmt_svc_prod.exposure_base_revenue_results
where business_id = 'db5e963cf7a7bca5b010a12f1e381599'

--grab all auto recently expired policies in remainder of states
select state, count(distinct business_id) as PIF, sum(highest_yearly_premium) as premIF
from dwh.quotes_policies_mlob
where lob_policy = 'GL'
  and cob = 'Restaurant'
  and highest_policy_status = 4
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
group by 1
order by 3 desc

--get user search input in funnel
select json_extract_path_text(interaction_data, 'data', true) as keyword, count(distinct tracking_id)
from user_interactions_prod.user_interactions_prod
where name = 'cobs_search_request'
  and placement = 'cobs_search'
  and dateid >= 20230417
  and dateid < 20230420
group by 1

--AS&R premium distribution
select lob_policy, cob, highest_yearly_premium
from dwh.quotes_policies_mlob
where highest_policy_status = 4
  and cob_group = 'Real estate services'
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')

select affiliate_id, affiliate_name, count(distinct (business_id))
from reporting.gaap_snapshots_asl
where affiliate_name <> ''
group by 1, 2
order by 3 desc

--new bpp amounts
select business_id,
       json_extract_path_text(json_extract_path_text(json_args, 'lob_app_json', true),
                              'bpp_building_improvements_and_remodeling', true)                                       as bldg_improvements,
       json_extract_path_text(json_extract_path_text(json_args, 'lob_app_json', true), 'bpp_kitchen_equipment',
                              true)                                                                                   as kitchen_eqt,
       json_extract_path_text(json_extract_path_text(json_args, 'lob_app_json', true), 'bpp_other',
                              true)                                                                                   as bpp_other,
       json_extract_path_text(json_extract_path_text(json_args, 'lob_app_json', true), 'bpp_tables_and_seating',
                              true)                                                                                   as tables_seating,
       json_extract_path_text(json_extract_path_text(json_args, 'lob_app_json', true), 'bpp_pos_system_cash_register',
                              true)                                                                                   as pos_system,
       json_args,
       highest_status_name,
       distribution_channel
from dwh.quotes_policies_mlob
where lob_policy = 'CP'
  and creation_time >= '2023-03-23'
  and cob = 'Restaurant'

--delete
--QTP and ASP for pre/post (pre post) post launch post-launch analysis (sub out classes and dates)
With base as (SELECT distinct qpm.creation_time::date                                           as quote_day,
                              qpm.business_id,
                              qpm.highest_policy_id,
                              qpm.highest_policy_status,
                              qpm.highest_yearly_premium,
                              CASE WHEN creation_time < '2023-01-01' THEN 'pre' ELSE 'post' END as pre_post
              FROM dwh.quotes_policies_mlob qpm
              WHERE qpm.lob_policy IN ('CA')
                and qpm.creation_time between '2022-01-01' and '2023-05-04'
                and qpm.offer_flow_type = 'APPLICATION'
    --and qpm.cob_group in ('Construction')
)
SELECT pre_post,
       average_purchased_premium,
       purchases / quotes::decimal(10, 2) as qtp
FROM (SELECT pre_post,
             AVG(CASE WHEN highest_policy_status >= 3 then highest_yearly_premium END) as average_purchased_premium,
             count(distinct business_id)                                               as quotes,
             SUM(CASE WHEN highest_policy_status >= 3 then 1 ELSE 0 END)               as purchases
      from base
      group by 1)

--to get limits distribution
select count(highest_policy_occurence_limit) as policy_count,
       extract(month from policy_start_date) as month,
       highest_policy_occurence_limit,
       highest_policy_aggregate_limit
from dwh.quotes_policies_mlob
where highest_policy_status = 4
  and lob_policy = 'GL'
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and cob_group = 'Construction'
  and creation_time >= '2022-12-20'
group by highest_policy_occurence_limit, highest_policy_aggregate_limit, month

--find business_id from failed address validation list (agents)
select eventtime, aa.tracking_id, related_business_id, business_id, funnelphase, placement
from dwh.all_activities_table aa
         join dwh.sources_attributed_table ss on aa.tracking_id = ss.tracking_id
where aa.tracking_id = '0fe6aa952f6abab610fd5a042cda2dd6'

--to get top declines by COB, channel and LOB
select cob,
       (CASE
            WHEN (affiliate_id = 'N/A' and agent_id = 'N/A') then 'direct'
            WHEN (affiliate_id <> 'N/A' and agent_id = 'N/A') then 'affiliate'
            else 'agent' end)        as channel,
       uw.lob,
       decline_reasons,
       count(distinct (business_id)) as biz_count
from dwh.underwriting_quotes_data uw
         join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
where execution_status = 'DECLINE'
  and decline_reasons not like '%","%'
  and offer_creation_time >= '2023-01-01'
  and offer_creation_time <= '2023-03-31'
  and uw.lob = 'IM'
group by 1, 2, 3, 4
order by biz_count desc

--QTP and ASP for pre/post (pre post) post launch post-launch analysis (overpriced 5/3/2023)
With base as (SELECT distinct qpm.creation_time::date                                           as quote_day,
                              qpm.business_id,
                              qpm.highest_policy_id,
                              qpm.highest_policy_status,
                              qpm.highest_yearly_premium,
                              CASE WHEN creation_time < '2023-05-03' THEN 'pre' ELSE 'post' END as pre_post,
                              CASE
                                  WHEN cob in ('Pressure Washing', 'Accountant', 'Real Estate Brokers', 'Notary',
                                               'Plastering or Stucco Work', 'Travel Agency', 'Bike Rentals',
                                               'Building Sign installation', 'Business Financing', 'Title Loans',
                                               'Legal Service', 'Real Estate Agent', 'Financial Adviser',
                                               'Insurance Agent', 'Claims Adjuster', 'Real Estate Investor')
                                      THEN 'overpriced'
                                  ELSE 'underpriced' END                                        as pricing_group
              FROM dwh.quotes_policies_mlob qpm
              WHERE qpm.lob_policy IN ('GL')
                and qpm.creation_time between '2023-04-01' and '2023-05-12'
                and qpm.offer_flow_type = 'APPLICATION'
                and qpm.cob in
                    ('Pressure Washing', 'Accountant', 'Real Estate Brokers', 'Notary', 'Plastering or Stucco Work',
                     'Travel Agency', 'Bike Rentals', 'Building Sign installation', 'Business Financing', 'Title Loans',
                     'Legal Service', 'Real Estate Agent', 'Financial Adviser', 'Insurance Agent', 'Claims Adjuster',
                     'Real Estate Investor'))
SELECT pre_post,
       pricing_group,
       average_purchased_premium,
       purchases / quotes::decimal(10, 2) as qtp
FROM (SELECT pricing_group,
             pre_post,
             AVG(CASE WHEN highest_policy_status >= 3 then highest_yearly_premium END) as average_purchased_premium,
             count(distinct business_id)                                               as quotes,
             SUM(CASE WHEN highest_policy_status >= 3 then 1 ELSE 0 END)               as purchases
      from base
      group by 1, 2
      order by pricing_group)

--to get GL policies that should have been issued on GL4
---GL4.3 SNIC states with 3/7/23 nb eff date
select business_id, current_amendment, policy_start_date, creation_time, distribution_channel
from dwh.quotes_policies_mlob
where offer_flow_type = 'APPLICATION'
  and lob_policy = 'GL'
  and highest_policy_status >= 3
  and policy_start_date >= '2023-03-07'
  and state in ('KS', 'IA', 'SD', 'ID', 'VT', 'ME')
  and current_amendment not in ('{"version": 4.0, "amendmentId": "SNIC_BMGL_4_new_policy"}')
limit 1000

--to get top LOBs by cob_group
select cob_group, lob_policy, count(distinct (business_id)) as active_policies
from dwh.quotes_policies_mlob
where highest_policy_status >= 3
group by 1, 2
order by 1 asc

--to get top COBs in a cob_group
select cob_group, cob, count(distinct (business_id)) as active_policies
from dwh.quotes_policies_mlob
where highest_policy_status >= 3
group by 1, 2
order by 1 asc

--pull list of leads by COB by month
select extract(year from event_date) || '-' ||
       right('00' + convert(varchar, extract(month from event_date)), 2)              as event_year_month,
       cob_name,
       marketing_cob_group,
       json_extract_path_text(source_json_first_related_business_id, 'channel', true) as dist_channel,
       count(distinct business_id)                                                    as num_businesses
from dwh.daily_activities
where start_date >= '2020-01-01'
  and lead = 1
group by 1, 2, 3, 4
order by 1 asc

--hazard group custom query
With PayG as (select distinct flow, offer_id
              from (select distinct 'BOOKROLL' as Flow, I.offer_id /*Bookrolling quotes*/
                    from s3_operational.mysql_underwriting_interface_prod_bi_underwriting_quotes_data I
                             join ap_intego_svc_prod.apintego_bookrolling_quote_data_responses BR
                                  on I.request_id = BR.uw_request_identifier
                    UNION
                    select distinct EQ.flow_source_type as flow, I.offer_id
                    from s3_operational.mysql_underwriting_interface_prod_bi_underwriting_quotes_data I
                             join ap_intego_svc_prod.apintego_early_quote_data_responses FQR
                                  on I.ap_intego_early_quote_application_id = FQR.uw_request_identifier
                             join ap_intego_svc_prod.apintego_early_quote_data EQ
                                  on EQ.request_unique_identifier = FQR.early_quote_request_id
                    UNION
                    select distinct EQ.flow_source_type as flow, I.offer_id
                    from s3_operational.mysql_underwriting_interface_prod_bi_underwriting_quotes_data I
                             join ap_intego_svc_prod.apintego_final_quote_data_responses FQR
                                  on I.ap_intego_early_quote_application_id = FQR.uw_request_identifier
                             join ap_intego_svc_prod.apintego_early_quote_data EQ
                                  on EQ.request_unique_identifier = FQR.early_quote_request_id) p),
     UQD as (select offer_creation_time,
                    offer_flow_type,
                    execution_status,
                    Q.Agent_id,
                    Va.current_agencytype,
                    case when PAYG.Flow is null then 'Non-PayG' else PAYG.flow end as PAYG_FLOW,
                    Q.offer_id,
                    quote_job_id,
                    business_id,
                    policy_id,
                    policy_reference,
                    policy_status,
                    policy_status_name,
                    max(policy_status) over (partition by Q.offer_id)              as higheststatusID,
                    lob,
                    yearly_premium,
                    start_date::date                                               as polstartdate,
                    end_date::date                                                 as polenddate,
                    Q.state_code,
                    num_of_employees,
                    cob,
                    bundle_name,
                    year_business_started,
                    lob_application_id
             from dwh.underwriting_quotes_data Q
                      left join dwh.v_agents VA on Q.agent_id = VA.agent_id
                      left join PAYG on Q.offer_id = PAYG.offer_id
             where lob = 'WC'
               and offer_creation_time::date >>= '2022-03-01' /*and offer_creation_time::date << '2022-07-01'*/
               and offer_flow_type in ('APPLICATION')),
     ProPlusQuotes as (select *
                       from UQD
                       where higheststatusID = 1 and bundle_name = 'proPlus'
                          or bundle_name is null),
     Selected_Bound as (select *
                        from UQD
                        where higheststatusID >> 1
                          and policy_status = higheststatusID),
     QP as (select *
            from (select *,
                         row_number()
                         over (partition by business_id, date_trunc('day', offer_creation_time) order by offer_creation_time desc) as DailyLastRecord
                  from (Select *
                        from ProPlusQuotes
                        UNION
                        Select *
                        from Selected_Bound) UQD)
            where DailyLastRecord = 1),
     Subcost as (select distinct creation_time,
                                 LA.lob_application_id,
                                 QP.offer_id,
                                 nullif(json_extract_path_text(answers, 'subcontractor_cost_in_12_months', true),
                                        '')::numeric                                                                as Subcost,
                                 json_extract_path_text(answers, 'wc_subs_coverage_required', true)                 as SubCovReq,
                                 nullif(json_extract_path_text(answers, 'num_of_owners_std_v2', true),
                                        '')::numeric                                                                as NumofOwners,
                                 json_extract_path_text(answers, 'wc_previous_coverage', true)                      as wc_prev_cov,
                                 nullif(json_extract_path_text(answers, 'payroll_in_12_months', true),
                                        '')::float                                                                  as payrollin12mo,
                                 nvl(nullif(json_extract_path_text(answers, 'wc_subs_primary_activity_v3', true), ''),
                                     nullif(json_extract_path_text(answers, 'wc_subs_primary_activity', true),
                                            ''))                                                                    as subactivity,
                                 right(subactivity, 4)                                                              as sub_class
                 from underwriting_svc_prod.lob_applications LA
                          join QP on LA.lob_application_id = QP.lob_application_id
                 where LA.lob = 'WC'),
/*SubClass as (
    select qpm.offer_id, qpm.offer_flow_type, highest_policy_id, highest_policy_reference, state,json_args,
       creation_time, policy_start_date, policy_end_date,
       nullif(json_extract_path_text(json_args,'lob_app_json','subcontractor_cost_in_12_months',true),'')::float as subcost,
       json_extract_path_text(json_args,'lob_app_json','wc_subs_coverage_required',true) as subcovreq,
       nvl(nullif(json_extract_path_text(json_args, 'lob_app_json','wc_subs_primary_activity_v3',true),''),
       nullif(json_extract_path_text(json_args, 'lob_app_json','wc_subs_primary_activity',true),'')) as subactivity,
       right(subactivity,4) as sub_class,
        json_extract_path_text(json_args,'lob_app_json','num_of_owners_std_v2',true) as NumofOwners,
        json_extract_path_text(json_args,'lob_app_json','wc_previous_coverage',true) as wc_prev_cov
       from dwh.quotes_policies_mlob qpm
        join qp on qpm.offer_id = qp.offer_id
where lob_policy = 'WC'
and qpm.offer_flow_type = 'APPLICATION'
),*/
     RatingInfo as (select job_id,
                           json_extract_path_text(calculation_summary, 'lob specific', 'has_owners_coverage',
                                                  true)                                                         as HasOwnerCov,
                           nullif(json_extract_path_text(calculation_summary, 'lob specific', 'pure_premium', true),
                                  '')::float                                                                    as PurePrem,
                           nullif(json_extract_path_text(calculation_summary, 'lob specific', 'total_manual_premium',
                                                         true),
                                  '')::float                                                                    as TotManPrem,
                           nullif(json_extract_path_text(calculation_summary, 'lob specific',
                                                         'effective_minimum_premium', true),
                                  '')::float                                                                    as EffMinPrem,
                           nullif(json_extract_path_text(calculation_summary, 'lob specific',
                                                         'total_standard_premium_pre_balance_to_min', true),
                                  '')::float                                                                    as totstdprem_prebal2min,
                           nullif(json_extract_path_text(calculation_summary, 'lob specific', 'total_modified_premium',
                                                         true),
                                  '')::float                                                                    as totmodprem,
                           nullif(json_extract_path_text(calculation_summary, 'lob specific',
                                                         'experience_modification_factor', true),
                                  '')::float                                                                    as EMOD_factor,
                           nullif(json_extract_path_text(nullif(json_extract_path_text(calculation_summary,
                                                                                       'lob specific',
                                                                                       'governingClassResults', true),
                                                                ''),
                                                         regexp_substr(
                                                                 nullif(json_extract_path_text(calculation_summary,
                                                                                               'lob specific',
                                                                                               'governingClassResults',
                                                                                               true), ''), '\\d{4}'),
                                                         'hazard_group'),
                                  '')                                                                           as govHazGrp,
                           json_extract_path_text(data_points, 'dataPointsWithoutPackageData', 'lcmTier',
                                                  true)                                                         as LCMTier,
                           json_extract_path_text(data_points, 'dataPointsWithoutPackageData', 'discounts',
                                                  'discountIds',
                                                  true)                                                         as Discount,
                           nullif(split_part(
                                          json_extract_path_text(calculation, 'schedule_rating_calculation',
                                                                 'riskModifier_CalcLog', 'schedule_rating_uncapped',
                                                                 true), ' |', 1),
                                  '')::float                                                                    as SRuncapped,
                           nullif(
                                   json_extract_path_text(calculation, 'schedule_rating_calculation',
                                                          'schedule_rating_capped', true),
                                   '')::float                                                                   as SRcapped
                    from s3_operational.rating_svc_prod_calculations RC
                             join qp on RC.job_id = qp.quote_job_id),
     Class as (select *
               from (select a.job_id
                          , 'Gov'                                                                                   as Class
                          , regexp_substr(a.test, '\\d{4}')                                                         as ClassCode
                          , nullif(json_extract_path_text(a.test, ClassCode, 'dividedPayroll', true), '')::float *
                            100                                                                                     as Payroll
                          , nullif(json_extract_path_text(a.test, ClassCode, 'lost_cost', true),
                                   '')::float                                                                       as LossCost
                          , nullif(json_extract_path_text(a.test, ClassCode, 'lcm', true), '')::float               as lcm
                          , nullif(json_extract_path_text(a.test, ClassCode, 'hazard_group', true),
                                   '')                                                                              as HazGrp
                     from (select job_id,
                                  json_extract_path_text(calculation_summary, 'lob specific', 'governingClassResults',
                                                         true) as test
                           from s3_operational.rating_svc_prod_calculations RC
                                    join qp on RC.job_id = qp.Quote_job_id) a
                     UNION
                     select a.job_id
                          , 'Addl'                                                                      as Class
                          , regexp_substr(json_extract_array_element_text(a.test, n.num - 1), '\\d{4}') as ClassCode
                          , nullif(json_extract_path_text(
                                           json_extract_array_element_text(a.test, n.num - 1), ClassCode,
                                           'dividedPayroll', true), '')::float * 100                    as Payroll
                          , nullif(json_extract_path_text(
                                           json_extract_array_element_text(a.test, n.num - 1), ClassCode, 'lost_cost',
                                           true), '')::float                                            as LossCost
                          , nullif(json_extract_path_text(
                                           json_extract_array_element_text(a.test, n.num - 1), ClassCode, 'lcm', true),
                                   '')::float                                                           as LCM
                          , nullif(json_extract_path_text(
                                           json_extract_array_element_text(a.test, n.num - 1), ClassCode,
                                           'hazard_group', true), '')                                   as HazGrp
                     from (select job_id,
                                  '[' || replace(json_extract_path_text(calculation_summary, 'lob specific',
                                                                        'additionalClassResults', true), '},',
                                                 '}},{') || ']' as test
                           from s3_operational.rating_svc_prod_calculations RC
                                    join qp on RC.job_id = qp.Quote_job_id) a
                              join bi_workspace.numbers n
                                   on json_array_length(a.test) >>= n.num)
               where classCode != ''),
     ModOwnerPR as (select job_id,
                           count(distinct classcode)                                                             as OwnerClass_ct,
                           max(losscost)                                                                         as highestOwnerLC,
                           sum(ownerpr)                                                                          as totownerpr,
                           sum(ownerpr * losscost) / 100                                                         as OwnerPP,
                           sum(case
                                   when ClassCode in ('8810', '8871', '8742', '0953', '0951')
                                       then ownerpr end)                                                         as exemptownerpR
                    from (select O.*, C.LossCost
                          from (select job_id,
                                       json_extract_array_element_text(modifiedOwnerPR, n.num - 1) as OwnerPRjson,
                                       regexp_substr(OwnerPRjson, '\\d{4}')                        as ClassCode,
                                       nullif(json_extract_path_text(OwnerPRjson, regexp_substr(OwnerPRjson, '\\d{4}')),
                                              '')::float                                           as ownerpr
                                from (select job_id,
                                             '[' || replace(json_extract_path_text(calculation_summary, 'lob specific',
                                                                                   'total_modified_owners_payroll',
                                                                                   true), ',', '},{') ||
                                             ']' as modifiedOwnerPR
                                      from s3_operational.rating_svc_prod_calculations RC
                                               join qp on RC.job_id = qp.quote_job_id) A
                                         join bi_workspace.numbers n
                                              on json_array_length(a.modifiedOwnerPR) >>= n.num) O
                                   left join Class C on O.job_id = C.job_id and O.ClassCode = C.ClassCode)
                    group by 1),
     OfficerTypes as (select offer_id, officertype, nvl(count(*), 0) as coveredOwners
                      from (select job_id,
                                   offer_id,
                                   json_extract_array_element_text(employees, n.num - 1)             as employee,
                                   nullif(json_extract_path_text(employee, 'officerType', true), '') as OfficerType
                            from (select job_id,
                                         qp.offer_id,
                                         json_extract_path_text(data_points, 'dataPointsWithoutPackageData',
                                                                'employees', true) as employees
                                  from s3_operational.rating_svc_prod_calculations RC
                                           join qp on RC.job_id = qp.Quote_job_id) A
                                     join bi_workspace.numbers n
                                          on json_array_length(a.employees) >>= n.num)
                      where officerType is not null
                      group by 1, 2),
     TotEmployeePR as (select job_id,
                              count(distinct classcode)            as EmpClass_Ct,
                              max(losscost)                        as highestEmpLC,
                              sum(trueemployeepr)                  as TrueEmployee_PR,
                              sum(trueemployeepr * losscost) / 100 as EmployeePP,
                              sum(case
                                      when ClassCode in ('8810', '8871', '8742', '0953', '0951')
                                          then trueemployeepr end) as exemptEmppR
                       from (select E.*,
                                    C.LossCost,
                                    SC.subcost,
                                    SC.subcovreq,
                                    SC.sub_class,
                                    case
                                        when subcovreq = 'No' then employeepr - nvl(subcost, 0)
                                        else employeepr end as TrueEmployeePR
                             from (select job_id,
                                          offer_id,
                                          json_extract_array_element_text(totEmployeePR, n.num - 1) as EmployeePRJson,
                                          regexp_substr(EmployeePRJson, '\\d{4}')                   as ClassCode,
                                          nullif(json_extract_path_text(EmployeePRJson,
                                                                        regexp_substr(EmployeePRJson, '\\d{4}')),
                                                 '')::float                                         as EmployeePR
                                   from (select job_id,
                                                qp.offer_id,
                                                '[' || replace(
                                                        json_extract_path_text(calculation_summary, 'lob specific',
                                                                               'total_employees_payroll', true), ',',
                                                        '},{') || ']' as totEmployeePR
                                         from s3_operational.rating_svc_prod_calculations RC
                                                  join qp on RC.job_id = qp.quote_job_id) A
                                            join bi_workspace.numbers n
                                                 on json_array_length(a.totEmployeePR) >>= n.num) E
                                      Left join Class C on E.job_id = C.job_id and E.ClassCode = C.ClassCode
                                      Left join SubCost SC on E.offer_id = SC.offer_id and SC.sub_class = E.ClassCode)
                       Group by 1),
     HasRHR as (select distinct RHR.business_id
                from db_data_science.ncci_rhr_check RHR
                         join QP on RHR.business_id = QP.business_id and
                                    RHR.polstartdate_pr <<= QP.offer_creation_time::date),
     PRFL as (select distinct offer_id,
                              json_extract_path_text(json_extract_array_element_text(answers_override_data, 0), 'type',
                                                     True) as Payroll_Initiative,
                              nullif(json_extract_path_text(json_extract_array_element_text(answers_override_data, 0),
                                                            'biData', 'original', 'employees', 'total', true),
                                     '')::float            as Orig_Emp_PR,
                              nullif(json_extract_path_text(json_extract_array_element_text(answers_override_data, 0),
                                                            'biData', 'original', 'owners', 'total', true),
                                     '')::float            as Orig_owner_PR,
                              nullif(json_extract_path_text(json_extract_array_element_text(answers_override_data, 0),
                                                            'biData', 'original', 'subcontractors', 'total', true),
                                     '')::float            as Orig_SubC_PR,
                              nullif(json_extract_path_text(json_extract_array_element_text(answers_override_data, 0),
                                                            'biData', 'overridden', 'employees', 'total', true),
                                     '')::float            as Override_Emp_PR,
                              nullif(json_extract_path_text(json_extract_array_element_text(answers_override_data, 0),
                                                            'biData', 'overridden', 'owners', 'total', true),
                                     '')::float            as Override_owner_PR,
                              nullif(json_extract_path_text(json_extract_array_element_text(answers_override_data, 0),
                                                            'biData', 'overridden', 'subcontractors', 'total', true),
                                     '')::float            as Override_SubC_PR
              from dwh.underwriting_quotes_data
              where answers_override_data is not null),
     PRCL_Chg as (select p.policy_id,
                         p.policy_reference,
                         sum(case when change_type_id = 14 then 1 else 0 end) as PayrollChg,
                         sum(case when change_type_id = 30 then 1 else 0 end) as ClassCodeChg
                  from nimi_svc_prod.policy_changes pc
                           join nimi_svc_prod.policies p on pc.policy_id = p.policy_id
                  where p.policy_type_id = 6
                    and p.policy_reference is not null
                    and p.end_date >> p.start_date
                    and change_type_id in (14, 30)
                  group by 1, 2),
     final as (select QP.*,
                      case
                          when QP.offer_creation_time::date << '2022-7-20' then 'Pre'
                          when QP.offer_creation_time::date << '2022-10-6' then 'PRFlr_Ph1'
                          else 'PRFlr_Ph2&RHR_based' end                                                    as PayrollFloorLaunch,
                      ec.decline_reasons,
                      stc.marketing_cob_group                                                               as cob_group,
                      case when HasRHR.business_id is not null then 'Yes' else 'No' end                     as HasRHR,
                      case when (agent_id is null or agent_id = 'N/A') then 'Non-Agent' else 'Agent' end    as Channel,
                      S.Subcost,
                      S.SubCovReq,
                      S.NumofOwners,
                      S.wc_prev_cov,
                      OT.officerType,
                      OT.CoveredOwners,
                      OP.annual_payroll_min                                                                 as OPR_Min,
                      OP.annual_payroll_max                                                                 as OPR_Max,
                      R.HasOwnerCov,
                      R.EffMinPrem,
                      R.LCMTier,
                      R.Discount,
                      R.SRuncapped,
                      R.SRcapped,
                      R.govHazGrp,
                      CL.Calc_pureprem,
                      R.PurePrem,
                      R.TotManPrem,
                      R.totstdprem_prebal2min,
                      R.totmodprem,
                      nvl(PRCL_Chg.PayrollChg, 0)                                                           as PayrollChgCt,
                      nvl(PRCL_Chg.classcodechg, 0)                                                         as ClasscodechgCt,
                      case
                          when R.SRuncapped is null then 'a.unknown'
                          when R.SRuncapped << -0.1 then 'b. <<-0.1'
                          when R.SRuncapped << 0 then 'c. [-0.1to0)'
                          when R.SRuncapped <<= 0.1 then 'd. [0to0.1)'
                          when R.SRuncapped <<= 0.2 then 'e. [0.1to0.2)'
                          when R.SRuncapped <<= 0.3 then 'f. [0.2to0.3)'
                          else 'g.0.3+' end                                                                 as SRUncapped_grp,
                      CL.NumofClass,
                      CL.HazGrp,
                      nvl(CL.LCM, 0)                                                                        as LossCostMultiplier,
                      nvl(CL.rated_payroll, 0)                                                              as RatedPayroll,
                      S.payrollin12mo,
                      case when S.subcovreq = 'No' then S.subcost else 0 end                                as AppliedSubCost,
                      nvl(Mop.totownerpr, 0)                                                                as TotalModOwnerPR,
                      nvl(Mop.exemptownerPR, 0)                                                             as Exempt_OwnerPR,
                      nvl(Mop.OwnerPP, 0)                                                                   as Owner_PP,
                      Mop.highestOwnerLC,
                      nvl(TEP.TrueEmployee_PR, 0)                                                           as TotEmployeePR,
                      nvl(TEP.exemptEmppR, 0)                                                               as Exempt_EmpPR,
                      TEP.EmployeePP                                                                        as Employee_PP,
                      TEP.highestEmpLC,
                      calc_pureprem - employee_pp - owner_PP                                                as Sub_PP,
                      case
                          when (QP.state_code in
                                ('AL', 'CT', 'FL', 'GA', 'ID', 'LA', 'MD', 'ME', 'MS', 'NH', 'NM', 'SD', 'TX', 'UT',
                                 'VA', 'VT', 'WV', 'HI') and OT.OfficerType = 'PARTNER_SOLE_PROP')
                              or (QP.state_code in ('MI') and OT.OfficerType in ('PARTNER', 'PARTNERS'))
                              or (QP.state_code in
                                  ('AK', 'AR', 'NC', 'OK', 'OR', 'CO', 'IL', 'KS', 'KY', 'MA', 'MO', 'NE', 'SC',
                                   'WI') and OT.OfficerType in ('PARTNERS_SOLE_PROPS_EXEC_OFFICERS_MEMBERS_OF_LLC',
                                                                'PARTNERS_SOLE_PROPS_EXEC_OFFICERS_MEMBERS_OF_LLCS'))
                              or (QP.state_code in ('NV') and OT.OfficerType in ('PARTNER_SOLE_PROP_DEEMED_WAGE',
                                                                                 'PARTNER_SOLE_PROP_ELECTIVE_WAGE',
                                                                                 'PARTNER_SOLE_PROP_LICENSED_SUB'))
                              then 'Y'
                          else 'N' end                                                                      as OwnerPR_Deemed,
                      case
                          when cob_group in ('Construction', 'Artisan contractor', 'Cleaning') and HasRHR = 'No' and
                               Discount not ilike '%PAYGO%'
                              then 'Yes'
                          else 'No' end                                                                     as PayrollfloorEligible,
                      case
                          when cob_group in ('Construction', 'Artisan contractor') then 50000
                          when cob_group in ('Cleaning') then 30000 end                                     as PRFL_min,
                      case
                          when cob_group in ('Construction', 'Artisan contractor') then 30000
                          when cob_group in ('Cleaning')
                              then 25000 end                                                                as PRFL_perEmp,
                   /* Pre launch */
                      case
                          when PayrollfloorEligible = 'Yes' and num_of_employees >> '0'
                              then greatest(PRFL_min, PRFL_perEmp * QP.num_of_employees, TotEmployeePR)
                          else TotEmployeePR end                                                            as newEmployeePR,
                      case
                          when PayrollfloorEligible = 'Yes' and num_of_employees = '0' and OT.coveredOwners >> 0 and
                               OwnerPR_Deemed = 'N'
                              then greatest(PRFL_min, PRFL_perEmp * OT.coveredOwners, TotalModOwnerPR)
                          else TotalModOwnerPR end                                                          as newOwnerPR,
                      case
                          when PayrollfloorEligible = 'Yes' and num_of_employees = '0' and OT.coveredOwners is null and
                               SubCovReq = 'No'
                              then greatest(PRFL_min, PRFL_perEmp * NumofOwners, AppliedSubCost)
                          else AppliedSubCost end                                                           as newSubPR,
                      case
                          when PayrollfloorEligible = 'Yes'
                              then greatest(newEmployeePR + newSubPR + newOwnerPR, RatedPayroll)
                          else ratedpayroll end                                                             as HypotheticalRatedPR,
                      case when ratedpayroll = 0 then 'Y' else 'N' end                                      as ZeroRatedPR,
                      PRFL.Payroll_Initiative,
                      PRFL.Orig_Emp_PR,
                      PRFL.Orig_owner_PR,
                      PRFL.Orig_SubC_PR,
                      PRFL.Override_Emp_PR,
                      PRFL.Override_owner_PR,
                      PRFL.Override_SubC_PR,
                      case
                          when ZeroRatedPR = 'N' and PayrollFloorLaunch = 'Pre'
                              then HypotheticalRatedPR / RatedPayroll - 1
                          else 0 end                                                                        as Payroll_incr_pre,
                      case
                          when ZeroRatedPR = 'N' and PayrollFloorLaunch = 'Post' and
                               PRFL.Payroll_Initiative is not null and coveredowners is not null
                              then case
                                       when nvl(orig_owner_pr, 0) << OPR_Min * coveredowners
                                           then OPR_Min * coveredowners /*These are not exactly right, if 1 owner is higher and 1 is lower than the deemed*/
                                       when nvl(orig_owner_pr, 0) >> OPR_Max * coveredowners
                                           then OPR_Max * coveredowners
                                       else nvl(Orig_Owner_PR, 0) end end                                   as Applied_Orig_OwnerPR,
                      case
                          when ZeroRatedPR = 'N' and PayrollFloorLaunch = 'Post' and PRFL.Payroll_Initiative is not null
                              then RatedPayroll /
                                   (nvl(orig_emp_pr, 0) + nvl(Applied_Orig_OwnerPR, 0) + nvl(orig_subc_pr, 0)) - 1
                          else 0 end                                                                        as Payroll_incr_post,
                      case
                          when PayrollFloorLaunch = 'Pre' then Payroll_incr_pre
                          else Payroll_incr_post end                                                        as Payroll_incr

               from QP
                        left join RatingInfo R on qp.quote_job_id = R.job_id
                        left join ModOwnerPR MOP on qp.quote_job_id = MOP.job_id
                        left join TotEmployeePR TEP on qp.quote_job_id = TEP.job_id
                        left join dwh.sources_test_cobs stc on qp.cob = stc.cob_name
                        left join (select job_id,
                                          max(case when class = 'Gov' then HazGrp end) as HazGrp,
                                          sum(Payroll)                                 as rated_payroll,
                                          max(LCM)                                     as LCM,
                                          count(distinct ClassCode)                    as NumofClass,
                                          sum(LossCost * payroll / 100)                as calc_pureprem
                                   from class
                                   group by 1) CL
                                  on QP.quote_job_id = CL.job_id
                        left join underwriting_svc_prod.eligibility_checks ec on QP.offer_id = ec.offer_id
                        left join Subcost S
                                  on QP.lob_application_id = S.lob_application_id
                        left join HasRHR
                                  on QP.business_id = HasRHR.business_id
                        left join OfficerTypes OT on QP.offer_id = OT.offer_id
                        left join PRFL on QP.offer_id = PRFL.offer_id
                        left join PRCL_Chg on QP.policy_id = PRCL_Chg.policy_id
                        left join db_data_science.wc_ownerpayroll_20220817 op
                                  on QP.state_code = op.state and upper(OT.OfficerType) = upper(OP.officer_type_key)
               order by business_id, offer_creation_time)
select *
from final


---getting hazard group mapping
select regexp_substr(
               nullif(json_extract_path_text(calculation_summary, 'lob specific', 'governingClassResults', true), ''),
               '\\d{4}'),
       'hazard_group'),'') as govHazGrp, *
from s3_operational.rating_svc_prod_calculations rc
limit 10

--consulting package distribution by month
select extract(year from creation_time) || '-' ||
       right('00' + convert(varchar, extract(month from creation_time)), 2) as creation_year_month,
       --extract(month from policy_start_date) as creation_year_month,
       --cob_group,
       highest_status_package,
       count(highest_status_package)                                        as package_count
from dwh.quotes_policies_mlob
where highest_policy_status >= 4
  and
  --(cob in ('Restaurant', 'Caterers', 'Food Truck', 'Coffee Shop', 'Grocery Store') or cob_group = 'Retail') and
    cob_group = 'Consulting'
  and
  --cob_group not in ('Construction', 'Cleaning') and
    lob_policy = 'GL'
  and offer_flow_type in ('APPLICATION')
  and highest_status_package in ('basic', 'pro', 'proPlus')
  and
  --distribution_channel <> 'agents' and
    creation_time >= '2020-01-01'
group by 1, 2
order by creation_year_month asc

--consulting avg premium by month
select extract(year from creation_time) || '-' ||
       right('00' + convert(varchar, extract(month from creation_time)), 2) as creation_year_month,
       sum(highest_yearly_premium) / count(business_id)                     as avg_premium
from dwh.quotes_policies_mlob
where highest_policy_status >= 4
  and cob_group = 'Consulting'
  and lob_policy = 'GL'
  and offer_flow_type in ('APPLICATION')
  and creation_time >= '2020-01-01'
group by 1
order by creation_year_month asc

select cob_group, sum(highest_yearly_premium)
from dwh.quotes_policies_mlob
where offer_flow_type = 'RENEWAL'
  and creation_time >= '2023-03-01'
group by 1
order by 2 desc

--counts of CP monoline, GL monoline, CP+GL, and other for non-construction - MLOB
SELECT cob_group,
       COUNT(DISTINCT CASE
                          WHEN lob_policy = 'CP' AND highest_policy_status = 4 AND
                               cob_group not in ('Construction', 'Cleaning')
                              AND business_id NOT IN (SELECT business_id
                                                      FROM dwh.quotes_policies_mlob
                                                      WHERE lob_policy = 'GL'
                                                        AND cob_group not in ('Construction', 'Cleaning'))
                              THEN business_id END) AS cp_only,
       COUNT(DISTINCT CASE
                          WHEN lob_policy = 'GL' AND highest_policy_status = 4 AND
                               cob_group not in ('Construction', 'Cleaning')
                              AND business_id NOT IN (SELECT business_id
                                                      FROM dwh.quotes_policies_mlob
                                                      WHERE lob_policy = 'CP'
                                                        AND highest_policy_status = 4
                                                        AND cob_group not in ('Construction', 'Cleaning'))
                              THEN business_id END) AS gl_only,
       COUNT(DISTINCT CASE
                          WHEN lob_policy IN ('CP', 'GL') AND highest_policy_status = 4 AND
                               cob_group not in ('Construction', 'Cleaning')
                              AND business_id IN (SELECT business_id
                                                  FROM dwh.quotes_policies_mlob
                                                  WHERE lob_policy = 'CP'
                                                    AND highest_policy_status = 4
                                                    AND cob_group not in ('Construction', 'Cleaning'))
                              AND business_id IN (SELECT business_id
                                                  FROM dwh.quotes_policies_mlob
                                                  WHERE lob_policy = 'GL'
                                                    AND highest_policy_status = 4
                                                    AND cob_group not in ('Construction', 'Cleaning'))
                              THEN business_id END) AS both_cp_and_gl,
       COUNT(DISTINCT business_id)                  AS total_distinct_ids
FROM dwh.quotes_policies_mlob
WHERE highest_policy_status = 4
  AND cob_group not in ('Construction', 'Cleaning')
group by 1

--list of weshop policies
select business_id,
       cob_name,
       lob,
       policy_status_name,
       yearly_premium,
       *
from db_data_science.v_all_agents_policies
where --agent_email_address = 'greg@traditions-group.com' --and
      agency_name like '%WeShop%'
--policy_status_name <> ''

select *
from db_data_science.all_claims_automation_master
limit 10

select *
from dwh.all_claims_financial_changes
limit 10

--to get 100 largest grocery policies (new biz only) by agency name
select aap.agency_name,
       count(distinct qpm.business_id),
       sum(qpm.highest_yearly_premium) as total_premium
from dwh.quotes_policies_mlob qpm
         join db_data_science.v_all_agents_policies aap on qpm.business_id = aap.business_id
where highest_policy_status >= 4
  and
  --distribution_channel = 'agents' and
    lob_policy = 'GL'
  and
  --new_reneweal= 'new' and
    creation_time >= '2023-01-01'
  and cob = 'Grocery Store'
  and offer_flow_type = 'APPLICATION'
group by 1
order by 3 desc
limit 1000

--find non-renewals NR
select business_id, distribution_channel, lob_policy, cob_group, cob, highest_status_name, policy_start_date
from dwh.quotes_policies_mlob
where distribution_channel not in
      ('agents', 'ap-gusto', 'ap-intuit', 'ap-non-isg', 'ap-square', 'Next Connect', 'partnerships')
  and lob_policy = 'CP'
  and cob_group in ('Unsupported COB')
  and policy_start_date >= '2022-05-01'
  and highest_policy_status >= 3

--pull all active insurance agent policies (for Jess G)
select business_id,
       json_extract_path_text(json_args, 'business_name', true)                                      as biz_name,
       json_extract_path_text(json_args, 'street', true)                                             as biz_address,
       json_extract_path_text(json_args, 'city', true)                                               as biz_city,
       state,
       json_extract_path_text(json_args, 'zip_code', true)                                           as biz_zip,
       json_extract_path_text(json_extract_path_text(json_args, 'lob_app_json', true),
                              'pl_aop_insurance_agents_unlisted_other_commercial_lines_alloc', true) as comm_lines_pct,
       cob,
       lob_policy,
       policy_start_date,
       highest_status_name
from dwh.quotes_policies_mlob
where cob = 'Insurance Agent'
  and highest_policy_status = 4
  and lob_policy = 'PL'
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and comm_lines_pct <> ''

select json_args
from dwh.quotes_policies_mlob
where business_id = '9aba53945c35d0e1c72bfd78f9e0d604'

---iso primary by cob
select json_extract_path_text(json_extract_path_text(calculation, 'ni factors', true),
                              'n9 <- primaryIsoClassFactor (primaryIsoClass, state)', true) as primary_iso,
       json_extract_path_text(json_extract_path_text(data_points, 'dataPointsWithoutPackageData', true), 'leadCob',
                              true)                                                         as lead_cob,
       count(distinct job_id)
from s3_operational.rating_svc_prod_calculations rc
where lob = 'GL'
  and dateid >= '2023-01-01'
group by 1, 2

--permitted / open WC states
select state_code
--     , lob
--     , cob_name
--     , marketing_cob_group
--     , cob_industry
--     , update_time
--     , update_message
from portfolio_svc_prod.permitted_cobs_states_lobs
         inner join portfolio_svc_prod.cobs using (cob_id)
         inner join dwh.sources_test_cobs using (cob_name)
where permitted = 1
  and lob = 'WC'
group by 1

--lob state month-year / year-month
select lob_policy,
       state,
       extract(year from creation_time) || '-' ||
       right('00' + convert(varchar, extract(month from creation_time)), 2) as creation_year_month,
       sum(highest_yearly_premium)
from dwh.quotes_policies_mlob
where highest_policy_status >= 3
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
group by 1, 2, 3

---certificate COI additional insured DOO details
select cert.business_id,
       qpm.num_of_employees,
       qpm.cob_group,
       qpm.cob,
       qpm.payroll_last_12_months,
       qpm.revenue_in_12_months,
       qpm.highest_yearly_premium,
       qpm.state         AS policy_state,
       qpm.purchase_date AS purchase_date,
       qpm.policy_start_date,
       qpm.policy_end_date,
       cert.description_of_operations,
       cert.certificate_type,
       cert.additional_insured_id,
       cert.status,
       cert.special_conditions,
       cert.updated_by,
       cert.creation_time,
       cert.update_time,
       cert.certificate_origin,
       AI.name           AS additional_insured_name,
       AI.state_code     AS AI_state,
       AI.zip_code       AS AI_zip
FROM certificate_svc_prod.certificates cert
         LEFT JOIN dwh.quotes_policies_mlob qpm ON cert.business_id = qpm.business_id
         LEFT JOIN nimi_svc_prod.additional_insured AI ON cert.additional_insured_id = AI.additional_insured_id
WHERE highest_policy_status >= 4
  AND lob_policy = 'GL'
  AND cob_group = 'Construction'
  AND cert.description_of_operations LIKE '%Additional Insured Automatic Status Endorsement%'
  AND qpm.creation_time >= '2022,01,01'
limit 10000

--for reinsurance -- 3 restaurant declines in 2023
WITH t1 as (select distinct business_id,
                            decline_reasons,
                            current_amendment,
                            end_date
            from dwh.underwriting_quotes_data
            where start_date >= '2022-01-01'
              and lob = 'PL'
              and execution_status = 'DECLINE'
    --and current_amendment like '%renewal%'
)
select distinct da.business_id, decline_reasons, current_amendment, end_date
from dwh.daily_activities da
         join t1
              on t1.business_id = da.business_id
where da.start_date >= '2022-01-01'
  and da.lob = 'PL'
--da.cob_name <> 'Restaurant' and
--da.marketing_cob_group <> 'Construction' and
--cob_name = 'Business Consulting'

select *
from dwh.underwriting_quotes_data
where start_date >= '2023-01-01'
  and lob = 'GL'
  and execution_status = 'DECLINE'
limit 10

---loss and EP
with loss as (select p.business_id
                   , lob
                   , exposure.policy_reference
                   , count(distinct claim_id) as claim_count
                   , sum(lossalae)            as lossalae_uncapped
                   , sum(TotReserve)          as TotReserve
              from (select case when tpa = 2 then substring(a.claim_id, 1, 13) else a.claim_id end as Claim_ID,
                           a.exposure_id,
                           a.policy_reference,
                           lob,
                           nvl(a.loss_paid_total, 0) +
                           nvl(a.loss_reserve_total, 0) +
                           nvl(a.expense_ao_paid_total, 0) +
                           nvl(a.expense_ao_reserve_total, 0) +
                           nvl(a.expense_dcc_paid_total, 0) +
                           nvl(a.expense_dcc_reserve_total, 0) +
                           nvl(a.recovery_salvage_collected_total, 0) +
                           nvl(a.recovery_subrogation_collected_total, 0)                          as LossALAE,
                           nvl(a.loss_reserve_total, 0) + nvl(a.expense_ao_reserve_total, 0) +
                           nvl(a.expense_dcc_reserve_total, 0)                                     as TotReserve
                    from dwh.all_claims_financial_changes_ds a
                             join (select policy_reference, claim_id, exposure_id, max(date) as maxdate
                                   from dwh.all_claims_financial_changes_ds
                                   where lob IN ('GL', 'WC')
                                   group by 1, 2, 3) b
                                  on a.policy_reference = b.policy_reference and a.claim_id = b.claim_id and
                                     a.exposure_id = b.exposure_id and a.date = b.maxdate) exposure
                       left join nimi_svc_prod.policies p on p.policy_reference = exposure.policy_reference
              group by 1, 2, 3),
     ep as (select policy_reference, sum(dollar_amount) earned_premium_gaap
            from reporting.gaap_snapshots_asl
            where trans in ('monthly earned premium', 'monthly earned premium endorsement')
              and date <= end_date
              and lob IN ('GL', 'WC')
            group by 1)
select case
           when cob_group = 'Food & beverage - deprecated' THEN 'Food & beverage'
           when cob_group = 'Retail - deprecated' THEN 'Retail'
           else cob_group end                 as cob_group,
       case
           when agent_id <> 'N/A' then 'agent'
           when affiliate_id <> 'N/A' then 'partnership'
           else 'direct'
           end                                as channel,
       lob_policy,
       qpm.business_id,
       claim_count,
       lossalae_uncapped,
       TotReserve,
       earned_premium_gaap                    as earned_premium,
       revenue_in_12_months,
       CASE
           WHEN revenue_in_12_months is null then 'NULL'
           WHEN revenue_in_12_months::numeric < 30000 then '0-30k'
           WHEN revenue_in_12_months::numeric < 75000 then '30k-75k'
           WHEN revenue_in_12_months::numeric < 100000 then '75k-100k'
           WHEN revenue_in_12_months::numeric < 200000 then '100k-200k'
           ELSE '200k+' END                   AS revenue_bin,
       count(case
                 when c.creation_time >= qpm.policy_start_date
                     and c.creation_time <= qpm.policy_end_date
                     and certificate_type = 'COI'
                     then certificate_id end) as cert_count
from dwh.quotes_policies_mlob qpm
         left join certificate_svc_prod.certificates c on qpm.business_id = c.business_id
         left join loss l on qpm.business_id = l.business_id and qpm.lob_policy = l.lob and
                             qpm.highest_policy_reference = l.policy_reference
         left join ep on qpm.highest_policy_reference = ep.policy_reference
where highest_policy_status >= 4
  and lob_policy IN ('GL')
  and cob_group IN ('Construction')
  and qpm.policy_start_date >= '2022-01-01'
  and qpm.policy_start_date <= '2022-12-31'
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

select json_args
from dwh.quotes_policies_mlob
where business_id = 'a4fa3792bb03e69498d82af72a75d072'
  and lob_policy = 'GL'

select json_extract_path_text(json_extract_path_text(calculation, 'ni factors', true),
                              'n9 <- primaryIsoClassFactor (primaryIsoClass, state)', true) as primary_iso,
       json_extract_path_text(json_extract_path_text(data_points, 'dataPointsWithoutPackageData', true), 'leadCob',
                              true)                                                         as lead_cob,
       count(distinct job_id)
from s3_operational.rating_svc_prod_calculations rc
where lob = 'GL'
  and dateid >= '2023-01-01'

select *
from s3_operational.rating_svc_prod_calculations rc
where lob = 'GL'
  and dateid >= '2023-06-01'
  and calculation like '%fcra%'
limit 10

---legal research
select *
from dwh.all_activities_table
where tracking_id = 'e95b4830d03896db166405a0680840a5'
--tracking_id in ('2zoo4zoo394be7390d50a80c5fc05f5f')
--policy_id = '7480953'
---legal research cont'd
select *
from dwh.all_activities_table
where interaction_data like '%agent_id":"XgF3TWBVe4Qu1NuY"%'
  and cob_name = 'Restaurant'
--legal research initial
select *
from dwh.quotes_policies_mlob
where business_id = 'f11923725f9a7fca476fc73a03b8f429'
--legal research tracking id
select *
from dwh.all_activities_table aa
         join dwh.sources_attributed_table ss on aa.tracking_id = ss.tracking_id
where business_id = 'f11923725f9a7fca476fc73a03b8f429'

select business_id, highest_status_name, highest_policy_status
from dwh.quotes_policies_mlob
where lob_policy = 'PL'
  and highest_policy_status = 1
  and cob = 'Property Manager'
  and creation_time >= '2023-01-01'
  and highest_yearly_premium >= 5000
limit 10

select distinct business_id, agent_id, affiliate_id
from dwh.underwriting_quotes_data
where business_id in ('475e2566ec8010d6a462d039cb5bbf40')

select business_id,
       highest_policy_id,
       policy_start_date,
       state,
       distribution_channel,
       cob_group,
       cob,
       highest_yearly_premium
from dwh.quotes_policies_mlob qpm
where highest_policy_status = 4
  and
  --distribution_channel = 'agents' and
    lob_policy = 'PL'
  and
  --new_reneweal= 'new' and
  --policy_start_date >= '2023-01-01' and
  --cob = 'Restaurant' and
  --highest_yearly_premium <> '' and
  --offer_flow_type in ('APPLICATION','RENEWAL','CANCEL_REWRITE') and
    business_id in ('5ea38f9d2a08e7be1e43c039042bd657', '4ddb1db0522de0508e88737eae0d782a')

select business_id,
       highest_policy_id,
       policy_start_date,
       state,
       distribution_channel,
       cob_group,
       cob,
       highest_yearly_premium
from dwh.quotes_policies_mlob
where business_id in ('5ea38f9d2a08e7be1e43c039042bd657', '4ddb1db0522de0508e88737eae0d782a')


select distinct business_id, cob, highest_yearly_premium
from dwh.quotes_policies_mlob
where lob_policy = 'GL'
  and offer_flow_type = 'APPLICATION'
  and policy_start_date >= '2023-06-01'
  and cob_group = 'Sports & fitness'
  and highest_policy_status = 4
  and highest_status_package = 'proPlus'


--large fitness policies
select distinct business_id,
                policy_start_date,
                lob_policy,
                revenue_in_12_months,
                cob,
                distribution_channel,
                highest_yearly_premium--, highest_status_package
from dwh.quotes_policies_mlob
where highest_policy_status = 4
  and
  --distribution_channel = 'agents' and
  --lob_policy = 'GL' and
    offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and creation_time >= '2023-06-01'
  and cob_group = 'Retail'
  and highest_yearly_premium >= 5000
order by highest_yearly_premium desc

--LOB dist for insurance agent COB
select cob,
       lob_policy,
       sum(highest_yearly_premium)
from dwh.quotes_policies_mlob
where highest_policy_status = 4
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and creation_time >= '2023-01-01'
  and cob in ('Insurance Agent', 'Property Manager', 'Real Estate Brokers', 'Home Inspectors', 'Accountant', 'Engineer',
              'Architect', 'Real Estate Agent', 'Interior Designer', 'Claims Adjuster', 'Notary', 'Insurance Inspector',
              'Insurance Appraisers', 'Urban and Regional Planners')
group by 1, 2
order by 1

--large claims research
select business_id,
       lob_policy,
       highest_policy_earned_premium,
       highest_policy_total_loss_plus_alae_expense_total,
       highest_policy_number_of_claims_total,
       highest_policy_status,
       highest_status_name
from dwh.quotes_policies_mlob
where creation_time >= '2020-01-01'
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and (highest_policy_total_loss_plus_alae_expense_total >= 20000 or highest_policy_number_of_claims_total > 2)
  and lob_policy = 'GL'
limit 10000

--large claims research
select business_id,
       lob_policy,
       highest_policy_earned_premium,
       highest_policy_total_loss_plus_alae_expense_total,
       highest_policy_number_of_claims_total,
       highest_policy_status,
       highest_status_name
from dwh.quotes_policies_mlob
where business_id = '946867bce20417fbb728413dcea327dd'
limit 10000

--get premium from high pct liquor sales
select business_id,
       creation_time,
       json_extract_path_text(json_extract_path_text(json_args, 'lob_app_json', true), 'liquor_sales_exposure',
                              true) as liq_sales_pct,
       highest_yearly_premium
from dwh.quotes_policies_mlob
where highest_policy_status >= 3
  and lob_policy = 'GL'
  and cob = 'Restaurant'
  and liq_sales_pct <> ''
  and offer_flow_type in ('APPLICATION')
  and policy_start_date >= '2023-01-01'
  and policy_start_date < '2023-04-01'

--get IM premium for limits >$10K
select *
from dwh.quotes_policies_mlob
where lob_policy = 'IM'
  and highest_policy_status >= 3
  and liq_sales_pct <> ''
  and offer_flow_type in ('APPLICATION')
  and policy_start_date >= '2023-01-01'
  and policy_start_date < '2023-04-01'

--get model based (model-based) exposure (MBE) logs for a business ID
select *
from riskmgmt_svc_prod.exposure_base_revenue_results
where business_id = '3e039efd82b34507508e370edbf5b43f'

--premium breakdown
select cob, lob_policy, count(distinct business_id), sum(highest_yearly_premium)
from dwh.quotes_policies_mlob
where highest_policy_status = 4
  and cob in ('Insurance Agent', 'Real Estate Agent', 'Accountant')
  and offer_flow_type in ('APPLICATION')
group by 1, 2
order by 1 asc, 3 desc

--cp start
select lob_policy,
       extract(year from creation_time) || '-' ||
       right('00' + convert(varchar, extract(month from creation_time)), 2) as creation_year_month,
       sum(highest_yearly_premium)
from dwh.quotes_policies_mlob
where highest_policy_status >= 3
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and lob_policy = 'CP'
group by 1, 2
order by 2 asc

--avg price and standard deviation by COB group
select lob_policy,
       cob_group,
       sum(highest_yearly_premium) / count(business_id)                             as avg_premium,
       SQRT(SUM(POW(highest_yearly_premium - avg_premium, 2)) / COUNT(business_id)) AS premium_std_dev,
       sum(highest_yearly_premium)                                                  as premIF
from dwh.quotes_policies_mlob
where highest_policy_status = 4
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and policy_start_date >= '2023-04-01'
  and lob_policy = 'GL'
group by 1, 2
order by 1 asc, 2 asc

--avg price and standard deviation by COB group (Wendy >> chatGPT)
with subquery as (SELECT lob_policy,
                         cob_group,
                         highest_yearly_premium,
                         business_id,
                         AVG(highest_yearly_premium) OVER (PARTITION BY lob_policy, cob_group) AS avg_premium
                  FROM dwh.quotes_policies_mlob
                  WHERE highest_policy_status = 4
                    and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
                    and policy_start_date >= '2023-01-01'
                    and lob_policy = 'GL')
    (SELECT lob_policy,
            cob_group,
            STDDEV(highest_yearly_premium),
            SUM(highest_yearly_premium) / COUNT(business_id) AS avg_premium,
            SUM(highest_yearly_premium)                      AS total_premium_in_force
     from subquery
     group by 1, 2)

--cheapest handyperson policy
select extract(year from qdata.offer_creation_time) || '-' ||
       right('00' + convert(varchar, extract(month from qdata.offer_creation_time)), 2) as creation_year_month
    state, carrier_name
     ,

from dwh.quotes_policies_mlob
where highest_policy_status >= 3
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and policy_start_date >= '2023-04-01'
  and lob_policy = 'GL'
limit 10

---research on NXUS WP
select extract(year from policy_start_date) || '-' ||
       right('00' + convert(varchar, extract(month from policy_start_date)), 2) as policy_start_year_month,
       state,
       carrier_name,
       sum(highest_yearly_premium)
from dwh.quotes_policies_mlob
where highest_policy_status >= 4
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and
  --offer_flow_type in ('RENEWAL') and
    policy_start_date >= '2023-01-01'
  and
  --lob_policy = 'GL' and
    state in ('NV', 'TX', 'FL', 'SC', 'UT')
group by 1, 2, 3
order by 1, 2, 3

--find GL restaurant denials due to BDL
select *
from dwh.all_claims_financial_changes_ds
where claim_id in ('lNLxQBDHg6mleYiW', 'WFkti1HW44NN8FgD', 'J4sWUuK5WYdsFZ6w', 'k6ES7kwITRpe69bi', 'k6ES7kwITRpe69bi',
                   'IzZkmPl8tfOZai7I', 'IzZkmPl8tfOZai7I', 'IzZkmPl8tfOZai7I', 'IzZkmPl8tfOZai7I', 'IzZkmPl8tfOZai7I',
                   'IzZkmPl8tfOZai7I', 'ThtA1lDbvvZnoHl1')

--is old coupon_code used
select business_id,
       lob_policy,
       cob,
       distribution_channel,
       policy_start_date,
       json_extract_path_text(qpm.json_args, 'lob_app_json', 'coupon_id', true) as coupon_id
from dwh.quotes_policies_mlob qpm
where json_args LIKE '%coupon_id%'
  and highest_policy_status in ('4', '7')
  and creation_time >= '2022-06-01'

select affiliate_id, count(distinct (business_id)), sum(highest_yearly_premium)
from dwh.quotes_policies_mlob
where affiliate_id in ('4700')
  and highest_policy_status >= 4
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
group by 1

select highest_status_package, count(distinct (business_id))
from dwh.quotes_policies_mlob
where lob_policy = 'PL'
  and highest_policy_status >= 4
  and distribution_channel = 'agents'
group by 1
order by 2 desc

select business_id, policy_start_date
from dwh.quotes_policies_mlob
where lob_policy = 'PL'
  and state = 'WY'
  and highest_policy_status in ('5', '6')

select highest_policy_status, highest_status_name
from dwh.quotes_policies_mlob
group by 1, 2

--Q3 GL COB expansion - AR consulting
select sum(highest_yearly_premium), count(distinct (business_id))
from dwh.quotes_policies_mlob
where lob_policy = 'GL'
  and state = 'AR'
  and cob in
      ('Business Consulting', 'Other Consulting', 'IT Consulting or Programming', 'Marketing', 'Graphic Designers',
       'Administrative Services Managers', 'Advertising and Promotions Managers', 'Computer Programmers',
       'Education Consulting', 'Employment Agencies',
       'Agents and Business Managers of Artists, Performers, and Athletes', 'Craft Artists', 'Editorial Services',
       'Educational, Guidance, School, and Vocational Counselors', 'Human Resources Specialists', 'Safety Consultant',
       'Training and Development Specialists', 'Writer', 'Art Consultants', 'Call Center Service',
       'Computer and Information Systems Managers', 'Computer Network Architects',
       'Computer Network Support Specialists', 'Emergency Management Directors', 'Etchers and Engravers',
       'Fine Artists, Including Painters, Sculptors, and Illustrators', 'Food Safety Training', 'Historians',
       'Logisticians', 'Product Designer', 'Public Relations Specialists', 'Reporters and Correspondents',
       'Set and Exhibit Designers', 'Speech Training', 'Telemarketing and Telesales Services', 'Wedding Officiant')
  and highest_policy_status >= 4
  and offer_flow_type = 'APPLICATION'
  and creation_time >= '2023-10-01'
  and creation_time <= '2023-11-30'

--Q3 GL COB expansion - IL/TX/WA 4 classes
select sum(highest_yearly_premium), count(distinct (business_id))
from dwh.quotes_policies_mlob
where lob_policy = 'GL'
  and state in ('IL', 'NY', 'TX')
  and cob in ('Day Care', 'Massage Therapist', 'Cpr and First Aid Training', 'Alternative Healing')
  and highest_policy_status >= 4
  and offer_flow_type = 'APPLICATION'
  and creation_time >= '2023-10-01'
  and creation_time <= '2023-11-30'

--Q3 GL COB expansion - NY non-construction
select sum(highest_yearly_premium), count(distinct (business_id))
from dwh.quotes_policies_mlob
where lob_policy = 'GL'
  and state = 'NY'
  and cob in ('Health and Wellness Coaching', 'Mental Health Counselors', 'Real Estate Brokers', 'Alternative Healing',
              'CPR and First Aid Training', 'Veterinarians', 'Medical Records and Health Information Technicians',
              'Speech Language Pathologists', 'Real Estate Appraisal', 'Occupational Therapists', 'Psychiatrists',
              'Psychologists', 'Sleep Specialists', 'Habilitative Services',
              'Occupational Health and Safety Specialists', 'Rehabilitation Counselors',
              'Dental Laboratory Technicians', 'Marriage and Family Therapists', 'Psychiatric Aides',
              'Sports Psychologists', 'Weight Loss Centers')
  and highest_policy_status >= 4
  and offer_flow_type = 'APPLICATION'
  and creation_time >= '2023-10-01'
  and creation_time <= '2023-11-30'

--Q3 GL COB expansion - AK real estate
select sum(highest_yearly_premium), count(distinct (business_id))
from dwh.quotes_policies_mlob
where lob_policy = 'GL'
  and state = 'AK'
  and cob in ('Real Estate Brokers', 'Real Estate Appraisal')
  and highest_policy_status >= 4
  and offer_flow_type = 'APPLICATION'
  and creation_time >= '2023-10-01'
  and creation_time <= '2023-11-30'

--Q3 GL COB expansion - healthcare
select sum(highest_yearly_premium), count(distinct (business_id))
from dwh.quotes_policies_mlob
where lob_policy = 'GL'
  and
  --state = 'AK' and
    cob in ('Home Health Care', 'Home Health Aides', 'Personal Care Services', 'Nurse Practitioners', 'Doctors',
            'Registered Nurses', 'Medical and Health Services Managers', 'Dentists', 'Nursing Assistants',
            'Medical and Clinical Laboratory Technicians', 'Personal Care Aides', 'Laboratory Testing',
            'Community Health Workers', 'Doulas', 'Skilled Nursing', 'Medical Assistants',
            'Environmental Science and Protection Technicians, Including Health', 'Dental and Orthodontic Services',
            'Optometrists', 'Licensed Practical and Licensed Vocational Nurses', 'Teeth Whitening', 'Family Practice',
            'Physician Assistants', 'Medical Appliance Technicians', 'Diagnostic Services', 'Health Retreats',
            'Osteopathic Physicians', 'Biological Technicians', 'Biomedical Engineers', 'Concierge Medicine',
            'Family and General Practitioners', 'Internal Medicine', 'Walk-in Clinics',
            'Medical Scientists, Except Epidemiologists', 'Ultrasound Imaging Centers', 'Podiatrists',
            'Nurse Anesthetists', 'Preventive Medicine', 'Diagnostic Imaging', 'Dental Hygienists', 'Midwives',
            'Dermatologists', 'Audiologists', 'Orthodontists', 'Pediatricians', 'Dental Assistants',
            'Hearing Aid Providers', 'Obstetricians and Gynecologists', 'Ophthalmologists', 'Radiologists',
            'Cardiovascular Technologists and Technicians', 'Hearing Aid Specialists', 'Respiratory Therapists',
            'Anesthesiologists', 'Nurse Midwives', 'Diagnostic Medical Sonographers', 'Orthopedists', 'Allergists',
            'Pediatric Dentists', 'Prenatal/Perinatal Care', 'Reproductive Health Services',
            'Magnetic Resonance Imaging Technologists', 'Colonics', 'Oncologist', 'Periodontists', 'Vascular Medicine',
            'Cardiologists', 'Gastroenterologist', 'Internists, General', 'Neurologist', 'Radiation Therapists',
            'Ayurveda', 'Ear Nose and Throat', 'Endocrinologists', 'Proctologists', 'Prosthodontists', 'Toxicologists',
            'Traditional Chinese Medicine', 'Endodontists', 'Fertility', 'Gerontologists', 'Hepatologists', 'Orderlies',
            'Otologists', 'Pulmonologist', 'Retina Specialists', 'Rheumatologists', 'Urologists')
  and highest_policy_status >= 4
  and offer_flow_type = 'APPLICATION'
  and creation_time >= '2023-10-01'
  and creation_time <= '2023-11-30'

--deeper cut - Q3 GL COB expansion - IL/TX/WA 4 classes
select cob, state, sum(highest_yearly_premium), count(distinct (business_id))
from dwh.quotes_policies_mlob
where lob_policy = 'GL'
  and state in ('IL', 'NY', 'TX')
  and cob in ('Day Care', 'Massage Therapist', 'Cpr and First Aid Training', 'Alternative Healing')
  and highest_policy_status >= 4
  and offer_flow_type = 'APPLICATION'
  and creation_time >= '2023-07-23'
group by 1, 2
order by 3 desc

select business_id, highest_yearly_premium, distribution_channel
from dwh.quotes_policies_mlob
where lob_policy = 'GL'
  and state in ('NY')
  and cob in ('Day Care')
  and highest_policy_status >= 4
  and offer_flow_type = 'APPLICATION'
  and creation_time >= '2023-07-23'
order by 2 desc

--TRIA GL policies
select business_id, cob, highest_status_package
from dwh.quotes_policies_mlob
where lob_policy = 'GL'
  and highest_policy_status = 4
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and creation_time >= '2023-10-17'
  and highest_status_package like '%Tria%'

--UMB policy list
select p.policy_id,
       p.business_id,
       p.policy_reference,
       p.carrier,
       p.policy_status,
       p.start_date,
       p.end_date,
       p.occ_limit,
       date_trunc('day', p.bind_date)             bind_date,
       p.yearly_premium,
       pt.code                                 as lob,
       c.cob_name                              as cob,
       c.marketing_cob_group                      cob_group,
       cast(
               nullif(
                       json_extract_path_text(
                               regexp_substr(
                                       json_extract_path_text(financial_attribution, 'policyItems', true),
                                       '\\{.{5,20}UMBRELLA.{60,90}\\}\\}'), 'amounts', 'premium', true),
                       '') as numeric(30, 15)) as gl_umb_prem,
       cast(
               nullif(
                       json_extract_path_text(
                               regexp_substr(
                                       json_extract_path_text(financial_attribution, 'policyItems', true),
                                       '\\{.{5,20}EXCESS_LIABILITY.{60,90}\\}\\}'), 'amounts', 'premium', true),
                       '') as numeric(30, 15)) as gl_excess_prem,
       CASE
           WHEN clm.business_id is null then 'N/A'
           WHEN clm.agent_id is not null then 'Agent'
           WHEN clm.affiliate_id is not null then 'Partnership'
           ELSE 'Direct' END                   AS channel,
       clm.agency_type
from nimi_svc_prod.policies p
         JOIN nimi_svc_prod.policy_types pt on p.policy_type_id = pt.policy_type_id
         JOIN dwh.sources_test_cobs c on p.cob_id = c.cob_id
         LEFT JOIN dwh.company_level_metrics_ds clm on p.business_id = clm.business_id
where (financial_attribution ilike '%umbrella%'
    or financial_attribution ilike '%excess_liability%')
  and pt.code = 'GL'

--pull list of leads by COB by month
select event_date,
       cob_name,
       json_extract_path_text(source_json_first_related_business_id, 'channel', true) as dist_channel,
       count(distinct business_id)                                                    as num_businesses
from dwh.daily_activities
where start_date >= '2023-08-16'
  and lead = 1
  and cob_name in ('Insurance Agent', 'Real Estate Brokers', 'Home Inspectors', 'Accountant', 'Engineer', 'Architect',
                   'Real Estate Agent', 'Interior Designer', 'Claims Adjuster', 'Notary', 'Insurance Inspector',
                   'Insurance Appraisers', 'Urban and Regional Planners')
group by 1, 2, 3
order by 1 asc

--pull top consulting policies
select business_id, cob, lob_policy, distribution_channel, highest_yearly_premium
from dwh.quotes_policies_mlob
where cob_group = 'Consulting'
  and creation_time >= '2023-08-15'
  and offer_flow_type = 'APPLICATION'
  and highest_policy_status in ('4', '7')
order by 5 desc

--pull top fitness studio policies
select business_id, cob, lob_policy, distribution_channel, highest_yearly_premium, policy_start_date
from dwh.quotes_policies_mlob
where cob_group = 'Sports & fitness'
  and creation_time >= '2023-08-15'
  and offer_flow_type = 'APPLICATION'
  and highest_policy_status in ('4', '7')
order by 5 desc


--looking for top unsupported leads by cob
select --extract(day from eventtime) as event_time_day,
       ss.cob_name,
       ss.marketing_cob_group,
       aa.placement,
       count(distinct related_business_id)
from dwh.all_activities_table aa
         join dwh.sources_attributed_table ss on aa.tracking_id = ss.tracking_id
         left join portfolio_svc_prod.permitted_cobs_states_lobs list
                   on list.cob_id = ss.cob_id and list.state_code = ss.state_code and aa.lob = list.lob
where date(eventtime) >= '2025-05-01'
  and funnelphase like '%Unsupported%'
group by 1, 2, 3
order by 4 desc

--top unsupported leads
select aa.cob_name,
       --funnelphase,
       count(distinct (ss.related_business_id))
from dwh.all_activities_table aa
         join dwh.sources_attributed_table ss on aa.tracking_id = ss.tracking_id
         left join portfolio_svc_prod.permitted_cobs_states_lobs list
                   on list.cob_id = ss.cob_id and list.state_code = ss.state_code and aa.lob = list.lob
where date(eventtime) >= '2024-01-01'
  and funnelphase like '%Unsupported%'
group by 1
order by 2 desc

--drill into unsupported COB
select aa.cob_name, ss.business_id
--funnelphase,
--count(distinct(ss.related_business_id))
from dwh.all_activities_table aa
         join dwh.sources_attributed_table ss on aa.tracking_id = ss.tracking_id
         left join portfolio_svc_prod.permitted_cobs_states_lobs list
                   on list.cob_id = ss.cob_id and list.state_code = ss.state_code and aa.lob = list.lob
where date(eventtime) >= '2024-01-01'
  and funnelphase like '%Unsupported%'
  and aa.cob_name = 'Distributor'
limit 500

--get recently written MT agent policies
select business_id, highest_policy_status, distribution_channel, state
from dwh.quotes_policies_mlob
where state = 'MT'
  and distribution_channel = 'agents'
  and highest_policy_status = 1
  and creation_time >= '2023-08-31'

--get top pro services classes
select cob, sum(highest_yearly_premium) as premium, count(distinct (business_id)) as businesses
from dwh.quotes_policies_mlob
where distribution_channel <> 'agents'
  and cob_group in ('Architects', 'Business & administrative services', 'Creative services', 'Legal', 'Real Estate',
                    'Architects & engineers', 'Travel & hospitality', 'Professional Services', 'Financial services',
                    'Consulting', 'Real estate services', 'Insurance professional', 'Media & communications',
                    'IT and technical services', 'Engineers', 'Finance', 'Insurance')
  and highest_policy_status >= 3
  and creation_time >= '2022-09-01'
group by 1
order by 2 desc

--all policies written by agent A
select business_id,
       cob_name,
       lob,
       policy_status_name,
       yearly_premium,
       *
from db_data_science.v_all_agents_policies
where --agent_email_address = 'greg@traditions-group.com' --and
      agency_name like '%TruPoint%'
--policy_status_name <> ''

select business_id
from dwh.quotes_policies_mlob
where creation_time < '2020-01-01'
  and creation_time > '2019-12-01'
  and highest_policy_status >= 3
  and cob = 'General Contractor'
  and state = 'FL'
limit 10

--digging into top unsupported leads
select ss.business_id, ss.cob_name, ss.marketing_cob_group, aa.placement
from dwh.all_activities_table aa
         join dwh.sources_attributed_table ss on aa.tracking_id = ss.tracking_id
         left join portfolio_svc_prod.permitted_cobs_states_lobs list
                   on list.cob_id = ss.cob_id and list.state_code = ss.state_code and aa.lob = list.lob
where date(eventtime) >= '2023-08-01'
  and funnelphase like '%Unsupported%'
  and ss.cob_name = 'Retail Stores'

--delete
select distinct(funnelphase)
from dwh.all_activities_agents
where eventtime >= '2023-09-17'
--limit 10

--top pro services agents, top agents
select agent_id,
       agent_name,
       agency_name,
       current_agencytype,
       sum(yearly_premium)           as total_premium,
       count(distinct (business_id)) as total_businesses
from dwh.agents_policies_ds
where start_date >= '2023-01-01'
  and
  --start_date < '2023-01-01' and
    policy_status >= 3
  and
  --(bundle_lobs like '%PL%' or bundle_lobs like '%GL%') and
    cob in ('Insurance Agent', 'Property Manager', 'Business Consulting Services', 'Accountant', 'Technology Services',
            'Engineer', 'Home Inspectors', 'Real Estate Agent', 'Marketing', 'Interior Designer', 'Architect',
            'Real Estate Brokers', 'Legal Service', 'Claims Adjuster', 'Financial Adviser', 'Administrative Support',
            'Notary', 'Product Designer', 'Building Inspector', 'Land Surveyor', 'Mortgage Broker',
            'Insurance Inspector', 'Business Financing', 'Occupational Health and Safety Specialists',
            'Environmental Science and Protection Technicians, Including Health',
            'Telemarketing and Telesales Services', 'Environmental Scientists and Specialists, Including Health',
            'Real Estate Appraisal', 'Securities, Commodities, and Financial Services Sales Agents',
            'Insurance Appraisers', 'Urban and Regional Planners', 'Loan Officers', 'Title Loans',
            'Debt Relief Services', 'Credit Authorizers, Checkers, and Clerks', 'Video Transfer Services',
            'Actuarial Service', 'Financial Examiners', 'Check Cashing and Pay day Loans')
group by 1, 2, 3, 4
order by 6 desc

--pro services top agents policy list (from top 20 agencies, above)
select agent_id,
       agent_name,
       agency_name,
       current_agencytype,
       yearly_premium,
       business_id,
       policy_status_name,
       bundle_lobs,
       cob
from dwh.agents_policies_ds
where start_date >= '2023-01-01'
  and
  --start_date < '2023-01-01' and
    policy_status >= 3
  and
  --(bundle_lobs like '%PL%' or bundle_lobs like '%GL%') and
    cob in ('Insurance Agent', 'Property Manager', 'Business Consulting Services', 'Accountant', 'Technology Services',
            'Engineer', 'Home Inspectors', 'Real Estate Agent', 'Marketing', 'Interior Designer', 'Architect',
            'Real Estate Brokers', 'Legal Service', 'Claims Adjuster', 'Financial Adviser', 'Administrative Support',
            'Notary', 'Product Designer', 'Building Inspector', 'Land Surveyor', 'Mortgage Broker',
            'Insurance Inspector', 'Business Financing', 'Occupational Health and Safety Specialists',
            'Environmental Science and Protection Technicians, Including Health',
            'Telemarketing and Telesales Services', 'Environmental Scientists and Specialists, Including Health',
            'Real Estate Appraisal', 'Securities, Commodities, and Financial Services Sales Agents',
            'Insurance Appraisers', 'Urban and Regional Planners', 'Loan Officers', 'Title Loans',
            'Debt Relief Services', 'Credit Authorizers, Checkers, and Clerks', 'Video Transfer Services',
            'Actuarial Service', 'Financial Examiners', 'Check Cashing and Pay day Loans')
  and agency_name in
      ('AP Intego', 'Insurancebee Inc', 'TruPoint Marketing', 'Renata Reis Insurance, LLC', 'First Connect',
       'Renegade Insurance LLC', 'Univista Insurance', 'D Insurance Group', 'GEICO', 'Jaishree Agrawal', 'AP Intego',
       'TLC Property and Casualty Inc', 'Today Insurance Group LLC',
       'Wilson Mathew Nechikat Insurance Agency Inc. DBA Way.com Insurance Agency', 'Mylo Llc', 'George Jacob',
       'Optimized Insurance', 'INACTIVE_Every Benefits_XReBo1_removed', 'LOYAL INSURANCE GROUP LLC', 'George Jacob')
order by 3

--top consultants agents, top agents
select cob,
       agency_name,
       territory_manager,
       sum(yearly_premium)           as total_premium,
       count(distinct (business_id)) as total_businesses
from dwh.agents_policies_ds
where start_date >= '2023-01-01'
  and
  --start_date < '2023-01-01' and
    policy_status >= 3
  and (bundle_lobs like '%PL%' or bundle_lobs like '%GL%')
  and cob in ('Accountant', 'Actuarial Service', 'Aerial Photography', 'Allergists', 'Alternative Healing',
              'Anesthesiologists', 'Architect', 'Art Consultants', 'Audiologists', 'Ayurveda', 'Biological Technicians',
              'Biomedical Engineers', 'Broadcast Technicians', 'Building Inspector', 'Business Consulting',
              'Business Financing', 'Call Center Service', 'Cardiologists',
              'Cardiovascular Technologists and Technicians', 'Check Cashing and Pay day Loans', 'Claims Adjuster',
              'Colonics', 'Community Health Workers', 'Computer and Information Systems Managers',
              'Computer Network Architects', 'Computer Network Support Specialists', 'Computer Programmers',
              'Computer Repair', 'Concierge Medicine', 'Craft Artists', 'Credit Authorizers, Checkers, and Clerks',
              'Crisis Pregnancy Centers', 'Debt Relief Services', 'Dental and Orthodontic Services',
              'Dental Assistants', 'Dental Hygienists', 'Dentists', 'Dermatologists', 'Diagnostic Imaging',
              'Diagnostic Medical Sonographers', 'Diagnostic Services', 'Dialysis Clinics', 'Doctors', 'Doulas',
              'Ear Nose and Throat', 'Education Consulting', 'Employment Agencies', 'Endocrinologists', 'Endodontists',
              'Engineer', 'Environmental Science and Protection Technicians, Including Health',
              'Environmental Scientists and Specialists, Including Health', 'Epidemiologists', 'Estate Liquidation',
              'Faith-based Crisis Pregnancy Centers', 'Family and General Practitioners', 'Family Practice',
              'Fertility', 'Financial Adviser', 'Financial Examiners', 'Gastroenterologist', 'Gerontologists',
              'Health and Wellness Coaching', 'Health Retreats', 'Hearing Aid Providers', 'Hearing Aid Specialists',
              'Hepatologists', 'Home Energy Auditors', 'Home Health Care', 'Hospice', 'Human Resources Specialists',
              'Immunodermatologists', 'Infectious Disease Specialists', 'Insurance Agent', 'Insurance Appraisers',
              'Insurance Inspector', 'Interior Designer', 'Internal Medicine', 'Internists, General',
              'IT Consulting or Programming', 'Laboratory Testing', 'Land Surveyor', 'Legal Service',
              'Licensed Practical and Licensed Vocational Nurses', 'Loan Officers', 'Logisticians',
              'Magnetic Resonance Imaging Technologists', 'Makerspaces', 'Marriage and Family Therapists',
              'Massage Therapist', 'Medical and Clinical Laboratory Technicians',
              'Medical and Health Services Managers', 'Medical Appliance Technicians', 'Medical Assistants',
              'Medical Records and Health Information Technicians', 'Medical Scientists, Except Epidemiologists',
              'Meditation Instruction', 'Mental Health Counselors', 'Midwives', 'Mortgage Broker', 'Nephrologists',
              'Neurologist', 'Neuropathologists', 'Notary', 'Nurse Anesthetists', 'Nurse Midwives',
              'Nurse Practitioners', 'Nursing Assistants', 'Obstetricians and Gynecologists',
              'Occupational Health and Safety Specialists', 'Occupational Therapists', 'Oncologist', 'Ophthalmologists',
              'Optometrists', 'Orderlies', 'Orthodontists', 'Orthopedists', 'Osteopathic Physicians',
              'Other Consulting', 'Otologists', 'Pain Management', 'Pathologists', 'Pediatric Dentists',
              'Pediatricians', 'Periodontists', 'Phone or Tablet Repair', 'Photographer', 'Physician Assistants',
              'Podiatrists', 'Prenatal/Perinatal Care', 'Preventive Medicine', 'Print Media', 'Printing Services',
              'Proctologists', 'Product Designer', 'Property Manager', 'Prosthodontists', 'Psychiatric Aides',
              'Psychiatrists', 'Psychologists', 'Public Relations Specialists', 'Pulmonologist', 'Radiation Therapists',
              'Radiologists', 'Real Estate Agent', 'Real Estate Appraisal', 'Real Estate Brokers',
              'Real Estate Investor', 'Registered Nurses', 'Rehabilitation Counselors', 'Reporters and Correspondents',
              'Reproductive Health Services', 'Respiratory Therapists', 'Retina Specialists', 'Rheumatologists',
              'Safety Consultant', 'Salesperson', 'Securities, Commodities, and Financial Services Sales Agents',
              'Skilled Nursing', 'Sleep Specialists', 'Speech Training', 'Sports Psychologists', 'Teeth Whitening',
              'Title Loans', 'Toxicologists', 'Traditional Chinese Medicine', 'Training and Development Specialists',
              'Translator', 'Ultrasound Imaging Centers', 'Urologists', 'Vascular Medicine', 'Veterinarians',
              'Video Transfer Services', 'Walk-in Clinics', 'Wedding and Event Venue Rental', 'Writer')
group by 1, 2, 3
order by 5 desc

--UMB direct issue
with t1 as (select distinct offer_creation_time,
                            uw.business_id,
                            business_name,
                            policy_status_name,
                            uw.lob,
                            uw.state_code,
                            uw.cob,
                            uw.city,
                            uw.zip_code,
                            uw.street,
                            json_extract_path_text(answers, 'asked_for_umbrella_excess_liability',
                                                   true)                                             as umbrella_yes_no,
                            json_extract_path_text(answers, 'umbrella_excess_liability_limit',
                                                   true)                                             as requested_umbrella_limit,
                            (CASE
                                 WHEN (uw.affiliate_id = 'N/A' and uw.agent_id = 'N/A') then 'direct'
                                 WHEN (uw.affiliate_id <> 'N/A' and uw.agent_id = 'N/A') then 'affiliate'
                                 else 'agent' end)                                                   as channel,
                            purchased_quote_job_id
            from dwh.underwriting_quotes_data uw
                     join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
                     join (select distinct business_id, purchased_quote_job_id
                           from dwh.quotes_policies_mlob qpm
                           where lob_policy = 'GL') a on uw.business_id = a.business_id
            where channel in ('direct', 'affiliate')
              and uw.lob = 'GL'
              and policy_status >= 3
              and umbrella_yes_no = 'Yes'
              and purchased_quote_job_id is not null
            order by offer_creation_time desc),

     t2 as (select job_id,
                   json_extract_path_text(json_extract_path_text(data_points, 'packageData', true), 'coverages',
                                          true)                                                                      as coveragesJSON,
                   json_extract_path_text(json_extract_path_text(data_points, 'packageData', true), 'version',
                                          true)                                                                      as packageDataVersion
            from s3_operational.rating_svc_prod_calculations
            where lob = 'GL'
              and (coveragesJSON like '%UMBRELLA%' or coveragesJSON like '%EXCESS_LIABILITY%')
              and creation_time > '2023-08-01')

select *
from t1
         left join t2 on t1.purchased_quote_job_id = t2.job_id

--coverage builder renewal issue
WITH t1 AS (select purchased_quote_job_id,
                   business_id,
                   highest_policy_id,
                   highest_status_name,
                   cob,
                   lob_policy,
                   policy_start_date,
                   new_reneweal,
                   highest_status_package
            from dwh.quotes_policies_mlob
            where lob_policy in ('GL', 'PL')
              and highest_policy_status >= 3
              and creation_time >= '2023-09-15'),
     t2 AS
         (select job_id,
                 revision,
                 creation_time,
                 json_extract_path_text(json_extract_path_text(data_points, 'packageData', true), 'version',
                                        true)                                                                        as packageDataVersion,
                 json_extract_path_text(json_extract_path_text(data_points, 'packageData', true), 'coverages',
                                        true)                                                                        as coveragesJSON
          from s3_operational.rating_svc_prod_calculations
          where lob in ('GL', 'PL')
            and creation_time >= '2023-09-15')

select *
from t1
         join t2 ON t1.purchased_quote_job_id = t2.job_id


--coverage builder renewals issue
WITH t1 AS (SELECT business_id
            FROM dwh.quotes_policies_mlob
            WHERE offer_flow_type = 'RENEWAL'
              AND creation_time BETWEEN '2023-09-15' AND '2023-09-30'),
     t2 AS (SELECT purchased_quote_job_id,
                   business_id,
                   highest_policy_id,
                   highest_status_name,
                   cob,
                   lob_policy,
                   policy_start_date,
                   policy_end_date,
                   new_reneweal,
                   offer_flow_type,
                   highest_status_package,
                   state
            FROM dwh.quotes_policies_mlob
            WHERE lob_policy IN ('GL', 'PL')
              AND highest_policy_status >= 3
              AND creation_time >= '2022-06-01'
              AND business_id IN (SELECT business_id FROM t1)
              and offer_flow_type = 'CHANGE_REQUEST'),
     t3 AS (SELECT job_id,
                   revision,
                   creation_time,
                   json_extract_path_text(json_extract_path_text(data_points, 'packageData', true), 'version',
                                          true)                                                  as packageDataVersion,
                   json_extract_path_text(json_extract_path_text(data_points, 'packageData', true), 'coverages',
                                          true)                                                  as coveragesJSON,
                   json_extract_path_text(data_points, 'packageData', 'coverages', 'BASE', 'limits', 'OCCURRENCE',
                                          true)                                                  as gl_base_occ,
                   json_extract_path_text(data_points, 'packageData', 'coverages', 'BASE', 'limits', 'AGGREGATE',
                                          true)                                                  as glpl_base_aggregate,
                   json_extract_path_text(data_points, 'packageData', 'coverages', 'BASE', 'limits', 'PER_CLAIM',
                                          true)                                                  as pl_base_per_claim,
                   json_extract_path_text(data_points, 'packageData', 'coverages', 'CONTRACTOR_ENO', 'limits',
                                          'AGGREGATE',
                                          true)                                                  as gl_contractor_eno_aggregate,
                   json_extract_path_text(data_points, 'packageData', 'coverages', 'LIQUOR_LIABILITY_COVERAGE',
                                          'limits', 'AGGREGATE',
                                          true)                                                  as gl_liquor_liability_aggregate,
                   json_extract_path_text(data_points, 'packageData', 'coverages', 'TRIA', true) as gl_tria
            FROM s3_operational.rating_svc_prod_calculations
            WHERE lob IN ('GL', 'PL')
              AND creation_time >= '2022-06-01')

SELECT *
FROM t2
         JOIN t3
              ON t2.purchased_quote_job_id = t3.job_id


--PL1 SNIC renewals issue
WITH t1 AS (select purchased_quote_job_id,
                   business_id,
                   policy_start_date,
                   offer_flow_type,
                   state,
                   distribution_channel
            from dwh.quotes_policies_mlob
            where lob_policy in ('PL')
              and highest_policy_status >= 3
              and creation_time >= '2023-07-01'),
     t2 AS
         (select job_id, json_extract_path_text(revision, 'scopeValue', true) as ratingRevision
          from s3_operational.rating_svc_prod_calculations
          where lob in ('PL')
            and creation_time >= '2023-07-01'
            and ratingRevision <> 'PL_V2')

select *
from t1
         join t2 ON t1.purchased_quote_job_id = t2.job_id

--albus db (lots of things after albus_prod as well)
select *
from albus_prod.business_detail
limit 10

select *
from albus_prod.search_result
limit 10

--get list of retail / e-comm business names & states
select json_extract_path_text(json_args, 'business_name', true) as biz_name,
       state,
       highest_yearly_premium,
       cob
from dwh.quotes_policies_mlob
where highest_policy_status >= 3
  and cob in ('Restaurant', 'E-Commerce', 'Retail Stores', 'Grocery Store', 'Food Truck', 'Caterer', 'Bakery',
              'Convenience Stores', 'Coffee Shop', 'Party Equipment Rentals', 'Laundromat', 'Farmers Market',
              'AV Equipment Rental for Events', 'Specialty Food', 'Candle Store', 'Medical Supplies Store',
              'Arts and Crafts Store', 'Clothing Store', 'Florist', 'Dumpster Rental')
  and offer_flow_type = 'APPLICATION'
  and creation_time >= '2023-09-20'
order by 3 desc
limit 100

--amazon app
select affiliate_id,
       offer_flow_type,
       extract(year from creation_time) || '-' ||
       right('00' + convert(varchar, extract(month from creation_time)), 2) as creation_year_month,
       count(distinct (business_id)),
       sum(highest_yearly_premium)
from dwh.quotes_policies_mlob
where affiliate_id in ('4700')
  and highest_policy_status >= 3
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and creation_time >= '2023-01-01'
group by 1, 2, 3

--amazon qtp by month (consistent with 2-dim QTP)
with t1 as (select last_day(creation_time)             as month,
                   count(distinct related_business_id) as sold_policy_count
            from dwh.quotes_policies_mlob
            where highest_policy_status >= 3
              and lob_policy = 'GL'
              and offer_flow_type in ('APPLICATION')
              and distribution_channel = 'partnerships'
              and
              --quotes_policies_mlob.affiliate_id = '4700' and
                creation_time >= '2023-01-01'
            group by 1
            order by month asc),

     t2 as (select last_day(creation_time)             as month,
                   count(distinct related_business_id) as quote_count
            from dwh.quotes_policies_mlob
            where lob_policy = 'GL'
              and offer_flow_type in ('APPLICATION')
              and distribution_channel = 'partnerships'
              and
              --quotes_policies_mlob.affiliate_id = '4700' and
                creation_time >= '2023-01-01'
            group by 1
            order by month asc)

select *, cast(sold_policy_count * 1.0 / quote_count * 1.0 as decimal(10, 4)) as qtp
from t1
         join t2 on t1.month = t2.month
order by t1.month asc


--delete find agent id
select *
from db_data_science.v_all_agents_policies
where start_date >= '2023-10-01'
limit 100

--nationwide production by cob
select cob_name, sum(yearly_premium) as total_premium, count(distinct (business_id)) as business_count
from db_data_science.v_all_agents_policies
where agency_aggregator_id = '14'
  and policy_status_name = 'Active'
  and bundle_lobs like '%GL%'
  and start_date >= '2023-07-01'
group by 1
order by 2 desc

--to get smartystreets / address validation/verification failures
select all_steps_related_business_id like '%Questionnaire - Address Validation - failure - correction%' as correction,
       all_steps_related_business_id like '%Questionnaire - Address Validation - failure - invalid%'    as invalid,
       all_steps_related_business_id like '%Questionnaire - Address Validation - failure - retryRange%' as retry,
       all_steps_related_business_id like '%Questionnaire - Address Validation - failure - stateError%' as stateError,
       all_steps_related_business_id like '%Questionnaire - Address Validation - failure - POBox%'      as POBoxError,
       all_steps_related_business_id like '%Questionnaire - Address Validation - success%'              as success,
       all_channels_related_business_id_before_purchase like '%agents%'                                 as agent_channel,
       marketing_cob_group = 'Food & beverage'                                                          as food_bev_lead,
       count(distinct related_business_id)
from dwh.daily_activities
where start_date >= '2023-09-01'
  and start_date < '2023-10-01'
group by 1, 2, 3, 4, 5, 6, 7, 8
order by 2 desc

--legalzoom risks by area
select business_id,
       cob,
       json_extract_path_text(json_args, 'lob_app_json', 'business_premises_area_sq_ft', true) as sqft,
       revenue_in_12_months,
       num_of_employees,
       num_of_owners,
       highest_yearly_premium
from dwh.quotes_policies_mlob
where highest_policy_status >= 3
  and lob_policy = 'GL'
  and offer_flow_type in ('APPLICATION')
  and creation_time >= '2023-01-01'
  and affiliate_id = 8500
  and sqft <> ''

--cp deductible
select json_extract_path_text(data_points, 'packageData', 'coverages', 'BASE', 'limits', 'DEDUCTIBLE',
                              true) as ded_amount
from s3_operational.rating_svc_prod_calculations rc
where lob = 'CP'
  and dateid >= '2023-10-01'
limit 100

--cp package type
select distribution_channel,
       cob_group,
       highest_status_package,
       count(distinct (business_id)) as policy_ct
from dwh.quotes_policies_mlob
where highest_policy_status = 4
  and lob_policy = 'CP'
  and offer_flow_type in ('APPLICATION')
  and creation_time >= '2023-09-22'
  and
  --distribution_channel <> 'agents' and
    cob_group = 'Retail'
group by 1, 2, 3
order by 4 desc

--cp package type
select extract(month from creation_time) as creation_month,
       count(distinct (business_id))     as policy_ct
from dwh.quotes_policies_mlob
where highest_policy_status = 4
  and lob_policy = 'CP'
  and offer_flow_type in ('APPLICATION')
  and creation_time >= '2023-01-01'
  and distribution_channel <> 'agents'
  and cob_group = 'Retail'
group by 1
order by 1 asc

--qtp by month (consistent with 2-dim QTP)
with t1 as (select last_day(creation_time)             as month,
                   count(distinct related_business_id) as sold_policy_count
            from dwh.quotes_policies_mlob
            where highest_policy_status >= 3
              and lob_policy = 'CP'
              and offer_flow_type in ('APPLICATION')
              and distribution_channel <> 'agents'
              and cob_group = 'Retail'
              and creation_time >= '2020-01-01'
              and creation_time < '2023-10-01'
            group by 1
            order by month asc),

     t2 as (select last_day(creation_time)             as month,
                   count(distinct related_business_id) as quote_count
            from dwh.quotes_policies_mlob
            where lob_policy = 'CP'
              and offer_flow_type in ('APPLICATION')
              and distribution_channel <> 'agents'
              and cob_group = 'Retail'
              and creation_time >= '2020-01-01'
              and creation_time < '2023-10-01'
            group by 1
            order by month asc)

select *, cast(sold_policy_count * 1.0 / quote_count * 1.0 as decimal(10, 4)) as qtp
from t1
         join t2 on t1.month = t2.month
order by t1.month asc

select distribution_channel, agent_id
from dwh.quotes_policies_mlob
where business_id = '0bddd95a308805581c9b47627fca9fcd'
  and creation_time >= '2023-09-30'

select business_id, cob, lob, offer_creation_time, policy_status_name
from dwh.underwriting_quotes_data
where start_date >= '2023-08-01'
  and agent_id = '2edqnHtPALP0Nldt'

select distinct business_id
from dwh.underwriting_quotes_data
where start_date >= '2023-08-01'
  and agent_id = '2edqnHtPALP0Nldt'


select lob_policy, count(distinct (business_id))
from dwh.quotes_policies_mlob
where offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and highest_policy_status = 4
  and cob = 'E-Commerce'
  and creation_time >= '2021-06-01'
group by 1

select highest_policy_status, highest_status_name
from dwh.quotes_policies_mlob
where creation_time >= '2023-10-08'
group by 1, 2
limit 10

select extract(year from creation_time) || '-' ||
       right('00' + convert(varchar, extract(month from creation_time)), 2) as creation_year_month,
       count(distinct (business_id))                                        as quote_count
from dwh.quotes_policies_mlob
where offer_flow_type in ('APPLICATION')
  and lob_policy = 'CP'
  and highest_policy_status >= 1
  and creation_time >= '2020-06-01'
--cob in ('Personal Trainer','Fitness Instructor','Yoga Instructor','Crossfit Instructor','Pilates Instructor','Indoor Cycling Instructor','Aerobics Instructor','Zumba Instructor','Auto Parts Store','Craft Artists','Etchers and Engravers','Set and Exhibit Designers','Community Gardens','Printing Services','Audio and Video Equipment Technicians','Camera and Photographic Equipment Repairers','Photo Editing, Scanning and Restoration','Art Space Rentals','Locksmith','Musical Instrument Services','AV Equipment Rental for Events','Medical Supplies Store','Knife Sharpening','Scavenger Hunts','Vending Machine Operator','Print Binding and Finishing Workers','Glass Blowing','Wedding and Event Invitations','Screen Printing and T Shirt Printing','Print Media','Retail Stores','Bike Shop','Bike Rentals','Bookstore','Newspaper and Magazine Store','Clothing Store','Department Stores','Discount Store','Electronics Store','Fabric Store','Furniture Store','Baby Gear and Furniture Store','Hardware Store','Arts and Crafts Store','Hobby Shop','Candle Store','Home and Garden Retailer','Lighting Store','Jewelry Store','Packing Supplies Store','Flea Markets','Nurseries and Gardening Shop','Eyewear and Optician Store','Paint Stores','Pet Stores','Furniture Rental','Sporting Goods Retailer','Fitness and Exercise Equipment Store','Horse Equipment Shop','Luggage Store','Pawn Shop','Toy Store','Demonstrators and Product Promoters','Fitness Studio','Fencing Instructor','Sports Coach','Umpires, Referees, and Other Sports Officials','Martial Arts Instructor')
group by 1
order by 1 asc

select *
from underwriting_svc_prod.prospects
limit 10

--get classy a/b test businesses
with user_base as
         (select distinct aa.tracking_id, sa.related_business_id, sa.business_id
          from "experiments_svc_prod".ab_tests t
                   inner join "experiments_svc_prod".ab_test_variants v on t.ab_test_id = v.ab_test_id
                   inner join dwh.all_activities_table aa on aa.ab_test_variant_id = v.ab_test_variant_id and
                                                             aa.funnelphase = 'Adding user to a/b test group'
                   left join dwh.sources_attributed_table sa on aa.tracking_id = sa.tracking_id
                   inner join next_insurance_prod.cookied_users cu on cu.id = sa.cookied_user_id
          where t.ab_test_id = 1401
            and eventtime >= '2023-09-01')

select distinct s.business_id,
                a.tracking_id,
                a.funnelphase,
                s.cob_name,
                json_extract_path_text(a.interaction_data, 'classyRequestId') as ClassyID,
                json_extract_path_text(a.interaction_data, 'selectedCob')     as SelectedCOB,
                json_extract_path_text(p.business_details, 'businessname')    as business_name,
                qpm.street,
                qpm.city,
                qpm.state,
                qpm.zip_code,
                qpm.cob,
                json_extract_path_text(p.business_details, 'emailaddress')    as business_email,
                max(eventtime)                                                as eventtime,
                max(p.creation_time)                                          as creation_time,
                qpm.highest_policy_status
from dwh.all_activities_table a
         inner join dwh.sources_attributed_table s on a.tracking_id = s.tracking_id
         left join user_base u on u.related_business_id = s.related_business_id
         left join underwriting_svc_prod.prospects p on p.business_id = s.business_id
         left join dwh.quotes_policies_mlob qpm on qpm.business_id = p.business_id
where 1 = 1
  and eventtime >= '2023-09-01'
  and a.funnelphase in
      ('cob_classification_COMPLETE - CHANGE lead_form_cob', 'cob_classification_FAILED - CHANGE lead_form_cob')
  and s.business_id is not null
  and qpm.highest_policy_status >= 3
group by s.business_id,
         a.tracking_id,
         a.funnelphase,
         s.cob_name,
         json_extract_path_text(a.interaction_data, 'classyRequestId'),
         json_extract_path_text(a.interaction_data, 'selectedCob'),
         json_extract_path_text(p.business_details, 'businessname'),
         qpm.street,
         qpm.city,
         qpm.state,
         qpm.zip_code,
         qpm.cob,
         json_extract_path_text(p.business_details, 'emailaddress'),
         qpm.highest_policy_status
order by business_id

select *
from dwh.quotes_policies_mlob
where highest_policy_status >= 3
limit 10


--backtest high risk keywords
select distinct p.business_id,
                qpm.cob,
                json_extract_path_text(p.business_details, 'businessname') as business_name,
                json_extract_path_text(p.business_details, 'emailaddress') as business_email
from underwriting_svc_prod.prospects p
         left join dwh.quotes_policies_mlob qpm on p.business_id = qpm.business_id
where qpm.creation_time >= '2023-01-01'
  and (business_name like '%industrial%' or business_name like '%Industrial%' or business_email like '%industrial%')
  and cob_group in ('Retail')
  and highest_policy_status >= 3



with user_base as
         (select distinct aa.tracking_id, sa.related_business_id, sa.business_id
          from "experiments_svc_prod".ab_tests t
                   inner join "experiments_svc_prod".ab_test_variants v on t.ab_test_id = v.ab_test_id
                   inner join dwh.all_activities_table aa on aa.ab_test_variant_id = v.ab_test_variant_id and
                                                             aa.funnelphase = 'Adding user to a/b test group'
                   left join dwh.sources_attributed_table sa on aa.tracking_id = sa.tracking_id
                   inner join next_insurance_prod.cookied_users cu on cu.id = sa.cookied_user_id
          where t.ab_test_id = 1401
            and eventtime >= '2023-09-01')

select distinct s.business_id,
                a.tracking_id,
                a.funnelphase,
                s.cob_name,
                json_extract_path_text(a.interaction_data, 'classyRequestId') as ClassyID,
                json_extract_path_text(a.interaction_data, 'selectedCob')     as SelectedCOB,
                json_extract_path_text(p.business_details, 'businessname')    as business_name,
                p.email,
                max(eventtime)                                                as eventtime,
                max(p.creation_time)                                          as creation_time
from dwh.all_activities_table a
         inner join dwh.sources_attributed_table s on a.tracking_id = s.tracking_id
         left join user_base u on u.related_business_id = s.related_business_id
         left join underwriting_svc_prod.prospects p on p.business_id = s.business_id
where 1 = 1
  and eventtime >= '2023-09-01'
  and a.funnelphase in
      ('cob_classification_COMPLETE - CHANGE lead_form_cob', 'cob_classification_FAILED - CHANGE lead_form_cob')
  and s.business_id is not null
group by s.business_id,
         a.tracking_id,
         a.funnelphase,
         s.cob_name,
         json_extract_path_text(a.interaction_data, 'classyRequestId'),
         json_extract_path_text(a.interaction_data, 'selectedCob'),
         json_extract_path_text(p.business_details, 'businessname'),
         p.email
order by business_id


--coverage builder limits selection PL
with t1 as (select purchased_quote_job_id,
                   business_id,
                   highest_policy_id,
                   highest_status_name,
                   cob,
                   lob_policy,
                   policy_start_date,
                   policy_end_date,
                   offer_flow_type,
                   highest_status_package,
                   state
            from dwh.quotes_policies_mlob
            where highest_policy_status >= 3
              and creation_time >= '2022-09-20'
              and offer_flow_type = 'APPLICATION'
              and distribution_channel = 'agents'
              and lob_policy = 'PL'),
     t2 as (select job_id,
                   revision,
                   creation_time,
                   json_extract_path_text(json_extract_path_text(data_points, 'packageData', true), 'version',
                                          true)                         as packageDataVersion,
                   json_extract_path_text(json_extract_path_text(data_points, 'packageData', true), 'coverages',
                                          true)                         as coveragesJSON,
                   --json_extract_path_text(data_points,'packageData','coverages','BASE','limits','OCCURRENCE', true) as gl_base_occ,
                   --json_extract_path_text(data_points,'packageData','coverages','BASE','limits','AGGREGATE', true) as glpl_base_aggregate,
                   json_extract_path_text(data_points, 'packageData', 'coverages', 'BASE', 'limits', 'PER_CLAIM',
                                          true)                         as pl_base_per_claim,
                   json_extract_path_text(data_points, 'packageData', 'coverages', 'BASE', 'limits', 'AGGREGATE',
                                          true)                         as pl_base_aggregate,
                   json_extract_path_text(data_points, 'packageData', 'coverages', 'BASE', 'limits',
                                          'PER_CLAIM_DEDUCTIBLE', true) as pl_base_deductible
            --json_extract_path_text(data_points,'packageData','coverages','CONTRACTOR_ENO','limits','AGGREGATE', true) as gl_contractor_eno_aggregate,
            --json_extract_path_text(data_points,'packageData','coverages','LIQUOR_LIABILITY_COVERAGE','limits','AGGREGATE', true) as gl_liquor_liability_aggregate,
            --json_extract_path_text(data_points,'packageData','coverages','TRIA', true) as gl_tria
            from s3_operational.rating_svc_prod_calculations
            where creation_time >= '2023-09-20')

select *
from t1
         join t2 on t1.purchased_quote_job_id = t2.job_id


--coverage builder limits selection GL
with t1 as (select purchased_quote_job_id,
                   business_id,
                   highest_policy_id,
                   highest_status_name,
                   cob,
                   lob_policy,
                   policy_start_date,
                   policy_end_date,
                   offer_flow_type,
                   highest_status_package,
                   state,
                   json_args
            from dwh.quotes_policies_mlob
            where highest_policy_status >= 3
              and creation_time >= '2023-09-28'
              and offer_flow_type = 'APPLICATION'
              and distribution_channel = 'agents'
              and lob_policy = 'GL'),
     t2 as (select job_id,
                   revision,
                   creation_time,
                   json_extract_path_text(json_extract_path_text(data_points, 'packageData', true), 'version',
                                          true)                                                                       as packageDataVersion,
                   json_extract_path_text(json_extract_path_text(data_points, 'packageData', true), 'coverages',
                                          true)                                                                       as coveragesJSON,
                   json_extract_path_text(data_points, 'packageData', 'coverages', 'BASE', 'limits', 'OCCURRENCE',
                                          true)                                                                       as gl_base_occ_limit,
                   json_extract_path_text(data_points, 'packageData', 'coverages', 'BASE', 'limits', 'AGGREGATE',
                                          true)                                                                       as glpl_base_agg_limit,
                   --json_extract_path_text(data_points,'packageData','coverages','BASE','limits','PER_CLAIM', true) as pl_base_per_claim,
                   --json_extract_path_text(data_points,'packageData','coverages','BASE','limits','AGGREGATE', true) as pl_base_aggregate,
                   --json_extract_path_text(data_points,'packageData','coverages','BASE','limits','PER_CLAIM_DEDUCTIBLE', true) as pl_base_deductible
                   json_extract_path_text(data_points, 'packageData', 'coverages', 'CONTRACTOR_ENO', 'limits',
                                          'AGGREGATE',
                                          true)                                                                       as gl_contractor_eno_agg_limit,
                   json_extract_path_text(data_points, 'packageData', 'coverages', 'CONTRACTOR_ENO', 'limits',
                                          'OCCURRENCE',
                                          true)                                                                       as gl_contractor_eno_occ_limit,
                   json_extract_path_text(data_points, 'packageData', 'coverages', 'LIQUOR_LIABILITY_COVERAGE',
                                          'limits', 'AGGREGATE',
                                          true)                                                                       as gl_liquor_liability_agg_limit,
                   json_extract_path_text(data_points, 'packageData', 'coverages', 'MEDICAL_EXPENSES', 'limits',
                                          'PER_PERSON',
                                          true)                                                                       as gl_medpay_limit,
                   json_extract_path_text(data_points, 'packageData', 'coverages', 'UMBRELLA', 'limits', 'OCCURRENCE',
                                          true)                                                                       as gl_umb_limit,
                   json_extract_path_text(data_points, 'packageData', 'coverages', 'CONTRACTOR_ENO',
                                          true)                                                                       as gl_contractor_eno_yesno,
                   json_extract_path_text(data_points, 'packageData', 'coverages', 'TRIA',
                                          true)                                                                       as gl_tria_yesno,
                   json_extract_path_text(data_points, 'packageData', 'coverages', 'UMBRELLA_COVERAGE',
                                          true)                                                                       as gl_umb_yesno,
                   json_extract_path_text(data_points, 'packageData', 'coverages', 'EXCESS_LIABILITY_COVERAGE',
                                          true)                                                                       as gl_xlia_yesno,
                   json_extract_path_text(data_points, 'packageData', 'coverages', 'PROFESSIONAL_LIABILITY_CONSULTING',
                                          true)                                                                       as gl_pl_consulting_yesno,
                   json_extract_path_text(data_points, 'packageData', 'coverages',
                                          'EXPANDED_DAMAGE_RENTED_PREMISES_COVERAGE',
                                          true)                                                                       as gl_tenant_legal_yesno,
                   json_extract_path_text(data_points, 'packageData', 'coverages', 'ABUSE_AND_MOLESTATION',
                                          true)                                                                       as gl_abuse_yesno,
                   json_extract_path_text(data_points, 'packageData', 'coverages', 'LIQUOR_LIABILITY_COVERAGE',
                                          true)                                                                       as gl_liquor_yesno,
                   json_extract_path_text(data_points, 'packageData', 'coverages', 'ASR_FAULTY_WORK_COVERAGE',
                                          true)                                                                       as gl_faulty_work_yesno,
                   json_extract_path_text(data_points, 'packageData', 'coverages', 'VEHICLE_SERVICES_COVERAGE',
                                          true)                                                                       as gl_garagekeepers_yesno,
                   json_extract_path_text(data_points, 'packageData', 'coverages',
                                          'PRODUCT_WITHDRAWAL_EXPENSE_COVERAGE',
                                          true)                                                                       as gl_product_recall_yesno,
                   json_extract_path_text(data_points, 'packageData', 'coverages', 'LIABILITY_LOST_KEY_COVERAGE',
                                          true)                                                                       as gl_lost_key_yesno
            from s3_operational.rating_svc_prod_calculations
            where creation_time >= '2023-09-28')

select *
from t1
         join t2 on t1.purchased_quote_job_id = t2.job_id

--CP property deductibles
select job_id,
       json_extract_path_text(data_points, 'packageData', 'coverages', 'BASE', 'limits', 'DEDUCTIBLE',
                              true) as cp_property_deductible
from s3_operational.rating_svc_prod_calculations
where job_id = '110846307'
  and --remove this in lieu of a search to get all relevant job IDs (see above)
    creation_time >= '2023-10-09'


--Sebastian / Heidi UMB query for UMB metrics dash
with quote as (SELECT a.offer_id,
                      a.quote_job_id,
                      a.business_id,
                      a.policy_status_name,
                      a.lob,
                      a.state_code,
                      a.cob,
                      c.marketing_cob_group,
                      a.offer_creation_time,
                      a.execution_status,
                      a.bundle_name,
                      a.new_renewal,
                      a.bind_date,
                      a.policy_reference,
                      a.umbrella_agg_limit,
                      b.agency_aggregator_name,
                      b.agency_aggregator_id,
                      b.current_agencytype,
                      a.policy_status,
                      a.yearly_premium                                                           as gl_premium,
                      case when a.offer_creation_time >= '2023-07-17' then 'post' else 'pre' end as pre_post,
                      umbrella_premiums._gl_umbrella_excess_liability_total_premium::float       as umb_premium,
                      CASE WHEN a.umbrella_premiums is not null then 'UMB' ELSE 'Non-UMB' END    as umbrella,
                      CASE
                          WHEN a.agent_id <> 'N/A' and b.agency_aggregator_id <> 38 then 'Agent'
                          WHEN a.affiliate_id <> 'N/A' then 'Partnership'
                          ELSE 'Direct' END                                                      as channel
               --row_number() over (partition by business_id order by offer_creation_time desc, policy_status desc) as rnk
               FROM dwh.underwriting_quotes_data a
                        JOIN dwh.sources_test_cobs c on a.cob = c.cob_name
                        LEFT JOIN dwh.v_agents b on a.agent_id = b.agent_id
               WHERE a.offer_creation_time >= '2023-07-17'
                 --and a.offer_creation_time <<= '2023-09-05'
                 AND a.offer_flow_type in ('APPLICATION')
                 AND a.lob = 'GL'),
     base as (select business_id,
                     policy_status_name,
                     lob,
                     state_code,
                     cob,
                     marketing_cob_group,
                     date(offer_creation_time) offer_creation_ds,
                     execution_status,
                     new_renewal,
                     umbrella_agg_limit,
                     agency_aggregator_name,
                     current_agencytype,
                     policy_status,
                     gl_premium          as    gl_and_umb_premium,
                     umb_premium,
                     channel,
                     case
                         when state_code in ('AL', 'AR', 'AZ', 'CT', 'DE', 'FL', 'HI', 'IA', 'ID',
                                             'IN', 'KS', 'KY', 'LA', 'MA', 'ME', 'MI', 'MN', 'MO', 'MT',
                                             'ND', 'NH', 'NJ', 'NM', 'OK', 'OR', 'RI', 'SC', 'SD', 'TN',
                                             'VA', 'VT', 'WI', 'WV', 'WY', 'IL', 'NV', 'TX', 'UT', 'CO', 'PA', 'MS'
                             ) then 'Batch1'
                         when state_code in ('NC', 'AK', 'GA', 'DC', 'OH', 'NJ', 'MD') then 'Batch2'
                         else 'Rest' end as    batches,
                     max(policy_status)        max_pol_status
              from quote
              group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17),
     umb_quoter as (select business_id
                    from base
                    where umb_premium is not null),
     bundled_premium as (select business_id,
                                sum(yearly_premium) as bundled_premium
                         from dwh.company_level_metrics_ds
                         where eventtime >= '2023-07-20'
                         group by 1)
select a.*,
       b.bundled_premium,
       case when a.business_id in (select * from umb_quoter) then 1 else 0 end as umb_quoter
from base a
         left join bundled_premium b
                   on a.business_id = b.business_id



select *
from nimi_svc_prod.policies a
         join nimi_svc_prod.policy_types p on a.policy_type_id = p.policy_type_id
         join nimi_svc_prod.policy_statuses b on b.status_id = a.policy_status
where code = 'CA'
  and policy_status in (3, 4, 7)
order by end_date

select lob_policy, count(distinct (business_id)) as biz_number, sum(highest_yearly_premium) as prem_in_force
from dwh.quotes_policies_mlob
where cob in ('Art Consultants', 'Business Consulting', 'Education Consulting', 'IT Consulting or Programming',
              'Other Consulting', 'Safety Consultant')
  and highest_policy_status = 4
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
group by 1
order by 2 desc

--direct TRIA policy impact
SELECT pid, bundle_name, data, overrides, prev_policy_id
FROM (SELECT *
      FROM (SELECT policy_id pid, bundle_name, prev_policy_id
            FROM (SELECT *
                  FROM nimi_svc_prod.bundles b
                  WHERE b.bundle_name LIKE '%Tria'
                    AND b.creation_time >= '2023-09-18'
                    AND b.creation_time <= '2023-09-30') bundles
                     JOIN nimi_svc_prod.policies p
                          ON p.bundle_id = bundles.bundle_id) pb
               JOIN nimi_svc_prod.package_data pd
                    ON pd.policy_id = pb.pid
      WHERE data NOT LIKE '%TRIA":{}%') pbpd
         LEFT JOIN nimi_svc_prod.package_data_overrides pdo
                   ON pdo.policy_id = pbpd.pid
WHERE (overrides NOT LIKE '%TRIA%' OR overrides IS NULL)
  AND prev_policy_id IS NULL

--TRIA issue policy info
select p.business_id,
       json_extract_path_text(p.business_details, 'businessname')       as business_name,
       json_extract_path_text(p.business_details, 'emailaddress')       as business_email,
       json_extract_path_text(p.business_details, 'applicantfirstname') as firstname,
       json_extract_path_text(p.business_details, 'applicantlastname')  as lastname
from underwriting_svc_prod.prospects p
where p.business_id in
      ('c196a3d12d616bec63d8d6c0d8c95a05', '95ce14467341c2b8f140aebc59206c4c', '63829fb605216c6eda81fa07466d50a0',
       'e63f07404570165e540584aefb558741', '1ec87deeea20123d666026e00c8a6dfb', 'd7da0099a1c8d33cb29f6a7989e476cf',
       '97d1fed5d8e3704c61348c5ddeee246d', '26272ed4c5293c5211af2a54388479b6', '19942f15876a1f4e5124b563450b844c',
       'ac7a3542822051940c81438a10bc2f1e', 'e76d46d8ebcacfc53fe3d6f17f977738', '11e4656dff957023178d9ba2b2cf5a4a',
       'c54a8fe3db93db7e313c3c544b1e29b8', 'ca9c399d2181155719276988b4b5bc78', '87a153e399c806e64bbb77db82f6057e',
       'bfcfcd3e4e71ad06aea68656244399e0', '48b324d0ca5afa8aaf24c233705c4d75', '2801b1ac172cc1d74c494d02ac002b60',
       'ebaa011b209bbb6be47bbecb4e31b06f', 'c62790173ef86f67221a74b766e7bdd7', 'f730fbc48b768efdc211b7ba33899002',
       'd4f390024f5aea02d287d402f10ac371', '7bae3e61be4e38242e0ada2accf56108', 'ee86ba59b70bb801e299efab33f2bf3d',
       '90c35fa49b0d46a64d3adf02b11a8d4f', '6324c801b5606a4112aa6f57f964d493', 'b406df93e757409f4ce57e87c3f082d3',
       '05c234b0717e5bb4700dc8a6d6b7ad2a', '02118ab88576a4d61ff3a2812deabc9a', 'e28c5d49078337c9df71445a9d3312af',
       'b87af17f1fba1832397c639895442661', '37253c21c1a61a4a327da8f54850c72b', '7fd0a16494aeaf08abcaae87a021abae',
       '8292393c1e4d8b985784ddc9e826e2c9', '1b93451c430a92fdb7f859962e1bd3fe', 'd01a689f51254ed4f05ac14275c5dbe7',
       '02ce63a0b73bc6fb9c34ad7cac395a66', '407dcc726cc094fd84b3b317de689cb8', '06ea81ae9a04f423d73bb20c925f8887',
       '3506d136e8f9bd380018ddf9778cffc2', '94d97dcbf0d6dc94be0f81ed83631d17', 'b6e1d55ba23d9bd1228e5de49a9bda20',
       '1e11cca5dfe0d9de65a1c7e298ec6084', 'ee1952de8de18ef31e076a066cb369d7', '5df7c2450effc8876cbbab4eef836fc8',
       '8653bb982117ac583c6f6c89d2057e03', '68087729b5176877596fe8b7cd7cfbce', '07aae81b7be38b75f3041a7ff170a6b7',
       '5503c57b5bc32ff88c5e188de6de380c', 'db6f50a89e392c69fc4cb0f781f6c3d4', '1b6da830705d329dadd2295c0a736671',
       'dbf802cc8e2ac931a676875d7dfeff76', '7bcf9883340ecdce30ccf0be43339f4c', '28eda3f44f901c127bd9358ee3482a00',
       '0c6afc3d8332ed17e82b06e9afca091f', '718fb8b44b39cf62c3ec712637aaeade', 'c8a90b9037bae2dce4590b3314cfec40',
       '1da8814aa2ca1277a1eb6e999c6abfcd', '3df4a79409752550b89ce1e84fd4aa47', '8f8b1ad8588ab6c6b3cc8c5955f143b2',
       '33795a8003e05dba60061b7a9fe480a8', '959a54e5e054cf401252df63bef7ff53', '0720177370c5b1676915b8e627043fa3',
       'c72b546a6d5d1977c4b2d2ef5c143947', 'd38c1eef846f8d417b0a51ea6f4f8842', '60668e13c63762693cfe3787a0d8309e',
       '6671704353b3e3dd494b6ea94930b4e4', '67147dd72b675155fdb9ce89d214a76a', '17013e82c500c5080f5b24dd3748c2b5',
       '7b76d526e98114c9286c02fa7da66a94', '82ad1d71fb383fd445f1eb9556a49e11', '46ac789077b2ce53da4bac595519e02f',
       '8588dfce584d921c1cdf90cc9f981883', 'acae17fb7fbf3c5d780cce6e1af7b1aa', '0ffdf5a37a6fca54c4ef4ca4cccd74f8',
       '607f10da6cf7cdd7e7c37979a9ca6a03', '667fbaa225956da5bbc5ca80e115827a', 'f39611b239ed4d721db53c821f567ccc',
       '227fdb438078a5024b510327ebfd4d59', '751362750e4861741d400c8804767ec4', 'd7b49cb6bec6b58656306b04ecc52dec',
       'a5ecb6d120d6c87ffb21631224fcac4e', '6de44785d5087469e061ec4b5bc6f61b', 'f4ba3a4c6ebc69e633a7a5606e80990a',
       'b106190491cde6c73c28efd4c07f9d7e', 'f3586144af59f78b03d6852fde3358fa', '7621d271c0728a263c004eaf30fdf4da',
       '74fc96dc8c2ae2a5582bb64be8612c7d')

--coverageBuilder issue
select p.business_id,
       json_extract_path_text(p.business_details, 'businessname')       as business_name,
       json_extract_path_text(p.business_details, 'emailaddress')       as business_email,
       json_extract_path_text(p.business_details, 'applicantfirstname') as firstname,
       json_extract_path_text(p.business_details, 'applicantlastname')  as lastname
from underwriting_svc_prod.prospects p
where p.business_id in
      ('cd047063a7d5099e853f616d959f4dab', '90777d67ebe72793b36cbde193ba4abc', 'bb5eb71d9c116c3c982c3fba109bb748',
       'eff658879e982f49a3e2f0c14805fac3', '9963ecaad7870645ccde1d49b0a31f7b', '7b257f925eb7b4bfbf7394d278168964',
       'd541b39bfac23507e238c9b13b3b2f3e', '4869b4cdc31367278fc6891e395c8f11', '518249b9efc3f49cc8519983ae86f656',
       '608f09569157e51d930f57f7f2faaca7', '59f464df050d3c075b69bab716fcbb0a', '8c69314c4f39a3d196e392ba70f85dec',
       '50e3a7143e488913919551681faa8c0e', 'b7b1e239e430cfd0c8c2d9e6119660ac', '5242b63285bfd5f954f205d0a04e27ee',
       'e7139100e44a72914a397d39ebbbd257', 'd3ad19754e23434d64fdd8f16e88f84a', '3b072c1ddfceb37022ce42fad8b604c6',
       'b89db1f87cd0dfba581a65eeaa91af2b', '8e757efd0d765a2cab1234eb2e539237', '13fc557a23cb74c76ba6ac10017ecea0')

select email_address, first_name, last_name, business_id
from nimi_svc_prod.contacts
where business_id in
      ('cd047063a7d5099e853f616d959f4dab', '90777d67ebe72793b36cbde193ba4abc', 'bb5eb71d9c116c3c982c3fba109bb748',
       'eff658879e982f49a3e2f0c14805fac3', '9963ecaad7870645ccde1d49b0a31f7b', '7b257f925eb7b4bfbf7394d278168964',
       'd541b39bfac23507e238c9b13b3b2f3e', '4869b4cdc31367278fc6891e395c8f11', '518249b9efc3f49cc8519983ae86f656',
       '608f09569157e51d930f57f7f2faaca7', '59f464df050d3c075b69bab716fcbb0a', '8c69314c4f39a3d196e392ba70f85dec',
       '50e3a7143e488913919551681faa8c0e', 'b7b1e239e430cfd0c8c2d9e6119660ac', '5242b63285bfd5f954f205d0a04e27ee',
       'e7139100e44a72914a397d39ebbbd257', 'd3ad19754e23434d64fdd8f16e88f84a', '3b072c1ddfceb37022ce42fad8b604c6',
       'b89db1f87cd0dfba581a65eeaa91af2b', '8e757efd0d765a2cab1234eb2e539237', '13fc557a23cb74c76ba6ac10017ecea0')

select distinct qpm.business_id,
                qpm.distribution_channel,
                qpm.agent_id,
                agent_name,
                current_agencytype,
                agency_name,
                agent_email_address,
                agency_aggregator_name
from dwh.quotes_policies_mlob qpm
         left join dwh.v_agents a on qpm.agent_id = a.agent_id
where qpm.business_id in ('b89db1f87cd0dfba581a65eeaa91af2b')

--find missing agent policies
select business_id,
       agent_id,
       agent_name,
       current_agencytype,
       agency_name,
       agent_email_address,
       agency_aggregator_name
from db_data_science.v_all_agents_policies
where business_id in ('0209c73bf57701b1695044790fa63140')

--end dates
select business_id, policy_end_date
from dwh.quotes_policies_mlob
where highest_policy_status = 4
  and lob_policy in ('GL', 'PL')
  and business_id in
      ('4b2b7fcd7f9b6ce7b1a0e4ad23a8bfe0', 'fd150fea2927378c6f338838f2f8691d', 'e220d41903d63e572c0350ab22a84dcf',
       'cf2a51769c1ea972bf090927cfe8ab5e', '13e140cdd9804b36371077bc80d52c7a', '86b7161169625d55a158cc0989fcceec',
       '25c0c06a3a27a7bfa691322adf9732b4', '7fc5e907001ca5a4ae118aab1aeb1ccc', '563367096dff12c20f4c756a4cebe97f',
       '48db3a02b5a4ce7be2878d6f933b26f7', 'ee2425e0ed7719220a7932c6e4886324', 'a2dd68893e06e68bfa93e064b78a096c',
       '56195b08b8215f0d325ee3440301bc3f', '76ef90f3ca53fbe3d0148aa714fbcce7', 'f31a86cd2cddf47fcf8dd5256f1a6266',
       'a9a5802e03a82b9916dfa9be19bfb4af', '11450b373cb53f2938c3bfd517565e69', 'e9eec90b59953b59f5f8ab78a82b8d16',
       '811637ccaa409534336c2fb4b624c982', '52f2b1607bb3afe666abf98f52cd6a39', '1c9f77f043d7797a344d9e426af8c6cc',
       'bd84a081cf9d67bd8ac7f9f167f8dad7', 'f3804c53bf6a65f1a60edf924fb814fa', 'da7f3af93f0e64061806fb996795199f',
       'b6a1066c07526a6a61b52bafa3d57561', 'f26a5af228197deade49def6b44754b9', '10002edef7eda896fd076c61e93f820b',
       'eebc754f1e1554377925ed4ecc879833', 'a22ab6d6d48a29884c0b8e9459de1482', 'd553437065c89765eed8802da49b6b63',
       '28531c78088836628c84b3dab01d046d', '6f5c09c88e25ae2aa48c3fdc255bcf15', '3a4daedb9ab84246924438f379a08bb4',
       '285208121881d7caa2f7c4abaa690387', 'abedcd6d17904e69d5e67d12865f83b3', 'c73a9a759926d34433d2e68f09a14cf1',
       '880c704bd082a7116603242891e12954', '01c7aaeb513866765ecfd9b2f5e460f6', '241dfdc6c12465199a783ac64ceef4a1',
       '3c475c2c85bc330588070f7c5a5b79f0', 'e4aff56193afe7c4f8d1ad9df8f06f42', '8a985b2f7d6f991fc217d4a2d7911daf',
       '786534be8c166dfff57c3ce06be956a2', 'd6db7e10728adf0b325eeea2eb88a1ec', '315e11fbe9f40f5cd9314ab53840cd7a',
       '392716ec30cb34ed25d9d27cb44f4ef2', 'e65ab14b3ca480b4745db9c24ec46e94', 'ab67a9c954116506573a376fb38f96de',
       '4ad76f18298e8d751598765ce2851b97', 'f54fc543a2d1e067c2cbea2dcc7400f6', '7486c574ed39d765b7fdc1ff843a94ad',
       '546e8cefb677f0935e7ab5550fad06e0', '18bdcac54081695fc286f5f54648a757', 'b32e58f462f5072d231fc141857a505b',
       '03c99160e2ee910b558ae9d6ef699c6b', '40453596b7672446281f089fefdb101c', 'deee1e06e5578f59cab65003cfad1ba4',
       'fde6cce3f58c7e57a878851506e7d558', 'de01f27ff2ed38867aae633100e879ea', 'c8bc1e5ad534ed77fe8619618dae144a',
       'a1d8b6a817b383c753940b1bf33c2b57', '2b15b7813f02c3cb2e9385ade402f534', 'a620839ecfc372cc83a13bd8f6408b68',
       '984b7072473e920efa316c050bc0f9ad', 'cd10c2533fa44494ca43d3aa8ca57dea', 'ff93d78c7a781c8d155d39a698248f86',
       '0e414389f382586fc4687bf8348701ee', 'c8cac58b2db87287c46f6639afa7c2ad', '7b8633c12ddc4691d5c9278c0b0b2bb7',
       'b5ff46f194116b543ddd64434461c9ca', 'ddf5c3fba9414cc5f26d3990928ec8b9', 'def62c9ec3f3d0c42a7c0e8d459f3fbb',
       'c62413a5a75df4727507aa59c0da4b16', 'e0c4515b140de3597b8f6573dc84ecad', '257e07cdbf3d9f971ae4d1caa41e8ca0',
       '42ca24529519ee6092e59a9c976639b6', '48a6ea483e417d04e69bbaa2408ca72e', '6bef07fb0b2a712bca6ce1641d210779',
       '632324cc5331887b6fb269389a01d270', '412a95f3ed4366e494e96a5b3bb71937', '2145392f305b60c134fe3d0df908879b',
       '41776be03a29d1b1faf2a8ca3ee80dbd', '0cbbe5ddc5e7e1e059a455e40aa2d6b5', '36b6432f007becb748b1794964474b2b',
       '3003e64c29bf43be1259e3046f037265', '44dcc25d7363b7fb7bc510038364516f', '7164521f56f2c7e4327759e0d5f64e31',
       '794ba19314f13e231461b5adaaf90cbc', '8eb6cdab45ad0b9b726b03214c7be96c', 'bd942672ee501f53d62a1e38ded7e0d6',
       '98e575978eaff33435498682f223bf47', '6616f77dae19640d48f0216928c9172a', '4bfd035207da3bedb3f7f8607bb72230',
       '485ded12dab2c74a61770e92cff2e8dd', '501b8e996fc1f9c331db177bcb54daee', '413fae2919c3cd9f980c81558cc9b8e3',
       '05093376b450aec169c975e02b23ba76', '9cb8332799925287c5a1d2b5b698a5fe', 'a3f41c2ff3803cd88aba18f921b10735',
       '0955d7d4db88de580174739d3da59313', '6b6f67043f020c82e43fafd015064df9', '80ebbbd94e401fe86339ce70c0f21c16',
       'd5ea86add30ee741c76ba14f09bee66c', 'bb12a857af628cd5849fde4dbd12d842', '30910668a3ab97ad97c423712c954bd3',
       '956381b6855746c9b127d6c4809023a2', '789fddcecb0c6674663c8b3ccf734dc2', '3374b0e256081aeab1bab0caa5c03844',
       'f5cffbcd9c60d3bfb18acec952336202', '78bec00e3de0e45bb3ab6715b548f00b', 'c096157eb70884ee12055d4ca52e6931',
       'b6150ada9cd54e6aafda3e88e8250ba5', 'b5e17d0e9909548cafd52f2bb4f0bbd8', '0f345c6c5958f37d16e99a6b615f6662',
       '3daac404699155bfe6f045bf40c677b7', '3b682bd27a611aa479bc26e7969a881d', '716b469ce0eb3f2ff5cf6a4737de141f',
       '55e57dfee9f3d4fb5ce47ff275d6995d', '79475bd722e3f6af176ce88e958d22c6', '6e283917c06d03069800f840fb325be6',
       'db9d17320f44d1d8d229523b851a68f0', '41748f938df609e286f182f09f725454', '13022bd26fd1f284492f13f9db76564f',
       'df93a0c5514e469691339269965fae81', '08515e6ae49e72a819d1cf6112547e13', '77c08a1da535d9a8bb8858d5d7ed5d3a',
       'b8ab50baa2ffbed64957fb03d5cfec26', 'b112d497e417676aec34250fd56dc864')

select extract(year from creation_time),
       json_extract_path_text(revision, 'ratingProductId', true) as rateRevision,
       count(distinct id)                                        as countThingy
from s3_operational.rating_svc_prod_calculations s3
where state = 'FL'
  and lob = 'GL'
  and creation_time >= '2021-01-01'
group by 1, 2
order by 1 asc

select distinct(qpm.business_id),
               json_extract_path_text(rr.revision, 'ratingProductId', true) as rateRevision,
               count(distinct id)                                           as countThingy
from s3_operational.rating_svc_prod_calculations rr
         left join dwh.quotes_policies_mlob
where state = 'FL'
  and lob = 'GL'
  and creation_time >= '2021-01-01'
group by 1, 2
order by 1 asc

select *
from s3_operational.rating_svc_prod_calculations
limit 10

select *
from dwh.quotes_policies_mlob
limit 10

--find specific business by business name
select p.business_id,
       json_extract_path_text(p.business_details, 'businessname')       as business_name,
       json_extract_path_text(p.business_details, 'emailaddress')       as business_email,
       json_extract_path_text(p.business_details, 'applicantfirstname') as firstname,
       json_extract_path_text(p.business_details, 'applicantlastname')  as lastname
from underwriting_svc_prod.prospects p
where business_name like '%Motive LLC%'
  and creation_time >= '2023-09-01'

--to get top declines by COB, channel and LOB
select cob,
       (CASE
            WHEN (affiliate_id = 'N/A' and agent_id = 'N/A') then 'direct'
            WHEN (affiliate_id <> 'N/A' and agent_id = 'N/A') then 'affiliate'
            else 'agent' end)        as channel,
       uw.lob,
       decline_reasons,
       count(distinct (business_id)) as biz_count
from dwh.underwriting_quotes_data uw
         join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
where execution_status = 'DECLINE'
  and decline_reasons not like '%","%'
  and offer_creation_time >= '2023-01-01'
--and offer_creation_time <= '2023-03-31'
group by 1, 2, 3, 4
order by biz_count desc

--construction package distribution by month
select extract(year from creation_time) || '-' ||
       right('00' + convert(varchar, extract(month from creation_time)), 2) as creation_year_month,
       --extract(month from policy_start_date) as creation_year_month,
       --cob_group,
       highest_status_package,
       count(highest_status_package)                                        as package_count
from dwh.quotes_policies_mlob
where highest_policy_status >= 4
  and
  --(cob in ('Restaurant', 'Caterers', 'Food Truck', 'Coffee Shop', 'Grocery Store') or cob_group = 'Retail') and
  --cob_group = 'Consulting' and
    cob_group not in ('Construction', 'Cleaning')
  and lob_policy = 'GL'
  and offer_flow_type in ('APPLICATION')
  and highest_status_package in ('basic', 'pro', 'proPlus')
  and distribution_channel = 'agents'
  and creation_time >= '2020-01-01'
group by 1, 2
order by creation_year_month asc

--active GC and handyperson count
select cob, count(distinct business_id) as active_count
from dwh.quotes_policies_mlob
where highest_policy_status = 4
  and cob_group in ('Construction')
group by 1
order by 2 desc

--custom query for daycare QTP and AOV
select case when highest_policy_status >= 4 then 1 else 0 end   purchase,
       case when creation_time > '2023-09-25' then 1 else 0 end post_launch,
       business_id,
       highest_policy_status,
       basic_yearly_premium,
       highest_yearly_premium,
       creation_time,
       case
           when agent_id <> 'N/A' then 'Agent'
           when affiliate_id <> 'N/A' then 'Partnership'
           else 'Direct' end                                    channel,
       cob,
       state
--datediff('day', '2023-07-01', '2023-09-25') days  --86 days
from dwh.quotes_policies_mlob
where lob_policy = 'GL'
  and creation_time >= '2023-07-01'
  --and creation_time >> '2023-09-25'
  and offer_flow_type = 'APPLICATION'
  and cob_group = 'Day Care'


--QTP and ASP for pre/post (pre post) post launch post-launch analysis (sub out classes and dates)
With base as (SELECT distinct qpm.creation_time::date                                           as quote_day,
                              qpm.business_id,
                              qpm.highest_policy_id,
                              qpm.highest_policy_status,
                              qpm.highest_yearly_premium,
                              CASE WHEN creation_time < '2023-09-26' THEN 'pre' ELSE 'post' END as pre_post,
                              case
                                  when qpm.agent_id <> 'N/A' then 'Agent'
                                  when qpm.affiliate_id <> 'N/A' then 'Partnership'
                                  else 'Direct' end                                                channel
              FROM dwh.quotes_policies_mlob qpm
              WHERE qpm.lob_policy IN ('GL')
                and qpm.creation_time >= '2023-07-01'
                and qpm.offer_flow_type = 'APPLICATION'
                and qpm.cob in ('Day Care'))
SELECT pre_post,
       --channel,
       average_purchased_premium,
       purchases / quotes::decimal(10, 2) as qtp
FROM (SELECT pre_post,
             --channel,
             AVG(CASE WHEN highest_policy_status >= 3 then highest_yearly_premium END) as average_purchased_premium,
             count(distinct business_id)                                               as quotes,
             SUM(CASE WHEN highest_policy_status >= 3 then 1 ELSE 0 END)               as purchases
      from base
      group by 1)


--CP deductibles selection by pre-post, channel, cob_group
with t1 as (select purchased_quote_job_id,
                   business_id,
                   highest_policy_id,
                   highest_status_name,
                   cob,
                   cob_group,
                   lob_policy,
                   policy_start_date,
                   policy_end_date,
                   offer_flow_type,
                   highest_status_package,
                   state,
                   case when creation_time < '2023-11-01' THEN 'pre' ELSE 'post' END as pre_post,
                   case
                       when agent_id <> 'N/A' then 'Agent'
                       when affiliate_id <> 'N/A' then 'Partnership'
                       else 'Direct' end                                                channel,
                   case
                       when cob in ('Personal Trainer', 'Fitness Instructor', 'Yoga Instructor', 'Crossfit Instructor',
                                    'Pilates Instructor', 'Indoor Cycling Instructor', 'Aerobics Instructor',
                                    'Zumba Instructor', 'Auto Parts Store', 'Craft Artists', 'Etchers and Engravers',
                                    'Set and Exhibit Designers', 'Community Gardens', 'Printing Services',
                                    'Audio and Video Equipment Technicians',
                                    'Camera and Photographic Equipment Repairers',
                                    'Photo Editing, Scanning and Restoration', 'Art Space Rentals', 'Locksmith',
                                    'Musical Instrument Services', 'AV Equipment Rental for Events',
                                    'Medical Supplies Store', 'Knife Sharpening', 'Scavenger Hunts',
                                    'Vending Machine Operator', 'Print Binding and Finishing Workers', 'Glass Blowing',
                                    'Wedding and Event Invitations', 'Screen Printing and T Shirt Printing',
                                    'Print Media', 'Retail Stores', 'Bike Shop', 'Bike Rentals', 'Bookstore',
                                    'Newspaper and Magazine Store', 'Clothing Store', 'Department Stores',
                                    'Discount Store', 'Electronics Store', 'Fabric Store', 'Furniture Store',
                                    'Baby Gear and Furniture Store', 'Hardware Store', 'Arts and Crafts Store',
                                    'Hobby Shop', 'Candle Store', 'Home and Garden Retailer', 'Lighting Store',
                                    'Jewelry Store', 'Packing Supplies Store', 'Flea Markets',
                                    'Nurseries and Gardening Shop', 'Eyewear and Optician Store', 'Paint Stores',
                                    'Pet Stores', 'Furniture Rental', 'Sporting Goods Retailer',
                                    'Fitness and Exercise Equipment Store', 'Horse Equipment Shop', 'Luggage Store',
                                    'Pawn Shop', 'Toy Store', 'Demonstrators and Product Promoters', 'Fitness Studio',
                                    'Fencing Instructor', 'Sports Coach',
                                    'Umpires, Referees, and Other Sports Officials', 'Martial Arts Instructor')
                           then 'cp_retail_store_cob'
                       else 'other_cob' end                                             cp_cob_group
            from dwh.quotes_policies_mlob
            where highest_policy_status >= 3
              and creation_time >= '2023-10-01'
              and offer_flow_type = 'APPLICATION'
              and
              --distribution_channel = 'agents' and
                lob_policy = 'CP'),
     t2 as (select job_id,
                   revision,
                   creation_time,
                   json_extract_path_text(data_points, 'packageData', 'coverages', 'BASE', 'limits', 'DEDUCTIBLE',
                                          true) as ded_amount
            from s3_operational.rating_svc_prod_calculations
            where creation_time >= '2023-10-01')

--select *
--from t1
--join t2 on t1.purchased_quote_job_id = t2.job_id

select pre_post,
       channel,
       cp_cob_group,
       ded_amount,
       count(distinct business_id)
from t1
         join t2 on t1.purchased_quote_job_id = t2.job_id
group by 1, 2, 3, 4
order by 1, 2, 3, 4

--QTP and ASP for pre/post CP deductibles
with base as (select distinct qpm.creation_time::date                                           as quote_day,
                              qpm.business_id,
                              qpm.highest_policy_id,
                              qpm.highest_policy_status,
                              qpm.highest_yearly_premium,
                              case when creation_time < '2023-11-01' then 'pre' else 'post' end as pre_post
              from dwh.quotes_policies_mlob qpm
              where qpm.lob_policy IN ('CP')
                and qpm.creation_time > '2023-10-01'
                and qpm.offer_flow_type = 'APPLICATION'
                and qpm.distribution_channel = 'direct'
                and qpm.cob in ('Personal Trainer', 'Fitness Instructor', 'Yoga Instructor', 'Crossfit Instructor',
                                'Pilates Instructor', 'Indoor Cycling Instructor', 'Aerobics Instructor',
                                'Zumba Instructor', 'Auto Parts Store', 'Craft Artists', 'Etchers and Engravers',
                                'Set and Exhibit Designers', 'Community Gardens', 'Printing Services',
                                'Audio and Video Equipment Technicians', 'Camera and Photographic Equipment Repairers',
                                'Photo Editing, Scanning and Restoration', 'Art Space Rentals', 'Locksmith',
                                'Musical Instrument Services', 'AV Equipment Rental for Events',
                                'Medical Supplies Store', 'Knife Sharpening', 'Scavenger Hunts',
                                'Vending Machine Operator', 'Print Binding and Finishing Workers', 'Glass Blowing',
                                'Wedding and Event Invitations', 'Screen Printing and T Shirt Printing', 'Print Media',
                                'Retail Stores', 'Bike Shop', 'Bike Rentals', 'Bookstore',
                                'Newspaper and Magazine Store', 'Clothing Store', 'Department Stores', 'Discount Store',
                                'Electronics Store', 'Fabric Store', 'Furniture Store', 'Baby Gear and Furniture Store',
                                'Hardware Store', 'Arts and Crafts Store', 'Hobby Shop', 'Candle Store',
                                'Home and Garden Retailer', 'Lighting Store', 'Jewelry Store', 'Packing Supplies Store',
                                'Flea Markets', 'Nurseries and Gardening Shop', 'Eyewear and Optician Store',
                                'Paint Stores', 'Pet Stores', 'Furniture Rental', 'Sporting Goods Retailer',
                                'Fitness and Exercise Equipment Store', 'Horse Equipment Shop', 'Luggage Store',
                                'Pawn Shop', 'Toy Store', 'Demonstrators and Product Promoters', 'Fitness Studio',
                                'Fencing Instructor', 'Sports Coach', 'Umpires, Referees, and Other Sports Officials',
                                'Martial Arts Instructor'))
select pre_post,
       average_purchased_premium,
       purchases / quotes::decimal(10, 2) as qtp
from (select pre_post,
             avg(case when highest_policy_status >= 3 then highest_yearly_premium end) as average_purchased_premium,
             count(distinct business_id)                                               as quotes,
             sum(case when highest_policy_status >= 3 then 1 else 0 end)               as purchases
      from base
      group by 1)


--prospect quoted not bound for pro services (de-duped; last seven days)
with query as (select p.business_id,
                      qpm.cob,
                      qpm.lob_policy,
                      json_extract_path_text(p.business_details, 'businessname')                       as business_name,
                      json_extract_path_text(p.business_details, 'applicantfirstname')                 as first_name,
                      json_extract_path_text(p.business_details, 'applicantlastname')                  as last_name,
                      json_extract_path_text(p.business_details, 'telephonenumber')                    as phone_number,
                      json_extract_path_text(p.business_details, 'emailaddress')                       as business_email,
                      row_number() OVER (PARTITION BY qpm.business_id ORDER BY qpm.creation_time DESC) AS rank,
                      qpm.distribution_channel,
                      qpm_max_date.max_creation_time
               from underwriting_svc_prod.prospects p
                        inner join (select business_id, MAX(creation_time) as max_creation_time
                                    from dwh.quotes_policies_mlob
                                    where creation_time >= (getdate() - 7)
                                    group by business_id) qpm_max_date on p.business_id = qpm_max_date.business_id
                        left join dwh.quotes_policies_mlob qpm on p.business_id = qpm.business_id and
                                                                  qpm.creation_time = qpm_max_date.max_creation_time
               where qpm.distribution_channel <> 'agents'
                 and qpm.distribution_channel not like '%ap-%'
                 and qpm.cob_group in ('Professional Services')
                 AND p.business_id in (select business_id
                                       from dwh.quotes_policies_mlob
                                       group by business_id
                                       having MAX(highest_policy_status) = 1))
select *
from query
where rank = 1

--active consulting policies
select --distribution_channel,
       cob,
       count(distinct business_id),
       sum(highest_yearly_premium)
from dwh.quotes_policies_mlob
where lob_policy = 'GL'
  and
  --highest_status_package not like '%basic%' and
    cob in ('Business Consulting', 'Other Consulting', 'IT Consulting or Programming', 'Marketing',
            'Fine Artists, Including Painters, Sculptors, and Illustrators',
            'Computer and Information Systems Managers', 'Computer Network Support Specialists', 'Education Consulting',
            'Employment Agencies', 'Advertising and Promotions Managers',
            'Educational, Guidance, School, and Vocational Counselors', 'Training and Development Specialists',
            'Computer Programmers', 'Craft Artists', 'Graphic Designers',
            'Agents and Business Managers of Artists, Performers, and Athletes', 'Set and Exhibit Designers', 'Writer',
            'Logisticians', 'Public Relations Specialists', 'Computer Network Architects',
            'Human Resources Specialists', 'Call Center Service', 'Product Designer', 'Etchers and Engravers',
            'Safety Consultant', 'Editorial Services', 'Art Consultants', 'Telemarketing and Telesales Services',
            'Speech Training', 'Historians', 'Wedding Officiant', 'Emergency Management Directors',
            'Administrative Services Managers', 'Food Safety Training', 'Reporters and Correspondents')
  and highest_policy_status in ('4', '7')
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
group by 1
order by 3 desc

select business_id, purchased_quote_job_id
from dwh.quotes_policies_mlob
where lob_policy = 'GL'
  and highest_status_package not like '%basic%'
  and cob in ('Business Consulting', 'Other Consulting', 'IT Consulting or Programming', 'Marketing',
              'Fine Artists, Including Painters, Sculptors, and Illustrators',
              'Computer and Information Systems Managers', 'Computer Network Support Specialists',
              'Education Consulting', 'Employment Agencies', 'Advertising and Promotions Managers',
              'Educational, Guidance, School, and Vocational Counselors', 'Training and Development Specialists',
              'Computer Programmers', 'Craft Artists', 'Graphic Designers',
              'Agents and Business Managers of Artists, Performers, and Athletes', 'Set and Exhibit Designers',
              'Writer', 'Logisticians', 'Public Relations Specialists', 'Computer Network Architects',
              'Human Resources Specialists', 'Call Center Service', 'Product Designer', 'Etchers and Engravers',
              'Safety Consultant', 'Editorial Services', 'Art Consultants', 'Telemarketing and Telesales Services',
              'Speech Training', 'Historians', 'Wedding Officiant', 'Emergency Management Directors',
              'Administrative Services Managers', 'Food Safety Training', 'Reporters and Correspondents')
  and highest_policy_status in ('4', '7')
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
limit 10

--L&H premium distribution
--NAPA L&H buckets -- 0-250K, 250K-500K, 500K-1M, >1M
select business_id, highest_yearly_premium, revenue_in_12_months
from dwh.quotes_policies_mlob
where lob_policy = 'PL'
  and highest_status_package not like '%basic%'
  and cob = 'Insurance Agent'
  and highest_policy_status in ('4', '7')
  and json_extract_path_text(json_args, 'lob_app_json', 'pl_aop_header_property_casualty_insurance', true) = 'No'
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')

--active GL policies with COB, revenue, premium
select business_id, cob, cob_group, revenue_in_12_months, highest_yearly_premium
from dwh.quotes_policies_mlob
where lob_policy = 'GL'
  and highest_policy_status in ('4', '7')
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and creation_time >= '2022-01-01'

--permitted view (**DEPRECATED ~june 2023** see below for new one to use)
select cob_id, cob_name, state_code, lob, permitted
from portfolio_svc_prod.permitted_cobs_states_lobs
         join portfolio_svc_prod.cobs using (cob_id)
where permitted = '1'
  and state_code = 'NY'
  and lob = 'GL'

--permitted view / live COBs
select distinct list.cob_name, list.cob_id, silver._type, silver.state, silver.lob, silver.channel
from silver_portfolio.permitted silver
         left join portfolio_svc_prod.cobs list
                   on list.cob_id = silver.cobid --and list.state_code = silver.state_code
where silver.flowtype = 'PURCHASE'
  and silver.lob = 'GL'
  and silver.state = 'NEW_YORK'
  and silver.channel = 'direct'
order by 1 asc

select *
from silver_portfolio.permitted silver
where silver.flowtype = 'PURCHASE'
  and silver.lob = 'GL'
  and silver.state = 'NEW_YORK'
  and silver.channel = 'direct'
  and silver.action = 'OPEN'
order by 1 asc

--fitness test proPlus forcing pre-post
with base as (select distinct qpm.creation_time::date                                           as quote_day,
                              qpm.business_id,
                              qpm.highest_policy_id,
                              qpm.highest_policy_status,
                              qpm.highest_yearly_premium,
                              case when creation_time < '2023-09-03' then 'pre' else 'post' end as pre_post
              from dwh.quotes_policies_mlob qpm
              where qpm.lob_policy IN ('GL')
                and qpm.creation_time > '2023-07-03'
                and qpm.creation_time <= '2023-11-03'
                and qpm.offer_flow_type = 'APPLICATION'
                and qpm.distribution_channel <> 'agents'
                and qpm.cob <> 'Fitness Studio'
                and qpm.cob in
                    ('Fitness Studio', 'Personal Trainer', 'Fitness Instructor', 'Yoga Instructor', 'Dance Instructor',
                     'Zumba Instructor', 'Pilates Instructor', 'Indoor Cycling Instructor', 'Crossfit Instructor',
                     'Aerobics Instructor', 'Health and Wellness Coaching'))
select pre_post,
       average_purchased_premium,
       purchases / quotes::decimal(10, 2) as qtp
from (select pre_post,
             avg(case when highest_policy_status >= 3 then highest_yearly_premium end) as average_purchased_premium,
             count(distinct business_id)                                               as quotes,
             sum(case when highest_policy_status >= 3 then 1 else 0 end)               as purchases
      from base
      group by 1)

--PIF for construction
select cob, count(distinct business_id)
from dwh.quotes_policies_mlob
where creation_time > '2022-01-01'
  and highest_policy_status = 4
  and
  --cob_group = 'Construction'
    cob <> 'Restaurant'
  and cob_group <> 'Retail'
  and lob_policy = 'GL'
  and distribution_channel = 'agents'
group by 1
order by 2 desc

--cp_retail_store COB groups
select distinct cob, cob_group
from dwh.quotes_policies_mlob
where cob in ('Personal Trainer', 'Fitness Instructor', 'Yoga Instructor', 'Crossfit Instructor', 'Pilates Instructor',
              'Indoor Cycling Instructor', 'Aerobics Instructor', 'Zumba Instructor', 'Auto Parts Store',
              'Craft Artists', 'Etchers and Engravers', 'Set and Exhibit Designers', 'Community Gardens',
              'Printing Services', 'Audio and Video Equipment Technicians',
              'Camera and Photographic Equipment Repairers', 'Photo Editing, Scanning and Restoration',
              'Art Space Rentals', 'Locksmith', 'Musical Instrument Services', 'AV Equipment Rental for Events',
              'Medical Supplies Store', 'Knife Sharpening', 'Scavenger Hunts', 'Vending Machine Operator',
              'Print Binding and Finishing Workers', 'Glass Blowing', 'Wedding and Event Invitations',
              'Screen Printing and T Shirt Printing', 'Print Media', 'Retail Stores', 'Bike Shop', 'Bike Rentals',
              'Bookstore', 'Newspaper and Magazine Store', 'Clothing Store', 'Department Stores', 'Discount Store',
              'Electronics Store', 'Fabric Store', 'Furniture Store', 'Baby Gear and Furniture Store', 'Hardware Store',
              'Arts and Crafts Store', 'Hobby Shop', 'Candle Store', 'Home and Garden Retailer', 'Lighting Store',
              'Jewelry Store', 'Packing Supplies Store', 'Flea Markets', 'Nurseries and Gardening Shop',
              'Eyewear and Optician Store', 'Paint Stores', 'Pet Stores', 'Furniture Rental', 'Sporting Goods Retailer',
              'Fitness and Exercise Equipment Store', 'Horse Equipment Shop', 'Luggage Store', 'Pawn Shop', 'Toy Store',
              'Demonstrators and Product Promoters', 'Fitness Studio', 'Fencing Instructor', 'Sports Coach',
              'Umpires, Referees, and Other Sports Officials', 'Martial Arts Instructor')
  and creation_time >= '2023-10-01'
  and lob_policy = 'CP'

--snow and ice removal premium distribution
select business_id, cob, lob_policy, highest_yearly_premium
from dwh.quotes_policies_mlob
where cob = 'Snow and Ice Removal'
  and highest_policy_status in ('4', '7')
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and creation_time >= '2022-06-01'

--pro services qtp and decline rates by lob cob
select lob,
       --cob,
       count(distinct case when policy_status >= 4 then business_id else null end)           purchase,
       count(distinct business_id)                                                           quotes,
       count(distinct case when execution_status = 'DECLINE' then business_id else null end) declines
from (select a.lob,
             cob,
             policy_id,
             policy_status,
             business_id,
             execution_status
      from dwh.underwriting_quotes_data a
               left join underwriting_svc_prod.lob_applications b
                         on a.lob_application_id = b.lob_application_id
               left join dwh.sources_test_cobs c
                         on a.cob = c.cob_name
      where a.offer_creation_time >= '2023-01-01'
        and a.offer_creation_time <= '2023-11-01'
        --and c.marketing_cob_group = 'Retail'
        and cob in
            ('Insurance Agent', 'Business Consulting', 'Photographer', 'IT Consulting or Programming', 'Accountant',
             'Other Consulting', 'Marketing', 'Property Manager', 'Real Estate Agent', 'Home Inspectors', 'Salesperson',
             'Engineer', 'Audio and Video Equipment Technicians', 'Real Estate Brokers', 'Videographers',
             'Legal Service', 'Travel Agency', 'Computer Programmers', 'Architect', 'Interior Designer',
             'Training and Development Specialists', 'Travel Guides', 'Graphic Designers', 'Claims Adjuster',
             'Computer and Information Systems Managers', 'Writer', 'Administrative Services Managers')
        --and a.lob in ('GL')
        and a.offer_flow_type in ('APPLICATION')) inner_query
group by 1
order by 4 desc


select eventtime,
       business_id,
       funnelphase,
       json_extract_path_text(aa.interaction_data, 'question_name')          as question_name,
       json_extract_path_text(aa.interaction_data, 'answer')                 as question_answer,
       json_extract_path_text(aa.interaction_data, 'question_source')        as question_source,
       json_extract_path_text(aa.interaction_data, 'question_default_value') as question_default_value
from dwh.all_activities_table aa
         join dwh.sources_attributed_table ss on aa.tracking_id = ss.tracking_id
where business_id = 'eccb5c88bd4c106e82d4c603cf607b9d'
  and aa.eventtime >= '2023-10-01'
  and funnelphase = 'Question Answered'

--UMB policy list (DELETE)
select distinct p.policy_id,
                p.business_id,
                p.policy_reference,
                p.carrier,
                p.policy_status,
                p.start_date,
                p.end_date,
                date_trunc('day', p.bind_date)                            bind_date,
                p.yearly_premium,
                pt.code                                                as lob,
                c.cob_name                                             as cob,
                c.marketing_cob_group                                     cob_group,
                cast(
                        nullif(
                                json_extract_path_text(
                                        regexp_substr(
                                                json_extract_path_text(financial_attribution, 'policyItems', true),
                                                '\\{.{5,20}UMBRELLA.{60,90}\\}\\}'), 'amounts', 'premium', true),
                                '') as numeric(30, 15))                as gl_umb_prem,
                cast(
                        nullif(
                                json_extract_path_text(
                                        regexp_substr(
                                                json_extract_path_text(financial_attribution, 'policyItems', true),
                                                '\\{.{5,20}EXCESS_LIABILITY.{60,90}\\}\\}'), 'amounts', 'premium',
                                        true), '') as numeric(30, 15)) as gl_excess_prem,
                CASE
                    WHEN clm.business_id is null then 'N/A'
                    WHEN clm.agent_id is not null then 'Agent'
                    WHEN clm.affiliate_id is not null then 'Partnership'
                    ELSE 'Direct' END                                  AS channel,
                clm.agency_type
from nimi_svc_prod.policies p
         JOIN nimi_svc_prod.policy_types pt on p.policy_type_id = pt.policy_type_id
         JOIN dwh.sources_test_cobs c on p.cob_id = c.cob_id
         LEFT JOIN dwh.company_level_metrics_ds clm on p.business_id = clm.business_id
where (financial_attribution ilike '%umbrella%'
    or financial_attribution ilike '%excess_liability%')
  and pt.code = 'GL'
  and p.start_date >= '2023-12-01'
  and p.creation_time <= '2023-11-29'
  and p.policy_status = 7

--top policies sold
select distinct business_id,
                policy_start_date,
                lob_policy,
                cob,
                distribution_channel,
                highest_yearly_premium,
                revenue_in_12_months--, highest_status_package
from dwh.quotes_policies_mlob
where highest_policy_status >= 4
  and
  --distribution_channel = 'agents' and
  --lob_policy = 'GL' and
    offer_flow_type in ('APPLICATION', 'RENEWAL')
  and creation_time >= '2023-12-01'
  and
  --cob_group = 'Food & beverage' and
    affiliate_id = '4700'
--revenue_in_12_months <> ''
order by highest_yearly_premium desc
limit 20

--prospect quoted not bound for retail
--despite my best efforts, there are still duplicates (when LOBs share same creation time), so manually removed duplicates :(
select p.business_id,
       qpm.cob,
       json_extract_path_text(p.business_details, 'businessname')       as business_name,
       json_extract_path_text(p.business_details, 'applicantfirstname') as first_name,
       json_extract_path_text(p.business_details, 'applicantlastname')  as last_name,
       json_extract_path_text(p.business_details, 'telephonenumber')    as phone_number,
       json_extract_path_text(p.business_details, 'emailaddress')       as business_email,
       qpm.distribution_channel,
       qpm_max_date.max_creation_time
from underwriting_svc_prod.prospects p
         inner join (select business_id, MAX(creation_time) as max_creation_time
                     from dwh.quotes_policies_mlob
                     where creation_time >= '2023-11-21'
                     group by business_id) qpm_max_date on p.business_id = qpm_max_date.business_id
         left join dwh.quotes_policies_mlob qpm
                   on p.business_id = qpm.business_id and qpm.creation_time = qpm_max_date.max_creation_time
where qpm.distribution_channel <> 'agents'
  and qpm.distribution_channel <> 'partnerships'
  and qpm.distribution_channel not like '%ap-%'
  and qpm.cob_group = 'Retail'
  and p.business_id in (select business_id
                        from dwh.quotes_policies_mlob
                        group by business_id
                        having MAX(highest_policy_status) = 1)

--DTRP selection by channel, cob_group
with t1 as (select purchased_quote_job_id,
                   business_id,
                   highest_policy_id,
                   highest_status_name,
                   cob,
                   cob_group,
                   lob_policy,
                   policy_start_date,
                   policy_end_date,
                   offer_flow_type,
                   highest_status_package,
                   state,
                   case
                       when agent_id <> 'N/A' then 'Agent'
                       when affiliate_id <> 'N/A' then 'Partnership'
                       else 'Direct' end channel
            from dwh.quotes_policies_mlob
            where highest_policy_status >= 3
              and creation_time >= '2023-12-06'
              and offer_flow_type = 'APPLICATION'
              and channel = 'Agent'
              and lob_policy = 'GL'),
     t2 as (select job_id,
                   revision,
                   creation_time,
                   json_extract_path_text(calculation, 'cobs', 'base_premium_addons', 'DTRP',
                                          'damage_to_rented_premises_limit', true) as dtrp_limit
            from s3_operational.rating_svc_prod_calculations
            where creation_time >= '2023-12-06')
select --cob_group,
       dtrp_limit,
       count(distinct business_id)
--business_id, state, cob, revision
from t1
         join t2 on t1.purchased_quote_job_id = t2.job_id
where state <> 'CA'
  and cob <> 'Restaurant'
  and dtrp_limit <> '' --dtrp limit blank for small number of agent honor quotes created prior to DTRP rating creation
group by 1
order by 2 desc


--consulting / consultant book by PL aggregate limit
with t1 as (select purchased_quote_job_id,
                   business_id,
                   cob,
                   highest_yearly_premium,
                   case
                       when agent_id <> 'N/A' then 'Agent'
                       when affiliate_id <> 'N/A' then 'Partnership'
                       else 'Direct' end channel
            from dwh.quotes_policies_mlob
            where highest_policy_status in ('4', '7')
              and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
              and lob_policy = 'GL'
              and cob in ('Business Consulting', 'Other Consulting', 'IT Consulting or Programming', 'Marketing',
                          'Fine Artists, Including Painters, Sculptors, and Illustrators',
                          'Computer and Information Systems Managers', 'Computer Network Support Specialists',
                          'Education Consulting', 'Employment Agencies', 'Advertising and Promotions Managers',
                          'Educational, Guidance, School, and Vocational Counselors',
                          'Training and Development Specialists', 'Computer Programmers', 'Craft Artists',
                          'Graphic Designers', 'Agents and Business Managers of Artists, Performers, and Athletes',
                          'Set and Exhibit Designers', 'Writer', 'Logisticians', 'Public Relations Specialists',
                          'Computer Network Architects', 'Human Resources Specialists', 'Call Center Service',
                          'Product Designer', 'Etchers and Engravers', 'Safety Consultant', 'Editorial Services',
                          'Art Consultants', 'Telemarketing and Telesales Services', 'Speech Training', 'Historians',
                          'Wedding Officiant', 'Emergency Management Directors', 'Administrative Services Managers',
                          'Food Safety Training', 'Reporters and Correspondents')
              and policy_start_date >= '2022-06-01'),
     t2 as (select job_id,
                   json_extract_path_text(data_points, 'packageData', 'coverages', 'PROFESSIONAL_LIABILITY', 'limits',
                                          'AGGREGATE', true) as pl_limit
            from s3_operational.rating_svc_prod_calculations
            where creation_time >= '2022-06-01')
select pl_limit,
       count(distinct business_id),
       sum(highest_yearly_premium)
--business_id, state, cob, revision
from t1
         join t2 on t1.purchased_quote_job_id = t2.job_id
group by 1
order by 2 desc


--list of all "other" activiites in PL app that led to sole-other decline
select qpm.cob,
       qdata.decline_reasons,
       json_extract_path_text(qpm.json_args, 'lob_app_json', 'pl_areas_of_practice_other', true) as pl_aop_other,
       count(distinct qdata.business_id)
from dwh.underwriting_quotes_data qdata
         left join dwh.quotes_policies_mlob qpm
                   on qpm.business_id = qdata.business_id
where qdata.start_date >= '2023-01-01'
  and qdata.lob = 'PL'
  and qpm.cob = 'Insurance Agent'
  and qdata.business_id <> ''
  and qdata.execution_status = 'DECLINE'
  and qdata.decline_reasons like '%This policy cannot cover only the activities you listed in the%'
group by 1, 2, 3
order by 4 desc

--list of all "other" activities in PL app for successfull quotes
select distinct business_id,
                cob,
                json_extract_path_text(json_args, 'lob_app_json', 'pl_areas_of_practice_other', true) as pl_aop_other
from dwh.quotes_policies_mlob
where creation_time >= '2023-01-01'
  and pl_aop_other <> ''
  and business_id <> ''

--json extract for A&E
select business_id, cob, json_args
from dwh.quotes_policies_mlob
where creation_time >= '2023-01-01'
  and cob in ('Architect', 'Engineer')
  and business_id <> ''
limit 1000

--premium by carrier / product / state
with sub as (Select json_extract_path_text(json_args, 'business_name', true)                         AS business_name,
                    qpm.highest_policy_reference,
                    qpm.COB,
                    qpm.offer_id,
                    qpm.new_reneweal,
                    qpm.insurance_product,
                    json_extract_path_text(current_amendment, 'version', true)                       AS version,
                    qpm.distribution_channel,

                    row_number() OVER (PARTITION BY qpm.business_id ORDER BY qpm.creation_time DESC) AS rank,
                    qpm.business_id,
                    lob_policy,
                    qpm.cob_group,
                    qpm.state,
                    qpm.highest_yearly_premium,
                    qpm.highest_policy_id,

                    qpm.highest_policy_status,
                    qpm.highest_status_name,/* p.end_date :: date,*/
                    qpm.policy_start_date:: date,
                    qpm.policy_end_date:: date --agents.agency_id,
--agents.agent_email_address, agents.agency_name, agents.territory_manager, agents.agency_aggregator_name, agents.current_agencytype

             from dwh.quotes_policies_mlob qpm
--left join dwh.v_agents agents on agents.agent_id=qpm.agent_id
             where highest_policy_status IN (4) --status = 'Active'
--and state in ('TN', 'AZ', 'MI')
--and cob in ('Day Care')--('Food Delivery')('Postal Service Mail Carriers') 'Day Care','Security Services')
               and lob_policy = 'GL'--, 'WC')
             --and insurance_product LIKE '%state_national'
-- and qpm.highest_yearly_premium >1

--and policy_start_date >= '09-25-2023'
-- and new_reneweal = 'new'
--and state = 'CA'
--and distribution_channel = 'agents'
             order by business_id, qpm.creation_time)
select state
     , insurance_product
     , version
     --, round(sum(highest_yearly_premium),00) AS premium, count(offer_id) as polcount
     , business_id
from sub
where rank = 1
  and state = 'MO'
  and version = '2.0'
--group by state, insurance_product, version
order by state, version
limit 20

--Andrew's BOP analysis bundle attach rate (GL CP bundling)
SELECT uqd.business_id,
       uqd.cob,
       uqd.agent_id,
       COUNT(DISTINCT CASE
                          WHEN uqd.lob = 'GL' AND uqd.execution_status = 'SUCCESS' and uqd.policy_status_name = 'Active'
                              THEN uqd.lob END)                                                       AS gl_purchase,
       COUNT(DISTINCT CASE
                          WHEN uqd.lob = 'CP' AND uqd.execution_status = 'SUCCESS' and uqd.policy_status_name = 'Active'
                              THEN uqd.lob END)                                                       AS cp_purchase,
       COUNT(DISTINCT CASE
                          WHEN uqd.lob = 'GL' AND uqd.execution_status = 'SUCCESS'
                              THEN uqd.lob END)                                                       AS gl_count_quote_success,
       COUNT(DISTINCT CASE
                          WHEN uqd.lob = 'CP' AND uqd.execution_status = 'SUCCESS'
                              THEN uqd.lob END)                                                       AS cp_count_quote_success,
       COUNT(DISTINCT CASE
                          WHEN uqd.lob = 'GL' AND uqd.execution_status = 'DECLINE'
                              THEN uqd.lob END)                                                       AS gl_count_decline,
       COUNT(DISTINCT CASE
                          WHEN uqd.lob = 'CP' AND uqd.execution_status = 'DECLINE'
                              THEN uqd.lob END)                                                       AS cp_count_decline,
       COUNT(DISTINCT CASE WHEN uqd.lob = 'GL' AND uqd.execution_status IS NOT NULL THEN uqd.lob END) AS gl_count_quote,
       COUNT(DISTINCT CASE WHEN uqd.lob = 'CP' AND uqd.execution_status IS NOT NULL THEN uqd.lob END) AS cp_count_quote
FROM dwh.underwriting_quotes_data uqd
WHERE uqd.lob IN ('GL', 'CP')
  AND uqd.execution_status is not null
  AND uqd.agent_id <> 'N/A'
  AND uqd.cob = 'Restaurant'
  AND uqd.offer_flow_type = 'APPLICATION'
  AND uqd.offer_creation_time >= '2023-09-01'
GROUP BY uqd.business_id, uqd.cob, uqd.agent_id

select extract(year from creation_time) as yearWritten, sum(highest_yearly_premium) as premiumWritten
from dwh.quotes_policies_mlob
where offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and highest_policy_status >= 3 --and cob_group not in ('Construction', 'Cleaning', 'Unsupported')
group by 1
order by 1 asc

SELECT extract(year from creation_time) as yearWritten,
       sum(highest_yearly_premium)      as premiumWritten,
       sum(case
               when cob_group not in ('Construction', 'Cleaning', 'Unsupported') then highest_yearly_premium
               else 0 end)              as premiumWrittenNonconstruction
FROM dwh.quotes_policies_mlob
WHERE offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  AND highest_policy_status >= 3
  AND creation_time >= '2020-01-01'
GROUP BY 1
ORDER BY 1 ASC

--2023 YTD writings by COB
select cob,
       count(distinct business_id),
       sum(highest_yearly_premium)
from dwh.quotes_policies_mlob
where highest_policy_status >= 3
  and offer_flow_type in ('APPLICATION', 'RENEWAL')
  and policy_start_date >= '2023-01-01'
group by 1
order by 3 desc

--currently active GL policies for GuyH
select lob_policy, count(distinct business_id) as active_policy_count
from dwh.quotes_policies_mlob
where highest_policy_status = 4
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
group by 1
order by 2 desc

--permitted query
--not 100% reliable. see https://next-insurance.slack.com/archives/D01HK6QHA1J/p1704419635906559.
WITH appetite_universe AS (
    -- Construct a universe of all possible COB/STATE/LOB permutations
    -- COB 1,248 ROWS
    -- STATE 51 ROWS
    -- LOB 5 ROWS
    -- COB X STATE = 63,648
    -- COB X STATE X LOB = 318,240

    SELECT cobs.cob_id,
           cobs."name"         cob_desc,
           upper(replace(s.name,
                         ' ',
                         '_')) state,
           s.code              state_code,
           lobs.code           lob,
           CASE
               WHEN EXISTS ( -- If it's present in the `permitted` table, it's still in use and therefore not deprecated
                   SELECT 'x'
                   FROM silver_portfolio.permitted p
                   WHERE cobs.cob_id = p.cobid) THEN
                   'ACTIVE'
               ELSE
                   'DEPRECATED'
               END             cob_deprec_status
    FROM nimi_svc_prod.cobs cobs
             CROSS JOIN nimi_svc_prod.states s
             CROSS JOIN nimi_svc_prod.policy_types lobs
    WHERE cobs."name" NOT LIKE '%deprecated%' -- extra cleanup
      AND lobs.code in ('CP',
        -- constraining to the current 5 as BOP handled as GL + CP distinctly
                        'GL',
                        'IM',
                        'PL',
                        'WC')),
     permitted AS (
         -- Create a distinct view of our current posture from portfolio at the same COB/STATE/LOB Granularity. Row Count = 125,024
         SELECT *
         FROM (SELECT cobid                                                                    AS cob_id,
                      lob,
                      state,
                      channel,
                      flowtype,
                      action,
                      row_number() OVER (PARTITION BY cobid,
                          state,
                          lob,
                          channel ORDER BY event_occurrence_time_pst_timestamp_formatted DESC) AS ranking
               FROM silver_portfolio.permitted p
               WHERE 1 = 1
                 AND flowtype = 'PURCHASE' -- OWL policies are presumed new
                 AND channel = 'direct' -- OWL using direct flow
              ) t
         WHERE ranking = 1)
-- Left join appetite_universe to portfolio. Expected row_count = 318,240
SELECT appetite_universe.cob_id,
       appetite_universe.cob_desc,
       appetite_universe.lob,
       appetite_universe.state_code,
       --appetite_universe.cob_deprec_status,
       --permitted.channel,
       --permitted.action,
       --permitted.flowtype,
       CASE
           WHEN appetite_universe.cob_deprec_status = 'DEPRECATED' THEN
               'CLOSED'
           WHEN permitted.action = 'OPEN' THEN
               'OPEN'
           ELSE
               'CLOSED' END current_status
FROM appetite_universe
         LEFT JOIN permitted ON permitted.cob_id = appetite_universe.cob_id
    AND permitted.state = appetite_universe.state
    AND permitted.lob = appetite_universe.lob
ORDER BY appetite_universe.cob_desc,
         appetite_universe.lob,
         appetite_universe.state_code;

select cob_group, distribution_channel, sum(highest_yearly_premium)
from dwh.quotes_policies_mlob
where highest_policy_status >= 3
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and policy_start_date >= '2023-01-01'
group by 1, 2

--pro services premium in force by cob
select cob, count(distinct (business_id))
from dwh.quotes_policies_mlob
where highest_policy_status = 4
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and policy_start_date >= '2023-01-01'
  and
  --cob in ('Architect', 'Engineer', 'Interior Designer', 'Building Inspector', 'Land Surveyor', 'Administrative Services Managers', 'Telemarketing and Telesales Services', 'Administrative Support', 'Urban and Regional Planners', 'Business Consulting', 'Other Consulting', 'IT Consulting or Programming', 'Safety Consultant', 'Education Consulting', 'Training and Development Specialists', 'Computer and Information Systems Managers', 'Public Relations Specialists', 'Product Designer', 'Computer Network Architects', 'Human Resources Specialists', 'Environmental Scientists and Specialists, Including Health', 'Environmental Science and Protection Technicians, Including Health', 'Occupational Health and Safety Specialists', 'Marketing', 'Securities, Commodities, and Financial Services Sales Agents', 'Loan Officers', 'Business Financing', 'Title Loans', 'Credit Authorizers, Checkers, and Clerks', 'Actuarial Service', 'Check Cashing and Pay day Loans', 'Debt Relief Services', 'Financial Examiners', 'Accountant', 'Financial Adviser', 'Insurance Agent', 'Claims Adjuster', 'Insurance Inspector', 'Insurance Appraisers', 'Computer Repair', 'Computer Programmers', 'Computer Network Support Specialists', 'Phone or Tablet Repair', 'Video Transfer Services', 'Camera and Photographic Equipment Repairers', 'Legal Service', 'Notary', 'Property Manager', 'Home Inspectors', 'Real Estate Brokers', 'Real Estate Agent', 'Mortgage Broker', 'Real Estate Appraisal')
    cob_group = 'Professional Services'
group by 1

--PL amendment ID
select distinct business_id,
                policy_status_name,
                current_amendment,
                carrier_name,
                state_code,
                revenue_in_12_months,
                new_renewal,
                start_date,
                (select MIN(start_date)
                 from dwh.underwriting_quotes_data
                 where business_id = main.business_id
                   and lob = 'PL') as earliest_pl_policy_start_date
from dwh.underwriting_quotes_data main
where
  --current_amendment IS NULL and
    policy_status = 4
  and lob = 'PL'
  and earliest_pl_policy_start_date <= '2021-01-01'

select business_id, distribution_channel, agent_id
from dwh.quotes_policies_mlob
where business_id in ('4e8f6cdd61913648c0e0e71f44954428')
  and highest_policy_status >= 3
  and lob_policy = 'PL'

--CP decline rate
select lob, execution_status, count(distinct business_id)
from dwh.underwriting_quotes_data uw
where --cob in ('Bakery', 'Caterer','Coffee Shop','Food Truck','Grocery Store','Restaurant') and
    offer_creation_time >= '2023-01-01'
  and offer_creation_time < '2024-01-01'
  and uw.lob = 'CP'
  and execution_status in ('DECLINE', 'SUCCESS')
  and offer_flow_type in ('APPLICATION')
group by lob, execution_status
order by lob, execution_status

--all policies written by CorePro / dave dubowy's commercial agency
select agency_name,
       business_id,
       business_name,
       state_code,
       cob_name,
       start_date,
       policy_status_name
from db_data_science.v_all_agents_policies
where agency_name = 'TLC Property and Casualty Inc'
--and lob = 'GL'
--and policy_status_name = 'Active'
--and cob_segments = 'Other COB'

--claims details given a claim ID
SELECT t.*
FROM claims_svc_prod.claims_v2 t
where claim_id in ('3WnNNeYBWHJGXVQn', 'eE18avj8sV1XzSvF', 'bo81TFYA4qmiQfeT', 'l7BaUpf7Jyb1SgXH', 'TZ1ovpp01GBn5z8F',
                   'XdbY9lskyqnXxaq6', '26Q8EzI2YpormCap', 'iCqOLROK0nUvfFXO', 'PK2BlfYenM2gY5BY', 'KlRquaz9dymR1wmb',
                   'F3qHi9sq0dzKGKkx', 'NMWmepPtz2gEnFFi', 'LX3lnZLF9VQ67RV2', '1Si44P6oQ4pDZYfZ', '6NvhoTgS9H0Mdgfp',
                   'jjyBWpTtgMDlZExr', 'J4sWUuK5WYdsFZ6w', 'q2j2tolddsTmYpBA', 'pnkKOLQAyTtAqiXm', 'SlYLW41UH1Izl1VT',
                   'SlYLW41UH1Izl1VT', 'dNHRNnAZj9fcy0t0', 'GegZpxlSKAdLZ7bu', 'nfkeouuc7iVcg3T3', 'nfkeouuc7iVcg3T3',
                   '92gXQaQ63D0qy5KR', 'kc0GAS5f3HztmV6E', 'Gyg5b7DXceX5tcZb', 'E4JD15mO7kgshEAI', '2YZVAMqNL4GHXyEL',
                   '2YZVAMqNL4GHXyEL', 'NbNGfZR3uZ1qWjo5', 'RLsNgMEQmOHyf9MG', '5Z4kXM2TgHbi7qYm', 'oSV2T90TdJb7tRBO',
                   'oSV2T90TdJb7tRBO', '4LOXcyFnZVT96oIk', 'P9UMrd8V0vlfSkro', 'bLHoVSnnHcFfMMmQ', 'lmXILEzkRgBFAciT',
                   'imEUYurm8vn83r4N', 'lNLxQBDHg6mleYiW', 'Fdc8vUMjSMxnT7Ol', 'Ooe8mMCSyExnESny', '7O6yrBysWiWkDJ4G',
                   'sYoN8ZHtONOYrMh9', 'NGTt0Sqnh7QPeW6K', 'IklgSbfdRY8NX5wk', 'dKYyYSajxyzMP6ow', 'LSLrXukePkxTUNNm',
                   'DPuHZ8NzX5O4NS2m', '8IDPiQXy62XPOEtt', 'pMmZvYendnx8S5ha', 'RDcQisgMmy3s6XDk', 'LMkH4exB7Y1bPVQD',
                   'AXB1gSKAqPCpcUzJ', '9oiRy0fHgGbukCs3', 'BExpmKiH9pBfRqiU', 'KQHUCjiA9MVCLxG3', 'doT45cdq6Vw529Cb',
                   'AyE260JBsvO3VTSR', 'wTlJL4zEVVRyKqlS', 'kLmaP1TLx4SLwCXB', 'WLzmxWkOQScEV7wk', 'P8ho6EpWXkhCz15V',
                   'P8ho6EpWXkhCz15V', 'CglLYmvE3gqEjSzS', 'dAvOiP9DSgUQ5KOt', 'DOC0MSJyVhjh145K', 'P8ho6EpWXkhCz15V',
                   'cJxcOqBanPaJ34bF', 'cJxcOqBanPaJ34bF', 'ofQxpM8pTKFFBB9y', 'I0Zf7y1CzT1xgwJE', 'Twut9oZufGpdTPKZ',
                   'Twut9oZufGpdTPKZ', 'Twut9oZufGpdTPKZ', 'Twut9oZufGpdTPKZ', 'Twut9oZufGpdTPKZ', 'Twut9oZufGpdTPKZ',
                   'PMnTmllNNAT6pQqu', '9VCgnG28IxINFKDc', 'Ez4FPnxZetuXuZXp', 'PMnTmllNNAT6pQqu', 'WaMiG8stD6dBj0Se',
                   'WCR8vJCXHJncO0TM', 'bwPA7YXDxEXFNKgi', 'KdRXwl8QXS1tG1jU', 'WCR8vJCXHJncO0TM', 'WCR8vJCXHJncO0TM',
                   'SwgCVrAUQ9AZTqwi', 'GeAY49XBxZ6frg3O', 'Cv4KcQzgtkuVK2Yn', '2gmra9Ky8Mh7cngl', 'cow4dIczU927BV49',
                   'DSmuBgtwB9wJBtsG', 'C9ndRLzqVALduJvl', 'RY6Sx5RSg1wvhQwK', 'WFkti1HW44NN8FgD', 'JkKmeWKtcblEQIkY',
                   'w8s9hZnjU0mkse1f', '0GHk2JhDafkr4OpJ', 'ktCObVoPjFSnMwA2', 'KsnFGFWaHvpeLTaG', 'WXRRAnmsOMpSJvCZ',
                   'yxXiI3ATyen7isrT', 'rxcvRodffE9hjj2i', 'op8YAUKk0fKPEGSW', 'BAAqljEU6ZgjeEAx', 'WiIHeVSz65eB0zWD',
                   'Rxz6Qg1Hlk7R1Uoy', 'sRZNI3vU3el3fE7d', 'YOodL0kkvCMQchiX', 'LTjGKfENFmeUG6pJ', 'LTjGKfENFmeUG6pJ',
                   '8vOMpgpoVBPdgiSV', '8vOMpgpoVBPdgiSV', 'KA1IBi98BsDAqWwn', 'JIR467iQRVvD2KeD', '8vOMpgpoVBPdgiSV',
                   'WMPfr7L4aCXGYgZK', 'lhQ2xb0cBNzTO4wA', '7iytkq6iA3evMQAW', 'Kfcyjm22fC47v2K2', '8vOMpgpoVBPdgiSV',
                   '8vOMpgpoVBPdgiSV', 'JQZKBQipF2BOBHNP', 'a1tyjKGj5HIU6dUY', 'a1tyjKGj5HIU6dUY', 'CzheOfOendDlCnjB',
                   'mR4bVQKv2ogdkR60', 'OCYNOmzucIOfjTQT', 'OCYNOmzucIOfjTQT', '1NaKaLFQEqQtgTIQ', 'wmSneP7hPl1VtojW',
                   'atG8572sGQf71wTf', 'O8z9ft2PgyCPRF8k', 'FgKQL2yCsjROXfb4', 'hqymjh2jNI2VPIZA', 'LvJwTud5zZbraCKY',
                   'QEAZUqhZnrIPBm2F', 'k5rF36gGNvhEbPbo', 'rkExI1fk7EhFBHLm', 'TRpdpeuhyLrrsefu', 'ThtA1lDbvvZnoHl1',
                   '3k7469bCRfek8OOu', '3k7469bCRfek8OOu', 'cW3xQay5vb2hCbqA', 'OmdOjoVkchKYorWj', 'rkExI1fk7EhFBHLm',
                   'BwpNKz6yXLV50WRI', 'MDdeL1qpjj15vMc2', 'CkNyyAlLN8Z9UyQT', '1eK1tWAeTKakxJxs', 'ZbnLMN2XHiMOPs8B',
                   'GQEB37oRiOfkSvd0', 'YvCsVdkr9UrgqPi4', 'TXHjn8QUp96RHyJY', 'ZYmaMxbP6xWrihRM', 'hNX3fQd81vg3KRpf',
                   '6j2OXpDIEz668Gjt', 'ucgwbQJMDwobXLSh', 'KxxGNQx9hKLX8pib', 'M1qNE9kAXwyW8sFQ', 'yMKeB501SSVvdq7Q',
                   'Gr9inTlkkapK97wZ', 'P8ho6EpWXkhCz15V', 'P8ho6EpWXkhCz15V', 'P8ho6EpWXkhCz15V', 'KnnnpJszm7739lZt',
                   'XmaT0NgVmeQbueO8', '0vwB6j7qyLmCGr1k', '0vwB6j7qyLmCGr1k', 'yxXiI3ATyen7isrT', 'EgFcAnVuHYGClaUh',
                   'wi91l82oQExgu2L8', '5tFrpmMVTHD7XjNi', 'eX3ip9aMT8oop0md', 'VIgpTGLywtuRdd73', 'cf5RxuRml2sMGj4c',
                   'yQ8fAFTsHgblxZIJ', 'GJs6ZiJFsBfw86RZ', 'GJs6ZiJFsBfw86RZ', 'SudYdwgGF7v02mIx', 'RcD85UIGXqvGAZJv',
                   'RcD85UIGXqvGAZJv', '7L9gmUFX7LwMZP2R', 'uAiDfDACeMv0Jvba', 'lv6VmABbukkJSMxw', 'LrKEswxPe39GsyHX',
                   'RMLU2E8quqqdeeZf', 'FehesdwVWp1kc6BR', 'otATNtmRnFFp4pOG', 'fpe1NlnvqiBLMU3g', 'ZhJegzykuXGV65te',
                   'KVnnFJga61mhxYJQ', 'UcUtJLDONDsocDEr', 'UcUtJLDONDsocDEr', 'Dl748oFom6BFW0rV', 'Ma9sjiKzjSeAfmVj',
                   'udjedcB86acM995o', 'H4ZaiuzJ68NImROt')

--to get lob mix for pro services
select cob, lob_policy, count(distinct business_id) as new_businesses
--, avg(revenue_in_12_months) as avg_revenue, avg(num_of_employees) as avg_employees
from dwh.quotes_policies_mlob
where highest_policy_status >= 3
  and highest_yearly_premium <> ''
  and distribution_channel in ('sem')
  and offer_flow_type in ('APPLICATION')
  and creation_time >= '2023-01-01'
  and cob_group = 'Professional Services'
group by 1, 2
order by 1 asc

--amazon decline rate
select lob,
       --cob,
       count(distinct case when policy_status >= 4 then business_id else null end)           purchase,
       count(distinct business_id)                                                           quotes,
       count(distinct case when execution_status = 'DECLINE' then business_id else null end) declines
from (select a.lob,
             cob,
             policy_id,
             policy_status,
             business_id,
             execution_status
      from dwh.underwriting_quotes_data a
               left join underwriting_svc_prod.lob_applications b
                         on a.lob_application_id = b.lob_application_id
               left join dwh.sources_test_cobs c
                         on a.cob = c.cob_name
      where a.offer_creation_time >= '2023-01-01'
        and a.offer_creation_time <= '2023-12-31'
        and affiliate_id = 4700
        --and c.marketing_cob_group = 'Retail'
        --and cob in ('Insurance Agent','Business Consulting','Photographer','IT Consulting or Programming','Accountant','Other Consulting','Marketing','Property Manager','Real Estate Agent','Home Inspectors','Salesperson','Engineer','Audio and Video Equipment Technicians','Real Estate Brokers','Videographers','Legal Service','Travel Agency','Computer Programmers','Architect','Interior Designer','Training and Development Specialists','Travel Guides','Graphic Designers','Claims Adjuster','Computer and Information Systems Managers','Writer','Administrative Services Managers')
        --and a.lob in ('GL')
        and a.offer_flow_type in ('APPLICATION')) inner_query
group by 1
order by 4 desc

--amazon app
select count(distinct (business_id)) as total_businesses,
       sum(highest_yearly_premium)   as total_premium,
       avg(revenue_in_12_months)     as avg_revenue
from dwh.quotes_policies_mlob
where affiliate_id in ('4700')
  and highest_policy_status >= 3
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and creation_time >= '2023-01-01'
  and creation_time <= '2023-12-31'

--UMB eligible cross-sell
with t1 as (select purchased_quote_job_id,
                   business_id,
                   highest_policy_id,
                   highest_status_name,
                   cob,
                   lob_policy,
                   creation_time,
                   policy_start_date,
                   policy_end_date,
                   offer_flow_type,
                   highest_status_package,
                   state,
                   json_args
            from dwh.quotes_policies_mlob
            where highest_policy_status = 4
              and creation_time >= '2023-09-20'
              and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
              and lob_policy = 'GL'),
     t2 as (select job_id,
                   revision,
                   creation_time,
                   json_extract_path_text(json_extract_path_text(data_points, 'packageData', true), 'version',
                                          true) as packageDataVersion,
                   json_extract_path_text(json_extract_path_text(data_points, 'packageData', true), 'coverages',
                                          true) as coveragesJSON,
            from s3_operational.rating_svc_prod_calculations
            where creation_time >= '2023-09-20')

select *
from t1
         join t2 on t1.purchased_quote_job_id = t2.job_id

--PL qtp by month (consistent with 2-dim QTP)
with t1 as (select last_day(creation_time)             as month,
                   count(distinct related_business_id) as sold_policy_count
            from dwh.quotes_policies_mlob
            where highest_policy_status >= 3
              and lob_policy = 'PL'
              and offer_flow_type in ('APPLICATION')
              and creation_time >= '2019-01-01'
              and creation_time <= '2025-08-01'
            group by 1
            order by month asc),

     t2 as (select last_day(creation_time)             as month,
                   count(distinct related_business_id) as quote_count
            from dwh.quotes_policies_mlob
            where lob_policy = 'PL'
              and offer_flow_type in ('APPLICATION')
              and creation_time >= '2019-01-01'
              and creation_time <= '2025-08-01'
            group by 1
            order by month asc)

select *, cast(sold_policy_count * 1.0 / quote_count * 1.0 as decimal(10, 4)) as qtp
from t1
         join t2 on t1.month = t2.month
order by t1.month asc

--intuit legacy volume by month
select last_day(creation_time)     as month,
       affiliate_id,
       count(distinct business_id) as sold_policy_count,
       sum(highest_yearly_premium) as new_wp
from dwh.quotes_policies_mlob
where affiliate_id = 4120
  and offer_flow_type in ('APPLICATION')
  and creation_time >= '2020-01-01'
group by 1, 2
order by new_wp desc

--get list of 'tattoo' actives
select business_id,
       json_extract_path_text(json_args, 'business_name', true) as biz_name,
       lob_policy,
       cob,
       distribution_channel,
       highest_policy_status,
       policy_end_date
from dwh.quotes_policies_mlob
where biz_name LIKE '%attoo%'
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and highest_policy_status in ('4', '7')
  and creation_time >= '2022-01-01'

select json_args
from dwh.quotes_policies_mlob
where offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and highest_policy_status in ('4', '7')
  and creation_time >= '2022-01-01'
limit 1

select distinct business_id,
                prospect_id,
                json_extract_path_text(json_args, 'business_name', true) as biz_name,
                lob_policy,
                cob,
                distribution_channel,
                marketing_source,
                highest_policy_status,
                policy_end_date
from dwh.quotes_policies_mlob
where business_id in
      ('5e158f3e1d04dbfa63905ea8c125de7b', '02c8b78a89b5c486f4617124c0ba2df3', '423b0d5d1e87c43a2e530fcc01ac0320',
       '98a67b1a48ab230580ac290af7b5c610', '066fecf93263e42c8fa4e651cca5b60b', 'f28ca13ba58cc1311fc17577c6128676',
       '21af2bf2a05516dbc8ec68eef8e4e935', '7fef832cfc34c088bf3bcc9f060549f3')

-- pull package data from individual policy: nimi_svc_prod.package_data
-- pull change info from individual policy: reporting.vw_policy_change_params

select distinct highest_policy_status, highest_status_name
from dwh.quotes_policies_mlob
where creation_time >= '2024-01-01'

select (CASE
            WHEN (affiliate_id = 'N/A' and agent_id = 'N/A') then 'direct'
            WHEN (affiliate_id <> 'N/A' and agent_id = 'N/A') then 'affiliate'
            else 'agent' end)      as channel,
       highest_policy_aggregate_limit,
       count(distinct business_id) as policy_count
from dwh.quotes_policies_mlob
where lob_policy = 'GL'
  and creation_time >= '2023-01-01'
  and creation_time <= '2023-01-31'
  and highest_policy_status >= 4
group by 1, 2
order by 1

SELECT t.mapping_data, t.update_time
FROM portfolio_svc_prod.cobs_to_categories_marketing t
ORDER BY update_time DESC
LIMIT 1

SELECT t.cob_id, t.category_str_id
FROM portfolio_svc_prod.cob_to_insurance_categories t

select creation_time
from dwh.quotes_policies_mlob
where business_id = 'b1e64ca56a0e51de00bf29b63586ba63'
  and lob_policy = 'GL'

select case
           when state in
                ('AL', 'AZ', 'DC', 'DE', 'FL', 'GA', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'MA', 'MD', 'ME', 'MI',
                 'MN', 'MO', 'MS', 'MT', 'NC', 'OH', 'PA', 'SC', 'TN', 'TX', 'UT', 'VA', 'WA', 'WI', 'WV')
               then 'eligible'
           else 'ineligible' end   as state_eligibility,
       case
           when cob in
                ('Insurance Agent', 'Property Manager', 'Real Estate Brokers', 'Business Consulting', 'Salesperson',
                 'Other Consulting', 'Accountant', 'Engineer', 'Architect', 'Real Estate Agent',
                 'IT Consulting or Programming', 'Interior Designer', 'Marketing',
                 'Fine Artists, Including Painters, Sculptors, and Illustrators',
                 'Computer and Information Systems Managers', 'Computer Network Support Specialists', 'Legal Service',
                 'Education Consulting', 'Financial Adviser', 'Travel Agency', 'Employment Agencies', 'Claims Adjuster',
                 'Advertising and Promotions Managers', 'Educational, Guidance, School, and Vocational Counselors',
                 'Training and Development Specialists', 'Computer Programmers', 'Craft Artists', 'Mortgage Broker',
                 'Notary', 'Travel Guides', 'Graphic Designers',
                 'Securities, Commodities, and Financial Services Sales Agents', 'Recording and Rehearsal Studios',
                 'Food Banks', 'Agents and Business Managers of Artists, Performers, and Athletes', 'Loan Officers',
                 'Set and Exhibit Designers', 'Writer', 'Logisticians', 'Public Relations Specialists',
                 'Estate Liquidation', 'Computer Network Architects', 'Human Resources Specialists',
                 'Call Center Service', 'Business Financing', 'Product Designer', 'Etchers and Engravers',
                 'Insurance Appraisers', 'Real Estate Appraisal', 'Dietitians and Nutritionists', 'Safety Consultant',
                 'Editorial Services', 'Art Consultants', 'Telemarketing and Telesales Services',
                 'Credit Authorizers, Checkers, and Clerks', 'Title Loans', 'Speech Training',
                 'Urban and Regional Planners', 'Actuarial Service', 'Allergists', 'Alternative Healing',
                 'Anesthesiologists', 'Historians', 'Audiologists', 'Ayurveda', 'Biological Technicians',
                 'Biomedical Engineers', 'Cardiologists', 'Cardiovascular Technologists and Technicians', 'Colonics',
                 'Community Health Workers', 'Concierge Medicine', 'Dental and Orthodontic Services',
                 'Dental Assistants', 'Dental Hygienists', 'Dental Laboratory Technicians', 'Dentists',
                 'Dermatologists', 'Diagnostic Imaging', 'Diagnostic Medical Sonographers', 'Diagnostic Services',
                 'Doctors', 'Doulas', 'Ear Nose and Throat', 'Endocrinologists', 'Endodontists', 'Wedding Officiant',
                 'Environmental Science and Protection Technicians, Including Health',
                 'Family and General Practitioners', 'Family Practice', 'Fertility', 'Gastroenterologist',
                 'Gerontologists', 'Habilitative Services', 'Health and Wellness Coaching', 'Health Retreats',
                 'Hearing Aid Providers', 'Hearing Aid Specialists', 'Hepatologists', 'Home Health Aides',
                 'Home Health Care', 'Check Cashing and Pay day Loans', 'Internal Medicine', 'Internists, General',
                 'Laboratory Testing', 'Licensed Practical and Licensed Vocational Nurses',
                 'Magnetic Resonance Imaging Technologists', 'Marriage and Family Therapists',
                 'Medical and Clinical Laboratory Technicians', 'Medical and Health Services Managers',
                 'Medical Appliance Technicians', 'Medical Assistants',
                 'Medical Records and Health Information Technicians', 'Medical Scientists, Except Epidemiologists',
                 'Mental Health Counselors', 'Midwives', 'Neurologist', 'Debt Relief Services', 'Nurse Anesthetists',
                 'Nurse Midwives', 'Nurse Practitioners', 'Nursing Assistants', 'Obstetricians and Gynecologists',
                 'Occupational Health and Safety Specialists', 'Occupational Therapists', 'Oncologist',
                 'Ophthalmologists', 'Optometrists', 'Orderlies', 'Orthodontists', 'Orthopedists',
                 'Osteopathic Physicians', 'Otologists', 'Pediatric Dentists', 'Pediatricians', 'Periodontists',
                 'Personal Care Aides', 'Personal Care Services', 'Physician Assistants', 'Podiatrists',
                 'Prenatal/Perinatal Care', 'Preventive Medicine', 'Proctologists', 'Prosthodontists',
                 'Psychiatric Aides', 'Psychiatrists', 'Psychologists', 'Pulmonologist', 'Radiation Therapists',
                 'Radiologists', 'Emergency Management Directors', 'Registered Nurses', 'Reproductive Health Services',
                 'Respiratory Therapists', 'Financial Examiners', 'Retina Specialists', 'Rheumatologists',
                 'Skilled Nursing', 'Sleep Specialists', 'Social Services', 'Speech Language Pathologists',
                 'Sports Psychologists', 'Teeth Whitening', 'Toxicologists', 'Traditional Chinese Medicine',
                 'Ultrasound Imaging Centers', 'Urologists', 'Vascular Medicine', 'Veterinarians', 'Walk-in Clinics',
                 'Weight Loss Centers', 'Administrative Services Managers', 'Food Safety Training',
                 'Reporters and Correspondents', 'CPR and First Aid Training', 'Rehabilitation Counselors')
               then 'ineligible'
           else 'eligible' end     as cob_eligibility,
       count(distinct business_id) as num_customers
from dwh.quotes_policies_mlob
where lob_policy = 'GL'
  and creation_time >= '2022-10-01'
  and highest_policy_status = 4
group by 1, 2
order by 1, 2

select email, business_id, json_extract_path_text(p.business_details, 'businessname') as biz_name, creation_time
from underwriting_svc_prod.prospects p
where email = 'pamela@renuance.net'
order by creation_time asc

--get PL amendment / version by month
select --last_day(creation_time) as month,
       date(creation_time),
       current_amendment,
       count(distinct business_id)
from dwh.quotes_policies_mlob
where lob_policy = 'PL'
  and highest_policy_status in (4, 7)
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and creation_time >= '2024-01-01'
group by 1, 2
order by 1 asc

--get GL amendment / version by month
select last_day(creation_time) as month,
       --date(creation_time),
       current_amendment,
       count(distinct business_id)
from dwh.quotes_policies_mlob
where lob_policy = 'GL'
  and highest_policy_status in (4, 7)
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and creation_time >= '2022-01-01'
  and state in ('FL', 'TX', 'SC', 'NV', 'UT')
group by 1, 2
order by 1 asc

--cyber policies
select c.yearly_premium,
       eventtime,
       c.business_id,
       agency_type,
       agent_id,
       agency_aggregator,
       split_part(
               split_part(financial_attribution, '{"itemId":"CYBER","itemType":"endorsement","amounts":{"premium":', 2),
               ',"surcharge":0.00}}]}', 1),
       p.financial_attribution,
       c.*
from db_data_science.cyber_tmp t
         join dwh.company_level_metrics_ds c
              on t.business_id = c.business_id
         join nimi_svc_prod.policies p
              on p.policy_reference = c.policy_reference
where eventtime::date >= '2024-02-28'
  and lob = 'GL'
  and financial_attribution not like
      '%{"itemId":"CYBER","itemType":"endorsement","amounts":{"premium":0.00,"surcharge":0.00}}%'

--cyber quotes
SET json_serialization_enable TO false
select q.quote_id,
       q.business_id,
       s.id as standalone_premium_type,
       s.value premium_value,
       q.cyber_risk_tier,
       q.dateid
from external_dwh.gl_quotes q,
     q.standalone_endorsements s
where s.id = 'cyber_coverage'
  and q.dateid >= 20240228
-- where business_id = '98d44abf2ead7d892327441e400fe50c'
limit 10000;

--get GL amendment / version by month
select last_day(creation_time) as month,
       state,
       --date(creation_time),
       current_amendment,
       count(distinct business_id)
from dwh.quotes_policies_mlob
where lob_policy = 'GL'
  and highest_policy_status in (4, 7)
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and creation_time >= '2022-01-01'
  and state in ('FL', 'TX', 'SC', 'NV', 'UT')
group by 1, 2, 3
order by 1 asc

--get highest CP
select business_id, cob, highest_yearly_premium
from dwh.quotes_policies_mlob
where lob_policy = 'GL'
  and highest_policy_status in (4, 7)
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and creation_time >= '2024-01-01'
  and highest_status_package = 'proPlusTria'
limit 5

--GL active policy counts by COB group and indicator of whether or not they also have active WC cross-sell
WITH gl_active AS (SELECT business_id, cob_group
                   FROM dwh.quotes_policies_mlob
                   WHERE highest_policy_status = 4
                     AND offer_flow_type IN ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
                     AND creation_time >= '2023-01-01'
                     AND lob_policy = 'GL'
                   GROUP BY business_id, cob_group),
     cp_active AS (SELECT business_id, cob_group
                   FROM dwh.quotes_policies_mlob
                   WHERE highest_policy_status = 4
                     AND offer_flow_type IN ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
                     AND creation_time >= '2023-01-01'
                     AND lob_policy = 'CP'
                   GROUP BY business_id, cob_group),
     gl_active_no_cp AS (SELECT business_id, cob_group
                         FROM gl_active
                         WHERE business_id NOT IN (SELECT business_id FROM cp_active)),
     gl_active_with_cp AS (SELECT business_id, cob_group
                           FROM gl_active
                           WHERE business_id IN (SELECT business_id FROM cp_active))
SELECT gl_active_no_cp.cob_group,
       COUNT(DISTINCT gl_active_no_cp.business_id)                     AS gl_active_no_cp,
       (SELECT COUNT(DISTINCT business_id)
        FROM gl_active_with_cp
        WHERE gl_active_with_cp.cob_group = gl_active_no_cp.cob_group) AS gl_active_with_cp
FROM gl_active_no_cp
GROUP BY gl_active_no_cp.cob_group
order by gl_active_no_cp desc;

--by cob
WITH gl_active AS (SELECT business_id, cob
                   FROM dwh.quotes_policies_mlob
                   WHERE highest_policy_status = 4
                     AND offer_flow_type IN ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
                     AND creation_time >= '2022-10-01'
                     AND lob_policy = 'GL'
                   GROUP BY business_id, cob),
     wc_active AS (SELECT business_id, cob
                   FROM dwh.quotes_policies_mlob
                   WHERE highest_policy_status = 4
                     AND offer_flow_type IN ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
                     AND creation_time >= '2022-10-01'
                     AND lob_policy = 'WC'
                   GROUP BY business_id, cob),
     gl_active_no_wc AS (SELECT business_id, cob
                         FROM gl_active
                         WHERE business_id NOT IN (SELECT business_id FROM wc_active)),
     gl_active_with_wc AS (SELECT business_id, cob
                           FROM gl_active
                           WHERE business_id IN (SELECT business_id FROM wc_active))
SELECT gl_active_no_wc.cob,
       COUNT(DISTINCT gl_active_no_wc.business_id)         AS gl_active_no_wc,
       (SELECT COUNT(DISTINCT business_id)
        FROM gl_active_with_wc
        WHERE gl_active_with_wc.cob = gl_active_no_wc.cob) AS gl_active_with_wc
FROM gl_active_no_wc
GROUP BY gl_active_no_wc.cob
order by gl_active_no_wc desc;

--get GL average costs by cob group
select lob_policy, median(highest_yearly_premium)
from dwh.quotes_policies_mlob
where --lob_policy = 'GL' and
    highest_policy_status in (4)
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and creation_time >= '2024-01-01'
group by 1
order by 2 desc

--lawyers active businesses
select count(distinct (business_id))
from dwh.quotes_policies_mlob
where --lob_policy = 'GL' and
    highest_policy_status in (4)
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and creation_time >= '2022-01-01'
  and cob = 'Legal Service'

--restaurant YIB over time
select last_day(creation_time)                                                                  as month,
       cast(json_extract_path_text(json_args, 'years_in_business_num', true) as decimal(10, 0)) as yib,
       count(distinct (business_id))                                                            as biz_count
from dwh.quotes_policies_mlob
where highest_policy_status >= 3
  and lob_policy = 'GL'
  and cob = 'Restaurant'
  and offer_flow_type in ('APPLICATION')
  and creation_time >= '2022-01-01'
group by 1, 2
order by 1 asc, 2 asc

--cp declines
/*
* Updated by: Tong Zhang
* Updated at: 1/16/2024
* Updated on: add GL/CP
* Subject: Decline Dashboard - all declines for NEW WC/gl/cp policies
*/

with AgentQuotes as (select distinct json_extract_path_text(interaction_data, 'offer_id', true) as offer_id,
                                     aaa.agent_id,
                                     a.current_agencytype
                     from dwh.all_activities_agents aaa
                              left join db_data_science.v_agents a on aaa.agent_id = a.agent_id
                     where funnelphase = 'Underwriting Processed'
                       and lob = 'WC'),

     PayG as (select distinct flow, offer_id
              from (select distinct 'BOOKROLL' as Flow, I.offer_id /*Bookrolling quotes*/
                    from s3_operational.mysql_underwriting_interface_prod_bi_underwriting_quotes_data I
                             join ap_intego_svc_prod.apintego_bookrolling_quote_data_responses BR
                                  on I.request_id = BR.uw_request_identifier
                    UNION
                    select distinct EQ.flow_source_type as flow, I.offer_id /*manual + api*/
                    from s3_operational.mysql_underwriting_interface_prod_bi_underwriting_quotes_data I
                             join ap_intego_svc_prod.apintego_early_quote_data_responses FQR
                                  on I.ap_intego_early_quote_application_id = FQR.uw_request_identifier
                             join ap_intego_svc_prod.apintego_early_quote_data EQ
                                  on EQ.request_unique_identifier = FQR.early_quote_request_id
                    UNION
                    select distinct EQ.flow_source_type as flow, I.offer_id /*api*/
                    from s3_operational.mysql_underwriting_interface_prod_bi_underwriting_quotes_data I
                             join ap_intego_svc_prod.apintego_final_quote_data_responses FQR
                                  on I.ap_intego_early_quote_application_id = FQR.uw_request_identifier
                             join ap_intego_svc_prod.apintego_early_quote_data EQ
                                  on EQ.request_unique_identifier = FQR.early_quote_request_id
                    UNION
                    select distinct 'Next Connect' as flow, f.offer_id /*next connect*/
                    from dwh.all_activities_table aa
                             join db_data_science.funnel_be_base f on aa.tracking_id = f.tracking_id
                    where aa.lob = 'WC'
                      and aa.funnelphase = 'Underwriting Processed'
                      and aa.payg_ind ilike '%payg%'
                      and f.affiliate_id = '7100') p)

select date_trunc('day', o.creation_time)::date                                                                       as create_Date,
       o.lob,
       o.lob_application_type                                                                                         as application_type,
       json_extract_path_text(o.metadata, 'flowStatus')                                                               as flowStatus,
       interface.state_code                                                                                           as state,
       interface.cob,
       stc.marketing_cob_group                                                                                        as cob_group,
       --qpmd.affiliate_id,
       case when AQ.offer_id is null then 'NonAgent' else 'Agent' end                                                 as Channel,
       f.channel                                                                                                      as Funnel_Channel,
       case
           when f.Channel = 'Agent' and AQ.current_agencytype is null then 'APIntego'
           else AQ.current_agencytype end                                                                             as AgencyType,
       PAYG.flow                                                                                                      as PAYG_FLOW,

       count(distinct interface.business_id)                                                                          as biz_ct,
       count(distinct o.offer_id)                                                                                     as offer_ct,
       count(distinct case
                          when o.execution_status = 'DECLINE' then o.offer_id
                          else null end)                                                                              as Decline_ct,
       -- Common reasons-----
       count(distinct case
                          when ec.decline_reasons ilike
                               '%We are unable to provide the right coverage and support for your business at this time. Your business is associated with services for which we cannot provide insurance%'
                              then o.offer_id
                          else null end)                                                                              as Keyword_Ct,
       count(distinct case
                          when ec.decline_reasons ilike
                               '%Sorry, we are unable to provide you a quote at this time as you previously were declined for%'
                              then o.offer_id
                          else null end)                                                                              as Persisted_Ct,
       count(distinct case
                          when ec.decline_reasons ilike
                               '%Unfortunately, we aren''t able to provide the appropriate coverage for all your business activities at this time%'
                              then o.offer_id
                          else null end)                                                                              as AlbusKW_ct,
       count(distinct case
                          when ec.decline_reasons like '%A previously held policy was declined or non renewed%'
                              then o.offer_id
                          else null end)                                                                              as PreviousNonRenewal_ct,
       count(distinct case
                          when ec.decline_reasons like
                               '%We are unable to provide this policy to you. This transaction is prohibited by the U.S. Department of Treasury’s Office of Foreign Assets Control. If you believe this is an error, please contact us at 1-855-222-5919%'
                              then o.offer_id
                          else null end)                                                                              as Foreign_Assets_ct,
       count(distinct case
                          when ec.decline_reasons like
                               '%You indicated that your commercial insurance coverage has been canceled, revoked, or non-renewed in the last 3 years (for reason other than cancellation for non-payment or non-renewal for discontinuation of program)%'
                              or ec.decline_reasons like '%A previously held policy was declined or non renewed%'
                              then o.offer_id
                          else null end)                                                                              as Insurance_History_ct,
       count(distinct case
                          when ec.decline_reasons like
                               '%You indicated that your business or its owners, officers, or partners: Has been convicted of a felony in the past 5 years%'
                              then o.offer_id
                          else null end)                                                                              as Criminal_History_ct,
       -- WC Decline reasons/msgs (possible mixed common ones here) -------------------------------------------------------
       count(distinct case
                          when ec.decline_reasons =
                               '["Your business'' risk profile does not fit our underwriting criteria for this insurance policy."]'
                              then o.offer_id
                          else null end)                                                                              as B505_lowscore_Ct,
       count(distinct case
                          when ec.decline_reasons ilike
                               '%Compensation coverage as your risk profile falls outside of our underwriting guidelines%'
                              then o.offer_id
                          else null end)                                                                              as Disparity_Ct,
       count(distinct case
                          when ec.decline_reasons ilike
                               '%support policies where the owner is a project manager who does not engage in construction work directly%'
                              then o.offer_id
                          else null end)                                                                              as Owner_PM_Ct,
       count(distinct case
                          when ec.decline_reasons ilike
                               '%Your payroll for administrative work is disproportionately high for this insurance policy%'
                              then o.offer_id
                          else null end)                                                                              as High_Admin_Payroll_Ct,
       count(distinct case
                          when ec.decline_reasons ilike
                               '%We are currently unable to provide workers compensation for a business of your premium size%'
                              then o.offer_id
                          else null end)                                                                              as Prem_Size_Ct,
       count(distinct case
                          when ec.decline_reasons ilike
                               '%We are unable to offer any coverage due to your policy history%'
                              then o.offer_id
                          else null end)                                                                              as Cancel_History_Ct,
       count(distinct case
                          when ec.decline_reasons ilike
                               '%We are not able to offer ghost policies where neither employees or owners are covered at this time%'
                              then o.offer_id
                          else null end)                                                                              as GhostPol_Ct,
       count(distinct case
                          when ec.decline_reasons ilike
                               '%Your business is associated with roofing activities for which we cannot provide workers compensation insurance%'
                              then o.offer_id
                          else null end)                                                                              as Assoc_Roofing_Ct,
       count(distinct case
                          when ec.decline_reasons ilike
                               '%Your annual payroll does not fit our criteria for this insurance policy%'
                              then o.offer_id
                          else null end)                                                                              as Ann_Payroll_Criteria_Ct,
       count(distinct case
                          when ec.decline_reasons ilike
                               '%You selected only Office or Sales as activities, which does not match with your chosen Class of Business%'
                              then o.offer_id
                          else null end)                                                                              as OfficeSales_Only_Ct,
       count(distinct case
                          when ec.decline_reasons ilike
                               '%We are unable to provide workers compensation to roofing contractors%'
                              then o.offer_id
                          else null end)                                                                              as Roofing_Ct,
       count(distinct case
                          when ec.decline_reasons ilike
                               '%We currently cannot support businesses with multiple office locations%' --WC
                              or ec.decline_reasons ilike
                                 '%We currently cannot support businesses with operations, buildings or employees in more than one location%' --WC
                              or ec.decline_reasons like
                                 '%This product is not currently available for businesses with more than one location%' --GL
                              or ec.decline_reasons like
                                 '%We currently do not offer property insurance for more than 1 location%' -- CP
                              then o.offer_id
                          else null end)                                                                              as MultiLoc_Ct,
       count(distinct case
                          when ec.decline_reasons ilike '%You perform mobile operations or services on customer%'
                              then o.offer_id
                          else null end)                                                                              as MobileOps_Servs_onCustPremises_Ct,
       count(distinct case
                          when ec.decline_reasons ilike '%NEXT cannot support you at this time%'
                              then o.offer_id
                          else null end)                                                                              as LicenseAcknowledgement_Ct,
       count(distinct case
                          when ec.decline_reasons like
                               '%We currently cannot support businesses with office locations in more than one state%' --WC
                              or ec.decline_reasons like
                                 '%We currently cannot support businesses with operations, buildings or employees in more than one state%' --WC
                              then o.offer_id
                          else null end)                                                                              as MultiStates_Ct,
       count(distinct case
                          when ec.decline_reasons like
                               '%This product is not suitable for businesses with employees in multiple states%' --WC
                              or ec.decline_reasons like
                                 '%This product is not suited for business that operate in multiple states%' -- GL
                              then o.offer_id
                          else null end)                                                                              as New_MultiStates_Ct,
       count(distinct case
                          when ec.decline_reasons ilike
                               '%We are unable to provide workers compensation for your type of business%'
                              or ec.decline_reasons ilike '%Compensation for your type of business%'
                              then o.offer_id
                          else null end)                                                                              as ModeledHighRisk_Ct,
       count(distinct case
                          when ec.decline_reasons ilike '%Your business does work we cannot cover%'
                              then o.offer_id
                          else null end)                                                                              as Prohibited_Activity_Ct,
       count(distinct case
                          when ec.decline_reasons ilike '%Work at heights higher than 15 feet%'
                              then o.offer_id
                          else null end)                                                                              as Higher_than_15ft_Ct,
       count(distinct case
                          when ec.decline_reasons ilike '%Work at heights higher than 30 feet%'
                              then o.offer_id
                          else null end)                                                                              as Higher_than_30ft_Ct,
       count(distinct case
                          when ec.decline_reasons ilike
                               '%You (or your employees / subcontractors) perform work that this policy does not cover: Siding, Framing, Exterior Painting, or Gutter Work%'
                              then o.offer_id
                          else null end)                                                                              as SidingFramingExtPaintGutter_ct,
       count(distinct case
                          when ec.decline_reasons ilike
                               '%Compensation coverage because your risk profile falls outside of our underwriting guidelines%'
                              then o.offer_id
                          else null end)                                                                              as F10SR_ct,
       count(distinct case
                          when ec.decline_reasons ilike '%We can’t support businesses with operations like yours%'
                              then o.offer_id
                          else null end)                                                                              as WorkatNight_ct,
       count(distinct case
                          when ec.decline_reasons like '%NEXT cannot offer coverage to businesses like yours%'
                              then o.offer_id
                          else null end)                                                                              as manpower_service_ct,
       count(distinct case
                          when ec.decline_reasons like '%We cannot support businesses that do your kind of work%'
                              then o.offer_id
                          else null end)                                                                              as construction_project_type_ct,
       count(distinct case
                          when ec.decline_reasons like
                               '%Unfortunately, your business'' risk profile does not fit our underwriting criteria for this insurance policy%'
                              then o.offer_id
                          else null end)                                                                              as comm_credit_waterfall_ct,
       count(distinct case
                          when ec.decline_reasons like
                               '%Unfortunately, we are unable to offer coverage as your business is outside of our current underwriting appetite%'
                              then o.offer_id
                          else null end)                                                                              as construction_min_5k_ManualPrem_ct,
       count(distinct case
                          when ec.decline_reasons like
                               '%We cannot offer Workers’ Compensation coverage as your business activities do not fall within our underwriting guidelines%'
                              then o.offer_id
                          else null end)                                                                              as construction_uninsured_sub_ct,
       -- GL Specific ----------------------------------------------------------------
       count(distinct case
                          when ec.decline_reasons like
                               '%We are unable to provide the right coverage and support for your business at this time due to the nature of your operations or products%'
                              or ec.decline_reasons like
                                 '%We are unable to offer you coverage due to the nature of your business%'
                              then o.offer_id
                          else null end)                                                                              as Gl_Adverse_Risk_ct,
       count(distinct case
                          when ec.decline_reasons like '%You sell products that cannot be covered by this policy%'
                              or ec.decline_reasons like '%You import products in restricted product categories%'
                              or ec.decline_reasons like
                                 '%You manufacture or private label goods in restricted product categories in excess of 1,500 units per year%'
                              or ec.decline_reasons like
                                 '%We are unable to provide you general liability coverage as your risk profile falls outside of our underwriting guidelines. You act as a distributor, or manufacturer’s sales representative%'
                              or ec.decline_reasons like
                                 '%We are unable to provide you general liability coverage as your risk profile falls outside of our underwriting guidelines. You Operate a warehouse or provide any other fulfilment or distribution services for others (does not include self-fulfillment)%'
                              or ec.decline_reasons like
                                 '%We are unable to provide you general liability coverage as your risk profile falls outside of our underwriting guidelines. You act as a wholesaler%'
                              or ec.decline_reasons like '%You sell Baby Products%'
                              then o.offer_id
                          else null end)                                                                              as Gl_Retail_ct,
       count(distinct case
                          when ec.decline_reasons like
                               '%You do not have enough years of experience for this insurance policy to cover you%'
                              then o.offer_id
                          else null end)                                                                              as Gl_Construction_ct,
       count(distinct case
                          when ec.decline_reasons like '%We are unable to cover your subcontracting work%'
                              or ec.decline_reasons like
                                 '%Your operations include the following activity that this policy cannot cover%'
                              or ec.decline_reasons like
                                 '%Your subcontractors perform the following activity that this policy cannot cover%'
                              then o.offer_id
                          else null end)                                                                              as Gl_GeneralContractor_ct,
       count(distinct case
                          when ec.decline_reasons like
                               '%We are unable to provide General Liability coverage as your risk profile falls outside of our underwriting guidelines%'
                              then o.offer_id
                          else null end)                                                                              as Gl_High_Rating_Retail_Risk_ct,
       count(distinct case
                          when ec.decline_reasons like
                               '%You do not perform criminal background checks on your employees%'
                              or ec.decline_reasons like '%You do not have the legally required licenses for your work%'
                              or ec.decline_reasons like
                                 '%You indicated that your business or its owners, officers, or partners: Has declared bankruptcy in the past 3 years%'
                              or ec.decline_reasons like
                                 '%You provide child care for more children than this policy can cover%'
                              or ec.decline_reasons like
                                 '%You perform work in New York state, which is not covered by this insurance policy%'
                              or ec.decline_reasons like
                                 '%You perform the following activity that this policy cannot cover:%'
                              then o.offer_id
                          else null end)                                                                              as GL_Business_Characteristic_Activity_ct,
       count(distinct case
                          when ec.decline_reasons like
                               '%Your annual revenue is too large for this insurance policy to cover%'
                              then o.offer_id
                          else null end)                                                                              as GL_Exposure_Limits_ct,
       ---- CP Specific ---------------------------------------------------------------------------------------------------
       count(distinct case
                          when ec.decline_reasons like
                               '%We cannot currently offer property insurance for this building''s Public Protection Classification%'
                              then o.offer_id
                          else null end)                                                                              as CP_Limited_Public_Fire_Protection_ct,
       count(distinct case
                          when ec.decline_reasons like
                               '%Your building systems (plumbing, electrical, and heating) have not been updated%'
                              then o.offer_id
                          else null end)                                                                              as CP_Outdated_Buidling_Systems_ct,
       count(distinct case
                          when ec.decline_reasons like
                               '%We are unable to offer you coverage as your business falls outside of our risk guidelines%'
                              OR ec.decline_reasons like
                                 '%We can not accept your business per our retail underwriting guideline%'
                              OR ec.decline_reasons like
                                 '%You indicated this space is a rental property, hotel, or motel%'
                              then o.offer_id
                          else null end)                                                                              as CP_Ineligible_Class_of_Business_ct,
       count(distinct case
                          when ec.decline_reasons like
                               '%At this time we can only provide an insurance policy if you have purchased wind coverage from a state sponsored wind pool or underwriting association%'
                              OR ec.decline_reasons like
                                 '%We do not currently offer this coverage in your area due to hurricane risk%'
                              OR ec.decline_reasons like
                                 '%We do not currently offer this coverage in your area due to Hurricane risk%'
                              OR ec.decline_reasons like
                                 '%We do not currently offer this coverage in your area due to your proximity to the coast%'
                              OR ec.decline_reasons like
                                 '%We do not currently offer this coverage in your area due to severe convective storm risk%'
                              OR ec.decline_reasons like
                                 '%We do not currently offer this coverage in your area due to Severe Convective Storm risk%'
                              OR ec.decline_reasons like
                                 '%We do not currently offer this coverage in your area due to your proximity to a major body of water%'
                              then o.offer_id
                          else null end)                                                                              as CP_CAT_Risk_ct,
       count(distinct case
                          when ec.decline_reasons like
                               '%We do not currently offer coverage for Landlords or Lessors of property%'
                              then o.offer_id
                          else null end)                                                                              as CP_Lessors_risk_ct,
       count(distinct case
                          when ec.decline_reasons like '%Your square footage is too large for this insurance policy%'
                              OR ec.decline_reasons like '%The limits you require are too high per our risk guidelines%'
                              OR ec.decline_reasons like '%The limits you have selected are too high for this policy%'
                              OR ec.decline_reasons like
                                 '%Your total exposure (property value and revenue) is too high for this policy%'
                              then o.offer_id
                          else null end)                                                                              as CP_Exposure_Limit_Exceeded_ct,
       count(distinct case
                          when ec.decline_reasons like '%Your building has aluminum wiring%'
                              then o.offer_id
                          else null end)                                                                              as CP_Outdated_Electrical_Wiring_ct,
       count(distinct case
                          when ec.decline_reasons like '%Your business is not accepted due to high fire hazard%'
                              then o.offer_id
                          else null end)                                                                              as CP_High_Hazard_Fire_Risk_ct,
       count(distinct case
                          when ec.decline_reasons like
                               '%You indicated that your business operates between the hours of midnight and 5:00 AM%'
                              then o.offer_id
                          else null end)                                                                              as CP_Restricted_Operating_Hours_ct,
       count(distinct case
                          when ec.decline_reasons like '%Your roof has not been updated in the last 30 years%'
                              then o.offer_id
                          else null end)                                                                              as CP_Outdated_Roofing_System_ct,
       count(distinct case
                          when ec.decline_reasons like '%Your business performs welding or metal work on the premises%'
                              OR ec.decline_reasons like '%Your business conducts woodworking operation at the premise%'
                              OR ec.decline_reasons like '%You indicated that your business only operates seasonally%'
                              OR ec.decline_reasons like
                                 '%Unfortunately, we aren''t able to provide the appropriate coverage for all your business activities at this time%'
                              then o.offer_id
                          else null end)                                                                              as CP_High_Hazard_Operations_ct,
       count(distinct case
                          when ec.decline_reasons like
                               '%We do not currently offer this coverage in your area due to wildfire risk%'
                              OR ec.decline_reasons like
                                 '%We do not currently offer this coverage in your area due to wildfire risk%'
                              OR ec.decline_reasons like
                                 '%Your business has too high of wildfire risks for this insurance policy%'
                              OR ec.decline_reasons like
                                 '%We do not currently offer this coverage in your area due to Wildfire risk%'
                              then o.offer_id
                          else null end)                                                                              as CP_High_Hazard_Wildfire_Risk_ct,
       count(distinct case
                          when ec.decline_reasons like
                               '%Your business occupies too high a floor for this insurance policy%'
                              then o.offer_id
                          else null end)                                                                              as CP_High_Hazard_High_Floor_ct,
       count(distinct case
                          when ec.decline_reasons like '%Your business sells antique items.%'
                              then o.offer_id
                          else null end)                                                                              as CP_High_Hazard_Antique_ct,
       count(distinct case
                          when ec.decline_reasons like
                               '%You indicated that your business does not maintain a written service contract for the automatic extinguishing system and for cleaning the cooking hoods (including the exhaust duct systems)%'
                              then o.offer_id
                          else null end)                                                                              as CP_Service_Contract_Noncompliance_ct,
       count(distinct case
                          when ec.decline_reasons like
                               '%Your building is undergoing structural renovation, demolition and/or ground up construction%'
                              then o.offer_id
                          else null end)                                                                              as CP_Building_Under_Construction_ct,
       --count(distinct case when ec.decline_reasons like '%%'
       --    then o.offer_id else null end) as _ct,
       ------------------- Others -----------------------------------------------------
       count(distinct case
                          when
                              ec.decline_reasons not ilike '%compensation for your type of business%'
                                  and ec.decline_reasons not like
                                      '%We currently cannot support businesses with office locations in more than one state%' --multi-state WC
                                  and ec.decline_reasons not like
                                      '%We currently cannot support businesses with operations, buildings or employees in more than one state%' --multi-state WC
                                  and ec.decline_reasons not like
                                      '%This product is not suitable for businesses with employees in multiple states%' -- new multi-state WC
                                  and ec.decline_reasons not like
                                      '%This product is not suited for business that operate in multiple states%' -- multi-state GL
                                  and ec.decline_reasons not ilike
                                      '%Sorry, we are unable to provide you a quote at this time as you previously were declined for%'
                                  and ec.decline_reasons not ilike
                                      '%We are unable to provide the right coverage and support for your business at this time. Your business is associated with services for which we cannot provide insurance%'
                                  and
                              ec.decline_reasons not ilike '%You perform mobile operations or services on customer%'
                                  and ec.decline_reasons not ilike
                                      '%We currently cannot support businesses with multiple office locations%' --multi-loc WC
                                  and ec.decline_reasons not ilike
                                      '%We currently cannot support businesses with operations, buildings or employees in more than one location%' -- multi-loc WC
                                  and ec.decline_reasons not like
                                      '%This product is not currently available for businesses with more than one location%' --multi-loc GL
                                  and ec.decline_reasons not like
                                      '%We currently do not offer property insurance for more than 1 location%' -- multi-loc CP
                                  and ec.decline_reasons not ilike
                                      '%We are unable to provide workers compensation to roofing contractors%'
                                  and ec.decline_reasons not ilike
                                      '%You selected only Office or Sales as activities, which does not match with your chosen Class of Business%'
                                  and ec.decline_reasons not ilike
                                      '%Your annual payroll does not fit our criteria for this insurance policy%'
                                  and ec.decline_reasons not ilike
                                      '%Your business is associated with roofing activities for which we cannot provide workers compensation insurance%'
                                  and ec.decline_reasons not ilike
                                      '%We are not able to offer ghost policies where neither employees or owners are covered at this time%'
                                  and ec.decline_reasons not ilike
                                      '%We are unable to offer any coverage due to your policy history%'
                                  and ec.decline_reasons not ilike
                                      '%We are currently unable to provide workers compensation for a business of your premium size%'
                                  and ec.decline_reasons not ilike
                                      '%Your payroll for administrative work is disproportionately high for this insurance policy%'
                                  and ec.decline_reasons not ilike
                                      '%support policies where the owner is a project manager who does not engage in construction work directly%'
                                  and ec.decline_reasons not ilike
                                      '%Compensation coverage as your risk profile falls outside of our underwriting guidelines%'
                                  and ec.decline_reasons <<>>
                                      '["Your business'' risk profile does not fit our underwriting criteria for this insurance policy."]' --b505
                                  and ec.decline_reasons not ilike '%Your business does work we cannot cover%'
                                  and ec.decline_reasons not ilike '%Work at heights higher than 15 feet%'
                                  and ec.decline_reasons not ilike '%Work at heights higher than 30 feet%'
                                  and ec.decline_reasons not ilike
                                      '%You (or your employees / subcontractors) perform work that this policy does not cover: Siding, Framing, Exterior Painting, or Gutter Work%'
                                  and ec.decline_reasons not ilike
                                      '%Compensation coverage because your risk profile falls outside of our underwriting guidelines%'
                                  and ec.decline_reasons not ilike '%NEXT cannot support you at this time%'
                                  and
                              ec.decline_reasons not ilike '%We can’t support businesses with operations like yours%'
                                  and ec.decline_reasons not ilike
                                      '%Unfortunately, we aren''t able to provide the appropriate coverage for all your business activities at this time%'
                                  and ec.decline_reasons not like
                                      '%NEXT cannot offer coverage to businesses like yours%' -- manpower
                                  and ec.decline_reasons not like
                                      '%We cannot support businesses that do your kind of work%' -- construction project
                                  and ec.decline_reasons not like
                                      '%Unfortunately, your business'' risk profile does not fit our underwriting criteria for this insurance policy%' -- comm score
                                  and ec.decline_reasons not like
                                      '%Unfortunately, we are unable to offer coverage as your business is outside of our current underwriting appetite%'
                                  and ec.decline_reasons not like
                                      '%We cannot offer Workers’ Compensation coverage as your business activities do not fall within our underwriting guidelines%'
                                  and ec.decline_reasons not like
                                      '%A previously held policy was declined or non renewed%' --PreviousNonRenewal
                                  and ec.decline_reasons not like
                                      '%We are unable to provide this policy to you. This transaction is prohibited by the U.S. Department of Treasury’s Office of Foreign Assets Control. If you believe this is an error, please contact us at 1-855-222-5919%' --Foreign_Assets
                                  and ec.decline_reasons not like
                                      '%You indicated that your commercial insurance coverage has been canceled, revoked, or non-renewed in the last 3 years (for reason other than cancellation for non-payment or non-renewal for discontinuation of program)%'
                                  and ec.decline_reasons not like
                                      '%A previously held policy was declined or non renewed%' --Insurance_History
                                  and ec.decline_reasons not like
                                      '%You indicated that your business or its owners, officers, or partners: Has been convicted of a felony in the past 5 years%' --Criminal_History
                              ---- GL Specific -------
                                  and ec.decline_reasons not like
                                      '%We are unable to provide the right coverage and support for your business at this time due to the nature of your operations or products%' --Gl_Adverse_Risk
                                  and ec.decline_reasons not like
                                      '%We are unable to offer you coverage due to the nature of your business%' --Gl_Adverse_Risk
                                  and ec.decline_reasons not like
                                      '%You sell products that cannot be covered by this policy%' --Gl_Retail
                                  and ec.decline_reasons not like
                                      '%You import products in restricted product categories%' --Gl_Retail
                                  and ec.decline_reasons not like
                                      '%You manufacture or private label goods in restricted product categories in excess of 1,500 units per year%' --Gl_Retail
                                  and ec.decline_reasons not like
                                      '%We are unable to provide you general liability coverage as your risk profile falls outside of our underwriting guidelines. You act as a distributor, or manufacturer’s sales representative%' --Gl_Retail
                                  and ec.decline_reasons not like
                                      '%We are unable to provide you general liability coverage as your risk profile falls outside of our underwriting guidelines. You Operate a warehouse or provide any other fulfilment or distribution services for others (does not include self-fulfillment)%' --Gl_Retail
                                  and ec.decline_reasons not like
                                      '%We are unable to provide you general liability coverage as your risk profile falls outside of our underwriting guidelines. You act as a wholesaler%' --Gl_Retail
                                  and ec.decline_reasons not like '%You sell Baby Products%' --Gl_Retail
                                  and ec.decline_reasons not like
                                      '%You do not have enough years of experience for this insurance policy to cover you%' --Gl_Construction
                                  and ec.decline_reasons not like
                                      '%We are unable to cover your subcontracting work%' --Gl_GeneralContractor
                                  and ec.decline_reasons not like
                                      '%Your operations include the following activity that this policy cannot cover%' --Gl_GeneralContractor
                                  and ec.decline_reasons not like
                                      '%Your subcontractors perform the following activity that this policy cannot cover%' --Gl_GeneralContractor
                                  and ec.decline_reasons not like
                                      '%We are unable to provide General Liability coverage as your risk profile falls outside of our underwriting guidelines%' --Gl_High_Rating_Retail_Risk
                                  and ec.decline_reasons not like
                                      '%You do not perform criminal background checks on your employees%' --GL_Business_Characteristic_Activity
                                  and ec.decline_reasons not like
                                      '%You do not have the legally required licenses for your work%' --GL_Business_Characteristic_Activity
                                  and ec.decline_reasons not like
                                      '%You indicated that your business or its owners, officers, or partners: Has declared bankruptcy in the past 3 years%' --GL_Business_Characteristic_Activity
                                  and ec.decline_reasons not like
                                      '%You provide child care for more children than this policy can cover%' --GL_Business_Characteristic_Activity
                                  and ec.decline_reasons not like
                                      '%You perform work in New York state, which is not covered by this insurance policy%' --GL_Business_Characteristic_Activity
                                  and ec.decline_reasons not like
                                      '%You perform the following activity that this policy cannot cover:%' --GL_Business_Characteristic_Activity
                                  and ec.decline_reasons not like
                                      '%Your annual revenue is too large for this insurance policy to cover%' --GL_Exposure_Limits
                              ---- cp --------------------
                                  and ec.decline_reasons not like
                                      '%We cannot currently offer property insurance for this building''s Public Protection Classification%' --CP_Limited_Public_Fire_Protection
                                  and ec.decline_reasons not like
                                      '%Your building systems (plumbing, electrical, and heating) have not been updated%' --CP_Outdated_Buidling_Systems
                                  and ec.decline_reasons not like
                                      '%We are unable to offer you coverage as your business falls outside of our risk guidelines%'--CP_Ineligible_Class_of_Business
                                  and ec.decline_reasons not like
                                      '%We can not accept your business per our retail underwriting guideline%'--CP_Ineligible_Class_of_Business
                                  and ec.decline_reasons not like
                                      '%You indicated this space is a rental property, hotel, or motel%' --CP_Ineligible_Class_of_Business
                                  and ec.decline_reasons not like
                                      '%At this time we can only provide an insurance policy if you have purchased wind coverage from a state sponsored wind pool or underwriting association%'--CP_CAT_Risk
                                  and ec.decline_reasons not like
                                      '%We do not currently offer this coverage in your area due to hurricane risk%'--CP_CAT_Risk
                                  and ec.decline_reasons not like
                                      '%We do not currently offer this coverage in your area due to Hurricane risk%'--CP_CAT_Risk
                                  and ec.decline_reasons not like
                                      '%We do not currently offer this coverage in your area due to your proximity to the coast%'--CP_CAT_Risk
                                  and ec.decline_reasons not like
                                      '%We do not currently offer this coverage in your area due to severe convective storm risk%'--CP_CAT_Risk
                                  and ec.decline_reasons not like
                                      '%We do not currently offer this coverage in your area due to Severe Convective Storm risk%'--CP_CAT_Risk
                                  and ec.decline_reasons not like
                                      '%We do not currently offer this coverage in your area due to your proximity to a major body of water%' --CP_CAT_Risk
                                  and ec.decline_reasons not like
                                      '%We do not currently offer coverage for Landlords or Lessors of property%' --CP_Lessors_risk
                                  and
                              ec.decline_reasons not like '%Your square footage is too large for this insurance policy%'--CP_Exposure_Limit_Exceeded
                                  and ec.decline_reasons not like
                                      '%The limits you require are too high per our risk guidelines%'--CP_Exposure_Limit_Exceeded
                                  and
                              ec.decline_reasons not like '%The limits you have selected are too high for this policy%'--CP_Exposure_Limit_Exceeded
                                  and ec.decline_reasons not like
                                      '%Your total exposure (property value and revenue) is too high for this policy%' --CP_Exposure_Limit_Exceeded
                                  and ec.decline_reasons not like
                                      '%Your building has aluminum wiring%' --CP_Outdated_Electrical_Wiring
                                  and ec.decline_reasons not like
                                      '%Your business is not accepted due to high fire hazard%' --CP_High_Hazard_Fire_Risk
                                  and ec.decline_reasons not like
                                      '%You indicated that your business operates between the hours of midnight and 5:00 AM%' --CP_Restricted_Operating_Hours
                                  and ec.decline_reasons not like
                                      '%Your roof has not been updated in the last 30 years%' --CP_Outdated_Roofing_System
                                  and ec.decline_reasons not like
                                      '%Your business performs welding or metal work on the premises%'--CP_High_Hazard_Operations
                                  and ec.decline_reasons not like
                                      '%Your business conducts woodworking operation at the premise%'--CP_High_Hazard_Operations
                                  and ec.decline_reasons not like
                                      '%You indicated that your business only operates seasonally%' --CP_High_Hazard_Operations
                                  and ec.decline_reasons not like
                                      '%Unfortunately, we aren''t able to provide the appropriate coverage for all your business activities at this time%' --CP_High_Hazard_Operations
                                  and ec.decline_reasons not like
                                      '%We do not currently offer this coverage in your area due to wildfire risk%'--CP_High_Hazard_Wildfire_Risk
                                  and ec.decline_reasons not like
                                      '%We do not currently offer this coverage in your area due to wildfire risk%'--CP_High_Hazard_Wildfire_Risk
                                  and ec.decline_reasons not like
                                      '%Your business has too high of wildfire risks for this insurance policy%'--CP_High_Hazard_Wildfire_Risk
                                  and ec.decline_reasons not like
                                      '%We do not currently offer this coverage in your area due to Wildfire risk%' --CP_High_Hazard_Wildfire_Risk
                                  and ec.decline_reasons not like
                                      '%Your business occupies too high a floor for this insurance policy%' --CP_High_Hazard_High_Floor
                                  and ec.decline_reasons not like
                                      '%Your business sells antique items.%' --CP_High_Hazard_Antique
                                  and ec.decline_reasons not like
                                      '%You indicated that your business does not maintain a written service contract for the automatic extinguishing system and for cleaning the cooking hoods (including the exhaust duct systems)%' --CP_Service_Contract_Noncompliance
                                  and ec.decline_reasons not like
                                      '%Your building is undergoing structural renovation, demolition and/or ground up construction%' --CP_Building_Under_Construction
                                  and o.execution_status = 'DECLINE'
                              then o.offer_id
                          else null end)                                                                              as OtherDeclines_Ct,
       --count(distinct case when ec.decline_reasons ilike '%We are not able to offer ghost policies where neither employees or owners are covered at this time%'
       --        then pr.business_id else null end) as GhostPol_biz_Ct,
       --count(distinct case when ec.decline_reasons = '["We are not able to offer ghost policies where neither employees or owners are covered at this time.","NEXT can''t support your business''s risk profile."]'
       --       then pr.business_id else null end) as GhostPol_ony_biz_Ct,
       sum(case
               when p.policy_reference is not null and p.policy_status >>= 3 then 1
               else 0 end)                                                                                            as bound_ct
from underwriting_svc_prod.offers o
         join underwriting_svc_prod.eligibility_checks ec on o.offer_id = ec.offer_id
    -- join dwh.quotes_policies_mlob_dec qpmd on o.offer_id = qpmd.offer_id /* Remove excluded users*/
         left join (select distinct offer_id, cob, state_code, business_id
                    from dwh.underwriting_quotes_data
    -- where lob ilike '%WC%'
) interface on o.offer_id = interface.offer_id
         left join dwh.sources_test_cobs stc on interface.cob = stc.cob_name
         left join underwriting_svc_prod.lob_purchases lp on o.offer_id = lp.offer_id
         left join nimi_svc_prod.policies p on p.policy_id = lp.policy_id
    ---------------------- PAYGO AND Channel info ---------------------------------------
         left join AgentQuotes AQ on o.offer_id = AQ.offer_id
         left join PAYG on o.offer_id = PAYG.offer_id
         left join (select distinct offer_id, channel from db_data_science.funnel_be_base) f on f.offer_id = o.offer_id
---------------------- get business id ------------------------------------
--left join underwriting_svc_prod.lob_applications la on o.application_id = la.lob_application_id
--left join underwriting_svc_prod.prospects pr on la.prospect_id = pr.prospect_id
where o.lob in ('WC', 'GL', 'CP')
  and o.creation_time::date >>= date_add('month', -24, current_date)
  and o.lob_application_type = 'APPLICATION'
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
order by 1

--delete
select p.business_id,
       creation_time,
       json_extract_path_text(p.business_details, 'businessname')       as business_name,
       json_extract_path_text(p.business_details, 'emailaddress')       as business_email,
       json_extract_path_text(p.business_details, 'applicantfirstname') as firstname,
       json_extract_path_text(p.business_details, 'applicantlastname')  as lastname
from underwriting_svc_prod.prospects p
where firstname like '%John%'
  and lastname like '%Fox%'
order by 2 desc


--to get top declines by COB, channel and LOB
select (CASE
            WHEN (affiliate_id = 'N/A' and agent_id = 'N/A') then 'direct'
            WHEN (affiliate_id <> 'N/A' and agent_id = 'N/A') then 'affiliate'
            else 'agent' end)        as channel,
       uw.lob,
       decline_reasons,
       count(distinct (business_id)) as biz_count
from dwh.underwriting_quotes_data uw
         join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
where execution_status = 'DECLINE'
  and decline_reasons not like '%","%'
  and offer_creation_time >= '2023-01-01'
  and uw.lob in ('GL', 'CP')
  and uw.cob not in ('General Contractor', 'Carpentry', 'Handyperson')
  and channel = 'agent'
group by 1, 2, 3
order by biz_count desc

--prospect quoted not bound for retail
--despite my best efforts, there are still duplicates (when LOBs share same creation time), so manually removed duplicates :(
select p.business_id,
       qpm.cob,
       json_extract_path_text(p.business_details, 'businessname')       as business_name,
       json_extract_path_text(p.business_details, 'applicantfirstname') as first_name,
       json_extract_path_text(p.business_details, 'applicantlastname')  as last_name,
       json_extract_path_text(p.business_details, 'telephonenumber')    as phone_number,
       json_extract_path_text(p.business_details, 'emailaddress')       as business_email,
       qpm.distribution_channel,
       qpm_max_date.max_creation_time
from underwriting_svc_prod.prospects p
         inner join (select business_id, MAX(creation_time) as max_creation_time
                     from dwh.quotes_policies_mlob
                     where creation_time >= (getdate() - 7)
                     group by business_id) qpm_max_date on p.business_id = qpm_max_date.business_id
         left join dwh.quotes_policies_mlob qpm
                   on p.business_id = qpm.business_id and qpm.creation_time = qpm_max_date.max_creation_time
where qpm.distribution_channel <> 'agents'
  and qpm.distribution_channel not like '%ap-%'
  and qpm.cob_group in ('Retail')
  AND p.business_id in (select business_id
                        from dwh.quotes_policies_mlob
                        group by business_id
                        having MAX(highest_policy_status) = 1)


--to get specific decline reason by COB, channel and LOB
select cob,
       --(CASE WHEN (affiliate_id = 'N/A' and  agent_id = 'N/A') then 'direct'
       --    WHEN (affiliate_id <> 'N/A' and  agent_id = 'N/A') then 'affiliate'
       --    else 'agent' end) as channel,
       uw.lob,
       decline_reasons,
       count(distinct (business_id)) as biz_count
from dwh.underwriting_quotes_data uw
         join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
where execution_status = 'DECLINE'
  and decline_reasons like '%"Your business is not accepted due to high fire hazard"%'
  and offer_creation_time >= '2024-01-01'
  and offer_creation_time <= '2024-03-25'
group by 1, 2, 3--,4
order by biz_count desc

select *
from dwh.quotes_policies_mlob
where business_id = '55a3bed990e8a021fee80dea3b46be56'
  and creation_time >= '2024-03-01'
limit 10

--in force real estate policies
select cob, lob_policy, count(distinct business_id), sum(highest_yearly_premium)
from dwh.quotes_policies_mlob
where highest_policy_status = 4
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and policy_start_date >= '2022-10-01'
  and cob in ('Real Estate Agent', 'Real Estate Brokers')
--lob_policy = 'PL'
group by 1, 2

select cob, count(distinct (business_id))
from dwh.quotes_policies_mlob
where highest_policy_status = 4
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and lob_policy = 'GL'
group by 1
order by 2 desc

--allstate declines -- hurricane
--for Yaniv, re : Declines: Had some odd messages about declines due to hurricane exposure in the center of TX and in PA.  can i get more detials on this one? was it a bug? where are we with this now?
--mentioned in Eran's congrats email
select
--(CASE WHEN (affiliate_id = 'N/A' and  uw.agent_id = 'N/A') then 'direct'
--WHEN (affiliate_id <> 'N/A' and  uw.agent_id = 'N/A') then 'affiliate'
--else 'agent' end) as channel,
uw.agent_id,
uw.business_id,
uw.state_code,
uw.lob,
--aaa.agency_aggregator_name,
decline_reasons
--count(distinct(uw.business_id)) as biz_count
from dwh.underwriting_quotes_data uw
         join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
--join db_data_science.v_all_agents_activities aaa on uw.offer_id = aaa.offer_id
where execution_status = 'DECLINE'
--and decline_reasons not like '%","%'
  and decline_reasons like '%hurricane%'
  and offer_creation_time >= '2024-04-01'
--and uw.lob in ('CP')
--and uw.cob not in ('General Contractor', 'Carpentry', 'Handyperson')
--and channel = 'agent'
  and uw.agent_id in
      ('0aCcDAnMOF9Zuqiz', '0AMg3S3UdzpOJHVJ', '0AxzGdikmYOFIOZx', '0BEXwGpCjjRxa6vY', '0bHZqhLGu01fB95a',
       '0BnlYR9bQSQshapz', '0BNr1ZWoC694wLUL', '0boUx0WP0R4EFdIt', '0BPl2Ob5gGhalNUc', '0BQN34GbC0FnuGGT',
       '0ccDBQeb724bNrce', '0CEYCoG6gFmB24cC', '0CJyJQovau0QXTjZ', '0CnkJJZxG4Z61nLZ', '0CvUU2FGvwmaVxnv',
       '0d2Aj4HDdLg7pyuf', '0d7jlKQNi4ZowpMQ', '0d8eW6rA55fN4Urf', '0DbbickviGAwE3mN', '0dbBLllGEDYrR9V4',
       '0DbojnmHZvI9IdT7', '0dF15xwaJfWPue2t', '0DJM9fl32pURxZzX', '0DO63O8fzMV7g7ow', '0DvnwEw6388E8vys',
       '0e2KLUvp4VIkpxeK', '0e4rtEAibbqTjKOc', '0ePjadmNPC599J30', '0eR6ZMdv3IyrWABO', '0eZGpie1Qy00C7G6',
       '0F5Mwm4X4Iz6DdIZ', '0F6Gd4fJHYRA4nkX', '0fGWeJJLdIsLEmv5', '0fIKt8MeND8YNSMB', '0fJrD5XhgBExYTyC',
       '0FqgUVOrxOQcG9mn', '0FSiLonHGws9FYP7', '0fVPC7swKp26SzED', '0G3mV8HrJ1VpYA2w', '0g3PTjdAN7AQs0Wd',
       '0g8RNUa7t8MHdN58', '0GdzSpUmuBPB8xfC', '0GrmZAPumG6c1C5f', '0gwjggeQbBsFzXNe', '0gzbOsFTSTKzkhIE',
       '0Hlt2QYZXSvEKWsN', '0HZkMKMpgorSsimp', '0I0xSFSd6OppHCl7', '0ig7tZoGIBX1kTfd', '0ISv4P44RLEsERCU',
       '0Jevj8H0IpTdixua', '0JhlXMFwQW3IpnYv', '0Jjgjg6CYXyRogzo', '0jpVFjQSPlA5nXsD', '0jVySSd94R72Y5OU',
       '0jwwo2fDrwQEXRMo', '0kcFf2woB9sakE4E', '0KkcXXeEty8h2edn', '0kuUKjtXFclaCoXq', '0kVRIFEXhLe6ynzd',
       '0lCbf8PSsQnV1pyO', '0lHl9O2GvYEq1FZu', '0LLu18iRTWWFwjoA', '0ln7JNZa6xsNBU3j', '0lPCJ9mst8oV1JOo',
       '0lS7DvaG9G54Ugu9', '0LwB6SVWdlOcEbqZ', '0m0eoBAWrEY2dXCd', '0M3TWncvcNHJHRa6', '0maGUWKWuVMugWjp',
       '0MX7KtTWt7yjkEWK', '0n3HAWsNBs5X2Dfj', '0n9D3WQoDHzqhsjK', '0NAyq3yXFUdszvRZ', '0nDHsDMfpawpcPaz',
       '0NETFqNXZPtEPfh8', '0NfE237e09lwyUvV', '0ntv5Nv6qT8y6d0Y', '0NutTVjakWAfy1cE', '0nVeptOqwEKFXqfs',
       '0o4xJQPf14UvVD0d', '0o58Ed9I2UZyWgCy', '0OaogUjih56NMHHf', '0OGkTdVDcxtyt30n', '0oHNW2wNZvZfrVXn',
       '0OLD6Zgt12wPO07C', '0olwBv32Z8vQRQwf', '0ONO9UaGDs1st6dT', '0Oo8yNBrxrbcQZpe', '0ooMFVx9Tr4mROet',
       '0oRz3xoT38QMBCri', '0P9ePdcQTaaOTDJo', '0pcP86hQ86FoCQge', '0pctZRt1VqBtUdZg', '0pEiXHfJ2AVkKPfO',
       '0pgXjm529zVh44C0', '0plRQBSi6f87c8Lo', '0Pukiqxxojv4I0Pf', '0pVFvJbMGMrzxze8', '0Pz4zaatpgFdT6Rd',
       '0QEu3fxbW5cyOBrY', '0QuQrIsmm4bWcl56', '0qwVOkjzcZ5okM1Q', '0qXGrn0mtPO9ekCh', '0QxKwwDmibu1BYQg',
       '0QZeLvOMw6hLz4ry', '0r4WP4Y5o2zeVA28', '0R8weq3U8MkaefO9', '0rbYD01ZqZilt85U', '0rEfyWkKIyIeAnKk',
       '0RkJKgDReGGgHQqG', '0Rl2blV8eldZS5Uf', '0RqJQPzNDnwh86nU', '0RYqnBnOVC4CeV4e', '0s5bt69GBXzx7f7a',
       '0S5ZUgyZAUQCjhfu', '0SeDlsaWNJhz3aLn', '0sLm0fcBWbJup9Zl', '0sQYrK3h3eoRuQNP', '0SU4jruu5sGsTLFy',
       '0T13VXcjCFja2AoY', '0TADyaHTI8Abf2JP', '0tckqV6SG69MDJg4', '0tcy47REX1VvG9lu', '0TdINNviMQKtAzai',
       '0teSlaCZ62rXhtiF', '0TGvQN1DRBOearVP', '0tKHXz7i0et0Ij5N', '0tXHAx87HqpfazWx', '0uCYuhkq4ICFD2D9',
       '0UiRmwZuUSaRL7Up', '0UqWlKCd6ZxEoEvj', '0uZOtyA1NFwNgnsk', '0vcRZlUqBUVCRJw9', '0VQZqs3XcdX0wsGf',
       '0vzZl64jFlOsZkH2', '0W5d67DTspw0NSCa', '0W7roGLOGNlGyffH', '0wDZZI1e5Cfx4sPT', '0WFC4ZDGFMRYnLDe',
       '0WLSzLKEb1Ff3Ktr', '0wPXaOKtcKgrjj7o', '0WXAZOBRkV7sUNLp', '0XbBqP3himcEqqAe', '0xXyu01ldyWNHcxn',
       '0y1e8oAiOcXmO4yL', '0Y21wth7ddkjuIpO', '0yEiQaffggW8uovT', '0yxyTEOhsXO4fXvX', '0ZArpBKxwMFS7bvf',
       '0ZjOdf9r2570Lhac', '0ZJR3f9lBQjhuhr8', '0Zx4JUg7ZSnkhUeD', '0zzAJJCjkjJK1Z6F', '1AIKKLCCK52ytRrl',
       '1atuLFodotlSAYQG', '1AXbuu2Po7x64xp6', '1BQdJkBKnJVx7l09', '1bTPzqLUPaPCfNin', '1cd1B9fBM3igsiBo',
       '1cGXucXhaRj7kIPa', '1CJL6SwNWFxXr6pM', '1cJR1rrO8eILjAEt', '1cl3ouYCq8iBzaCA', '1cqnA8PELrvYFR7a',
       '1CUkVD3UmJ0UzI6h', '1DaBCdFiutHiU5KY', '1dp3siGaOnZnjmdU', '1DV7zM1qgSbLKjqM', '1E0B6YVTWPX96I4u',
       '1E3alnwHJ6NvN0lk', '1E8CwzbEQpqIoIoz', '1E9Pha8x1SwdsErq', '1eoadIIkxcYgXMoK', '1eoamExtVH05Eg7D',
       '1F8oDzYrZ92pHuCt', '1F18XNwGVxqMG3Eh', '1Fd3wHObX5nrLSZ8', '1FHwKMDyCepG0ZSK', '1fRE2lcYgocpY1Pv',
       '1FuRmieiysSEbvkl', '1fYjBd5bRgvyalyO', '1G0lIVIyVjqn9QaM', '1GN0eMuEnyrJ53y4', '1GoFZ47b1W5b4nUT',
       '1Gtfr8o5tIpFM9oi', '1h2A5GizlEmWHL9R', '1H8SrCyvQiVkXrgL', '1H39IatWm8571XIJ', '1HgcKkHouElSVj68',
       '1hHoSKgi7OG2dKr3', '1hiEwacrUlLkjyeB', '1HJLD61hgkHTiyaf', '1I23QUFpfTKgkANA', '1ICkTDFCkBE28VSW',
       '1iD0NoOBHGrN76zT', '1iEUvdYVOG3imnAL', '1IpIOClghO3pIUMc', '1iVY9bYe9mMjmTam', '1iwXaff0T8uzcM2a',
       '1IZZJlFoA76HVF0m', '1J2gDSkd6hieEKNI', '1JjcM0JlXPg71Ws8', '1KHPlLnJCNfpRV9B', '1KJoTWkqRLEr8MOF',
       '1KL9VSzxyHi7j5OJ', '1kO1SVkRRXqmvw7v', '1KUuftKVLycDSpCr', '1KVOZFVGw1JtY0OC', '1L4Val9pLss5XGuk',
       '1lajGrKpUoH4cFiJ', '1leX2RMyJ8tlvWBN', '1Lg59CEvh7kHBjKG', '1LjvOd59PEm9jTdE', '1lQscGs6BOmNcotf',
       '1LZQY5Q4U8ZbHyFZ', '1m1DGZ2qtntIFSgZ', '1M31jzTzSChFo6eP', '1MbDXOKdu7vk2Wyp', '1mVzIfISkZEGiGbj',
       '1NmqPNBlBuwf0lUc', '1nvNe8jTWaj4Fndq', '1nwtuthM0xaxQLB0', '1NZZ6EYCO15a17cl', '1OA7qgDxfFaPmIyO',
       '1OagEgXX3eNAAeqk', '1obEq02BGKq0dxGa', '1oeb4V0zDVQi6f8L', '1OlXIpC8CdfUp0Tk', '1otjF1x5BZcuc5B3',
       '1PNfE6j53vz3lzff', '1q46BtXqYjOl16NM', '1qHe3PpBnwe1JDNt', '1r8tGv6kfOF5FCTO', '1RVB1EowLYZdB48y',
       '1rZVWIDp3VPZbSup', '1slF6ddUtv44EcpA', '1sp2Y7pCEnaTlkCd', '1st4s9pirWyD3em3', '1TgLJ1dZMbQThXsT',
       '1Th26njBfgDBjF0j', '1tkkoBVLA9hkQCED', '1trXP79v8olIx3Hz', '1tZJm8cqwYm70QJM', '1U3yQyePaaWaa3HW',
       '1UCv1MYM1SsEIJsI', '1upS2xeFTikgRKSG', '1uUgKCxm0JpJWzb6', '1UVDXATPgtth573T', '1V1vAwdSDVBD5urw',
       '1vV6lZI9X3fdlCV7', '1vZSWx7SBZv2L3He', '1wUq0JwXhfgHxhOI', '1wY0mA0XeFzNBWQM', '1xPI4n84YbrM6kHq',
       '1XSW0dPf1FU0ynxz', '1xZisRv9VP0EoJuE', '1yBbAAXfYpUh6J8h', '1YodO9G4snHPlpUY', '1ZFBeWyacn8BRWVv',
       '1zGD32mHXYlju87x', '1Zw31DUNR4mjf746', '1Zwx1pZgkzQspfYB', '1ZXIh5Y5OpQ7Gr46', '2A9ShVu99tFsOJjq',
       '2AAgy9LjdRzJfmfN', '2aLte9edXK52j03z', '2AvU9bSYKhKSka87', '2BbCalSBlOM6WFUv', '2Bi1t0F6qbrP7JeS',
       '2bmHKzxmj9V0mdqr', '2Bpqd1QnLHKF2iwh', '2bPuRsnQyNx0YYID', '2bXkesMCRLWZLpav', '2CrViInN3BKdkfhs',
       '2D0BZeRWkKuL6m5W', '2d4el9yvZX3oRDZ7', '2DMZToylzD84Shs6', '2dNRI4rxi6mfj88B', '2DoIuGSHotkInU9z',
       '2dx4CTza0UeC4xVs', '2E8KQ495sx8qyqsZ', '2EUGW0ttwtnE1VHo', '2exlgTDEhmt7ofU9', '2eY5GO3PtUpzXnSx',
       '2faFLlvQOt625cpY', '2fPgyPf7sZh8KksE', '2fTBnWQfM6ynWw63', '2FwklMLsDCJEAVfy', '2fzt8qxMTD9wEu4p',
       '2fzvD1F54OEJhjtY', '2G049jo5S1kL7XBo', '2GBlgtU9Xrlh1Eg4', '2gnmYoETFsgEfLRS', '2Goo7Gbl5It97t3i',
       '2gOZvtWde94u5bYa', '2gp4AQ7ueX7zNVrg', '2gxHKTp68UHbWfUd', '2H2ht3GdrNlEVTkr', '2H57g2AKAyJV6Y3s',
       '2HbVAUTcisKDWJfu', '2hEYTAUEzY1YTzU3', '2HGNucsghZT9nchn', '2HJgePtLe6Dbg3tR', '2HnBqNFcUQqcmDZa',
       '2hq3rpLTCpz7zTWE', '2Ic0ML8sbkfkidrB', '2icfx9Q20EnJEi1Q', '2IfchwBNNAzIHEAE', '2IMMZiGgjwaiEhow',
       '2Iohq1oIUXMKodNH', '2iptt4eF1GQdYam3', '2JDkLFK5UgzwBhWK', '2jOXApaipsPc00jf', '2K4eS0fvvhRocPnd',
       '2KbUoYJoxfKsrQLu', '2kCJe1ccIgqhpK2x', '2KdoAzPKUIJMhF2c', '2KqRB5ndnnHA0iiB', '2KspNYj1aKCrn32y',
       '2KSYDl2ZKpB35XJy', '2kvUClbX8NYSw8Ku', '2l1llOKDYzKc1FwL', '2Lj3ySHqO24IAsvt', '2LOYj7fa9wcjKdV5',
       '2lS9EKjABSGTujqe', '2lXYtxvjx06Cxq2n', '2Lyo9YVEsvIxoJY9', '2MhCSrioTFiDhC2i', '2n6WGxarFozEYdH9',
       '2NBh3zAOGUFn0vzY', '2ndYtziGBsnQcqjq', '2nJ8jH0n5VYmIHCj', '2NngAttyFSPGWCjE', '2npkFIYoVo5ziy8b',
       '2nPt7mdU7K20mUie', '2nSAR6uAclqGLBUb', '2nWA98htY4aSyZc0', '2Nx1xG18qW5mogR8', '2oKIyGGwBKPFrRXQ',
       '2OrI3pZVL5uLV2Ii', '2OZ5MGZrUX71AtbU', '2p8YsWoB6tf9YQ3n', '2Peowqo1kitEZPZl', '2PIeTJWlYR2BnmbK',
       '2PwcM96xcTbAkpg0', '2q6d0p9L3kMAgJIt', '2q933hAFl07oSHVb', '2qlmx7eQSM6vBCEf', '2qnpVEjVOSxx9inH',
       '2qpa2Xdk4WbmRAjC', '2RMGhI5XR2VuZ5HG', '2S7GEr6gjlX5tRPe', '2S9ABQVICmqP8Xcs', '2SAmFqUScNbc9dyY',
       '2Sj01ZZO5kKlt9MU', '2sOlrszWQsFgbNXr', '2sOvL1fpHTwmQvOC', '2sTTJj0wQljfB5TL', '2SVVdrPF6JQukPNw',
       '2SyaiLpP7ReAiTfn', '2SzMxVgQUXhNkazV', '2t99wExkYzLwjrvH', '2tfHg1i3bh29NOWV', '2TFvd977VgDFNVMb',
       '2UMZmMB3tG97bWge', '2unII1RmEu6Tl2K5', '2UsvizYOO7jlORCE', '2uVjV9oLDuPWCxMi', '2V4c1rZzbVNIAPRW',
       '2vAIEsFpjoROEgsR', '2vcDiLSyVbKekqkP', '2vCSPdj4oO0vXjYa', '2VoiBFLHZfffGxUh', '2vtAElBCJnAz1plD',
       '2VXlfgyniYYRaaC6', '2W7IRCCRAoV6Q0GE', '2weQkJfM1W9xQWUO', '2WF9TB2jICrDErA0', '2WjbKZBkx2s8fpQP',
       '2WkEMLWDoBuWFsa1', '2wKWxk3grMjCdyHX', '2wLfa9Drf5Xb291I', '2wnQGXjZ8oVMpKpk', '2Wo9IO9Wd7Aui0VR',
       '2WOXhRAvvOP0NemH', '2WrDD7DO2fp7RZOJ', '2x0wrPatnv75Ho2p', '2x4fPjiVVOBDx07o', '2xdEqN04qV5XvFoj',
       '2xqblWbSGPw57y1s', '2ybet1sWmpCNzEtz', '2yKzxzGVTPpa5SC6', '2yPk5WmS9BeNXjs2', '2YqiBLoKYXThOrbw',
       '2yv2j6kvQdcYodO9', '2Z12nVsoRvhTWqvH', '2ZhHsW7wSGqb4S7j', '2zMiCKEpaAyVjJ4K', '2zRwgPwsnPxYQnwb',
       '3aekxNkrWn1MJWOR', '3AiIHktAl4HYwzAw', '3AjcNsUyEBjwlgJO', '3AsEt5WXKzrB2n3t', '3b1y3s2yBRveySIL',
       '3b7ATl7uDYTgb0gY', '3BDgt4Ntk2YNWpkr', '3bfkYqdrrjBjheHC', '3BLi5ZYQ8I1TZhmY', '3C4nNjHx0SwpoNCM',
       '3CaGXOk8IGf7qZe6', '3Ch16RvEjsNKj1J9', '3cTfPNhSaUpgFHnU', '3cYt1unXgsEkD2mQ', '3dhpigoLitG3TKuO',
       '3dkXkpFdd3KekFZ7', '3dR5Nuq3LQywaDXT', '3Ds9ohOHjTkVojXL', '3dStsiDxWLDX0rOd', '3e9oH0N3EQ2NYuwy',
       '3Ede3JfQFgvx9RfV', '3EJ05gVjE7LeUuFL', '3ejPxsA74cVTcekN', '3ElKZEt2HYL5wzPI', '3EzkvGmbDHtLznwN',
       '3Fi5zSdZlEZo3nfD', '3fSFJId77YKIjC9h', '3Gb9z90nkQBDyo7Z', '3gKx4EHoGAyZkepC', '3GpWccY564V6d81y',
       '3GVXz4dxB3XiqqHI', '3Hc5RriSihjefOoq', '3hE4UgjgZQtLNe7l', '3hkrNKLLLljtl9xj', '3HXLzbMzzRlNJnrn',
       '3ibIrFxVnfxC994F', '3IDAgw8AiANKESAL', '3igJX0fKATdVhEcV', '3ilYknrOj2iD6nxu', '3IRgIKPm8R0JVqyu',
       '3iU7SFt4fgDoi7D4', '3IUsh4Gv7GVdqnzj', '3J0TCKJCyJ7DMzAR', '3JgyoW9GbUppbQfU', '3jPslA1uNmxuDT3S',
       '3JrZjX5rV74zevWJ', '3K3Kved3TEgG0IS9', '3k6su4AgepHWoLFL', '3kiwGfuemor3O6MI', '3kODpE8BLPr4o4y8',
       '3kQYgK0Vxz72egdX', '3l8buUxxgETvToKE', '3lgMrnk59pqTXby5', '3LqkKIezX6Lr2tiz', '3lrWg4kvKuS3seZL',
       '3LUZi4PGt7UH7bKU', '3meTVQSHONY1kFHj', '3mkAJiPVbv9KCOuE', '3mkpe9WgWiJKdQjM', '3MPqDqM85KWjaLPw',
       '3MwM8XqbVzIJK1eb', '3NBoAXhPblBAkfvZ', '3nXmnxq9TJ2ExXRh', '3oagRhfl8ei3ItyD', '3Or6561WOiiY9ebt',
       '3ouekXVtjkJjzruY', '3PhiOLlARAtXPrF4', '3PL95XT8rl35KHGU', '3pOVYvWqnhAgGvEC', '3pXFKkhNhSONGgpj',
       '3Q7VXgsyfFhsjiHk', '3qeBjCjscqd8mhcs', '3Qko6H4qjISHARnP', '3r6lskuMJrDlxbjy', '3R9AmqMuoRnDcNAX',
       '3R63r3WI7GlV3sDy', '3rQ6ZJQqEg23xfzM', '3rtreaVSzs9S61UI', '3rYL6MqPTxTv9uFr', '3s7LVdVxw3qafX8e',
       '3SDfblQuQ7AWIC9r', '3SFhvwoL3lIqpoOq', '3sIyVbWFEtbVzMkc', '3SPP3TCHU9k7iyc2', '3SyXfT32Aycxe8pX',
       '3sZlzTanPnd1ULp2', '3tHq17yc3mcsblae', '3tsoiJCmq98mpdSP', '3TU2WmVHKRRNhMNY', '3u69z9h1M0NSv5mH',
       '3UAc1qx1rbszUTDS', '3uhQg42mPROGVQyP', '3uL9U27GVz7xNOZW', '3UYvOsxI8CMH97u8', '3V1NnyRIqXodlETM',
       '3vCuxtG0sMf8gZYt', '3vrdLtYgBKn3VJvu', '3VvI0CVqJbK6HHrt', '3VyTNVO3GOIapsHq', '3w82pQXZVGnvV2eT',
       '3wmlkzH3qxFUgExO', '3XiZwIqcAJy07EvS', '3Xl4hGnnCIVCXpXm', '3XmvIjsKLQGghKDg', '3xpe3C6ru0vuWeWC',
       '3XTKym5vRkyJ2Aen', '3XwaAcoFKRRJV1By', '3YAqB7FigA1JZn34', '3YNfTanyz8eoe98w', '3YRHTdhEaWUcwxuy',
       '3YSyXpGEWMfvojFu', '3Z7KRDV6dUJ7ze7w', '3Zd8tGqm3lYNwvhH', '3ZnE8VUXOQTxVqEw', '3zP0gFr8Yte4AUcp',
       '3ztFuu44uSimXJp2', '3ZVO2ui8o91ZSiYb', '3ZYpRANBTlO5lOV9', '4ACYJnKSg3Chr87K', '4ACYQ7n32J1B1FVW',
       '4AVSBe1c4M9tq1i3', '4b54SzJFqPcKE9BL', '4Bf1dcIhOjtEuhPi', '4BG2OdUivHh3JwKY', '4BgOCieFifvgMwX1',
       '4blnjVgauBumESFf', '4BqdPDjFGxoYavsP', '4bTLVl5G7xrXwSr2', '4cCqv7TwFwltaKv0', '4Cd5YTjkbV5WyzE1',
       '4CGVQxqMlU8U5qqQ', '4cnCeNJnHrxWF05Y', '4d0A0PJHQqgjUC9B', '4DAWC2sa0AAzxqn3', '4dfmzRix5kHSg5cq',
       '4DGNUnMKT9RMXRhN', '4dOdFJ511O8AGyuD', '4dtJsdz1nDRnAPXF', '4DZTdJtkz2uutMOB', '4e3pyDZ0P2HvUWIX',
       '4EAhGTpvgkU79P7E', '4eGgL0eS2dKWc6QA', '4EgVDn2ciZZgiEF7', '4eI2GcjaEryoUxcu', '4Ela6Cmw1IdiV0PN',
       '4ePBE7jOHzIqUqwF', '4F0Q5A5YsVLReKwz', '4f7IwxCPQIiWcQIO', '4f15VlOSsSYMB6Jg', '4FcldwNxX4Ymq1ff',
       '4fdWSySLCK7VrhR2', '4FIA6EHt8ZNHQQhK', '4fK13UySxAhwT00P', '4FOAzrsmhGedorlF', '4FSHElmJcswsQklX',
       '4G3HrfqFTSRGa9kq', '4gDyvQnM0H9QOJPj', '4geN1H3MG3201o8r', '4GLjoVFbs2cLTp6S', '4Gptgh6I0DwJH9B6',
       '4h0QE6d3w5P8K3Hu', '4hd3jZNraYPQCdI7', '4HfQgT9qYsQQOiTm', '4HOl70bkbDjq2Ypx', '4hTlTfrB8UvHFe0P',
       '4Hx7G1BtgciUXC5n', '4i6Edd0FVXBh8ZSz', '4I239sa5vbIrPkEd', '4ib20vzFajJq35UX', '4icDte3r5MZHkZWZ',
       '4IdXcrEZF1WCdaQB', '4IEOG7q3Y9WYzmOX', '4iKcbivi2ZamrjuU', '4iUJdgVmLmn3zFkm', '4iUrZHol2aCS2SXp',
       '4J05eLmQ5MQcBibh', '4jCzo0UxyyXrydrH', '4K2iUkm9E7EHzu2a', '4KdKqihr52lHVc6Q', '4kFgFygP1AsIYQw2',
       '4KmC4LAI1srMBxVu', '4KNety3T46AiMxav', '4L82CibIN1juEcZ7', '4LALJX2Lk69Xnfm2', '4lanSvhwXATXzbsb',
       '4LImRZpkJuYXRamk', '4LL8S06Xi37HweVr', '4Lq5wAUk3SDePSdW', '4M0vpQK53pHxrJxF', '4meZUcxqR5raiWMD',
       '4MlTdLB9vBwHbIcA', '4MMvorBrU7OvdYMh', '4n2ocZX11k92lpCd', '4N8dqPBtaCEduvAn', '4nuFlkekEkguHiTL',
       '4oAAGV8FGvToASsA', '4Od0PHsftcstV9Vl', '4oj21PspyQGM8OAB', '4oKPbiVHk9UEwDzi', '4oriTTxtDfK32tKp',
       '4ox0JPSfwxlIvKCS', '4p7amDTBeduNbGr9', '4p7y9Urb2eYkjrY8', '4PkF1zI8r50QEZmN', '4PZlzEUCWinpplR8',
       '4qg8vOHdZyIoyYd1', '4QI7wfkVY26urRRB', '4qqo8R9Xantu9Jds', '4r8eXmZUIZE4h6Cl', '4rD7dmm7I2RS54Lw',
       '4rDRUPHB17bbqaaW', '4ri66Kdcb5WleqTL', '4RYkK3llFix5vqDn', '4s4UZ6z4eVdrzD5k', '4sGNbUEqnSE30pGW',
       '4sHtrlK7280DUdPw', '4SmLhOmmsXWhD3qi', '4ssWkhPq3HwqdzRx', '4stYQW37aMEtiMep', '4sWXruICwvxnSGpD',
       '4SxIalNXdOHyMIfx', '4t4VqHECQTicSdCo', '4t527l4Rma7KD0Hy', '4tcN7s5314GETwYt', '4tgiMne0lNOOzDz2',
       '4TjtboUuiZxDxFfv', '4Tru6SNUUoHHULyo', '4tvAMpQZxvz7Dnto', '4TYneWK3zGvwcxb2', '4u7Vzshv7wHesYqG',
       '4U9uOOF62rdngbda', '4uAyU0nLu929zPuA', '4uJQkIojtwoQCPA6', '4UKrGZV99gQCl4Kc', '4uLarVmQawgORTZf',
       '4v5pae8CYfyZxFWz', '4V9ANaetdwkbXbm2', '4VGKpDKi8OZy5f5J', '4VimwRAci0wFzbPk', '4vkIfp4hPImCOLu6',
       '4VvuFfSONV1bMKsy', '4VwEXbtGycMqH62Q', '4WB9QAaBcD03hcK8', '4WfU0PBhcVMc9yYb', '4Wh8kRRknME3z85n',
       '4wnpfxK7w3jyblos', '4Wq5qDc8MXWbTyXk', '4wQiWj2sd5VI97as', '4WvEf8OB26C7bC1C', '4wzMUdbIzI02C05G',
       '4x7rYCTNNBjtKGNQ', '4xkaf9MyDd5fYXlb', '4XzYljEptDHD58qQ', '4yExMIuRfcxYMo82', '4yovgopnigfuD4G5',
       '4zAjKluP5gYQmpuf', '4zDS3A7r2rXpWNPJ', '4Zh6ZsGhtl38bwL9', '4ZksxmKccLouzLgd', '4zlnDWzgkRVwnkCS',
       '4ZU3TgEG2aT4PYER', '5a0jOFUIXmcITiJp', '5af7E4Pl02cW6BtG', '5aVteJaBLfxh77gb', '5B08sSWR0PNLU1ws',
       '5BdXhJ064uPXSlWK', '5BuLvHQhF8bgDQvg', '5CiV6yuC0UUPp32i', '5cvTkJNQkKPxguk2', '5DHm8YCA28fxdmhA',
       '5dl2n02I3Dhqej1b', '5DneiCkPDy0ROSBK', '5DOLuhJDWztcEUVe', '5dr2AE4SczPNCT13', '5drIgyI3UW6lhBAV',
       '5dUepRx5WWqHTi7Q', '5dVxPwrYVnHaJscy', '5E6hrvRFAzjd82S6', '5EQd7fqqKYREntPN', '5eTtSyFRxcYC0BxB',
       '5f98A5D6c0iEd7Es', '5F737v2UEqLu4rLV', '5FWgOvIApxaaf70P', '5GBYsTUn9Pf7Avy4', '5gcNURBjRHYaXpiX',
       '5H13AXWoHO9l9Rlj', '5han4an8wJuKb7Xk', '5hdluroihGQuGXdK', '5HImpjY30VylkV98', '5HKomAjMKV0qTHC4',
       '5hl39GuVtItHbxfI', '5HnTHIbW3K3g3nVr', '5hSfJjfJLtyoJPUe', '5hx3GA2UI5m0x5JS', '5HY3QqO8sgBW3uPq',
       '5i7hQVDEwPvMNDpD', '5I7VQ2XNEGxAZ7qe', '5i77e9tdFiGW9ryf', '5Io1LzpCcqD8C5FA', '5isgGNANFxOe0l7w',
       '5jFTFodt4klfHeUa', '5jjIequmLsZ9T7FS', '5jKMLOMccohzapoV', '5K1mGkNmXBIDeJ8o', '5K1wZRg9HTRS8HcD',
       '5kcA7EfdDvTjmvXz', '5KdWP3sTuF9s96SA', '5Kpelyk46VAlqXa8', '5KUo32HBq7PZ6XJq', '5kv3DaJTRLbVQuLX',
       '5L2JDB2y9ngWFhSk', '5LCQbzApkxDCQSk0', '5LTkQzR0N1zhCnbv', '5Lvw61CWP8asxVou', '5m2tlta6V0JI8kP8',
       '5mA0PlBymbL9YDRB', '5Mg4pTKbYTmre7Ls', '5MhJIUhd8RNeigAr', '5MIQzdFKynDF8rXJ', '5muqGRDutKGJ7wdm',
       '5mwjumVaxCIkAPRo', '5MwNVHOWF5ojKRvl', '5mx5qjaI16I52Cyq', '5n9DMz7tvyOuAKhn', '5nEKqbuXQ0SNwJ7o',
       '5NEMrNCy7RDhBQ17', '5NKxKR8p9wxEzUso', '5nlc5GTyMcMoC1nE', '5NMCxZBOT8COI242', '5oBeV0M6pY67LCNz',
       '5OEL7VQ2XstdOKKa', '5oGhFqGG2vpvXrxM', '5OUDo1ijV1CZnuiI', '5p4NYzzNlSIBxP3o', '5PeXrmJCbatdTYzr',
       '5pfKdeuUtcqrFSEO', '5PG8a12T6AeQdDbk', '5PGRcho1Rn10Ihlh', '5PI4ntf7bYH3XGcB', '5PiOKuxrwqoMpLxH',
       '5pkFIkYocBRI16YM', '5PKQWdLnTv3XPS8V', '5QBhqQLsGuFzo8y0', '5qdJqc5cAqYCbzZS', '5QMvgpeLJcloubHq',
       '5QrAOBJYRuQ4n56c', '5QRdXw4b463kbWyW', '5qxJ8oEKNffzxACi', '5RB5XVgeRiyMxqqH', '5ro62ZRIejiaO8Lv',
       '5RpwOW8LnxN5ObNs', '5rr7CgAxbBxlaR4f', '5S7UrPCA8MQw2LvQ', '5Sp0sMB6U6taFZVP', '5tbhsk0kwR03MqTr',
       '5tFdPMJYhY1RhLFr', '5TGJoTZsmglOHxpl', '5tvnlOlCe7vdtEIq', '5tWYfhV9vCCVCh7F', '5U1n65leZ67M0xok',
       '5U5wb7U8pELbkxdB', '5U8FhoE8MIyKDEcp', '5U9sLzo80n0CSrZm', '5urc3nHInWQ8BbfZ', '5V993sCLx6CQbRPI',
       '5VekpDrYlag2PlAu', '5vi8i3h9JyppYnn8', '5viemTGaC2KvImHM', '5volb6xOGREbNFGH', '5VpJiQRQ5xZCEz30',
       '5VrKn4OFuk9HUtWb', '5VSxuyXbfMNRbGPw', '5vzcdL7CVthFFuyj', '5wb2yZREbkYrxclq', '5wDcsUwX9D3kb49o',
       '5WpISSHuXUQN5Uuy', '5WpXUll2Dyq3yzAD', '5WXeEzltq71Pbfdn', '5XfKPz3O4GWYwBpI', '5XfPbTxyCyuMSmqI',
       '5XOgGTd7Cou2QWU8', '5yfU2T0M6ONXZne0', '5yXLuhDyAZE3dyuH', '5yxXDiCsiA273wfY', '5zj1uVAFE5XxlgpL',
       '5zU26NBbavKThoP5', '5zuimNLFqFYcAvGW', '5ZX6aw1BjwFSQcKc', '6A7WLyFPDP0m09qL', '6ar3wuCSjazwubON',
       '6AuoLTBpmitCIkPE', '6AZenA3wKjHmpHHf', '6bblrTKFlZcGsD0T', '6br1i5Zk5GULlhTg', '6Bu0dLvU0olhEiFt',
       '6bzDrd1y1AWiTiKD', '6C69xvEicTp5ovzV', '6CesNLB45kmLVeJ6', '6CkPT1KiTCXuHQCr', '6csfFjUYCqya0Gl1',
       '6cvhUFKr2EvuyB55', '6djFj7qHQ4CPgjOF', '6dK2zMv1Jd5lvdbI', '6DmSYbT5EAXaUt6C', '6dvikID2Z7cM7y2P',
       '6EbPEr39JZ7JwYkj', '6ec4wW8T5iY7UPoY', '6ECOAIajM9ux8V4W', '6emfMeXM0iQ0FSIa', '6EnmPNXy1mIZ5iT3',
       '6eOujgsdvvzBTekH', '6EsedVDfEDS3xiZQ', '6FaR7CJCJrfbFHIS', '6fF0F79pLpw2IbnX', '6FJHxA6txr3nnLbq',
       '6Fk6ynZ1UvWUExro', '6fLWvIrTepTcypuV', '6fModxjLmp5UzxtZ', '6FUNfN5xanDL42yH', '6fZxdLA5cwT8F1um',
       '6GiskNQVNQKHhhwP', '6gppBXF1y317bu6E', '6GQhsqvKuy5ob0j0', '6GuqFLondU2vZR14', '6Gvnwb76JkyfxY9o',
       '6gwnpXbey9HCvpoB', '6h234xvJ42YlFqGS', '6hD7Hg49tf7iOc2y', '6I7B3YjusoBG9dKH', '6IiqWDiXTujAODS0',
       '6IqMY3SFy1ndc5ib', '6ISoc5HWtOUawdyZ', '6Iub5SaKataiRdVu', '6jSt4Qq8kgm0pgzx', '6JvuhMULTfBdrTL2',
       '6k6RJ4kdh4flFg4u', '6kaCXFx2GqGUuhyF', '6kezxMMcMQnSjUyr', '6KfopfHCJfBaXt8N', '6KjLhdWQUJE7frN7',
       '6KpkpZy9oMpbKol1', '6ktpPV1U9Oz9KMiN', '6KXHZNm5Ldjgpqya', '6L79w7u8MT63TFOx', '6Ld9gDDO8mptkkLG',
       '6LxXdcmZa0AsqZ7l', '6M0x0AIRgm8ugrGN', '6M11npNKJ7TSOkaw', '6MCwQXXlE2yhRPSm', '6mqb9nLrMhw52qyr',
       '6MV4exmAr28y1tvU', '6n2nXgENihVr2pjP', '6nAXOiGVcGZHluon', '6niDfGrga0qmj1Vj', '6Nn9GIj0LkG5zsv0',
       '6ntb5YLJPWtBv0b5', '6NVOxRSxbHbNCb6t', '6o1yGdaBJfJkFQ2I', '6OPkVSRN64zTiI9y', '6oto2K79ifP7mLI9',
       '6Ox9MgaEZ25aIjfS', '6PQRl8tLch8mSlK8', '6Q5sJ7nSO0BgpKdo', '6QOr7ndwLlMyAvbJ', '6QskcJeYHRy2JYSA',
       '6qwKGhNpxr1Bp0UC', '6QXbbnNtIOjbE7os', '6R7IofrfoBMXEnSV', '6rCqhPBnBjRraH8F', '6rqOedsPgs0AhQvg',
       '6S0dZK6g92j9vopI', '6S4jmgpv17nraHqw', '6SHdyxDdUIAxxkEQ', '6sqt6swf7j2NGHMF', '6sTnSlObbssD1y0H',
       '6SxldgrZiOxJkiPb', '6T2KKoGVpN7TOPgJ', '6tbjlPmcvYzgEoud', '6tSN6aEIUNVMZXwn', '6TT3Rjnz0rF0VFbs',
       '6TUuqR675yj66nNt', '6TXm9BZxcxhGzw9m', '6umqhbGDTb6eIC2u', '6usHAqHai56PEYl4', '6UwURva79wJM1wMc',
       '6V4EJSfk7EqJhPVI', '6v5wR9bnwG5Rqw3v', '6VoovbiHEAKos2E4', '6VxqbFp3d1AwBXOR', '6W5EE58CqX9jsENp',
       '6W8zsxbljT26ecnD', '6wcVCfnHKoHoq0CJ', '6WjqFV0brPIelFDA', '6WR2ua6EwaY8OFHS', '6wr51oC0FWQEkYBx',
       '6WROfcebq7LL7e1H', '6WYIX63MFrjl1jti', '6wZY8te8R3GXs9w5', '6x8KRdlCgvN8o3Cq', '6xGotQP7lEGcEPJT',
       '6xGZ1yTz91cz4U0B', '6XJwzqcJsJp0Yqoq', '6xQ8TA4e0d38vw2k', '6Y5SC0rN3zzgU0sL', '6YbJOsxqLtH9O9Lb',
       '6Ydp9c2wVKLu3f1V', '6YoHb1L9mEa7WZU4', '6yoYxQmsK3oJFQav', '6yqnGemEYiOVLkTN', '6Yrqp3YxzdLznMug',
       '6YwmpyVrsSxbSCWD', '6YWy4vnuYaClJTti', '6z0dTD9HBQIIENu1', '6zc9yWKqhsq9Mzbp', '6ZedyW1w17NTx2Ih',
       '6zHgotZ8E1X1LwVC', '6zI4jtAgQop19zwU', '6ZPLrU0SNLv5ccTc', '7A2PJk7CqHGhm05q', '7aHeQ1vKUf2Mfzzc',
       '7B6FvfkDU7Sn684P', '7bDWzUnrC0lB7Woo', '7BM9o0WSLq0BwgkY', '7BnkzvbpWsMT3bHA', '7C0IlFhSdMDxYuxO',
       '7c9uIge9bJysnC9Z', '7CfjA2OAmJSg1EG0', '7CmwDATs4t0b3KEO', '7cXb4O206tGkDJ8e', '7DlMBeC0zvb4sxMo',
       '7dNuL8cEvQQFT53T', '7dZkuHVLiI40e0QI', '7EE4fan8HFjN0RBI', '7eFByF97PksMV6Di', '7Ek0AYyA9ZJbGEuT',
       '7ELqKY4De4EfflbJ', '7ENjYMpvlhZc93kz', '7Exba2EVDpHEQ9Fj', '7faTJwSLWNW9uMYh', '7FjebHNbJypdkyXM',
       '7fqIz104xVdMyIj5', '7fwM10IHl04Qi2Nu', '7g1YcV8jXyPFp6zO', '7G9Fdr3eZUYtMl4C', '7gu7CAuY3JlwsIvA',
       '7GVFrLPAQMq4j6ZB', '7h50v1LBMmDRLtv0', '7hFt5jXqFaZzwkwD', '7HIzW8hf4GWNZz81', '7ho4Wbx93Gi9tYty',
       '7HoGNrlxwOWWQzGy', '7hOWf75gNiurSuzc', '7hr5OKm5fDoIgq5v', '7hRNE5vXewmacgCu', '7HRPtCexdsbX7NCZ',
       '7HV1zE1A7buuSEkp', '7i5oAIwso1qUEY1I', '7i6Ec6YEK89zQcGK', '7ig7VOnEtWb0CFcJ', '7IIFwFfsOa26KG3A',
       '7iIookMcnr6ih5Ty', '7ImyHWhxCcotUZIK', '7IqgPiK5RkOB6R37', '7iSWyjtRjVbRchGV', '7ITlz0K8Clvvzevx',
       '7j1UQJ8MILa0OEg4', '7J9ZmQrg2VXInZfh', '7jA5TLfgigFoWQh6', '7jBMMqqSsYf8O0Gy', '7JDit0Tz1Vn3uJwo',
       '7jWL4qlcA1hRIyPL', '7kDTpcaBvKCmFb8g', '7l1LiepfCnWl2Wa9', '7L3wsyESfrHOkrQ4', '7L49Mg7tRXsNVyqH',
       '7l0784TaGtnZDeCz', '7lXafwVPg9c7FDoK', '7MCdw2q8Wq56kBpE', '7mfzzpThRvo7G1iW', '7mJsFPwpuLXefEC7',
       '7mmmxQIqq4p8IXEJ', '7MtqpT9yADNC2pBV', '7MTSvDCXxn5IEMQZ', '7mUHsG3F2p40acjX', '7muLKxihyKJ16DwG',
       '7mY3Gm8gAEIVqnQJ', '7ndaRFz6GsFtJVz3', '7nFQKunyw7ZqnpBi', '7nop1ARRK10zvkmH', '7NXT6FUIQGk9ZdIH',
       '7O8y0UzPIgaRnQq8', '7ocAzUGlJ7c7cwWe', '7oEepvvTgIQTfkie', '7OQb7mg7zH3D83F1', '7OUv43kqmD2Ks4qR',
       '7PhBtDFpAaeJi4Ve', '7PQITDxVKOujkmrA', '7PRScNFBlLewZVln', '7pvrwPDYa1kuR6Ag', '7pYFoWl4gDI1QTuB',
       '7q5R0WPJuQzSptUr', '7QAn5j75eYyj3AhG', '7QdlNH5qTbp99X5x', '7QLIwvlRNNFnuCCv', '7qnK79yzDemHOCKS',
       '7qNXvacRYkzovQxt', '7QQhY8XIMtyDmSe0', '7QS6Svyan9ktauBz', '7qtgnuuR1RjfzR94', '7QYJPbXr32rSrj4A',
       '7rdT4fDw5TuVt3bZ', '7RzZu0Nb4yWbO5q4', '7s7Nfsjz8RTFHe1p', '7sEbhMmmTbHbrsXj', '7sPuRnM65ax7M3UV',
       '7SvjHAErnOQLQlsS', '7tJdI58VTbmFJH2e', '7tNmMriep2F0B1Fo', '7TwAZZsgGGu7iX7h', '7TXlAScIFBHUFr0v',
       '7tzCmIXTPJRl7XC8', '7u2CP4LMApFJMojB', '7ugKJAgYRtnDYqMJ', '7UUiKKGhe9QnTgJ9', '7UW0BbWczT6DNNHv',
       '7vDuVqlJvqGnAvvQ', '7ViKvoKTfGJe1QFx', '7Vk8hJQpAJW8W5xv', '7VkLqSJRYfliTY11', '7VTsPePCdwjp1qUj',
       '7w8r2AEobzO4qHVc', '7WotDmSaCF7Es6QQ', '7X17FCYU9XczUdh2', '7xaOxk73DXPOlcrp', '7XBhZJzIFl5EzKP8',
       '7xUsWbUKid2dGKID', '7xvqy5r33r5vuLAM', '7xW7tCHSbYGpdiKX', '7xYF7ZXS9Z6Fwu92', '7Y2EUmqym0y0vbYC',
       '7y5R6rtIDxNxQmwW', '7YBI0awtj7PA5GQf', '7YQ0EEpNvZsySM6B', '7yQaQg2kOeZJDZ0n', '7yqvSTOYr5G8lQTI',
       '7YrOdPLZtI7hEyoA', '7yt9TuHfJcYJZ5FM', '7YwY6eTycb8QCEVp', '7yYjL0f42yDC5zD6', '7zBP1trYOscwYfiO',
       '7ZoUgwu5lFQFqiBd', '8aCWxrHlQnkXdYM1', '8ApyUU01L5Kiqzrh', '8B9kUGFWLgXzKwxR', '8bQcmIMAwj5fQ1mg',
       '8bVg6rBmINZRdh4l', '8BxrKGyCRM1rRex5', '8cjdVHIZdOsug8mE', '8cK6hU8NZaTsbukJ', '8CTarnX4yf6HOwQY',
       '8CUX9VMHOWTfKLhj', '8deyZP7NwZxyeYW9', '8DLBrhj02QnRtomw', '8DTJ13XqmIpPhHgK', '8DXIcPkJOUPXTDaE',
       '8DYj1YXAkPsSriUc', '8DyLvzKXy94oLaT0', '8EBGLIsRfD4MIdpN', '8EeyTVwmxIPL2bUU', '8EJ2VcuGuNgC9jCH',
       '8eWGwT05pn0YJjBH', '8f1dplQg5GgDV63w', '8f6cMpUMlx1ghvuL', '8fdYNlt7zuXjyjzg', '8fIIyGpbHlycTz0I',
       '8fixfcVOjEZ1wmLH', '8fTaOconaiQ96pf1', '8fxvOEs3A88ttUDz', '8g66Yx3p151CLp7r', '8giH0hWGf38UewPF',
       '8GKilUmOwOi331Ta', '8GS8jnOfxOR0pMyn', '8GT3ngO9QDv6zgcY', '8GtzFj56JGjpPvwY', '8gZAoARVtXQ6BDxd',
       '8HfHPpWgXNrNJiE3', '8Hmkd2P6rGsJs6dl', '8hpjJHctCreyXsjY', '8HX1GsydYZSegzgw', '8iAScX1HpL6QpTTW',
       '8IlfuVhqGA8bQQO2', '8iLzJd2kFmJ4ysCo', '8iucOuzWwdKollTv', '8JGMlz4NzFH7Q8CP', '8JmHyOZswSG1yur0',
       '8JmKxbFrW1ZzEuHe', '8jrlMDbdQJWLc2XY', '8JU59ZkKgsUkGH4p', '8Jz4kpJ53K4TvgPy', '8k5mDSihO2eSRn9R',
       '8K9MGg5Ip63vPUaV', '8k008srJx0O45a9m', '8KAZN8RAEozdrRc4', '8KF2zbV8PfCpfEog', '8kK3bdUtgieXzwGS',
       '8kkpJ9h5Noccx0kI', '8Kkv92POGiSR1Quf', '8ktuDl7wPmRVKv17', '8l6qGHA37vuHepaB', '8L7EWpteFOTNWXWy',
       '8L8nhyLOTa0DgSDb', '8LM0Kc3OrgT5s7Ib', '8lpbYba7AFIi3Dj0', '8ma8UpH6pw6V1Wcq', '8MpFuVvsowL81yV9',
       '8mtSaE7T3bT6l1Nh', '8N0CR5SIxlJhnCQZ', '8N3HDCc4AbysYKDh', '8nA3M6CgdmR9XFyd', '8ncZrQA8XqnWeGxa',
       '8NdLBuitUXXF5bmR', '8Ng7GLF7Idri9wEN', '8nJwhzte4UpJZBtj', '8OCiKX5TJCLmKwlf', '8ocmvpPjgYUO9XAd',
       '8oJ6U9z4xzyQLxyE', '8ojICOGMvYdb7aLI', '8ormFWnuihEVybOF', '8oY2xZOo8yAZqjyg', '8p0mT3Z1ov1ClnDR',
       '8P2D0UuhUz2LuBbi', '8P5Omkya7PDlCJcK', '8PBcOnzoUqWyqkfi', '8pL1zSo1ynM0B61n', '8Pln2XAfAVVTpwbg',
       '8PPNTwaQz655HsSM', '8Px3JnTZLse40UN0', '8PzcIx1FiKYzLY80', '8q7vONR0oaW9Ig1I', '8qKXadO6ooEXp0VG',
       '8qLrrxZRvVoVbmBd', '8r1H6GD0dZyI0E2H', '8r2dnv0IxbyK61cJ', '8r9iC832CrtvZ4PB', '8R09gJNuHr7AlN5S',
       '8RAtdmti2wNAwWmo', '8ruSECPnb1l2caIS', '8rV8pps679m6iSJm', '8SHaT4knEgll6v0q', '8shO8KGxcB3bo3Gh',
       '8shZdU2c8a2pdUEX', '8SK1DdUI2xAN6pkP', '8SPFped1a9utCnih', '8stii8NvoGjTXXLT', '8sY7JOyygBWdIdl4',
       '8sy8vrvCHuqkiwes', '8TbYGhMRXvWQaeua', '8ti9tz01gzp4wpD7', '8TLeKYj4OegEgxl3', '8TXTptxZ8ecOzxX6',
       '8U1kGVleX7aekT4T', '8U7HnPVSiAIzhCNR', '8UI9UNvLqfyLD9Yg', '8UlL3tETDpVXNobv', '8uQYzXuTqB2GA2iR',
       '8uRzyBtqkipEqzpq', '8US5yyY2oOcKFeJi', '8va3DwzHkT3lMR4T', '8vABpUpxLRUkSp1P', '8vE7RAkyeQy9Wci9',
       '8Vj3OJaMbdBolJ06', '8VndwyViXBsvTgIC', '8vOGkzebn9TUPhPO', '8VSNrCtZ1wTUSxka', '8vUwXR9MLUxn91jn',
       '8vWsKY7llrYjlQe2', '8VyFCS2aV1mxpVLu', '8Wk7vbL6mPnr67xJ', '8wKgXxADPF5r3CE7', '8WZ5SvfScbWzD9Nz',
       '8WZYdi8f0dRLWMaF', '8x9RC7UC4sLBZbtL', '8xKV0sn8G25Tq1oq', '8XnRtfuFj2214JAt', '8XtKG4bMKngoQT9B',
       '8Y2jbxP4CBAyxI7H', '8Y2zJHSUHnXXd8l3', '8Y7JiIpMQtpwyzdb', '8yHuHiOgeY0o9SkD', '8yw8Wi2cuFnB3whv',
       '8YwTygdYyZlgWTbQ', '8Z9LGQcffwkqNLVI', '8zy0k9DvaUqoPkQc', '9A7xAfXAjzwlmHs0', '9A8ozrdb4V9sPmaf',
       '9adPMQYvOBkq2zhi', '9aFEY49vYFKKvgCs', '9aqFN2SmSiQmUxj9', '9Azzgc2PVa7cFEPI', '9B4catyroG2TEQn4',
       '9BCpNgYSqLp40ZMZ', '9BOiX4h00k9jNsxo', '9bOTROaOKIZ4gt2y', '9Bq1aV7n6kaILZS9', '9bWO5eDivKMQYB5I',
       '9cdrFg5PxZsYT2JA', '9CKdr4zJQqj06zgv', '9clDrT4bR8TlxMrH', '9cx2zPQedwxLMtly', '9cZ8qTooPSYpqNoc',
       '9d4pxV9euvNHxCWO', '9dGWljwGgaEalYMD', '9DIyETpxF0G3lf8g', '9DSWHEbxv6n5oPQq', '9dYWpzxL32a0SCpZ',
       '9e2zx55ZeBEGmjZP', '9E7ygT55ey4tGe04', '9EAnQj2TDTBmVhou', '9emnrXnugqX5oo8l', '9erGoLOsZLFjZvl2',
       '9eTdFUOdTgu7RGTr', '9f6mAks3HKLmH46r', '9FLMofOevNuVxFSj', '9FmLZgIJQ6eB78lz', '9fPwEdBX0zrUrdV2',
       '9fSa95v08vasZShv', '9fy6cx7YSOAqBQw4', '9GeLibBUqwrMwq7u', '9GtA80SzA78R0ajm', '9GuA3csNnKmW2ubw',
       '9h0nPM0WYzCHNOYi', '9h4pBokUSEeD4WZM', '9H8cXk5s9S8WPhX0', '9h76WgE4lC9YrdQm', '9H86pDYHzymLKdhY',
       '9He4yKU3EyXXYvcC', '9HHAZ4CJwuvaI7hB', '9hICeoRLfkol0bRg', '9hIG960q86jdfSwW', '9HqLU93YCwjipYXN',
       '9IAl7rO1p9jbgB2o', '9iFuELQHoJk4YrXI', '9IOt9loEXzEdGPZc', '9Irr8leD3BFTTpyY', '9j1aHhfdgsBk8WuT',
       '9JHOnasrYprhnaRH', '9jnS9JcvABytJZSK', '9JPpNAzZt8qjSTpC', '9jq9Z8Xxjj0NK1Uu', '9JWbEPFRiXjPXYak',
       '9k5b3Nr3OtRIuxSI', '9kpUweyXS4FM3nkZ', '9kVTPU5MrWtFG2fl', '9lCWMQwJjnR41OJ9', '9lKyJbNR8iu9wXK0',
       '9ls5Ecr4hEdhRcwq', '9LsbMmmokLwwEUE8', '9lzjK9tZpqb10YTJ', '9M0xqgRgPwNmzzUn', '9m5Hz8h0Vd3tfr1q',
       '9Mbshori6AnDLu85', '9ml6B7fYhhVFpmOP', '9MP8dgi0PIaLJN3C', '9mVVnI63Gs4dV6Bi', '9n8ivq5vRGonPQu9',
       '9n8zPkf1rWC6h2JW', '9N63kTy436Fite4n', '9NFEZPagsFOtPVzU', '9njsUq2ucZBvSc9K', '9nU0C17trh1oCyIf',
       '9O2OqjrF86xCjrWO', '9OQNfQudotqdpCIr', '9ouNKwDndvnTbuGF', '9P2x1wcGPTsKQknR', '9PjFVhwiWSuvePtd',
       '9Q2nwvGuyTeGkQef', '9QOxBc2ZD2LIzqVy', '9QsTwRRTpEgNcFf9', '9QvpbZwY66GK80vO', '9R5YxVzrpzMrXCmk',
       '9rdF8A19jVomtrdD', '9rP6q62o6Bijt8l6', '9RtbhEQFt3MrduBH', '9RtqbPCm5tSKSTYW', '9rw5XXbrbrwqnqNN',
       '9s77dplBVOuFS0i1', '9sEGS9iUxyZzTz9z', '9sgl9qE3UquwMxny', '9sSuBc0IM6xStiWy', '9st63WcmuhfgDgsA',
       '9stao0E70nELL28y', '9SvhkMVeUfHIkpSW', '9sxEYr0eORzsmuJd', '9TC29TIawtBTwpBb', '9TCg1bHzpf5GonqB',
       '9tcrBXGem2dNZcx4', '9Te83IhnFJV6g2WJ', '9Tl5HIEaHrVCyu0u', '9TqAiBgTXXoem1zP', '9tqL0u1VcZCGJSVV',
       '9TRUmfXMVUepwblo', '9TwBvn7mwClKtqev', '9uE852MYXH9FUlYQ', '9uGP3Nh0bx7YEU0a', '9Ugq5NsFEvuw1khR',
       '9ujeiEnK8fjrkZ7A', '9UNZLrmHFczCpGCy', '9uRKXcv1LUdXiHp6', '9UTClg1VG44xHUwL', '9V0WmyqbDjWluklb',
       '9VJSGNPAdcfFSZnM', '9VWAFufKqEP8P2ZR', '9vzI8YfdyfzfFcxf', '9W1z6sl4156jqQiR', '9WayuYUKEhQPSmG2',
       '9WjzHyvVj83IKYh9', '9WKvkt41LCZskcA3', '9Wp1rgeUgEvC6747', '9wu87kLKaVprmVG8', '9WWewxKox9rKqTeP',
       '9X2MqW6UNEUfYKmw', '9xn2u7ljQlAqXk7s', '9XOXlY0bQA1tpslU', '9xtuIPj9OVUD4yDC', '9YEsKmuZ9xptxNze',
       '9YL6PljLGC7cSx05', '9YMIps1XrWY0jstW', '9yOf9wjwSsyMEegP', '9yw1u10ZlYr2sfCc', '9zefnZR3dwGjoQU5',
       '9Zt2XkMmABIe07Vg', '00LTUaz5sxDNK3eP', '00rUVcjFaP93VrdQ', '01BJqoyk83IuckqS', '01m8WsSifoHy0Zql',
       '01MpcF6GIBD2zPB1', '02BSEad6SndInlZo', '02cj3d7jMgLyKcMC', '02cO1989tpPWQzuk', '02KykirAYEOPnpat',
       '02lIot8UbtDOvhxl', '05KsPllDyZGasKeY', '05OdhyJsr7gdqP1z', '06kbwZGO5xLIvUnk', '07bauKlSGpj4pl5a',
       '07Hw0fSy7YxARYzs', '08XvmOWj7WNzo4Bv', '09TVTDj7f65gmun2', '10DGTzil6eNpsVHO', '10ORVIJ6s7uF0lS6',
       '11B0uRWfh9FqZ2NG', '11M2cC8cfBN92BS8', '11MCwXFm1tXYoT8I', '11RB63P9oK2Qft9A', '11so2KUnXrVJtgSo',
       '11y9AU8pEJ0bRhXR', '12hbnv1q5D6wdDwx', '12XfNXl5EjnEtkEc', '13gpULpMMQwcyf4C', '13JtB5x9wpzHVjNy',
       '13xWp1wtiWPeCTsg', '14ccICKnMXixJzxI', '14oBYmwoDY1RY5OE', '14rmKanIIv6pvhrz', '15jyceMdusMQB5c8',
       '15Qizomt5mlIfoAk', '16fKWojvChpeQTK5', '16Pb5ND7TZMnNqPR', '16yOVEMGEvHScrd5', '17uJZon9p3wDMKWa',
       '17UtimMobjmzOUpo', '18GMk11RP8q5UiMz', '18iINpwFpD9r6tMj', '19L7znGciEietMbY', '19lFXgTlTXiXVLV9',
       '19m7hnJD9dTzV9pK', '19trCD5PG9s1AGh1', '19uY5qbTujot03nd', '20NfeX6GTxUzhHu2', '22Br5UNJqcDg5eeG',
       '22CP0J9WFhlFpiAk', '22ESCVhmU245ySaj', '22TzXKZESUSXdhVm', '22zOPzXPnumCmtLl', '23nIZzZ9NlPSzQYJ',
       '23z3OEu6HvKnxNjD', '24epRnhJ0ji3JeM4', '24uNGjQMLuMhezNt', '24vC4KoLGwEp0gXX', '26hOqD339cjevQzT',
       '26KdiD0GjAKfszjn', '26Uk0buDgWFbIFNA', '26Y11sufdlxKGVpY', '28dLaCNr73MHQbDZ', '28txfGPdVnmq8Swa',
       '29nOsqK054wyFjQM', '30EJdVcH9kqjlg6b', '30s5WmqE3N8OUBoK', '30z9jcVct9E3ioqh', '31xBJQOKMI5V0US8',
       '32MBrrZchffluxAf', '32uUcbKjUaA4AVF2', '33fFqV3cBo6MkKXH', '33kitUwrylsxmur8', '34LBVbn72yzgXtRc',
       '34v2QzJXDvvj5Bh0', '35DWyPVvU4FBNhLt', '35OkHJxQFO3qFjLz', '36dXpirQXDdjWMRJ', '36lvMd3zf0jHHvnV',
       '36nh3sVNOtd0N41A', '36xdBaCORut4PESS', '37hyGfAlMgvKMJYX', '38sfkj4Q4oelfUuK', '39EUfn76ci2tadb9',
       '39FDOEY1ZYVaA3ld', '39RU47mhWMFPc89Y', '39xxblL93ov87Cng', '39zem3HfCltVAdTd', '40pXxNtDCRkaZlvn',
       '40QcpvWIqfOsW1oV', '40zh44WLvx2VXme8', '41J3cMxd4SqpbDIR', '42slf3XVbbrShopq', '43JPQ1K3bP8RcMNG',
       '43u6KwU9hFVF8AmD', '43UOhKPcwowcHrts', '44VlfgBTU9EehaSW', '45bCP62j4IvfcJzM', '46eajmNeorevPpkr',
       '48E8BkiMwLOLH5Q9', '48M9YRl14PYCZDQ1', '48xRPsUeB9bQuvcU', '49R50cAgWFntwEOY', '49SPQPtX1GDDApY7',
       '50BOMYWhHsb3toHm', '50IpJ0YeuUoTI5Gs', '50ODAF0OCzMOFy4d', '50y1eoTmT5oi70RS', '51Azm2N03wNa6nfw',
       '51M4PykUPvhix4Xk', '52j3sygWzI8fmwWh', '52mj5jsefRb2bLEz', '53A662ckcDO66P0o', '53q5gK6qrog5oj03',
       '53TZBMpp0tWtymGB', '54HQOm4vrGKOo72y', '54LJCldJig6f1wGS', '54PQo3BcQX19pZ7q', '54R6oJ1F7AhkH5N0',
       '55Kgx10fherIWkIb', '55Nr3MJthpei4LSd', '55s3mJ96pMgfYuly', '55YPkfkBrfUT7Y9P', '57tQ1qYPGyKxZ4Vi',
       '58wTWDq3ZlaVqkIC', '59a4zWQmuU1vCAij', '59OcoFkqmkzija8h', '60D8DQTsCaK5rzSQ', '60MVURpgJivvewgq',
       '60XKMy1bhC1936N4', '60XlGZWJTn7y3Ffy', '61hNQLY77HCxMPtu', '62cK3Omml8bJeCOI', '62DlCJOvO9nLQpis',
       '62LXO6oXJ3s9caGp', '62vMlsAdap5TjD98', '62ZoRWRr7uTujOaX', '63nkNYSQM4vCUtuH', '63pN4H6b2FkmUij4',
       '63z6FICUs25jWSwr', '64iQZHtGxd5T1NfY', '64WlveZvZQHkWRmd', '65jklwSGzSdARC8D', '65NsxKFDqhnuBEcG',
       '65pzMymAglCsp70v', '65YoW7dX9IzeSJ6O', '66JbK87klrD3H1i6', '66QMzB0mvH4OZCJj', '66rQlPAq7rPHUj1L',
       '68bv0eNThL3gpjtw', '68f3bHISbD5ZSZjV', '68x3UqKGR3bRy4Ek', '68yFJ8nBsmxCFfNy', '69EmbcjwK3DD1ZfK',
       '69kgdVqxtD0qJJTf', '72CPg3nubprrWUMr', '72GMADV0mXYddT4g', '73DXN9A8yD2N6Ant', '73PesXgbC2rQufsb',
       '73QEubMitsDkfjet', '74kLZOABqBymgmRM', '74yJBkgWfYFhMs2U', '76drK9dnztL15utg', '76n3wdIykJaEyEyy',
       '77oWY6CJ5yDk4aXI', '78Bv1vXkYp93lxpg', '78xIFByrfOrIaRFe', '79LkDKxw80Bz54ke', '79MngHyAWo3UVctE',
       '79ZmnehDxL6bmeT0', '80om7p5D24pKPuuB', '81cn95AdzXYlzkxO', '81Gs2cmuQjdM3y5N', '81WJ4hNf28wpabUb',
       '82GYPlH3D9S08AWc', '82WSMgvax8QbFrrB', '83aWute7PL9pkqPW', '83CQKbAcwSHO0Lx9', '84ccWXIQuMU9gRHQ',
       '85PlDRYWyFIY0ekL', '86dMMkCEi44If4TR', '87o05Q9bHlm521Ba', '87tZt8gPSDENL6BN', '88cegd1ZRrtdYye0',
       '88kLOKWNFmPNbimR', '88qZS7A1ET3sSRyH', '88RGGaeHJZsP4Zx4', '88xDtE6AOyVSVcqC', '89KRNNLUA6WYzJDw',
       '89ORPC669RYcYe5R', '90b5t3IMaNSqxKiu', '90DxpIDfgW2jR5hq', '90QoTGAEW1etgfxG', '93JEGtlInjLMao24',
       '95CRvMfevBPmd2q9', '95MoV8gM9cZHOp4H', '96TppLRmtwduNIHv', '97FpYMCjXvPXttZW', '97ldAayzJEmUxOvY',
       '98q73WvEuPmNkdKi', '98Qb6k74ZRWGfeI1', '98tMO8FVIcctI2w6', '99WUB7zxpSrkl6cC', '094mNXmP4c0jqdFU',
       '114PvF9ItibPamq5', '156CYIWyBY913dLi', '185nTFwGI7bKNhTS', '208uT1TxDDpJxNFn', '251ixp6zij2qOx1j',
       '257unzii0dPvrLu4', '267gjt1kLKIfGxxz', '327SDbV4Ypgr1pXu', '374QzqiWX9dRJj7b', '432hCU6yGqg68xF9',
       '439lQQ8V2aQrD82x', '465Gf4yLzEvRM1Tq', '514ThD1GcOPurnJv', '522d8drZgu0srfTR', '543naOutUXH2nDiv',
       '625fxhBvnI9jSYpp', '674Fe9VoxQmseM5j', '704t75feBbKACnnV', '723Np8VYo0yrSs6v', '784Qm38DzQ6hfxCT',
       '872uPf91DkLexUm2', '902KymrMyLvR93l1', '913GWJIJPL6KczCz', '917vkLximePb6S3J', '927Eg5oa2bWbq2wo',
       '970eblmikhk8Skv0', '971sNb28zf8VeYIs', '1444pwNLrdPEKk4s', '3430YBVvmFxmTKt0', '4519kQdpcjbPgO5g',
       '7280IG77k7q0I7mP', '7978EJuo2mdShr2j', '9368xx0Scad3IiGJ', '9420b3avcXOMQLES', '00393PQhlcR77kQC',
       'a0V0L1Geuegbd1VQ', 'a1U9xnDMSsxLVo3I', 'A2BG5tRs5cay4pn7', 'a2CkeunOz28i7prT', 'a2jHyRCmleg7wQYy',
       'A2PRC3GfnyKwjpsd', 'a2Z4RTK31jlQ2ZLP', 'a3R6z3VR8Rtjk7R2', 'a3TvUH5zpgQZ5Io7', 'a3un5xJ7Mb2MJrw0',
       'a3wg1BLx1AUQNY6m', 'A4JdFljgnQApkO4M', 'a4r7fJuv7M5E7uYR', 'a4vCCg5Zdp0DAMtz', 'a4X9VszwPwCWoiEx',
       'a5iWtovjWWqE1j81', 'A5jpQtFRD99v5xz7', 'A5M0Tc4P0aYx7gLT', 'a5oRbyirFCJxTp8x', 'a5OZQ79hEnwNryQ9',
       'A5w8OlfoQICFzHeD', 'A5WKOthKup2IsvWO', 'A6QqlkpE7ZPBzmFz', 'a6VRpHEUWqxea2kd', 'a7a6tu7n3tEHPzCl',
       'A7KuHcwqYTK8YO3T', 'A7leLAWatKyzit9n', 'a7zjvxPiux3hmM72', 'a8kNO9h9ERIX9OjU', 'a8Kx8lF298sGU2mP',
       'A8LlQ5bOhlNe1bYS', 'a8p8QtqrHs9M75YA', 'A8sadY2LKlDU7pph', 'A8VFgXu7Ahz4LV8M', 'a8W1gFbma6bCbwx8',
       'A8XY4DtviA2rAmzt', 'a9GsDly7KHJquDgC', 'A9iXGUQ7ngJeB4JF', 'a9M3ITUSv1jrJ0y7', 'a12FzCmZmq5uhWLp',
       'a14mrEaEhqMoj0Bh', 'a23fUx84feNvyvLh', 'a27q1y2H8hU7SS5i', 'A43jl77IUoWeB6pH', 'A43Kkf99Jrqei2fz',
       'a48chw9mjp7XsRHj', 'a81yKy8SLspVFYEY', 'A82LZIIGIhXHZJKq', 'A83zvAHXk3h6Gfuu', 'a85J9jG5Udd11ptC',
       'A87Oxkftl30ZHjHD', 'a339BgihpzuTtWoa', 'aAazJH8VAiYaD3KK', 'aabri6IegLOwyfkU', 'aahDepP1EOLrzxyJ',
       'AahgSE2m1ac8ufkD', 'aakx6l8OH8tvPVCk', 'AAp5k9BfLrY9HcI4', 'aapikGp19wexRZ5j', 'AAPPO2s11WE6Oj5G',
       'aAqRYWQpNKHoAQVN', 'AaSyML7AE3JR3wOH', 'AAtDhMJpklTUXDuf', 'aazI9V4PyAkYJCRL', 'ab1lHTb91MA0zvGa',
       'AB1qeLt1D4ugvOTp', 'aB9f69vKgf30rdYp', 'Ab68Xpe6kBuY99Sr', 'ABAqUPOBnjWk91Xq', 'AbbmGBayRMq3WmwL',
       'aBD7fRvzIVvd7iu0', 'ABdiCeXqGRtVuT5K', 'aBhaSTB78cfhcdnO', 'aBK0U9Dz9QGtEm0L', 'aBL4PWiek24ubWiJ',
       'ABMYedLvGOOQ1JKi', 'AbnAneEHaLm6Lliz', 'ABPSw6mFk5n5NSsM', 'abRHOKtnzqYp7q1r', 'ABWmTqIhju6pfF7F',
       'abznv2gzpoJ403TI', 'aC36scae72oUEjzN', 'aCdJdVxoW0ias8uN', 'acEIxkh1RbGbs8gH', 'acgp2InQaj5H03mK',
       'aCp1U2cN0ypqV7In', 'acQTufI0fwmFm4sz', 'AD3zTwOKt1UQ2owG', 'AdBcF89o6Vp0TryW', 'AdCDU3dVEGonVSa1',
       'AdF3NInKK1mck3VJ', 'adFa8UkzgrMq0fH9', 'ADFuJpNNQH0eJ1Ia', 'AdjiifcOnaWSo8QL', 'adMhZ1rKjsOkOHCS',
       'aDn47PcYGOFwvzLw', 'AdNCJak78DJKGzcD', 'aDqNVf4oTVim2PP3', 'ADXxpUBKit8RykFI', 'AeAoaISgLKT5n15C',
       'AEbqTnRG48ltTzJE', 'Aeh1QnoowSHmZMFW', 'Aei2jb9ieAJW6D2l', 'aEIkAiPb8FpeQoNk', 'aejfQIRIYeHvp5hC',
       'aeJjpYAWm3iZq2Ui', 'Aeo9wXFcguaYhbr8', 'aEoxmdtlgC418Ia6', 'AEQouHPTGfvvKY2N', 'AeSJSz2edLGV7K5L',
       'AesRWxrK85Ie92D0', 'AEvFDyzBDSsYXdLg', 'aexhHXXt3bGmdkYI', 'aeY3FvtNKQFIB9uV', 'AF4KroJHLqrcMvXY',
       'aF8WZmuoVIjdWqWY', 'af23eZ6TeA7srdRN', 'aFCAWgQrFTRrleh4', 'afkssOp0CLaACzAe', 'aFmIrLFMWSSTYsiD',
       'aFSMECR2gldYbZwz', 'ag1Xq4ztvEJt5fNr', 'aGbQV0Y6qboimUjV', 'agvnZ6rAWfAS8I4f', 'AGWNFAqNec0Aet2E',
       'aGz2rTkAV15R7vxW', 'AgzlrVkZ80XMy4eu', 'Ah8KI02srNQl7dlc', 'aH9Qe87w0K1wga4Z', 'ahiKWh85WycUM5hr',
       'ahl5Xvdd9DTInXh1', 'AHsZvYYL31WEZ8Bj', 'AhW0K9CiudKhGKkv', 'AHWlPiB10M2PPYXl', 'AhwNhIc0M3yOiqqw',
       'AI6fy8p2sN4MnXKd', 'aidIK6HFFpBC7Zzb', 'AIe7ZDghLyYep2z1', 'AIgfjFtb014Ywdxo', 'AiggopDFKULo4IAJ',
       'aiNa5E83qoANA2iu', 'aiOxUzrqoftoLrif', 'Aj4Y3YPvw8YzyAHQ', 'aj6vVnkXjr5gD4lG', 'aj7lyMIFBAgAUa99',
       'Aj90OpZg3Xw5XOuc', 'aJikJmEAub5H7II5', 'aJo7spzdX57a871v', 'AjRFVSa6XYVJVx2B', 'AjrSObXXjhrmrV3g',
       'AJU5CtSk4O8nsEkD', 'aJUHaUi7OC48QyGN', 'aJv6pJnUK3z3ih2e', 'ak3oaRsfSFVjgx6Q', 'ak9pAyTxDJ')
--group by 1,2,3,4
--order by biz_count desc

select date(creation_time),
       count(distinct business_id)
from dwh.quotes_policies_mlob
where creation_time >= '2024-04-01'
group by 1
order by 1 asc

select json_args,
       json_extract_path_text(json_args, 'prospects_json', 'businessownershipstructure', true) as entity_type,
       json_extract_path_text(json_args, 'lob_app_json', 'businessownershipstructure', true)   as entity_type2
from dwh.quotes_policies_mlob
where highest_policy_status >= 3
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and lob_policy = 'GL'
  and creation_time >= '2024-01-01'
limit 10

SELECT COALESCE(
               NULLIF(json_extract_path_text(json_args, 'prospects_json', 'businessownershipstructure', true), ''),
               json_extract_path_text(json_args, 'lob_app_json', 'businessownershipstructure', true)
       )                           AS entity_type_combined,
       count(distinct business_id) as biz_count,
       sum(highest_yearly_premium) as wp
FROM dwh.quotes_policies_mlob
WHERE highest_policy_status >= 3
  AND offer_flow_type IN ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  AND lob_policy = 'GL'
  AND creation_time >= '2024-01-01'
--and entity_type_combined = ''
group by 1
order by 3 desc

--March PIF and avg premium by segments
select cob_group,
       count(distinct business_id)                               as pif_count,
       sum(highest_yearly_premium) / count(distinct business_id) as avg_premium
from dwh.quotes_policies_mlob
WHERE highest_policy_status >= 3
  AND offer_flow_type IN ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  AND lob_policy = 'GL'
  AND creation_time >= '2024-03-01'
  and creation_time < '2024-04-01'
  and cob_group in ('Professional Services', 'Retail', 'Food & beverage')
  and distribution_channel not in ('agents', 'partnerships', 'Next Connect', 'ap-intego', 'support', 'support_outbound')
group by 1
order by 2 desc

--top grocery decline reasons
select cob,
       --(CASE WHEN (affiliate_id = 'N/A' and  agent_id = 'N/A') then 'direct'
       --    WHEN (affiliate_id <> 'N/A' and  agent_id = 'N/A') then 'affiliate'
       --    else 'agent' end) as channel,
       --uw.lob,
       decline_reasons,
       execution_status,
       count(distinct (business_id)) as biz_count
from dwh.underwriting_quotes_data uw
         join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
where --execution_status = 'DECLINE'
    decline_reasons like '%You sell products that cannot%'
  and offer_creation_time >= '2024-01-01'
  and offer_creation_time <= '2024-03-31'
  and uw.lob = 'GL'
  and uw.cob = 'Grocery Store'
group by 1, 2, 3
order by biz_count desc

--to get 100 largest grocery policies
select business_id,
       highest_yearly_premium
from dwh.quotes_policies_mlob qpm
where highest_policy_status >= 4
  and lob_policy = 'GL'
  and creation_time >= '2023-01-01'
  and cob = 'Grocery Store'
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
order by 2 desc
limit 100

--top daycare agents, top agents
select agent_id,
       agent_name,
       agency_name,
       current_agencytype,
       sum(yearly_premium)           as total_premium,
       count(distinct (business_id)) as total_businesses
from dwh.agents_policies_ds
where start_date >= '2023-01-01'
  and
  --start_date < '2023-01-01' and
    policy_status >= 3
  and (bundle_lobs like '%GL%')
  and cob in ('Day Care')
group by 1, 2, 3, 4
order by 6 desc

--top non-renewal decline reasons
select uw.business_id,
       uw.lob,
       decline_reasons,
       execution_status,
       count(distinct (business_id)) as biz_count
from dwh.underwriting_quotes_data uw
         join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
where --execution_status = 'DECLINE'
    decline_reasons like '%You sell products that cannot%'
  and offer_creation_time >= '2024-01-01'
  and offer_creation_time <= '2024-03-31'
  and uw.lob = 'GL'
  and uw.cob = 'Grocery Store'
group by 1, 2, 3
order by biz_count desc


--MLOC policies sold
select distinct decline_reasons,
                lob,
                p.email,
                json_extract_path_text(business_details, 'applicantfirstname')                                 as first_name,
                json_extract_path_text(business_details, 'applicantlastname')                                  as last_name,
                d.business_id,
                marketing_cob_group                                                                            as cob_group,
                d.cob,
                offer_creation_time                                                                            as creation_time,
                json_extract_path_text(source_json_last_related_business_id_before_first_lead_paid,
                                       'distribution_channel_type')                                            as distribution_channel,
                json_extract_path_text(session_json_last_related_business_id_before_first_lead_paid,
                                       'device')                                                               as device,
                policy_id                                                                                      as highest_policy_id,
                policy_status                                                                                  as highest_policy_status,
                policy_status_name                                                                             as highest_status_name,
                yearly_premium                                                                                 as highest_yearly_premium,
                json_args, --mloc
                lob                                                                                            as lob_policy,
                execution_status,
                d.offer_flow_type,
                d.state_code                                                                                   as state,
                policy_type_name,
                bundle_name,
                case
                    when bundle_name = 'pro' or bundle_name = 'proTria'
                        then yearly_premium end                                                                as pro_yearly_premium,
                agent_first_name,
                agent_last_name,
                agy.agency_id,
                agency_name,
                agency_aggregator_name,
                agy.agency_aggregator_id,
                dec.related_business_id
from dwh.underwriting_quotes_data d
         join dwh.quotes_policies_mlob_dec dec
              on dec.offer_id = d.offer_id -- and dec.offer_flow_type = 'APPLICATION' and d.lob = dec.lob_policy
         join dwh.sources_attributed_table sat on sat.business_id = d.business_id
         left join partnership_svc_prod.agents ag on ag.agent_id = dec.agent_id
         left JOIN partnership_svc_prod.agencies agy ON ag.agency_id = agy.agency_id
         left JOIN partnership_svc_prod.agency_aggregators agg ON agy.agency_aggregator_id = agg.agency_aggregator_id
         left join underwriting_svc_prod.prospects p on p.business_id = d.business_id
where offer_creation_time >= '2023-07-01'
  and d.offer_flow_type = 'APPLICATION'

select json_extract_path_text(json_args, 'additional_locations_in_state_of_primary_location', true) as addtl_location,
       json_args
from dwh.quotes_policies_mlob
where highest_policy_status >= 4
  and lob_policy = 'GL'
  and creation_time >= '2024-01-01'
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
limit 1000


select cob, cob_group, sum(highest_yearly_premium) as premiums_total, count(distinct business_id) as policy_count
from dwh.quotes_policies_mlob
where highest_policy_status >= 4
  and lob_policy = 'PL'
  and policy_end_date >= '2024-01-01'
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
group by 1, 2
order by 3 desc

--de-duped prospect list deduped
with query as (select p.business_id,
                      qpm.cob,
                      qpm.lob_policy,
                      json_extract_path_text(p.business_details, 'businessname')                       as business_name,
                      json_extract_path_text(p.business_details, 'applicantfirstname')                 as first_name,
                      json_extract_path_text(p.business_details, 'applicantlastname')                  as last_name,
                      json_extract_path_text(p.business_details, 'telephonenumber')                    as phone_number,
                      json_extract_path_text(p.business_details, 'emailaddress')                       as business_email,
                      row_number() OVER (PARTITION BY qpm.business_id ORDER BY qpm.creation_time DESC) AS rank,
                      qpm.distribution_channel,
                      qpm_max_date.max_creation_time
               from underwriting_svc_prod.prospects p
                        inner join (select business_id, MAX(creation_time) as max_creation_time
                                    from dwh.quotes_policies_mlob
                                    where creation_time >= (getdate() - 7)
                                    group by business_id) qpm_max_date on p.business_id = qpm_max_date.business_id
                        left join dwh.quotes_policies_mlob qpm on p.business_id = qpm.business_id and
                                                                  qpm.creation_time = qpm_max_date.max_creation_time
               where qpm.distribution_channel <> 'agents'
                 and qpm.distribution_channel not like '%ap-%'
                 and qpm.cob_group in ('Retail')
                 AND p.business_id in (select business_id
                                       from dwh.quotes_policies_mlob
                                       group by business_id
                                       having MAX(highest_policy_status) = 1))
select *
from query
where rank = 1

--list of active restaurant policies bound on/after 1/1/2023
select business_id,
       cob,
       distribution_channel, --if you just want direct/agents/partnerships, replace with (CASE WHEN (affiliate_id = 'N/A' and  agent_id = 'N/A') then 'direct' WHEN (affiliate_id <> 'N/A' and  agent_id = 'N/A') then 'affiliate' else 'agent' end) as channel,
       lob_policy,
       highest_yearly_premium,
       creation_time,
       policy_start_date
from dwh.quotes_policies_mlob
where highest_policy_status = 4
  and                     --if you want all policies active or bound, replace with >=3
    offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and                     --if you want only new business, delete 'RENEWAL' and 'CANCEL_REWRITE'
    creation_time >= '2023-01-01'
  and                     --this is when bound; if you want effective date, replace with policy_start_date
    cob in ('Restaurant') --add COBs if you want more in F&B group or update to cob_group = 'Food & beverage'
order by 5 desc

--day spa policies
select business_id, highest_yearly_premium
from dwh.quotes_policies_mlob
where cob = 'Day Spas'
  and creation_time >= '2024-01-01'
  and highest_policy_status = 4
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
order by 2 desc

--amazon PIF
select (CASE
            WHEN (affiliate_id = 'N/A' and agent_id = 'N/A') then 'direct'
            WHEN (affiliate_id <> 'N/A' and agent_id = 'N/A') then 'affiliate'
            else 'agent' end) as channel,
       sum(highest_yearly_premium),
       count(distinct business_id)
from dwh.quotes_policies_mlob
where highest_policy_status = 4
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and creation_time >= '2023-01-01'
  and cob = 'E-Commerce'
  and lob_policy = 'GL'
group by 1

select affiliate_id, count(distinct (business_id)), sum(highest_yearly_premium)
from dwh.quotes_policies_mlob
where affiliate_id in ('4700')
  and highest_policy_status = 4
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
group by 1

--decline rate by channel (Reed Heim table)
select distribution_channel,
       count(distinct case when stepstatus = 'DECLINE' THEN lob_application_id ELSE NULL END) as declines,
       count(distinct lob_application_id)                                                     as total_applications
FROM db_data_science.ds_decline_monitoring
WHERE offer_creation_time >= '2023-11-15'
  AND offer_creation_time <= '2024-04-18'
  AND offer_flow_type = 'APPLICATION'
GROUP BY 1

--AP intego next carrier % (does not work -- no access)
SELECT date_trunc('month', effective_date) as month,
       COUNT(DISTINCT policy_id),
       COUNT(DISTINCT CASE WHEN policy_number ilike 'NXT%' THEN policy_id END),
       COUNT(DISTINCT CASE WHEN policy_number ilike 'NXT%' THEN policy_id END) * 1.0 / COUNT(DISTINCT policy_id)
FROM ap_intego_db.partnership.dim_policy
WHERE origin_domain = 'AP Intego'
  AND looker_new_vs_renewal = 'New'
GROUP BY 1
ORDER BY 1 DESC


--QTP and ASP for pre/post CP deductibles
with base as (select distinct qpm.creation_time::date                                           as quote_day,
                              qpm.business_id,
                              qpm.highest_policy_id,
                              qpm.highest_policy_status,
                              qpm.highest_yearly_premium,
                              case when creation_time < '2023-11-01' then 'pre' else 'post' end as pre_post
              from dwh.quotes_policies_mlob qpm
              where qpm.lob_policy IN ('CP')
                and qpm.creation_time > '2023-10-01'
                and qpm.offer_flow_type = 'APPLICATION'
                and qpm.distribution_channel = 'direct'
                and qpm.cob in ('Personal Trainer', 'Fitness Instructor', 'Yoga Instructor', 'Crossfit Instructor',
                                'Pilates Instructor', 'Indoor Cycling Instructor', 'Aerobics Instructor',
                                'Zumba Instructor', 'Auto Parts Store', 'Craft Artists', 'Etchers and Engravers',
                                'Set and Exhibit Designers', 'Community Gardens', 'Printing Services',
                                'Audio and Video Equipment Technicians', 'Camera and Photographic Equipment Repairers',
                                'Photo Editing, Scanning and Restoration', 'Art Space Rentals', 'Locksmith',
                                'Musical Instrument Services', 'AV Equipment Rental for Events',
                                'Medical Supplies Store', 'Knife Sharpening', 'Scavenger Hunts',
                                'Vending Machine Operator', 'Print Binding and Finishing Workers', 'Glass Blowing',
                                'Wedding and Event Invitations', 'Screen Printing and T Shirt Printing', 'Print Media',
                                'Retail Stores', 'Bike Shop', 'Bike Rentals', 'Bookstore',
                                'Newspaper and Magazine Store', 'Clothing Store', 'Department Stores', 'Discount Store',
                                'Electronics Store', 'Fabric Store', 'Furniture Store', 'Baby Gear and Furniture Store',
                                'Hardware Store', 'Arts and Crafts Store', 'Hobby Shop', 'Candle Store',
                                'Home and Garden Retailer', 'Lighting Store', 'Jewelry Store', 'Packing Supplies Store',
                                'Flea Markets', 'Nurseries and Gardening Shop', 'Eyewear and Optician Store',
                                'Paint Stores', 'Pet Stores', 'Furniture Rental', 'Sporting Goods Retailer',
                                'Fitness and Exercise Equipment Store', 'Horse Equipment Shop', 'Luggage Store',
                                'Pawn Shop', 'Toy Store', 'Demonstrators and Product Promoters', 'Fitness Studio',
                                'Fencing Instructor', 'Sports Coach', 'Umpires, Referees, and Other Sports Officials',
                                'Martial Arts Instructor'))
select pre_post,
       average_purchased_premium,
       purchases / quotes::decimal(10, 2) as qtp
from (select pre_post,
             avg(case when highest_policy_status >= 3 then highest_yearly_premium end) as average_purchased_premium,
             count(distinct business_id)                                               as quotes,
             sum(case when highest_policy_status >= 3 then 1 else 0 end)               as purchases
      from base
      group by 1)


select avg(case when highest_policy_status >= 3 then highest_yearly_premium end) as average_purchased_premium
from dwh.quotes_policies_mlob qpm
where qpm.lob_policy IN ('GL')
  and qpm.creation_time > '2024-01-01'
  and qpm.offer_flow_type = 'APPLICATION'
  and qpm.cob = 'Restaurant'


--pro services top agents policy list
select agent_id,
       agent_name,
       agency_name,
       current_agencytype,
       yearly_premium,
       business_id,
       policy_status_name,
       bundle_lobs,
       cob
from dwh.agents_policies_ds
where start_date >= '2024-01-01'
  and policy_status >= 3
  and
  --(bundle_lobs like '%PL%' or bundle_lobs like '%GL%') and --add if you're looking for specific LOB
  --cob in ('Accountant','Actuarial Service') and --add if you want to narrow by COB
    agency_name like '%TruPoint Marketing%' --case sensitive
order by 3

select business_id
from dwh.quotes_policies_mlob
where creation_time >= '2024-01-01'
  and highest_policy_status = 4
  and highest_yearly_premium < 700
  and highest_status_package like '%Tria%'
  and cob = 'General Contractor'
  and lob_policy = 'GL'
limit 10

--user activity logs
select *
from dwh.all_activities_table aa
         join dwh.sources_attributed_table ss on aa.tracking_id = ss.tracking_id
where business_id = 'ebeb433af75b5a353002762245f1208b'

--permitted LOB / state / COB
WITH appetite_universe AS (SELECT cobs.cob_id,
                                  cobs."name"         cob_desc,
                                  upper(replace(s.name,
                                                ' ',
                                                '_')) state,
                                  s.code              state_code,
                                  lobs.code           lob,
                                  CASE
                                      WHEN EXISTS ( -- If it's present in the `permitted` table, it's still in use and therefore not deprecated
                                          SELECT 'x'
                                          FROM silver_portfolio.permitted p
                                          WHERE cobs.cob_id = p.cobid) THEN
                                          'ACTIVE'
                                      ELSE
                                          'DEPRECATED'
                                      END             cob_deprec_status
                           FROM nimi_svc_prod.cobs cobs
                                    CROSS JOIN nimi_svc_prod.states s
                                    CROSS JOIN nimi_svc_prod.policy_types lobs
                           WHERE cobs."name" NOT LIKE '%deprecated%' -- extra cleanup
                             AND lobs.code in ('CP', 'GL', 'IM', 'PL', 'WC')),
     permitted AS (
         -- Create a distinct view of our current posture from portfolio at the same COB/STATE/LOB Granularity.
         SELECT *
         FROM (SELECT cobid                                                                    AS cob_id,
                      lob,
                      state,
                      channel,
                      action,
                      row_number() OVER (PARTITION BY cobid,
                          state,
                          lob,
                          channel ORDER BY event_occurrence_time_pst_timestamp_formatted DESC) AS ranking
               FROM silver_portfolio.permitted p
               WHERE flowtype = 'PURCHASE'
                 AND channel = 'direct' -- direct marketing concerned about direct channel flows
              ) t
         WHERE ranking = 1)
SELECT appetite_universe.cob_id,
       appetite_universe.cob_desc,
       appetite_universe.lob,
       appetite_universe.state_code,
       permitted.channel,
       CASE
           WHEN appetite_universe.cob_deprec_status = 'DEPRECATED' THEN
               'DEPRECATED'
           WHEN permitted.action = 'OPEN' THEN
               'OPEN'
           ELSE
               'CLOSED' END current_status -- Key status determination here
FROM appetite_universe
         LEFT JOIN permitted ON permitted.cob_id = appetite_universe.cob_id
    AND permitted.state = appetite_universe.state
    AND permitted.lob = appetite_universe.lob
WHERE current_status = 'OPEN'
ORDER BY appetite_universe.cob_desc,
         appetite_universe.lob,
         appetite_universe.state_code;

--Aditya cross-sell research
select case when agent_id <> 'N/A' then 'Agent' else 'Direct / Partnerships' end            channel,
       cob_group,
       case when cob = 'E-Commerce' then 'e-comm' else 'not_e-comm' end                     e_comm_group,
       json_extract_path_text(json_args, 'lob_app_json', 'business_is_located_in', true) as location_type,
       count(distinct business_id)
from dwh.quotes_policies_mlob
where creation_time >= '2023-01-01'
  and highest_policy_status = 4
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and lob_policy = 'CP'
  and cob_group in ('Retail', 'Food & beverage', 'Professional Services')
  and channel <> 'Agent'
  and location_type <> ''
group by 1, 2, 3, 4
order by 2, 3, 4

--Aditya cross-sell research (retail market)
SELECT outer_query.channel,
       outer_query.cob_group,
       outer_query.e_comm_group,
       outer_query.brick_mortar_store,
       CASE WHEN inner_query.business_id IS NOT NULL THEN 'yes' ELSE 'no' END AS cp_customer_yes_no,
       COUNT(DISTINCT outer_query.business_id)
FROM (SELECT CASE WHEN agent_id <> 'N/A' THEN 'Agent' ELSE 'Direct / Partnerships' END channel,
             cob_group,
             CASE WHEN cob = 'E-Commerce' THEN 'e-comm' ELSE 'not_e-comm' END          e_comm_group,
             json_extract_path_text(json_args, 'lob_app_json', 'retail_market_physical_store',
                                    true) as                                           brick_mortar_store,
             business_id
      FROM dwh.quotes_policies_mlob
      WHERE creation_time >= '2023-01-01'
        AND highest_policy_status = 4
        AND offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
        AND lob_policy = 'GL'
        AND cob_group in ('Retail')
        AND channel <> 'Agent'
        AND brick_mortar_store <> ''
      GROUP BY 1, 2, 3, 4, 5) outer_query
         LEFT JOIN
     (SELECT DISTINCT business_id
      FROM dwh.quotes_policies_mlob
      WHERE lob_policy = 'CP') inner_query
     ON outer_query.business_id = inner_query.business_id
GROUP BY outer_query.channel, outer_query.cob_group, outer_query.e_comm_group, outer_query.brick_mortar_store,
         cp_customer_yes_no
ORDER BY outer_query.cob_group, outer_query.e_comm_group, outer_query.brick_mortar_store, cp_customer_yes_no

--Aditya research #2
SELECT outer_query.channel,
       outer_query.cob_group,
       outer_query.employees_yesno,
       CASE WHEN inner_query.business_id IS NOT NULL THEN 'Yes' ELSE 'No' END AS cp_customer_yes_no,
       COUNT(DISTINCT outer_query.business_id)
FROM (SELECT CASE WHEN agent_id <> 'N/A' THEN 'Agent' ELSE 'Direct / Partnerships' END channel,
             cob_group,
             case when num_of_employees > 0 then 'Yes' else 'No' end                   employees_yesno,
             business_id
      FROM dwh.quotes_policies_mlob
      WHERE creation_time >= '2023-01-01'
        AND highest_policy_status = 4
        AND offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
        AND lob_policy = 'GL'
        AND cob_group in ('Retail', 'Food & beverage', 'Professional Services')
        AND channel <> 'Agent'
      GROUP BY 1, 2, 3, 4) outer_query
         LEFT JOIN
     (SELECT DISTINCT business_id
      FROM dwh.quotes_policies_mlob
      WHERE lob_policy = 'CP') inner_query
     ON outer_query.business_id = inner_query.business_id
GROUP BY outer_query.channel, outer_query.cob_group, outer_query.employees_yesno, cp_customer_yes_no
ORDER BY outer_query.cob_group, outer_query.employees_yesno, cp_customer_yes_no

--to get top declines by COB, channel and LOB
select (CASE
            WHEN (qpm.affiliate_id = 'N/A' and qpm.agent_id = 'N/A') then 'direct'
            WHEN (qpm.affiliate_id <> 'N/A' and qpm.agent_id = 'N/A') then 'affiliate'
            else 'agent' end)            as channel,
       uw.lob,
       decline_reasons,
       count(distinct (qpm.business_id)) as biz_count
from dwh.underwriting_quotes_data uw
         join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
         join dwh.quotes_policies_mlob qpm on uw.business_id = qpm.business_id
where execution_status = 'DECLINE'
  and decline_reasons not like '%","%'
  and offer_creation_time >= '2024-01-01'
  and uw.lob in ('CP')
  and qpm.state = 'DC'
  and channel = 'agent'
group by 1, 2, 3
order by biz_count desc

--CP decline rate
select uw.business_id, state, lob, execution_status
from dwh.underwriting_quotes_data uw
         join dwh.quotes_policies_mlob qpm on uw.business_id = qpm.business_id
where uw.offer_creation_time >= '2023-01-01'
  and uw.lob = 'CP'
  and
  --execution_status in ('DECLINE','SUCCESS') and
    execution_status = 'SUCCESS'
  and uw.offer_flow_type in ('APPLICATION')
  and qpm.state = 'DC'

--unsupported leads by date and channel
select eventtime::date,
       json_extract_path_text(source_json_last_related_business_id_before_first_lead, 'channel', true) as channel,
       count(distinct related_business_id)
from dwh.all_activities_table aa
         join dwh.sources_attributed_table ss on aa.tracking_id = ss.tracking_id
         left join portfolio_svc_prod.permitted_cobs_states_lobs list
                   on list.cob_id = ss.cob_id and list.state_code = ss.state_code and aa.lob = list.lob
where date(eventtime) >= '2024-03-01'
  and funnelphase like '%Unsupported%'
  and aa.cob_name in
      ('3D Modeling', 'Acai Bowls', 'Acne Treatment', 'Acupressurist', 'Acupuncturist', 'Addiction Medicine',
       'Adoption Services', 'Aestheticians', 'Alterations, Tailoring, and Clothing Design', 'Alternative Medicine',
       'Amateur Sports Teams Coach', 'Animal Trainers', 'Animation', 'Anthropology and Archeology Teachers',
       'Antiques Store', 'Apartment Agents', 'Appraisal Services', 'Arabic Lessons', 'Archery Coach',
       'Architecture Teachers', 'Area, Ethnic, and Cultural Studies Teachers', 'Art Directors', 'Art Tours',
       'Art, Drama, and Music Teachers', 'Astrologers', 'Athletic Trainer',
       'Atmospheric, Earth, Marine, and Space Sciences Teachers', 'Audio Production Lessons', 'Audio Recording',
       'Audio Services', 'Audio Visual and Multimedia Collections Specialists', 'Auto Loan Providers',
       'Badminton Coach', 'Bagels', 'Balloon Decorations', 'Bankruptcy Law', 'Barbecue and Grill Services',
       'Barre Classes', 'Baseball Coach', 'Basketball Coach', 'Bass Guitar Lessons', 'Behavior Analysts',
       'Bicycle Repairers', 'Bike Repair and Maintenance', 'Bike tours', 'Bill and Account Collectors',
       'Billing Services', 'Biological Science Teachers', 'Bird Shops', 'Bookbinding', 'Bookkeepers',
       'Books, Mags, Music and Video Store', 'Boot Camps', 'Botanical Gardens', 'Boxing Trainer', 'Bridal Shop',
       'Bridal Stylist', 'Broadcast News Analysts', 'Bubble Tea', 'Budget Analysts', 'Bus Tours', 'Business Law',
       'Business Teachers', 'Butcher', 'Calligraphy', 'Camera Operators, Television, Video, and Motion Picture',
       'Candy Buffet Services', 'Capoeira Instructor', 'Cardio Classes', 'Cards and Stationery Store',
       'Career Counseling', 'Caricaturing', 'Cat Grooming', 'Cello Lessons', 'Cheese Shops', 'Cheese Tasting Classes',
       'Chefs and Cooks', 'Chemistry Teachers', 'Chess Lessons', 'Child, Family, and School Social Workers',
       'Chimney Cakes', 'Civil Engineer', 'Clinical, Counseling, and School Psychologists', 'Clock Repair', 'Clowns',
       'Coffee Roasteries', 'College Admissions Counseling', 'Comic Book Store', 'Commissioned Artists',
       'Communications Teachers', 'Compensation and Benefits Managers',
       'Compensation, Benefits, and Job Analysis Specialists', 'Compliance Officers',
       'Computer and Information Research Scientists', 'Computer Hardware Engineers', 'Computer Operators',
       'Computer Science Teachers', 'Computer Store', 'Computer Systems Analysts', 'Computer User Support Specialists',
       'Concierges', 'Consumer Law', 'Contracts Attorney', 'Cost Estimators', 'Costume Store',
       'Counseling and Mental Health', 'CPR Classes', 'Credit Analysts', 'Credit Counselors', 'Cremation Services',
       'Criminal Defense Attorney', 'Custom Airbrushing', 'Cycling Classes', 'Dance Entertainment', 'Day Camps',
       'Desktop Publishers', 'Detectives and Criminal Investigators', 'Disability Attorney', 'Disc Golf Coach',
       'Dishwashers', 'Divorce and Family Attorney', 'Do It Yourself Food', 'Dog Grooming', 'Dog Training', 'Donuts',
       'Drawing Lessons', 'Drum Lessons', 'Dry Cleaning', 'DUI Attorney', 'Eatertainment', 'Economics Teachers',
       'Editors', 'Education Teachers', 'Elder Law', 'Electrical Engineer', 'Electronics Engineers, Except Computer',
       'Embalmers', 'Embroidery', 'Embroidery and Crochet Store', 'Empanadas', 'Employment Law', 'Employment Service',
       'Engineering and Technical Design', 'Engineering Teachers', 'English Language and Literature Teachers',
       'Engraving', 'Entertainment Law', 'Environmental Engineer', 'Environmental Science Teachers',
       'ESL, English as a Second Language Lessons', 'Estate Attorney',
       'Executive Secretaries and Executive Administrative Assistants', 'Exercise Equipment Repair',
       'Fabric and Apparel Patternmakers', 'Fabric Menders, Except Garment', 'Facial Treatments', 'Family Counseling',
       'Family Law Attorney', 'Fashion Designers', 'Fashion Retailer', 'Feng Shui', 'Film and Video Editors',
       'Film and Video Production', 'Financial Analysts', 'First Aid Classes',
       'First Line Supervisors of Food Preparation and Serving Workers',
       'First Line Supervisors of Office and Administrative Support Workers', 'Fitness Equipment Assembly', 'Float Spa',
       'Floral Designers', 'Flowers and Gifts Store', 'Flute Lessons', 'Food Cart Operator', 'Food Preparation Workers',
       'Food Servers, Nonrestaurant', 'Food Service Managers', 'Food Tours', 'Framing Store', 'French Lessons',
       'Fruits and Veggies', 'Games and Concession Rental', 'Gelato', 'Gemstones and Minerals Shop',
       'General and Operations Managers', 'General Litigation', 'Geography Teachers', 'German Lessons', 'Gift Shop',
       'Gold Buyers', 'Golf Equipment Retailer', 'Graduate Teaching Assistants', 'Graphic Design Instruction',
       'Grill Services', 'Grilling Equipment Store', 'Guitar Lessons', 'Guitar Stores', 'Hair Removal', 'Halotherapy',
       'Health and Safety Engineers, Except Mining Safety Engineers and Inspectors', 'Health Insurance Offices',
       'Health Specialties Teachers', 'Healthcare Social Workers', 'Henna Artists', 'Henna Tattooing', 'Herbal Shops',
       'High Fidelity Audio Equipment', 'Historical Tours', 'History Teachers', 'Hockey Equipment Retailer',
       'Holiday Decorating Services', 'Holiday Decorations Store', 'Holistic Animal Care', 'Home Decor Shop',
       'Home Economics Teachers', 'Home Staging', 'Hot Springs',
       'Human Resources Assistants, Except Payroll and Timekeeping', 'Human Resources Managers',
       'Hunting and Fishing Supplies Retailer', 'Hydroponics Store', 'Hypnotherapy', 'Ice Cream and Frozen Yogurt',
       'Illustrating', 'Immigration Attorney', 'Impersonating', 'Imported Food', 'Industrial Engineer',
       'Installment Loans', 'Instructional Coordinators', 'Instrument Instructor', 'Insurance Underwriters',
       'Intellectual Property Attorney', 'International Law Attorney', 'Investing', 'IP and Internet Law',
       'Italian Lessons', 'Japanese Lessons', 'Jazz and Blues', 'Jewelers and Precious Stone and Metal Workers',
       'Jewelry Repair', 'Karaoke Machine Rental', 'Karate Instructor', 'Kickboxing Trainer', 'Kitchen and Bath Store',
       'Kitchen Supplies Shop', 'Knitting Supplies Shop', 'Kombucha', 'Korean Lessons', 'Labor and Employment Attorney',
       'Labor Relations Specialists', 'Law Teachers', 'Legal Document Preparation', 'Legal Secretaries',
       'Library Science Teachers', 'Literacy Teachers and Instructors', 'Loan Interviewers and Clerks', 'Local Flavor',
       'Logo Design', 'Luggage Storage', 'Macarons', 'Magician', 'Makeup Artists, Theatrical and Performance',
       'Management Analysts', 'Mandarin Lessons', 'Manicurists and Pedicurists', 'Manufacturer Sales Representative',
       'Mass Media', 'Materials Engineers', 'Mathematical Science Teachers', 'Mattress Store', 'Mechanical Engineers',
       'Mediators', 'Medical Billing Agency', 'Medical Law', 'Medical Secretaries',
       'Merchandise Displayers and Window Trimmers', 'Mobile Design', 'Mobile Phone Accessories Retailer',
       'Mobile Phone Repair', 'Mobile Phone Retailer', 'Models', 'Mortgage Lenders',
       'Morticians, Undertakers, and Funeral Directors', 'Motion Picture Projectionists',
       'Multimedia Artists and Animators', 'Muralist', 'Music and DVDs Store', 'Music Directors and Composers',
       'Music Theory Lessons', 'Musicians and Singers', 'Mystics', 'Nanny Services', 'Naturopathic/Holistic',
       'Network and Computer Systems Administrators', 'Network Support Services', 'Nursing Instructors and Teachers',
       'Nutritionists', 'Office Clerks, General', 'Office Equipment Retailer', 'Officiants', 'Olive Oil',
       'Online Auctioneer', 'Opera and Ballet', 'Organic Stores', 'Outdoor Gear Store', 'Outlet Store', 'Oxygen Bars',
       'Package Delivery', 'Painting Lessons', 'Palm Reading', 'Parenting Classes', 'Party Characters',
       'Party Supplies', 'Pasta Shops', 'Patent Law', 'Perfume', 'Personal Bankruptcy Attorney',
       'Personal Injury Attorney', 'Pet Insurance Agent', 'Pet Services', 'Pet Sitting',
       'Philosophy and Religion Teachers', 'Photo Booth Rentals', 'Photography Lessons', 'Photography Store',
       'Physics Teachers', 'Piadina', 'Piano Lessons', 'Piano Services', 'Piano Stores', 'Piano Tuning',
       'Pickleball Coach', 'Poke', 'Political Science Teachers', 'Pool and Billiards Store',
       'Pool Table Repair Services', 'Pop up Shop', 'Portrait Artistry', 'Portuguese Lessons',
       'Prepress Technicians and Workers', 'Preschools', 'Presentation Design', 'Pretzels', 'Private Investigation',
       'Process Engineer', 'Producers and Directors', 'Production, Planning, and Expediting Clerks',
       'Project Management', 'Props Store', 'Psychic Mediums', 'Psychics', 'Psychology Teachers', 'Public Adjusters',
       'Public Markets', 'Public Speaking Lessons', 'Purchasing Managers', 'Qi Gong', 'Quilting and Crochet',
       'Radio Stations', 'Real Estate Attorney', 'Receptionists and Information Clerks', 'Recreational Therapists',
       'Recruiting', 'Reflexology', 'Reiki Lessons', 'Religious Items Retailer', 'Reptile Shops', 'Restaurant Supplies',
       'Retail Salespersons', 'Rug Store', 'Safe Store', 'Salon Owner', 'Saxophone Lessons', 'Scooter Tours',
       'Scrapbooking', 'Self Enrichment Education Teachers', 'Self-defense Instructor', 'Sewing and Alterations',
       'Sewing Lessons', 'Sewing Machine Operators', 'Sex Therapists', 'Shampooers', 'Shaved Ice', 'Shaved Snow',
       'Shoe Shine', 'Sign Language Lessons', 'Singing Lessons', 'Singing Telegram', 'Skate Shop',
       'Ski and Snowboard Shop', 'Skin Care', 'Skincare Specialists', 'Smokehouse', 'Soccer Coach',
       'Social Media Marketing', 'Social Security Law', 'Social Work Teachers', 'Sociology Teachers', 'Softball Coach',
       'Software Developers', 'Software Sales Engineer', 'Songwriting', 'Sound Engineering Technicians',
       'Souvenir Shop', 'Spanish Lessons', 'Speech Therapists', 'Spiritual Shop', 'Squash Coach',
       'Statistical Data Analysis', 'Street Vendors', 'Substance Abuse Counselor', 'Substitute Teachers', 'Sugaring',
       'Sunglasses Shop', 'Supernatural Readings', 'Surveying and Mapping Technicians',
       'Switchboard Operators, Including Answering Service', 'Tabletop Games Store', 'Tableware Store',
       'Taekwondo Instructor', 'Tai Chi Instructor', 'Talent Agencies', 'Tasting Classes', 'Tax Attorney',
       'Tax Preparers', 'Tax Services', 'Tea Rooms', 'Teacher Assistants', 'Teacher Supplies Store',
       'Technical Support', 'Telephone Operators', 'Television Stations', 'Temporary Tattoo Artistry',
       'Tenant and Eviction Law', 'Tennis Coach', 'Test Prep Services', 'Therapy or Mental Health Services',
       'Threading Services', 'Thrift Store', 'Tour Guides and Escorts', 'Traffic Law Attorney',
       'Transportation Engineer', 'Transportation Security Screeners', 'Trophy Shop', 'Tui Na',
       'Undersea/Hyperbaric Medicine', 'Uniform Store', 'Used Bookstore', 'Vacation Rental Agents',
       'Ventriloquist and Puppet Entertainment', 'Veterinary Assistants and Laboratory Animal Caretakers',
       'Veterinary Technologists and Technicians', 'Video Booth Rental', 'Video Editing', 'Video Game Store',
       'Video Production', 'Video Streaming and Webcasting Services', 'Videos and Video Game Rental Store',
       'Vinyl Record Store', 'Violin Lessons', 'Voice Over Lessons', 'Volleyball Coach', 'Waiters and Waitresses',
       'Walking Tours', 'Watch Repairers', 'Watches Retailer', 'Waxing', 'Web Developers', 'Web Hosting',
       'Web Site Designer', 'Wedding and Event Makeup', 'Wedding Cakes', 'Wedding Chapels', 'Wedding Coordination',
       'Wedding Planning', 'Wig Store', 'Wills and Estate Planning', 'Workers Compensation Law')
group by 1, 2
order by 1 asc

--unsupported leads by cob
select --eventtime::date,
       --json_extract_path_text(source_json_last_related_business_id_before_first_lead, 'channel',true) as channel,
       aa.cob_name,
       count(distinct related_business_id)
from dwh.all_activities_table aa
         join dwh.sources_attributed_table ss on aa.tracking_id = ss.tracking_id
         left join portfolio_svc_prod.permitted_cobs_states_lobs list
                   on list.cob_id = ss.cob_id and list.state_code = ss.state_code and aa.lob = list.lob
where date(eventtime) >= '2024-03-01'
  and funnelphase like '%Unsupported%'
--and aa.cob_name in ('3D Modeling','Acai Bowls','Acne Treatment','Acupressurist','Acupuncturist','Addiction Medicine','Adoption Services','Aestheticians','Alterations, Tailoring, and Clothing Design','Alternative Medicine','Amateur Sports Teams Coach','Animal Trainers','Animation','Anthropology and Archeology Teachers','Antiques Store','Apartment Agents','Appraisal Services','Arabic Lessons','Archery Coach','Architecture Teachers','Area, Ethnic, and Cultural Studies Teachers','Art Directors','Art Tours','Art, Drama, and Music Teachers','Astrologers','Athletic Trainer','Atmospheric, Earth, Marine, and Space Sciences Teachers','Audio Production Lessons','Audio Recording','Audio Services','Audio Visual and Multimedia Collections Specialists','Auto Loan Providers','Badminton Coach','Bagels','Balloon Decorations','Bankruptcy Law','Barbecue and Grill Services','Barre Classes','Baseball Coach','Basketball Coach','Bass Guitar Lessons','Behavior Analysts','Bicycle Repairers','Bike Repair and Maintenance','Bike tours','Bill and Account Collectors','Billing Services','Biological Science Teachers','Bird Shops','Bookbinding','Bookkeepers','Books, Mags, Music and Video Store','Boot Camps','Botanical Gardens','Boxing Trainer','Bridal Shop','Bridal Stylist','Broadcast News Analysts','Bubble Tea','Budget Analysts','Bus Tours','Business Law','Business Teachers','Butcher','Calligraphy','Camera Operators, Television, Video, and Motion Picture','Candy Buffet Services','Capoeira Instructor','Cardio Classes','Cards and Stationery Store','Career Counseling','Caricaturing','Cat Grooming','Cello Lessons','Cheese Shops','Cheese Tasting Classes','Chefs and Cooks','Chemistry Teachers','Chess Lessons','Child, Family, and School Social Workers','Chimney Cakes','Civil Engineer','Clinical, Counseling, and School Psychologists','Clock Repair','Clowns','Coffee Roasteries','College Admissions Counseling','Comic Book Store','Commissioned Artists','Communications Teachers','Compensation and Benefits Managers','Compensation, Benefits, and Job Analysis Specialists','Compliance Officers','Computer and Information Research Scientists','Computer Hardware Engineers','Computer Operators','Computer Science Teachers','Computer Store','Computer Systems Analysts','Computer User Support Specialists','Concierges','Consumer Law','Contracts Attorney','Cost Estimators','Costume Store','Counseling and Mental Health','CPR Classes','Credit Analysts','Credit Counselors','Cremation Services','Criminal Defense Attorney','Custom Airbrushing','Cycling Classes','Dance Entertainment','Day Camps','Desktop Publishers','Detectives and Criminal Investigators','Disability Attorney','Disc Golf Coach','Dishwashers','Divorce and Family Attorney','Do It Yourself Food','Dog Grooming','Dog Training','Donuts','Drawing Lessons','Drum Lessons','Dry Cleaning','DUI Attorney','Eatertainment','Economics Teachers','Editors','Education Teachers','Elder Law','Electrical Engineer','Electronics Engineers, Except Computer','Embalmers','Embroidery','Embroidery and Crochet Store','Empanadas','Employment Law','Employment Service','Engineering and Technical Design','Engineering Teachers','English Language and Literature Teachers','Engraving','Entertainment Law','Environmental Engineer','Environmental Science Teachers','ESL, English as a Second Language Lessons','Estate Attorney','Executive Secretaries and Executive Administrative Assistants','Exercise Equipment Repair','Fabric and Apparel Patternmakers','Fabric Menders, Except Garment','Facial Treatments','Family Counseling','Family Law Attorney','Fashion Designers','Fashion Retailer','Feng Shui','Film and Video Editors','Film and Video Production','Financial Analysts','First Aid Classes','First Line Supervisors of Food Preparation and Serving Workers','First Line Supervisors of Office and Administrative Support Workers','Fitness Equipment Assembly','Float Spa','Floral Designers','Flowers and Gifts Store','Flute Lessons','Food Cart Operator','Food Preparation Workers','Food Servers, Nonrestaurant','Food Service Managers','Food Tours','Framing Store','French Lessons','Fruits and Veggies','Games and Concession Rental','Gelato','Gemstones and Minerals Shop','General and Operations Managers','General Litigation','Geography Teachers','German Lessons','Gift Shop','Gold Buyers','Golf Equipment Retailer','Graduate Teaching Assistants','Graphic Design Instruction','Grill Services','Grilling Equipment Store','Guitar Lessons','Guitar Stores','Hair Removal','Halotherapy','Health and Safety Engineers, Except Mining Safety Engineers and Inspectors','Health Insurance Offices','Health Specialties Teachers','Healthcare Social Workers','Henna Artists','Henna Tattooing','Herbal Shops','High Fidelity Audio Equipment','Historical Tours','History Teachers','Hockey Equipment Retailer','Holiday Decorating Services','Holiday Decorations Store','Holistic Animal Care','Home Decor Shop','Home Economics Teachers','Home Staging','Hot Springs','Human Resources Assistants, Except Payroll and Timekeeping','Human Resources Managers','Hunting and Fishing Supplies Retailer','Hydroponics Store','Hypnotherapy','Ice Cream and Frozen Yogurt','Illustrating','Immigration Attorney','Impersonating','Imported Food','Industrial Engineer','Installment Loans','Instructional Coordinators','Instrument Instructor','Insurance Underwriters','Intellectual Property Attorney','International Law Attorney','Investing','IP and Internet Law','Italian Lessons','Japanese Lessons','Jazz and Blues','Jewelers and Precious Stone and Metal Workers','Jewelry Repair','Karaoke Machine Rental','Karate Instructor','Kickboxing Trainer','Kitchen and Bath Store','Kitchen Supplies Shop','Knitting Supplies Shop','Kombucha','Korean Lessons','Labor and Employment Attorney','Labor Relations Specialists','Law Teachers','Legal Document Preparation','Legal Secretaries','Library Science Teachers','Literacy Teachers and Instructors','Loan Interviewers and Clerks','Local Flavor','Logo Design','Luggage Storage','Macarons','Magician','Makeup Artists, Theatrical and Performance','Management Analysts','Mandarin Lessons','Manicurists and Pedicurists','Manufacturer Sales Representative','Mass Media','Materials Engineers','Mathematical Science Teachers','Mattress Store','Mechanical Engineers','Mediators','Medical Billing Agency','Medical Law','Medical Secretaries','Merchandise Displayers and Window Trimmers','Mobile Design','Mobile Phone Accessories Retailer','Mobile Phone Repair','Mobile Phone Retailer','Models','Mortgage Lenders','Morticians, Undertakers, and Funeral Directors','Motion Picture Projectionists','Multimedia Artists and Animators','Muralist','Music and DVDs Store','Music Directors and Composers','Music Theory Lessons','Musicians and Singers','Mystics','Nanny Services','Naturopathic/Holistic','Network and Computer Systems Administrators','Network Support Services','Nursing Instructors and Teachers','Nutritionists','Office Clerks, General','Office Equipment Retailer','Officiants','Olive Oil','Online Auctioneer','Opera and Ballet','Organic Stores','Outdoor Gear Store','Outlet Store','Oxygen Bars','Package Delivery','Painting Lessons','Palm Reading','Parenting Classes','Party Characters','Party Supplies','Pasta Shops','Patent Law','Perfume','Personal Bankruptcy Attorney','Personal Injury Attorney','Pet Insurance Agent','Pet Services','Pet Sitting','Philosophy and Religion Teachers','Photo Booth Rentals','Photography Lessons','Photography Store','Physics Teachers','Piadina','Piano Lessons','Piano Services','Piano Stores','Piano Tuning','Pickleball Coach','Poke','Political Science Teachers','Pool and Billiards Store','Pool Table Repair Services','Pop up Shop','Portrait Artistry','Portuguese Lessons','Prepress Technicians and Workers','Preschools','Presentation Design','Pretzels','Private Investigation','Process Engineer','Producers and Directors','Production, Planning, and Expediting Clerks','Project Management','Props Store','Psychic Mediums','Psychics','Psychology Teachers','Public Adjusters','Public Markets','Public Speaking Lessons','Purchasing Managers','Qi Gong','Quilting and Crochet','Radio Stations','Real Estate Attorney','Receptionists and Information Clerks','Recreational Therapists','Recruiting','Reflexology','Reiki Lessons','Religious Items Retailer','Reptile Shops','Restaurant Supplies','Retail Salespersons','Rug Store','Safe Store','Salon Owner','Saxophone Lessons','Scooter Tours','Scrapbooking','Self Enrichment Education Teachers','Self-defense Instructor','Sewing and Alterations','Sewing Lessons','Sewing Machine Operators','Sex Therapists','Shampooers','Shaved Ice','Shaved Snow','Shoe Shine','Sign Language Lessons','Singing Lessons','Singing Telegram','Skate Shop','Ski and Snowboard Shop','Skin Care','Skincare Specialists','Smokehouse','Soccer Coach','Social Media Marketing','Social Security Law','Social Work Teachers','Sociology Teachers','Softball Coach','Software Developers','Software Sales Engineer','Songwriting','Sound Engineering Technicians','Souvenir Shop','Spanish Lessons','Speech Therapists','Spiritual Shop','Squash Coach','Statistical Data Analysis','Street Vendors','Substance Abuse Counselor','Substitute Teachers','Sugaring','Sunglasses Shop','Supernatural Readings','Surveying and Mapping Technicians','Switchboard Operators, Including Answering Service','Tabletop Games Store','Tableware Store','Taekwondo Instructor','Tai Chi Instructor','Talent Agencies','Tasting Classes','Tax Attorney','Tax Preparers','Tax Services','Tea Rooms','Teacher Assistants','Teacher Supplies Store','Technical Support','Telephone Operators','Television Stations','Temporary Tattoo Artistry','Tenant and Eviction Law','Tennis Coach','Test Prep Services','Therapy or Mental Health Services','Threading Services','Thrift Store','Tour Guides and Escorts','Traffic Law Attorney','Transportation Engineer','Transportation Security Screeners','Trophy Shop','Tui Na','Undersea/Hyperbaric Medicine','Uniform Store','Used Bookstore','Vacation Rental Agents','Ventriloquist and Puppet Entertainment','Veterinary Assistants and Laboratory Animal Caretakers','Veterinary Technologists and Technicians','Video Booth Rental','Video Editing','Video Game Store','Video Production','Video Streaming and Webcasting Services','Videos and Video Game Rental Store','Vinyl Record Store','Violin Lessons','Voice Over Lessons','Volleyball Coach','Waiters and Waitresses','Walking Tours','Watch Repairers','Watches Retailer','Waxing','Web Developers','Web Hosting','Web Site Designer','Wedding and Event Makeup','Wedding Cakes','Wedding Chapels','Wedding Coordination','Wedding Planning','Wig Store','Wills and Estate Planning','Workers Compensation Law')
group by 1
order by 2 desc

select --eventtime::date,
       eventtime,
       --json_extract_path_text(source_json_last_related_business_id_before_first_lead, 'channel',true) as channel,
       count(distinct related_business_id)
from dwh.all_activities_table aa
         join dwh.sources_attributed_table ss on aa.tracking_id = ss.tracking_id
         left join portfolio_svc_prod.permitted_cobs_states_lobs list
                   on list.cob_id = ss.cob_id and list.state_code = ss.state_code and aa.lob = list.lob
where date(eventtime) >= '2024-05-01'
  and funnelphase like '%Unsupported%'
  and aa.cob_name in
      ('3D Modeling', 'Acai Bowls', 'Acne Treatment', 'Acupressurist', 'Acupuncturist', 'Addiction Medicine',
       'Adoption Services', 'Aestheticians', 'Alterations, Tailoring, and Clothing Design', 'Alternative Medicine',
       'Amateur Sports Teams Coach', 'Animal Trainers', 'Animation', 'Anthropology and Archeology Teachers',
       'Antiques Store', 'Apartment Agents', 'Appraisal Services', 'Arabic Lessons', 'Archery Coach',
       'Architecture Teachers', 'Area, Ethnic, and Cultural Studies Teachers', 'Art Directors', 'Art Tours',
       'Art, Drama, and Music Teachers', 'Astrologers', 'Athletic Trainer',
       'Atmospheric, Earth, Marine, and Space Sciences Teachers', 'Audio Production Lessons', 'Audio Recording',
       'Audio Services', 'Audio Visual and Multimedia Collections Specialists', 'Auto Loan Providers',
       'Badminton Coach', 'Bagels', 'Balloon Decorations', 'Bankruptcy Law', 'Barbecue and Grill Services',
       'Barre Classes', 'Baseball Coach', 'Basketball Coach', 'Bass Guitar Lessons', 'Behavior Analysts',
       'Bicycle Repairers', 'Bike Repair and Maintenance', 'Bike tours', 'Bill and Account Collectors',
       'Billing Services', 'Biological Science Teachers', 'Bird Shops', 'Bookbinding', 'Bookkeepers',
       'Books, Mags, Music and Video Store', 'Boot Camps', 'Botanical Gardens', 'Boxing Trainer', 'Bridal Shop',
       'Bridal Stylist', 'Broadcast News Analysts', 'Bubble Tea', 'Budget Analysts', 'Bus Tours', 'Business Law',
       'Business Teachers', 'Butcher', 'Calligraphy', 'Camera Operators, Television, Video, and Motion Picture',
       'Candy Buffet Services', 'Capoeira Instructor', 'Cardio Classes', 'Cards and Stationery Store',
       'Career Counseling', 'Caricaturing', 'Cat Grooming', 'Cello Lessons', 'Cheese Shops', 'Cheese Tasting Classes',
       'Chefs and Cooks', 'Chemistry Teachers', 'Chess Lessons', 'Child, Family, and School Social Workers',
       'Chimney Cakes', 'Civil Engineer', 'Clinical, Counseling, and School Psychologists', 'Clock Repair', 'Clowns',
       'Coffee Roasteries', 'College Admissions Counseling', 'Comic Book Store', 'Commissioned Artists',
       'Communications Teachers', 'Compensation and Benefits Managers',
       'Compensation, Benefits, and Job Analysis Specialists', 'Compliance Officers',
       'Computer and Information Research Scientists', 'Computer Hardware Engineers', 'Computer Operators',
       'Computer Science Teachers', 'Computer Store', 'Computer Systems Analysts', 'Computer User Support Specialists',
       'Concierges', 'Consumer Law', 'Contracts Attorney', 'Cost Estimators', 'Costume Store',
       'Counseling and Mental Health', 'CPR Classes', 'Credit Analysts', 'Credit Counselors', 'Cremation Services',
       'Criminal Defense Attorney', 'Custom Airbrushing', 'Cycling Classes', 'Dance Entertainment', 'Day Camps',
       'Desktop Publishers', 'Detectives and Criminal Investigators', 'Disability Attorney', 'Disc Golf Coach',
       'Dishwashers', 'Divorce and Family Attorney', 'Do It Yourself Food', 'Dog Grooming', 'Dog Training', 'Donuts',
       'Drawing Lessons', 'Drum Lessons', 'Dry Cleaning', 'DUI Attorney', 'Eatertainment', 'Economics Teachers',
       'Editors', 'Education Teachers', 'Elder Law', 'Electrical Engineer', 'Electronics Engineers, Except Computer',
       'Embalmers', 'Embroidery', 'Embroidery and Crochet Store', 'Empanadas', 'Employment Law', 'Employment Service',
       'Engineering and Technical Design', 'Engineering Teachers', 'English Language and Literature Teachers',
       'Engraving', 'Entertainment Law', 'Environmental Engineer', 'Environmental Science Teachers',
       'ESL, English as a Second Language Lessons', 'Estate Attorney',
       'Executive Secretaries and Executive Administrative Assistants', 'Exercise Equipment Repair',
       'Fabric and Apparel Patternmakers', 'Fabric Menders, Except Garment', 'Facial Treatments', 'Family Counseling',
       'Family Law Attorney', 'Fashion Designers', 'Fashion Retailer', 'Feng Shui', 'Film and Video Editors',
       'Film and Video Production', 'Financial Analysts', 'First Aid Classes',
       'First Line Supervisors of Food Preparation and Serving Workers',
       'First Line Supervisors of Office and Administrative Support Workers', 'Fitness Equipment Assembly', 'Float Spa',
       'Floral Designers', 'Flowers and Gifts Store', 'Flute Lessons', 'Food Cart Operator', 'Food Preparation Workers',
       'Food Servers, Nonrestaurant', 'Food Service Managers', 'Food Tours', 'Framing Store', 'French Lessons',
       'Fruits and Veggies', 'Games and Concession Rental', 'Gelato', 'Gemstones and Minerals Shop',
       'General and Operations Managers', 'General Litigation', 'Geography Teachers', 'German Lessons', 'Gift Shop',
       'Gold Buyers', 'Golf Equipment Retailer', 'Graduate Teaching Assistants', 'Graphic Design Instruction',
       'Grill Services', 'Grilling Equipment Store', 'Guitar Lessons', 'Guitar Stores', 'Hair Removal', 'Halotherapy',
       'Health and Safety Engineers, Except Mining Safety Engineers and Inspectors', 'Health Insurance Offices',
       'Health Specialties Teachers', 'Healthcare Social Workers', 'Henna Artists', 'Henna Tattooing', 'Herbal Shops',
       'High Fidelity Audio Equipment', 'Historical Tours', 'History Teachers', 'Hockey Equipment Retailer',
       'Holiday Decorating Services', 'Holiday Decorations Store', 'Holistic Animal Care', 'Home Decor Shop',
       'Home Economics Teachers', 'Home Staging', 'Hot Springs',
       'Human Resources Assistants, Except Payroll and Timekeeping', 'Human Resources Managers',
       'Hunting and Fishing Supplies Retailer', 'Hydroponics Store', 'Hypnotherapy', 'Ice Cream and Frozen Yogurt',
       'Illustrating', 'Immigration Attorney', 'Impersonating', 'Imported Food', 'Industrial Engineer',
       'Installment Loans', 'Instructional Coordinators', 'Instrument Instructor', 'Insurance Underwriters',
       'Intellectual Property Attorney', 'International Law Attorney', 'Investing', 'IP and Internet Law',
       'Italian Lessons', 'Japanese Lessons', 'Jazz and Blues', 'Jewelers and Precious Stone and Metal Workers',
       'Jewelry Repair', 'Karaoke Machine Rental', 'Karate Instructor', 'Kickboxing Trainer', 'Kitchen and Bath Store',
       'Kitchen Supplies Shop', 'Knitting Supplies Shop', 'Kombucha', 'Korean Lessons', 'Labor and Employment Attorney',
       'Labor Relations Specialists', 'Law Teachers', 'Legal Document Preparation', 'Legal Secretaries',
       'Library Science Teachers', 'Literacy Teachers and Instructors', 'Loan Interviewers and Clerks', 'Local Flavor',
       'Logo Design', 'Luggage Storage', 'Macarons', 'Magician', 'Makeup Artists, Theatrical and Performance',
       'Management Analysts', 'Mandarin Lessons', 'Manicurists and Pedicurists', 'Manufacturer Sales Representative',
       'Mass Media', 'Materials Engineers', 'Mathematical Science Teachers', 'Mattress Store', 'Mechanical Engineers',
       'Mediators', 'Medical Billing Agency', 'Medical Law', 'Medical Secretaries',
       'Merchandise Displayers and Window Trimmers', 'Mobile Design', 'Mobile Phone Accessories Retailer',
       'Mobile Phone Repair', 'Mobile Phone Retailer', 'Models', 'Mortgage Lenders',
       'Morticians, Undertakers, and Funeral Directors', 'Motion Picture Projectionists',
       'Multimedia Artists and Animators', 'Muralist', 'Music and DVDs Store', 'Music Directors and Composers',
       'Music Theory Lessons', 'Musicians and Singers', 'Mystics', 'Nanny Services', 'Naturopathic/Holistic',
       'Network and Computer Systems Administrators', 'Network Support Services', 'Nursing Instructors and Teachers',
       'Nutritionists', 'Office Clerks, General', 'Office Equipment Retailer', 'Officiants', 'Olive Oil',
       'Online Auctioneer', 'Opera and Ballet', 'Organic Stores', 'Outdoor Gear Store', 'Outlet Store', 'Oxygen Bars',
       'Package Delivery', 'Painting Lessons', 'Palm Reading', 'Parenting Classes', 'Party Characters',
       'Party Supplies', 'Pasta Shops', 'Patent Law', 'Perfume', 'Personal Bankruptcy Attorney',
       'Personal Injury Attorney', 'Pet Insurance Agent', 'Pet Services', 'Pet Sitting',
       'Philosophy and Religion Teachers', 'Photo Booth Rentals', 'Photography Lessons', 'Photography Store',
       'Physics Teachers', 'Piadina', 'Piano Lessons', 'Piano Services', 'Piano Stores', 'Piano Tuning',
       'Pickleball Coach', 'Poke', 'Political Science Teachers', 'Pool and Billiards Store',
       'Pool Table Repair Services', 'Pop up Shop', 'Portrait Artistry', 'Portuguese Lessons',
       'Prepress Technicians and Workers', 'Preschools', 'Presentation Design', 'Pretzels', 'Private Investigation',
       'Process Engineer', 'Producers and Directors', 'Production, Planning, and Expediting Clerks',
       'Project Management', 'Props Store', 'Psychic Mediums', 'Psychics', 'Psychology Teachers', 'Public Adjusters',
       'Public Markets', 'Public Speaking Lessons', 'Purchasing Managers', 'Qi Gong', 'Quilting and Crochet',
       'Radio Stations', 'Real Estate Attorney', 'Receptionists and Information Clerks', 'Recreational Therapists',
       'Recruiting', 'Reflexology', 'Reiki Lessons', 'Religious Items Retailer', 'Reptile Shops', 'Restaurant Supplies',
       'Retail Salespersons', 'Rug Store', 'Safe Store', 'Salon Owner', 'Saxophone Lessons', 'Scooter Tours',
       'Scrapbooking', 'Self Enrichment Education Teachers', 'Self-defense Instructor', 'Sewing and Alterations',
       'Sewing Lessons', 'Sewing Machine Operators', 'Sex Therapists', 'Shampooers', 'Shaved Ice', 'Shaved Snow',
       'Shoe Shine', 'Sign Language Lessons', 'Singing Lessons', 'Singing Telegram', 'Skate Shop',
       'Ski and Snowboard Shop', 'Skin Care', 'Skincare Specialists', 'Smokehouse', 'Soccer Coach',
       'Social Media Marketing', 'Social Security Law', 'Social Work Teachers', 'Sociology Teachers', 'Softball Coach',
       'Software Developers', 'Software Sales Engineer', 'Songwriting', 'Sound Engineering Technicians',
       'Souvenir Shop', 'Spanish Lessons', 'Speech Therapists', 'Spiritual Shop', 'Squash Coach',
       'Statistical Data Analysis', 'Street Vendors', 'Substance Abuse Counselor', 'Substitute Teachers', 'Sugaring',
       'Sunglasses Shop', 'Supernatural Readings', 'Surveying and Mapping Technicians',
       'Switchboard Operators, Including Answering Service', 'Tabletop Games Store', 'Tableware Store',
       'Taekwondo Instructor', 'Tai Chi Instructor', 'Talent Agencies', 'Tasting Classes', 'Tax Attorney',
       'Tax Preparers', 'Tax Services', 'Tea Rooms', 'Teacher Assistants', 'Teacher Supplies Store',
       'Technical Support', 'Telephone Operators', 'Television Stations', 'Temporary Tattoo Artistry',
       'Tenant and Eviction Law', 'Tennis Coach', 'Test Prep Services', 'Therapy or Mental Health Services',
       'Threading Services', 'Thrift Store', 'Tour Guides and Escorts', 'Traffic Law Attorney',
       'Transportation Engineer', 'Transportation Security Screeners', 'Trophy Shop', 'Tui Na',
       'Undersea/Hyperbaric Medicine', 'Uniform Store', 'Used Bookstore', 'Vacation Rental Agents',
       'Ventriloquist and Puppet Entertainment', 'Veterinary Assistants and Laboratory Animal Caretakers',
       'Veterinary Technologists and Technicians', 'Video Booth Rental', 'Video Editing', 'Video Game Store',
       'Video Production', 'Video Streaming and Webcasting Services', 'Videos and Video Game Rental Store',
       'Vinyl Record Store', 'Violin Lessons', 'Voice Over Lessons', 'Volleyball Coach', 'Waiters and Waitresses',
       'Walking Tours', 'Watch Repairers', 'Watches Retailer', 'Waxing', 'Web Developers', 'Web Hosting',
       'Web Site Designer', 'Wedding and Event Makeup', 'Wedding Cakes', 'Wedding Chapels', 'Wedding Coordination',
       'Wedding Planning', 'Wig Store', 'Wills and Estate Planning', 'Workers Compensation Law')
group by 1--,2
order by 1 asc

--permitted WC NC research (DELETE)
WITH appetite_universe AS (SELECT cobs.cob_id,
                                  cobs."name"         cob_desc,
                                  upper(replace(s.name,
                                                ' ',
                                                '_')) state,
                                  s.code              state_code,
                                  lobs.code           lob,
                                  CASE
                                      WHEN EXISTS ( -- If it's present in the `permitted` table, it's still in use and therefore not deprecated
                                          SELECT 'x'
                                          FROM silver_portfolio.permitted p
                                          WHERE cobs.cob_id = p.cobid) THEN
                                          'ACTIVE'
                                      ELSE
                                          'DEPRECATED'
                                      END             cob_deprec_status
                           FROM nimi_svc_prod.cobs cobs
                                    CROSS JOIN nimi_svc_prod.states s
                                    CROSS JOIN nimi_svc_prod.policy_types lobs
                           WHERE cobs."name" NOT LIKE '%deprecated%' -- extra cleanup
                             AND lobs.code in ('GL', 'WC', 'IM', 'PL')),
     permitted AS (
         -- Create a distinct view of our current posture from portfolio at the same COB/STATE/LOB Granularity.
         SELECT *
         FROM (SELECT cobid                                                                    AS cob_id,
                      lob,
                      state,
                      channel,
                      action,
                      row_number() OVER (PARTITION BY cobid,
                          state,
                          lob,
                          channel ORDER BY event_occurrence_time_pst_timestamp_formatted DESC) AS ranking
               FROM silver_portfolio.permitted p
               WHERE flowtype = 'PURCHASE'
                 AND channel = 'direct' -- direct marketing concerned about direct channel flows
              ) t
         WHERE ranking = 1)
SELECT appetite_universe.cob_id,
       appetite_universe.cob_desc,
       appetite_universe.lob,
       appetite_universe.state_code,
       permitted.channel,
       CASE
           WHEN appetite_universe.cob_deprec_status = 'DEPRECATED' THEN
               'DEPRECATED'
           WHEN permitted.action = 'OPEN' THEN
               'OPEN'
           ELSE
               'CLOSED' END current_status -- Key status determination here
FROM appetite_universe
         LEFT JOIN permitted ON permitted.cob_id = appetite_universe.cob_id
    AND permitted.state = appetite_universe.state
    AND permitted.lob = appetite_universe.lob
WHERE current_status = 'OPEN'
  and state_code = 'AZ'
ORDER BY appetite_universe.cob_desc,
         appetite_universe.lob,
         appetite_universe.state_code;

--WC bellhops
select business_id
from dwh.quotes_policies_mlob
where cob like '%Bellhop%'
  and highest_policy_status = 4
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')

--insurance agents
select json_extract_path_text(json_args, 'lob_app_json', 'pl_aop_activities_property_casualty_insurance',
                              true)                                                                       as property_casualty,
       json_extract_path_text(json_args, 'lob_app_json', 'pl_aop_activities_life_health_insurance',
                              true)                                                                       as life_and_health,
       json_extract_path_text(json_args, 'lob_app_json', 'pl_aop_activities_financial_products',
                              true)                                                                       as financial_products,
       json_extract_path_text(json_args, 'lob_app_json',
                              'pl_aop_insurance_agents_unlisted_other_commercial_lines_alloc',
                              true)                                                                       as commercial_pct,
       extract(year from creation_time) || '-' ||
       right('00' + convert(varchar, extract(month from creation_time)), 2)                               as creation_year_month,
       count(distinct business_id),
       sum(highest_yearly_premium)
from dwh.quotes_policies_mlob
where highest_policy_status >= 4
  and lob_policy = 'PL'
  and creation_time >= '2022-01-01'
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and cob = 'Insurance Agent'
group by 1, 2, 3, 4, 5
order by 5 asc

--allstate bookroll declines
select *
from screening_data
where business_id in ("VBOuhdnsL5SgZirN",
                      "QYIaq9egjFGUArgk",
                      "tLyJV9PNvHDaLaLs",
                      "imr19fIbU68W7sPO",
                      "U3bvpwuTGdwy36E4",
                      "vfZfihdx9WIAnvCX",
                      "WOBvHQoORdLHUPXw",
                      "1OPzs1qphG3LXBFs",
                      "FQPTpfp0AoNhvLT8",
                      "QU3EFdSg4aMEYQUx",
                      "iYH2n2WPRv7m4Sop",
                      "1FTswLyNDn1FWKeX",
                      "tii6vSXwPaXTG0z6",
                      "FyU6SInqEsyAs1qf",
                      "fR34Gx567xkfxkix",
                      "ApETvQrFedMZcJqf",
                      "X3Tzrngvp8LkL8CV",
                      "xSrqM9xD0XlvLgdd",
                      "C8TjLESUhZ5m83MH",
                      "wBr3JZznqhhJHSzD",
                      "8KdPWP35g1zHQ4GA",
                      "bS6shddeYrF8W99n",
                      "w6xBiKDQ8Q8bBY1w",
                      "tZeyqNBqbLFpNEYR")
  and step_status = "DECLINE";

--top retail agents, top agents
select cob,
       agency_name,
       current_agencytype,
       territory_manager,
       sum(yearly_premium)           as total_premium,
       count(distinct (business_id)) as total_businesses
from dwh.agents_policies_ds
where start_date >= '2023-01-01'
  and
  --start_date < '2023-01-01' and
    policy_status >= 3
  and (bundle_lobs like '%CP%' or bundle_lobs like '%GL%')
  and cob in ('E-Commerce', 'Retail Stores', 'Grocery Store', 'Clothing Store', 'Electronics Store', 'Florist',
              'Jewelry Store', 'Sporting Goods Retailer', 'Tailors, Dressmakers, and Custom Sewers',
              'Nurseries and Gardening Shop', 'Candle Store', 'Pet Stores', 'Paint Stores', 'Flea Markets',
              'Arts and Crafts Store', 'Eyewear and Optician Store', 'Hardware Store', 'Discount Store', 'Pawn Shop',
              'Hobby Shop', 'Beach Equipment Rentals', 'Furniture Rental', 'Packing Supplies Store',
              'Horse Equipment Shop', 'Demonstrators and Product Promoters', 'Fabric Store', 'Lighting Store',
              'Luggage Store', 'Bike Rentals', 'Bike Shop', 'Bookstore', 'Home and Garden Retailer',
              'Newspaper and Magazine Store', 'Department Stores', 'Furniture Store', 'Wholesalers')
group by 1, 2, 3, 4
order by 5 desc

--top e-commmerce agents, top agents
select cob,
       bundle_lobs,
       agency_name,
       current_agencytype,
       territory_manager,
       sum(yearly_premium)           as total_premium,
       count(distinct (business_id)) as total_businesses
from dwh.agents_policies_ds
where start_date >= '2024-01-01'
  and policy_status >= 3
  and
  --(bundle_lobs like '%CP%' or bundle_lobs like '%GL%') and
    cob in ('E-Commerce')
group by 1, 2, 3, 4, 5
order by 6 desc

--risk screening AAL from RMS winter storm, wildfire, severe convective storm, hurricane
select distinct business_id, ws_gross_loss, wf_gross_loss, cs_gross_loss, wt_gross_loss
from riskmgmt_svc_prod.rms_property_gross_loss
where business_id in
      ('OzOJXD1wOO31ZYJo', '2bp0Awl2ch2rc09i', 'mmO6IhWrguC5OKah', 'ru3MGRWADPBlie8P', 'jTqwjm7a9QGFozH3',
       'ZtvHbQ2mCICNhAOE', '24x8DNdCeoVYuNzP', 'RC9AS3wWWIoBLKrz', 'YJBKm7qVpfireiXZ', 'oiXLu6lyuhiWieUT',
       '3yaNj23qgAGicd9R', 'K8xSuaYMlaq58lhR', 'l7264Gc10eqrwmgF', 'IXZS7m7aUrHZRBsK', 'ErJIEHswwG8ENAJo');

--pull list of leads by COB by month
select extract(year from event_date) || '-' ||
       right('00' + convert(varchar, extract(month from event_date)), 2)              as event_year_month,
       cob_name,
       marketing_cob_group,
       json_extract_path_text(source_json_first_related_business_id, 'channel', true) as dist_channel,
       count(distinct business_id)                                                    as num_businesses
from dwh.daily_activities
where start_date >= '2023-01-01'
  and lead = 1
group by 1, 2, 3, 4
order by 1 asc

--retail qtp and decline rates by lob cob
select lob,
       --cob,
       count(distinct case when policy_status >= 4 then business_id else null end)           purchase,
       count(distinct business_id)                                                           quotes,
       count(distinct case when execution_status = 'DECLINE' then business_id else null end) declines
from (select a.lob,
             cob,
             policy_id,
             policy_status,
             business_id,
             execution_status
      from dwh.underwriting_quotes_data a
               left join underwriting_svc_prod.lob_applications b
                         on a.lob_application_id = b.lob_application_id
               left join dwh.sources_test_cobs c
                         on a.cob = c.cob_name
      where a.offer_creation_time >= '2024-05-01'
        --and a.offer_creation_time <= '2023-11-01'
        and c.marketing_cob_group = 'Retail'
        --and cob in ('Insurance Agent','Business Consulting','Photographer','IT Consulting or Programming','Accountant','Other Consulting','Marketing','Property Manager','Real Estate Agent','Home Inspectors','Salesperson','Engineer','Audio and Video Equipment Technicians','Real Estate Brokers','Videographers','Legal Service','Travel Agency','Computer Programmers','Architect','Interior Designer','Training and Development Specialists','Travel Guides','Graphic Designers','Claims Adjuster','Computer and Information Systems Managers','Writer','Administrative Services Managers')
        --and a.lob in ('GL')
        and a.offer_flow_type in ('APPLICATION')) inner_query
group by 1
order by 4 desc

--retail top declines by LOB
select uw.lob,
       decline_reasons,
       count(distinct (business_id)) as biz_count
from dwh.underwriting_quotes_data uw
         join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
where execution_status = 'DECLINE'
  and decline_reasons not like '%","%'
  and offer_creation_time >= '2024-01-01'
--and uw.lob in ('GL','CP')
  and cob in ('E-Commerce', 'Retail Stores', 'Grocery Store', 'Clothing Store', 'Electronics Store', 'Florist',
              'Jewelry Store', 'Sporting Goods Retailer', 'Tailors, Dressmakers, and Custom Sewers',
              'Nurseries and Gardening Shop', 'Candle Store', 'Pet Stores', 'Paint Stores', 'Flea Markets',
              'Arts and Crafts Store', 'Eyewear and Optician Store', 'Hardware Store', 'Discount Store', 'Pawn Shop',
              'Hobby Shop', 'Beach Equipment Rentals', 'Furniture Rental', 'Packing Supplies Store',
              'Horse Equipment Shop', 'Demonstrators and Product Promoters', 'Fabric Store', 'Lighting Store',
              'Luggage Store', 'Bike Rentals', 'Bike Shop', 'Bookstore', 'Home and Garden Retailer',
              'Newspaper and Magazine Store', 'Department Stores', 'Furniture Store', 'Wholesalers')
group by 1, 2
order by biz_count desc

--Verisk building replacement cost Verisk 360 building cost
SELECT vpvr.business_id, vpv.rebuild_cost
FROM riskmgmt_svc_prod.verisk_property_valuation vpv
         join riskmgmt_svc_prod.verisk_property_valuation_request vpvr on vpvr.id = vpv.id
where vpv.creation_time > '2024-05-01'
  and vpvr.business_id in
      ('R98ssyDivJRX5nO1', '42hUgqjpJ8ZToLkT', 'pGn51mJltNGafnHB', 'XLGca7vakcEnivc1', 'D1Zm16qmJC02GRZp',
       'sXYYKnDhKgAAgOta', 'FRMr7Ywfu0Be2Ogy', '8GogzyqX2vCS2X4f', 'AI5jcvK1VpFEh1ZZ', '5VdP4MbMLwJFr3os',
       'gXP5kGsphcAK9fLn', 'ZMFh8jM3q0KcOpAf', 'MiHQsec3kZeKf74f', 'yJTpNVz83m9duf0r', 'KyedYDk5nGQA0xbO',
       'Lk2cRQlsj9maluSg', '9OIVmZLvXPGZvON9', '95qywfTyXtJzVQ4j', '6QWH5FjWMAMwD8ma', 'xQXYFtm7Rvav5Tt1',
       'ArvMmjurtwJvug3j', 'ZIkn6Dhc699Q4lwx', 'JhJrMANlyptQ9GJE', 'nfjUKgRJAgLdEQkA', 'KIpQKvYpqnWTUlas',
       'Nfg1CwX4ibfMmVat', 'm6rGIIUVwGtIYKeh', 'W9zmnCIYT1nlNQtz', 'rumFsC2jc3nepxK9', 'oXb7I5d2BDT0zgt9',
       'UGsEBnm2fMdR34Oh', '2T0s0V6A1lvS3Ycw', 'BSSHmoLygGvxGg7E', 'geuuaU3xmSYrFwkq', 'Krm1MjLpkN8DUu9V',
       'zY3CdzWfgzOWU2a8', 'y8ThK5bhDhJqEbu2', '6rwo4lBqlf4htPKR', 'miHpSuw90hWNL2xq', 'zH6UoKLKzojLzFkM',
       'rBubi9TfNlpxGaff', 'OkJPS6oBv4Ur8tDK', 'nNODUm35UsgQBKaS', 'X68pn46anXZ0U1vf', 'QWhupIiiUDcskueq',
       'LH3zEwU0bL8OAFC5', '7BmweiVlVd6zsoJD', '0L7azkxcbxXNWskA', 'mmYU2I9RYG1JvCfO', '4v55u8SdSBbCalxw',
       'VwcaRdS1V69JYKAC', 'fgni6qc0PixuxLB4', 'IwpUjv7g2d2z5EqH', 'hPXV7VzKVG7INJ7q', 'iC1PqYWlFdp2A56K',
       'ovYyfLeCXoqVEzK6', 'yi6AQWWPbn8odZVi', 'CKZidhw0aEWGBJzU', '25rBe1BlBZFcvGbA', 'BobpZOuv2HMcwV6D',
       'gnkAHqGI2EUZ2Vex', 'nN2q123Gdo49pEwm', 'VDhgshcDetdBfHca', 'ro2ElFre7RPYlY0B', 'XzxSe91lFhGz85tS',
       'w3dI9fu88a0W9Yyf', 'Bl37CLwK9N2UagKv', 'zXrbIdjrDCbsaTAa', 'Un8gnzEjGsa3sODy', 'UoP2vBqNPt3PV7ae',
       'nrchL8MSNtBSWdHE', 'Qza8NnEyYkgS5H5x', 'I0B5ezW9scW59ysD', 'nY94audpm2pFvvYJ', 'Do8l6vVqt5U5TURG',
       'CvYIjn6MyI0nTIgD', '4azkhgfuJjdHG2ZN', '2El0ln4Id3rardwo', '4cL0oc6v6HI8GVAL', 'fLkVjuckXU2WOXbV',
       'rIQlmPm8aIw8j30t', 'qV619W4kGBQzveGL', '6W5t3YTmk8SfNeWi', 'cAvGbqYCWCFYLzJT', 'hLj5QcjbakdEY7Aw',
       'K297wRXjOwSiK3fq', 'C8L5kQHvqw4HksQ8', 'uSaf8lr7yzv9vovI', 'yE4VDxxsEQoZl8jX', 'AU60GIi9tpcIMwd0',
       'rcCOnwuqjUjBpHUv', 'bLjgo2da1nl6kbNQ', 'itDYtzTgSoS4gr9j', 'CylVFBOyFbUcgtrF', '7jWANkOGjWysg9iL',
       'cQK3bpycMvDy1HjJ', 'qwLiJzfHpbTK51S2', 'DyUGG88SpoHF7NgE', 'YHsVk6VTDMvEWpZN', 'REznDQBXhcqJCOdh',
       'bFH8NjgZsIk9qVXD', 'YT6jPfZkxocpBHIb', 'wHlvWPh0QJyd2TYC', 'u6cHaApd4KLRbToM', 'KENdou44YZP83EQD',
       'W7hzfaViZSHqFPT0', 'uaI5XaErAi34QOP4', '8ymBLlCiHqNlNIEU', 'MUWIoiXQwFiu5cUQ', 'hmEuYIwNxopNNxWb',
       'fVHXo7E3Ifu8DEdN', 'NZXVAlWWAHu9pGkc', 'C8ZTnyCxNbL2WlOi', 'MqUX8j3Y7L8C0WYK', 'CF2ADHomooBitSbm',
       'pnZyBbjJF7QznEWY', 'wj02K43ChCRtTDYA', 'ZtxHjkmde6gbzxSw', 'CKN8wCJ3600fV2ux', 'mccU3VYfeFwWGD3f',
       'O8FAJKsaCbT7uCXV', 'fYmCtqqdg1qxH86a', 'RJ1EXoTHbxDBUdJI', 'D5wC5Pmkoc2JbrHz', '9nUlQXWKZHrrxv2j',
       'sFTB9iLfghcEPUHp', 'xDpRSMRkxOE9ZMG2', 'o8SSEGgPYt4FSjAw', 'Kdp7IDAYVcNyoUv2', '4Kye6EMJmlZ3q8ij',
       'wBFOd3fLClOZGJ3f', 'AYi57CZJ47BrjYUS', 'VATPJeoW8mfXwsqH', 'ddYBBpP4XCCj1HrY', 'U4bobUZoL3aYhFIJ',
       'WNtZiEc570wwFVzy', 'znkbr5ku9sDy3nUZ', 'i8W2Xw8K2ZQYQ0zK', 'JDAtrCunKsn7GAoI', '2gImHXdCQZb8j59a',
       'SH6vI4ZQm0jcmQo1', 'yNCnSmMdGphpPFXE', 'xYdTAKCvrSsRAGWX', 'ShZ3PHnANsFQFXZ3', 'BlbPDIVn3IhIew4C',
       'mwtjzayelhJAfMRk', '9SXHoQfElVC0umzS', 'mKUMHbItOsYbtkJh', 'b3nEZK7cRBdAdNTY', 'ISInEeaX6jY1aNjX',
       'AMTU87NGLmouHKJc', 'jQbm56MXxVgF0UjB', 'GIEGLSm6a0TyX83w', 'aB4mCnaOwGDFJLR9', 'RxsACCWeyTnRA96J',
       'rjWTCP9Wu6gurqOV', 'ooHkmdrWk4OfxtzH', 'vcvAF61VdBqElkbs', 'Ev2kQb4rT4ubAIqG', 'iWLDXzrbHrHU8wjg',
       'Q4UQLDOLv3N9Y0pH', 'A6yHRkrkP4Bod1kN', 'hiFO1WWq2dDzpIu6', '9zHpfSdRsQHFpO02', 'C0QcK994K6WGgeVC',
       'V2yaUGhcVwajYDl4', 'J2gk089B7COXQhi9', 'gWtbXtyaLbNpjhUa', 'IqZYhEDCr3GWoySC', 'bHy5AH3XiGebunn6',
       'PA47RWUrKGiw4lMV', 'LachEfU2KdZUUdy1', 'RBphe4BjCv7LVkwQ', 'NzKckRZVzPRjsOEC', 'iOUBIAqqEaYV1nJD',
       'EA19RZOmUwtMjjuv', '12v2K8PhV9MAhVhK', '7gcPJDpA36XkZJMo', 'yY84FyngFmDeAbjw', 'To47uX4n7eXA9U9o',
       'PtRh9Iii8Rs7qajb', 'jNcpseGBunKHRlu1', '7f8pQn2jH5GnSYr0', 'SVK06wQbAxoKzfZQ', 'DIBfrZCBLWwIknGi',
       'pjc0tgOgu2hxI5EZ', 'Ezo6hUWUQjYP2BQR', 'M9GiW93lhUEgKqdL', 'pk2lkWapjvGETdff', 'PHckRWbyPyyuBG3N',
       'ioGqSDFi5nFYuaxf', 'rwkV8DNoUTPb6P57', 'bOVeRYAn6ckDPlLQ', 'SIaGhlWAHzE8LSVg', 'Aj28o24eqoO2iHEe',
       'fwHgpvV26ONp1Hgc', 'hUzK8ybZyYnUZ8zJ', 'kTKTwQ4sH7E6epou', 'LjBJlERgKs9qAqfr', 'IeYSxZRImqACvzJm',
       'mFNA5ucwNZd4PZdd', 'WXbKzKflZ04WQX6i', 'zXi5lxfQ8rUA9Kau', '3aCzwzPVkqZ57gZk', 'b7gl25jknaB1gncX',
       'FOmYi1Z3qDlQJrfe', 'DtMaVYA8vVf1g3mh', 'mfQemF8GPb3EMV1o', 'GzVXKyNzLWC2CwxE', 'TT7oz3qxBtPVY4K5',
       'ppR7vr6yXQit1GtJ', 'LDNNBtOaQGPI5sre', '4TPniJRSA2Db2EVt', 'kGDAHVFGIb0mZvj1', 'rCBvP0uutPi9zz1I',
       'SsKPYstrqA3k0UOY', 'liDyug8ImEUAdncw', '0k7x2MUra2TnG5wq', 'IvFDg9jbFgL3ZNw6', 'U3nWlvZ01TaNA56j',
       'ItyhjLzQig4960Ek', 'ZKIa5iZMFVg3z4sr', '5Oz9OJ6bsDHnQ5oW', 'm9TqSoKadMqGK0WZ', 'ZZgPdaweDPwR4suu',
       'oF6RqsmPctCfZCi9', 'msavS6jjUNrCeny3', 'SXhckJGtM5NvYgFK', 'hVUed6XW1JdkBAPH', 'RaVtcxDLbEjxDP3X',
       'CExQNOoF4oxLm3jG', 'bb74AwQG2H20xqeg', 'RXcEZMemx52oxUBC', 'Alxlg5rXdS77lU9o', 'ZPyf8wfPmCjGmc64',
       'Eu1aMaj4YX7hhcR7', 'wJ1HrVgnU906W8oK', 'iaR6dgNlEOolKor4', '8GhB9xlQlJ42C9YR', 'T9iMk1GMsA86j9wo',
       'HnT6CauaWdvF049V', 'Y5xJJvRzkcYphqUI', 'gbqvs5VKZUHZy5Py', '9F7XM0Fs9GrJlfmN', '9ChpjWQFebptkQcG',
       'wsT2UN2cqFpNACoz', 'Svk2dcSRc3JAHtLW', 'Ro3PYZOJixGl5uPc', 'fL7EqySsvPLTbqil', 'XXbe609g2zHuZwNg',
       'VMDPthHdQ7lQKou9', 'BJJ9wmGiwBmS6qxq', 'iv1Vn7s7FVyz5eUn', 'jW6PVYMMx2DSaFPi', 'pLjGA3kuhO0MhQL6',
       '2Jd7ntvSgGjWNQHg', 'S3snRjbSClyYq67t', 'NHUg58zUnB7SCgps', 'ntoDxCBfIAksv9Pu', 'h8uaDMe4o3Ply8se',
       'COTrlPG59aml777Z', 'xT5P6kShViL5TYy2', 'i5rUEWyPTM1ENKpu', 'e41v42niCzweKNPS', 'HdNL49MkHZBP7bqq',
       'cZywrpuXVgM14pEu', 'JhSm27Ir9Wof2cwe', 'F8PXx85gmf5YLm0A', 'jeBnUjrAp93xlcrR', 'nGqLvlGZY91ZFlVf',
       'SmbPGrOt52LpIxqF', 'h7hk4Hdoiq6Wd2Il', '8LuAKZp6aTMYmtFk', 'PPvzuGKf9AB8KsZt', 'k1WJp7QYNxJlPQcz',
       'G1DdjAm3KZtKIP7s', 'B4jbwq04MqHBjNbj', 'ZbIisCnpkQ9u1E93', '6RBd56JpzPVTfPAL', 'L4gc4re0IhiP1fAp',
       'eMq6nJqH1b3c4fC0', '8ZDtbUdBwafOFOoH', 'iWtaqfHtmHfhWogp', 'i9eHx4hmUM1QkZtL', 'N1nrPqERLNJTtafJ',
       'gdUmfrj1dq4Lkf3o', 'ZnoKaw0Je3QaN8Wp', 'W7qJydjKFeyQL5Mh', 'ZyeSSAfg5lpfIzJs', 'Zjgzkqhxzmabr3U3',
       'PqRRzaSX5mjs9DF2', 'tdTBkSGNHH5QFFxj', 'X59MlJPeMbIcNDcs', 'QzztaxOn8Cvi2Vie', 'yhBFAeTaubB0Fsow',
       'MQS6XhC8ulQE49R6', 'U6pNyfcK0oleNjIz', 'L2cy87vZGS6YMvh1', 'oXPwxlbQFvYfTx7t', '6XjyZj9y8lHQEJ15',
       'McA159JorWNtzxPL', 'U53rIa336KhBr40X', 'FSOziNemYmYBJI00', 'gKPzQX2KVrPMbV0b', 'vlBYBMXcb1UQCCXZ',
       'DZIjkb0HMT4EcmOY', 'x1QKYjmirZlqKGGn', 'rPbBb6pJ8iNWAG8y', 'FyCU6I2ksuOsURJz', 'wcouvE7n3xsfvvEk',
       'MxxqUEfeNy66MD50', '5924ymMadFPDDRSz', 'epGF0qzUOJpC2lZ0', 'bDBf5TxA7HelTOM0', 'AAqkMeQDE0PGoUus',
       'SrIZfKgjgpEy6QCs', 'WHtZzxEF8otqZOW5', 'NThILMVzYOxev2To', 'vc9B4TBlj0FIwOzR', 'bqNLYOvaca9tai7w',
       'BoRJvrCurnJj7FJ3', 'slXCsge187VBBcsJ', 'WbfCQ4uSAj3AJR4f', 'gx5QTGmzyt0Hblwe', 'YwlF2yAH6EaIzhYY',
       '0heE7UktKl4tjzPo', 'm4hPJRPluwMmbM72', 'jqGJgPZL69sEJuge', 'uwprtCtEo5z5kz7z', 'Db2k0BbWfX4KsIUz',
       'RC9AS3wWWIoBLKrz', 'ahqEi7c8Xzvds9LW', 'l38NOfFUgHHl97pa', 'A8bduuz6cNkdFhEr', '8iNoZywOkFBuGHAS',
       'i55ALVu58eDkmFnI', 'XqkNuIR7Z0QNG6tT', '2kOl9ExiEeoXR2lo', '3x0zvxp05u8x3j8h', 'QzEwHCf22mosKlif',
       'BJefxipwzQae0ciV', 'ytEelgthMw3c4c82', 'K6cJN5jLyfGANXbn', 'Uie8AD1IJBeGhT7i', 'HNbo5CkUsix3kDrL',
       'sbcIRoCpahqE1YZX', 'vmGom8bkaeq9e748', 'NcD2a8MJ7faa4Oy9', 'Uwfz52rJV65AjpZ9', 'q5AnyahXhSf8PwEK',
       'K4ENugN0Zy8XPlS9', 'LF1mfs2vZBisNC3c', 'pU9obbIpKFWhHT5e', 'TTDfQstrczWLyebu', 'ifXgRxhXKcvwjWBZ',
       'HOMZrTQZaC158xiL', 'SHehRsGA91xUmY3e', 'MA6o29qyPIcdvy2h', 'UUzO6jNJIK7HMJUP', 'Eo1yQyBTNri8TFpD',
       'fb6MjH0diVtUn0CB', 'NESmqtK28rqd6nUs', '3eYEdkf7nLmRVvTt', 'pojPkLfchOpI3eij', 'wZKBlLihjveBUF0N',
       'kPJurDkeWK3T7351', 'f8e7bdEd31PyZ3M3', 'ZTq5Ry3IWrarxCKt', 'aGQlIZWMkViDzRRC', 'pYYUor6hbOY0ODhs',
       'qCh4ylrZCaBXGyfM', 'Oc9ZUlHClWpOP4Uy', 'ZMxjxGuyKCw18t1q', 'Px0qYBBdnq5pMuju', 'CMhHL5FkfOr2dJUw',
       'Kglf9wG0GTcBKh1d', '2dzhVawtXPloP9gJ', 'auWk8ubRIE3q6LZV', 'VMC58W3EnHAqVFRF', 'esnWd8NHnaD9suNb',
       '6YPHHFF6VrQ3s1ea', 'KG8QuN9CCqnic73P', 'DaylDxVPRGv4r8JO', 'LqsHtBBP1BNMU4Gg', 'LLv3biTyiNwvNrZs',
       'DaQQipzG3Fj1E14d', '8p6ePR52eDs3EaOz', 'oMI525H9c8vu7TIm', 'hPgjiORBfij7FIop', 'kIXUhPO4PdyfuxUa',
       '7RGpkeMF9TaaCfrM', 'EsJCmCY9H4fRtx6H', 'lhuSAP1VtTP6C8fy', 'FKOjFuIULpMg1PjB', 'UzKWLN73UqeODKZY',
       'WIuWNXo0hvgsrHKE', '8PD4BJEqJFHw2rbu', 'z6x0ditYOINhGkQa', 'iRs79F5XiVXTwRDG', 'igc9NGbX7TxySgE7',
       'kmTSzSViSNlvqBed', '3mE0VMKpfLFWPfi8', 'fFOL6h5PdEXXwRwL', 'BgPq1IkG5VYBdLvh', '97HLEYa0DAWdlOYh',
       'fFgcTWt1zuo4jTFo', 'fdQmePwk7HXI1qna', 'PI2WhIrhwOgAlmDw', '3iuNNdh9U5PTIznw', 'za5UJBu7hGOMfvuD',
       'QrirP7kTMKG0lEYF', 'duQMFe4XANLKLXB1', 'JJcs4nmhXNMdgLIh', 'gHebYfN5kegdjrQe', 'mBhSe0OwczcBmmOx',
       'DU8zXJImxgyyi6wM', 'fS4jLqPGSe1gLMwd', 'p0f83qCBqop8aJ5J', '9IL3NL71GLmr4RpS', 'yxjfpabZzoeKW85T',
       'LqOXvSsZJCcNk0Xz', '9qkOpwBfub0g2een', 'FnodwsMkIlolyUXf', 'nPj3WnC7avQFdkip', 'jZ6bnxEZXFWknsUc',
       '2HMtKmyV4yU12Rpn', 'jzzYRjQjip8KaBA4', 'ZHPp3NMBy4gBBRX6', 'gozZkQCgT9svLuB2', 'rZB8MFFKWI0Cah84',
       'DjM0gisAD4JdEvCH', 'wRQG4HeVorp4jOuL', 'rei4K5WCTHrRwoor', 'kSAPyKm9ax51H4eG', 'U2wKhkrob00j3H6A',
       'Tsknel6ouVBqeQMC', 'h57L9t6LsSpkYrd9', 'hCApLGKgXlx0JVgg', 'xyVA7VFw2PT5W4wC', '9y7BRRN2AeuJNWoy',
       'TXWsM39IvakredFG', '8p2b4Jxy3EsMHkxp', 'NLSOjDftct9odyiT', 'laVPVarmBqrZ3mCD', 'WbgqOR6H4FGDjf12',
       'rwm629fkVwP1blQz', 'YOap52iGlN1CTfP1', 'BU0ro01mUGLb6j6q', 'DVBQ6dzyKUoN8Fm9', 'jKD9eTXFIAQFjCKx',
       'AFxH6OiNogFesPiJ', 'XZ0PqmgdMlpngSoM', 'GYT4RxYzHErXk1iE', '1IoGnnRKdDw9UwNc', 'nMn8MDTR1Kwe9MIc',
       'ixpLD8RsizewEGgE', 'AhgEGTwk3cYCd7IG', '81GYeM8I1xKM8WHI', 'b8gjpSvUNlr9zWJ3', 'MRZWXG3b5KOz371K',
       'oWtf1fYfzCfgdPAC', 'YptlFVyBtqBtAo1Y', 'HTt35AjxTaaqP6iT', 'vga0WKHfkr5yOrQR', 'fRQcSxGUse5iLuHn',
       'cTYd5umiFXvlrcUx', '0dZBoS6ct4fD6V63', 'ItocFHqIpkhHylsv', '8m72qAC9BQRHMDz7', 'Yk0SWLqc4h1797Yh',
       '7bMbjqqgcN1iqp2R', 'PckEKPBeiFLJD6CW', 'eInkOgSjE2z1OgFw', 'V0VzEiksWMYodpXb', 'mHq3gyDK7kz9FMJR',
       'lfqjpnylNx5gmkvT', 'snh9S9bZVhHIoWt5', 'HaXWYkthBGHT4Wr2', 'ppMWSf5ImCrEUlTC', 'FxKUA9kpnNgmjjZZ',
       's8HgRVYkGMtHwP9w', 'cUZ66eOTEOGGsY1k', 'EQOHyHF3DWmcQsNW', 'd8MGAosKYmrmtWPS', 'czh0pa2zBhB1wL4k',
       'sv8s6TjrXpb2OUjh', 'hMwZdzpBpdP4wZmV', 'zayNzLfGxRjER3Mc', '6vADK8Wl05NXGzWE', 'E5WXXvxomtiWrtjo',
       'UyCkHP3YyeOPR6Oy', 'ZI7IL2uXAZQVURJL', 'bJobeUxNkVoQJLqw', 'aQJ0SVIXSnVKzBpO', '8RBKAM6nGwS5WW8B',
       'HQj8Akngs55oD2HG', 'MXsCNmgm5MbZ8pb4', '2yHhOigF0yznhQIE', 'gURvV8S6apkeEJb7', '2SK079hU8XjY4lf6',
       'deKlD5zS3OrnUY9P', 'hvWCAGxJnzzg09x1', 'RHSPIbK3dXWaMlKW', '2I2tdRVAVNEj7ujp', 'mBCDf76pPSfJDpL4',
       '0FO0olR8kCR0V3zU', 'cEnexXgM2qHNKOGG', 'GbxjnEynvSQOTO2P', 'Sxumcu7xudUaQIIR', 'dd4WRtEMcrfmlEog',
       '8rePCCHk47iz22Qx', 'pmuvm9QwJQDArALa', '54O0GGmlG6xvmaET', 'Z8NPhuAW4FaD2GlK', '3Oh5UBjrZXYRNWUj',
       'Fqb7kO9cBtqdS6Ko', 'fbvpaUW9xWLbgBWL', 'DsL1KY9esR1a3qZs', 'Ksf5FdjryJVUAmly', 'B90VgaF0OkoSLOil',
       'Hzjh4sENZqe6NuWp', 'sTFOrb0VAj1lxO8E', 'tqCMeQALjGKZcses', 'wP7FdAEZUSThSZo5', 'wYfPHhcMZPomCTNp',
       'd5KtHsR6fGkY7ABL', 'mykfJIQGi0P5soBR', 'IKCf64oh1i6dk31F', 'pTABTmXYccFCuibX', 's58LKBc3mfrp7ItS',
       'drrJsYuwRGhvDil2', 'IDoVYlGwA20R6E0j', 'xhcqXS09c9oRab5d', 'jmszJqFPmQeYRJyv', '3HOvLmZRdARQJL3K',
       'm9lETT6aT3riFGbF', '5qEWSk9fkgcd74va', 'Bg2izkZwP5wLDuAr', '4PvjhzDXN4gCVc0d', '2RuVoFaDnRcK44Ci',
       'h5HxhbYQbmJ7kJuZ', 'epoUV3mEsRuLsBI5', 'XHFxR4Wtm7SntYsX', 'kT9xxJ8q6CC0RIe1', '1qC7Ekkn0wQdJJ92',
       '9YzfB23wSMCQ7OLu', 'C0ioViz2e3AUBOA6', 'MWVLNZs8X2lEcImo', 'E5rTj8eTMtmrK4Zf', 'hgVGpAUnGCuFy1Tv',
       'PuJzKs6BtiRBmVXG', 'qsfqmUZwIukJ87Oz', 'GD52BqAQ3UKMoTlr', 'CBFvUxsLFB1OInyz', 'GnjuKQw7x5FmR7jv',
       'T80iBmclISVvPh8c', 'et8XNsB0xrtJA5Hh', '4u81bnzmuDpuf83k', '7dnWGsgmOnaYeeyK', 'aZlzPEdtH9UkW7rR',
       'ld7L5rO3dCYpOsNi', 'S4BMsPYIeBJkk29e', 'T4r2h2CKnCSLCs0B', 'SEmSMp3cls4z1oUy', 'pYt65ezmdmbSzYpN',
       '9WFZSCAgEtHqsQxU', 'Pn13qFgcp44BZjjW', 'CV7YxAxeGL8uHOjU', 'V1yIyBjdVEpWfhGA', 'bYdD2gp7m7tpjy3T',
       'IG6EaVY0tm1mwRuM', 'w2ChpE6UDALS5pG9', 'PcSEqaDh9reFnkly', 'tL2sXbAHMUF8sjXI', 'mh1sVZyFIRPgdHeK',
       'k6lySjFExGalKnOo', 'BWhAouEab0Oyz3WH', 'aVkJaOlpEXEu1sA6', 'psdoNnM7Wy3dFnvT', 'OeMXRYnVEak7xaje',
       'qr6L3cx06S2mQwg8', 'glQXHvhDuuNHqwpG', 'Fq78fai5yrXRuQ2o', '4FdbhXS6VbQiJvTc', '2tD8A4kyPAm8iEeJ',
       'T6VJQu2MUSFSsNwn', 'Hu4Zr7fAQmaeNjps', 'LTFBZt3P9WcgfOqA', 'wAp2qTiqWEkRsZRe', 'I6FhAjcOlVyxLNhy',
       'J0McwGk9Ik5yfCxr', 'cxchEshsXvusOZag', '3Ix4H7CpLruwpv1s', 'ue8aCEpSJwfIbsmv', 'Gr5QZfj4yZ4SYmsm',
       '2eoiXKdLz7shE2cK', '7WLHrme0SMvwWc6H', 'BIIYQWlbwCKMyoZN', 'ErfOJHTdA09fd3n2', 'veAgcpuKKzipzKVb',
       '9xWu2496tOsY6pgH', 'B0ZoYNVRgdN92LT2', 'RkBrykODCpWJ8jXE', '99NtmZRBptP7Ypxx', 'fCIwD1F37qm1mGxc',
       '8xMfSrsDWTLkhdjc', '7n0bmPLQT5XgnPr2', 'SV8RJcF1tlCK7fqO', 'UeDJh1uZ0v95JKCD', 'xnmwnXIqqw354i51',
       'Wddn72AXrDZrT27e', 'l8baVF5yAl7Poiz0', 'yOUGoRvzIYtGgHNN', '8zTUeoCLaQFw7wRY', 'bP0BWwLiOr1YPsPF',
       'gViVBwCVwXn04LcJ', 'qDJat9CEAgxchvYv', 'Eq13N3emLIPPvQNJ', 'mNjatWaQ9Rwe043r', 'RM6ojbIIVnkD4qea',
       'd7UAKcKsGLV5CL3f', 'if3MtaPgSVCdrbpj', '8UPudbJ60QS7TQJg', 'szFHXg80ERz4jpb0', 'aaGmp1MPYAhB7BdT',
       '39jwZOryd8pSepSB', 'qNham7v8ZYDVll3F', '0oGAp0RA9KOzq5Jg', 'aVfPNCcfi3l7sOim', 'FZukd2BlmebwYcmK',
       '0M4LzGPLZpKR584g', 'kM9lqPwx5tfhc9bv', 'BwcbgQAJOXVmqM51', 'CA72eV9khuEIapEO', 'Cn4f6dSnmaGPh4yf',
       'cXsyz7d04ugVfumJ', 'XhvQEkUio3gfGPrc', 'L6VzE9XUfnWRRMog', 'f5OkfuzBqzTWweGW', 'JjOg3m8zSMo21JA0',
       '50Kth3CeCbkvZS31', 'hSX5Yd9IY5IbX4zy', 'oBKKV8ov8Q0CEH7K', 'iLiycMSn6KWtrUE0', 'MVsgEhtr0BlsLrg1',
       'O5i2GQtImtJiwBYt', 'ym69iFQD8R0nZCap', 'hCxY8V7Ex7ubufUh', 'MXB3ze25RztAtSWt', 'rwxqfOaqBodX2gJ2',
       '3jEMdQ6iZiLIGLRF', '8MoHJNFCV7EyaTA9', 'VOev1kuIuwgmA9Ng', '5QaYAt7bcVWtDT26', 'jdaRzlbnvdHNGDXw',
       '1REnVX6XziDJwG6E', 'hsx4Fk3pMUaxDZY1', 'HoiYLoK1OLPfoghg', 'uTmT7pdRtoeLLm99', 'RPkwN0LENpDiokWy',
       'd8ezPzOSXJ9uVOV8', 'H3aq8AiCBWGG55ua', 'oND8cTGsNu774LYo', 'hp7zzMFyZ6ltsKa0', '7t2Y6sx4qcxDszWf',
       '5ztAmDKrqwB91WJu', 'oA9UyG0P4KG2rA0c', 'aZUlehTxWlUFD0X4', 'KJay76T22RgKkKXj', 'TNuGNvhvj9t2K28z',
       'PTVeC0utF0HbSuE6', 'iHiiy6qZgSpjlrfB', 'ABCLJkrEbbTooGJW', 'LiEMdU99uEhQLxrk', 'NLgsYvcCIIIvkZkD',
       'ZWUrm4b7QFQ7R6qr', 'kTzHTiMd1KLCmTzp', 'gCIaPIE11HjwvM6H', 'yY7oLKRegTOdZsW3', 'qn2FqKfjTW7adG58',
       'GlRVBrPzlgRPOuBb', 'zrw3CoknymE1eCyB', '26I5QwklMWZ0BBs0', '9PmMNYnstLFLFgax', 'jLDJooGTii3Zx5uN',
       'tPvRc11cuwvvIBKm', 's0oPEsHjGgX1CZ9V', 'fBVhXpQGnBWpIb1M', 'SLOIqtoznyS8B5v9', 'EEJLSSbMvQ7ZTErF',
       'y9hvC70anRpUC6c8', 'y6NenInaZVFxByn0', 'vABov8HDPEWLEcgH', '8KWgmOJ1Ex0WgvkE', '3Wevylc1NNHtCT7U',
       '1St5AGXfF5fDSmAx', 'Mey3Yx4KqdFHAM6R', 'n539lSfp4TLVtwnV', 'EssjteBYIRndGNx1', 'KOlSFwaIOpcvPPhX',
       'FKZuxJm7YHBtCnaQ', '8u8UKDH1TMa6VsBA', 'NfgpSgrxnMswLHFI', 'IQGq4I3QPf8mYoC0', 'XcQ6jftKAqVFBzC7',
       'E2t1oD6VH8psPBp5', 'NKf06WqjUqxcGT7l', 'EcVL9CtMyjgrT3Pw', 'cx6OTyGXJhVKOBp3', 'xSXrOVJGPxGqPfj4',
       'ajFueo1BAlEAMvOS', 'F8yIeQADvXgKROXh', 'hDRTShUiJi0jl8sK', 'afK6DAmCyEGRxPcN', 'zTSeCSqcQ58S6zAJ',
       'bXbG05XWI4BcqVJ8', 't85zzTNtyXLfNx03', 'w7W4Uc7E1yq1Px5s', 'P1vE2O9r4HMakdMP', 'cy56x59lEI0XLqrI',
       'uBY4K4ffSkSQBhas', 'WyXKTRxUPwqYz9x2', 't66Zuefq28Tjlfsb', 'OSJfh0s745gYRvhp', 'jYw37AKBjmq8JCbg',
       'qLCunS1U3MSGtAgW', 'VNXg2MH4JhrMfcMw', 'WYnwYA1qwXNU2QdQ', 't3Y2gTz0IPRTPQWV', 'NtOfpzNGZbo9MaKQ',
       'pNJpDwL6hrjTjdA0', 'WVzNA0LcYbiMrv0K', 'Z8ne28l8mdUzZDaH', 'UjdywC2oN8fFSPQV', 'NAID5fDFgfg3i3mg',
       'TS6XHbxerZEGzQ46', 'IpzNPf8t7I8GBWmT', 'Ttjv4OIiX5nEBTBz', '9JGlPQabKXAgygYv', 'oqaaPBMQYMNjHwCQ',
       'cyXPFN9WpcM1RHA7', 'YMTbiT9WgqrO9Aww', 'FyU6SInqEsyAs1qf', 'HO0HqOAEbLoPpwEG', 'vqepN1AQOxaSrsF9',
       'm1UbvCtxPAqasAsS', 'OtfVOvG2da5cFnIA', 'OdNbThSyFuB6yF8w', 'bIcU0oWt0rgBT5bw', 'U4RIog4yhiLRhium',
       'M7EhYfMrz3RV79zH', 'o2qfXgIPKTkkUOt1', 'zx45c8Wi7WjLN3l4', 'R86vg9r2XhL6m0Qp', '59LthLQyAHeW54lR',
       'L5d5X4j0mw8SWd5l', 'vE9np0mpqi4vLx2h', 'KSROji1OvjlpxNVh', 'TrS1qRhKKvNk64uo', 'kryR9lUQZC9uyK1U',
       'YYGUgOprH9tEKHJG', 'wX7ZNVLTJwkFZPA1', 'qfPWn788EZrLJdqn', 'edrTR0GB0ea7cizN', 'cpLAUjI1PF9fkWdU',
       'j6Tm8TlHdXd8su3J', 'WF8BZ6vxDroK2Qla', 'judeoHiZYfNLywH1', 'kKeStuw4d1l0cpM9', 'tdPlHNd1rdSjc59l',
       'b9m9CT6mpSRAzWlx', '87yMWXl724EF6wbs', 'dKLhlv8jomFXY61v', 'GoiLw34hvBMzHiZN', 'S1ORA42nW3AB2D6W',
       'oj6k90yivT9ehlPX', 'E5u2POQTApvFTN8W', 'b2zpufVxDF7cKACq', 'hQmA25nUESbHc4w0', 'yfBwYPncishH8MJI',
       '8FS1mp7nueT4gHze', 'ipI3ltPMrtN1cwaG', '34WP6BBCo39seYdc', 'rTHtaxr831oU1urX', 'YbAwOeUnU5zcEFRQ',
       'isFvNzE39MlWMlpm', 'Ej0zyHtx9FiN6Vsj', 'sccrj5lksmuZZEos', '7uuwy6QFTO6B3pw6', '73nGlBSdoAuBWswu',
       'y86ZoJqjIYNVB3N0', 'NEpkjZhceWIrCmKE', 'NoFlBQqdghMOjcxg', '5oZc02uKTG3qLjMl', 'oedmFLH79o9T83r3',
       'pSDJfajaAnW3zVdS', 'NyyiD1UlqrT184M7', 'Rv2f7KOL3xr2az2M', 'UtrGWK6Q47pciWKz', 'V0ZIhUrWrpxWavbG',
       'rix93I4DHf31LTmz', 'dwkxbKdcz4dcxx9n', 'J42wy1JDV47JG9Yr', 'tw8EN2CYJVhk8hH3', 'Cm1Ptu7WOHTNBhp3',
       'CZ6yzeBoUSwwKn4I', 'yTwNfcoLHY77hIFk', 'dl4PfBGMjVpxb88v', '2HaEFkV6bMKK3KJq', 'KehdfY0omb0QcPmz')

--risk screening AAL from RMS winter storm, wildfire, severe convective storm, hurricane
select distinct business_id, ws_gross_loss, wf_gross_loss, cs_gross_loss, wt_gross_loss
from riskmgmt_svc_prod.rms_property_gross_loss
where business_id in
      ('R98ssyDivJRX5nO1', '42hUgqjpJ8ZToLkT', 'pGn51mJltNGafnHB', 'XLGca7vakcEnivc1', 'D1Zm16qmJC02GRZp',
       'sXYYKnDhKgAAgOta', 'FRMr7Ywfu0Be2Ogy', '8GogzyqX2vCS2X4f', 'AI5jcvK1VpFEh1ZZ', '5VdP4MbMLwJFr3os',
       'gXP5kGsphcAK9fLn', 'ZMFh8jM3q0KcOpAf', 'MiHQsec3kZeKf74f', 'yJTpNVz83m9duf0r', 'KyedYDk5nGQA0xbO',
       'Lk2cRQlsj9maluSg', '9OIVmZLvXPGZvON9', '95qywfTyXtJzVQ4j', '6QWH5FjWMAMwD8ma', 'xQXYFtm7Rvav5Tt1',
       'ArvMmjurtwJvug3j', 'ZIkn6Dhc699Q4lwx', 'JhJrMANlyptQ9GJE', 'nfjUKgRJAgLdEQkA', 'KIpQKvYpqnWTUlas',
       'Nfg1CwX4ibfMmVat', 'm6rGIIUVwGtIYKeh', 'W9zmnCIYT1nlNQtz', 'rumFsC2jc3nepxK9', 'oXb7I5d2BDT0zgt9',
       'UGsEBnm2fMdR34Oh', '2T0s0V6A1lvS3Ycw', 'BSSHmoLygGvxGg7E', 'geuuaU3xmSYrFwkq', 'Krm1MjLpkN8DUu9V',
       'zY3CdzWfgzOWU2a8', 'y8ThK5bhDhJqEbu2', '6rwo4lBqlf4htPKR', 'miHpSuw90hWNL2xq', 'zH6UoKLKzojLzFkM',
       'rBubi9TfNlpxGaff', 'OkJPS6oBv4Ur8tDK', 'nNODUm35UsgQBKaS', 'X68pn46anXZ0U1vf', 'QWhupIiiUDcskueq',
       'LH3zEwU0bL8OAFC5', '7BmweiVlVd6zsoJD', '0L7azkxcbxXNWskA', 'mmYU2I9RYG1JvCfO', '4v55u8SdSBbCalxw',
       'VwcaRdS1V69JYKAC', 'fgni6qc0PixuxLB4', 'IwpUjv7g2d2z5EqH', 'hPXV7VzKVG7INJ7q', 'iC1PqYWlFdp2A56K',
       'ovYyfLeCXoqVEzK6', 'yi6AQWWPbn8odZVi', 'CKZidhw0aEWGBJzU', '25rBe1BlBZFcvGbA', 'BobpZOuv2HMcwV6D',
       'gnkAHqGI2EUZ2Vex', 'nN2q123Gdo49pEwm', 'VDhgshcDetdBfHca', 'ro2ElFre7RPYlY0B', 'XzxSe91lFhGz85tS',
       'w3dI9fu88a0W9Yyf', 'Bl37CLwK9N2UagKv', 'zXrbIdjrDCbsaTAa', 'Un8gnzEjGsa3sODy', 'UoP2vBqNPt3PV7ae',
       'nrchL8MSNtBSWdHE', 'Qza8NnEyYkgS5H5x', 'I0B5ezW9scW59ysD', 'nY94audpm2pFvvYJ', 'Do8l6vVqt5U5TURG',
       'CvYIjn6MyI0nTIgD', '4azkhgfuJjdHG2ZN', '2El0ln4Id3rardwo', '4cL0oc6v6HI8GVAL', 'fLkVjuckXU2WOXbV',
       'rIQlmPm8aIw8j30t', 'qV619W4kGBQzveGL', '6W5t3YTmk8SfNeWi', 'cAvGbqYCWCFYLzJT', 'hLj5QcjbakdEY7Aw',
       'K297wRXjOwSiK3fq', 'C8L5kQHvqw4HksQ8', 'uSaf8lr7yzv9vovI', 'yE4VDxxsEQoZl8jX', 'AU60GIi9tpcIMwd0',
       'rcCOnwuqjUjBpHUv', 'bLjgo2da1nl6kbNQ', 'itDYtzTgSoS4gr9j', 'CylVFBOyFbUcgtrF', '7jWANkOGjWysg9iL',
       'cQK3bpycMvDy1HjJ', 'qwLiJzfHpbTK51S2', 'DyUGG88SpoHF7NgE', 'YHsVk6VTDMvEWpZN', 'REznDQBXhcqJCOdh',
       'bFH8NjgZsIk9qVXD', 'YT6jPfZkxocpBHIb', 'wHlvWPh0QJyd2TYC', 'u6cHaApd4KLRbToM', 'KENdou44YZP83EQD',
       'W7hzfaViZSHqFPT0', 'uaI5XaErAi34QOP4', '8ymBLlCiHqNlNIEU', 'MUWIoiXQwFiu5cUQ', 'hmEuYIwNxopNNxWb',
       'fVHXo7E3Ifu8DEdN', 'NZXVAlWWAHu9pGkc', 'C8ZTnyCxNbL2WlOi', 'MqUX8j3Y7L8C0WYK', 'CF2ADHomooBitSbm',
       'pnZyBbjJF7QznEWY', 'wj02K43ChCRtTDYA', 'ZtxHjkmde6gbzxSw', 'CKN8wCJ3600fV2ux', 'mccU3VYfeFwWGD3f',
       'O8FAJKsaCbT7uCXV', 'fYmCtqqdg1qxH86a', 'RJ1EXoTHbxDBUdJI', 'D5wC5Pmkoc2JbrHz', '9nUlQXWKZHrrxv2j',
       'sFTB9iLfghcEPUHp', 'xDpRSMRkxOE9ZMG2', 'o8SSEGgPYt4FSjAw', 'Kdp7IDAYVcNyoUv2', '4Kye6EMJmlZ3q8ij',
       'wBFOd3fLClOZGJ3f', 'AYi57CZJ47BrjYUS', 'VATPJeoW8mfXwsqH', 'ddYBBpP4XCCj1HrY', 'U4bobUZoL3aYhFIJ',
       'WNtZiEc570wwFVzy', 'znkbr5ku9sDy3nUZ', 'i8W2Xw8K2ZQYQ0zK', 'JDAtrCunKsn7GAoI', '2gImHXdCQZb8j59a',
       'SH6vI4ZQm0jcmQo1', 'yNCnSmMdGphpPFXE', 'xYdTAKCvrSsRAGWX', 'ShZ3PHnANsFQFXZ3', 'BlbPDIVn3IhIew4C',
       'mwtjzayelhJAfMRk', '9SXHoQfElVC0umzS', 'mKUMHbItOsYbtkJh', 'b3nEZK7cRBdAdNTY', 'ISInEeaX6jY1aNjX',
       'AMTU87NGLmouHKJc', 'jQbm56MXxVgF0UjB', 'GIEGLSm6a0TyX83w', 'aB4mCnaOwGDFJLR9', 'RxsACCWeyTnRA96J',
       'rjWTCP9Wu6gurqOV', 'ooHkmdrWk4OfxtzH', 'vcvAF61VdBqElkbs', 'Ev2kQb4rT4ubAIqG', 'iWLDXzrbHrHU8wjg',
       'Q4UQLDOLv3N9Y0pH', 'A6yHRkrkP4Bod1kN', 'hiFO1WWq2dDzpIu6', '9zHpfSdRsQHFpO02', 'C0QcK994K6WGgeVC',
       'V2yaUGhcVwajYDl4', 'J2gk089B7COXQhi9', 'gWtbXtyaLbNpjhUa', 'IqZYhEDCr3GWoySC', 'bHy5AH3XiGebunn6',
       'PA47RWUrKGiw4lMV', 'LachEfU2KdZUUdy1', 'RBphe4BjCv7LVkwQ', 'NzKckRZVzPRjsOEC', 'iOUBIAqqEaYV1nJD',
       'EA19RZOmUwtMjjuv', '12v2K8PhV9MAhVhK', '7gcPJDpA36XkZJMo', 'yY84FyngFmDeAbjw', 'To47uX4n7eXA9U9o',
       'PtRh9Iii8Rs7qajb', 'jNcpseGBunKHRlu1', '7f8pQn2jH5GnSYr0', 'SVK06wQbAxoKzfZQ', 'DIBfrZCBLWwIknGi',
       'pjc0tgOgu2hxI5EZ', 'Ezo6hUWUQjYP2BQR', 'M9GiW93lhUEgKqdL', 'pk2lkWapjvGETdff', 'PHckRWbyPyyuBG3N',
       'ioGqSDFi5nFYuaxf', 'rwkV8DNoUTPb6P57', 'bOVeRYAn6ckDPlLQ', 'SIaGhlWAHzE8LSVg', 'Aj28o24eqoO2iHEe',
       'fwHgpvV26ONp1Hgc', 'hUzK8ybZyYnUZ8zJ', 'kTKTwQ4sH7E6epou', 'LjBJlERgKs9qAqfr', 'IeYSxZRImqACvzJm',
       'mFNA5ucwNZd4PZdd', 'WXbKzKflZ04WQX6i', 'zXi5lxfQ8rUA9Kau', '3aCzwzPVkqZ57gZk', 'b7gl25jknaB1gncX',
       'FOmYi1Z3qDlQJrfe', 'DtMaVYA8vVf1g3mh', 'mfQemF8GPb3EMV1o', 'GzVXKyNzLWC2CwxE', 'TT7oz3qxBtPVY4K5',
       'ppR7vr6yXQit1GtJ', 'LDNNBtOaQGPI5sre', '4TPniJRSA2Db2EVt', 'kGDAHVFGIb0mZvj1', 'rCBvP0uutPi9zz1I',
       'SsKPYstrqA3k0UOY', 'liDyug8ImEUAdncw', '0k7x2MUra2TnG5wq', 'IvFDg9jbFgL3ZNw6', 'U3nWlvZ01TaNA56j',
       'ItyhjLzQig4960Ek', 'ZKIa5iZMFVg3z4sr', '5Oz9OJ6bsDHnQ5oW', 'm9TqSoKadMqGK0WZ', 'ZZgPdaweDPwR4suu',
       'oF6RqsmPctCfZCi9', 'msavS6jjUNrCeny3', 'SXhckJGtM5NvYgFK', 'hVUed6XW1JdkBAPH', 'RaVtcxDLbEjxDP3X',
       'CExQNOoF4oxLm3jG', 'bb74AwQG2H20xqeg', 'RXcEZMemx52oxUBC', 'Alxlg5rXdS77lU9o', 'ZPyf8wfPmCjGmc64',
       'Eu1aMaj4YX7hhcR7', 'wJ1HrVgnU906W8oK', 'iaR6dgNlEOolKor4', '8GhB9xlQlJ42C9YR', 'T9iMk1GMsA86j9wo',
       'HnT6CauaWdvF049V', 'Y5xJJvRzkcYphqUI', 'gbqvs5VKZUHZy5Py', '9F7XM0Fs9GrJlfmN', '9ChpjWQFebptkQcG',
       'wsT2UN2cqFpNACoz', 'Svk2dcSRc3JAHtLW', 'Ro3PYZOJixGl5uPc', 'fL7EqySsvPLTbqil', 'XXbe609g2zHuZwNg',
       'VMDPthHdQ7lQKou9', 'BJJ9wmGiwBmS6qxq', 'iv1Vn7s7FVyz5eUn', 'jW6PVYMMx2DSaFPi', 'pLjGA3kuhO0MhQL6',
       '2Jd7ntvSgGjWNQHg', 'S3snRjbSClyYq67t', 'NHUg58zUnB7SCgps', 'ntoDxCBfIAksv9Pu', 'h8uaDMe4o3Ply8se',
       'COTrlPG59aml777Z', 'xT5P6kShViL5TYy2', 'i5rUEWyPTM1ENKpu', 'e41v42niCzweKNPS', 'HdNL49MkHZBP7bqq',
       'cZywrpuXVgM14pEu', 'JhSm27Ir9Wof2cwe', 'F8PXx85gmf5YLm0A', 'jeBnUjrAp93xlcrR', 'nGqLvlGZY91ZFlVf',
       'SmbPGrOt52LpIxqF', 'h7hk4Hdoiq6Wd2Il', '8LuAKZp6aTMYmtFk', 'PPvzuGKf9AB8KsZt', 'k1WJp7QYNxJlPQcz',
       'G1DdjAm3KZtKIP7s', 'B4jbwq04MqHBjNbj', 'ZbIisCnpkQ9u1E93', '6RBd56JpzPVTfPAL', 'L4gc4re0IhiP1fAp',
       'eMq6nJqH1b3c4fC0', '8ZDtbUdBwafOFOoH', 'iWtaqfHtmHfhWogp', 'i9eHx4hmUM1QkZtL', 'N1nrPqERLNJTtafJ',
       'gdUmfrj1dq4Lkf3o', 'ZnoKaw0Je3QaN8Wp', 'W7qJydjKFeyQL5Mh', 'ZyeSSAfg5lpfIzJs', 'Zjgzkqhxzmabr3U3',
       'PqRRzaSX5mjs9DF2', 'tdTBkSGNHH5QFFxj', 'X59MlJPeMbIcNDcs', 'QzztaxOn8Cvi2Vie', 'yhBFAeTaubB0Fsow',
       'MQS6XhC8ulQE49R6', 'U6pNyfcK0oleNjIz', 'L2cy87vZGS6YMvh1', 'oXPwxlbQFvYfTx7t', '6XjyZj9y8lHQEJ15',
       'McA159JorWNtzxPL', 'U53rIa336KhBr40X', 'FSOziNemYmYBJI00', 'gKPzQX2KVrPMbV0b', 'vlBYBMXcb1UQCCXZ',
       'DZIjkb0HMT4EcmOY', 'x1QKYjmirZlqKGGn', 'rPbBb6pJ8iNWAG8y', 'FyCU6I2ksuOsURJz', 'wcouvE7n3xsfvvEk',
       'MxxqUEfeNy66MD50', '5924ymMadFPDDRSz', 'epGF0qzUOJpC2lZ0', 'bDBf5TxA7HelTOM0', 'AAqkMeQDE0PGoUus',
       'SrIZfKgjgpEy6QCs', 'WHtZzxEF8otqZOW5', 'NThILMVzYOxev2To', 'vc9B4TBlj0FIwOzR', 'bqNLYOvaca9tai7w',
       'BoRJvrCurnJj7FJ3', 'slXCsge187VBBcsJ', 'WbfCQ4uSAj3AJR4f', 'gx5QTGmzyt0Hblwe', 'YwlF2yAH6EaIzhYY',
       '0heE7UktKl4tjzPo', 'm4hPJRPluwMmbM72', 'jqGJgPZL69sEJuge', 'uwprtCtEo5z5kz7z', 'Db2k0BbWfX4KsIUz',
       'RC9AS3wWWIoBLKrz', 'ahqEi7c8Xzvds9LW', 'l38NOfFUgHHl97pa', 'A8bduuz6cNkdFhEr', '8iNoZywOkFBuGHAS',
       'i55ALVu58eDkmFnI', 'XqkNuIR7Z0QNG6tT', '2kOl9ExiEeoXR2lo', '3x0zvxp05u8x3j8h', 'QzEwHCf22mosKlif',
       'BJefxipwzQae0ciV', 'ytEelgthMw3c4c82', 'K6cJN5jLyfGANXbn', 'Uie8AD1IJBeGhT7i', 'HNbo5CkUsix3kDrL',
       'sbcIRoCpahqE1YZX', 'vmGom8bkaeq9e748', 'NcD2a8MJ7faa4Oy9', 'Uwfz52rJV65AjpZ9', 'q5AnyahXhSf8PwEK',
       'K4ENugN0Zy8XPlS9', 'LF1mfs2vZBisNC3c', 'pU9obbIpKFWhHT5e', 'TTDfQstrczWLyebu', 'ifXgRxhXKcvwjWBZ',
       'HOMZrTQZaC158xiL', 'SHehRsGA91xUmY3e', 'MA6o29qyPIcdvy2h', 'UUzO6jNJIK7HMJUP', 'Eo1yQyBTNri8TFpD',
       'fb6MjH0diVtUn0CB', 'NESmqtK28rqd6nUs', '3eYEdkf7nLmRVvTt', 'pojPkLfchOpI3eij', 'wZKBlLihjveBUF0N',
       'kPJurDkeWK3T7351', 'f8e7bdEd31PyZ3M3', 'ZTq5Ry3IWrarxCKt', 'aGQlIZWMkViDzRRC', 'pYYUor6hbOY0ODhs',
       'qCh4ylrZCaBXGyfM', 'Oc9ZUlHClWpOP4Uy', 'ZMxjxGuyKCw18t1q', 'Px0qYBBdnq5pMuju', 'CMhHL5FkfOr2dJUw',
       'Kglf9wG0GTcBKh1d', '2dzhVawtXPloP9gJ', 'auWk8ubRIE3q6LZV', 'VMC58W3EnHAqVFRF', 'esnWd8NHnaD9suNb',
       '6YPHHFF6VrQ3s1ea', 'KG8QuN9CCqnic73P', 'DaylDxVPRGv4r8JO', 'LqsHtBBP1BNMU4Gg', 'LLv3biTyiNwvNrZs',
       'DaQQipzG3Fj1E14d', '8p6ePR52eDs3EaOz', 'oMI525H9c8vu7TIm', 'hPgjiORBfij7FIop', 'kIXUhPO4PdyfuxUa',
       '7RGpkeMF9TaaCfrM', 'EsJCmCY9H4fRtx6H', 'lhuSAP1VtTP6C8fy', 'FKOjFuIULpMg1PjB', 'UzKWLN73UqeODKZY',
       'WIuWNXo0hvgsrHKE', '8PD4BJEqJFHw2rbu', 'z6x0ditYOINhGkQa', 'iRs79F5XiVXTwRDG', 'igc9NGbX7TxySgE7',
       'kmTSzSViSNlvqBed', '3mE0VMKpfLFWPfi8', 'fFOL6h5PdEXXwRwL', 'BgPq1IkG5VYBdLvh', '97HLEYa0DAWdlOYh',
       'fFgcTWt1zuo4jTFo', 'fdQmePwk7HXI1qna', 'PI2WhIrhwOgAlmDw', '3iuNNdh9U5PTIznw', 'za5UJBu7hGOMfvuD',
       'QrirP7kTMKG0lEYF', 'duQMFe4XANLKLXB1', 'JJcs4nmhXNMdgLIh', 'gHebYfN5kegdjrQe', 'mBhSe0OwczcBmmOx',
       'DU8zXJImxgyyi6wM', 'fS4jLqPGSe1gLMwd', 'p0f83qCBqop8aJ5J', '9IL3NL71GLmr4RpS', 'yxjfpabZzoeKW85T',
       'LqOXvSsZJCcNk0Xz', '9qkOpwBfub0g2een', 'FnodwsMkIlolyUXf', 'nPj3WnC7avQFdkip', 'jZ6bnxEZXFWknsUc',
       '2HMtKmyV4yU12Rpn', 'jzzYRjQjip8KaBA4', 'ZHPp3NMBy4gBBRX6', 'gozZkQCgT9svLuB2', 'rZB8MFFKWI0Cah84',
       'DjM0gisAD4JdEvCH', 'wRQG4HeVorp4jOuL', 'rei4K5WCTHrRwoor', 'kSAPyKm9ax51H4eG', 'U2wKhkrob00j3H6A',
       'Tsknel6ouVBqeQMC', 'h57L9t6LsSpkYrd9', 'hCApLGKgXlx0JVgg', 'xyVA7VFw2PT5W4wC', '9y7BRRN2AeuJNWoy',
       'TXWsM39IvakredFG', '8p2b4Jxy3EsMHkxp', 'NLSOjDftct9odyiT', 'laVPVarmBqrZ3mCD', 'WbgqOR6H4FGDjf12',
       'rwm629fkVwP1blQz', 'YOap52iGlN1CTfP1', 'BU0ro01mUGLb6j6q', 'DVBQ6dzyKUoN8Fm9', 'jKD9eTXFIAQFjCKx',
       'AFxH6OiNogFesPiJ', 'XZ0PqmgdMlpngSoM', 'GYT4RxYzHErXk1iE', '1IoGnnRKdDw9UwNc', 'nMn8MDTR1Kwe9MIc',
       'ixpLD8RsizewEGgE', 'AhgEGTwk3cYCd7IG', '81GYeM8I1xKM8WHI', 'b8gjpSvUNlr9zWJ3', 'MRZWXG3b5KOz371K',
       'oWtf1fYfzCfgdPAC', 'YptlFVyBtqBtAo1Y', 'HTt35AjxTaaqP6iT', 'vga0WKHfkr5yOrQR', 'fRQcSxGUse5iLuHn',
       'cTYd5umiFXvlrcUx', '0dZBoS6ct4fD6V63', 'ItocFHqIpkhHylsv', '8m72qAC9BQRHMDz7', 'Yk0SWLqc4h1797Yh',
       '7bMbjqqgcN1iqp2R', 'PckEKPBeiFLJD6CW', 'eInkOgSjE2z1OgFw', 'V0VzEiksWMYodpXb', 'mHq3gyDK7kz9FMJR',
       'lfqjpnylNx5gmkvT', 'snh9S9bZVhHIoWt5', 'HaXWYkthBGHT4Wr2', 'ppMWSf5ImCrEUlTC', 'FxKUA9kpnNgmjjZZ',
       's8HgRVYkGMtHwP9w', 'cUZ66eOTEOGGsY1k', 'EQOHyHF3DWmcQsNW', 'd8MGAosKYmrmtWPS', 'czh0pa2zBhB1wL4k',
       'sv8s6TjrXpb2OUjh', 'hMwZdzpBpdP4wZmV', 'zayNzLfGxRjER3Mc', '6vADK8Wl05NXGzWE', 'E5WXXvxomtiWrtjo',
       'UyCkHP3YyeOPR6Oy', 'ZI7IL2uXAZQVURJL', 'bJobeUxNkVoQJLqw', 'aQJ0SVIXSnVKzBpO', '8RBKAM6nGwS5WW8B',
       'HQj8Akngs55oD2HG', 'MXsCNmgm5MbZ8pb4', '2yHhOigF0yznhQIE', 'gURvV8S6apkeEJb7', '2SK079hU8XjY4lf6',
       'deKlD5zS3OrnUY9P', 'hvWCAGxJnzzg09x1', 'RHSPIbK3dXWaMlKW', '2I2tdRVAVNEj7ujp', 'mBCDf76pPSfJDpL4',
       '0FO0olR8kCR0V3zU', 'cEnexXgM2qHNKOGG', 'GbxjnEynvSQOTO2P', 'Sxumcu7xudUaQIIR', 'dd4WRtEMcrfmlEog',
       '8rePCCHk47iz22Qx', 'pmuvm9QwJQDArALa', '54O0GGmlG6xvmaET', 'Z8NPhuAW4FaD2GlK', '3Oh5UBjrZXYRNWUj',
       'Fqb7kO9cBtqdS6Ko', 'fbvpaUW9xWLbgBWL', 'DsL1KY9esR1a3qZs', 'Ksf5FdjryJVUAmly', 'B90VgaF0OkoSLOil',
       'Hzjh4sENZqe6NuWp', 'sTFOrb0VAj1lxO8E', 'tqCMeQALjGKZcses', 'wP7FdAEZUSThSZo5', 'wYfPHhcMZPomCTNp',
       'd5KtHsR6fGkY7ABL', 'mykfJIQGi0P5soBR', 'IKCf64oh1i6dk31F', 'pTABTmXYccFCuibX', 's58LKBc3mfrp7ItS',
       'drrJsYuwRGhvDil2', 'IDoVYlGwA20R6E0j', 'xhcqXS09c9oRab5d', 'jmszJqFPmQeYRJyv', '3HOvLmZRdARQJL3K',
       'm9lETT6aT3riFGbF', '5qEWSk9fkgcd74va', 'Bg2izkZwP5wLDuAr', '4PvjhzDXN4gCVc0d', '2RuVoFaDnRcK44Ci',
       'h5HxhbYQbmJ7kJuZ', 'epoUV3mEsRuLsBI5', 'XHFxR4Wtm7SntYsX', 'kT9xxJ8q6CC0RIe1', '1qC7Ekkn0wQdJJ92',
       '9YzfB23wSMCQ7OLu', 'C0ioViz2e3AUBOA6', 'MWVLNZs8X2lEcImo', 'E5rTj8eTMtmrK4Zf', 'hgVGpAUnGCuFy1Tv',
       'PuJzKs6BtiRBmVXG', 'qsfqmUZwIukJ87Oz', 'GD52BqAQ3UKMoTlr', 'CBFvUxsLFB1OInyz', 'GnjuKQw7x5FmR7jv',
       'T80iBmclISVvPh8c', 'et8XNsB0xrtJA5Hh', '4u81bnzmuDpuf83k', '7dnWGsgmOnaYeeyK', 'aZlzPEdtH9UkW7rR',
       'ld7L5rO3dCYpOsNi', 'S4BMsPYIeBJkk29e', 'T4r2h2CKnCSLCs0B', 'SEmSMp3cls4z1oUy', 'pYt65ezmdmbSzYpN',
       '9WFZSCAgEtHqsQxU', 'Pn13qFgcp44BZjjW', 'CV7YxAxeGL8uHOjU', 'V1yIyBjdVEpWfhGA', 'bYdD2gp7m7tpjy3T',
       'IG6EaVY0tm1mwRuM', 'w2ChpE6UDALS5pG9', 'PcSEqaDh9reFnkly', 'tL2sXbAHMUF8sjXI', 'mh1sVZyFIRPgdHeK',
       'k6lySjFExGalKnOo', 'BWhAouEab0Oyz3WH', 'aVkJaOlpEXEu1sA6', 'psdoNnM7Wy3dFnvT', 'OeMXRYnVEak7xaje',
       'qr6L3cx06S2mQwg8', 'glQXHvhDuuNHqwpG', 'Fq78fai5yrXRuQ2o', '4FdbhXS6VbQiJvTc', '2tD8A4kyPAm8iEeJ',
       'T6VJQu2MUSFSsNwn', 'Hu4Zr7fAQmaeNjps', 'LTFBZt3P9WcgfOqA', 'wAp2qTiqWEkRsZRe', 'I6FhAjcOlVyxLNhy',
       'J0McwGk9Ik5yfCxr', 'cxchEshsXvusOZag', '3Ix4H7CpLruwpv1s', 'ue8aCEpSJwfIbsmv', 'Gr5QZfj4yZ4SYmsm',
       '2eoiXKdLz7shE2cK', '7WLHrme0SMvwWc6H', 'BIIYQWlbwCKMyoZN', 'ErfOJHTdA09fd3n2', 'veAgcpuKKzipzKVb',
       '9xWu2496tOsY6pgH', 'B0ZoYNVRgdN92LT2', 'RkBrykODCpWJ8jXE', '99NtmZRBptP7Ypxx', 'fCIwD1F37qm1mGxc',
       '8xMfSrsDWTLkhdjc', '7n0bmPLQT5XgnPr2', 'SV8RJcF1tlCK7fqO', 'UeDJh1uZ0v95JKCD', 'xnmwnXIqqw354i51',
       'Wddn72AXrDZrT27e', 'l8baVF5yAl7Poiz0', 'yOUGoRvzIYtGgHNN', '8zTUeoCLaQFw7wRY', 'bP0BWwLiOr1YPsPF',
       'gViVBwCVwXn04LcJ', 'qDJat9CEAgxchvYv', 'Eq13N3emLIPPvQNJ', 'mNjatWaQ9Rwe043r', 'RM6ojbIIVnkD4qea',
       'd7UAKcKsGLV5CL3f', 'if3MtaPgSVCdrbpj', '8UPudbJ60QS7TQJg', 'szFHXg80ERz4jpb0', 'aaGmp1MPYAhB7BdT',
       '39jwZOryd8pSepSB', 'qNham7v8ZYDVll3F', '0oGAp0RA9KOzq5Jg', 'aVfPNCcfi3l7sOim', 'FZukd2BlmebwYcmK',
       '0M4LzGPLZpKR584g', 'kM9lqPwx5tfhc9bv', 'BwcbgQAJOXVmqM51', 'CA72eV9khuEIapEO', 'Cn4f6dSnmaGPh4yf',
       'cXsyz7d04ugVfumJ', 'XhvQEkUio3gfGPrc', 'L6VzE9XUfnWRRMog', 'f5OkfuzBqzTWweGW', 'JjOg3m8zSMo21JA0',
       '50Kth3CeCbkvZS31', 'hSX5Yd9IY5IbX4zy', 'oBKKV8ov8Q0CEH7K', 'iLiycMSn6KWtrUE0', 'MVsgEhtr0BlsLrg1',
       'O5i2GQtImtJiwBYt', 'ym69iFQD8R0nZCap', 'hCxY8V7Ex7ubufUh', 'MXB3ze25RztAtSWt', 'rwxqfOaqBodX2gJ2',
       '3jEMdQ6iZiLIGLRF', '8MoHJNFCV7EyaTA9', 'VOev1kuIuwgmA9Ng', '5QaYAt7bcVWtDT26', 'jdaRzlbnvdHNGDXw',
       '1REnVX6XziDJwG6E', 'hsx4Fk3pMUaxDZY1', 'HoiYLoK1OLPfoghg', 'uTmT7pdRtoeLLm99', 'RPkwN0LENpDiokWy',
       'd8ezPzOSXJ9uVOV8', 'H3aq8AiCBWGG55ua', 'oND8cTGsNu774LYo', 'hp7zzMFyZ6ltsKa0', '7t2Y6sx4qcxDszWf',
       '5ztAmDKrqwB91WJu', 'oA9UyG0P4KG2rA0c', 'aZUlehTxWlUFD0X4', 'KJay76T22RgKkKXj', 'TNuGNvhvj9t2K28z',
       'PTVeC0utF0HbSuE6', 'iHiiy6qZgSpjlrfB', 'ABCLJkrEbbTooGJW', 'LiEMdU99uEhQLxrk', 'NLgsYvcCIIIvkZkD',
       'ZWUrm4b7QFQ7R6qr', 'kTzHTiMd1KLCmTzp', 'gCIaPIE11HjwvM6H', 'yY7oLKRegTOdZsW3', 'qn2FqKfjTW7adG58',
       'GlRVBrPzlgRPOuBb', 'zrw3CoknymE1eCyB', '26I5QwklMWZ0BBs0', '9PmMNYnstLFLFgax', 'jLDJooGTii3Zx5uN',
       'tPvRc11cuwvvIBKm', 's0oPEsHjGgX1CZ9V', 'fBVhXpQGnBWpIb1M', 'SLOIqtoznyS8B5v9', 'EEJLSSbMvQ7ZTErF',
       'y9hvC70anRpUC6c8', 'y6NenInaZVFxByn0', 'vABov8HDPEWLEcgH', '8KWgmOJ1Ex0WgvkE', '3Wevylc1NNHtCT7U',
       '1St5AGXfF5fDSmAx', 'Mey3Yx4KqdFHAM6R', 'n539lSfp4TLVtwnV', 'EssjteBYIRndGNx1', 'KOlSFwaIOpcvPPhX',
       'FKZuxJm7YHBtCnaQ', '8u8UKDH1TMa6VsBA', 'NfgpSgrxnMswLHFI', 'IQGq4I3QPf8mYoC0', 'XcQ6jftKAqVFBzC7',
       'E2t1oD6VH8psPBp5', 'NKf06WqjUqxcGT7l', 'EcVL9CtMyjgrT3Pw', 'cx6OTyGXJhVKOBp3', 'xSXrOVJGPxGqPfj4',
       'ajFueo1BAlEAMvOS', 'F8yIeQADvXgKROXh', 'hDRTShUiJi0jl8sK', 'afK6DAmCyEGRxPcN', 'zTSeCSqcQ58S6zAJ',
       'bXbG05XWI4BcqVJ8', 't85zzTNtyXLfNx03', 'w7W4Uc7E1yq1Px5s', 'P1vE2O9r4HMakdMP', 'cy56x59lEI0XLqrI',
       'uBY4K4ffSkSQBhas', 'WyXKTRxUPwqYz9x2', 't66Zuefq28Tjlfsb', 'OSJfh0s745gYRvhp', 'jYw37AKBjmq8JCbg',
       'qLCunS1U3MSGtAgW', 'VNXg2MH4JhrMfcMw', 'WYnwYA1qwXNU2QdQ', 't3Y2gTz0IPRTPQWV', 'NtOfpzNGZbo9MaKQ',
       'pNJpDwL6hrjTjdA0', 'WVzNA0LcYbiMrv0K', 'Z8ne28l8mdUzZDaH', 'UjdywC2oN8fFSPQV', 'NAID5fDFgfg3i3mg',
       'TS6XHbxerZEGzQ46', 'IpzNPf8t7I8GBWmT', 'Ttjv4OIiX5nEBTBz', '9JGlPQabKXAgygYv', 'oqaaPBMQYMNjHwCQ',
       'cyXPFN9WpcM1RHA7', 'YMTbiT9WgqrO9Aww', 'FyU6SInqEsyAs1qf', 'HO0HqOAEbLoPpwEG', 'vqepN1AQOxaSrsF9',
       'm1UbvCtxPAqasAsS', 'OtfVOvG2da5cFnIA', 'OdNbThSyFuB6yF8w', 'bIcU0oWt0rgBT5bw', 'U4RIog4yhiLRhium',
       'M7EhYfMrz3RV79zH', 'o2qfXgIPKTkkUOt1', 'zx45c8Wi7WjLN3l4', 'R86vg9r2XhL6m0Qp', '59LthLQyAHeW54lR',
       'L5d5X4j0mw8SWd5l', 'vE9np0mpqi4vLx2h', 'KSROji1OvjlpxNVh', 'TrS1qRhKKvNk64uo', 'kryR9lUQZC9uyK1U',
       'YYGUgOprH9tEKHJG', 'wX7ZNVLTJwkFZPA1', 'qfPWn788EZrLJdqn', 'edrTR0GB0ea7cizN', 'cpLAUjI1PF9fkWdU',
       'j6Tm8TlHdXd8su3J', 'WF8BZ6vxDroK2Qla', 'judeoHiZYfNLywH1', 'kKeStuw4d1l0cpM9', 'tdPlHNd1rdSjc59l',
       'b9m9CT6mpSRAzWlx', '87yMWXl724EF6wbs', 'dKLhlv8jomFXY61v', 'GoiLw34hvBMzHiZN', 'S1ORA42nW3AB2D6W',
       'oj6k90yivT9ehlPX', 'E5u2POQTApvFTN8W', 'b2zpufVxDF7cKACq', 'hQmA25nUESbHc4w0', 'yfBwYPncishH8MJI',
       '8FS1mp7nueT4gHze', 'ipI3ltPMrtN1cwaG', '34WP6BBCo39seYdc', 'rTHtaxr831oU1urX', 'YbAwOeUnU5zcEFRQ',
       'isFvNzE39MlWMlpm', 'Ej0zyHtx9FiN6Vsj', 'sccrj5lksmuZZEos', '7uuwy6QFTO6B3pw6', '73nGlBSdoAuBWswu',
       'y86ZoJqjIYNVB3N0', 'NEpkjZhceWIrCmKE', 'NoFlBQqdghMOjcxg', '5oZc02uKTG3qLjMl', 'oedmFLH79o9T83r3',
       'pSDJfajaAnW3zVdS', 'NyyiD1UlqrT184M7', 'Rv2f7KOL3xr2az2M', 'UtrGWK6Q47pciWKz', 'V0ZIhUrWrpxWavbG',
       'rix93I4DHf31LTmz', 'dwkxbKdcz4dcxx9n', 'J42wy1JDV47JG9Yr', 'tw8EN2CYJVhk8hH3', 'Cm1Ptu7WOHTNBhp3',
       'CZ6yzeBoUSwwKn4I', 'yTwNfcoLHY77hIFk', 'dl4PfBGMjVpxb88v', '2HaEFkV6bMKK3KJq', 'KehdfY0omb0QcPmz')

--risk screening AAL from RMS winter storm, wildfire, severe convective storm, hurricane
select distinct business_id, ws_gross_loss, wf_gross_loss, cs_gross_loss, wt_gross_loss
from riskmgmt_svc_prod.rms_property_gross_loss
where business_id in ('D1Zm16qmJC02GRZp')

--Verisk building replacement cost Verisk 360 building cost
SELECT vpvr.business_id, vpv.rebuild_cost
FROM riskmgmt_svc_prod.verisk_property_valuation vpv
         join riskmgmt_svc_prod.verisk_property_valuation_request vpvr on vpvr.id = vpv.id
where vpv.creation_time > '2024-05-01'
  and vpvr.business_id in ('D1Zm16qmJC02GRZp')

--Allie's query to get TPM over time
--see https://next-insurance.slack.com/archives/C0762PZPDS6/p1717609834828339
with tpm_sr as (select created,
                       business_id,
                       factor,
                       id
                from nidl_loss_ratio_model_schedule_rating_prod.schedule_rating_factor_retrieval_new_output
                where silent_run = 'false'
                union all
                select created,
                       business_id,
                       factor,
                       id
                from nidl_loss_ratio_model_schedule_rating_prod.schedule_rating_factor_retrieval_renewal_output
                where silent_run = 'false')
   , tpm_declines as (select model_output_v2.created,
                             model_output_v2.business_id,
                             model_output_v2.predicted_losses,
                             model_output_v2.predicted_loss_ratio,
                             model_output_v2.decline,
                             model_output_v2.endpoint,
                             case
                                 when model_version like '%gl_v3%' then 'tpm 2.0 models'
                                 when model_version in ('models/gl_v2/pipeline_model_2023_05_25_gl_4_new.pkl',
                                                        'models/gl_v2/pipeline_model_2023_04_24_gl_4_renewal.pkl')
                                     then 'sprint 2023 models'
                                 when model_version like '%2023_01_29%' then 'jan 2023 models'
                                 when model_version like '%2022_09_02%' then 'sep 2022 models'
                                 when model_version is null then 'jul 2022 models'
                                 end as model_group,
                             gl_model_input.premium_before_ml,
                             model_output_v2.id
                      from nidl_loss_ratio_model_prod.model_output_v2
                               join nidl_loss_ratio_model_prod.gl_model_input on
                          gl_model_input.rating_result_id = model_output_v2.rating_result_id
                      where (model_output_v2.silent_run is null or model_output_v2.silent_run = 'false')
                        and model_output_v2.created > '2022-07-01'
                      union all
                      select model_output_v2.created,
                             model_output_v2.business_id,
                             model_output_v2.predicted_losses,
                             model_output_v2.predicted_loss_ratio,
                             model_output_v2.decline,
                             model_output_v2.endpoint,
                             case
                                 when model_version like '%gl_v3%' then 'tpm 2.0 models'
                                 when model_version in ('models/gl_v2/pipeline_model_2023_05_25_gl_4_new.pkl',
                                                        'models/gl_v2/pipeline_model_2023_04_24_gl_4_renewal.pkl')
                                     then 'sprint 2023 models'
                                 when model_version like '%2023_01_29%' then 'jan 2023 models'
                                 when model_version like '%2022_09_02%' then 'sep 2022 models'
                                 when model_version is null then 'jul 2022 models'
                                 end as model_group,
                             gl_model_input.premium_before_ml,
                             model_output_v2.id
                      from nidl_loss_ratio_model_prod.model_output_v2
                               join nidl_loss_ratio_model_prod.gl_model_input on
                          gl_model_input.business_id = model_output_v2.business_id and
                          gl_model_input.current_quote_time = model_output_v2.current_quote_time
                      where (model_output_v2.silent_run is null or model_output_v2.silent_run = 'false')
                        and model_output_v2.created > '2022-07-01')
   , tpm_mbe as (select
                     --distinct
                     revenue_model_output.created,
                     revenue_model_output.business_id,
                     revenue_model_output.current_quote_time,
                     revenue_model_output.increase_applied,
                     revenue_model_output.predicted_revenue,
                     case
                         when revenue_model_output.capped_revenue_exposure_increase_pct < 1
                             then 1 end                                                                as capped_revenue_exposure_increase_pct,
                     case
                         when revenue_model_output.capped_revenue_premium_increase_pct < 1
                             then 1 end                                                                as capped_revenue_premium_increase_pct,
                     gl_model_input.premium_before_ml,
                     gl_model_input.revenue_in_12_months,
                     gl_model_input.exposure_base_value,
                     revenue_model_output.est_yearly_premium,
                     revenue_model_output.endpoint,
                     revenue_model_output.id,
                     revenue_model_output.predicted_loss_ratio,
                     case
                         when model_version like '%gl_v3%' then 'tpm 2.0 models'
                         when model_version in ('models/gl_v2/pipeline_model_2023_05_25_gl_4_new.pkl',
                                                'models/gl_v2/pipeline_model_2023_04_24_gl_4_renewal.pkl')
                             then 'sprint 2023 models'
                         when model_version like '%2023_01_29%' then 'jan 2023 models'
                         when model_version like '%2022_09_02%' then 'sep 2022 models'
                         when model_version is null then 'jul 2022 models'
                         end                                                                           as model_group
                 from nidl_loss_ratio_model_prod.revenue_model_output
                          join nidl_loss_ratio_model_prod.gl_model_input on
                     revenue_model_output.rating_result_id = gl_model_input.rating_result_id
                 where (revenue_model_output.silent_run is null
                     or revenue_model_output.silent_run = 'false')
                   and increase_applied = 'true'
                 union all
                 select revenue_model_output.created,
                        revenue_model_output.business_id,
                        revenue_model_output.current_quote_time,
                        revenue_model_output.increase_applied,
                        revenue_model_output.predicted_revenue,
                        case
                            when revenue_model_output.capped_revenue_exposure_increase_pct < 1
                                then 1 end                                                                as capped_revenue_exposure_increase_pct,
                        case
                            when revenue_model_output.capped_revenue_premium_increase_pct < 1
                                then 1 end                                                                as capped_revenue_premium_increase_pct,
                        gl_model_input.premium_before_ml,
                        gl_model_input.revenue_in_12_months,
                        gl_model_input.exposure_base_value,
                        revenue_model_output.est_yearly_premium,
                        revenue_model_output.endpoint,
                        revenue_model_output.id,
                        revenue_model_output.predicted_loss_ratio,
                        case
                            when model_version like '%gl_v3%' then 'tpm 2.0 models'
                            when model_version in ('models/gl_v2/pipeline_model_2023_05_25_gl_4_new.pkl',
                                                   'models/gl_v2/pipeline_model_2023_04_24_gl_4_renewal.pkl')
                                then 'sprint 2023 models'
                            when model_version like '%2023_01_29%' then 'jan 2023 models'
                            when model_version like '%2022_09_02%' then 'sep 2022 models'
                            when model_version is null then 'jul 2022 models'
                            end                                                                           as model_group
                 from nidl_loss_ratio_model_prod.revenue_model_output
                          join nidl_loss_ratio_model_prod.gl_model_input on
                     revenue_model_output.business_id = gl_model_input.business_id and
                     revenue_model_output.created > gl_model_input.created and
                     revenue_model_output.created < gl_model_input.created + interval '10 seconds'
                 where (revenue_model_output.silent_run is null
                     or revenue_model_output.silent_run = 'false')
                   and increase_applied = 'true'
                 union all
                 select revenue_model_output.created,
                        revenue_model_output.business_id,
                        revenue_model_output.current_quote_time,
                        revenue_model_output.increase_applied,
                        revenue_model_output.predicted_revenue,
                        gl_model_input.revenue_in_12_months,
                        gl_model_input.exposure_base_value,
                        case
                            when revenue_model_output.capped_revenue_exposure_increase_pct < 1
                                then 1 end                                                                as capped_revenue_exposure_increase_pct,
                        case
                            when revenue_model_output.capped_revenue_premium_increase_pct < 1
                                then 1 end                                                                as capped_revenue_premium_increase_pct,
                        gl_model_input.premium_before_ml,
                        revenue_model_output.est_yearly_premium,
                        revenue_model_output.endpoint,
                        revenue_model_output.id,
                        revenue_model_output.predicted_loss_ratio,
                        case
                            when model_version like '%gl_v3%' then 'tpm 2.0 models'
                            when model_version in ('models/gl_v2/pipeline_model_2023_05_25_gl_4_new.pkl',
                                                   'models/gl_v2/pipeline_model_2023_04_24_gl_4_renewal.pkl')
                                then 'sprint 2023 models'
                            when model_version like '%2023_01_29%' then 'jan 2023 models'
                            when model_version like '%2022_09_02%' then 'sep 2022 models'
                            when model_version is null then 'jul 2022 models'
                            end                                                                           as model_group
                 from nidl_loss_ratio_model_prod.revenue_model_output
                          join nidl_loss_ratio_model_prod.gl_model_input on
                     revenue_model_output.previous_policy_id = gl_model_input.previous_policy_id
                 where (revenue_model_output.silent_run is null
                     or revenue_model_output.silent_run = 'false')
                   and increase_applied = 'true')

   , quotes as (select business_id,
                       creation_time,
                       offer_id,
                       highest_policy_id,
                       basic_yearly_premium,
                       highest_status_package,
                       case
                           when highest_status_package = 'basic' then basic_quote_job_id
                           when highest_status_package = 'basicTria' then basic_tria_quote_job_id
                           when highest_status_package = 'pro' then pro_quote_job_id
                           when highest_status_package = 'proTria' then pro_tria_quote_job_id
                           when highest_status_package = 'proPlus' then pro_plus_quote_job_id
                           when highest_status_package = 'proPlusTria' then pro_plus_tria_quote_job_id
                           end as quote_job_id,
                       case
                           when state in
                                ('DE', 'OR', 'GA', 'WA', 'AR', 'IA', 'OH', 'AZ', 'UT', 'NJ', 'NC', 'DC', 'MO', 'TX',
                                 'ID', 'MI', 'VA', 'FL', 'HI')
                               and creation_time > '2024-05-29 10:30:00' then 'TPM 2.0 Test'
                           when creation_time > '2024-05-29 10:30:00' then 'TPM 2.0 Control'
                           when state in
                                ('DE', 'OR', 'GA', 'WA', 'AR', 'IA', 'OH', 'AZ', 'UT', 'NJ', 'NC', 'DC', 'MO', 'TX',
                                 'ID', 'MI', 'VA', 'FL', 'HI')
                               and creation_time > '2024-05-15 10:30:00' then 'TPM 2.0 Variant Group Pre-Experiment'
                           when creation_time > '2024-05-15 10:30:00' then 'TPM 2.0 Control Group Pre-Experiment'
                           end as experiment_group
                from
                    -- not using gl_quotes because the timestamps are important and seem inconsistent in gl_quotes
                    dwh.quotes_policies_mlob_dec
                where creation_time > '2022-07-01'
                  and lob_policy = 'GL'
                  and offer_flow_type in ('APPLICATION', 'RENEWAL')
                  and new_reneweal <> 'cancel_rewrite')

   , model_version_avg_predicted_lr as (select case
                                                   when model_version like '%gl_v3%' then 'tpm 2.0 models'
                                                   when model_version in
                                                        ('models/gl_v2/pipeline_model_2023_05_25_gl_4_new.pkl',
                                                         'models/gl_v2/pipeline_model_2023_04_24_gl_4_renewal.pkl')
                                                       then 'sprint 2023 models'
                                                   when model_version like '%2023_01_29%' then 'jan 2023 models'
                                                   when model_version like '%2022_09_02%' then 'sep 2022 models'
                                                   when model_version is null then 'jul 2022 models'
                                                   end                   as model_group,
                                               avg(predicted_loss_ratio) as avg_pred_lr
                                        from nidl_loss_ratio_model_prod.model_output_v2
                                        group by 1)

   , mbe_quote_merging as (select quotes.offer_id,
                                  tpm_mbe.id                         as mbe_id,
                                  rank() over (partition by quotes.offer_id
                                      order by tpm_mbe.created desc) as rnk
                           from quotes
                                    join tpm_mbe on quotes.business_id = tpm_mbe.business_id
                               and quotes.creation_time > tpm_mbe.created
                               and quotes.creation_time < tpm_mbe.created + interval '6 weeks')

   , sr_quote_merging as (select quotes.offer_id,
                                 tpm_sr.id                         as sr_id,
                                 rank() over (partition by quotes.offer_id
                                     order by tpm_sr.created desc) as rnk
                          from quotes
                                   join tpm_sr on quotes.business_id = tpm_sr.business_id
                              and quotes.creation_time > tpm_sr.created
                              and quotes.creation_time < tpm_sr.created + interval '1 minute')

   , decline_quote_merging as (select quotes.offer_id,
                                      tpm_declines.id                         as decline_id,
                                      rank() over (partition by quotes.offer_id
                                          order by tpm_declines.created desc) as rnk
                               from quotes
                                        join tpm_declines on quotes.business_id = tpm_declines.business_id
                                   and quotes.creation_time > tpm_declines.created - interval '1 minute'
                                   and quotes.creation_time < tpm_declines.created + interval '1 minute')

   , exposure as (select job_id,
                         json_extract_path_text(calculation_summary, 'lob specific', 'exposure base value',
                                                true) as exposure_base_value
                  from s3_operational.rating_svc_prod_calculations rc
                  where data_points is not null
                    and data_points != ''
                    and lob = 'GL'
                    and job_id != 0
                    and job_id in (select quote_job_id from quotes))

select distinct quotes.creation_time,
                quotes.offer_id,
                quotes.business_id,
                quotes.highest_policy_id                                              as policy_id,
                quotes.basic_yearly_premium,
                mbe_quote_merging.mbe_id,
                sr_quote_merging.sr_id,
                decline_quote_merging.decline_id,
                tpm_mbe.revenue_in_12_months                                          as user_input_revenue,
                tpm_mbe.predicted_revenue                                             as mbe_predicted_revenue,
                tpm_mbe.premium_before_ml,
                tpm_mbe.premium_before_ml * nvl(tpm_mbe.capped_revenue_premium_increase_pct,
                                                capped_revenue_exposure_increase_pct) as estimated_post_mbe_basic_premium,
                tpm_mbe.premium_before_ml
                    * nvl(tpm_mbe.capped_revenue_premium_increase_pct, tpm_mbe.capped_revenue_exposure_increase_pct)
                    *
                (1 + (nvl(tpm_sr.factor, 0) / 100)::float)                            as estimated_post_tpm_basic_premium,
                exposure.exposure_base_value                                          as final_exposure_base_value,
                tpm_mbe.exposure_base_value                                           as pre_ml_exposure_base_value,
                tpm_mbe.exposure_base_value * capped_revenue_exposure_increase_pct    as estimated_post_mbe_exposure_base_value,
                tpm_mbe.exposure_base_value * capped_revenue_exposure_increase_pct    as estimated_post_mbe_exposure_base_value,
                tpm_sr.factor::float                                                  as tpm_schedule_rating_factor,
                nvl(tpm_mbe.predicted_loss_ratio, tpm_declines.predicted_loss_ratio)  as pre_ml_predicted_loss_ratio,
                nvl(tpm_mbe.predicted_loss_ratio, tpm_declines.predicted_loss_ratio)::float /
                nvl(exposure.exposure_base_value::float / tpm_mbe.exposure_base_value,
                    tpm_mbe.capped_revenue_premium_increase_pct,
                    tpm_mbe.capped_revenue_exposure_increase_pct, 1)::float /
                (1 + (nvl(tpm_sr.factor, 0) / 100)::float),
                tpm_declines.decline                                                  as tpm_decline,
                quotes.experiment_group,
                model_version_avg_predicted_lr.avg_pred_lr                            as model_grouop_avg_pred_lr,
                nvl(tpm_mbe.predicted_loss_ratio, tpm_declines.predicted_loss_ratio) /
                model_version_avg_predicted_lr.avg_pred_lr                            as risk_compared_to_avg_for_model_group
from quotes
         left join mbe_quote_merging
                   on mbe_quote_merging.offer_id = quotes.offer_id
                       and mbe_quote_merging.rnk = 1
         left join tpm_mbe on mbe_quote_merging.mbe_id = tpm_mbe.id
         left join sr_quote_merging
                   on sr_quote_merging.offer_id = quotes.offer_id
                       and sr_quote_merging.rnk = 1
         left join tpm_sr on sr_quote_merging.sr_id = tpm_sr.id
         left join decline_quote_merging
                   on decline_quote_merging.offer_id = quotes.offer_id
                       and decline_quote_merging.rnk = 1
         left join tpm_declines on decline_quote_merging.decline_id = tpm_declines.id
         left join exposure on exposure.job_id = quotes.quote_job_id
         left join model_version_avg_predicted_lr
                   on model_version_avg_predicted_lr.model_group = nvl(tpm_mbe.model_group, tpm_declines.model_group)

-- join in additional tables as needed on offer_id!

--FieldNation FN declines
select gaap.business_id,
       dwh.offer_id,
       dwh.lob,
       dwh.cob,
       gaap.affiliate_name,
       dwh.decline_reasons
from reporting.gaap_snapshots_asl gaap
         inner join dwh.underwriting_quotes_data dwh on gaap.business_id = dwh.business_id and
                                                        gaap.affiliate_name = 'LegalZoom' and
                                                        dwh.decline_reasons <> '[]'
group by 1, 2, 3, 4, 5, 6

select business_id,
       cob,
       policy_status_name,
       decline_reasons
from dwh.underwriting_quotes_data
where affiliate_id = 59161
  and start_date >= '2024-01-01'
  and execution_status = 'DECLINE'
order by 3 desc

--hbb by category
select --case when agent_id <> 'N/A' then 'Agent' else 'Direct / Partnerships' end channel,
       cob_group,
       --case when cob = 'E-Commerce' then 'e-comm' else 'not_e-comm' end e_comm_group,
       json_extract_path_text(json_args, 'lob_app_json', 'business_is_located_in', true) as location_type,
       count(distinct business_id)
from dwh.quotes_policies_mlob
where creation_time >= '2023-01-01'
  and highest_policy_status = 4
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and lob_policy = 'CP'
  and cob_group in ('Retail', 'Professional Services')
  and
  --channel <> 'Agent' and
    location_type <> ''
group by 1, 2
order by 1, 2, 3 desc

--caterer
select distinct revenue_in_12_months, count(distinct business_id)
from dwh.quotes_policies_mlob
where highest_policy_status in (4, 7)
  and lob_policy = 'GL'
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and creation_time >= '2023-01-01'
  and cob = 'Caterer'
group by 1
order by 1 asc

--caterer 2
select json_extract_path_text(qpm.json_args, 'lob_app_json', 'liquor_sales_yes_no', true)   as liquor_yes_no,
       --json_extract_path_text(qpm.json_args, 'lob_app_json','liquor_risk_byob_alcohol',true) as liquor_byob,
       json_extract_path_text(qpm.json_args, 'lob_app_json', 'liquor_sales_exposure', true) as liquor_pct,
       count(distinct business_id)
from dwh.quotes_policies_mlob qpm
where liquor_yes_no = 'Yes'
  and
  --liquor_byob = 'Yes' and
    highest_policy_status in (4, 7)
  and lob_policy = 'GL'
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and creation_time >= '2023-01-01'
  and cob = 'Restaurant'
group by 1, 2
order by 3 asc

--cp closures
select highest_status_name, count(distinct business_id), sum(highest_yearly_premium)
from dwh.quotes_policies_mlob
where highest_policy_status > 3
  and lob_policy = 'CP'
  and offer_flow_type in ('APPLICATION')
  and creation_time >= '2024-01-01'
  and cob in ('Aerial Photography', 'Auction House', 'Baby Gear and Furniture Store', 'Crisis Pregnancy Centers',
              'Dialysis Clinics', 'Epidemiologists', 'Faith-based Crisis Pregnancy Centers', 'Hospice',
              'Immunodermatologists', 'Infectious Disease Specialists', 'Nephrologists', 'Neuropathologists',
              'Pain Management', 'Pathologists', 'Waterproofing', 'Welding, Cutting and Metal Frame Erection',
              'Real Estate Investor', 'Shared Office Spaces', 'Makerspaces', 'Shopping Center')
group by 1
order by 3 desc

--to view all policy coverage and limit options for a given offer ID
select distinct offer_id
from underwriting_svc_prod.policy_coverages_and_options o
where --o.offer_id = '0XSd6i8HCotnmavM'
    policy_coverages_options like '%CYBER%'
  and creation_time >= '2024-06-10'
limit 100

--distinct pro services customers
select distinct business_id, cob, lob_policy, distribution_channel
from dwh.quotes_policies_mlob
where highest_policy_status > 3
  and offer_flow_type in ('APPLICATION')
  and creation_time >= '2025-01-01'
  and cob_group = 'Professional Services'

--distinct pro services customers by lob
SELECT business_id,
       cob,
       distribution_channel,
       agent_id,
       affiliate_id,
       (MAX(CASE WHEN lob_policy = 'PL' THEN 1 ELSE 0 END) = 1) AS has_PL,
       (MAX(CASE WHEN lob_policy = 'GL' THEN 1 ELSE 0 END) = 1) AS has_GL,
       (MAX(CASE WHEN lob_policy = 'BP' THEN 1 ELSE 0 END) = 1) AS has_BP,
       (MAX(CASE WHEN lob_policy = 'CP' THEN 1 ELSE 0 END) = 1) AS has_CP,
       (MAX(CASE WHEN lob_policy = 'WC' THEN 1 ELSE 0 END) = 1) AS has_WC
FROM dwh.quotes_policies_mlob
WHERE highest_policy_status > 3
  AND offer_flow_type IN ('APPLICATION')
  AND creation_time >= '2025-01-01'
  AND cob_group = 'Professional Services'
GROUP BY 1, 2, 3, 4, 5

--algolia query
SELECT tracking_id,
       funnelphase,
       cob_name,
       json_extract_path_text(interaction_data_raw, 'data', true) as search_string
FROM dwh.all_activities_table t
where eventtime >= '2025-04-01'
  and funnelphase = 'Search Cob Lead Form'
  and cob_name in
      ('Insurance Agent', 'Property Manager', 'Business Consulting Services', 'Accountant', 'Technology Services',
       'Engineer', 'Home Inspectors', 'Real Estate Agent', 'Marketing', 'Interior Designer', 'Architect',
       'Real Estate Brokers', 'Legal Service', 'Claims Adjuster', 'Financial Adviser', 'Administrative Support',
       'Notary', 'Product Designer', 'Building Inspector', 'Land Surveyor', 'Mortgage Broker', 'Insurance Inspector',
       'Business Financing', 'Occupational Health and Safety Specialists',
       'Environmental Science and Protection Technicians, Including Health', 'Telemarketing and Telesales Services',
       'Environmental Scientists and Specialists, Including Health', 'Real Estate Appraisal',
       'Securities, Commodities, and Financial Services Sales Agents', 'Insurance Appraisers',
       'Urban and Regional Planners', 'Loan Officers', 'Title Loans', 'Debt Relief Services',
       'Credit Authorizers, Checkers, and Clerks', 'Video Transfer Services', 'Actuarial Service',
       'Financial Examiners', 'Check Cashing and Pay day Loans', 'Law Office and Legal Services')
--cob_name in ('Restaurant','Food Truck','Caterer','Bakery','Coffee Shop','Bar') and
--cob in ('E-Commerce','Retail Stores','Grocery Store','Clothing Store','Electronics Store','Florist','Jewelry Store','Sporting Goods Retailer','Tailors, Dressmakers, and Custom Sewers','Nurseries and Gardening Shop','Candle Store','Pet Stores','Paint Stores','Flea Markets','Arts and Crafts Store','Eyewear and Optician Store','Hardware Store','Discount Store','Pawn Shop','Hobby Shop','Beach Equipment Rentals','Furniture Rental','Packing Supplies Store','Horse Equipment Shop','Demonstrators and Product Promoters','Fabric Store','Lighting Store','Luggage Store','Bike Rentals','Bike Shop','Bookstore','Home and Garden Retailer','Newspaper and Magazine Store','Department Stores','Furniture Store','Wholesalers')
--full list: --cob in ('Insurance Agent','Property Manager','Business Consulting Services','Accountant','Technology Services','Engineer','Home Inspectors','Real Estate Agent','Marketing','Interior Designer','Architect','Real Estate Brokers','Legal Service','Claims Adjuster','Financial Adviser','Administrative Support','Notary','Product Designer','Building Inspector','Land Surveyor','Mortgage Broker','Insurance Inspector','Business Financing','Occupational Health and Safety Specialists','Environmental Science and Protection Technicians, Including Health','Telemarketing and Telesales Services','Environmental Scientists and Specialists, Including Health','Real Estate Appraisal','Securities, Commodities, and Financial Services Sales Agents','Insurance Appraisers','Urban and Regional Planners','Loan Officers','Title Loans','Debt Relief Services','Credit Authorizers, Checkers, and Clerks','Video Transfer Services','Actuarial Service','Financial Examiners','Check Cashing and Pay day Loans', 'Law Office and Legal Services', 'Restaurant','Food Truck','Caterer','Bakery','Coffee Shop','Bar', 'E-Commerce','Retail Stores','Grocery Store','Clothing Store','Electronics Store','Florist','Jewelry Store','Sporting Goods Retailer','Tailors, Dressmakers, and Custom Sewers','Nurseries and Gardening Shop','Candle Store','Pet Stores','Paint Stores','Flea Markets','Arts and Crafts Store','Eyewear and Optician Store','Hardware Store','Discount Store','Pawn Shop','Hobby Shop','Beach Equipment Rentals','Furniture Rental','Packing Supplies Store','Horse Equipment Shop','Demonstrators and Product Promoters','Fabric Store','Lighting Store','Luggage Store','Bike Rentals','Bike Shop','Bookstore','Home and Garden Retailer','Newspaper and Magazine Store','Department Stores','Furniture Store','Wholesalers')

--full policy list with premiums
WITH policy_changes_filtered AS ( -- adding post-purchase policy change premium to initial written
    SELECT policy_id,
           SUM(yearly_amount_diff) AS total_change
    FROM nimi_svc_prod.policy_changes
    WHERE change_status_id = 2 -- change status is approved
    GROUP BY policy_id),

     latest_policy_list AS (WITH policy_list AS (SELECT *
                                                 FROM (SELECT qpm.highest_policy_id,
                                                              qpm.policy_start_date,
                                                              qpm.policy_end_date,
                                                              qpm.state,
                                                              qpm.cob,
                                                              ROW_NUMBER() OVER (
                                                                  PARTITION BY highest_policy_id
                                                                  ORDER BY policy_end_date DESC NULLS LAST
                                                                  ) AS rn
                                                       FROM dwh.quotes_policies_mlob qpm
                                                       WHERE lob_policy = 'GL'
                                                         and qpm.policy_start_date > '2022-03-31'
                                                         and qpm.policy_start_date <= '2023-03-31'
                                                         and cob in ('Insurance Agent', 'Property Manager',
                                                                     'Business Consulting Services', 'Accountant',
                                                                     'Technology Services', 'Engineer',
                                                                     'Home Inspectors', 'Real Estate Agent',
                                                                     'Marketing', 'Interior Designer', 'Architect',
                                                                     'Real Estate Brokers', 'Legal Service',
                                                                     'Claims Adjuster', 'Financial Adviser',
                                                                     'Administrative Support', 'Notary',
                                                                     'Product Designer', 'Building Inspector',
                                                                     'Land Surveyor', 'Mortgage Broker',
                                                                     'Insurance Inspector', 'Business Financing',
                                                                     'Occupational Health and Safety Specialists',
                                                                     'Environmental Science and Protection Technicians, Including Health',
                                                                     'Telemarketing and Telesales Services',
                                                                     'Environmental Scientists and Specialists, Including Health',
                                                                     'Real Estate Appraisal',
                                                                     'Securities, Commodities, and Financial Services Sales Agents',
                                                                     'Insurance Appraisers',
                                                                     'Urban and Regional Planners', 'Loan Officers',
                                                                     'Title Loans', 'Debt Relief Services',
                                                                     'Credit Authorizers, Checkers, and Clerks',
                                                                     'Video Transfer Services', 'Actuarial Service',
                                                                     'Financial Examiners',
                                                                     'Check Cashing and Pay day Loans',
                                                                     'Law Office and Legal Services', 'Restaurant',
                                                                     'Food Truck', 'Caterer', 'Bakery', 'Coffee Shop',
                                                                     'Bar', 'E-Commerce', 'Retail Stores',
                                                                     'Grocery Store', 'Clothing Store',
                                                                     'Electronics Store', 'Florist', 'Jewelry Store',
                                                                     'Sporting Goods Retailer',
                                                                     'Tailors, Dressmakers, and Custom Sewers',
                                                                     'Nurseries and Gardening Shop', 'Candle Store',
                                                                     'Pet Stores', 'Paint Stores', 'Flea Markets',
                                                                     'Arts and Crafts Store',
                                                                     'Eyewear and Optician Store', 'Hardware Store',
                                                                     'Discount Store', 'Pawn Shop', 'Hobby Shop',
                                                                     'Beach Equipment Rentals', 'Furniture Rental',
                                                                     'Packing Supplies Store', 'Horse Equipment Shop',
                                                                     'Demonstrators and Product Promoters',
                                                                     'Fabric Store', 'Lighting Store', 'Luggage Store',
                                                                     'Bike Rentals', 'Bike Shop', 'Bookstore',
                                                                     'Home and Garden Retailer',
                                                                     'Newspaper and Magazine Store',
                                                                     'Department Stores', 'Furniture Store',
                                                                     'Wholesalers')) t
                                                 WHERE rn = 1)
                            SELECT lp.highest_policy_id,
                                   p.policy_reference,
                                   lp.policy_start_date,
                                   lp.policy_end_date,
                                   lp.state,
                                   lp.cob,
                                   p.yearly_premium
                            FROM sl_prod_dwh.fact_written_premium t
                                     JOIN policy_list lp ON t.policy_id = lp.highest_policy_id
                                     JOIN nimi_svc_prod.policies p ON t.policy_id = p.policy_id
                            WHERE lp.rn = 1),

     policy_data AS (SELECT p.policy_id,
                            p.policy_reference,
                            p.business_id,
                            lpl.state,
                            lpl.cob,
                            p.start_date                                    AS policy_start_date,
                            p.end_date                                      AS policy_end_date,
                            p.yearly_premium,
                            pc.total_change,
                            p.yearly_premium + COALESCE(pc.total_change, 0) AS total_premium
                     FROM nimi_svc_prod.policies p
                              LEFT JOIN policy_changes_filtered pc ON p.policy_id = pc.policy_id
                              INNER JOIN latest_policy_list lpl ON p.policy_id = lpl.highest_policy_id
                     WHERE p.policy_type_id = 1 -- GL policies
                       AND p.start_date > DATE '2022-03-31')

SELECT policy_id,
       policy_reference,
       business_id,
       state,
       cob,
       policy_start_date       AS start_date,
       policy_end_date         AS end_date,
       ROUND(total_premium, 2) AS total_premium
FROM policy_data
ORDER BY policy_id;

--gl sectors claims
with latest_policy as (select *
                       from (select qpm.*,
                                    row_number()
                                    over (partition by highest_policy_id order by policy_end_date desc nulls last) as rn
                             from dwh.quotes_policies_mlob qpm) t
                       where rn = 1)
select t.claim_number,
       t.policy_reference,
       t.policy_id,
       lp.policy_start_date,
       lp.policy_end_date,
       lp.state                                                                  as policy_state,
       lp.cob,
       t.claim_date_of_loss,
       t.date_submitted,
       t.state_of_loss,
       t.loss_paid_total,
       (t.expense_ao_paid_total + t.expense_dcc_paid_total)                      as alae_paid_total,
       t.loss_reserve_total,
       (t.expense_ao_reserve_total + t.expense_dcc_reserve_total)                as alae_reserve_total,
       (t.recovery_salvage_reserve_total + t.recovery_subrogation_reserve_total) as salvage_subro_total,
       t.incurred_total
from sl_prod_dwh.dim_claim t
         join
     latest_policy lp
     on t.policy_id = lp.highest_policy_id
where t.lob = 'GL'
  and t.is_current = 1
  and t.incurred_total > 0
  and lp.policy_start_date > '2022-03-31'
  and lp.policy_start_date <= '2024-03-31'
  and lp.cob in
      ('Insurance Agent', 'Property Manager', 'Business Consulting Services', 'Accountant', 'Technology Services',
       'Engineer', 'Home Inspectors', 'Real Estate Agent', 'Marketing', 'Interior Designer', 'Architect',
       'Real Estate Brokers', 'Legal Service', 'Claims Adjuster', 'Financial Adviser', 'Administrative Support',
       'Notary', 'Product Designer', 'Building Inspector', 'Land Surveyor', 'Mortgage Broker', 'Insurance Inspector',
       'Business Financing', 'Occupational Health and Safety Specialists',
       'Environmental Science and Protection Technicians, Including Health', 'Telemarketing and Telesales Services',
       'Environmental Scientists and Specialists, Including Health', 'Real Estate Appraisal',
       'Securities, Commodities, and Financial Services Sales Agents', 'Insurance Appraisers',
       'Urban and Regional Planners', 'Loan Officers', 'Title Loans', 'Debt Relief Services',
       'Credit Authorizers, Checkers, and Clerks', 'Video Transfer Services', 'Actuarial Service',
       'Financial Examiners', 'Check Cashing and Pay day Loans', 'Law Office and Legal Services', 'Restaurant',
       'Food Truck', 'Caterer', 'Bakery', 'Coffee Shop', 'Bar', 'E-Commerce', 'Retail Stores', 'Grocery Store',
       'Clothing Store', 'Electronics Store', 'Florist', 'Jewelry Store', 'Sporting Goods Retailer',
       'Tailors, Dressmakers, and Custom Sewers', 'Nurseries and Gardening Shop', 'Candle Store', 'Pet Stores',
       'Paint Stores', 'Flea Markets', 'Arts and Crafts Store', 'Eyewear and Optician Store', 'Hardware Store',
       'Discount Store', 'Pawn Shop', 'Hobby Shop', 'Beach Equipment Rentals', 'Furniture Rental',
       'Packing Supplies Store', 'Horse Equipment Shop', 'Demonstrators and Product Promoters', 'Fabric Store',
       'Lighting Store', 'Luggage Store', 'Bike Rentals', 'Bike Shop', 'Bookstore', 'Home and Garden Retailer',
       'Newspaper and Magazine Store', 'Department Stores', 'Furniture Store', 'Wholesalers')

--BDL business description for consultants
WITH ranked_data AS (SELECT bad.lob_application_id,
                            la.opportunity_id,
                            p.business_id,
                            bad.creation_time,
                            bad.data_point_value,
                            la.answers,
                            o.lead_answers,
--      qpm.purchase_date,
--		qpm.cob,
--		qpm.cob_group,
--		qpm.json_args,
--		qpm.highest_status_name,
--		qpm.highest_policy_reference,
--      qpm.distribution_channel
                            ROW_NUMBER() OVER (PARTITION BY la.opportunity_id ORDER BY bad.creation_time DESC) AS rn
                     FROM underwriting_svc_prod.bi_applications_data bad
                              JOIN underwriting_svc_prod.lob_applications la
                                   ON la.lob_application_id = bad.lob_application_id
                              JOIN underwriting_svc_prod.prospects p on p.prospect_id = la.prospect_id
                              JOIN underwriting_svc_prod.opportunities o on o.opportunity_id = la.opportunity_id
                              LEFT JOIN dwh.quotes_policies_mlob qpm on qpm.business_id = p.business_id
                     WHERE data_point_id = 'business_description_of_operations'
                       and qpm.lob_policy = 'PL')
SELECT r.*
FROM ranked_data r
WHERE rn = 1;

--datapoint source, express tag
select policy_reference,
       business_id,
       commission_channel,
       flow_type,
       written_premium,
       (num_fallback_application_data_points + num_third_party_application_data_points) * 100.0 /
       num_application_data_points as pct_default_or_third_party
from sl_prod_dwh.dim_policy
where policy_reference = 'NXTLKRPDKJ-00-CP'

SELECT distinct business_id,
                policy_reference,
                policy_bind_date,
                num_customer_application_data_points,
                num_internal_application_data_points,
                num_third_party_application_data_points,
                num_fallback_application_data_points,
                num_application_data_points,
                CASE
                    WHEN num_application_data_points IS NOT NULL
                        AND num_application_data_points != 0
                        AND ((num_fallback_application_data_points + num_third_party_application_data_points) * 100.0 /
                             num_application_data_points) >= 50
                        AND package IN ('GL', 'CP')
                        THEN 'Express'
                    ELSE 'Non-Express'
                    END AS food_and_bev_express_flag
FROM sl_prod_dwh.dim_policy
where policy_bind_date >= '2025-05-11'
  and num_application_data_points IS NOT NULL
--policy_reference in ('NXTLKRPDKJ-00-CP', 'NXTJH99JFH-00-GL')

--get top pro services classes
select cob, sum(highest_yearly_premium) as premium, count(distinct (business_id)) as businesses
from dwh.quotes_policies_mlob
where --distribution_channel <> 'agents' and
    cob in ('Insurance Agent', 'Property Manager', 'Business Consulting Services', 'Accountant', 'Technology Services',
            'Engineer', 'Home Inspectors', 'Real Estate Agent', 'Marketing', 'Interior Designer', 'Architect',
            'Real Estate Brokers', 'Legal Service', 'Claims Adjuster', 'Financial Adviser', 'Administrative Support',
            'Notary', 'Product Designer', 'Building Inspector', 'Land Surveyor', 'Mortgage Broker',
            'Insurance Inspector', 'Business Financing', 'Occupational Health and Safety Specialists',
            'Environmental Science and Protection Technicians, Including Health',
            'Telemarketing and Telesales Services', 'Environmental Scientists and Specialists, Including Health',
            'Real Estate Appraisal', 'Securities, Commodities, and Financial Services Sales Agents',
            'Insurance Appraisers', 'Urban and Regional Planners', 'Loan Officers', 'Title Loans',
            'Debt Relief Services', 'Credit Authorizers, Checkers, and Clerks', 'Video Transfer Services',
            'Actuarial Service', 'Financial Examiners', 'Check Cashing and Pay day Loans')
  and highest_policy_status >= 3
  and creation_time >= '2025-01-01'
  and offer_flow_type in ('APPLICATION')
group by 1
order by 2 desc

-- get top pro services classes with agent-channel breakdown
select cob,
       sum(highest_yearly_premium)                    as premium,
       count(distinct business_id)                    as businesses,

       -- agents-specific aggregates
       (select sum(qp2.highest_yearly_premium)
        from dwh.quotes_policies_mlob qp2
        where qp2.cob = qp.cob
          and qp2.distribution_channel = 'agents'
          and qp2.highest_policy_status >= 3
          and qp2.creation_time >= '2025-01-01'
          and qp2.offer_flow_type in ('APPLICATION')) as agents_premium,

       (select count(distinct qp2.business_id)
        from dwh.quotes_policies_mlob qp2
        where qp2.cob = qp.cob
          and qp2.distribution_channel = 'agents'
          and qp2.highest_policy_status >= 3
          and qp2.creation_time >= '2025-01-01'
          and qp2.offer_flow_type in ('APPLICATION')) as agents_businesses

from dwh.quotes_policies_mlob qp
where cob in (
              'Insurance Agent', 'Property Manager', 'Business Consulting Services', 'Accountant',
              'Technology Services', 'Engineer', 'Home Inspectors',
              'Real Estate Agent', 'Marketing', 'Interior Designer', 'Architect', 'Real Estate Brokers',
              'Legal Service', 'Claims Adjuster', 'Financial Adviser',
              'Administrative Support', 'Notary', 'Product Designer', 'Building Inspector', 'Land Surveyor',
              'Mortgage Broker', 'Insurance Inspector',
              'Business Financing', 'Occupational Health and Safety Specialists',
              'Environmental Science and Protection Technicians, Including Health',
              'Telemarketing and Telesales Services', 'Environmental Scientists and Specialists, Including Health',
              'Real Estate Appraisal',
              'Securities, Commodities, and Financial Services Sales Agents', 'Insurance Appraisers',
              'Urban and Regional Planners', 'Loan Officers',
              'Title Loans', 'Debt Relief Services', 'Credit Authorizers, Checkers, and Clerks',
              'Video Transfer Services', 'Actuarial Service',
              'Financial Examiners', 'Check Cashing and Pay day Loans'
    )
  and highest_policy_status >= 3
  and creation_time >= '2025-01-01'
  and offer_flow_type in ('APPLICATION')
group by cob
order by premium desc;

--silver IDG and vendor data query
--see table here with vendors: https://docs.google.com/document/d/1vuXFOJGJ1GHUu3SmkgyuzoHtzQQICctk2ME0bYBwnAg/edit?tab=t.0
SELECT request.id
     , request.provider                                            as vendor
     , request.operation                                           as product
     , context_id.key                                              as context_key
     , context_id.value                                            as context_key_value
     , request.event_occurrence_time_pst_timestamp_formatted       as request_ts
     , request.event_occurrence_time_pst_timestamp_formatted::date as request_dt
     , case when response.id is not null then 1 else 0 end         as dim_responsed
     , response.body                                               as response_value
     , round(response_time_in_millis * 1.0 / 1000, 2)              as response_time_secs
     , response.event_occurrence_time_pst_timestamp_formatted      as response_ts
FROM silver_insurance_data_gateway.third_party_request_v1 request
         left join silver_insurance_data_gateway.consumer_context_v1 context_id
                   on request.service_request_scope_id = context_id.service_request_scope_id
         left join silver_insurance_data_gateway.third_party_response_v1 response
                   on request.id = response.id
where true
  and request.provider = 'Dunn_And_Bradstreet'
  and request.operation = 'Fetch Company Data Blocks'
  and context_key = 'offer_id'

--zesty roof data
SELECT creation_time
     , business_id
     , url
     , input_location
     , matched_location
     , geocode_confidence
     , geocoded_coordinates
     , street_address
     , state
     , zip

     , image_date.date::DATE                             as image_date

     , lot_size::INT
     , property_debris::VARCHAR
     , distance_to_coast::FLOAT
     , secondary_structures::VARCHAR
     , distance_to_body_water::FLOAT
     , closest_water_body_type::VARCHAR
     , distance_to_fire_station::FLOAT
     , number_of_buildings_on_property::INT
     , deck::VARCHAR
     , slope::FLOAT
     , aspect::FLOAT
     , trampoline::VARCHAR
     , zoning_type::VARCHAR
     , swimming_pool::VARCHAR
     , fema_flood_zone::VARCHAR

     , roof_area.value::INT                              as roof_value
     , skylights.value::VARCHAR                          as skylights
     , roof_shape.value::VARCHAR                         as roof_shape
     , year_built.value::INT                             as year_built
     , mobile_home.value::VARCHAR                        as mobile_home
     , roof_quality.value::VARCHAR                       as roof_quality
     , solar_panels.value::VARCHAR                       as solar_panels
     , roof_material.value::VARCHAR                      as roof_material
     , eave_height_avg.value::FLOAT                      as eave_height_avg
     , eave_height_max.value::FLOAT                      as eave_height_max
     , eave_height_min.value::FLOAT                      as eave_height_min
     , solar_panel_area.value::INT                       as solar_panel_area
     , count_roof_pen_all.value::INT                     as count_roof_pen_all
     , kitchen_permit_age.value::INT                     as kitchen_permit_age
     , roof_facets_number.value::INT                     as roof_facets_number
     , roof_pitch_average.value::FLOAT                   as roof_pitch_average
     , roof_pitch_maximum.value::FLOAT                   as roof_pitch_maximum
     , bathroom_permit_age.value::INT                    as bathroom_permit_age
     , roof_pitch_dominant.value::FLOAT                  as roof_pitch_dominant
     , kitchen_permit_count.value::INT                   as kitchen_permit_count
     , roof_quality_reasons.value::VARCHAR               as roof_quality_reasons
     , bathroom_permit_count.value::INT                  as bathroom_permit_count
     , roof_pen_count_chimney.value::INT                 as roof_pen_count_chimney
     , building_height_average.value::FLOAT              as building_height_average
     , roof_pen_count_box_vent.value::INT                as roof_pen_count_box_vent
     , building_area_2d_computed.value::INT              as building_area_2d_computed
     , building_area_3d_computed.value::INT              as building_area_3d_computed
     , number_of_stories_computed.value::INT             as number_of_stories_computed
     , distance_to_nearest_building.value::INT           as distance_to_nearest_building
     , building_density_zone_1_percent.value::INT        as building_density_zone_1_percent
     , vegetation_density_zone_1_percent.value::INT      as vegetation_density_zone_1_percent
     , overhanging_vegetation_density_percent.value::INT as overhanging_vegetation_density_percent

FROM (SELECT a.creation_time
           , coalesce(json_extract_path_text(b.additional_data, 'business_id'),
                      json_extract_path_text(b.additional_data, 'businessId'))                                       AS business_id
           , json_extract_path_text(a.response_data, 'meta', 'zview_urls',
                                    'zproperty')                                                                     as url
           , json_extract_path_text(a.response_data, 'meta', 'input_location')                                       as input_location
           , json_extract_path_text(a.response_data, 'meta', 'matched_location')                                     as matched_location
           , json_extract_path_text(a.response_data, 'meta', 'geocode_confidence')                                   as geocode_confidence
           , json_extract_path_text(a.response_data, 'meta', 'matched_location_parts',
                                    'street_address')                                                                as street_address
           , json_extract_path_text(a.response_data, 'meta', 'matched_location_parts',
                                    'state')                                                                         as state
           , json_extract_path_text(a.response_data, 'meta', 'matched_location_parts',
                                    'postal_code')                                                                   as zip
           , json_extract_path_text(a.response_data, 'meta',
                                    'geocoded_coordinates')                                                          as geocoded_coordinates

           , json_parse(json_extract_path_text(a.response_data, 'assessment', 'property', 'images',
                                               '0'))::SUPER                                                          as image_date

           , json_extract_path_text(a.response_data, 'assessment', 'property', 'features', 'lot_size',
                                    'value')                                                                         as lot_size
           , json_extract_path_text(a.response_data, 'assessment', 'property', 'features', 'property_debris',
                                    'value')                                                                         as property_debris
           , json_extract_path_text(a.response_data, 'assessment', 'property', 'features', 'distance_to_coast',
                                    'value')                                                                         as distance_to_coast
           , json_extract_path_text(a.response_data, 'assessment', 'property', 'features', 'secondary_structures',
                                    'value')                                                                         as secondary_structures
           , json_extract_path_text(a.response_data, 'assessment', 'property', 'features', 'distance_to_body_water',
                                    'value')                                                                         as distance_to_body_water
           , json_extract_path_text(a.response_data, 'assessment', 'property', 'features', 'closest_water_body_type',
                                    'value')                                                                         as closest_water_body_type
           , json_extract_path_text(a.response_data, 'assessment', 'property', 'features', 'distance_to_fire_station',
                                    'value')                                                                         as distance_to_fire_station
           , json_extract_path_text(a.response_data, 'assessment', 'property', 'features',
                                    'number_of_buildings_on_property',
                                    'value')                                                                         as number_of_buildings_on_property
           , json_extract_path_text(a.response_data, 'assessment', 'property', 'features', 'deck',
                                    'value')                                                                         as deck
           , json_extract_path_text(a.response_data, 'assessment', 'property', 'features', 'slope',
                                    'value')                                                                         as slope
           , json_extract_path_text(a.response_data, 'assessment', 'property', 'features', 'aspect',
                                    'value')                                                                         as aspect
           , json_extract_path_text(a.response_data, 'assessment', 'property', 'features', 'trampoline',
                                    'value')                                                                         as trampoline
           , json_extract_path_text(a.response_data, 'assessment', 'property', 'features', 'zoning_type',
                                    'value')                                                                         as zoning_type
           , json_extract_path_text(a.response_data, 'assessment', 'property', 'features', 'swimming_pool',
                                    'value')                                                                         as swimming_pool
           , json_extract_path_text(a.response_data, 'assessment', 'property', 'features', 'fema_flood_zone',
                                    'value')                                                                         as fema_flood_zone

           , json_parse(json_extract_path_text(a.response_data, 'assessment', 'buildings', '0', 'features',
                                               'roof_area'))::super                                                  as roof_area
           , json_parse(json_extract_path_text(a.response_data, 'assessment', 'buildings', '0', 'features',
                                               'skylights'))::super                                                  as skylights
           , json_parse(json_extract_path_text(a.response_data, 'assessment', 'buildings', '0', 'features',
                                               'roof_shape'))::super                                                 as roof_shape
           , json_parse(json_extract_path_text(a.response_data, 'assessment', 'buildings', '0', 'features',
                                               'year_built'))::super                                                 as year_built
           , json_parse(json_extract_path_text(a.response_data, 'assessment', 'buildings', '0', 'features',
                                               'mobile_home'))::super                                                as mobile_home
           , json_parse(json_extract_path_text(a.response_data, 'assessment', 'buildings', '0', 'features',
                                               'roof_quality'))::super                                               as roof_quality
           , json_parse(json_extract_path_text(a.response_data, 'assessment', 'buildings', '0', 'features',
                                               'solar_panels'))::super                                               as solar_panels
           , json_parse(json_extract_path_text(a.response_data, 'assessment', 'buildings', '0', 'features',
                                               'roof_material'))::super                                              as roof_material
           , json_parse(json_extract_path_text(a.response_data, 'assessment', 'buildings', '0', 'features',
                                               'eave_height_avg'))::super                                            as eave_height_avg
           , json_parse(json_extract_path_text(a.response_data, 'assessment', 'buildings', '0', 'features',
                                               'eave_height_max'))::super                                            as eave_height_max
           , json_parse(json_extract_path_text(a.response_data, 'assessment', 'buildings', '0', 'features',
                                               'eave_height_min'))::super                                            as eave_height_min
           , json_parse(json_extract_path_text(a.response_data, 'assessment', 'buildings', '0', 'features',
                                               'solar_panel_area'))::super                                           as solar_panel_area
           , json_parse(json_extract_path_text(a.response_data, 'assessment', 'buildings', '0', 'features',
                                               'count_roof_pen_all'))::super                                         as count_roof_pen_all
           , json_parse(json_extract_path_text(a.response_data, 'assessment', 'buildings', '0', 'features',
                                               'kitchen_permit_age'))::super                                         as kitchen_permit_age
           , json_parse(json_extract_path_text(a.response_data, 'assessment', 'buildings', '0', 'features',
                                               'roof_facets_number'))::super                                         as roof_facets_number
           , json_parse(json_extract_path_text(a.response_data, 'assessment', 'buildings', '0', 'features',
                                               'roof_pitch_average'))::super                                         as roof_pitch_average
           , json_parse(json_extract_path_text(a.response_data, 'assessment', 'buildings', '0', 'features',
                                               'roof_pitch_maximum'))::super                                         as roof_pitch_maximum
           , json_parse(json_extract_path_text(a.response_data, 'assessment', 'buildings', '0', 'features',
                                               'bathroom_permit_age'))::super                                        as bathroom_permit_age
           , json_parse(json_extract_path_text(a.response_data, 'assessment', 'buildings', '0', 'features',
                                               'roof_pitch_dominant'))::super                                        as roof_pitch_dominant
           , json_parse(json_extract_path_text(a.response_data, 'assessment', 'buildings', '0', 'features',
                                               'kitchen_permit_count'))::super                                       as kitchen_permit_count
           , json_parse(json_extract_path_text(a.response_data, 'assessment', 'buildings', '0', 'features',
                                               'roof_quality_reasons'))::super                                       as roof_quality_reasons
           , json_parse(json_extract_path_text(a.response_data, 'assessment', 'buildings', '0', 'features',
                                               'bathroom_permit_count'))::super                                      as bathroom_permit_count
           , json_parse(json_extract_path_text(a.response_data, 'assessment', 'buildings', '0', 'features',
                                               'roof_pen_count_chimney'))::super                                     as roof_pen_count_chimney
           , json_parse(json_extract_path_text(a.response_data, 'assessment', 'buildings', '0', 'features',
                                               'building_height_average'))::super                                    as building_height_average
           , json_parse(json_extract_path_text(a.response_data, 'assessment', 'buildings', '0', 'features',
                                               'roof_pen_count_box_vent'))::super                                    as roof_pen_count_box_vent
           , json_parse(json_extract_path_text(a.response_data, 'assessment', 'buildings', '0', 'features',
                                               'building_area_2d_computed'))::super                                  as building_area_2d_computed
           , json_parse(json_extract_path_text(a.response_data, 'assessment', 'buildings', '0', 'features',
                                               'building_area_3d_computed'))::super                                  as building_area_3d_computed
           , json_parse(json_extract_path_text(a.response_data, 'assessment', 'buildings', '0', 'features',
                                               'number_of_stories_computed'))::super                                 as number_of_stories_computed
           , json_parse(json_extract_path_text(a.response_data, 'assessment', 'buildings', '0', 'features',
                                               'distance_to_nearest_building'))::super                               as distance_to_nearest_building
           , json_parse(json_extract_path_text(a.response_data, 'assessment', 'buildings', '0', 'features',
                                               'building_density_zone_1_percent'))::super                            as building_density_zone_1_percent
           , json_parse(json_extract_path_text(a.response_data, 'assessment', 'buildings', '0', 'features',
                                               'vegetation_density_zone_1_percent'))::super                          as vegetation_density_zone_1_percent
           , json_parse(json_extract_path_text(a.response_data, 'assessment', 'buildings', '0', 'features',
                                               'overhanging_vegetation_density_percent'))::super                     as overhanging_vegetation_density_percent


      FROM insurance_data_gateway_svc_prod.third_parties_data a
               left join insurance_data_gateway_svc_prod.third_party_communications b
                         on a.request_id = b.request_id
      WHERE a.provider = 'Zesty')
    );

--user activity logs
select *
from dwh.all_activities_table aa
--join dwh.sources_attributed_table ss on aa.tracking_id = ss.tracking_id
where business_id = '0bc639e1060c41e49b3f7ad2203e41f5'

--qtp (consistent with 2-dim QTP)
With base as (SELECT distinct qpm.creation_time::date                                           as quote_day,
                              qpm.business_id,
                              qpm.highest_policy_id,
                              qpm.highest_policy_status,
                              qpm.highest_yearly_premium,
                              CASE WHEN creation_time < '2024-12-19' THEN 'pre' ELSE 'post' END as pre_post,
                              case
                                  when qpm.agent_id <> 'N/A' then 'Agent'
                                  when qpm.affiliate_id <> 'N/A' then 'Partnership'
                                  else 'Direct' end                                                channel
              FROM dwh.quotes_policies_mlob qpm
              WHERE qpm.lob_policy IN ('PL')
                and qpm.creation_time >= '2024-07-01'
                and qpm.offer_flow_type = 'APPLICATION'
                and qpm.cob in ('Business Consulting Services', 'Technology Services'))
SELECT pre_post,
       --channel,
       average_purchased_premium,
       purchases / quotes::decimal(10, 2) as qtp
FROM (SELECT pre_post,
             --channel,
             AVG(CASE WHEN highest_policy_status >= 3 then highest_yearly_premium END) as average_purchased_premium,
             count(distinct business_id)                                               as quotes,
             SUM(CASE WHEN highest_policy_status >= 3 then 1 ELSE 0 END)               as purchases
      from base
      group by 1)

--CP QSR over time
select lob,
       cob,
       CASE
           WHEN cob in
                ('IT Consulting or Programming', 'Computer Programmers', 'Computer and Information Systems Managers',
                 'Computer Network Support Specialists', 'Computer Network Architects', 'Business Consulting',
                 'Education Consulting', 'Other Consulting', 'Safety Consultant', 'Marketing',
                 'Administrative Services Managers', 'Training and Development Specialists',
                 'Human Resources Specialists', 'Public Relations Specialists', 'Business Consulting Services',
                 'Technology Services', 'Insurance Agent', 'Property Manager', 'Home Inspectors', 'Accountant',
                 'Engineer', 'Real Estate Agent', 'Real Estate Brokers', 'Architect', 'Interior Designer',
                 'Legal Service', 'Claims Adjuster', 'Computer Repair', 'Financial Adviser', 'Notary',
                 'Administrative Support', 'Building Inspector', 'Insurance Inspector', 'Product Designer',
                 'Phone or Tablet Repair', 'Land Surveyor', 'Mortgage Broker', 'Loan Officers',
                 'Check Cashing and Pay day Loans', 'Securities, Commodities, and Financial Services Sales Agents',
                 'Occupational Health and Safety Specialists', 'Business Financing', 'Insurance Appraisers',
                 'Real Estate Appraisal', 'Telemarketing and Telesales Services', 'Urban and Regional Planners',
                 'Environmental Science and Protection Technicians, Including Health', 'Debt Relief Services',
                 'Title Loans', 'Camera and Photographic Equipment Repairers', 'Actuarial Service',
                 'Credit Authorizers, Checkers, and Clerks', 'Video Transfer Services', 'Financial Examiners',
                 'Environmental Scientists and Specialists, Including Health') THEN 'pro_services'
           WHEN cob in ('Restaurant', 'Coffee Shop', 'Bakery', 'Food Truck', 'Caterer') then 'food_bev'
           ELSE 'retail'
           END                                                                     as cob_group,
       extract(year from offer_creation_time) || '-' ||
       right('00' + convert(varchar, extract(month from offer_creation_time)), 2)  as quote_year_month,
       count(distinct case when execution_status = 'SUCCESS' then business_id end) as num_quote,
       count(distinct business_id)                                                 as num_uw
from dwh.underwriting_quotes_data uw
where offer_creation_time >= '2023-01-01'
  and offer_creation_time < '2025-06-01'
  and
--uw.lob = 'CP' and
    offer_flow_type in ('APPLICATION')
  and
--cob in ('IT Consulting or Programming', 'Computer Programmers', 'Computer and Information Systems Managers', 'Computer Network Support Specialists', 'Computer Network Architects', 'Business Consulting', 'Education Consulting', 'Other Consulting', 'Safety Consultant', 'Marketing', 'Administrative Services Managers', 'Training and Development Specialists', 'Human Resources Specialists', 'Public Relations Specialists', 'Business Consulting Services', 'Technology Services', 'Insurance Agent', 'Property Manager', 'Home Inspectors', 'Accountant', 'Engineer', 'Real Estate Agent', 'Real Estate Brokers', 'Architect', 'Interior Designer', 'Legal Service', 'Claims Adjuster', 'Computer Repair', 'Financial Adviser', 'Notary', 'Administrative Support', 'Building Inspector', 'Insurance Inspector', 'Product Designer', 'Phone or Tablet Repair', 'Land Surveyor', 'Mortgage Broker', 'Loan Officers', 'Check Cashing and Pay day Loans', 'Securities, Commodities, and Financial Services Sales Agents', 'Occupational Health and Safety Specialists', 'Business Financing', 'Insurance Appraisers', 'Real Estate Appraisal', 'Telemarketing and Telesales Services', 'Urban and Regional Planners', 'Environmental Science and Protection Technicians, Including Health', 'Debt Relief Services', 'Title Loans', 'Camera and Photographic Equipment Repairers', 'Actuarial Service', 'Credit Authorizers, Checkers, and Clerks', 'Video Transfer Services', 'Financial Examiners', 'Environmental Scientists and Specialists, Including Health')
--cob in ('Restaurant', 'Coffee Shop', 'Bakery', 'Food Truck', 'Caterer')
--cob in ('E-Commerce','Retail Stores','Grocery Store','Clothing Store','Electronics Store','Florist','Jewelry Store','Sporting Goods Retailer','Tailors, Dressmakers, and Custom Sewers','Nurseries and Gardening Shop','Candle Store','Pet Stores','Paint Stores','Flea Markets','Arts and Crafts Store','Eyewear and Optician Store','Hardware Store','Discount Store','Pawn Shop','Hobby Shop','Beach Equipment Rentals','Furniture Rental','Packing Supplies Store','Horse Equipment Shop','Demonstrators and Product Promoters','Fabric Store','Lighting Store','Luggage Store','Bike Rentals','Bike Shop','Bookstore','Home and Garden Retailer','Newspaper and Magazine Store','Department Stores','Furniture Store','Wholesalers')
--cob = 'Retail Stores'
    cob in ('IT Consulting or Programming', 'Computer Programmers', 'Computer and Information Systems Managers',
            'Computer Network Support Specialists', 'Computer Network Architects', 'Business Consulting',
            'Education Consulting', 'Other Consulting', 'Safety Consultant', 'Marketing',
            'Administrative Services Managers', 'Training and Development Specialists', 'Human Resources Specialists',
            'Public Relations Specialists', 'Business Consulting Services', 'Technology Services', 'Insurance Agent',
            'Property Manager', 'Home Inspectors', 'Accountant', 'Engineer', 'Real Estate Agent', 'Real Estate Brokers',
            'Architect', 'Interior Designer', 'Legal Service', 'Claims Adjuster', 'Computer Repair',
            'Financial Adviser', 'Notary', 'Administrative Support', 'Building Inspector', 'Insurance Inspector',
            'Product Designer', 'Phone or Tablet Repair', 'Land Surveyor', 'Mortgage Broker', 'Loan Officers',
            'Check Cashing and Pay day Loans', 'Securities, Commodities, and Financial Services Sales Agents',
            'Occupational Health and Safety Specialists', 'Business Financing', 'Insurance Appraisers',
            'Real Estate Appraisal', 'Telemarketing and Telesales Services', 'Urban and Regional Planners',
            'Environmental Science and Protection Technicians, Including Health', 'Debt Relief Services', 'Title Loans',
            'Camera and Photographic Equipment Repairers', 'Actuarial Service',
            'Credit Authorizers, Checkers, and Clerks', 'Video Transfer Services', 'Financial Examiners',
            'Environmental Scientists and Specialists, Including Health', 'Restaurant', 'Coffee Shop', 'Bakery',
            'Food Truck', 'Caterer', 'E-Commerce', 'Retail Stores', 'Grocery Store', 'Clothing Store',
            'Electronics Store', 'Florist', 'Jewelry Store', 'Sporting Goods Retailer',
            'Tailors, Dressmakers, and Custom Sewers', 'Nurseries and Gardening Shop', 'Candle Store', 'Pet Stores',
            'Paint Stores', 'Flea Markets', 'Arts and Crafts Store', 'Eyewear and Optician Store', 'Hardware Store',
            'Discount Store', 'Pawn Shop', 'Hobby Shop', 'Beach Equipment Rentals', 'Furniture Rental',
            'Packing Supplies Store', 'Horse Equipment Shop', 'Demonstrators and Product Promoters', 'Fabric Store',
            'Lighting Store', 'Luggage Store', 'Bike Rentals', 'Bike Shop', 'Bookstore', 'Home and Garden Retailer',
            'Newspaper and Magazine Store', 'Department Stores', 'Furniture Store', 'Wholesalers')
group by 1, 2, 3, 4
order by 1, 2, 3, 4

--policies with hnoa
select *
from external_dwh.gl_quotes q
where q.quote_package_data like '%GL_HIRED%' and
      q.cob_category <> 'gl_auto_service_and_repair' and
      q.current_amendment like '%5.0%' and
      q.policy_id is not null
limit 10

--TPM GL adverse risk decline info
SELECT t.*
             FROM riskmgmt_svc_prod.adverse_risk_result t
             where business_id in ('9b132455cd89dc6fc63f833a5303bec2')
             LIMIT 501

SELECT t.*
             FROM riskmgmt_svc_prod.risk_score_result t
             where business_id in ('9b132455cd89dc6fc63f833a5303bec2')
             LIMIT 501

SELECT
request.id
,request.provider as vendor
,request.operation as product
,context_id.key as context_key
,context_id.value as context_key_value
,request.event_occurrence_time_pst_timestamp_formatted as request_ts
,request.event_occurrence_time_pst_timestamp_formatted::date as request_dt
,case when response.id is not null then 1 else 0 end as dim_responsed
,response.body as response_value
,round(response_time_in_millis*1.0/1000,2) as response_time_secs
,response.event_occurrence_time_pst_timestamp_formatted as response_ts
, SPLIT_PART(
 SPLIT_PART(response.body,'<ns3:ProcessingStatus>', 3),
       '</ns3:ProcessingStatus>',
       1
) AS second_processing_status
,   SPLIT_PART(
SPLIT_PART(response.body, '<ns3:Score>', 3),
       '</ns3:Score>',
       1
   ) AS score
FROM silver_insurance_data_gateway.third_party_request_v1 request
   left join silver_insurance_data_gateway.consumer_context_v1 context_id
        on request.service_request_scope_id = context_id.service_request_scope_id
   left join silver_insurance_data_gateway.third_party_response_v1 response
        on request.id=response.id
where true
     and request.provider = 'LexisNexis'
     and request.operation='Personal Credit Score'
     and request_dt='2025-04-01'
     and context_key_value = '9b132455cd89dc6fc63f833a5303bec2'

SELECT t.*
             FROM silver_insurance_data_gateway.consumer_context_v1 t
             where t.key = 'business_id' and
                   t.value = 'dbd9010074c0d2c793b8f8547d0cd190'
             LIMIT 501

--get consultants PIF
select cob, lob_policy,
       count(distinct business_id)                  as policy_count,
       sum(highest_yearly_premium)         as total_premium
from dwh.quotes_policies_mlob
where highest_policy_status >= 4
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and cob in ('Technology Services','Business Consulting Services','IT Consulting or Programming','Computer Programmers','Computer and Information Systems Managers','Computer Network Support Specialists','Computer Network Architects','Business Consulting','Education Consulting','Other Consulting','Safety Consultant','Logisticians','Art Consultants','Marketing (re-opened)','Administrative Services Managers','Training and Development Specialists','Human Resources Specialists','Public Relations Specialists')
group by 1,2

--get consultants PIF
select distinct business_id, cob, lob_policy, highest_yearly_premium, highest_status_name, policy_end_date
from dwh.quotes_policies_mlob
where highest_policy_status = 4
  and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and cob in ('Technology Services','Business Consulting Services','IT Consulting or Programming','Computer Programmers','Computer and Information Systems Managers','Computer Network Support Specialists','Computer Network Architects','Business Consulting','Education Consulting','Other Consulting','Safety Consultant','Logisticians','Art Consultants','Marketing (re-opened)','Administrative Services Managers','Training and Development Specialists','Human Resources Specialists','Public Relations Specialists')

--amazon direct channel research
select distinct business_id,
                cob,
                distribution_channel,
                json_extract_path_text(json_args, 'lob_app_json', 'retail_market_amazon_seller_id', true) as seller_id,
                highest_status_name,
                policy_start_date
from dwh.quotes_policies_mlob
where highest_policy_status = 4
  and lob_policy = 'GL'
  and cob = 'E-Commerce'
  and distribution_channel not in ('partnerships','ap-intego','agents','Next Connect','support_inbound','support','unknown')
  --and seller_id <> ''
  and policy_start_date > '2025-05-01'
  and offer_flow_type in ('APPLICATION')

SELECT t.rating_id, t.schedule_rating_data_points
FROM silver_rating.pl t
LIMIT 501

SELECT r.rating_id, r.schedule_rating_adjustments
FROM silver_rating.gl r
LIMIT 501

SELECT t.business_id, t.offer_id, t.rating_id
FROM external_dwh.gl_quotes t
LIMIT 501

--inspect SR IDs for a given business_id
SELECT
    t.business_id,
    t.offer_id,
    t.rating_id,
    r.schedule_rating_adjustments
FROM external_dwh.gl_quotes t
JOIN silver_rating.gl r
    ON t.rating_id = r.rating_id
WHERE t.business_id = 'e2eb5e536fd24490d81708160f28877d' and
      r.dateid >= '2025-06-01'
LIMIT 10;

--top pro services CP decline reasons
select decline_reasons,
       execution_status,
       count(distinct (business_id)) as biz_count
from dwh.underwriting_quotes_data uw
         join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
where offer_creation_time >= '2025-01-01'
  and offer_creation_time < '2025-06-01'
  and uw.lob = 'CP'
  and uw.cob in ('Insurance Agent', 'Property Manager', 'Business Consulting Services', 'Accountant', 'Technology Services',
       'Engineer', 'Home Inspectors', 'Real Estate Agent', 'Marketing', 'Interior Designer', 'Architect',
       'Real Estate Brokers', 'Legal Service', 'Claims Adjuster', 'Financial Adviser', 'Administrative Support',
       'Notary', 'Product Designer', 'Building Inspector', 'Land Surveyor', 'Mortgage Broker', 'Insurance Inspector',
       'Business Financing', 'Occupational Health and Safety Specialists',
       'Environmental Science and Protection Technicians, Including Health', 'Telemarketing and Telesales Services',
       'Environmental Scientists and Specialists, Including Health', 'Real Estate Appraisal',
       'Securities, Commodities, and Financial Services Sales Agents', 'Insurance Appraisers',
       'Urban and Regional Planners', 'Loan Officers', 'Title Loans', 'Debt Relief Services',
       'Credit Authorizers, Checkers, and Clerks', 'Video Transfer Services', 'Actuarial Service',
       'Financial Examiners', 'Check Cashing and Pay day Loans', 'Law Office and Legal Services')
group by 1, 2
order by biz_count desc

--top pro services CP declines
Select risk_internal_decline_reason, count(distinct business_id) as business_count
FROM db_data_science.ds_decline_monitoring
WHERE stepstatus = 'DECLINE'
AND cob IN  ('Insurance Agent', 'Property Manager', 'Business Consulting Services', 'Accountant', 'Technology Services',
       'Engineer', 'Home Inspectors', 'Real Estate Agent', 'Marketing', 'Interior Designer', 'Architect',
       'Real Estate Brokers', 'Legal Service', 'Claims Adjuster', 'Financial Adviser', 'Administrative Support',
       'Notary', 'Product Designer', 'Building Inspector', 'Land Surveyor', 'Mortgage Broker', 'Insurance Inspector',
       'Business Financing', 'Occupational Health and Safety Specialists',
       'Environmental Science and Protection Technicians, Including Health', 'Telemarketing and Telesales Services',
       'Environmental Scientists and Specialists, Including Health', 'Real Estate Appraisal',
       'Securities, Commodities, and Financial Services Sales Agents', 'Insurance Appraisers',
       'Urban and Regional Planners', 'Loan Officers', 'Title Loans', 'Debt Relief Services',
       'Credit Authorizers, Checkers, and Clerks', 'Video Transfer Services', 'Actuarial Service',
       'Financial Examiners', 'Check Cashing and Pay day Loans', 'Law Office and Legal Services')
AND risk_internal_decline_reason IS NOT NULL
AND lob = 'CP'
GROUP BY 1
ORDER BY 2 DESC
LIMIT 500

--latest marketing COB groups
Select *
FROM dwh.marketing_groups_history
WHERE is_current = 1

--CA CP declines 6/11/2025
Select risk_internal_decline_reason, count(distinct business_id) as business_count
FROM db_data_science.ds_decline_monitoring
WHERE --stepstatus = 'DECLINE'
--cob IN  ('Insurance Agent', 'Property Manager', 'Business Consulting Services', 'Accountant', 'Technology Services',
--       'Engineer', 'Home Inspectors', 'Real Estate Agent', 'Marketing', 'Interior Designer', 'Architect',
--       'Real Estate Brokers', 'Legal Service', 'Claims Adjuster', 'Financial Adviser', 'Administrative Support',
--       'Notary', 'Product Designer', 'Building Inspector', 'Land Surveyor', 'Mortgage Broker', 'Insurance Inspector',
--       'Business Financing', 'Occupational Health and Safety Specialists',
--       'Environmental Science and Protection Technicians, Including Health', 'Telemarketing and Telesales Services',
--       'Environmental Scientists and Specialists, Including Health', 'Real Estate Appraisal',
--       'Securities, Commodities, and Financial Services Sales Agents', 'Insurance Appraisers',
--       'Urban and Regional Planners', 'Loan Officers', 'Title Loans', 'Debt Relief Services',
--       'Credit Authorizers, Checkers, and Clerks', 'Video Transfer Services', 'Actuarial Service',
--       'Financial Examiners', 'Check Cashing and Pay day Loans', 'Law Office and Legal Services', 'Restaurant', 'Caterers', 'Food Truck', 'Coffee Shop', 'Grocery Store', 'E-Commerce','Retail Stores','Grocery Store','Clothing Store','Electronics Store','Florist','Jewelry Store','Sporting Goods Retailer','Tailors, Dressmakers, and Custom Sewers','Nurseries and Gardening Shop','Candle Store','Pet Stores','Paint Stores','Flea Markets','Arts and Crafts Store','Eyewear and Optician Store','Hardware Store','Discount Store','Pawn Shop','Hobby Shop','Beach Equipment Rentals','Furniture Rental','Packing Supplies Store','Horse Equipment Shop','Demonstrators and Product Promoters','Fabric Store','Lighting Store','Luggage Store','Bike Rentals','Bike Shop','Bookstore','Home and Garden Retailer','Newspaper and Magazine Store','Department Stores','Furniture Store','Wholesalers')
--AND risk_internal_decline_reason IS NOT NULL
lob = 'CP'
and state = 'CA'
and offer_creation_time >= '2025-06-11'
GROUP BY 1
ORDER BY 2 DESC
LIMIT 500

--user activity logs
select *
from dwh.all_activities_table aa
         join dwh.sources_attributed_table ss on aa.tracking_id = ss.tracking_id
where ss.business_id = '04a4ab1334f406fa65e044e36683f5b7'

--PL retro date
select business_id,
       cob,
       json_extract_path_text(json_args, 'years_in_business_num',true) as yib,
       json_extract_path_text(json_args, 'offers_json', 'pl_calculated_retro_date',true) as retro_date,
       policy_start_date,
       creation_time,
       json_args
from dwh.quotes_policies_mlob
where highest_policy_status >= 3
  --and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and offer_flow_type in ('APPLICATION')
  and lob_policy = 'PL'
  and creation_time >= '2025-05-21'
  and cast(json_extract_path_text(json_args, 'offers_json', 'pl_calculated_retro_date', true) AS DATE)
      < policy_start_date - INTERVAL '1 day'
limit 100

SELECT
    DATE(creation_time) AS creation_date,
    COUNT(DISTINCT CASE
        WHEN CAST(json_extract_path_text(json_args, 'offers_json', 'pl_calculated_retro_date', true) AS DATE)
             >= policy_start_date - INTERVAL '1 day'
             AND CAST(json_extract_path_text(json_args, 'offers_json', 'pl_calculated_retro_date', true) AS DATE)
             <= policy_start_date
        THEN business_id
    END) AS retro_within_1_day,

    COUNT(DISTINCT CASE
        WHEN CAST(json_extract_path_text(json_args, 'offers_json', 'pl_calculated_retro_date', true) AS DATE)
             < policy_start_date - INTERVAL '1 day'
        THEN business_id
    END) AS retro_more_than_1_day
FROM dwh.quotes_policies_mlob
WHERE highest_policy_status >= 3
  AND offer_flow_type IN ('APPLICATION')
  AND lob_policy = 'PL'
  AND creation_time >= '2025-01-01'
GROUP BY DATE(creation_time)
ORDER BY creation_date;

select  q.ni_factors_v2, q.ni_factors_v2.existing_insured_factor eif from external_dwh.gl_quotes q where eif <> '' limit 10

--franchise
select business_id,
       cob,
       --json_extract_path_text(json_args, 'years_in_business_num',true) as yib,
       --json_extract_path_text(json_args, 'offers_json', 'pl_calculated_retro_date',true) as retro_date,
       json_extract_path_text(json_args, 'lob_app_json', 'is_part_of_franchise', true) as franchise_yes_no,
       json_extract_path_text(json_args, 'lob_app_json', 'franchise_name', true) as franchise_name,
       policy_start_date,
       creation_time,
       json_args
from dwh.quotes_policies_mlob
where highest_policy_status >= 3
  --and offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and offer_flow_type in ('APPLICATION')
  and lob_policy = 'CP'
  and creation_time >= '2025-06-01'
  --and cast(json_extract_path_text(json_args, 'offers_json', 'pl_calculated_retro_date', true) AS DATE)
      --< policy_start_date - INTERVAL '1 day'
  and franchise_yes_no <> ''
order by creation_time asc
limit 100

select qpm.business_id,
       json_extract_path_text(qpm.json_args, 'lob_app_json', 'retail_market_amazon_seller_id', true) as seller_id,
       json_extract_path_text(qpm.json_args, 'lob_app_json', 'retail_appliances_gl_activities', true) as appliances,
       json_extract_path_text(qpm.json_args, 'lob_app_json', 'retail_automotive_industrial_powersports_gl_activities', true) as automotive,
       qpm.json_args,
       qpm.highest_policy_status,
       qpm.creation_time
from dwh.quotes_policies_mlob qpm
where qpm.lob_policy = 'GL'
  and qpm.cob = 'E-Commerce'
  and qpm.creation_time >= '2025-06-01'


select
    qpm.business_id,
    -- amazon seller ID
    json_extract_path_text(qpm.json_args, 'lob_app_json', 'retail_market_amazon_seller_id', true) as seller_id,
    -- figure out from Prerana how to pull retail categories
    ---- used to be json_extract_path_text(qpm.json_args, 'lob_app_json', 'retail_appliances_gl_activities', true) as appliances
    -- prohibited products
    json_extract_path_text(qpm.json_args, 'lob_app_json', 'aircraft_vehicles_sales', true) as aircraft_vehicle_sales,
    json_extract_path_text(qpm.json_args, 'lob_app_json', 'animal_sales', true) as animal_sales,
    ---- add other prohibited products here
    -- permitted products
    json_extract_path_text(qpm.json_args, 'lob_app_json', 'retail_permitted_product_car_seats', true) as permitted_car_seats,
    json_extract_path_text(qpm.json_args, 'lob_app_json', 'retail_permitted_product_children_products', true) as permitted_childrens_products,
    ---- add other permitted products here
    -- other fields
    qpm.json_args,
    qpm.highest_policy_status,
    qpm.creation_time
from dwh.quotes_policies_mlob qpm
where qpm.lob_policy = 'GL'
  and qpm.cob = 'E-Commerce'
  and qpm.creation_time >= '2025-06-01'

SELECT t.*
FROM nimi_svc_prod.data_points t
--where policy_id = '8743096'
where creation_time >= '2025-06-01'
LIMIT 501

--all policy datapoints DPs PDPs given a business_id
select business_id,
       offer_id,
       offer_creation_time,
       lob,
       lob_application_id,
       data_point_id,
       value,
       source,
       effective_date as dp_effective_source,
       original_execution_status as quote_status
from dwh.underwriting_quotes_data
left join underwriting_svc_prod.lob_applications_data_points
    using(lob_application_id)
where business_id = '5393c94562962f75c40c722a1c47a579' -- change this to any business ID
and bundle_name = 'basic' -- filtering to one of the packages since every offer has 6
and policy_reference is null -- removing duplicate offers created for the purpose of purchase
-- and offer_creation_time = '' -- add date filter as necessary

--retail products PDPs
select distinct business_id,
       data_point_id,
       value
from dwh.underwriting_quotes_data
left join underwriting_svc_prod.lob_applications_data_points
    using(lob_application_id)
where data_point_id = 'location.car_seats_sales' and
      value = 'true' and
      effective_date >= '2025-07-01' and
      cob = 'E-Commerce' and
      policy_status >= 4
limit 100

--find customers who purchased standalone GL / CP / other LOB
select pol.policy_id, pol.cob_name, lob.lob_name, pol.business_id
from sl_prod_dwh.policy_dim_policy as pol
join sl_prod_dwh.dim_lob as lob on pol.lob_id = lob.lob_id
limit 10

--get all info including business_id from a given policy_id
select *
from nimi_svc_prod.policies
where policy_id = '7861484'

--consultant PL policies
select cob_id, policy_type_id, carrier, policy_status, policy_id
from nimi_svc_prod.policies
where cob_id = '111712' and
      policy_type_id = '2' and
      policy_status = '4' and
      carrier = '8'
limit 100

--lawyer PL policies
select cob_id, policy_type_id, carrier, policy_status, policy_id
from nimi_svc_prod.policies
where cob_id = '110229' and
      policy_type_id = '2' and
      policy_status = '4' and
      carrier = '7'
limit 100

--F&B list
select distinct uw.business_id,
                business_name,
                policy_status_name,
                uw.lob,
                uw.state_code,
                uw.cob,
                uw.city,
                uw.zip_code,
                uw.street
from dwh.underwriting_quotes_data uw
    join underwriting_svc_prod.lob_applications la on uw.lob_application_id = la.lob_application_id
    join (select distinct business_id, purchased_quote_job_id
                           from dwh.quotes_policies_mlob qpm
                           where lob_policy = 'GL') a on uw.business_id = a.business_id
where uw.cob in ('Restaurant','Coffee Shop','Caterer','Food Truck','Bakery')
  and policy_status = 4
  and purchased_quote_job_id is not null

--PL3 amdt date
select business_id, current_amendment, state, policy_start_date, creation_time, distribution_channel
from dwh.quotes_policies_mlob
where offer_flow_type = 'APPLICATION'
  and lob_policy = 'PL'
  and highest_policy_status >= 3
  and policy_start_date >= '2025-08-01'
  and state in ('TX','GA','CO','NC','AZ','PA','SC','MI','NV','UT','AL','IN','WI','MS','AR','WV','WY','NE')
  and current_amendment not in ('{"version": 3.0, "amendmentId": "NEXT_NXPL_3_new_policy"}')
  --removing SNIC PL2 for TX, since we are handling a carrier migration and this amendment version is compliant
  and not (current_amendment = '{"version": 2.0, "amendmentId": "SNIC_BMPL_2_new_policy"}' and state = 'TX')
limit 1000

--CP4 amdt date
select business_id, current_amendment, state, policy_start_date, creation_time, distribution_channel
from dwh.quotes_policies_mlob
where offer_flow_type = 'APPLICATION'
  and lob_policy = 'CP'
  and highest_policy_status >= 3
  and policy_start_date >= '2025-08-26'
  and state in ('AZ','MN', 'MI')
  and current_amendment not in ('{"version": 4.0, "amendmentId": "SNIC_SNCP_4_new_policy"}')
limit 1000

select json_extract_path_text(quote_package_data, 'version') from external_dwh.gl_quotes limit 10;

select distinct policy_id, left(json_extract_path_text(current_amendment, 'amendmentId'), 11) from external_dwh.gl_quotes where policy_id = '7206920';

--GA restaurant impacted policies list
select distinct business_id,
       policy_reference,
       end_date,
       business_name,
       agent_id,
       agent_name,
       agency_name,
       current_agencytype,
       agency_aggregator_name,
       agent_email_address,
       agent_phone_number,
       territory_manager
from db_data_science.v_all_agents_policies
where business_id in ('778e101acf9e1b8a77d52df356870b4d','45e6a87e811fc3071c93414256cefe60','fc9715a584c120d426381e315dcbdf83','b18736d047256a68b03075777a2ff116','c95aeb9a7c6e6d10c0d531935450a39e','9a848c10ef18d2ad948e6bdef7e1a1b8','ef7f15cd4d0b9affadbb9bcbbab8cea2','00adab4a4ca2f47c91e10d7c70ba1361','90a6b734c92515bdd9b674df324b389d','7631dd52bc363acf611624a20028a8d0','7cdf8187c38ee0d9d959a7d8bbec38cf','11a281e358f9fca42c89443aba572499','4863ff821e7dcfe3346c662008a40223','23063f984e9f403e8ee5ec65ca56e259','370040f90d617fc5c8e68414d647bcb0','d98042c637fb7a2cb564d333f894fe30','3246e38fd19680fd1d768b7b7b6a2cb4','1e906d036e9bda0e0b3ce02f9af266ea','303a7b65401f03dd6e261951a17ec46f','fc1f93b4468a6cc412376b7c5a4e9b67','082fc38b9f8415ed1ee9d1e91b24e0aa','b984db6ba11057662af2e202ba3b5b83','e20c542d300d8ee5f13bd750397cce2c','f87098151b7b2866a7e8321ddb626980','e76a8b9e3285cd41652ff8bdb98a8882','7ee0be2d9ea3abb2129445e46176f211','b1df7cd6335b337619384dd6fc422e60','c1ebb2fd7950f35d204e7214883019aa','0f36a95e71780f5ebb3bac0a9713fca6','1dde14f8db2111fd4f8ca8e82a061d40','e1dc317e75c6fe2a3669665683bd001a','cd34c85a9c15bd0e0a70c3b90df33ff7','9e080ecb978de40b28be4247abf447b2','37fa76ed9531572a80e3e9b4ed0d6f27','5106dc30f1ebc1ad79b21a75ee3f226d','9f0a4fd11cc079dd3cf638a57bd1d67e','554a3187ecfd7a6b372ec92d2a00abd5','ca8fecec26bf6899326c2b1a6d8f0683','81cf209374c074bc1a60c911f365beae','fc5c2e409b4d9cccaeff0e7c07ddba21','548518b74973f9b95c05e190182f8fcd','a800e7f50e7a54d8c3e969ee7b298848','215f3432ec942c18bdecaf4ee43837ac','9560bdb331e1bfa138f8e01401c681ef','a279459ea0b9bf96f6042c590b40e2db','6394c57b1cedc3194f963e174416305b','b8d3e87b445752065a68a9d80bb536e1','ec66ee6ac0eb0a8131821e22cf4b9354','29246ba841f11d812603670532e029b9','93916e2650b028b71ba295223d7967e8','9a5d654cc537bc1de5eec0dfd7308319','90182fee933c72f0d6b8893d85422b77','8ed67bead460151cc729409065aaa4cc','085e9b491c73ced951bec2e25f4f7989','6d1dbb704470ee80789d391b402a1a9b','32fcdc717646d8f8750f1ac047204bfe','4b1c49ad942751d937c51d78d38d100f','8710235f4b3cee5773bd5fac334bf114','a5322857347447f72dd1bfc36becf07e','42589b89912eab2de69b8f3d761879b0','cbb6c40bc87362d6a1cf54086a17e3c6','879ff064c01decce4948d6e324dce60b','e66b0d8902cd1ab61bba4c4071a8329e','44800046d9f9f7a3cf4bf4c875bdab5c','7af9b4c327a938f5abf64c400892a2c9','bfaaa0ff178f3820b6e5726fd730113a','dad7b9080342dd850d296aedd7f60ae6','9b0cf2993dd8cf76a258274df97ee402','22b431fd6808c8f315a0557c3f7ca038','4735baa6de3716a47ccba71b15f5ba23','0e0ea5a4452106ce7a5e3fef17c14d73','a424ffcdd9a895f519b59b5ba08810e1','e7f0e9176e607d7211bc40100dbe7243','671f8ba11bbadf978cf19d96e25b8115','1267034b810e9b4b7168084775df21b3','51b1589b516901aca6fe594fc5e61005','a4a467c81650dddf4c218f6f60954f06','a168406418bc4760505b84afd7bec364','1d65aafa1d90e120f41e61403452a1ee','950c5db9886af38468c92ec3671dbd9e','326830e49431e3a44db42fe5934ea793','b83768eca74b094c896e9e9771488ce8','148fadc72fcdfe5bfb2a2cf3d58bb825','e1f5cc600f33d6124b0601935ed32ed5','2cf98cf45eb1c370994f2e190efea134','3cbbcf5f8f69b748a59856dd9c407e6e','bca5a4800f73ee0496e6719cd32686a7','45ae9ab1c66608178fec37243152570e','54afb8c2586ce2120c7641f03e4b4ae1','5207d22416ac9ca012ba451efd2a2f6d','c0724077fc5a2f35e24cd3d67faf06e9','c1853d55fc9cb0decce0d4a4c5b8cd2e','37b259eeefc8254388d47490f3f66e33','4612f8d3af81843bd9a0f53656f56ba9','1c2ca3e74a5cfbc481700f93ae9ecf22') and
      policy_status_name = 'Active' and
      lob in ('GL','BP')

-- GL new vs. renewal mix by calendar quarter
select
    date_trunc('quarter', qpm.creation_time) as calendar_quarter,
    qpm.offer_flow_type,
    count(distinct qpm.business_id) as biz_count
from dwh.quotes_policies_mlob qpm
where qpm.highest_policy_status >= 1
  and qpm.lob_policy = 'GL'
  and qpm.cob_group not like '%Construction%'
  and qpm.cob_group not like '%Cleaning%'
  and qpm.offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and qpm.creation_time >= '2024-01-01'
group by 1, 2
order by 1 asc;

--amazon direct channel research
select distinct business_id,
                json_extract_path_text(json_args, 'lob_app_json', 'retail_market_amazon_seller_id', true) as seller_id
from dwh.quotes_policies_mlob
where business_id in ('ca615040b22c3c58ab667de92efd7c2d','de93b555104832fc9e4a33b87beba6a2','4c75e5d929f53ebf0e592dd8783a1b47','c149378d2f691d0b2c6e89806b6ab00d','6445ed09a03b725b60933928697184bc','2fde6d1a0141a7df57c9bb54e30d794b','9355483278540cad479609cd2005aee2','cb7a1a239bacf88a10724bdf8aca1262','c492596e70ba5374d747862b048cb0ec','a8f1e18288d21b7500db3c22d78f0be5','3f2751aca7b1084fd4cd49e04cfc6a39','d54496fc6045a0adef7309445ab9428d','841dc2a4bbe51aa5e8539036e85e2680','a70d7198be5f234f3d6f9bb4300de304','bb34344bbbfb6d62ac7b66c333085a1c','d5825d8f9d5d84fdaf364c1242f80459','e053fe2153c47833e950701e09808e06','6e1160e2175fcb7fe858a7e6dc45a11d','c14f99c5dde6e7fe46138e66d002fea7','6832e6e7bf22cdc01b217d73e49ecaa4','5a16c2a66f91683c59c69455db4dd300','1332259c59f22fe1e5b67f4e4672108c','34281c5918a80787697d70bc1f1bd87a','b3a277119b7f8292e41ee4a26e16f98f','2a41e204a83dfc01fab15ae769547467','208df0b327495d598b9117b9a690e257','814cac9903a0c297c6aeff4bb13ddbda','1aa1419375bb35f8f97881da0d531b3d','8f040bc1c9b8aaa027465c06781dab70','5f5286d17a13ba328f4ed4d177ac6792','1bb9f7c735aae62481a24014cf905b27','1fbcebb784ed2956653d67a3abf347e3','a84002fc2b6d31d8e8277784346d0d9f','61c40b6d7dc270b06d21031afb309645','5707cd7e373663baa95400eed493321f','d157411ef710c13c54d7950c13049e3c','cab853bdebcb624a8e4b9099cd9837dd','3c5dc10e06f1dd4380a20eb7d67e1d3b','c8c9d0b6f270e7f36d4f31fb605a29a5','b23c50e8a29c4da4070c70c5a11acacb','df34c2c6ed0976c354adb91822dd75c6','d92d09528628081855098545a594b401','5a5ba513f2fddf565bd3f67932b1acdf','3c4d3cfb39dbc630fe0de713dee508a7','d8bee61f090929a1cd7058e531dcc36d','4b40b6da1e14abb4353cb42d50899cee','b3aaee98818211578c34ecbc78427293','bbd827575288d3dcf38a5cc199fd7908','4fe279c0cd216aa761644502caeef8c1','ad247b1a4af88bd7bad6705c14a44111','114cd0bb81a621a43d27e3e5bbca4ce5','94fe70ffae383d5d9b70218de6e86cea','5586ece2e1c84c307064f3bf17e3ab3f','94fe70ffae383d5d9b70218de6e86cea','2a5ef418a4fe601bee0667bace6f3387','78409c2dcca9b053270c375abe72fc65','8ff2d50a5a4ed1f5e765c2a976d40235','b3aaee98818211578c34ecbc78427293','26dde25acd8d0ade10af5422c7d468bb','97c3ea07da02e6cc3b043927dcb91d85','649804ba0fdf148d49978f2b52714ca6','b9018ee01dc80597c1a1686dde012cb7','627ba68117513714e3818c93df5bcaa7','72c7c52f8ac32aef8a9820bd96b388d7','235af818be39a9b751d593e6123604e5','0ab0d530c2c240f57c924393898ca296','c6aaa25403e58310336759b2e47e093a','b2d8809cfdcf1c4d232f443377224f7c','c149378d2f691d0b2c6e89806b6ab00d','c5101efa8991b87d8ae82c14ee5fe3dd','43fbf700a394e885af08251015f61da5','520f58d1dc352b46e5f88fa877e77e5d','65d63b85538e05ad37add9cbc699572c','5278118a4ef0acba8a2af568751173ed','20f22ef176f174ca04db0171bec90ff7','80faac939371b5283789ae90e17921d9','2dca9b0d40f938204891c8c61c0865fe','a5d8c92393e5d60a509a15fa3e1e0bbf','bdf534c6c6c15c51b88127c1842d0d88','7fed1f4359e0c778323bcfe676d621fb','b6eeaf55311efb6966e899e83cb93889','802b8313831db137777c6b6032b2953c','6d80fb315447a109aa774c5d66677794','ac58f6211fbfc4c7772186bfd85ecb81','dff716034ab19a5e2529b9d28c1405eb','0ab0d530c2c240f57c924393898ca296','80ab528aaf09614d80d43314b08785a3','2ed053c129f7a833275d3c50799b9918','885a00e89147e1467b2dd91f9af232b1','1f2254c4f6fc22f81944f6a65b25db44','b04397851afb00f9ea9256052814f384','bd3e2752460f947a97d6f278a7fe77ab','26ed0a7c4cae77740e1051a22400f7a9','94fe70ffae383d5d9b70218de6e86cea','fd3874a4f767c4964c09e31da09a0ee6','90b6d6999d2c04ce40ecf1a55f8377d7','d4775c1160c91cbff19a75868757fb9f','629401c945c1e4bcdfcd67eb06c31c83','2a7170cff792ccc8909c7794234aa8d5','1e51cd36318f8915eab36ac98a0f30d8','3e467934ccb4ec6824a3b068626984fa','84447bc2f3e77e5b61db6f08cc772345','8541c79c0cfcff631d090dcf17947aa1','8e73cf55ca26c8d3437ff6dbbf63ea60','42a2946726261106492c44600f230fa6','e3852b75999c7b9780040d3aef722b5d','cb7a1a239bacf88a10724bdf8aca1262','8aec607df6bdeb62f4f8fb2f4f481556','b9e6594664411b90e0b2302717ca66c0','a70d7198be5f234f3d6f9bb4300de304','46d2b010b62e677a81ba45c2ee519744','09f5b78c0227fad9ea5c29f10da1c9d4','c7a34a8a9a8caa3bd2f3b7f8cc8bad65','67f929a146ab9e3ff78da654251d707e','bcb43eb64762c52116bf24f9e4777109','201b9e25dd0f37e4ef0df3a2613a2f39','2ed053c129f7a833275d3c50799b9918','aa59444f06164f27f9697e40ddea8dfe','5f04a59cb1f3480651a42415df83fb4e','7effd66966f64ccac060e8f247341781','b311d193d12abe001fa642c21ff645ff','da632c58b2dcd30d35f8b6f2d19c3961','2176c312f562e27719e0791760c4bb57','20d49b38244a5679aa36326cd25aa34f','55e7ccc096f3bb17ceddeab34def96ac','9c84dcaa34f323fd45781c916fb1d82d','5817eaafcab69d19346ffa3df03d2bf9','3bc824a3a8cd4dadfe4e51e09de72ae2','91968141e10744904d6b0b4f3b61de06','080a44690555d8d0460c4199c05d8407','8e551dc00406d704d9267de2b3a3f77b','c56e9246a67a8b2db02f8e815297ebd6','ca4efec7fcbef3af2297f88276fa6ed2','bd85d9cda67e42bda07cdf35eee11e5f','3d0a7fa992326875666b69fc9db275de','841dc2a4bbe51aa5e8539036e85e2680','d9912dfe37ff31e986a0a938ed1f7bc4','0d8596a303767f74645dce1e4bedf284','9c4a76e1644a6de5be6b09007ad72de5','a6db3127df3d00180f0a9dadc9177ffe','887b86edb18bb802b8b35a33af06d399','bc634f8b823be27e296d95d14d642687','3f2751aca7b1084fd4cd49e04cfc6a39','dff716034ab19a5e2529b9d28c1405eb','9fe073d07f3b5db21f40e917577efd09','208df0b327495d598b9117b9a690e257','3b2ad92238f2f8953530a3a02f245d63','7e30b5657b386333fe96e6429e99b45f','b93179b636873554ce9ff4837610d5a1','7b245e16f1e4e5be7437d1a5ac75102a','ac3c8c7550bb42cdd36863b0cc7b7cbb','1d21961110458ac33cd81dfff221200c','f3d7f08b3e21dcae119ab02b34a79d3e','3f2d3dbbf5698d9fe318a54e6f961df6','bd91b48615f5bf4e8cc4168e535dc995','4f7e596775ef7e6238493313819952f8','8541c79c0cfcff631d090dcf17947aa1','df493a31f854234462aa74311778a5f1','97c2c479cd3758e48c6fe780b0e2a4b6','1baa7ac842cc3989b1c46505208b1eb8','c149378d2f691d0b2c6e89806b6ab00d','daa1c84af5779f1f022f51c89ce8ab1e','ad0196caf24fabeb90815c15e19ab80c','ac58f6211fbfc4c7772186bfd85ecb81','a10af69a373eef6ee5b5e3345016cb5f','67585c00f8d351c46f7e9346e926b292','94a74c47ad4d5b16f63d6f81cf0eb854','72c7c52f8ac32aef8a9820bd96b388d7','746ff21a43bf40f9e82de4b189236267','f776afb76cddb0b074ebbaf0486bad88','8c467394024f138ebaf7b8ae8eb533da','72653f8709359b20b9ca01fc02d3a6af','cf34c40c90066c36170b61a4df4c40e1','ba861638033fd95c13a97641620b278d','5f246fecacbe4aad994922ab0e75e934','d32ea9894a2e1f0e5c5cab273c5d608d','ea764287c4cd4f459eff7d5e4aacc9c9','a056e6726d60117a5d36af73443d8118','9c3d827cfcbaad1df60de9aff64f2e81','09f5b78c0227fad9ea5c29f10da1c9d4','013def7baaabba9d452ed1f761df6924','6f8940f1b01c0d255be362a355909f24','859a748b232d80798a7c1f1bb586e88f','b3aaee98818211578c34ecbc78427293','6ae9ba4726243507165d3cb9fa2a3831','856749f37dd7f5aa755790e61a498681','c149378d2f691d0b2c6e89806b6ab00d','4256be57a49feb7d84ef1cfff7d30cfa','7a724d8f143b384d0d5ce605c4c33726','fc556f1dbd1487199ea80968504d11ec','9355483278540cad479609cd2005aee2','51456f313c2091cbea377a45e7bb4e08','26dde25acd8d0ade10af5422c7d468bb','6ba6d4eed9a30c4e8aa2fe738165f96d','9355483278540cad479609cd2005aee2','1bcef2408af7a9a3a7b87eb1692fe5ba','cb9244c187e57a030e7069e57de286e5','a7a5050338349fb23a8832cc5f742b64','b7494a0ad775f9b433bad516d5327512','42a2946726261106492c44600f230fa6','6de644a772643eb83ed172d8ca95fb60','beafa630817d9fddc54f0a3c1d8446f3','d893f03bb61a331f0822be4cdb800e94','e15c26f86e0c44632e7db34713442c9a','1bd8f4d877b83364cd1d555291e57836','61bfc54417a654817eec7d90a9784f28','8edf7aea01ab9cd64a0e4e0562c0bb81','55e7ccc096f3bb17ceddeab34def96ac','af69ee3a36284487d69b0e46cdc626bc','235af818be39a9b751d593e6123604e5','38b115e89e66f8a91bdf815fca9cd530','f6a3342a7f889c8b2c1b02f74439f5d5','8c4e44e5f3213d205a588be8fbfba82b','8c4e44e5f3213d205a588be8fbfba82b','09f5b78c0227fad9ea5c29f10da1c9d4','dff716034ab19a5e2529b9d28c1405eb','e279b4ba48317770502b56e8011a76a5','859a748b232d80798a7c1f1bb586e88f','65d63b85538e05ad37add9cbc699572c','55e7ccc096f3bb17ceddeab34def96ac','39cf6f167b924e5f819c9ef03a6619c0','20f22ef176f174ca04db0171bec90ff7','f1d0f0368066b9b6f6db276dcce5f94a','e0fcd3fb7f43514090d89ad05202e564','469542f7e48c5558c9fc1684149b14f7','627ba68117513714e3818c93df5bcaa7','e9935edd69de9fe359f60cbe7be41cc3','819723f8c9b874589d1e40fdd927ba07','208df0b327495d598b9117b9a690e257','1337346e99f7ded9da39ca91d786137e','f959d5be64006b515be39b78ed54a2ba','aba6e542728feea322cd973825933ff4','d6af688e50438159e1071acb0e78ce31','72f6438e8492500683c9ae9c2215a6c4','f072deafce8843246deadbb71a2dc426','dae12782fe9355ad30b9ec3c5f1a9046','a1d4c47d16961cece84da2f93b3b5107','01bd95f235ffd22e207779134dfca215','5a16c2a66f91683c59c69455db4dd300','46d2b010b62e677a81ba45c2ee519744','5ba804ab91cb74509eab031a5e61bd8e','e7edd9aa8597cb752786a0c2aaccd0ff','d61ecc8e5318369563ce867498786f1d','4c75e5d929f53ebf0e592dd8783a1b47','f776afb76cddb0b074ebbaf0486bad88','235af818be39a9b751d593e6123604e5','d65dc8e12a727946309b63986b3bd154','b31da372484c31a7e61fe00184c7290c','20f22ef176f174ca04db0171bec90ff7','3f39004d7aaf4847c4855a71a778a5e7','8febde5a8891b081546f32d5ac1efdc0','5bacb4c15e987f2abb76c8b6aae1282d','b574e717c03a416e415f916aa93e8542','784ef116f4aca1ba561d531dc0cceba0','bd3e2752460f947a97d6f278a7fe77ab','0cf695118bc4247f044edfb3ff39443f','dff716034ab19a5e2529b9d28c1405eb','51f2cef48d215b44d7876b8b9c03d753','3c28ec26ae5777ba9a2255bd6ec8ff8c','bd91b48615f5bf4e8cc4168e535dc995','764265c4c562875f5687e9ae140b6f31','4d5d0a18df0a7f4a165a0b2367d13f30','77d269f63e0d3988e40d1d4572c0ed0b','a7a5050338349fb23a8832cc5f742b64','290808cef3130f029bf79d3c843e6f16','7a724d8f143b384d0d5ce605c4c33726','1bc8524859adef829cede12bbeb4749e','887b86edb18bb802b8b35a33af06d399','e72d4ed07acdcd9b8b00c106f3d5ee81','45a39898791c753c229934d3e2201f52','510b77e24ce49eef0484ba1ac49dca27','ad178fe87688b5fc9c4b7ad87940712a','b1d8f70c4475d9b6710c004fdb274c91','c7819ffc43a47a0f369c9beac431ca8b','fd3874a4f767c4964c09e31da09a0ee6','201b9e25dd0f37e4ef0df3a2613a2f39','a2f7c4ddbc2d9f620f4a2d4710b6ea82','f6eeb7bed262adb6965f436d9d218d9d','a6db3127df3d00180f0a9dadc9177ffe','cc3df3b0f96f2bb73731a0e8da3e9bec','7fed1f4359e0c778323bcfe676d621fb','ba9dd5490a2224ba6f37c5c4485532fc','7b245e16f1e4e5be7437d1a5ac75102a','9d202d56b7277830d47209a15b6883ee','2a7170cff792ccc8909c7794234aa8d5','20f22ef176f174ca04db0171bec90ff7','648845fdfa92525123550e94e3e956df','b210caf92038e4a01d6e1651cde5edc8','a7184dca0ae8bfe49f606ae8a7a21fe3','5817eaafcab69d19346ffa3df03d2bf9','a9aa2cd91f2802bc20a0c38ac032b941','2d276e10eb30ca2a1dc5f6141ddd4b10','b23c50e8a29c4da4070c70c5a11acacb','2d276e10eb30ca2a1dc5f6141ddd4b10','cf3f966785007e1f62129a21a8c06f69','01bd95f235ffd22e207779134dfca215','c5c5bbcf023c0b5ade83dd8c9a83192f','ed55ee97ea834fb97895fbca53b849a7','627ba68117513714e3818c93df5bcaa7','3677b601396e9bb4d86fbf20c53e4a3b','5a49c3084d77303e2b414605517c0983','5f8e1f14146cabd07075450894c68985','2d8afe3d39f002a30720c20e3733f6e7','78e08315fc2c41d702e2fbec09c9f382','bbd827575288d3dcf38a5cc199fd7908','7fed1f4359e0c778323bcfe676d621fb','9a744db5e91391b423183c24d9f602f3','46d2b010b62e677a81ba45c2ee519744','38ecb22053c99f67a5c6080b156610b2','3bc824a3a8cd4dadfe4e51e09de72ae2','d619473c1662887df9b16ed415db0ed2','5588b51a4f34b734c4ee82ea83233e15','37f266f5b187bb61aae5adc9154f653b','208df0b327495d598b9117b9a690e257','c30e9017ddbcc9ae25c664e529e336d1','3d9168e05e4bd6c529ce74aa221eb105','f072deafce8843246deadbb71a2dc426','b05a1569b2569a40f63b6e4e68edcff7','03a810e9d03659c0dd8bb4cc5c24ddbd','94fe70ffae383d5d9b70218de6e86cea','8d36695ec4b56a65d0274887113ad931','bd91b48615f5bf4e8cc4168e535dc995','f739288109f9f5fbfee5fbce638991a1','c2e84864cba458e7a6156db5ea40d25e','3ef71c9ffb7cf314d5fad2d4d0aea0dd','208df0b327495d598b9117b9a690e257','24881de0a74f6a19005be2b4ccae93fe','75200194d3f83b2602649fbde07057c8','8d1f4af0211f836a7693db788a7b13b8','130f9d833b0644525d6f3a52dfb2bbad','e067118d7bfa0f135a8c480798c2bab7','9355483278540cad479609cd2005aee2','6b2e2292efc619b0a571d9438e0cf492','d1ea6b0d326f428fc25f0076f2f772ff','43bad5c586b8afdee3f7834f111c6188','767f8241e8151e1c860a25580b6acbb2','235af818be39a9b751d593e6123604e5','7ab7aadd813681d3076fc82e07cddfb7','5817eaafcab69d19346ffa3df03d2bf9','e2d4b0eb5db87e8958974dd2f9d1f563','6e5498faf2489a26163032e742c9857f','8c4e44e5f3213d205a588be8fbfba82b','23d9448ca968e901bbb245159d270eaf','3948ea61d14f9570453a8be9700f80d9','b7ca65d4e741c7ba1fcc321e7e5e4112','10dbff3849477237db02e863801d2675','e9e5fad11e113b3ab7842c9aedb53e1e','4400353da1b2036e9252ee5aff97b87b','9450099095d856c94ceb6a9defc88795','94fe70ffae383d5d9b70218de6e86cea','62ff3240eafb2f5e4e599772e63c52d5','94fe70ffae383d5d9b70218de6e86cea','8541c79c0cfcff631d090dcf17947aa1','57b99c3237778005a4887be5ce7db91e','9cb8613ab580710a023c54527a5fa81f','7ad1e93fc037f50667e4c07ad21fc214','a70d7198be5f234f3d6f9bb4300de304','9355483278540cad479609cd2005aee2','ff1e39ae024090f32d924899196b091c','1a7d5adbeb22ddee5dad63d19851a364','33cbca5155b443e1366b4b1c65f7958c','fc63cfb0da8cd270338413a9de5e89a2','5885e0409d2a7a9aaa9b85c875546d97','07601da14c4294105106df4d05b49686','96b06d247768b295aef4e49458cd7f8a','38773c3278bffe7c44081cff30e7d85e','1b0eb42b88045424b183136473eb2843','5885e0409d2a7a9aaa9b85c875546d97','1a7fd05966f8398568e7eb827b322d2b','7a724d8f143b384d0d5ce605c4c33726','95d5b187ac77ed5cc3382e020b2f8129','2a53590d3604330319e735134e3d9015','d8bee61f090929a1cd7058e531dcc36d','eeb713faf146efe8be399d7bb0f63a13','e4a5a847a16f19db61c3c6a744ac2220','55e7ccc096f3bb17ceddeab34def96ac','df493a31f854234462aa74311778a5f1','bd91b48615f5bf4e8cc4168e535dc995','62ff3240eafb2f5e4e599772e63c52d5','4680c5bb2be3e00e1d6dcf079be1a3eb','235af818be39a9b751d593e6123604e5','1aa1419375bb35f8f97881da0d531b3d','2ce590e680ee9ac3b964116bed5d9d8c','7a724d8f143b384d0d5ce605c4c33726','7abb776830a7a60eb165dc387a7bd638','76333449b898f6f016b68bf748ec0ce1','b3aaee98818211578c34ecbc78427293','c4e66bba503cf674dbeafecd5422ce6d','eca43cfa0e476dacd36d88146ec7ef29','1aa1419375bb35f8f97881da0d531b3d','9b0639c768dd965b9a5e3718c93499a1','5588b51a4f34b734c4ee82ea83233e15','65a9b74ac562b1a7400c25fa6512fe81','59975bea85a1eb9f12f2e6940cd29605','3bc824a3a8cd4dadfe4e51e09de72ae2','6c956c082d04a8c7d1f3a3812841aec6','24881de0a74f6a19005be2b4ccae93fe','c1a5a803465c9d384a504a0f3d07056f','841dc2a4bbe51aa5e8539036e85e2680','00cf5e69e3adbf5dcebce694a62b95bb','d65dc8e12a727946309b63986b3bd154','66e866fea2136cfefd5fe2ea5fe810b5','2656ffcefba087d3f3edadf9b58e5f79','67585c00f8d351c46f7e9346e926b292','381b64e675674d661f1e99e23457738f','72faa69da5c9b68ca0aedc1d5f76deeb','d55398dc8c72fafa21cffc30756f1369','65d63b85538e05ad37add9cbc699572c','0ab0d530c2c240f57c924393898ca296','1f2dce07f9b3cc4f0045595159864e57','00e04ab68922b033e170d170b2d80064','bd91b48615f5bf4e8cc4168e535dc995','2a5ef418a4fe601bee0667bace6f3387','3ddce0272318f942ba1574d003bd662e','bbd827575288d3dcf38a5cc199fd7908','7fed1f4359e0c778323bcfe676d621fb','19a1110dfd018c704de70cc9f8e14a25','cf6fe4ea9c8f712f4c30370d95c759e4','3f2d3dbbf5698d9fe318a54e6f961df6','16eee112ea89ec0cfe6a55a9229d69ed','2f333ae4f74eccb134c13e34bf085203','bd91b48615f5bf4e8cc4168e535dc995','59434449fc7fef2dd2b0283fe01ce891','2e2d12ebd05debad8d0f24c11a405ca7','208df0b327495d598b9117b9a690e257','208df0b327495d598b9117b9a690e257','8de5cd3563c8465f400060994d8fbc8a','841dc2a4bbe51aa5e8539036e85e2680','e05256a30a10736542db6da6497b3128','5c0a29c8bd568ac1fbbff52ff235455a','7fed1f4359e0c778323bcfe676d621fb','0217e1aae09f34226663ee9c291f740a','6445ed09a03b725b60933928697184bc','46d2b010b62e677a81ba45c2ee519744','5a2af187171d4374e44c6d02c8bdb934','30868101d89d427560b3851343a6612c','a8a45becab80b334a17363a4b3797b9e','52c3dd00d20ebea726d7b02fd7d8127e','16e191f9751190dd0102ac656987d7c8','c149378d2f691d0b2c6e89806b6ab00d','768b48a1576217f081164afd3fc230e8','f072deafce8843246deadbb71a2dc426','92c6bbe2f0bbf09716cae0ff98eae1ba','7fed1f4359e0c778323bcfe676d621fb','d2746aadb7c5373d0912f8edaa7706e8','da93244c5937369a576997b7be7cc213','1a7cbba3b03889f1f3fc69e4e869942f','f1f41f8807183e95512a06628815e6ed','1332259c59f22fe1e5b67f4e4672108c','94fe70ffae383d5d9b70218de6e86cea','4226886d3a60dc1e1f97f3412dadf3ba','8e73cf55ca26c8d3437ff6dbbf63ea60','76f6bc2c309e555dd6935b9aa3bd6f65','8c4e44e5f3213d205a588be8fbfba82b','08546fd74f731a4b7eb507097c5d661c','9355483278540cad479609cd2005aee2','2b5ff41d891d2a0a9a2aa6f2fe3e8b79','26dde25acd8d0ade10af5422c7d468bb','5f04a59cb1f3480651a42415df83fb4e','c74bd2163058ad23c7fc61c7c50bf243','678c9572bfcbefd4ab68fbafbbbad29a','9f8b23e095f78af594c73a5e8656bdc2','a433b632866aba97ba59bea0f38c41ba','558de1fdd42d985eef5c2f0b8495ec30','2ed053c129f7a833275d3c50799b9918','26dde25acd8d0ade10af5422c7d468bb','7fed1f4359e0c778323bcfe676d621fb','6f42d6340fb6d23c088cf5d6f3080b63','43fbf700a394e885af08251015f61da5','15986cf7bc74030365891698d8b786ec','2a5ef418a4fe601bee0667bace6f3387','9d60bbd4bad7db90977a2c2cbe5e748f','4d47c273d69690d5ca9e224f222582b8','65a9b74ac562b1a7400c25fa6512fe81','8234e0d1a6b0d640cb3fff4fb2c41ae8','24881de0a74f6a19005be2b4ccae93fe','8541c79c0cfcff631d090dcf17947aa1','fe72ab32fe0f464b615f55f679d766d4','65d63b85538e05ad37add9cbc699572c','463b7d4d1be800ef7f2a912ba991a55b','cb7a1a239bacf88a10724bdf8aca1262','8541c79c0cfcff631d090dcf17947aa1','4390b8fdf4e735df31eb5b7c4ec348b6','439d17cf7117554d1a97d49ef4cd25a0','dff716034ab19a5e2529b9d28c1405eb','fd3874a4f767c4964c09e31da09a0ee6','65c1bff1f198a88a18cb4f514c18c573','fddb2fcb0ac61d25cdd47981fff9db8c','a03bb24c8a25f9ce09d67c71f13f3d9f','35c145d455a41b41fb80cc1fe4c09d40','6de644a772643eb83ed172d8ca95fb60','a7184dca0ae8bfe49f606ae8a7a21fe3','69448761a9d10bc78812c45b4a7198b5','8541c79c0cfcff631d090dcf17947aa1','f6eeb7bed262adb6965f436d9d218d9d','6e6079feba50ee07bd23cc260c3b97af','cf6fe4ea9c8f712f4c30370d95c759e4','f1f5e99653dded71237101ce5c4aa666','6fc553b3006ec5279117659132b4d5cc','2f333ae4f74eccb134c13e34bf085203','e942155a4df6fa79a6e6a39666bec8e6','6e1160e2175fcb7fe858a7e6dc45a11d','128bf8fff2170e2163bb5a1760dc1f56','8541c79c0cfcff631d090dcf17947aa1','7fed1f4359e0c778323bcfe676d621fb','f451373ddc0b7a44f4b5b84f7339bad7','6d95e9b42f2774a291e9e0bc1dc69de5','036ee508fe4f70a63d16bf2bd8ccc69e','00cf5e69e3adbf5dcebce694a62b95bb','ab4989429a6a95520af11197ca2fcbb4','dff716034ab19a5e2529b9d28c1405eb','d8bee61f090929a1cd7058e531dcc36d','beafa630817d9fddc54f0a3c1d8446f3','09f5b78c0227fad9ea5c29f10da1c9d4','fddb2fcb0ac61d25cdd47981fff9db8c','625a66b8f2475dca8e1fd773a916032f','625a66b8f2475dca8e1fd773a916032f','5817eaafcab69d19346ffa3df03d2bf9','caa253752ae5209796d2e42a0af1b080','f365c9c5e4a498cc7992ff5254835705','3bc824a3a8cd4dadfe4e51e09de72ae2','776768affab74e4b82e02b927e662e99','e731e910f819a3f98e8dc8a867be080d','ea12e0393c7a52d9041f7639793d48bc','e6124a103c652bf1aa992b92bee35ffd','d21919d2bebe0836551ac9054abc389a','74162eccebea0723a613c2b4f130ce8f','4256be57a49feb7d84ef1cfff7d30cfa','42409180620fe2dc5497d0ddfa995cbe','42a2946726261106492c44600f230fa6','9da963b751f1aa6e28852014804ec99c','e7edd9aa8597cb752786a0c2aaccd0ff','196cbad6acc3adcf9e6e9d082b79bab3','6befcce1eaa226b260d03f3b02c58d16','38b115e89e66f8a91bdf815fca9cd530','8541c79c0cfcff631d090dcf17947aa1','bb567651a4f74d01e7e0f3b61a870de3','8e4cdac45f5afedacba8ad0215dff624','f6a3342a7f889c8b2c1b02f74439f5d5','e72d4ed07acdcd9b8b00c106f3d5ee81','8a05444d300c9aa53ad96894f1bcda0f','24881de0a74f6a19005be2b4ccae93fe','65c1bff1f198a88a18cb4f514c18c573','c149378d2f691d0b2c6e89806b6ab00d','94fe70ffae383d5d9b70218de6e86cea','94fe70ffae383d5d9b70218de6e86cea','3f39e777fb0cecac4255d0e04f60f27a','f5a52a10a5a07eeb35e9ec0eb7c36354','cb48a3e8ffa4a5d5a8c64b913b7f4a56','8bdbf1de797f1dbb1e5ffe3297bcdc14','caa253752ae5209796d2e42a0af1b080','8234e0d1a6b0d640cb3fff4fb2c41ae8','09f5b78c0227fad9ea5c29f10da1c9d4','3f39e777fb0cecac4255d0e04f60f27a','96b06d247768b295aef4e49458cd7f8a','29a43dab3fc033a1d798376415e6dacd','2dfbd12f19650c952242977123c0766a','6d957e70abd5571a0872045fcb21c414','94fe70ffae383d5d9b70218de6e86cea','ca4efec7fcbef3af2297f88276fa6ed2','7d8b232f628edd658d011bd8ee1e861a','692d029a7339e76f11baecdc88e8052a','235af818be39a9b751d593e6123604e5','a70d7198be5f234f3d6f9bb4300de304','d50c45e4c78da8371c9ee489dd51b407','b8a282db96b296818bb645382ed7c9f9','cc1aa7f0c724f41504eb7bda0225b8b4','336d9bc9354f59147ad1a1c0b2e3fcc5','cb7a1a239bacf88a10724bdf8aca1262','bc8cb675c7869529601c68d4307fc5a1','e7874e879d2b673e127761e255c5511c','a6db3127df3d00180f0a9dadc9177ffe','8e73cf55ca26c8d3437ff6dbbf63ea60','cb3b79930099076a7bacbf8dfa7c0bcf','dff716034ab19a5e2529b9d28c1405eb','d0dd9e05fd31f4be828cf297f1913f40','9355483278540cad479609cd2005aee2','4fe279c0cd216aa761644502caeef8c1','841dc2a4bbe51aa5e8539036e85e2680','b60594926123a8bd5e743f8651f38c35','46ec97343ae441214717f30a3df4448b','6f05010a386a41ce70ee3144489977fd','d1ea6b0d326f428fc25f0076f2f772ff','10748092a3a7e78ccda4078265430945','5b82fcb6a5fbdfbfb76d804361fb336f','59975bea85a1eb9f12f2e6940cd29605','1ce8ae321fe5733d6fc5118d08f83df7','38b115e89e66f8a91bdf815fca9cd530','c149378d2f691d0b2c6e89806b6ab00d','d5024f4fca4bfdbc947742a0012b5190','a72158f9e14ce42e481110016216d01a','478fd65bc2134fb70d1dfcdceb1e44e5','530ecb8101444e6c02433d1a9f2a2331','65d63b85538e05ad37add9cbc699572c','4f7e596775ef7e6238493313819952f8','05d63ee81b8118734819721e70ec7136','e368cd371bbcbb8c0bec589973dcccab','0fcf7ad3bf3e48c933bc347922e59fb7','0fc78534bd5a6b1c5716718e3164505c','cb9244c187e57a030e7069e57de286e5','fc1b48eaba45d211b5c2d4953f2c8ffb','1d2bab32c5eb29a58bc310a81cbf8d68','0abcb80f265ec3d7f460db9a3b673ec8','7b03aa767cd9bccea09b32f95444346c','63e746ddd186ed5d14ec608b821a7b40','a70d7198be5f234f3d6f9bb4300de304','fd905c638c0b5134db3f4bf1c006fd68','28975a020e3ea31cca44ada3e524b280','d46618c68afc62dec1b501f4f0b8d232','0be039e374dd7506a1fe9d547d07fab3','11b95c62f5dd8abfed1f252c6686cca1','7fed1f4359e0c778323bcfe676d621fb','f788930516ec470d5583fd163e663e25','96250a695cb2f84bf964e325618f1076','c149378d2f691d0b2c6e89806b6ab00d','d0b33e4d39666bd962cb094185c82e7e','000f28b39609fb58b6290c9d5d326b39','38b115e89e66f8a91bdf815fca9cd530','fd3874a4f767c4964c09e31da09a0ee6','278d6b9f668bd3fa4f65822aa6b174d5','1aa1419375bb35f8f97881da0d531b3d','72f6438e8492500683c9ae9c2215a6c4','f776afb76cddb0b074ebbaf0486bad88','e72d4ed07acdcd9b8b00c106f3d5ee81','e1bc49fe96208056bac9861e49420819','96250a695cb2f84bf964e325618f1076','cf6fe4ea9c8f712f4c30370d95c759e4','3a0341cc1f40f9e4e7cac7578ea395f5','0de2a6732d008f7e68711fe53fefa1df','94fe70ffae383d5d9b70218de6e86cea','7ab7aadd813681d3076fc82e07cddfb7','f6eeb7bed262adb6965f436d9d218d9d','eaae38764652eb59c0566cf55c09cae7','b9e6594664411b90e0b2302717ca66c0','99ddb41a380ce37de9eb746d788e04f4','ba861638033fd95c13a97641620b278d','c5c5bbcf023c0b5ade83dd8c9a83192f','31179578d9db78c110042237ae8f4f2a','74aa98905ef10d6a2f108ccaa11ebf10','f0a803be827102a4f39aac63061f630a','208da1a99bdd3267c37b47942ebf284b','65942caf5f860fc00cd34fd63932aba0','3bc824a3a8cd4dadfe4e51e09de72ae2','eaae38764652eb59c0566cf55c09cae7','cda4b0b1774933ef4956061a0edd0e90','bd91b48615f5bf4e8cc4168e535dc995','5c0a29c8bd568ac1fbbff52ff235455a','bd3e2752460f947a97d6f278a7fe77ab','46d2b010b62e677a81ba45c2ee519744','aba6e542728feea322cd973825933ff4','a03bb24c8a25f9ce09d67c71f13f3d9f','a71042cd8cbde6db51f99595b472e480','26dde25acd8d0ade10af5422c7d468bb','8da6b331464611fe8fe231deaa649534','4aca50f24e092281a1f57147ee5da2fb','2a7170cff792ccc8909c7794234aa8d5','fd3874a4f767c4964c09e31da09a0ee6','5c0a29c8bd568ac1fbbff52ff235455a','b224604adbb14b303a6edd037eb9f3ca','d301dadbf3d2246d1d300eb9d12b8ddf','5f8e1f14146cabd07075450894c68985','3a36b0af738ab0b523d94e7aa04c850b','29fb0676b974960efa1d27656ab17de5','5a5ba513f2fddf565bd3f67932b1acdf','f068f984203949c482b5885d418f267f','7e30b5657b386333fe96e6429e99b45f','84b1ea91e4ea31136f5c5c8ea98e3e0d','13a589df314379b6a86c224537cc6b2c','89ad3d1099dbff6246678fde59bd1106','d388bd4ea4f627887bb5c7905d965854','b7ff3447d34eb31a6d35f2a2e5396e3e','b9f2fb744fc0bb378702150862b65fea','8f040bc1c9b8aaa027465c06781dab70','24881de0a74f6a19005be2b4ccae93fe','a9aa2cd91f2802bc20a0c38ac032b941','0ab0d530c2c240f57c924393898ca296','2a7170cff792ccc8909c7794234aa8d5','80faac939371b5283789ae90e17921d9','87c58cfcde70147aef924008d2d8f253','cf6fe4ea9c8f712f4c30370d95c759e4','aba6e542728feea322cd973825933ff4','8c4e44e5f3213d205a588be8fbfba82b','776768affab74e4b82e02b927e662e99','0e425fd9639940e446d739de7d01ea37','841dc2a4bbe51aa5e8539036e85e2680','f1f5e99653dded71237101ce5c4aa666','cb3b79930099076a7bacbf8dfa7c0bcf','b23c50e8a29c4da4070c70c5a11acacb','75200194d3f83b2602649fbde07057c8','d95ccef91cd57c65045718a2e4111999','4390b8fdf4e735df31eb5b7c4ec348b6','6521f44153c8736fc361f111d8d47971','733f503515afa7fd446a4a93a8c46a09','71608e41582457a036eaacda2d880b19','9cb8613ab580710a023c54527a5fa81f','8d8f8c0484df6a5f33fae91a3d238480','bd91b48615f5bf4e8cc4168e535dc995','9355483278540cad479609cd2005aee2','c149378d2f691d0b2c6e89806b6ab00d','6445ed09a03b725b60933928697184bc','1bd0f77d90497c45c5ccc001d20e6f2e','ac58f6211fbfc4c7772186bfd85ecb81','e7edd9aa8597cb752786a0c2aaccd0ff','cf9a6a47e63ed68aa0edca2b9b40e739','e684bd7ecaeeac1e4fff6eb9ffed0f81','9c0cfee1141b5de241ce628000c0af29','1796edaf1af90a0f008c1a164f439a4f','1aa1419375bb35f8f97881da0d531b3d','6064359ed28ed4df450e2891099a3982','8cb7e0ec18e351eec569c5db1660b9e3','f776afb76cddb0b074ebbaf0486bad88','3ea1ba35363c50cd9d425d1c9a940891','46d2b010b62e677a81ba45c2ee519744','e028c172d58416ecc1210d45c93537bc','52bb4ae34531c795e384fd1da9c25911','7fed1f4359e0c778323bcfe676d621fb','f143ac61ed3bd4b0a546f477be490f6c','f5a52a10a5a07eeb35e9ec0eb7c36354','ee511809bb9003fdfc3d8aba77d9815f','2c88b7c46c21d7717374d399d16f8a8a','29c94f40b2ec3432cda6d3ddbdc4aa4c','e80f8e2c93de662705ba7c5b4030364d','8c4e44e5f3213d205a588be8fbfba82b','7f69ab5502b8bc38ea0b2f1d5f71ba89','7f2b71d627e191ce41468ccd4c063ea9','81ccb733f1c5e2ce7157444e79c2e10e','258e08f3b9af6f8884cb827383375ff1','773cdd7c47040689720c50fc2b1ab654','7fed1f4359e0c778323bcfe676d621fb','6295d50209de58aeed9044e4525233fe','9fd2de122008279405a41751431c408e','71608e41582457a036eaacda2d880b19','1aa1419375bb35f8f97881da0d531b3d','35f494c4b9de1e8d7792e437ece08fde','f150fb45c409b9396b342dbf18bdffcc','054113ce7fce5639ddd3f6578ad5b7c7','d499304e4d62e5fc77afbb216b57a61f','6de644a772643eb83ed172d8ca95fb60','e36dc971e83c9f3d26b160fbb1bd3774','f3ba7b0c750e566ac0e658f380c45feb','6de644a772643eb83ed172d8ca95fb60','a8f1e18288d21b7500db3c22d78f0be5','50c217b6b9466dbdd62347dd1415439e','ffa5141f84340b6842e17c2314c77f51','289bc0a8d286b0ef8eace8040b3fc771','bdf534c6c6c15c51b88127c1842d0d88','2a7170cff792ccc8909c7794234aa8d5','7a8e685b3582e1eed8f89fe3a44dc1eb','b23c50e8a29c4da4070c70c5a11acacb','72653f8709359b20b9ca01fc02d3a6af','20335ffee55dca31177cbd255148ccdd','24881de0a74f6a19005be2b4ccae93fe','a649a47188603920d6907ba23e415d29','208df0b327495d598b9117b9a690e257','fd3874a4f767c4964c09e31da09a0ee6','2a41e204a83dfc01fab15ae769547467','c70f486cfccce089a40e2bb85046fa79','ba9dd5490a2224ba6f37c5c4485532fc','4b17ad46286d1a116bc6a67bcd558f52','65d63b85538e05ad37add9cbc699572c','f46b911a9400d646e69c3c41552bc69b','5a5ba513f2fddf565bd3f67932b1acdf','200944eab2c2f9e1b34f324a597dc933','6e6079feba50ee07bd23cc260c3b97af','5a16c2a66f91683c59c69455db4dd300','36c0d4e09d7daa2536d4c60519754388','7a724d8f143b384d0d5ce605c4c33726','2b9fa7cae2af103987457653bbeac6ce','e7b9348c6e9e1c265b16b2f6e705ac2b','25879ebbc56b2887e7208242166571f5','f89c6b0456bec1902bef98b21c160932','4e7a652a59918e4da793e0ebb0efdf6e','819723f8c9b874589d1e40fdd927ba07','f1f5e99653dded71237101ce5c4aa666','be7570bc428e43f44fe5ee8599844af4','4cc220d58054608d16daf68cc44a3e78','91f054d27681d3ba2110fdd4f48bf1d1','6f8492adbd7155df0524770bc4d2f4b4','26dde25acd8d0ade10af5422c7d468bb','ca68d9c7275d83d42e3643abb12d0972','f150fb45c409b9396b342dbf18bdffcc','9c0bac8e21352be61cdcfe67ce1345de','d96982934ff0f953dfb3626bf1d6eb10','9cb8613ab580710a023c54527a5fa81f','9d8dffcf4f7c7b4f1c193fa0faad9f58','7fed1f4359e0c778323bcfe676d621fb','ff768bb0ba3c89cb4686724e3a8d619d','b6eb5295145f4858d9c7ad19f81dd5ce','b986d1a5fc646454d0fa0b3dcfa89acb','4fe279c0cd216aa761644502caeef8c1','841dc2a4bbe51aa5e8539036e85e2680','df2180b87f98c0eb3f3e448304a44389','a8f1e18288d21b7500db3c22d78f0be5','b3aaee98818211578c34ecbc78427293','ecc661d65f90b05c9a6df007fd6c7978','78b7354f48a5603cc3a9e0da3bd4eee8','468ef7722237e480a96a980cc7ebc671','c19318f9e523cee5fd69b50204652922','8df7603a37f484e918fa66645f2916b7','8c186433aa31825d19524784d9a5af1c','7fed1f4359e0c778323bcfe676d621fb','b905139955b5283adc92fb6e64a1d851','3089035e35ba9fe0966879d170504784','9da963b751f1aa6e28852014804ec99c','cd353e2497c5406186ca5b8299c753cb','b45ee22f41a2cdf28b9240ab88702ec6','9c83c067083e033adeb333f5db39e984','d9eb3ba55998e698747531b2d0e3c3ac','235af818be39a9b751d593e6123604e5','6ba6d4eed9a30c4e8aa2fe738165f96d','196cbad6acc3adcf9e6e9d082b79bab3','a70d7198be5f234f3d6f9bb4300de304','0b8e3ab4b00cfe397657af80c7c7a5ce','036ee508fe4f70a63d16bf2bd8ccc69e','641f4ca22d1f8158c78127162d69a34b','f53db7d6c5e134a9e42228cf51835431','7500ad3a63ff1660e91ec51a7a063053','a86abf92bbd27cdd79677d9d8b6a2093','7b622635727c182006e29eafa3d97581','a70d7198be5f234f3d6f9bb4300de304','a9aa2cd91f2802bc20a0c38ac032b941','2a41e204a83dfc01fab15ae769547467','8541c79c0cfcff631d090dcf17947aa1','7abb776830a7a60eb165dc387a7bd638','1ae673df7a88026807eb41886d7d2686','7a2be8c2f87524075a13a87023224b2c','5a5ba513f2fddf565bd3f67932b1acdf','cc6a5470a4cba931766f1c3f0754c1fb','373e3b8e0b1316cdc4ffdbc1d7e41ebc','5141a0ad7bb35fbf3f55fcfa3ee97d22','fa0f11ae9b8391a18597bf934b4ab61f','b9eba62a6e0a26cbbde9b3dc074752b3','af319cc18edad5a13b5785f66a579252','d91c2604198e7317baa3d21b2273184d','5c0a29c8bd568ac1fbbff52ff235455a','768b48a1576217f081164afd3fc230e8','c149378d2f691d0b2c6e89806b6ab00d','9355483278540cad479609cd2005aee2','b7ff3447d34eb31a6d35f2a2e5396e3e','497269cbae7fda974a5fe2b8c067ffdc','94fe70ffae383d5d9b70218de6e86cea','5cacd357f9e72b43207c0da6031fed14','cc3df3b0f96f2bb73731a0e8da3e9bec','743e8110d2cb06c2eab4dbd352d37f5c','f072deafce8843246deadbb71a2dc426','bbd827575288d3dcf38a5cc199fd7908','3bc824a3a8cd4dadfe4e51e09de72ae2','55e7ccc096f3bb17ceddeab34def96ac','a4966c87d6c5d36f85ed35bbafced696','d028336506c24657d0f532b78cea3a61','91fec1f3b03354eae44b633f7cbec7a1','bd3e2752460f947a97d6f278a7fe77ab','d5825d8f9d5d84fdaf364c1242f80459','cb7f957f92e1bd38b7fc3ed9f9129769','e202ba9fe0b8c4c1fff67639f6f7f2d1','bbd827575288d3dcf38a5cc199fd7908','8c4e44e5f3213d205a588be8fbfba82b','84edecc81610b7666aacd4009fd700b7','9355483278540cad479609cd2005aee2','f3d7f08b3e21dcae119ab02b34a79d3e','8541c79c0cfcff631d090dcf17947aa1','b6090ef1fd84764c0b686736f19a7d2f','f72bea3cd406d84e64110bc82c1fc531','939247d9bdbbb2f743e225f4b7946995','7e087f55dcd6acd53e90417b57d1567f','0d4e756e84af9aa6ca00b519067b61ea','43fbf700a394e885af08251015f61da5','899ff68b15b6c6b28f9b298e76a09e93','f46b911a9400d646e69c3c41552bc69b','220cd5510a833387b801ed5ca40220c0','845ba941d4273560a6a41256a7534ab1','ce1bfb119f7b87dba35d2f2fcdcf3c1b','84447bc2f3e77e5b61db6f08cc772345','089bb2af72ecee2993809d0e8e40f5a5','6c697c8d5072c4630d5d768fe3153b08','7a2879db56243e137cbebf56c548f89c','20a3338c228437d339ae6a2cddbacbe5','fd3874a4f767c4964c09e31da09a0ee6','96250a695cb2f84bf964e325618f1076','96b06d247768b295aef4e49458cd7f8a','9bd489efc11ed014424d373b978d8cb3','4e4778943e543724545a19617556a5db','fffde930cbc1a155d2aaa896c1061c79','7abb776830a7a60eb165dc387a7bd638','daa1c84af5779f1f022f51c89ce8ab1e','4994355e15835182b69c5a249012d102','a4371a5556cba3717ca305626fd9c2bb','8541c79c0cfcff631d090dcf17947aa1','20f22ef176f174ca04db0171bec90ff7','e0774acdf02487d66a9b4b4831e43614','7fed1f4359e0c778323bcfe676d621fb','e9d55923a3a7104660e326b402571795','ad247b1a4af88bd7bad6705c14a44111','a39d7048f7196fb16465d63c823812c6','61c40b6d7dc270b06d21031afb309645','9b2d73b570b2b22d4ccdd7e0f2035bc2','8541c79c0cfcff631d090dcf17947aa1','cdcffa4009281f04d073cb546fc303f5','1a1d86e5c11469c7a01bc4996b04dada','a431c06c70e9411ecdd827768946ed1c','7ad1e93fc037f50667e4c07ad21fc214','f150fb45c409b9396b342dbf18bdffcc','463b7d4d1be800ef7f2a912ba991a55b','96b06d247768b295aef4e49458cd7f8a','2f4eb187098f937ef55c0fb3d66bb82d','9355483278540cad479609cd2005aee2')

--amazon products
select qpm.business_id,
       json_extract_path_text(qpm.json_args, 'lob_app_json', 'retail_market_amazon_seller_id', true) as seller_id,
       qpm.creation_time
from dwh.quotes_policies_mlob qpm
where qpm.lob_policy = 'GL'
  and qpm.cob = 'E-Commerce'
  and qpm.creation_time >= '2025-01-01'

-- amazon retail products datapoints
with data_points_pivoted as (
    select
        uq.business_id,
        -- Pivot each data_point_id to a column
        max(case when dp.data_point_id = 'aircraft_vehicles_sales' then dp.value end) as aircraft_vehicles_sales,
        max(case when dp.data_point_id = 'antiques_dealers' then dp.value end) as antiques_dealers,
        max(case when dp.data_point_id = 'battery_sales' then dp.value end) as battery_sales,
        max(case when dp.data_point_id = 'cannabis_sales' then dp.value end) as cannabis_sales,
        max(case when dp.data_point_id = 'children_furniture_sales' then dp.value end) as children_furniture_sales,
        max(case when dp.data_point_id = 'children_sleepwear_sales' then dp.value end) as children_sleepwear_sales,
        max(case when dp.data_point_id = 'developmental_disorder_product_sales' then dp.value end) as developmental_disorder_product_sales,
        max(case when dp.data_point_id = 'exercise_equipment_sales' then dp.value end) as exercise_equipment_sales,
        max(case when dp.data_point_id = 'firearms_sales' then dp.value end) as firearms_sales,
        max(case when dp.data_point_id = 'gasoline_sales' then dp.value end) as gasoline_sales,
        max(case when dp.data_point_id = 'heavy_machinery_motor_sales' then dp.value end) as heavy_machinery_motor_sales,
        max(case when dp.data_point_id = 'high_fire_hazard_product_sales' then dp.value end) as high_fire_hazard_product_sales,
        max(case when dp.data_point_id = 'infant_toddler_sales' then dp.value end) as infant_toddler_sales,
        max(case when dp.data_point_id = 'location.appliances_category_sales' then dp.value end) as appliances_category_sales,
        max(case when dp.data_point_id = 'location.automotive_powersports_category_sales' then dp.value end) as automotive_powersports_category_sales,
        max(case when dp.data_point_id = 'location.baby_products_category_sales' then dp.value end) as baby_products_category_sales,
        max(case when dp.data_point_id = 'location.books_music_movies_video_games_category_sales' then dp.value end) as books_music_movies_video_games_category_sales,
        max(case when dp.data_point_id = 'location.car_seats_sales' then dp.value end) as car_seats_sales,
        max(case when dp.data_point_id = 'location.childrens_products_sales' then dp.value end) as childrens_products_sales,
        max(case when dp.data_point_id = 'location.cleaning_products_sales' then dp.value end) as cleaning_products_sales,
        max(case when dp.data_point_id = 'location.clothing_jewelry_accessories_category_sales' then dp.value end) as clothing_jewelry_accessories_category_sales,
        max(case when dp.data_point_id = 'location.collectibles_category_sales' then dp.value end) as collectibles_category_sales,
        max(case when dp.data_point_id = 'location.computer_electronics_category_sales' then dp.value end) as computer_electronics_category_sales,
        max(case when dp.data_point_id = 'location.corded_window_coverings_sales' then dp.value end) as corded_window_coverings_sales,
        max(case when dp.data_point_id = 'location.decorative_lighting_sales' then dp.value end) as decorative_lighting_sales,
        max(case when dp.data_point_id = 'location.food_beverage_category_sales' then dp.value end) as food_beverage_category_sales,
        max(case when dp.data_point_id = 'location.furniture_sales' then dp.value end) as furniture_sales,
        max(case when dp.data_point_id = 'location.health_beauty_category_sales' then dp.value end) as health_beauty_category_sales,
        max(case when dp.data_point_id = 'location.heating_cooling_sales' then dp.value end) as heating_cooling_sales,
        max(case when dp.data_point_id = 'location.home_and_garden_category_sales' then dp.value end) as home_and_garden_category_sales,
        max(case when dp.data_point_id = 'location.misc_category_sales' then dp.value end) as misc_category_sales,
        max(case when dp.data_point_id = 'location.non_educational_video_games_sales' then dp.value end) as non_educational_video_games_sales,
        max(case when dp.data_point_id = 'location.office_products_category_sales' then dp.value end) as office_products_category_sales,
        max(case when dp.data_point_id = 'location.perishable_foods_sales' then dp.value end) as perishable_foods_sales,
        max(case when dp.data_point_id = 'location.pet_medication_sales' then dp.value end) as pet_medication_sales,
        max(case when dp.data_point_id = 'location.pet_supplies_category_sales' then dp.value end) as pet_supplies_category_sales,
        max(case when dp.data_point_id = 'location.software_category_sales' then dp.value end) as software_category_sales,
        max(case when dp.data_point_id = 'location.sports_outdoors_category_sales' then dp.value end) as sports_outdoors_category_sales,
        max(case when dp.data_point_id = 'location.swimming_pool_toys_sales' then dp.value end) as swimming_pool_toys_sales,
        max(case when dp.data_point_id = 'location.toys_category_sales' then dp.value end) as toys_category_sales,
        max(case when dp.data_point_id = 'magnet_sales' then dp.value end) as magnet_sales,
        max(case when dp.data_point_id = 'medical_equipment_sales' then dp.value end) as medical_equipment_sales,
        max(case when dp.data_point_id = 'nutraceuticals_sales' then dp.value end) as nutraceuticals_sales,
        max(case when dp.data_point_id = 'performance_food_sales' then dp.value end) as performance_food_sales,
        max(case when dp.data_point_id = 'permanent_ink_sales' then dp.value end) as permanent_ink_sales,
        max(case when dp.data_point_id = 'pesticide_fertilizer_sales' then dp.value end) as pesticide_fertilizer_sales,
        max(case when dp.data_point_id = 'pharmaceuticals_sales' then dp.value end) as pharmaceuticals_sales,
        max(case when dp.data_point_id = 'play_equipment_sales' then dp.value end) as play_equipment_sales,
        max(case when dp.data_point_id = 'raw_meat_seafood_sales' then dp.value end) as raw_meat_seafood_sales,
        max(case when dp.data_point_id = 'safety_equipment_sales' then dp.value end) as safety_equipment_sales,
        max(case when dp.data_point_id = 'sexually_explicit_materials_sales' then dp.value end) as sexually_explicit_materials_sales,
        max(case when dp.data_point_id = 'tobacco_sales' then dp.value end) as tobacco_sales,
        max(case when dp.data_point_id = 'water_absorbing_gel_beads_sales' then dp.value end) as water_absorbing_gel_beads_sales,
        max(case when dp.data_point_id = 'weapon_sales' then dp.value end) as weapon_sales,
        max(case when dp.data_point_id = 'weight_loss_food_sales' then dp.value end) as weight_loss_food_sales
    from dwh.underwriting_quotes_data uq
             join underwriting_svc_prod.lob_applications_data_points dp using (lob_application_id)
    where uq.cob = 'E-Commerce'
    group by uq.business_id
)
select
    distinct qpm.business_id,
             json_extract_path_text(qpm.json_args, 'lob_app_json', 'retail_market_amazon_seller_id', true) as seller_id,
             aircraft_vehicles_sales,
             antiques_dealers,
             battery_sales,
             cannabis_sales,
             children_furniture_sales,
             children_sleepwear_sales,
             developmental_disorder_product_sales,
             exercise_equipment_sales,
             firearms_sales,
             gasoline_sales,
             heavy_machinery_motor_sales,
             high_fire_hazard_product_sales,
             infant_toddler_sales,
             appliances_category_sales,
             automotive_powersports_category_sales,
             baby_products_category_sales,
             books_music_movies_video_games_category_sales,
             car_seats_sales,
             childrens_products_sales,
             cleaning_products_sales,
             clothing_jewelry_accessories_category_sales,
             collectibles_category_sales,
             computer_electronics_category_sales,
             corded_window_coverings_sales,
             decorative_lighting_sales,
             food_beverage_category_sales,
             furniture_sales,
             health_beauty_category_sales,
             heating_cooling_sales,
             home_and_garden_category_sales,
             misc_category_sales,
             non_educational_video_games_sales,
             office_products_category_sales,
             perishable_foods_sales,
             pet_medication_sales,
             pet_supplies_category_sales,
             software_category_sales,
             sports_outdoors_category_sales,
             swimming_pool_toys_sales,
             toys_category_sales,
             magnet_sales,
             medical_equipment_sales,
             nutraceuticals_sales,
             performance_food_sales,
             permanent_ink_sales,
             pesticide_fertilizer_sales,
             pharmaceuticals_sales,
             play_equipment_sales,
             raw_meat_seafood_sales,
             safety_equipment_sales,
             sexually_explicit_materials_sales,
             tobacco_sales,
             water_absorbing_gel_beads_sales,
             weapon_sales,
             weight_loss_food_sales
from dwh.quotes_policies_mlob qpm
         left join data_points_pivoted dpp on dpp.business_id = qpm.business_id
where qpm.lob_policy = 'GL'
  and qpm.cob = 'E-Commerce'
  and qpm.creation_time >= '2025-01-01'
  and json_extract_path_text(qpm.json_args, 'lob_app_json', 'retail_market_amazon_seller_id', true) <> ''

-- GA restaurant action
select
    (CASE
            WHEN (qpm.state = 'GA') then 'true'
            else 'false' end) as georgia_policy,
    qpm.offer_flow_type,
    count(distinct qpm.business_id) as biz_count
from dwh.quotes_policies_mlob qpm
where qpm.highest_policy_status >= 3
  and qpm.lob_policy = 'GL'
  and qpm.cob = 'Restaurant'
  and qpm.offer_flow_type in ('APPLICATION', 'RENEWAL', 'CANCEL_REWRITE')
  and qpm.creation_time >= '2025-01-01'
group by 1, 2
order by 1 asc;

--all policy datapoints DPs PDPs given a business_id
select distinct business_id,
       data_point_id,
       value,
       original_execution_status as quote_status
from dwh.underwriting_quotes_data
left join underwriting_svc_prod.lob_applications_data_points
    using(lob_application_id)
where data_point_id = 'building.fire_alarm_devices'
      and lob = 'CP'
      and value = ''
limit 10