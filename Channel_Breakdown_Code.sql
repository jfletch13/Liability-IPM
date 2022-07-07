-- rc not currently being used, but may incorporate in the future
with rc as (select distinct job_id,
                       total_premium,
                       --calculation,
                       trim(split_part(json_extract_path_text(calculation,'exposure base','payroll exposure', 'factor', true), '|',1)) as payroll_factor,
                       trim(json_extract_path_text(calculation,'exposure base','payroll exposure', 'y1 <<- declared owner payroll', true)) as declared_payroll,
                       trim(split_part(json_extract_path_text(calculation,'ni factors','payroll ni Factor', true), '|',1)) as payroll_ni_factor,
                       json_extract_path_text(calculation_summary, 'lob specific', 'ni factor', true) as ni_factor,
                        -- find the FCRA no-hit policies
                       trim(split_part(json_extract_path_text(calculation,'ni factors','n1 <<- risk Score Factor (riskScoreGroup)', true),'|',1)) as ni1,
                    trim(json_extract_path_text(calculation,'p2 <<- schedule rating rate','factor', true)) as SRF,
                    json_extract_path_text(calculation_summary, 'lob specific', 'exposure base type', true) as exposure_base_type,

                    -- revenue calc payroll
                    trim(split_part(json_extract_path_text(calculation,'exposure base','payroll exposure', 'w1 <<- option 1', true), '|',1)) as rev_calc_payroll_w1,
                    trim(split_part(json_extract_path_text(calculation,'cobs', 'exposure bases', 'payroll exposure', 'w1 <<- option 1', true), '|',1)) as rev_calc_payroll_w1_2,
                    trim(split_part(json_extract_path_text(calculation,'exposure base','payroll exposure', 'y3 <<- option 2', true), '|',1)) as rev_calc_payroll_y3,

                    -- minimum payroll
                    trim(split_part(json_extract_path_text(calculation,'exposure base','payroll exposure', 'w3 <<- option 3', true), '|',1)) as min_payroll_w3,
                    trim(split_part(json_extract_path_text(calculation,'cobs', 'exposure bases', 'payroll exposure', 'w3 <<- option 3', true), '|',1)) as min_payroll_w3_2,
                    trim(json_extract_path_text(calculation,'exposure base','payroll exposure', 'y2 <<- minimum payroll', true)) as min_payroll_y2


                        --json parsing when num_of_employees == 1
                       --trim(split_part(json_extract_path_text(calculation, 'exposure base','gross sales exposure', 'factor', true), '|',1)) as gross_sales_factor,
                       --trim(split_part(json_extract_path_text(calculation,'cobs','calculated base premium', true), '|',1)) as calc_base_premium,
                       --trim(split_part(json_extract_path_text(calculation,'cobs','min premium', true), '|',1)) as min_premium,
                       --trim(json_extract_path_text(calculation,'exposure base','payroll exposure', 'w1 <<- option 1', true)) as w1,
                      --trim(json_extract_path_text(calculation,'exposure base','payroll exposure', 'n1 <<- owner payroll adjustment', true)) as n1,
                       --trim(split_part(json_extract_path_text(calculation,'exposure base','payroll exposure', 'n1 <<- owner payroll adjustment', true), '|',1)) as n1,

       from s3_operational.rating_svc_prod_calculations
       where lob = 'GL'
       and creation_time >= '2021-12-01'
    ),
/*
yahoo as (
        select offer_id, 1 as yahoo_email_bot
        from underwriting_svc_prod.offers o
        left join underwriting_svc_prod.lob_applications la
        on o.lob_application_id = la.lob_application_id
        left join underwriting_svc_prod.prospects p
        on la.prospect_id = p.prospect_id
        where lob_application_type = 'APPLICATION'
        and email ilike '%@yahoo.com%'),*/

--GAAP not being used. keeping in for future proofing and to look up EP calcs
/*GAAP AS (
        select policy_id,
          sum(case when months_since_start_year > 12 then 0 else gaap.earned_premium end) as earned_premium
        FROM gaap.
        group by policy_id
)*/

    --pulls FCRA score data and sorts by most recent, line 59 sorts to only look at most recent fcra score based on creation date of unique business id
fcra as (
    select distinct business_id,
           last_value(score) over (partition by business_id order by creation_time rows between unbounded preceding and unbounded following ) as fcra_score
           from riskmgmt_svc_prod.risk_score_result
    order by business_id desc),
--check this table, do we reorder fcra scores after binding?

    --not currently being used, but may use going forward
raven as (

    select distinct business_id,
     last_value(score) over (partition by business_id order by creation_time rows between unbounded preceding and unbounded following ) as raven_score,
     last_value(bin_num) over (partition by business_id order by creation_time rows between unbounded preceding and unbounded following ) as raven_bin
from risk_model_svc_prod.risk_score_requests
order by business_id desc),

     final as (
select qpm.creation_time,
       qpm.offer_id,
       highest_policy_id,
       purchase_date,
       --ds.channel_type_attributed,
       --ds.distribution_channel_attributed,
       ds.agency_type,
       --case statement to determine which agents are wholesalers, using dwh.company_level_metrics table
       CASE WHEN ds.agency_type = 'Wholesaler' and ds.channel = 'Agent' then 'Wholesale Agents' else ds.channel end as Distribution_channel_revised,
       --ds.agency_aggregator,
       qpm.business_id, related_business_id,
       --distribution_channel,
       lob_policy,
       cob, qpm.cob_group, qpm.state, qpm.num_of_employees,
       --years_of_experience,
       nvl(json_extract_path_text(json_args,'year_business_started',true),'') as year_business_started,
       revenue_in_12_months,
       highest_yearly_premium, policy_start_date:: date, /*policy_end_date,*/ p.end_date :: date, highest_status_package, highest_policy_status, highest_status_name,
       -- line 93 is step 1 to eliminate duplicates, orders by creation time descending, so #1 is most recent
       row_number() over (partition by qpm.business_id order by qpm.creation_time desc) as offer_rank,
       json_extract_path_text(current_amendment, 'version', TRUE) as amendment_version, offer_flow_type,

       basic_premium_before_minimum, basic_minimum_premium, basic_yearly_premium,

      /*  case when highest_status_package = 'proPlus' then pro_plus_premium_before_minimum
            when highest_status_package = 'proPlusTria' then pro_plus_tria_premium_before_minimum
            when highest_status_package = 'pro' then pro_premium_before_minimum
            when highest_status_package = 'proTria' then pro_tria_premium_before_minimum
            when highest_status_package = 'basic' then basic_premium_before_minimum
            when highest_status_package = 'basicTria' then basic_tria_premium_before_minimum end as basePremiumBeforeMinimum,

        case when highest_status_package = 'proPlus' then pro_plus_minimum_premium
            when highest_status_package = 'proPlusTria' then pro_plus_tria_minimum_premium
            when highest_status_package = 'pro' then pro_minimum_premium
            when highest_status_package = 'proTria' then pro_tria_minimum_premium
            when highest_status_package = 'basic' then basic_minimum_premium
            when highest_status_package = 'basicTria' then basic_tria_minimum_premium end as min_premium,

       case when highest_status_package = 'basic' then basic_quote_job_id
                when highest_status_package='basicTria' then basic_tria_quote_job_id
                when highest_status_package= 'pro' then pro_quote_job_id
                when highest_status_package= 'proTria'then pro_tria_quote_job_id
                when highest_status_package='proPlus' then pro_plus_quote_job_id
                when highest_status_package='proPlusTria' then pro_plus_tria_quote_job_id
                else pro_quote_job_id
                end as quote_job_id,*/

       -- rating calc table
        --rc.*,
        fcra_score /*isnull  then 'No-Hit_No-Score' else fcra_score end as fcra_score*/, raven_score, raven_bin
        --case when yahoo_email_bot is null then 0 else 1 end as yahoo_email_bot
    from dwh.quotes_policies_mlob qpm
    /*left join rc on case when highest_status_package = 'basic' then basic_quote_job_id
                when highest_status_package='basicTria' then basic_tria_quote_job_id
                when highest_status_package= 'pro' then pro_quote_job_id
                when highest_status_package= 'proTria'then pro_tria_quote_job_id
                when highest_status_package='proPlus' then pro_plus_quote_job_id
                when highest_status_package='proPlusTria' then pro_plus_tria_quote_job_id
                else pro_quote_job_id
                end = rc.job_id*/
    left join fcra on fcra.business_id = qpm.business_id
    left join raven on raven.business_id = qpm.business_id
    --left join yahoo on yahoo.offer_id = qpm.offer_id
    left join nimi_svc_prod.policies p on qpm.highest_policy_id = p.policy_id
    left join dwh.company_level_metrics_ds DS on qpm.highest_policy_id = ds.policy_id
where lob_policy = 'GL'
  and qpm.cob_group IN ('Artisan contractor', 'Cleaning', 'Construction')
--and qpm.creation_time >= '2019-01-01'
and qpm.highest_policy_status >= 3
order by qpm.business_id, qpm.creation_time
)



Select * --extract(year from policy_start_date) AS year, highest_policy_id, highest_status_name, cob_group, highest_yearly_premium
from final
where offer_rank =1

limit 500

/*select extract(year from policy_start_date) AS year, highest_status_name, SUM(highest_yearly_premium)
from final
where offer_rank=1
      group by 1,2
order by 1 desc


--USE FOR SPOT CHECKING
/*Select  distribution_channel_revised,
count(*)
from final
group by distribution_channel_revised
having count(*) >1
order by Distribution_channel_revised desc
  dwh.company_level_metrics_ds
 */

