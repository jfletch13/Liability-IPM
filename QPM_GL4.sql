with final as(
select qpm.creation_time,
       qpm.offer_id,
       qpm.highest_policy_id,
       qpm.purchase_date,
       --qpm.current_amendment,
       qpm.insurance_product,
       --ds.channel_type_attributed,
       --ds.distribution_channel_attributed,
       ds.agency_type,
       ds.channel,
       --use channel from DS
       --CASE WHEN ds.agency_type = 'Wholesaler' and distribution_channel = 'agents' then 'Wholesale Agents' else ds.channel end as Distribution_channel_revised,
       --ds.agency_aggregator,
       qpm.business_id, --related_business_id,
       --distribution_channel,
       qpm.lob_policy,
       qpm.cob, qpm.cob_group, qpm.state, qpm.num_of_employees,
       --years_of_experience,
       nvl(json_extract_path_text(json_args,'year_business_started',true),'') as year_business_started,
       revenue_in_12_months,
       highest_yearly_premium, policy_start_date:: date, /*policy_end_date, p.end_date :: date,*/ qpm.highest_status_package, qpm.highest_policy_status, qpm.highest_status_name,
       row_number() over (partition by qpm.business_id order by qpm.creation_time desc) as offer_rank,
       json_extract_path_text(current_amendment, 'version', TRUE) as amendment_version ,
       case when amendment_version ISNULL then '1.0' else amendment_version END AS GL_Amendment,
                                     offer_flow_type,

       basic_premium_before_minimum, basic_minimum_premium, basic_yearly_premium

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
        --fcra_score /*isnull  then 'No-Hit_No-Score' else fcra_score end as fcra_score*/, raven_score, raven_bin
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
    --left join fcra on fcra.business_id = qpm.business_id
    --left join raven on raven.business_id = qpm.business_id
    --left join yahoo on yahoo.offer_id = qpm.offer_id
    --left join nimi_svc_prod.policies p on qpm.highest_policy_id = p.policy_id
    left join dwh.company_level_metrics_ds DS on qpm.highest_policy_id = ds.policy_id
where qpm.lob_policy = 'GL'
  and qpm.cob_group IN ('Artisan contractor', 'Cleaning', 'Construction')
--and qpm.creation_time >= '2019-01-01'
and qpm.highest_policy_status >= 3
order by qpm.business_id, qpm.creation_time)

select *
from final
--where offer_rank = 1
limit 500

/*select distinct extract(year from policy_start_date), count( highest_policy_id)
from final
where offer_rank=1
group by 1
order by 1 desc
