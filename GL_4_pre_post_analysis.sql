WITH qpm as (select *, json_extract_path_text(current_amendment, 'version', TRUE) as amendment_version,
       case when amendment_version ISNULL then '1.0' else amendment_version END AS GL_Amendment,
       nvl(json_extract_path_text(json_args,'year_business_started',true),'') as year_business_started
from dwh.quotes_policies_mlob
    where creation_time>='2022-01-01'),

fcra as (
    select distinct business_id,
           last_value(score) over (partition by business_id order by creation_time rows between unbounded preceding and unbounded following ) as fcra_score
           from riskmgmt_svc_prod.risk_score_result
    order by business_id desc),

raven as (

    select distinct business_id,
     last_value(score) over (partition by business_id order by creation_time rows between unbounded preceding and unbounded following ) as raven_score,
     last_value(bin_num) over (partition by business_id order by creation_time rows between unbounded preceding and unbounded following ) as raven_bin
from risk_model_svc_prod.risk_score_requests
order by business_id desc),


srf as (select job_id, trim(json_extract_path_text(calculation,'p2 <- schedule rating rate','schedule_rating_factor', true)) as SRF
from s3_operational.rating_svc_prod_calculations
where lob = 'GL')

select * from qpm
    left join fcra on fcra.business_id = qpm.business_id
    left join raven on raven.business_id = qpm.business_id
    left join srf on case when highest_status_package = 'basic' then basic_quote_job_id
                when highest_status_package='basicTria' then basic_tria_quote_job_id
                when highest_status_package= 'pro' then pro_quote_job_id
                when highest_status_package= 'proTria'then pro_tria_quote_job_id
                when highest_status_package='proPlus' then pro_plus_quote_job_id
                when highest_status_package='proPlusTria' then pro_plus_tria_quote_job_id
                else pro_quote_job_id
                end = srf.job_id





-- select *, trim(json_extract_path_text(calculation,'p2 <- schedule rating rate','schedule_rating_factor', true)) as SRF
-- from s3_operational.rating_svc_prod_calculations
-- where lob = 'GL' and job_id = '33899173'




