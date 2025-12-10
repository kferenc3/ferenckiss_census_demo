with base as (
    select
        l.lead_id,
        l.company,
        l.first_name,
        l.last_name,
        l.title,
        l.email,
        l.industry,
        l.lead_source,
        l.rating,
        l.status,
        l.number_of_employees,
        l.annual_revenue,
        l.country,
        l.created_date,

        m.task_count_30d,
        m.completed_task_count_30d,
        m.last_activity_date,
        m.days_since_last_activity
    from {{ ref('stg_lead') }} l
    left join {{ ref('int_lead_activity_metrics') }} m
        on l.lead_id = m.lead_id
),

scored as (
    select
        *,
        coalesce(task_count_30d, 0)             as task_count_30d_coalesced,
        coalesce(completed_task_count_30d, 0)   as completed_task_count_30d_coalesced,
        coalesce(days_since_last_activity, 999) as days_since_last_activity_coalesced
    from base
),

demographic as (
    select
        *,
        -- Title-based points
        case
            when upper(title) like '%VP%' 
              or upper(title) like '%VICE PRESIDENT%' 
              or upper(title) like '%CHIEF%' 
              or upper(title) like '%CFO%' 
              or upper(title) like '%CEO%'
                then 30
            when upper(title) like '%DIRECTOR%' then 25
            when upper(title) like '%MANAGER%'  then 20
            else 10
        end as title_points,

        -- Company size-based points
        case
            when number_of_employees >= 1000 then 20
            when number_of_employees >= 200  then 15
            when number_of_employees >= 50   then 10
            when number_of_employees > 0     then 5
            else 0
        end as size_points,

        -- Industry-based points (example)
        case
            when upper(industry) in ('SOFTWARE', 'TECHNOLOGY', 'INTERNET') then 10
            when upper(industry) in ('FINANCIAL SERVICES', 'BANKING')      then 8
            else 5
        end as industry_points
    from scored
),

behavioral as (
    select
        *,
        -- Recency points
        case
            when days_since_last_activity_coalesced <= 300   then 20
            when days_since_last_activity_coalesced <= 600   then 15
            when days_since_last_activity_coalesced <= 800  then 10
            when days_since_last_activity_coalesced <= 1000  then 5
            else 0
        end as recency_points,

        -- Activity volume points (completed tasks)
        case
            when completed_task_count_30d_coalesced >= 10 then 20
            when completed_task_count_30d_coalesced >= 5  then 15
            when completed_task_count_30d_coalesced >= 2  then 10
            when completed_task_count_30d_coalesced >= 1  then 5
            else 0
        end as activity_points
    from demographic
),

final_scores as (
    select
        lead_id,
        company,
        first_name,
        last_name,
        title,
        email,
        industry,
        lead_source,
        rating,
        status,
        number_of_employees,
        annual_revenue,
        country,
        created_date,
        task_count_30d_coalesced               as task_count_30d,
        completed_task_count_30d_coalesced     as completed_task_count_30d,
        last_activity_date,
        days_since_last_activity_coalesced     as days_since_last_activity,
        title_points,
        size_points,
        industry_points,
        recency_points,
        activity_points,
        (title_points + size_points + industry_points)     as demographic_score,
        (recency_points + activity_points)                 as behavioral_score,
        (title_points + size_points + industry_points
         + recency_points + activity_points)               as lead_score
    from behavioral
),

graded as (
    select
        *,
        case
            when lead_score >= 80 then 'A'
            when lead_score >= 60 then 'B'
            when lead_score >= 40 then 'C'
            else 'D'
        end as lead_grade,

        case
            when lead_score >= 60 then true
            else false
        end as is_mql
    from final_scores
)

select * from graded
