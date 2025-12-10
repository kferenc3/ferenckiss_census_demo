with tasks as (
    select
        t.task_id,
        t.who_id,
        t.status,
        t.activity_date,
        t.is_closed
    from {{ ref('stg_task') }} t
    -- keep only tasks related to leads (SF lead ID prefix 00Q)
    where substr(t.who_id, 1, 3) = '00Q'
),

agg as (
    select
        who_id as lead_id,
        count(*) as task_count_30d,
        sum(case when is_closed then 1 else 0 end) as completed_task_count_30d,
        max(activity_date) as last_activity_date
    from tasks
    group by who_id
),

metrics as (
    select
        lead_id,
        task_count_30d,
        completed_task_count_30d,
        last_activity_date,
        datediff(day, last_activity_date, current_date) as days_since_last_activity
    from agg
)

select * from metrics
