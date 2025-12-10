select
    id as task_id,
    who_id,
    subject,
    status,
    activity_date,
    created_date,
    is_closed,
    is_deleted
from {{ source('salesforce', 'task') }}
where not is_deleted
