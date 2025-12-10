select
    id as event_id,
    who_id,
    subject,
    start_date_time,
    end_date_time,
    location,
    description,
    is_deleted
from {{ source('salesforce', 'event') }}
where not is_deleted