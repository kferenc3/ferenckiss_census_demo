select
    id as campaign_id,
    name,
    type,
    status,
    start_date,
    end_date,
    budgeted_cost,
    expected_revenue,
    is_deleted
from {{ source('salesforce', 'campaign') }}
where not is_deleted