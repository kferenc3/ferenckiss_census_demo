select
    id as campaign_member_id,
    campaign_id,
    lead_id,
    status,
    created_date,
    is_deleted
from {{ source('salesforce', 'campaign_member') }}
where not is_deleted