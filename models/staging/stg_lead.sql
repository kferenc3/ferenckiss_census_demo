select
    id as lead_id,
    company,
    first_name,
    last_name,
    title,
    email,
    phone,
    industry,
    lead_source,
    rating,
    status,
    number_of_employees,
    annual_revenue,
    country,
    created_date,
    is_deleted
from {{ source('salesforce', 'lead') }}
where not is_deleted