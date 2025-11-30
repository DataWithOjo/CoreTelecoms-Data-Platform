{{ config(
    materialized='incremental',
    unique_key='complaint_pk'
) }}

WITH calls AS (
    SELECT 
        call_id as source_complaint_id,
        customer_id,
        agent_id,
        'Call Center' as channel,
        complaint_category,
        resolution_status as status,
        call_start_time as started_at, 
        call_end_time as ended_at,
        _ingestion_time as ingested_at
    FROM {{ ref('stg_call_logs') }}
),

web AS (
    SELECT 
        request_id as source_complaint_id,
        customer_id,
        agent_id,
        'Web Form' as channel,
        complaint_category,
        resolution_status as status,
        request_date as started_at,   
        resolution_date as ended_at,
        _ingestion_time as ingested_at
    FROM {{ ref('stg_web_complaints') }}
),

social AS (
    SELECT 
        complaint_id as source_complaint_id,
        customer_id,
        agent_id,
        'Social Media' as channel,
        complaint_category,
        resolution_status as status,
        request_date as started_at,    
        resolution_date as ended_at,
        _ingestion_time as ingested_at
    FROM {{ ref('stg_social_media') }}
),

unioned AS (
    SELECT * FROM calls
    UNION ALL
    SELECT * FROM web
    UNION ALL
    SELECT * FROM social
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['source_complaint_id', 'channel']) }} as complaint_pk,
    *
FROM unioned

{% if is_incremental() %}
  WHERE ingested_at > (SELECT MAX(ingested_at) FROM {{ this }})
{% endif %}