{{ config(
    materialized='incremental',
    unique_key='complaint_pk'
) }}

WITH calls AS (
    SELECT 
        call_id AS source_complaint_id,
        customer_id,
        agent_id,
        'Call Center' AS channel,
        complaint_category,
        resolution_status AS status,
        call_start_time AS started_at, 
        call_end_time AS ended_at,
        _ingestion_time AS ingested_at
    FROM {{ ref('stg_call_logs') }}
),

web AS (
    SELECT 
        request_id AS source_complaint_id,
        customer_id,
        agent_id,
        'Web Form' AS channel,
        complaint_category,
        resolution_status AS status,
        request_date AS started_at,   
        resolution_date AS ended_at,
        _ingestion_time AS ingested_at
    FROM {{ ref('stg_web_complaints') }}
),

social AS (
    SELECT 
        complaint_id AS source_complaint_id,
        customer_id,
        agent_id,
        'Social Media' AS channel,
        complaint_category,
        resolution_status AS status,
        request_date AS started_at,    
        resolution_date AS ended_at,
        _ingestion_time AS ingested_at
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
    {{ dbt_utils.generate_surrogate_key(['source_complaint_id', 'channel']) }} AS complaint_pk,
    *
FROM unioned

{% if is_incremental() %}
  WHERE ingested_at > (SELECT MAX(ingested_at) FROM {{ this }})
{% endif %}
