WITH source AS (
    SELECT * FROM {{ source('telecom_raw', 'call_center_logs') }}
),

renamed AS (
    SELECT
        call_id,
        source_row_id,
        customer_id,
        agent_id,
        complaint_catego_ry AS complaint_category,
        resolutionstatus AS resolution_status,
        TRY_TO_TIMESTAMP(call_start_time) AS call_start_time,
        TRY_TO_TIMESTAMP(call_end_time) AS call_end_time,
        TRY_TO_DATE(calllogsgenerationdate) AS call_logs_generation_date,
        _ingestion_time
    FROM source
)

SELECT * FROM renamed;
