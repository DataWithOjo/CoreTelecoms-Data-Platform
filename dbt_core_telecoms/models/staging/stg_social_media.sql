WITH source AS (
    SELECT * FROM {{ source('telecom_raw', 'social_media') }}
),

renamed AS (
    SELECT
        complaint_id,
        customer_id,
        agent_id,
        complaint_catego_ry AS complaint_category,
        media_channel,
        resolutionstatus AS resolution_status,
        TRY_TO_TIMESTAMP(request_date) AS request_date,
        TRY_TO_TIMESTAMP(resolution_date) AS resolution_date,
        TRY_TO_DATE(mediacomplaintgenerationdate) AS media_complaint_generation_date,
        _ingestion_time
    FROM source
)

SELECT * FROM renamed
