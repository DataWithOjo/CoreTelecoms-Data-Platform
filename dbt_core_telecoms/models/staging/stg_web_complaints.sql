WITH source AS (
    SELECT * FROM {{ source('telecom_raw', 'web_complaints') }}
),

renamed AS (
    SELECT
        request_id,
        source_row_id,
        customer_id,
        agent_id,
        complaint_catego_ry AS complaint_category,
        resolutionstatus AS resolution_status,
        TRY_TO_TIMESTAMP(request_date) AS request_date,
        TRY_TO_TIMESTAMP(resolution_date) AS resolution_date,
        TRY_TO_DATE(webformgenerationdate) AS web_form_generation_date,
        _ingestion_time
    FROM source
)

SELECT * FROM renamed;
