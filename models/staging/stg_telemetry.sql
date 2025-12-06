{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw', 'raw_telemetry') }}
),

renamed as (
    select
        event_id,
        vehicle_id,
        timestamp,
        odometer_value,
        engine_status,
        speed
    from source
)

select * from renamed

