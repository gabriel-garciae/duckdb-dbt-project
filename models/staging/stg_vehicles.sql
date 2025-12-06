{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw', 'raw_vehicles') }}
),

renamed as (
    select
        vehicle_id,
        plate,
        type,
        status
    from source
)

select * from renamed

