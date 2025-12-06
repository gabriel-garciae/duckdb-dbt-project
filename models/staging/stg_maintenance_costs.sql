{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw', 'raw_maintenance_costs') }}
),

renamed as (
    select
        maintenance_id,
        vehicle_id,
        cost_type,
        cast(amount as decimal(10, 2)) as amount,
        entry_date,
        return_date
    from source
)

select * from renamed

