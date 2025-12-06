{{ config(materialized='table') }}

with vehicles as (
    select * from {{ ref('stg_vehicles') }}
)

select
    vehicle_id,
    plate,
    type,
    status
from vehicles

