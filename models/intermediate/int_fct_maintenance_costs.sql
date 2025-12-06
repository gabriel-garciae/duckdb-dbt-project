{{ config(materialized='table') }}

with maintenance as (
    select * from {{ ref('stg_maintenance_costs') }}
)

select
    maintenance_id,
    vehicle_id,
    cost_type,
    amount,
    entry_date,
    return_date,
    -- Calcula dias em manutenção (usa data atual se return_date for NULL)
    case
        when return_date is not null then
            datediff('day', entry_date::date, return_date::date)
        else
            datediff('day', entry_date::date, current_date)
    end as maintenance_days,
    -- Flag indicando se a manutenção está fechada
    case
        when return_date is not null then true
        else false
    end as is_closed
from maintenance

