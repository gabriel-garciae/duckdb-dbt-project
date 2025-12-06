{{ config(materialized='table') }}

with maintenance_costs as (
    select
        vehicle_id,
        date_trunc('month', entry_date) as period_start,
        date_trunc('month', entry_date) + interval '1 month' - interval '1 day' as period_end,
        sum(amount) as total_costs
    from {{ ref('int_fct_maintenance_costs') }}
    where return_date is not null  -- Apenas manutenções fechadas
    group by vehicle_id, date_trunc('month', entry_date)
),

telemetry_daily as (
    select
        vehicle_id,
        date_trunc('month', date) as period_start,
        sum(total_km_driven) as total_km_driven
    from {{ ref('int_fct_telemetry_daily') }}
    group by vehicle_id, date_trunc('month', date)
),

vehicles as (
    select * from {{ ref('int_dim_vehicles') }}
)

select
    v.vehicle_id,
    v.plate,
    v.type,
    coalesce(mc.period_start, td.period_start) as period_start,
    coalesce(mc.period_end, td.period_start + interval '1 month' - interval '1 day') as period_end,
    coalesce(mc.total_costs, 0) as total_costs,
    coalesce(td.total_km_driven, 0) as total_km_driven,
    case
        when coalesce(td.total_km_driven, 0) > 0 then
            coalesce(mc.total_costs, 0) / td.total_km_driven
        else 0
    end as cost_per_km
from vehicles v
left join maintenance_costs mc
    on v.vehicle_id = mc.vehicle_id
left join telemetry_daily td
    on v.vehicle_id = td.vehicle_id
    and mc.period_start = td.period_start
where v.status = 'Active'  -- Apenas veículos ativos

