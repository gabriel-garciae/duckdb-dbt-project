-- Query SQL para responder: 
-- "Qual foi o CPK médio, por tipo de veículo (Truck vs Van vs Car), 
--  para o último mês fechado?"

-- Fórmula: Total Costs in the period / Total KM driven in the period

with last_closed_month as (
    select
        max(period_end) as last_month_end
    from {{ ref('mrt_cost_per_km') }}
    where period_end < current_date
),

cpk_by_type as (
    select
        type,
        avg(cost_per_km) as avg_cost_per_km,
        sum(total_costs) as total_costs,
        sum(total_km_driven) as total_km_driven,
        count(distinct vehicle_id) as vehicle_count
    from {{ ref('mrt_cost_per_km') }}
    cross join last_closed_month
    where period_end = last_closed_month.last_month_end
        and total_km_driven > 0  -- Apenas veículos que rodaram
    group by type
)

select
    type,
    avg_cost_per_km,
    total_costs,
    total_km_driven,
    vehicle_count
from cpk_by_type
order by type

