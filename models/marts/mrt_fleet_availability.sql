{{ config(materialized='table') }}

with maintenance_days_detail as (
    select
        vehicle_id,
        date,
        date_trunc('month', date) as month_start
    from {{ ref('int_fct_vehicle_availability') }}
),

-- Conta dias de manutenção por veículo/mês (apenas dias do mês específico)
maintenance_days_by_month as (
    select
        vehicle_id,
        month_start,
        count(distinct date) as maintenance_days
    from maintenance_days_detail
    group by vehicle_id, month_start
),

vehicles as (
    select * from {{ ref('int_dim_vehicles') }}
    where status = 'Active'
),

-- Identifica todos os meses únicos onde há atividade
active_months as (
    select distinct month_start
    from maintenance_days_by_month
),

-- Calcula disponibilidade mensal para cada veículo
availability_monthly as (
    select
        v.vehicle_id,
        m.month_start as period_start,
        m.month_start + interval '1 month' - interval '1 day' as period_end,
        -- Total de dias no mês
        datediff('day', m.month_start, m.month_start + interval '1 month' - interval '1 day') + 1 as total_days,
        -- Dias em manutenção (garantindo que não exceda total_days)
        least(
            coalesce(md.maintenance_days, 0),
            datediff('day', m.month_start, m.month_start + interval '1 month' - interval '1 day') + 1
        ) as maintenance_days
    from vehicles v
    cross join active_months m
    left join maintenance_days_by_month md
        on v.vehicle_id = md.vehicle_id
        and m.month_start = md.month_start
)

-- Calcula métricas finais com proteção contra valores negativos
select
    v.vehicle_id,
    v.plate,
    v.type,
    va.period_start,
    va.period_end,
    va.total_days,
    -- Garante que available_days nunca seja negativo
    greatest(va.total_days - va.maintenance_days, 0) as available_days,
    va.maintenance_days,
    -- Calcula taxa de disponibilidade (0-100%)
    case
        when va.total_days > 0 then
            greatest(
                ((va.total_days - va.maintenance_days)::float / va.total_days::float) * 100,
                0
            )
        else 100 -- caso o veiculo nao existisse em certo mês, consideramos 100% de disponibilidade ()
    end as availability_rate
from vehicles v
inner join availability_monthly va
    on v.vehicle_id = va.vehicle_id
where va.total_days > 0  -- Apenas meses válidos
