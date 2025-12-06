{{ config(materialized='table') }}

with maintenance as (
    select
        vehicle_id,
        entry_date::date as entry_date,
        coalesce(return_date::date, current_date) as return_date,
        maintenance_id
    from {{ ref('stg_maintenance_costs') }}
),

-- Expande APENAS os dias onde há manutenção (não todos os dias)
-- Exemplo: Manutenção 10/05 a 20/05 gera 11 linhas (uma por dia)
maintenance_days as (
    select
        m.vehicle_id,
        m.maintenance_id,
        m.entry_date + (day_offset || ' days')::interval as date
    from maintenance m
    cross join (
        select unnest(range(0, 1000)) as day_offset  -- Limite de 1000 dias por manutenção
    ) d
    where m.entry_date + (day_offset || ' days')::interval <= m.return_date
      and m.entry_date + (day_offset || ' days')::interval <= current_date
)

-- Retorna apenas dias em manutenção
-- Para saber se está disponível: se não está aqui, está disponível!
select
    vehicle_id,
    date,
    false as is_available,  -- Sempre false pois são apenas dias em manutenção
    maintenance_id
from maintenance_days
