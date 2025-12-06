{{
    config(
        materialized='incremental',
        unique_key=['vehicle_id', 'date'],
        incremental_strategy='delete+insert',
        on_schema_change='append_new_columns'
    )
}}

with telemetry_filtered as (
    select * from {{ ref('stg_telemetry') }}
    {% if is_incremental() %}
        -- LOOKBACK WINDOW: Reprocessa últimos 7 dias para capturar late arriving data
        -- Exemplo: Se hoje é 10/01 e lookback_days=7, reprocessa de 03/01 até 10/01
        where date(timestamp) >= (
            select 
                coalesce(
                    max(date) - interval '{{ var("lookback_days", 7) }}' day,
                    current_date - interval '{{ var("lookback_days", 7) }}' day
                )
            from {{ this }}
        )
        and date(timestamp) <= current_date
    {% endif %}
),

-- Calcula KM rodado usando LAG (ordena por timestamp para lidar com late arriving data)
telemetry_with_km as (
    select
        vehicle_id,
        date(timestamp) as date,
        timestamp,
        odometer_value,
        -- LAG ordenado por timestamp garante cálculo correto mesmo com dados fora de ordem
        lag(odometer_value) over (
            partition by vehicle_id
            order by timestamp
        ) as prev_odometer_value,
        engine_status,
        speed
    from telemetry_filtered
),

-- Calcula KM rodado por evento (apenas quando motor estava ligado e odômetro aumentou)
km_per_event as (
    select
        vehicle_id,
        date,
        case
            when engine_status = true 
                and prev_odometer_value is not null 
                and odometer_value >= prev_odometer_value
            then odometer_value - prev_odometer_value
            else 0
        end as km_driven,
        speed,
        engine_status
    from telemetry_with_km
)

-- Agrega por veículo/dia
select
    vehicle_id,
    date,
    sum(km_driven) as total_km_driven,
    count(*) as total_events,
    avg(speed) as avg_speed,
    max(speed) as max_speed,
    sum(case when engine_status = true then 1 else 0 end) as engine_on_events
from km_per_event
group by vehicle_id, date
