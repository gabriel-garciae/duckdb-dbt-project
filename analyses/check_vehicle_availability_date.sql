-- Query SIMPLES para validar se um veículo estava disponível em uma data específica
-- 
-- Uso: Ajuste vehicle_id e check_date conforme necessário
-- Exemplo: Veículo 135 estava disponível no dia 15/05/2024?

SELECT
    135 as vehicle_id,  -- AJUSTE AQUI
    '2024-05-15'::date as check_date,  -- AJUSTE AQUI
    CASE
        WHEN EXISTS (
            SELECT 1 
            FROM staging.stg_maintenance_costs
            WHERE vehicle_id = 135  -- AJUSTE AQUI
              AND '2024-05-15'::date BETWEEN entry_date AND coalesce(return_date, current_date)
        ) THEN false  -- Em manutenção
        ELSE true     -- Disponível
    END as is_available,
    (
        SELECT maintenance_id
        FROM staging.stg_maintenance_costs
        WHERE vehicle_id = 135  -- AJUSTE AQUI
          AND '2024-05-15'::date BETWEEN entry_date AND coalesce(return_date, current_date)
        LIMIT 1
    ) as maintenance_id;

