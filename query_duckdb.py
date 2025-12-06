#!/usr/bin/env python3
"""
Script simples para conectar ao DuckDB e fazer queries ad-hoc
"""

import duckdb
import os

# Caminho do banco de dados
DB_PATH = 'data/duckdb_dbt_project.duckdb'

# Verificar se o banco existe
if not os.path.exists(DB_PATH):
    print(f"❌ Banco de dados não encontrado: {DB_PATH}")
    print("Execute primeiro: python setup_database.py")
    exit(1)

# Conectar ao DuckDB
conn = duckdb.connect(DB_PATH)
print("✅ Conectado ao DuckDB\n")

# ============================================================================
# 1. LISTAR TODAS AS TABELAS E VIEWS
# ============================================================================
print("="*60)
print("TABELAS E VIEWS DISPONÍVEIS")
print("="*60)

tables = conn.execute("""
    SELECT 
        table_schema,
        table_name,
        table_type
    FROM information_schema.tables
    WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_temp')
    ORDER BY table_schema, table_name
""").fetchdf()

print(tables.to_string(index=False))
print("\n")

# ============================================================================
# 2. SELECT SIMPLES - EXEMPLO
# ============================================================================
print("="*60)
print("EXEMPLO DE QUERY - Veículos Ativos")
print("="*60)

result = conn.execute("""
    SELECT 
        vehicle_id,
        plate,
        type,
        status
    FROM staging.stg_vehicles
    WHERE status = 'Active'
    LIMIT 10
""").fetchdf()

print(result.to_string(index=False))
print("\n")

# ============================================================================
# 3. ADICIONE SUAS PRÓPRIAS QUERIES AQUI
# ============================================================================
print("="*60)
print("PARA QUERIES INTERATIVAS, USE O DUCKDB CLI:")
print("="*60)
print("""
Comando no terminal:
  duckdb data/duckdb_dbt_project.duckdb

Isso abrirá o prompt interativo do DuckDB onde você pode executar queries diretamente.

Exemplo de uso:
  D SELECT * FROM staging.stg_vehicles LIMIT 5;
  D SELECT COUNT(*) FROM raw.raw_telemetry;
  D .tables    -- Listar todas as tabelas
  D .quit      -- Sair

Ou adicione suas queries neste script Python acima da linha conn.close()
""")

# Fechar conexão
conn.close()
print("✅ Conexão fechada")
