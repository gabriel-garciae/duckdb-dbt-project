#!/usr/bin/env python3
"""
Script para configurar o banco de dados DuckDB e carregar dados raw dos seeds.
Execute este script antes de rodar 'dbt build'.
"""

import duckdb
import os
import pandas as pd

# Configurações
DB_PATH = 'data/duckdb_dbt_project.duckdb'
SCHEMA_RAW = 'raw'

# Criar diretório se não existir
os.makedirs('data', exist_ok=True)

# Conectar ao DuckDB (cria o banco se não existir)
print(f"Criando/configurando banco de dados: {DB_PATH}")
conn = duckdb.connect(DB_PATH)

# Criar schema raw
conn.execute(f'CREATE SCHEMA IF NOT EXISTS {SCHEMA_RAW}')
print(f"Schema '{SCHEMA_RAW}' criado/verificado")

# Carregar seeds como tabelas raw
print("\nCarregando dados dos seeds...")

# Carregar vehicles
if os.path.exists('seeds/raw_vehicles.csv'):
    df_vehicles = pd.read_csv('seeds/raw_vehicles.csv')
    # Garantir que vehicle_id seja inteiro
    df_vehicles['vehicle_id'] = df_vehicles['vehicle_id'].astype(int)
    conn.execute(f'DROP TABLE IF EXISTS {SCHEMA_RAW}.raw_vehicles')
    conn.execute(f'CREATE TABLE {SCHEMA_RAW}.raw_vehicles AS SELECT * FROM df_vehicles')
    print(f"✓ Carregado {len(df_vehicles)} veículos")

# Carregar maintenance_costs
if os.path.exists('seeds/raw_maintenance_costs.csv'):
    df_maintenance = pd.read_csv('seeds/raw_maintenance_costs.csv')
    # Garantir que IDs sejam inteiros
    df_maintenance['maintenance_id'] = df_maintenance['maintenance_id'].astype(int)
    df_maintenance['vehicle_id'] = df_maintenance['vehicle_id'].astype(int)
    # Converter datas
    df_maintenance['entry_date'] = pd.to_datetime(df_maintenance['entry_date'])
    df_maintenance['return_date'] = pd.to_datetime(df_maintenance['return_date'], errors='coerce')
    conn.execute(f'DROP TABLE IF EXISTS {SCHEMA_RAW}.raw_maintenance_costs')
    conn.execute(f'CREATE TABLE {SCHEMA_RAW}.raw_maintenance_costs AS SELECT * FROM df_maintenance')
    print(f"✓ Carregado {len(df_maintenance)} registros de manutenção")

# Carregar telemetry
if os.path.exists('seeds/raw_telemetry.csv'):
    df_telemetry = pd.read_csv('seeds/raw_telemetry.csv')
    # Garantir que IDs sejam inteiros
    df_telemetry['event_id'] = df_telemetry['event_id'].astype(int)
    df_telemetry['vehicle_id'] = df_telemetry['vehicle_id'].astype(int)
    # Converter timestamp
    df_telemetry['timestamp'] = pd.to_datetime(df_telemetry['timestamp'])
    conn.execute(f'DROP TABLE IF EXISTS {SCHEMA_RAW}.raw_telemetry')
    conn.execute(f'CREATE TABLE {SCHEMA_RAW}.raw_telemetry AS SELECT * FROM df_telemetry')
    print(f"✓ Carregado {len(df_telemetry)} eventos de telemetria")

print(f"\n✓ Banco de dados configurado com sucesso!")
print(f"✓ Localização: {DB_PATH}")
print(f"✓ Schema '{SCHEMA_RAW}' criado")
print("\nPróximo passo:")
print("  dbt build")

conn.close()

