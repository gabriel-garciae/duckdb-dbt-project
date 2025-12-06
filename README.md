# Projeto dbt - Análise de Frota Infleet

Este projeto utiliza **dbt Core** com **DuckDB** para transformar dados brutos de uma frota de veículos em informações úteis para análise de negócios.

![dbt + DuckDB](docs/1_yisK9_gKv5E--gk2kh4elQ.png)

## Início rápido

```bash
git clone https://github.com/gabriel-garciae/duckdb-dbt-project.git
cd duckdb-dbt-project
poetry install && poetry shell
dbt deps
python setup_database.py
dbt build
```

## Estrutura do projeto

O projeto segue as melhores práticas do dbt com a arquitetura em camadas:

```
models/
├── staging/
│   ├── stg_vehicles.sql
│   ├── stg_maintenance_costs.sql
│   └── stg_telemetry.sql
├── intermediate/     # Tabelas - Fatos e dimensões intermediárias
│   ├── int_dim_vehicles.sql
│   ├── int_fct_maintenance_costs.sql
│   ├── int_fct_telemetry_daily.sql
│   └── int_fct_vehicle_availability.sql
└── marts/            # Tabelas - Modelos de negócio finais
    ├── mrt_cost_per_km.sql
    └── mrt_fleet_availability.sql
```

## Instalação e configuração

### Pré-requisitos

- Python 3.12+
- Poetry (gerenciador de dependências)

### Instalação com Poetry

```bash
# 1. Instalar Poetry (se ainda não tiver)
curl -sSL https://install.python-poetry.org | python3 -

# 2. Instalar dependências do projeto
poetry install

# 3. Ativar o ambiente virtual
poetry shell

# 4. Instalar pacotes do dbt
dbt deps

# 5. Configurar banco de dados e carregar dados raw dos seeds
python setup_database.py

# 6. Executar build (cria todos os modelos e executa testes)
dbt build
```

**Importante:**
- Os dados de exemplo já estão incluídos no diretório `seeds/`. Não é necessário gerar dados adicionais.
- O DuckDB não precisa ser instalado separadamente - é uma dependência Python gerenciada pelo Poetry.
- O script `setup_database.py` cria automaticamente o banco em `data/duckdb_dbt_project.duckdb` e carrega os dados dos seeds.
- O comando `dbt build` cria todas as camadas automaticamente na ordem correta (staging → intermediate → marts).

### Configuração do banco de dados

O projeto está configurado para usar DuckDB. O arquivo `profiles.yml` contém a configuração:

```yaml
duckdb_dbt_project:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: 'data/duckdb_dbt_project.duckdb'
      schema: ''
```

**Como o DuckDB funciona neste projeto:**

- **Persistência em disco**: O DuckDB está configurado para usar um arquivo em disco (`data/duckdb_dbt_project.duckdb`), não em memória
- **Dados persistem**: Todos os dados (raw, staging, intermediate, marts) são salvos permanentemente no arquivo
- **Entre sessões**: Quando você fecha o terminal e volta amanhã, todos os dados ainda estarão lá
- **Tamanho do arquivo**: O banco cresce conforme você adiciona dados (atualmente ~7MB com os seeds)
- **Modo interativo**: Quando você usa `duckdb data/duckdb_dbt_project.duckdb` no terminal, está conectando ao mesmo arquivo persistente

### Dados de entrada

O projeto utiliza dados de exemplo que já estão incluídos no diretório `seeds/`:

1. **raw_vehicles.csv**: Informações dos veículos (vehicle_id, plate, type, status)
2. **raw_maintenance_costs.csv**: Custos de manutenção (maintenance_id, vehicle_id, cost_type, amount, entry_date, return_date)
3. **raw_telemetry.csv**: Dados de telemetria GPS (event_id, vehicle_id, timestamp, odometer_value, engine_status, speed)

O script `setup_database.py` carrega automaticamente estes dados para o banco DuckDB:
- Cria o diretório `data/` se não existir
- Cria o banco DuckDB em `data/duckdb_dbt_project.duckdb`
- Cria o schema `raw` no DuckDB
- Carrega os arquivos CSV dos seeds como tabelas raw
- Garante que IDs sejam tratados como inteiros
- Converte tipos de dados corretamente (datas, timestamps, etc.)

## Modelos do projeto

### Staging (Views)

Modelos de staging limpam e padronizam os dados brutos:

- **stg_vehicles**: Informações básicas dos veículos
- **stg_maintenance_costs**: Custos de manutenção com tipos padronizados
- **stg_telemetry**: Dados de telemetria GPS

### Intermediate (Tabelas)

Modelos intermediários criam fatos e dimensões:

- **int_dim_vehicles**: Dimensão de veículos
- **int_fct_maintenance_costs**: Fato de custos de manutenção com cálculos de duração
- **int_fct_telemetry_daily**: Fato agregado de telemetria por dia (materialização incremental para Big Data)
- **int_fct_vehicle_availability**: Fato de disponibilidade de veículos por dia

### Marts (Tabelas)

Modelos de negócio finais:

- **mrt_cost_per_km**: Custo por quilômetro por veículo e período
- **mrt_fleet_availability**: Taxa de disponibilidade da frota por período mensal

## Desafio e soluções implementadas

### Parte 2: A métrica de negócio (SQL)

**Pergunta:** "Qual foi o Custo por KM (CPK) médio, por tipo de veículo (Truck vs Van vs Car), no último mês fechado?"

**Fórmula:** Total de Custos no período / Total de KM rodados no período

**Solução:** Utilizando o modelo `mrt_cost_per_km`, podemos responder esta pergunta com a seguinte query:

```sql
with last_closed_month as (
    select
        max(period_end) as last_month_end
    from marts.mrt_cost_per_km
    where period_end < current_date
),

cpk_by_type as (
    select
        type,
        avg(cost_per_km) as avg_cost_per_km,
        sum(total_costs) as total_costs,
        sum(total_km_driven) as total_km_driven,
        count(distinct vehicle_id) as vehicle_count
    from marts.mrt_cost_per_km
    cross join last_closed_month
    where period_end = last_closed_month.last_month_end
        and total_km_driven > 0
    group by type
)

select
    type,
    avg_cost_per_km,
    total_costs,
    total_km_driven,
    vehicle_count
from cpk_by_type
order by type;
```

**Resultado:**

| type | avg_cost_per_km | total_costs | total_km_driven | vehicle_count |
|------|-----------------|------------|-----------------|---------------|
| Car  | 47.12           | 50,048.53  | 1,033.97        | 27            |
| Truck| 44.17           | 31,091.03 | 730.46          | 19            |
| Van  | 32.30           | 19,437.38 | 647.54          | 18            |

### Parte 3: arquitetura e performance (estratégico)

#### 1. Big Data: Processamento Incremental

**Contexto:** A tabela `raw_telemetry` possui **5 bilhões de linhas** e cresce **10GB/dia**. O processamento diário não deve levar mais de **1 hora**.

**Pergunta 1:** "Como você configuraria a materialização incremental deste modelo no dbt para garantir que o processamento diário não demore mais que 1 hora?"

**Resposta:**

O modelo `int_fct_telemetry_daily` está configurado com materialização incremental otimizada:

```sql
{{
    config(
        materialized='incremental',
        unique_key=['vehicle_id', 'date'],
        incremental_strategy='delete+insert',
        on_schema_change='append_new_columns'
    )
}}
```

**Estratégias implementadas:**

1. **Agregação diária**
   - Reduz volume: 5 bilhões de eventos (1 por veículo/dia)
   - Redução de 99.9998% no volume processado

2. **Lookback Window para Late Arriving Data**
   ```sql
   {% if is_incremental() %}
       where date(timestamp) >= (
           select max(date) - interval '{{ var("lookback_days", 7) }}' day
           from {{ this }}
       )
       and date(timestamp) <= current_date
   {% endif %}
   ```
   - Reprocessa últimos **7 dias** (configurável via `var('lookback_days')`)
   - Captura dados que chegaram atrasados dos rastreadores

3. **Filtro incremental**
   - Processa apenas período necessário (últimos N dias)
   - Não reprocessa todo o histórico

4. **Delete+Insert Strategy**
   - Atualiza apenas registros afetados (veículo/dia)

**Performance estimada:**
- Dados processados: ~70GB (7 dias × 10GB/dia)
- Tempo estimado: **40-60 minutos/dia** (dentro do limite de 1 hora)


**Pergunta 2:** "Como lidar com dados que chegam atrasados (late arriving data) dos rastreadores?"

**Resposta:**

A estratégia de **Lookback Window** está implementada diretamente no modelo `int_fct_telemetry_daily`:

**Como funciona:**

1. **Reprocessamento automático dos ultimos N dias**
   - O modelo reprocessa automaticamente os últimos 7 dias (configurável)
   - Captura dados que chegaram atrasados dos rastreadores GPS
   - Usa `delete+insert` para atualizar registros existentes quando necessário

2. **Re-agregação inteligente**
   - Recalcula métricas diárias apenas para os dias afetados
   - Window functions (`LAG`) ordenam dados corretamente mesmo quando chegam fora de ordem
   - Garante cálculo preciso de KM rodado mesmo com eventos atrasados

3. **Configuração flexível**
   ```yaml
   # dbt_project.yml
   vars:
     lookback_days: 7  # Ajuste conforme necessidade
   ```
   
   Ou via linha de comando:
   ```bash
   dbt run --select int_fct_telemetry_daily --vars '{"lookback_days": 14}'
   ```

4. **Trade-offs**
   - **Mais dias de lookback** = Mais completude, mas processamento mais lento
   - **Menos dias de lookback** = Processamento mais rápido, mas pode perder dados muito atrasados
   - **Recomendação**: Começar com 7 dias e ajustar conforme padrão de atraso observado

#### 2. Disponibilidade: Modelagem de disponibilidade por dia

**Contexto:** O cálculo de "Dias Parados para Manutenção" depende de `entry_date` e `return_date` na tabela B. Como modelar para determinar se um veículo estava disponível em um dia específico (ex: 15 de maio), sabendo que a manutenção ocorreu de 10 a 20 de maio?

**Resposta:**

Criei o modelo `int_fct_vehicle_availability` que resolve este problema:

**Abordagem implementada:**

1. **Expansão de Períodos de Manutenção**
   - Expande apenas os dias onde há manutenção (não todos os dias)
   - Exemplo: Manutenção de 10/05 a 20/05 gera 11 linhas (uma por dia)
   - Muito mais eficiente que gerar todas as combinações veículo/dia

2. **Determinação de disponibilidade**

   - Para saber se está disponível: se o dia NÃO está na tabela, está disponível
   - Query simples: `SELECT * WHERE vehicle_id = X AND date = '2024-05-15'`
     - Se retornar linha → em manutenção
     - Se não retornar → disponível

**Vantagens desta abordagem:**

- Responde diretamente: "O veículo estava disponível no dia X?"
- Muito menos linhas que gerar todos os dias (só dias com manutenção)
- Suporta múltiplas manutenções no mesmo período
- Lida com manutenções em aberto (`return_date = NULL`)
- Facilita agregações (disponibilidade mensal, anual, etc.)

## Testes

O projeto inclui testes automatizados definidos nos arquivos YAML:

- **Testes de unicidade**: Garantem que chaves primárias são únicas
- **Testes de not Null**: Validam campos obrigatórios
- **Testes de valores aceitos**: Validam enums (type, status, cost_type)
- **Testes de relacionamento**: Validam foreign keys entre tabelas

## Consultas Ad-Hoc

Para consultas ad-hoc, você pode usar o DuckDB CLI em modo interativo:

```bash
duckdb data/duckdb_dbt_project.duckdb
```

No prompt do DuckDB, você pode executar queries SQL diretamente:

```sql
-- Exemplo: Ver disponibilidade da frota
SELECT * FROM marts.mrt_fleet_availability LIMIT 10;
```

## Dependências

O projeto utiliza:
- **dbt-core**: Framework de transformação de dados
- **dbt-duckdb**: Adapter para DuckDB
- **duckdb**: Banco de dados analítico in-memory
- **pandas**: Manipulação de dados Python

Todas as dependências são gerenciadas via Poetry no arquivo `pyproject.toml`.
