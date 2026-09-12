# Portfólio Databricks - Engenharia de Dados

## Visão Geral

Repositório com projetos práticos em Databricks focados em Engenharia de Dados moderna. Este portfólio demonstra competências em arquitetura Lakehouse, processamento streaming, Change Data Capture (CDC), pipelines declarativos e governança de dados com Unity Catalog.

## Autor

**Fabio Medeiros**  
Engenheiro de Dados  
Email: fabiolrm78@gmail.com  
Telefone: (21) 99663-7177

---

## Arquitetura Geral

Todos os projetos seguem práticas modernas de engenharia de dados:

```
┌─────────────────────────────────────────────────────────────────┐
│                    DATABRICKS LAKEHOUSE                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐        │
│  │   BRONZE     │  │    SILVER    │  │     GOLD     │        │
│  │   (Raw)      │─→│(Transformed) │─→│ (Aggregated) │        │
│  └──────────────┘  └──────────────┘  └──────────────┘        │
│                                                                 │
│  ┌────────────────────────────────────────────────────────┐   │
│  │           DELTA LAKE + UNITY CATALOG                   │   │
│  └────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌────────────────────────────────────────────────────────┐   │
│  │      SPARK DECLARATIVE PIPELINES (LAKEFLOW)            │   │
│  └────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

---

## Projetos Desenvolvidos

### 1. Pipeline ETL - Bike Share Chicago

**Localização:** `02_Pipeline_Limpa_Dados_Bicke/`

**Descrição:**  
Pipeline completo implementando Arquitetura Medallion (Bronze → Silver → Gold) para processar dados de compartilhamento de bicicletas de Chicago usando Databricks Lakeflow.

**Fluxo de Dados:**

```
┌─────────────────────────────────────────────────────────────────────┐
│                    PIPELINE BIKE SHARE CHICAGO                      │
└─────────────────────────────────────────────────────────────────────┘

┌──────────────────┐
│   UC VOLUME      │
│ bike-share-      │
│  rides.csv       │
└────────┬─────────┘
         │
         │ Auto Loader
         │ (read_files)
         ▼
┌─────────────────────────────────────────────────────┐
│           BRONZE LAYER                              │
│  tb_bronze_bike_eventos                             │
│  ──────────────────────────────────────             │
│  Tipo: STREAMING TABLE                              │
│  Função: Ingestão incremental de dados brutos       │
│  Origem: CSV via Auto Loader                        │
│  Features:                                          │
│  - Ingestão contínua (streaming)                    │
│  - Schema inference automático                      │
│  - Detecção de novos arquivos                       │
└──────────────────────┬──────────────────────────────┘
                       │
                       │ Stream Processing
                       ▼
┌─────────────────────────────────────────────────────┐
│           SILVER LAYER                              │
│  tb_silver_bike_eventos                             │
│  ─────────────────────────────────────              │
│  Tipo: STREAMING TABLE                              │
│  Função: Transformação e limpeza                    │
│  Transformações:                                    │
│  - Cálculo de receita (member vs casual)            │
│  - Extração de data da viagem                       │
│  - Filtro de viagens inválidas (duração <= 0)       │
│  - Padronização de tipos de dados                   │
│  - Qualidade de dados aplicada                      │
└──────────────────────┬──────────────────────────────┘
                       │
                       │ Aggregation
                       ▼
┌─────────────────────────────────────────────────────┐
│            GOLD LAYER                               │
│  tb_gold_bike_eventos                               │
│  ───────────────────────────────────                │
│  Tipo: MATERIALIZED VIEW                            │
│  Função: Agregações para análise                    │
│  Métricas:                                          │
│  - Contagem de viagens por dia/bicicleta            │
│  - Receita total por dia/bicicleta                  │
│  - Primeira e última estação do dia                 │
│  - COUNT(DISTINCT) implementado                     │
└──────────────────────┬──────────────────────────────┘
                       │
                       ▼
              ┌─────────────────┐
              │   ANALYTICS     │
              │   DASHBOARDS    │
              │   SQL QUERIES   │
              └─────────────────┘
```

**Tecnologias Utilizadas:**
- Databricks Lakeflow (Spark Declarative Pipelines)
- Delta Lake (Lakehouse Storage)
- Unity Catalog (Governança de Dados)
- Auto Loader (Ingestão Incremental)
- SQL Streaming Tables
- Materialized Views

**Resultados:**
- 188 registros agregados na camada Gold
- Pipeline com execução incremental automática
- Processamento streaming funcionando
- Qualidade de dados garantida

**Notebooks:**
- `01-Notebook_SQL_bronze_bike_eventos` - Camada de ingestão
- `02-Notebook_SQL_silver_bike_eventos` - Camada de transformação
- `03-Notebook_SQL_gold_bike_eventos` - Camada de agregação
- `Notebook_Drop_Tables_Bike` - Utilitário para reset

**Boas Práticas Implementadas:**
- Separação clara de responsabilidades por camada
- Streaming Tables para processamento incremental eficiente
- Materialized View para agregações complexas
- Filtros de qualidade de dados na camada Silver
- Uso correto de tipos de datasets (Streaming vs Batch)

---

### 2. Pipeline CDC (Change Data Capture)

**Localização:** `03_CDC Pipeline/declarative-pipeline-cdc/`

**Descrição:**  
Implementação de pipelines para captura de mudanças de dados (CDC) usando Spark Declarative Pipelines em SQL e Python.

**Fluxo de Dados:**

```
┌──────────────────────────────────────────────────────┐
│            PIPELINE CDC                              │
└──────────────────────────────────────────────────────┘

┌─────────────────┐
│  Source System  │
│  (Database)     │
└────────┬────────┘
         │
         │ CDC Events
         │ (INSERT/UPDATE/DELETE)
         ▼
┌──────────────────────────────────────────┐
│      BRONZE - CDC RAW EVENTS             │
│  ────────────────────────────────        │
│  Captura de eventos:                     │
│  - op_type: I (Insert)                   │
│  - op_type: U (Update)                   │
│  - op_type: D (Delete)                   │
│  - Timestamp da operação                 │
│  - Before/After state                    │
└──────────────┬───────────────────────────┘
               │
               │ APPLY CHANGES INTO
               │ (Merge Operations)
               ▼
┌──────────────────────────────────────────┐
│      SILVER - CURRENT STATE              │
│  ──────────────────────────────          │
│  Tabela consolidada com estado atual     │
│  Operações:                              │
│  - MERGE baseado em chave primária       │
│  - Soft deletes (is_deleted flag)        │
│  - Histórico de versões (SCD Type 2)     │
└──────────────┬───────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────┐
│      GOLD - ANALYTICS                    │
│  ──────────────────────────────          │
│  Views e agregações para análise         │
└──────────────────────────────────────────┘
```

**Estrutura:**
- `1-sdp-sql/` - Implementação CDC em SQL
- `2-sdp-python/` - Implementação CDC em Python
- `_resources/` - Dados e recursos auxiliares

**Conceitos Implementados:**
- Change Data Capture (CDC)
- APPLY CHANGES INTO para merge automático
- Slowly Changing Dimensions (SCD) Type 2
- Soft deletes
- Versionamento de dados

---

### 3. Fundamentos Delta Lake e Engenharia de Dados

**Localização:** `01_Get_Started_with_Data_Engineering/`

**Descrição:**  
Conjunto de exercícios e práticas do treinamento oficial Databricks focado em fundamentos de Lakehouse, Delta Lake e técnicas modernas de engenharia de dados.

**Módulos:**

#### 3.1 Delta Lake Fundamentals
**Pasta:** `01 - Delta Lake Fundamentals/`

Conceitos abordados:
- ACID Transactions
- Time Travel
- Schema Evolution
- Otimização de tabelas (OPTIMIZE, Z-ORDER)
- Vacuum e gerenciamento de storage

#### 3.2 Ingestion Techniques
**Pasta:** `02 - Ingestion Techniques/`

Técnicas implementadas:
- Auto Loader para ingestão incremental
- Batch vs Streaming ingestion
- Schema inference e enforcement
- File format handling (CSV, JSON, Parquet)

#### 3.3 Medallion Architecture
**Pasta:** `Medallion Architecture/`

**Fluxo de Dados:**

```
┌────────────────────────────────────────────────┐
│     MEDALLION ARCHITECTURE - EMPLOYEES         │
└────────────────────────────────────────────────┘

┌──────────────┐
│  Raw Files   │
│  employees/  │
└──────┬───────┘
       │
       ▼
┌─────────────────────────────────┐
│   BRONZE                        │
│   Notebook_Employes_Bronze      │
│   ─────────────────────────     │
│   - Ingestão raw                │
│   - Schema original             │
│   - Audit columns               │
└──────────┬──────────────────────┘
           │
           ▼
┌─────────────────────────────────┐
│   SILVER                        │
│   Notebook_Employes_Silver_Gold │
│   ─────────────────────────     │
│   - Data cleansing              │
│   - Type casting                │
│   - Business rules              │
└──────────┬──────────────────────┘
           │
           ▼
┌─────────────────────────────────┐
│   GOLD                          │
│   Notebook_Employes_Silver_Gold │
│   ─────────────────────────     │
│   - Aggregations                │
│   - Analytics views             │
│   - KPIs                        │
└─────────────────────────────────┘
```

#### 3.4 Pipeline Bronze-Silver-Gold SDP
**Pasta:** `Pipeline_bronze_silver_gold_sdp/`

Pipeline declarativo completo implementando Medallion com Spark Declarative Pipelines.

#### 3.5 Orchestration
**Pasta:** `Orchestration/`

Conceitos de orquestração:
- Databricks Jobs
- Task dependencies
- Scheduling
- Error handling e retry policies

---

## Tecnologias e Ferramentas

### Plataforma
- Databricks Lakehouse Platform
- Databricks Runtime (Serverless)
- Unity Catalog

### Processamento
- Apache Spark 3.x
- Spark SQL
- PySpark
- Spark Structured Streaming

### Storage e Formato
- Delta Lake
- Parquet
- CSV
- JSON

### Pipelines
- Databricks Lakeflow (Spark Declarative Pipelines)
- Streaming Tables
- Materialized Views
- Auto Loader

### Padrões Arquiteturais
- Arquitetura Medallion (Bronze-Silver-Gold)
- Change Data Capture (CDC)
- Slowly Changing Dimensions (SCD)
- Lambda Architecture principles

---

## Estrutura do Repositório

```
Databricks/
├── README.md (este arquivo)
├── 01_Get_Started_with_Data_Engineering/
│   ├── 01 - Delta Lake Fundamentals/
│   ├── 02 - Ingestion Techniques/
│   ├── Medallion Architecture/
│   │   ├── Notebook_Employes_Bronze
│   │   └── Notebook_Employes_Silver_Gold
│   ├── Pipeline_bronze_silver_gold_sdp/
│   │   └── transformations/
│   └── Orchestration/
│
├── 02_Pipeline_Limpa_Dados_Bicke/
│   ├── README.md
│   ├── 01-Notebook_SQL_bronze_bike_eventos
│   ├── 02-Notebook_SQL_silver_bike_eventos
│   ├── 03-Notebook_SQL_gold_bike_eventos
│   └── Notebook_Drop_Tables_Bike
│
├── 03_CDC Pipeline/
│   └── declarative-pipeline-cdc/
│       ├── 1-sdp-sql/
│       │   ├── explorations/
│       │   └── transformations/
│       ├── 2-sdp-python/
│       └── _resources/
│
└── Jornada_de_Dados/
```

---

## Competências Demonstradas

### Engenharia de Dados
- Design e implementação de pipelines ETL/ELT
- Processamento batch e streaming
- Modelagem de dados (Medallion Architecture)
- Data quality e data validation
- Ingestão incremental de dados

### Databricks & Spark
- Spark Declarative Pipelines (Lakeflow)
- PySpark e Spark SQL
- Delta Lake operations
- Unity Catalog governance
- Auto Loader configuration

### Boas Práticas
- Separação de camadas (Bronze-Silver-Gold)
- Código versionado e documentado
- Tratamento de erros e qualidade de dados
- Otimização de performance
- Governança e segurança de dados

---

## Como Executar os Projetos

### Pré-requisitos
- Workspace Databricks configurado
- Unity Catalog habilitado
- Permissões de leitura/escrita em catálogo
- Serverless compute ou cluster configurado

### Execução

1. Clone o repositório no seu workspace Databricks
2. Navegue até o projeto desejado
3. Siga as instruções no README específico do projeto
4. Para pipelines:
   - Abra o Pipeline no Lakeflow
   - Configure o catálogo e schema de destino
   - Clique em "Start" para executar
5. Para notebooks:
   - Anexe um compute (ou use serverless)
   - Execute as células sequencialmente

---

## Próximos Passos

- Implementação de testes automatizados para pipelines
- Monitoramento e observabilidade com Databricks Monitoring
- CI/CD com Declarative Automation Bundles (DABs)
- Integração com ferramentas de BI (Power BI, Tableau)
- Machine Learning pipelines com MLflow

---

## Contato

**Fabio Medeiros**  
Engenheiro de Dados  
Email: fabiolrm78@gmail.com  
Telefone: (21) 99663-7177

Desenvolvido como parte do aprendizado contínuo em Databricks e práticas modernas de Engenharia de Dados.
