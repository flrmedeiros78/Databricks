# InvestFacil - Pipeline de Dados B3

Pipeline de dados para ingestao e processamento de acoes da B3 com calculo de indicadores fundamentalistas.

## Arquitetura Medallion

O pipeline segue a arquitetura Medallion com 3 camadas + exportacao:

```mermaid
flowchart TD
    API[("Yahoo Finance<br/>API")]
    JOB["Databricks Job<br/>InvestFacil_Pipeline_Diario"]
    
    BRONZE["Bronze<br/>Ingestão"]
    SILVER["Silver<br/>Transformação"]
    GOLD["Gold<br/>Indicadores"]
    EXPORT["Export<br/>JSON + Push"]
    GITHUB["GitHub<br/>InvestFacilWeb"]
    WEB(("Netlify<br/>📦"))
    
    UC_BRONZE[/"Unity Catalog<br/>bronze.raw_quotes<br/>bronze.raw_fundamentals"/]
    UC_SILVER[/"Unity Catalog<br/>silver.cotacoes<br/>silver.fundamentos"/]
    UC_GOLD[/"Unity Catalog<br/>gold.indicadores_completos<br/>gold.cotacoes_historico"/]
    
    API -->|"866 ações<br/>cotações + fundamentos"| JOB
    JOB -->|"Tarefa 1<br/>107s"| BRONZE
    BRONZE -->|"grava"| UC_BRONZE
    BRONZE -->|"Tarefa 2<br/>42s"| SILVER
    SILVER -->|"grava"| UC_SILVER
    SILVER -->|"Tarefa 3<br/>26s"| GOLD
    GOLD -->|"grava"| UC_GOLD
    GOLD -->|"Tarefa 4<br/>14s"| EXPORT
    EXPORT -->|"3 arquivos JSON<br/>REST API"| GITHUB
    GITHUB -->|"Netlify<br/>deploy automático"| WEB
    
    style API fill:#b3e5fc,stroke:#0288d1,stroke-width:2px
    style JOB fill:#DDA0DD,color:#000,stroke:#9370DB,stroke-width:2px
    style BRONZE fill:#8d6e63,color:#fff,stroke:#5d4037,stroke-width:2px
    style SILVER fill:#bdbdbd,color:#333,stroke:#757575,stroke-width:2px
    style GOLD fill:#ffd54f,color:#333,stroke:#f57f17,stroke-width:2px
    style EXPORT fill:#81c784,color:#fff,stroke:#388e3c,stroke-width:2px
    style GITHUB fill:#81c784,color:#fff,stroke:#388e3c,stroke-width:2px
    style WEB fill:#00C7B7,color:#000,stroke:#00A896,stroke-width:2px
    style UC_BRONZE fill:#bbdefb,color:#000,stroke:#1976d2,stroke-width:2px
    style UC_SILVER fill:#bbdefb,color:#000,stroke:#1976d2,stroke-width:2px
    style UC_GOLD fill:#bbdefb,color:#000,stroke:#1976d2,stroke-width:2px
```

### Bronze (Ingestao)
- **Notebook**: `01_Bronze_Ingestao_B3.py`
- **Fonte**: Yahoo Finance API (yfinance) + brapi.dev
- **Frequencia**: Diaria
- **Output**: 
  - `investfacil_catalog.bronze.raw_quotes` (866 registros)
  - `investfacil_catalog.bronze.raw_fundamentals` (866 registros)

### Silver (Transformacao)
- **Notebook**: `02_Silver_Transformacao.py`
- **Processamento**: Limpeza, normalizacao, mapeamento de nomes (74 empresas) e setores (11 setores traduzidos para portugues)
- **Output**:
  - `investfacil_catalog.silver.cotacoes` (6.062 registros)
  - `investfacil_catalog.silver.fundamentos` (866 registros)

### Gold (Indicadores)
- **Notebook**: `03_Gold_Indicadores.py`
- **Calculos**: 27 indicadores por acao
  - Dividend Yield, P/L (Preco/Lucro), ROE, LPA
  - Volatilidade, Momentum 30d, Beta
  - Volume medio, Valor de mercado, Variacao percentual
- **Output**:
  - `investfacil_catalog.gold.indicadores_completos` (41 acoes x 27 colunas)
  - `investfacil_catalog.gold.cotacoes_historico`

### Export (JSON + Push GitHub)
- **Notebook**: `04_Export_JSON_S3.py`
- **Formato**: JSON para consumo web
- **Arquivos**: indicadores.json, historico.json, metadata.json
- **Destino**: `InvestFacilWeb/Import-Json-InvestFacil/`
- **Push automatico**: Via GitHub REST API com token do Databricks Secret

## Job Diario

**Nome**: InvestFacil_Pipeline_Diario  
**ID**: 679490622792902  
**Schedule**: Diariamente as 19h (apos fechamento do mercado)
- Tarefas: Bronze -> Silver -> Gold -> Export
- Compute: Serverless
- Duracao media: ~3 minutos (189s)
- Tempo por etapa: Bronze 107s | Silver 42s | Gold 26s | Export 14s

## Dados Processados

- **866 acoes** coletadas na Bronze (raw)
- **41 acoes** filtradas e processadas na Gold (top Ibovespa)
- **27 indicadores** calculados por acao
- **Dados atualizados** diariamente
- Nomes e setores em portugues

## Tecnologias

- **Databricks**: Plataforma de processamento (Serverless)
- **PySpark**: Transformacoes de dados
- **Delta Lake**: Armazenamento (tabelas ACID)
- **Yahoo Finance API**: Fonte de dados gratuita
- **GitHub REST API**: Push automatico dos JSONs

## Estrutura de Arquivos

```
Databricks/InvestFacil/
  notebooks/
    00_ConfigToken.ipynb        - Configuracao de tokens/secrets
    01_Bronze_Ingestao_B3.py     - Ingestao de dados B3
    02_Silver_Transformacao.py   - Transformacao e mapeamento
    03_Gold_Indicadores.py       - Calculo de indicadores
    04_Export_JSON_S3.py         - Exportacao e push GitHub
    VALIDACAO.py                 - Validacao dos dados
  config/                        - Configuracoes
  docs/                          - Documentacao
  README.md                      - Este arquivo
  ManualExec.md                  - Manual de execucao passo a passo
```

## Secrets Necessarios

- Scope: `investfacil`
- Key: `github_token` (GitHub PAT para push automatico)

## GitHub

- Repo pipeline: `flrmedeiros78/Databricks` (pasta `InvestFacil/`)
- Repo web: `flrmedeiros78/InvestFacilWeb` (pasta `Import-Json-InvestFacil/`)

## Aplicacao Web

Os dados processados alimentam a aplicacao web InvestFacilWeb:
`https://github.com/flrmedeiros78/InvestFacilWeb`

## Autor

Desenvolvido com Databricks por fabiolrm78@gmail.com
