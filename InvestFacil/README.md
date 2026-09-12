# InvestFacil - Pipeline de Dados B3

Pipeline de dados para ingestão e processamento de ações da B3.

## Arquitetura Medallion

O pipeline segue a arquitetura Medallion com 3 camadas:

### Bronze (Ingestão)
- **Notebook**: `01_Bronze_Ingestao_B3.ipynb`
- **Fonte**: Yahoo Finance API (yfinance)
- **Frequência**: Diária
- **Output**: 
  - `investfacil_catalog.bronze.raw_quotes`
  - `investfacil_catalog.bronze.raw_fundamentals`

### Silver (Transformação)
- **Notebook**: `02_Silver_Transformacao.ipynb`
- **Processamento**: Limpeza, normalização, validação
- **Output**:
  - `investfacil_catalog.silver.cotacoes`
  - `investfacil_catalog.silver.fundamentos`

### Gold (Indicadores)
- **Notebook**: `03_Gold_Indicadores.ipynb`
- **Cálculos**: 
  - Dividend Yield
  - P/L (Preço/Lucro)
  - Volatilidade
  - Momentum
  - Liquidez
- **Output**:
  - `investfacil_catalog.gold.indicadores_completos`
  - `investfacil_catalog.gold.cotacoes_historico`

### Export (JSON)
- **Notebook**: `04_Export_JSON_S3.ipynb`
- **Formato**: JSON para consumo web
- **Destino**: Pasta `InvestFacilWeb/`

## Job Diário

**Nome**: InvestFacil_Pipeline_Diario  
**ID**: 679490622792902  
**Schedule**: Diariamente às 19h (após fechamento do mercado)  
**Timeout**: 30 minutos

## Tecnologias

- **Databricks**: Plataforma de processamento
- **PySpark**: Transformações de dados
- **Delta Lake**: Armazenamento (tabelas ACID)
- **Yahoo Finance API**: Fonte de dados gratuita

## Dados Processados

- **251 ações** do Ibovespa
- **Indicadores fundamentalistas** e técnicos
- **Dados atualizados** diariamente
- **Histórico** incremental

## Aplicação Web

Os dados processados alimentam a aplicação web em:
`../InvestFacilWeb/`

## Autor

Desenvolvido com Databricks Community Edition
