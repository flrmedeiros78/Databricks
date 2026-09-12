# InvestFácil - Pipeline de Dados B3

## 📁 Estrutura do Projeto

```
InvestFacil/
├── notebooks/          # Notebooks do pipeline ETL
│   ├── 01_Bronze_Ingestao_B3.ipynb
│   ├── 02_Silver_Transformacao.ipynb
│   ├── 03_Gold_Indicadores.ipynb
│   └── 04_Export_JSON_S3.ipynb
├── docs/              # Documentação do projeto
│   └── arquitetura.md
└── config/            # Arquivos de configuração
    └── parametros.json

```

## 🎯 Objetivo

Pipeline automatizado para ingestão, transformação e análise de dados da B3 (Bolsa de Valores brasileira), 
fornecendo indicadores fundamentalistas para o site InvestFácil.

## 🏗️ Arquitetura Medalhão

- **Bronze**: Dados brutos da API brapi.dev
- **Silver**: Dados limpos e normalizados  
- **Gold**: Indicadores calculados e prontos para consumo

## 📊 Unity Catalog

- **Catalog**: `investfacil_catalog`
- **Schemas**: `bronze`, `silver`, `gold`

## 🔄 Job Orquestrador

- **Nome**: InvestFacil_Pipeline_Diario
- **Schedule**: Diariamente às 19h (America/Sao_Paulo)
- **Compute**: Serverless

## 📤 Output

Arquivos JSON exportados para S3:
- `s3://bucket-aws-databricks/investfacil/export/indicadores.json`
- `s3://bucket-aws-databricks/investfacil/export/historico.json`
- `s3://bucket-aws-databricks/investfacil/export/metadata.json`

## 🌐 Integração Netlify

O front-end consome os JSONs do S3 via CloudFront (zero custo de compute).
