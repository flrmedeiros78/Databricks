# InvestFacil - Manual de Execucao

## Descricao
Pipeline de dados que coleta cotacoes da B3, transforma e calcula indicadores fundamentalistas para o app web InvestFacil.

## Arquitetura
```
Bronze (Ingestao) -> Silver (Transformacao) -> Gold (Indicadores) -> Export (JSON)
```

## Estrutura de Arquivos
```
Databricks/InvestFacil/
  notebooks/
    00_ConfigToken.ipynb      - Configuracao de tokens/secrets
    01_Bronze_Ingestao_B3.py   - Coleta dados da B3 via Yahoo Finance API
    02_Silver_Transformacao.py - Limpeza, mapeamento de nomes e setores
    03_Gold_Indicadores.py     - Calculo de Dividend Yield, P/L, ROE, etc
    04_Export_JSON_S3.py       - Exportacao de JSONs e push para GitHub
    VALIDACAO.py               - Validacao dos dados
  config/                      - Configuracoes do pipeline
  docs/                        - Documentacao
  export/                      - JSONs exportados (temporario)
  README.md                    - Documentacao do projeto
  ManualExec.md                - Este arquivo
```

## Unity Catalog
- Catalog: `investfacil_catalog`
- Schemas: `bronze`, `silver`, `gold`
- Tabelas principais:
  - `investfacil_catalog.bronze.cotacoes_raw`
  - `investfacil_catalog.silver.cotacoes_limpo`
  - `investfacil_catalog.gold.indicadores_completos`
  - `investfacil_catalog.gold.cotacoes_historico`

## Job Databricks
- Nome: `InvestFacil_Pipeline_Diario`
- Job ID: `679490622792902`
- Tarefas: Bronze_Ingestao -> Silver_Transformacao -> Gold_Indicadores -> Export_JSON
- Compute: Serverless
- Duracao media: ~2-3 minutos

## Como Executar

### Opcao 1: Via Job (Recomendado)
1. Acesse o Job no Databricks: Jobs > InvestFacil_Pipeline_Diario
2. Clique em "Run Now"
3. Aguarde a conclusao (2-3 minutos)
4. Verifique os resultados nas tabelas Gold

### Opcao 2: Manual (notebook por notebook)
1. Abra o notebook `01_Bronze_Ingestao_B3` e execute todas as celulas
2. Abra o notebook `02_Silver_Transformacao` e execute todas as celulas
3. Abra o notebook `03_Gold_Indicadores` e execute todas as celulas
4. Abra o notebook `04_Export_JSON_S3` e execute todas as celulas

### Opcao 3: Via CLI
```bash
databricks jobs run-now 679490622792902 --no-wait
```

## Secrets Necessarios
- Scope: `investfacil`
- Key: `github_token` (GitHub PAT para push automatico dos JSONs)

## Resultados Esperados
- Bronze: ~743 cotacoes brutas
- Silver: ~41 acoes com nomes traduzidos para portugues
- Gold: 41 acoes com 27 indicadores calculados
- Export: 3 arquivos JSON (indicadores.json, historico.json, metadata.json)

## GitHub
- Repo pipeline: `flrmedeiros78/Databricks` (pasta InvestFacil/)
- Repo web: `flrmedeiros78/InvestFacilWeb` (pasta Import-Json-InvestFacil/)

## Troubleshooting
- **Erro na Bronze**: Verificar conexao com a API do Yahoo Finance
- **Erro no push GitHub**: Verificar se o secret `investfacil/github_token` esta configurado
- **Dados desatualizados**: Re-executar o pipeline completo
- **Setores em ingles**: Verificar o notebook 02_Silver_Transformacao