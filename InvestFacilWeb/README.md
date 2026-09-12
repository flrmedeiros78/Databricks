# InvestFacil - Aplicacao Web

Interface web para analise de acoes da B3 com dados atualizados diariamente.

## Arquivos

- `index.html` - Interface web interativa (12 KB)
- `indicadores.json` - Dados de 251 acoes (143 KB)
- `historico.json` - Historico de cotacoes (em desenvolvimento)
- `metadata.json` - Metadados do pipeline (597 bytes)
- `LINKS_RAPIDOS.txt` - Guia rapido de deploy

## Recursos da Interface

- Dashboard com estatisticas gerais
- Filtros interativos:
  - Busca por ticker
  - Filtro por setor
  - Dividend Yield minimo
  - P/L maximo
- Tabela responsiva ordenada por DY
- Design moderno com gradientes
- Funciona em mobile, tablet e desktop

## Deploy no Netlify

### Opcao 1: Drag & Drop (Rapido)
1. Baixe todos os arquivos desta pasta
2. Acesse: https://app.netlify.com/drop
3. Arraste os arquivos para a pagina
4. Pronto! Site no ar em segundos

### Opcao 2: GitHub + Netlify (Automatico)
1. Faca push desta pasta para um repo GitHub
2. Conecte o repo no Netlify
3. Deploy automatico configurado

## APIs JSON Publicas

Apos o deploy, as APIs estarao disponiveis:

```
https://seu-site.netlify.app/indicadores.json
https://seu-site.netlify.app/historico.json
https://seu-site.netlify.app/metadata.json
```

Use em qualquer aplicacao:
- Excel (Power Query)
- Power BI / Tableau
- Apps mobile
- Qualquer linguagem de programacao

## Estrutura dos Dados

### indicadores.json
Array de objetos com:
- ticker, nome, setor, industria
- preco_atual, volume
- dividend_yield, preco_lucro_pl
- volatilidade, momentum_30d, beta

### historico.json
Array de objetos com historico por ticker (em desenvolvimento)

### metadata.json
Informacoes sobre o pipeline:
- ultima_atualizacao
- total_acoes
- fonte_dados
- indicadores_disponiveis

## Pipeline de Dados

Os dados sao gerados pelo pipeline em:
`../InvestFacil/notebooks/`

Atualizacao: Diaria (19h, apos fechamento do mercado)

## Tecnologias

- Frontend: HTML5, CSS3, JavaScript (puro, sem frameworks)
- Hospedagem: Netlify (gratuito, 100GB banda/mes)
- Dados: Databricks (PySpark, Delta Lake)
- API: Yahoo Finance

## Custo

R$ 0,00 - Completamente gratuito!

## Aviso Legal

Este site nao oferece consultoria financeira.
Dados fornecidos apenas para fins educacionais.

## Autor

Desenvolvido no Databricks Community Edition
