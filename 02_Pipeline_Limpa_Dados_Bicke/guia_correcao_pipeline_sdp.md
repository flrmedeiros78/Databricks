# Guia Passo a Passo: Como Diagnosticar e Corrigir um Pipeline SDP com Erros

> Guia prático baseado na correção do pipeline `Pipeline_SQL_bronze_silver_gold_bike_enventos`.
> Aplicável a qualquer pipeline Spark Declarative Pipelines (SDP) em Databricks.

---

## Visão Geral do Fluxo

```
Pipeline falha
    ↓
Passo 1: Ler o event log → identificar tipo de erro
    ↓
Erro de infraestrutura? → Passo 2: Liberar compute serverless
    ↓
Erro de código? → Passo 3: Verificar sintaxe obsoleta
    ↓
                 Passo 3b: Remover instruções não-DLT
    ↓
                 Passo 4: Adicionar STREAM() no read_files()
    ↓
Passo 5: Validar com dry run
    ↓
    Falhou? → Voltar ao Passo 3/4
    Passou? → Passo 6: Executar pipeline
    ↓
Passo 7: Verificar resultados
    ↓
✅ Concluído
```

---

## Passo 1 — Identificar o Tipo de Erro

Quando um pipeline falha, o primeiro passo é verificar o **status da última atualização**.
Acesse a página de monitoramento do pipeline e veja a última execução (update).

Existem dois tipos principais de erro:

- **Erro de código** — o SDP não aceita a sintaxe usada nos notebooks
- **Erro de infraestrutura** — falta de recursos de compute (serverless, cluster, etc.)

### Como identificar

Leia a **mensagem de erro** no event log da atualização falha:

| Mensagem de erro | Tipo |
| --- | --- |
| `CLUSTER_CREATION_RESOURCE_EXHAUSTED` ou `RESOURCE_EXHAUSTED` | Infraestrutura |
| `_LEGACY_ERROR_TEMP_54_UNSUPPORTED_SQL_COMMAND` | Código — sintaxe obsoleta |
| `UNSUPPORTED_SQL_COMMAND` | Código — instrução não-DLT |
| `CREATE_APPEND_ONCE_FLOW_FROM_BATCH_QUERY_NOT_ALLOWED` | Código — faltando STREAM() |

---

## Passo 2 — Erro de Infraestrutura (Compute Serverless)

### Sintoma

```
RESOURCE_EXHAUSTED: You've hit the limit for severless compute for free usage.
Stop or delete existing serverless compute to free up capacity.
```

### O que fazer

1. **Verificar recursos ativos** — liste os SQL warehouses e compute serverless do workspace
2. **Parar recursos desnecessários** — se houver um SQL warehouse serverless rodando, pare-o:

   ```python
   from databricks.sdk import WorkspaceClient
   w = WorkspaceClient()
   w.warehouses.stop(id="<warehouse_id>")
   print("Warehouse parado com sucesso")
   ```

3. **Tentar novamente** — depois de liberar recursos, execute um dry run novamente

> **Importante:** Mesmo um dry run (validação apenas) precisa de compute serverless.
> Sem recursos disponíveis, nenhuma validação ou execução acontece.

---

## Passo 3 — Erro de Código: Sintaxe Obsoleta

### Sintoma

```
_LEGACY_ERROR_TEMP_54_UNSUPPORTED_SQL_COMMAND:
The following code is not supported in a DLT pipeline.
DLT currently accepts 'CREATE MATERIALIZED VIEW', 'CREATE STREAMING TABLE',
'APPLY CHANGES INTO', and 'SET' statements.
```

### 3a. Sintaxe obsoleta `STREAMING LIVE TABLE` ou `LIVE TABLE`

O SDP (antigo DLT) não aceita mais a sintaxe antiga:

| ❌ Obsoleto (DLT) | ✅ Atual (SDP) |
| --- | --- |
| `CREATE OR REFRESH STREAMING LIVE TABLE` | `CREATE OR REFRESH STREAMING TABLE` |
| `CREATE OR REFRESH LIVE VIEW` | `CREATE OR REFRESH MATERIALIZED VIEW` |
| `SELECT * FROM LIVE.tabela` | `SELECT * FROM tabela` |

### Como corrigir

1. Abrir cada notebook do pipeline
2. Substituir todas as instruções `STREAMING LIVE TABLE` por `STREAMING TABLE`
3. Substituir todas as instruções `LIVE VIEW` por `MATERIALIZED VIEW`
4. Remover qualquer referência a `LIVE.` nas consultas
5. **Limpar linhas comentadas** que contenham a sintaxe obsoleta (para evitar confusão futura)

### Exemplo prático

❌ **Antes (obsoleto):**
```sql
CREATE OR REFRESH STREAMING LIVE TABLE tb_bronze_bike_eventos
AS SELECT * FROM LIVE.outra_tabela;
```

✅ **Depois (correto):**
```sql
CREATE OR REFRESH STREAMING TABLE tb_bronze_bike_eventos
AS SELECT * FROM outra_tabela;
```

---

## Passo 3b — Instruções não-DLT no Notebook

### Sintoma

```
UNSUPPORTED_SQL_COMMAND: code is not supported in a DLT pipeline
```

### Causa

O notebook do pipeline contém consultas SQL que não são instruções DLT válidas:

- `SELECT * FROM tabela LIMIT 5;` (consulta de debug)
- `DROP TABLE tabela;`
- `INSERT INTO ...`
- `SHOW TABLES;`
- `DESCRIBE TABLE ...`

### O que o SDP aceita (apenas estes 4 tipos)

1. `CREATE STREAMING TABLE` — para dados streaming/incrementais
2. `CREATE MATERIALIZED VIEW` — para dados batch/agregações
3. `APPLY CHANGES INTO` — para CDC/upserts
4. `SET` — inclui `USE CATALOG` e `USE SCHEMA`

### Como corrigir

1. Identificar a célula com o erro (o event log indica `[cell: X]`)
2. **Remover** a célula de debug/consulta ou **convertê-la em markdown** (célula de texto)
3. Manter apenas instruções DLT válidas no notebook

> **Dica:** Consultas de exploração (`SELECT`, `DESCRIBE`, `SHOW`) devem ficar em
> notebooks separados, fora do pipeline.

---

## Passo 4 — Erro de Streaming: Faltando `STREAM()` no `read_files()`

### Sintoma

```
CREATE_APPEND_ONCE_FLOW_FROM_BATCH_QUERY_NOT_ALLOWED:
Cannot create a streaming table append once flow from a batch query.
To fix this, use the STREAM() or readStream operator to declare the source
as a streaming input.
```

### Causa

Ao criar uma `STREAMING TABLE` com `read_files()` (Auto Loader), é obrigatório
usar a palavra-chave `STREAM`. Sem ela, o SDP trata a leitura como batch (lote
único) e não como streaming incremental.

### ❌ Incorreto

```sql
CREATE OR REFRESH STREAMING TABLE tb_bronze_bike_eventos
AS SELECT *
FROM read_files(
  '/Volumes/dbacademy/default/raw_data/rides/build_data_pipeline_demo/',
  format => 'csv',
  header => 'true',
  inferSchema => 'true',
  sep => ','
);
```

### ✅ Correto

```sql
CREATE OR REFRESH STREAMING TABLE tb_bronze_bike_eventos
AS SELECT *
FROM STREAM(read_files(
  '/Volumes/dbacademy/default/raw_data/rides/build_data_pipeline_demo/',
  format => 'csv',
  header => 'true',
  inferSchema => 'true',
  sep => ','
));
```

### Como corrigir

1. Envolva `read_files(...)` com `STREAM(...)`
2. Adicione o parêntese de fechamento extra no final: `STREAM(read_files(...))`

---

## Passo 5 — Validar com Dry Run

Após cada correção de código, **sempre valide** antes de executar:

1. Inicie um **dry run** (validação sem execução)
2. Se falhar — leia o novo erro, volte ao Passo 3 ou 4 e corrija
3. Se passar — prossiga para o Passo 6

> O dry run é rápido e não processa dados. Ele apenas valida se o código está
> correto sintaticamente e semanticamente para o SDP.

---

## Passo 6 — Executar o Pipeline

Após o dry run passar:

1. Inicie uma **atualização regular** (`fullRefresh: false`) — o pipeline criará as
   tabelas se não existirem
2. Se as tabelas já existirem com dados corrompidos ou esquema incompatível, use
   **full refresh** (`fullRefresh: true`) — mas atenção: isso apaga e recria tudo
3. Monitore a execução até o status **COMPLETED**

---

## Passo 7 — Verificar os Resultados

Após a conclusão:

1. Verifique os **datasets** criados pelo pipeline
2. Confirme o número de linhas em cada tabela
3. Se necessário, faça consultas de validação nos dados

---

## Checklist Rápido de Validação de Código SDP

Antes de executar o pipeline, verifique cada notebook:

- [ ] Usa `CREATE STREAMING TABLE` (não `STREAMING LIVE TABLE`)
- [ ] Usa `CREATE MATERIALIZED VIEW` (não `LIVE VIEW`)
- [ ] Não contém `SELECT`, `DROP`, `INSERT`, `SHOW` isolados
- [ ] `read_files()` em `STREAMING TABLE` está envolvido com `STREAM()`
- [ ] `USE CATALOG` e `USE SCHEMA` são as únicas instruções `SET`
- [ ] Não há referências a `LIVE.` em nenhuma consulta
- [ ] Cada célula de código contém apenas uma instrução DLT válida

---

## Erros Comuns e Soluções Resumidas

| Erro | Causa | Solução |
| --- | --- | --- |
| `_LEGACY_ERROR_TEMP_54_UNSUPPORTED_SQL_COMMAND` | Sintaxe `STREAMING LIVE TABLE` obsoleta | Trocar por `STREAMING TABLE` |
| `UNSUPPORTED_SQL_COMMAND` | Célula com `SELECT` ou `DROP` no pipeline | Remover célula ou converter em markdown |
| `CREATE_APPEND_ONCE_FLOW_FROM_BATCH_QUERY_NOT_ALLOWED` | `read_files()` sem `STREAM()` | Envolver com `STREAM(read_files(...))` |
| `CLUSTER_CREATION_RESOURCE_EXHAUSTED` | Cota de serverless esgotada | Parar SQL warehouses serverless desnecessários |

---

## Tabelas Válidas no SDP

| Tipo | Quando usar |
| --- | --- |
| `CREATE STREAMING TABLE` | Fontes streaming/incrementais (Auto Loader, Kafka, CDC) |
| `CREATE MATERIALIZED VIEW` | Fontes batch/agregações (SELECT, GROUP BY, JOIN) |
| `APPLY CHANGES INTO` | CDC/upserts/SCD Type 1, 2 ou bitemporal |
| `SET` | `USE CATALOG`, `USE SCHEMA`, configurações |

---

*Guia criado em 06/10/2026 — baseado na correção do pipeline Pipeline_SQL_bronze_silver_gold_bike_enventos.*