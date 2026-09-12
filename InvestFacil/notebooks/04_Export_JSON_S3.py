# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Título e Descrição
# MAGIC %md
# MAGIC # 04 - Export JSON para Netlify (InvestFacil)
# MAGIC
# MAGIC **Proposito**: Exportar dados Gold para JSON para consumo do Netlify
# MAGIC
# MAGIC **Entrada**:
# MAGIC - investfacil_catalog.gold.indicadores_completos
# MAGIC - investfacil_catalog.gold.cotacoes_historico
# MAGIC
# MAGIC **Saida**:
# MAGIC - indicadores.json
# MAGIC - historico.json
# MAGIC - metadata.json
# MAGIC
# MAGIC **Uso no Netlify**:
# MAGIC - O site pode acessar esses JSONs diretamente
# MAGIC - Arquivos disponiveis no Workspace para download
# MAGIC - Custo ZERO (sem SQL Warehouse rodando)

# COMMAND ----------

# DBTITLE 1,Imports e Configurações
import json
from datetime import datetime
from pyspark.sql.functions import col, to_json, struct, collect_list

# Configurações de Export (Unity Catalog Volume)
# Nota: Para usar S3, configure as credenciais AWS no cluster primeiro
EXPORT_PATH = "/Workspace/Users/fabiolrm78@gmail.com/InvestFacil/export/"

# Criar diretório de export se não existir
try:
    dbutils.fs.mkdirs(f"file:{EXPORT_PATH}")
except:
    pass  # Diretório já existe

# COMMAND ----------

# DBTITLE 1,Ler Dados Gold
# 1. Ler dados Gold
print("[INFO] Lendo dados Gold...")

df_indicadores = spark.table("investfacil_catalog.gold.indicadores_completos")
df_historico = spark.table("investfacil_catalog.gold.cotacoes_historico")

print(f"[OK] Indicadores: {df_indicadores.count()} acoes")
print(f"[OK] Historico: {df_historico.count()} registros")

# COMMAND ----------

# DBTITLE 1,Converter Indicadores para JSON
# 2. Converter indicadores para JSON
print("\n[INFO] Convertendo indicadores para JSON...")

# Selecionar apenas campos relevantes para o site
df_indicadores_export = df_indicadores.select(
    "ticker",
    "symbol",
    "nome_curto",
    "nome_completo",
    "setor",
    "industria",
    "preco_atual",
    "variacao_percentual",
    "volume",
    "volume_medio",
    "valor_mercado",
    "dividend_yield",
    "preco_lucro_pl",
    "retorno_patrimonio_roe",
    "lucro_por_acao_lpa",
    "volatilidade",
    "momentum_30d",
    "beta",
    "logo_url",
    "website"
)

# Converter para Pandas e depois para JSON
indicadores_json = df_indicadores_export.toPandas().to_json(
    orient='records', 
    date_format='iso',
    force_ascii=False
)

print(f"[OK] JSON de indicadores gerado ({len(indicadores_json)} bytes)")

# COMMAND ----------

# DBTITLE 1,Converter Histórico para JSON (agrupado por ticker)
# 3. Converter historico para JSON agrupado por ticker
print("\n[INFO] Convertendo historico para JSON...")

# Agrupar por ticker para reduzir tamanho do JSON
from pyspark.sql.functions import collect_list, struct

df_historico_agrupado = df_historico \
    .groupBy("ticker") \
    .agg(
        collect_list(
            struct(
                col("data").cast("string").alias("data"),
                col("preco")
            )
        ).alias("historico")
    )

historico_json = df_historico_agrupado.toPandas().to_json(
    orient='records',
    date_format='iso',
    force_ascii=False
)

print(f"[OK] JSON de historico gerado ({len(historico_json)} bytes)")

# COMMAND ----------

# DBTITLE 1,Criar Metadata JSON
# 4. Criar arquivo de metadata com informacoes do pipeline
print("\n[INFO] Criando metadata...")

metadata = {
    "projeto": "InvestFacil",
    "versao": "1.0",
    "ultima_atualizacao": datetime.now().isoformat(),
    "fonte_dados": "brapi.dev",
    "bolsas": ["B3"],
    "total_acoes": df_indicadores.count(),
    "total_dias_historico": df_historico.select("data").distinct().count(),
    "indicadores_disponiveis": [
        "dividend_yield",
        "preco_lucro_pl",
        "retorno_patrimonio_roe",
        "volatilidade",
        "momentum_30d",
        "volume_medio"
    ],
    "descricao": "Dados atualizados diariamente. O usuario escolhe seus proprios criterios de investimento.",
    "aviso": "Este site nao oferece consultoria financeira. Dados fornecidos apenas para fins educacionais."
}

metadata_json = json.dumps(metadata, indent=2, ensure_ascii=False)
print(f"[OK] Metadata gerado")

# COMMAND ----------

# DBTITLE 1,Gravar JSONs no Workspace
# 5. Gravar JSONs no Workspace
print("\n[INFO] Gravando JSONs no Workspace...")

# Função auxiliar para gravar JSON no Workspace
def write_json_to_workspace(json_content, filename):
    """Grava conteudo JSON no Workspace"""
    full_path = f"file:{EXPORT_PATH}{filename}"
    
    # Usar dbutils.fs.put para gravar no Workspace
    dbutils.fs.put(full_path, json_content, overwrite=True)
    print(f"[OK] {filename} gravado em {EXPORT_PATH}{filename}")

# Gravar os 3 JSONs
write_json_to_workspace(indicadores_json, "indicadores.json")
write_json_to_workspace(historico_json, "historico.json")
write_json_to_workspace(metadata_json, "metadata.json")

print(f"\n[SUCCESS] Export concluido! Arquivos em: {EXPORT_PATH}")

# COMMAND ----------

# DBTITLE 1,Validar Arquivos no Workspace
# 6. Validar arquivos gravados no Workspace
print("\n[INFO] Validando arquivos no Workspace...")

files = dbutils.fs.ls(f"file:{EXPORT_PATH}")
for file_info in files:
    size_kb = file_info.size / 1024
    print(f"  [OK] {file_info.name} - {size_kb:.2f} KB")

print(f"\n[SUCCESS] Arquivos prontos em: {EXPORT_PATH}")
print(f"\n[INFO] Arquivos disponiveis para download e deploy no Netlify")

# COMMAND ----------

# DBTITLE 1,Próximos Passos para Netlify
# MAGIC %md
# MAGIC ## Proximos Passos para Netlify
# MAGIC
# MAGIC ### Passo 1: Download dos Arquivos JSON
# MAGIC
# MAGIC Os arquivos estao disponiveis em:
# MAGIC /Workspace/Users/fabiolrm78@gmail.com/InvestFacil/export/
# MAGIC
# MAGIC **Arquivos gerados:**
# MAGIC - indicadores.json (107 KB)
# MAGIC - historico.json (vazio - aguardando dados historicos)
# MAGIC - metadata.json (0.6 KB)
# MAGIC
# MAGIC **Para fazer download:**
# MAGIC 1. Navegue ate o diretorio export/ no Workspace
# MAGIC 2. Clique com botao direito em cada arquivo
# MAGIC 3. Selecione "Download"
# MAGIC
# MAGIC ### Passo 2: Deploy no Netlify
# MAGIC
# MAGIC **Opcao 1: Arquivos Estaticos no Netlify**
# MAGIC 1. Coloque os JSONs no diretorio public/ do seu projeto Netlify
# MAGIC 2. Acesse via: https://seu-site.netlify.app/indicadores.json
# MAGIC 3. Vantagem: Simples e rapido
# MAGIC 4. Desvantagem: Precisa fazer upload manual a cada atualizacao
# MAGIC
# MAGIC **Opcao 2: Netlify Function (Recomendado)**
# MAGIC 1. Crie uma funcao que busca dados diretamente do Databricks
# MAGIC 2. Use Databricks SQL Warehouse API ou Unity Catalog API
# MAGIC 3. Vantagem: Dados sempre atualizados
# MAGIC 4. Desvantagem: Requer configuracao de API
# MAGIC
# MAGIC ### Passo 3 (Futuro): Integracao com S3
# MAGIC
# MAGIC Apos validar o funcionamento no Netlify, podemos configurar:
# MAGIC - Upload automatico para S3
# MAGIC - CloudFront para cache
# MAGIC - Reducao de custos e latencia
# MAGIC
# MAGIC ### Estrutura dos JSONs
# MAGIC
# MAGIC **indicadores.json** (107 KB)
# MAGIC ```json
# MAGIC [
# MAGIC   {
# MAGIC     "ticker": "PETR4",
# MAGIC     "nome_curto": "PETROBRAS PN",
# MAGIC     "preco_atual": 49.0,
# MAGIC     "dividend_yield": 0.15,
# MAGIC     "preco_lucro_pl": 4.2,
# MAGIC     "volatilidade": 0.025,
# MAGIC     "momentum_30d": 5.3,
# MAGIC     "setor": "Energy"
# MAGIC   }
# MAGIC ]
# MAGIC ```
# MAGIC
# MAGIC **historico.json** (Vazio - aguardando dados historicos)
# MAGIC ```json
# MAGIC [
# MAGIC   {
# MAGIC     "ticker": "PETR4",
# MAGIC     "historico": [
# MAGIC       {"data": "2026-09-11", "preco": 49.0},
# MAGIC       {"data": "2026-09-10", "preco": 48.5}
# MAGIC     ]
# MAGIC   }
# MAGIC ]
# MAGIC ```
# MAGIC
# MAGIC **metadata.json** (0.6 KB)
# MAGIC ```json
# MAGIC {
# MAGIC   "projeto": "InvestFacil",
# MAGIC   "ultima_atualizacao": "2026-09-12T18:30:00",
# MAGIC   "total_acoes": 251,
# MAGIC   "fonte_dados": "yahoo_finance"
# MAGIC }
# MAGIC ```
# MAGIC ```

# COMMAND ----------

