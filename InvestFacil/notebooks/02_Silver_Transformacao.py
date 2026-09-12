# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Título e Descrição
# MAGIC %md
# MAGIC # 02 - Transformacao Silver (InvestFacil)
# MAGIC
# MAGIC **Proposito**: Limpeza, normalizacao e validacao de dados da B3
# MAGIC
# MAGIC **Entrada**:
# MAGIC - investfacil_catalog.bronze.raw_quotes
# MAGIC - investfacil_catalog.bronze.raw_fundamentals
# MAGIC
# MAGIC **Saida**:
# MAGIC - investfacil_catalog.silver.cotacoes - Cotacoes normalizadas
# MAGIC - investfacil_catalog.silver.fundamentos - Dados fundamentalistas normalizados
# MAGIC
# MAGIC **Transformacoes**:
# MAGIC - Parse do JSON raw_payload
# MAGIC - Normalizacao de tipos de dados
# MAGIC - Validacao de qualidade (precos > 0, datas validas)
# MAGIC - Remocao de duplicatas
# MAGIC - Padronizacao de formatos

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.functions import (
    col, from_json, to_date, to_timestamp, when, coalesce, 
    lit, regexp_replace, trim, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    LongType, TimestampType, DateType
)
import json

# COMMAND ----------

# DBTITLE 1,Ler Tabelas Bronze
# 1. Ler tabelas Bronze
print("[INFO] Lendo dados da camada Bronze...")

df_raw_quotes = spark.table("investfacil_catalog.bronze.raw_quotes")
df_raw_fundamentals = spark.table("investfacil_catalog.bronze.raw_fundamentals")

print(f"[OK] raw_quotes: {df_raw_quotes.count()} registros")
print(f"[OK] raw_fundamentals: {df_raw_fundamentals.count()} registros")

# COMMAND ----------

# DBTITLE 1,Schema para Parse de Cotações
# 2. Definir schema para parse do JSON de cotacoes (Yahoo Finance)
quote_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("volume", LongType(), True),
    StructField("timestamp", StringType(), True)
])

# COMMAND ----------

# DBTITLE 1,Transformar Cotações para Silver
# 3. Parse e transformacao de cotacoes (Yahoo Finance)
print("\n[INFO] Transformando cotacoes...")

df_cotacoes_silver = df_raw_quotes \
    .withColumn("parsed", from_json(col("raw_payload"), quote_schema)) \
    .select(
        col("ticker"),
        col("parsed.symbol").alias("symbol"),
        col("ticker").alias("nome_curto"),
        col("ticker").alias("nome_completo"),
        lit("BRL").alias("moeda"),
        col("parsed.price").alias("preco_atual"),
        col("parsed.open").alias("preco_abertura"),
        col("parsed.high").alias("preco_maximo"),
        col("parsed.low").alias("preco_minimo"),
        lit(None).cast(DoubleType()).alias("preco_fechamento_anterior"),
        lit(None).cast(DoubleType()).alias("variacao_absoluta"),
        lit(None).cast(DoubleType()).alias("variacao_percentual"),
        col("parsed.volume").alias("volume"),
        lit(None).cast(LongType()).alias("valor_mercado"),
        to_timestamp(col("parsed.timestamp")).alias("data_cotacao"),
        lit(None).cast(StringType()).alias("logo_url"),
        col("ingestion_timestamp").alias("data_ingestao")
    ) \
    .filter(col("preco_atual") > 0) \
    .filter(col("preco_atual").isNotNull()) \
    .dropDuplicates(["ticker", "data_cotacao"])

print(f"[OK] {df_cotacoes_silver.count()} cotacoes transformadas")

# COMMAND ----------

# DBTITLE 1,Schema para Parse de Fundamentos
# 4. Definir schema para parse de dados fundamentalistas (Yahoo Finance)
fundamentals_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("marketCap", LongType(), True),
    StructField("peRatio", DoubleType(), True),
    StructField("dividendYield", DoubleType(), True),
    StructField("beta", DoubleType(), True),
    StructField("fiftyTwoWeekHigh", DoubleType(), True),
    StructField("fiftyTwoWeekLow", DoubleType(), True),
    StructField("sector", StringType(), True),
    StructField("industry", StringType(), True)
])

# COMMAND ----------

# DBTITLE 1,Transformar Fundamentos para Silver
# 5. Parse e transformacao de fundamentos (Yahoo Finance)
print("\n[INFO] Transformando fundamentos...")

df_fundamentos_silver = df_raw_fundamentals \
    .withColumn("parsed", from_json(col("raw_payload"), fundamentals_schema)) \
    .select(
        col("ticker"),
        col("parsed.symbol").alias("symbol"),
        col("parsed.sector").alias("setor"),
        col("parsed.industry").alias("industria"),
        lit(None).cast(StringType()).alias("website"),  # Yahoo nao retorna no basic info
        lit(None).cast(StringType()).alias("descricao_negocio"),  # Yahoo nao retorna no basic info
        col("parsed.peRatio").alias("preco_lucro_pl"),
        lit(None).cast(DoubleType()).alias("retorno_patrimonio_roe"),  # Nao disponivel no Yahoo basic
        lit(None).cast(DoubleType()).alias("lucro_por_acao_lpa"),  # Nao disponivel no Yahoo basic
        col("parsed.dividendYield").alias("dividend_yield"),
        lit(None).cast(DoubleType()).alias("dividendo_valor"),  # Nao disponivel no Yahoo basic
        lit(None).cast(DoubleType()).alias("payout_ratio"),  # Nao disponivel no Yahoo basic
        col("parsed.beta").alias("beta"),
        col("parsed.marketCap").alias("valor_mercado"),
        col("parsed.fiftyTwoWeekHigh").alias("maxima_52_semanas"),
        col("parsed.fiftyTwoWeekLow").alias("minima_52_semanas"),
        col("ingestion_timestamp").alias("data_ingestao")
    ) \
    .dropDuplicates(["ticker", "data_ingestao"])

print(f"[OK] {df_fundamentos_silver.count()} fundamentos transformados")

# COMMAND ----------

# DBTITLE 1,Gravar Tabelas Silver
# 6. Gravar tabelas Silver (modo OVERWRITE - sempre a ultima versao limpa)
print("\n[INFO] Gravando tabelas Silver...")

df_cotacoes_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("investfacil_catalog.silver.cotacoes")

print("[OK] Tabela silver.cotacoes criada")

if df_fundamentos_silver.count() > 0:
    df_fundamentos_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("investfacil_catalog.silver.fundamentos")
    print("[OK] Tabela silver.fundamentos criada")
else:
    print("[WARNING] Nenhum fundamento para gravar")

# COMMAND ----------

# DBTITLE 1,Validar Dados Silver
# MAGIC %sql
# MAGIC -- 7. Validar dados Silver
# MAGIC SELECT 
# MAGIC     'cotacoes' as tabela,
# MAGIC     COUNT(*) as total_registros,
# MAGIC     COUNT(DISTINCT ticker) as tickers_unicos,
# MAGIC     MIN(preco_atual) as preco_minimo,
# MAGIC     MAX(preco_atual) as preco_maximo,
# MAGIC     AVG(preco_atual) as preco_medio
# MAGIC FROM investfacil_catalog.silver.cotacoes
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'fundamentos' as tabela,
# MAGIC     COUNT(*) as total_registros,
# MAGIC     COUNT(DISTINCT ticker) as tickers_unicos,
# MAGIC     0 as preco_minimo,
# MAGIC     0 as preco_maximo,
# MAGIC     0 as preco_medio
# MAGIC FROM investfacil_catalog.silver.fundamentos;

# COMMAND ----------

# DBTITLE 1,Visualizar Amostra Silver
# MAGIC %sql
# MAGIC -- 8. Visualizar amostra de dados Silver
# MAGIC SELECT 
# MAGIC     ticker,
# MAGIC     nome_curto,
# MAGIC     moeda,
# MAGIC     preco_atual,
# MAGIC     variacao_percentual,
# MAGIC     volume,
# MAGIC     data_cotacao
# MAGIC FROM investfacil_catalog.silver.cotacoes
# MAGIC ORDER BY volume DESC
# MAGIC LIMIT 10;

# COMMAND ----------

