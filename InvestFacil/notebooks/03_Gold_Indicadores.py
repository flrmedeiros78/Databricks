# Databricks notebook source
# DBTITLE 1,Título e Descrição
# MAGIC %md
# MAGIC # 03 - Camada Gold - Indicadores para InvestFacil
# MAGIC
# MAGIC **Proposito**: Calcular indicadores fundamentalistas e tecnicos para o site
# MAGIC
# MAGIC **Entrada**:
# MAGIC - investfacil_catalog.silver.cotacoes
# MAGIC - investfacil_catalog.silver.fundamentos
# MAGIC - investfacil_catalog.bronze.raw_quotes (para historico de volatilidade)
# MAGIC
# MAGIC **Saida**:
# MAGIC - investfacil_catalog.gold.indicadores_completos - Todos os indicadores por acao
# MAGIC - investfacil_catalog.gold.cotacoes_historico - Historico para graficos
# MAGIC
# MAGIC **Indicadores Calculados**:
# MAGIC 1. **Dividend Yield** (rendimento de dividendos)
# MAGIC 2. **P/L** (preco sobre lucro)
# MAGIC 3. **ROE** (retorno sobre patrimonio)
# MAGIC 4. **Volatilidade** (desvio padrao dos retornos diarios)
# MAGIC 5. **Liquidez** (volume medio negociado)
# MAGIC 6. **Momentum** (variacao de preco nos ultimos 30 dias)
# MAGIC
# MAGIC **Nota**: O site permite ao usuario escolher seus proprios criterios - nao fazemos ranking automatico

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.functions import (
    col, avg, stddev, min as spark_min, max as spark_max, 
    count, sum as spark_sum, datediff, lag, when, coalesce,
    first, last, round as spark_round, lit
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
import json

# COMMAND ----------

# DBTITLE 1,Ler Dados Silver
# 1. Ler dados Silver
print("[INFO] Lendo dados Silver...")

df_cotacoes = spark.table("investfacil_catalog.silver.cotacoes")
df_fundamentos = spark.table("investfacil_catalog.silver.fundamentos")

print(f"[OK] Cotacoes: {df_cotacoes.count()} registros")
print(f"[OK] Fundamentos: {df_fundamentos.count()} registros")

# COMMAND ----------

# DBTITLE 1,Calcular Histórico para Volatilidade e Momentum
# 2. Buscar historico de cotacoes da Bronze (para calculo de volatilidade)
print("\n[INFO] Calculando historico e volatilidade...")

from pyspark.sql.functions import from_json, to_date

# Schema simplificado para extrair apenas preco e data
historico_schema = "struct<symbol:string, regularMarketPrice:double, regularMarketTime:string>"

df_historico = spark.table("investfacil_catalog.bronze.raw_quotes") \
    .withColumn("parsed", from_json(col("raw_payload"), historico_schema)) \
    .select(
        col("ticker"),
        col("parsed.regularMarketPrice").alias("preco"),
        to_date(col("parsed.regularMarketTime")).alias("data")
    ) \
    .filter(col("preco") > 0) \
    .filter(col("data").isNotNull()) \
    .dropDuplicates(["ticker", "data"]) \
    .orderBy("ticker", "data")

print(f"[OK] Historico extraido: {df_historico.count()} registros")

# COMMAND ----------

# DBTITLE 1,Calcular Retornos e Volatilidade
# 3. Calcular retornos diarios e volatilidade
print("\n[INFO] Calculando retornos e volatilidade...")

# Window para calcular retorno diario (preco de hoje / preco de ontem - 1)
window_spec = Window.partitionBy("ticker").orderBy("data")

df_retornos = df_historico \
    .withColumn("preco_anterior", lag("preco", 1).over(window_spec)) \
    .withColumn(
        "retorno_diario",
        when(col("preco_anterior").isNotNull(), 
             (col("preco") / col("preco_anterior") - 1))
        .otherwise(None)
    ) \
    .filter(col("retorno_diario").isNotNull())

# Calcular volatilidade (desvio padrao dos retornos) e momentum (retorno acumulado 30 dias)
df_volatilidade = df_retornos \
    .groupBy("ticker") \
    .agg(
        stddev("retorno_diario").alias("volatilidade"),
        count("retorno_diario").alias("dias_historico")
    )

# Calcular momentum (variacao % nos ultimos 30 dias)
window_30d = Window.partitionBy("ticker").orderBy(col("data").desc()).rowsBetween(0, 29)

df_momentum = df_historico \
    .withColumn("preco_30d_atras", last("preco").over(window_30d)) \
    .withColumn(
        "momentum_30d",
        (col("preco") / col("preco_30d_atras") - 1) * 100
    ) \
    .groupBy("ticker") \
    .agg(first("momentum_30d").alias("momentum_30d"))

print(f"[OK] Volatilidade calculada para {df_volatilidade.count()} tickers")
print(f"[OK] Momentum calculado para {df_momentum.count()} tickers")

# COMMAND ----------

# DBTITLE 1,Calcular Liquidez Média
# 4. Calcular liquidez (volume medio negociado)
print("\n[INFO] Calculando liquidez media...")

df_liquidez = df_cotacoes \
    .groupBy("ticker") \
    .agg(
        avg("volume").alias("volume_medio"),
        spark_sum("volume").alias("volume_total")
    )

print(f"[OK] Liquidez calculada para {df_liquidez.count()} tickers")

# COMMAND ----------

# DBTITLE 1,Consolidar Todos os Indicadores
# 5. Consolidar todos os indicadores em uma unica tabela Gold
print("\n[INFO] Consolidando indicadores...")

# Comecar com as cotacoes mais recentes
df_gold = df_cotacoes \
    .select(
        "ticker",
        "symbol",
        "nome_curto",
        "nome_completo",
        "moeda",
        "preco_atual",
        "variacao_percentual",
        "volume",
        "valor_mercado",
        "logo_url",
        "data_cotacao"
    )

# Join com fundamentos
df_gold = df_gold.join(
    df_fundamentos.select(
        "ticker",
        "setor",
        "industria",
        "website",
        "descricao_negocio",
        "preco_lucro_pl",
        "retorno_patrimonio_roe",
        "lucro_por_acao_lpa",
        "dividend_yield",
        "dividendo_valor",
        "payout_ratio",
        "beta"
    ),
    on="ticker",
    how="left"
)

# Join com volatilidade
df_gold = df_gold.join(df_volatilidade, on="ticker", how="left")

# Join com momentum
df_gold = df_gold.join(df_momentum, on="ticker", how="left")

# Join com liquidez
df_gold = df_gold.join(df_liquidez, on="ticker", how="left")

# Arredondar valores para apresentacao
df_gold = df_gold \
    .withColumn("preco_atual", spark_round("preco_atual", 2)) \
    .withColumn("variacao_percentual", spark_round("variacao_percentual", 2)) \
    .withColumn("preco_lucro_pl", spark_round("preco_lucro_pl", 2)) \
    .withColumn("retorno_patrimonio_roe", spark_round("retorno_patrimonio_roe", 4)) \
    .withColumn("dividend_yield", spark_round("dividend_yield", 4)) \
    .withColumn("volatilidade", spark_round("volatilidade", 4)) \
    .withColumn("momentum_30d", spark_round("momentum_30d", 2))

print(f"[OK] Indicadores consolidados: {df_gold.count()} acoes")

# COMMAND ----------

# DBTITLE 1,Gravar Tabela Gold de Indicadores
# 6. Gravar tabela Gold de indicadores
print("\n[INFO] Gravando tabela gold.indicadores_completos...")

df_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("investfacil_catalog.gold.indicadores_completos")

print("[OK] Tabela gold.indicadores_completos criada com sucesso!")

# COMMAND ----------

# DBTITLE 1,Criar Tabela de Histórico para Gráficos
# 7. Criar tabela de historico simplificada para graficos
print("\n[INFO] Criando tabela de historico para graficos...")

df_historico_gold = df_historico \
    .withColumn("preco", spark_round("preco", 2)) \
    .orderBy("ticker", "data")

df_historico_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("investfacil_catalog.gold.cotacoes_historico")

print("[OK] Tabela gold.cotacoes_historico criada com sucesso!")

# COMMAND ----------

# DBTITLE 1,Validar Tabela Gold
# MAGIC %sql
# MAGIC -- 8. Validar tabela Gold
# MAGIC SELECT 
# MAGIC     COUNT(*) as total_acoes,
# MAGIC     COUNT(dividend_yield) as acoes_com_dividend_yield,
# MAGIC     COUNT(preco_lucro_pl) as acoes_com_pl,
# MAGIC     COUNT(retorno_patrimonio_roe) as acoes_com_roe,
# MAGIC     COUNT(volatilidade) as acoes_com_volatilidade,
# MAGIC     COUNT(momentum_30d) as acoes_com_momentum
# MAGIC FROM investfacil_catalog.gold.indicadores_completos;

# COMMAND ----------

# DBTITLE 1,Visualizar Top Ações por Dividend Yield
# MAGIC %sql
# MAGIC -- 9. Exemplo: Top 10 ações por Dividend Yield
# MAGIC SELECT 
# MAGIC     ticker,
# MAGIC     nome_curto,
# MAGIC     setor,
# MAGIC     preco_atual,
# MAGIC     dividend_yield * 100 as dividend_yield_percentual,
# MAGIC     preco_lucro_pl,
# MAGIC     volatilidade,
# MAGIC     volume_medio
# MAGIC FROM investfacil_catalog.gold.indicadores_completos
# MAGIC WHERE dividend_yield IS NOT NULL
# MAGIC ORDER BY dividend_yield DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Visualizar Estatísticas Gerais
# MAGIC %sql
# MAGIC -- 10. Estatísticas gerais dos indicadores
# MAGIC SELECT 
# MAGIC     'Dividend Yield' as indicador,
# MAGIC     ROUND(AVG(dividend_yield) * 100, 2) as media_percentual,
# MAGIC     ROUND(MIN(dividend_yield) * 100, 2) as minimo_percentual,
# MAGIC     ROUND(MAX(dividend_yield) * 100, 2) as maximo_percentual
# MAGIC FROM investfacil_catalog.gold.indicadores_completos
# MAGIC WHERE dividend_yield IS NOT NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'P/L' as indicador,
# MAGIC     ROUND(AVG(preco_lucro_pl), 2) as media,
# MAGIC     ROUND(MIN(preco_lucro_pl), 2) as minimo,
# MAGIC     ROUND(MAX(preco_lucro_pl), 2) as maximo
# MAGIC FROM investfacil_catalog.gold.indicadores_completos
# MAGIC WHERE preco_lucro_pl IS NOT NULL AND preco_lucro_pl > 0
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'Volatilidade' as indicador,
# MAGIC     ROUND(AVG(volatilidade) * 100, 2) as media_percentual,
# MAGIC     ROUND(MIN(volatilidade) * 100, 2) as minimo_percentual,
# MAGIC     ROUND(MAX(volatilidade) * 100, 2) as maximo_percentual
# MAGIC FROM investfacil_catalog.gold.indicadores_completos
# MAGIC WHERE volatilidade IS NOT NULL;

# COMMAND ----------

