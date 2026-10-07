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

# DBTITLE 1,Mapeamento de Nomes e Setores
# Mapeamento manual de nomes das empresas e setores em português
# Fonte: B3, Yahoo Finance e sites especializados

MAPEAMENTO_NOMES = {
    # Petróleo e Gás
    'PETR3': ('Petrobras ON', 'Petróleo Brasileiro S.A. - Petrobras'),
    'PETR4': ('Petrobras PN', 'Petróleo Brasileiro S.A. - Petrobras'),
    'PRIO3': ('PetroRio ON', 'PetroRio S.A.'),
    'RRRP3': ('3R Petroleum ON', '3R Petroleum Óleo e Gás S.A.'),
    
    # Mineração
    'VALE3': ('Vale ON', 'Vale S.A.'),
    
    # Bancos
    'ITUB4': ('Itaú Unibanco PN', 'Itaú Unibanco Holding S.A.'),
    'BBDC4': ('Bradesco PN', 'Banco Bradesco S.A.'),
    'BBDC3': ('Bradesco ON', 'Banco Bradesco S.A.'),
    'BBAS3': ('Banco do Brasil ON', 'Banco do Brasil S.A.'),
    'SANB11': ('Santander Units', 'Banco Santander Brasil S.A.'),
    'BPAC11': ('BTG Pactual Units', 'Banco BTG Pactual S.A.'),
    
    # Varejo
    'LREN3': ('Lojas Renner ON', 'Lojas Renner S.A.'),
    'MGLU3': ('Magazine Luiza ON', 'Magazine Luiza S.A.'),
    'AMER3': ('Americanas ON', 'Americanas S.A.'),
    'PCAR3': ('GPA ON', 'Companhia Brasileira de Distribuição'),
    'CRFB3': ('Carrefour Brasil ON', 'Carrefour Brasil S.A.'),
    'VIIA3': ('Via ON', 'Via S.A.'),
    'BHIA3': ('Casas Bahia ON', 'Casas Bahia S.A.'),
    'ASAI3': ('Assaí ON', 'Assaí Atacadista S.A.'),
    
    # Alimentos e Bebidas
    'ABEV3': ('Ambev ON', 'Ambev S.A.'),
    'JBSS3': ('JBS ON', 'JBS S.A.'),
    'BRFS3': ('BRF ON', 'BRF S.A.'),
    'BEEF3': ('Minerva ON', 'Minerva S.A.'),
    'MRFG3': ('Marfrig ON', 'Marfrig Global Foods S.A.'),
    
    # Energia Elétrica
    'ELET3': ('Eletrobras ON', 'Centrais Elétricas Brasileiras S.A.'),
    'ELET6': ('Eletrobras PNB', 'Centrais Elétricas Brasileiras S.A.'),
    'EGIE3': ('Engie Brasil ON', 'Engie Brasil Energia S.A.'),
    'TAEE11': ('Taesa Units', 'Transmissora Aliança de Energia Elétrica S.A.'),
    'CPFE3': ('CPFL Energia ON', 'CPFL Energia S.A.'),
    'CMIG4': ('Cemig PN', 'Companhia Energética de Minas Gerais'),
    'NEOE3': ('Neoenergia ON', 'Neoenergia S.A.'),
    
    # Telecomunicações
    'VIVT3': ('Telefônica Brasil ON', 'Telefônica Brasil S.A.'),
    'TIMS3': ('Tim ON', 'TIM S.A.'),
    
    # Siderurgia
    'GGBR4': ('Gerdau PN', 'Gerdau S.A.'),
    'GOAU4': ('Gerdau Metalúrgica PN', 'Metalúrgica Gerdau S.A.'),
    'USIM5': ('Usiminas PNA', 'Usinas Siderúrgicas de Minas Gerais S.A.'),
    'CSNA3': ('CSN ON', 'Companhia Siderúrgica Nacional'),
    
    # Locação de Veículos
    'RENT3': ('Localiza ON', 'Localiza Rent a Car S.A.'),
    'MOVI3': ('Movida ON', 'Movida Participações S.A.'),
    
    # Construção Civil
    'CYRE3': ('Cyrela ON', 'Cyrela Brazil Realty S.A.'),
    'MRVE3': ('MRV ON', 'MRV Engenharia e Participações S.A.'),
    'EZTC3': ('EZ Tec ON', 'EZ Tec Empreendimentos e Participações S.A.'),
    
    # Saúde
    'RADL3': ('Raia Drogasil ON', 'RD - Raia Drogasil S.A.'),
    'HAPV3': ('Hapvida ON', 'Hapvida Participações e Investimentos S.A.'),
    'FLRY3': ('Fleury ON', 'Fleury S.A.'),
    'PNVL3': ('Dasa ON', 'Diagnósticos da América S.A.'),
    
    # Utilities / Saneamento
    'SBSP3': ('Sabesp ON', 'Companhia de Saneamento Básico do Estado de São Paulo'),
    'CSAN3': ('Cosan ON', 'Cosan S.A.'),
    
    # Papel e Celulose
    'SUZB3': ('Suzano ON', 'Suzano S.A.'),
    'KLBN11': ('Klabin Units', 'Klabin S.A.'),
    
    # Logística
    'RAIL3': ('Rumo ON', 'Rumo S.A.'),
    
    # Seguros
    'BBSE3': ('BB Seguridade ON', 'BB Seguridade Participações S.A.'),
    'PSSA3': ('Porto Seguro ON', 'Porto Seguro S.A.'),
    
    # Educação
    'YDUQ3': ('Yduqs ON', 'Yduqs Participações S.A.'),
    'COGN3': ('Cogna ON', 'Cogna Educação S.A.'),
    
    # Aéreas
    'GOLL4': ('Gol PN', 'Gol Linhas Aéreas Inteligentes S.A.'),
    'AZUL4': ('Azul PN', 'Azul S.A.'),
    
    # Tecnologia
    'TOTS3': ('Totvs ON', 'TOTVS S.A.'),
    'LWSA3': ('Locaweb ON', 'Locaweb Serviços de Internet S.A.'),
    
    # Shoppings
    'MULT3': ('Multiplan ON', 'Multiplan Empreendimentos Imobiliários S.A.'),
    'IGTI11': ('Iguatemi Units', 'Iguatemi Empresa de Shopping Centers S.A.'),
    
    # Frigoríficos
    'SMTO3': ('São Martinho ON', 'São Martinho S.A.'),
    
    # Química
    'UGPA3': ('Ultrapar ON', 'Ultrapar Participações S.A.'),
    'BRKM5': ('Braskem PNA', 'Braskem S.A.'),
    
    # Outros
    'WEGE3': ('Weg ON', 'WEG S.A.'),
    'EMBR3': ('Embraer ON', 'Embraer S.A.'),
    'NTCO3': ('Natura ON', 'Natura &Co Holding S.A.'),
    'RECV3': ('PetroReconcavo ON', 'PetroRecôncavo S.A.'),
    'IRBR3': ('IRB Brasil ON', 'IRB Brasil Resseguros S.A.'),
    'CCRO3': ('CCR ON', 'CCR S.A.'),
    'CIEL3': ('Cielo ON', 'Cielo S.A.'),
    'B3SA3': ('B3 ON', 'B3 S.A. - Brasil, Bolsa, Balcão'),
    'TIMP3': ('Tim Participações ON', 'Tim Participações S.A.'),
    'ECOR3': ('Ecorodovias ON', 'Ecorodovias Infraestrutura e Logística S.A.'),
    'QUAL3': ('Qualicorp ON', 'Qualicorp Consultoria e Corretora de Seguros S.A.'),
}

# Mapeamento de setores em inglês para português
MAPEAMENTO_SETORES = {
    'Energy': 'Petróleo e Gás',
    'Basic Materials': 'Materiais Básicos',
    'Financial Services': 'Financeiro',
    'Financial': 'Financeiro',
    'Industrials': 'Industrial',
    'Consumer Cyclical': 'Consumo Cíclico',
    'Consumer Defensive': 'Consumo Não-Cíclico',
    'Utilities': 'Utilidades Públicas',
    'Communication Services': 'Telecomunicações',
    'Healthcare': 'Saúde',
    'Real Estate': 'Imobiliário',
    'Technology': 'Tecnologia',
}

print(f"✅ Mapeamento carregado: {len(MAPEAMENTO_NOMES)} empresas")
print(f"✅ Tradução de setores: {len(MAPEAMENTO_SETORES)} setores")

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

df_cotacoes_temp = df_raw_quotes \
    .withColumn("parsed", from_json(col("raw_payload"), quote_schema)) \
    .select(
        col("ticker"),
        col("parsed.symbol").alias("symbol"),
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

print(f"[OK] {df_cotacoes_temp.count()} cotacoes transformadas (aguardando join com nomes)")

# COMMAND ----------

# DBTITLE 1,Schema para Parse de Fundamentos
# 4. Definir schema para parse de dados fundamentalistas (Yahoo Finance)
fundamentals_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("shortName", StringType(), True),
    StructField("longName", StringType(), True),
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
# 5. Parse e transformacao de fundamentos (Yahoo Finance) com mapeamento
from pyspark.sql.functions import udf, from_json, col, coalesce, lit
from pyspark.sql.types import ArrayType, StringType, DoubleType

print("\n[INFO] Transformando fundamentos...")

# UDF para aplicar mapeamento de nomes
def aplicar_mapeamento_nome(ticker):
    if ticker in MAPEAMENTO_NOMES:
        return list(MAPEAMENTO_NOMES[ticker])  # Retorna [nome_curto, nome_completo]
    return [None, None]

mapear_nome_udf = udf(aplicar_mapeamento_nome, ArrayType(StringType()))

# UDF para traduzir setores
def traduzir_setor(setor_ingles):
    if setor_ingles and setor_ingles in MAPEAMENTO_SETORES:
        return MAPEAMENTO_SETORES[setor_ingles]
    return setor_ingles

traduzir_setor_udf = udf(traduzir_setor, StringType())

df_fundamentos_silver = df_raw_fundamentals \
    .withColumn("parsed", from_json(col("raw_payload"), fundamentals_schema)) \
    .withColumn("nomes_mapeados", mapear_nome_udf(col("ticker"))) \
    .select(
        col("ticker"),
        col("parsed.symbol").alias("symbol"),
        # Usar mapeamento se disponivel, senão usar do Yahoo, senão ticker
        coalesce(
            col("nomes_mapeados")[0],
            col("parsed.shortName"),
            col("ticker")
        ).alias("nome_curto"),
        coalesce(
            col("nomes_mapeados")[1],
            col("parsed.longName"),
            col("ticker")
        ).alias("nome_completo"),
        # Traduzir setor para português
        traduzir_setor_udf(col("parsed.sector")).alias("setor"),
        col("parsed.industry").alias("industria"),
        lit(None).cast(StringType()).alias("website"),
        lit(None).cast(StringType()).alias("descricao_negocio"),
        col("parsed.peRatio").alias("preco_lucro_pl"),
        lit(None).cast(DoubleType()).alias("retorno_patrimonio_roe"),
        lit(None).cast(DoubleType()).alias("lucro_por_acao_lpa"),
        col("parsed.dividendYield").alias("dividend_yield"),
        lit(None).cast(DoubleType()).alias("dividendo_valor"),
        lit(None).cast(DoubleType()).alias("payout_ratio"),
        col("parsed.beta").alias("beta"),
        col("parsed.marketCap").alias("valor_mercado"),
        col("parsed.fiftyTwoWeekHigh").alias("maxima_52_semanas"),
        col("parsed.fiftyTwoWeekLow").alias("minima_52_semanas"),
        col("ingestion_timestamp").alias("data_ingestao")
    ) \
    .dropDuplicates(["ticker", "data_ingestao"])

print(f"✅ {df_fundamentos_silver.count()} fundamentos transformados com nomes e setores em português")

# COMMAND ----------

# DBTITLE 1,Enriquecer Cotações com Nomes das Empresas
# 5.1. Enriquecer cotacoes com nomes das empresas dos fundamentos
print("\n[INFO] Enriquecendo cotacoes com nomes das empresas...")

# Fazer join das cotacoes com fundamentos apenas para pegar os nomes
df_nomes = df_fundamentos_silver.select(
    col("ticker"),
    col("nome_curto"),
    col("nome_completo")
)

# Join e criar df_cotacoes_silver final
df_cotacoes_silver = df_cotacoes_temp \
    .join(df_nomes, on="ticker", how="left") \
    .withColumn("nome_curto", coalesce(col("nome_curto"), col("ticker"))) \
    .withColumn("nome_completo", coalesce(col("nome_completo"), col("ticker")))

print(f"[OK] {df_cotacoes_silver.count()} cotacoes enriquecidas com nomes")

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

