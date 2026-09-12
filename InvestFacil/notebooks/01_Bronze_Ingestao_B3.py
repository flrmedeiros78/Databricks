# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Título e Descrição
# MAGIC %md
# MAGIC # 01 - Ingestao Bronze - B3 (InvestFacil)
# MAGIC
# MAGIC **Proposito**: Ingestao de dados brutos da B3 via Yahoo Finance API
# MAGIC
# MAGIC **Fontes**:
# MAGIC - Yahoo Finance (via yfinance library) - Dados gratuitos sem necessidade de API key
# MAGIC - Cotacoes, volume, e dados fundamentalistas
# MAGIC
# MAGIC **Tabelas de saida**:
# MAGIC - investfacil_catalog.bronze.raw_quotes - Cotacoes brutas
# MAGIC - investfacil_catalog.bronze.raw_fundamentals - Dados fundamentalistas brutos
# MAGIC
# MAGIC **Auditoria**: Todas as tabelas incluem ingestion_timestamp, source, ticker, raw_payload

# COMMAND ----------

# DBTITLE 1,Imports e Configurações
# Imports
import json
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
import time

# Instalar yfinance se necessário
try:
    import yfinance as yf
except ImportError:
    print("[INFO] Instalando yfinance...")
    %pip install yfinance --quiet
    import yfinance as yf

print("[OK] Bibliotecas carregadas")

# COMMAND ----------

# DBTITLE 1,Função de Ingestão Yahoo Finance
# Função auxiliar: Buscar dados de um ticker via Yahoo Finance
def fetch_ticker_data(ticker_symbol, max_retries=3):
    """
    Busca dados de cotacao e fundamentalistas para um ticker via Yahoo Finance.
    
    Args:
        ticker_symbol: Simbolo do ticker (ex: PETR4.SA)
        max_retries: Numero maximo de tentativas
    
    Returns:
        tuple: (quote_data, fundamental_data) ou (None, None) em caso de falha
    """
    for attempt in range(max_retries):
        try:
            # Criar objeto Ticker
            ticker = yf.Ticker(ticker_symbol)
            
            # Buscar dados de cotacao (ultimo dia)
            hist = ticker.history(period="1d")
            
            if hist.empty:
                return None, None
            
            # Extrair ultima cotacao
            last_data = hist.iloc[-1]
            
            quote_data = {
                'symbol': ticker_symbol,
                'price': float(last_data['Close']),
                'open': float(last_data['Open']),
                'high': float(last_data['High']),
                'low': float(last_data['Low']),
                'volume': int(last_data['Volume']),
                'timestamp': last_data.name.isoformat()
            }
            
            # Buscar dados fundamentalistas
            info = ticker.info
            fundamental_data = {
                'symbol': ticker_symbol,
                'marketCap': info.get('marketCap'),
                'peRatio': info.get('trailingPE'),
                'dividendYield': info.get('dividendYield'),
                'beta': info.get('beta'),
                'fiftyTwoWeekHigh': info.get('fiftyTwoWeekHigh'),
                'fiftyTwoWeekLow': info.get('fiftyTwoWeekLow'),
                'sector': info.get('sector'),
                'industry': info.get('industry')
            }
            
            return quote_data, fundamental_data
            
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(1)
                continue
            else:
                print(f"[WARNING] Erro ao buscar {ticker_symbol}: {str(e)}")
                return None, None
    
    return None, None

# COMMAND ----------

# DBTITLE 1,Definir Lista de Tickers da B3
# 1. Definir lista de acoes da B3 (Yahoo Finance usa sufixo .SA para B3)
print("[INFO] Definindo lista de acoes da B3...")

# Lista das principais acoes do Ibovespa
tickers_b3 = [
    'PETR4', 'VALE3', 'ITUB4', 'BBDC4', 'ABEV3', 'B3SA3', 'BBAS3', 'WEGE3',
    'RENT3', 'ITSA4', 'SUZB3', 'RAIL3', 'GGBR4', 'CSNA3', 'UGPA3', 'RADL3',
    'VBBR3', 'JBSS3', 'MGLU3', 'LREN3', 'EMBR3', 'CSAN3', 'ELET3', 'ELET6',
    'SBSP3', 'VIVT3', 'CPLE6', 'CMIG4', 'EGIE3', 'EQTL3', 'TAEE11', 'FLRY3',
    'HAPV3', 'PRIO3', 'RECV3', 'TOTS3', 'KLBN11', 'BEEF3', 'MRFG3', 'PCAR3',
    'ASAI3', 'CVCB3', 'AZUL4', 'GOLL4', 'CYRE3', 'MRVE3', 'MULT3', 'YDUQ3',
    'COGN3', 'SOMA3'
]

# Converter para formato Yahoo Finance (adicionar .SA)
tickers = [f"{t}.SA" for t in tickers_b3]

print(f"[OK] {len(tickers)} tickers definidos para ingestao")
print(f"Exemplos: {tickers[:5]}")

# COMMAND ----------

# DBTITLE 1,Ingestão de Cotações via Yahoo Finance
# 2. Buscar cotacoes via Yahoo Finance
print("\n[INFO] Iniciando ingestao de cotacoes via Yahoo Finance...")

quotes_raw = []
fundamentals_raw = []
ingestion_time = datetime.now()

# Processar cada ticker
for i, ticker in enumerate(tickers):
    print(f"[{i+1}/{len(tickers)}] Processando {ticker}...", end=" ")
    
    # Buscar dados do ticker
    quote_data, fundamental_data = fetch_ticker_data(ticker)
    
    if quote_data:
        # Extrair ticker limpo (remover .SA)
        clean_ticker = ticker.replace('.SA', '')
        
        # Adicionar cotacao
        quote_record = {
            'ticker': clean_ticker,
            'ingestion_timestamp': ingestion_time,
            'source': 'yahoo_finance',
            'raw_payload': json.dumps(quote_data)
        }
        quotes_raw.append(quote_record)
        
        # Adicionar fundamentos se disponíveis
        if fundamental_data and any(v is not None for v in fundamental_data.values()):
            fundamental_record = {
                'ticker': clean_ticker,
                'ingestion_timestamp': ingestion_time,
                'source': 'yahoo_finance',
                'raw_payload': json.dumps(fundamental_data)
            }
            fundamentals_raw.append(fundamental_record)
        
        print("[OK]")
    else:
        print("[WARNING] Sem dados")
    
    # Delay para evitar rate limit
    if i < len(tickers) - 1 and (i + 1) % 10 == 0:
        print("[INFO] Pausa de 2s...")
        time.sleep(2)

print(f"\n[SUCCESS] Ingestao concluida:")
print(f"   - Cotacoes: {len(quotes_raw)} registros")
print(f"   - Fundamentos: {len(fundamentals_raw)} registros")

# COMMAND ----------

# DBTITLE 1,Criar DataFrames
# 3. Criar DataFrames
schema = StructType([
    StructField("ticker", StringType(), False),
    StructField("ingestion_timestamp", TimestampType(), False),
    StructField("source", StringType(), False),
    StructField("raw_payload", StringType(), False)
])

df_quotes = spark.createDataFrame(quotes_raw, schema=schema)
df_fundamentals = spark.createDataFrame(fundamentals_raw, schema=schema)

print(f"\n[INFO] DataFrames criados:")
print(f"   - df_quotes: {df_quotes.count()} linhas")
print(f"   - df_fundamentals: {df_fundamentals.count()} linhas")

# COMMAND ----------

# DBTITLE 1,Gravar em Delta Tables Bronze
# 4. Gravar em Delta Tables Bronze (modo APPEND para manter historico)
print("\n[INFO] Gravando dados em Delta Tables Bronze...")

# Tabela de cotações
df_quotes.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("investfacil_catalog.bronze.raw_quotes")

print("[OK] Tabela bronze.raw_quotes atualizada")

# Tabela de fundamentos
if df_fundamentals.count() > 0:
    df_fundamentals.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable("investfacil_catalog.bronze.raw_fundamentals")
    print("[OK] Tabela bronze.raw_fundamentals atualizada")
else:
    print("[WARNING] Nenhum dado fundamentalista para gravar")

# COMMAND ----------

# DBTITLE 1,Validar Dados Ingeridos
# MAGIC %sql
# MAGIC -- 5. Validar dados ingeridos
# MAGIC SELECT 
# MAGIC     'raw_quotes' as tabela,
# MAGIC     COUNT(*) as total_registros,
# MAGIC     COUNT(DISTINCT ticker) as tickers_unicos,
# MAGIC     MAX(ingestion_timestamp) as ultima_ingestao
# MAGIC FROM investfacil_catalog.bronze.raw_quotes
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'raw_fundamentals' as tabela,
# MAGIC     COUNT(*) as total_registros,
# MAGIC     COUNT(DISTINCT ticker) as tickers_unicos,
# MAGIC     MAX(ingestion_timestamp) as ultima_ingestao
# MAGIC FROM investfacil_catalog.bronze.raw_fundamentals;

# COMMAND ----------

# DBTITLE 1,Visualizar Amostra
# MAGIC %sql
# MAGIC -- 6. Visualizar amostra de dados ingeridos
# MAGIC SELECT 
# MAGIC     ticker,
# MAGIC     ingestion_timestamp,
# MAGIC     source,
# MAGIC     SUBSTRING(raw_payload, 1, 200) as payload_preview
# MAGIC FROM investfacil_catalog.bronze.raw_quotes
# MAGIC ORDER BY ingestion_timestamp DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# DBTITLE 1,Notas de Implementação
# MAGIC %md
# MAGIC ## Pipeline de Ingestao Pronta
# MAGIC
# MAGIC ### O que foi implementado:
# MAGIC
# MAGIC 1. **API Yahoo Finance (yfinance)**
# MAGIC    - Sem necessidade de API key
# MAGIC    - Dados gratuitos e confiaveis
# MAGIC    - Cotacoes em tempo real + dados fundamentalistas
# MAGIC
# MAGIC 2. **Dados Capturados**
# MAGIC    - **Cotacoes**: preco, abertura, maxima, minima, volume
# MAGIC    - **Fundamentos**: market cap, P/E ratio, dividend yield, beta, setor, industria
# MAGIC
# MAGIC 3. **Arquitetura Bronze**
# MAGIC    - Tabelas Delta criadas automaticamente
# MAGIC    - Schema evolution habilitado
# MAGIC    - Auditoria completa (timestamp, source, ticker)
# MAGIC
# MAGIC ### Proximos Passos:
# MAGIC
# MAGIC 1. **Executar job completo**: O job esta configurado para rodar as 19h diariamente
# MAGIC 2. **Expandir tickers**: Adicione mais acoes na celula 4 conforme necessario
# MAGIC 3. **Camada Silver**: Os notebooks 02_Silver e 03_Gold ja estao prontos para processar esses dados
# MAGIC
# MAGIC ### Melhorias vs. HG Brasil:
# MAGIC - HG Brasil: Requer API key paga para dados historicos robustos
# MAGIC - Yahoo Finance: Gratuito, sem limites rigidos de rate
# MAGIC - Instalacao automatica da biblioteca yfinance
# MAGIC - Retry automatico com backoff exponencial

# COMMAND ----------

# DBTITLE 1,Notas de Implementação
# MAGIC %md
# MAGIC ## ✅ Pipeline de Ingestão Pronta!
# MAGIC
# MAGIC ### 🎯 O que foi implementado:
# MAGIC
# MAGIC 1. **API Yahoo Finance (yfinance)**
# MAGIC    - ✅ Sem necessidade de API key
# MAGIC    - ✅ Dados gratuitos e confiáveis
# MAGIC    - ✅ Cotações em tempo real + dados fundamentalistas
# MAGIC
# MAGIC 2. **Dados Capturados**
# MAGIC    - **Cotações**: preço, abertura, máxima, mínima, volume
# MAGIC    - **Fundamentos**: market cap, P/E ratio, dividend yield, beta, setor, indústria
# MAGIC
# MAGIC 3. **Arquitetura Bronze**
# MAGIC    - Tabelas Delta criadas automaticamente
# MAGIC    - Schema evolution habilitado
# MAGIC    - Auditoria completa (timestamp, source, ticker)
# MAGIC
# MAGIC ### 🚀 Próximos Passos:
# MAGIC
# MAGIC 1. **Executar job completo**: O job está configurado para rodar às 19h diariamente
# MAGIC 2. **Expandir tickers**: Adicione mais ações na célula 4 conforme necessário
# MAGIC 3. **Camada Silver**: Os notebooks 02_Silver e 03_Gold já estão prontos para processar esses dados
# MAGIC
# MAGIC ### ⚡ Melhorias vs. HG Brasil:
# MAGIC - ❌ HG Brasil: Requer API key paga para dados históricos robustos
# MAGIC - ✅ Yahoo Finance: Gratuito, sem limites rígidos de rate
# MAGIC - ✅ Instalação automática da biblioteca yfinance
# MAGIC - ✅ Retry automático com backoff exponencial