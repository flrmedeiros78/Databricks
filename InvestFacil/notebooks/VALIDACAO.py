
# ============================================================================
# SCRIPT DE VALIDAÇÃO - InvestFácil Pipeline
# Execute este script após o Job InvestFacil_Pipeline_Diario terminar
# ============================================================================

print("="*70)
print("🔍 VALIDANDO RESULTADOS DO PIPELINE INVESTFÁCIL")
print("="*70 + "\n")

# 1. Verificar Catálogo e Schemas
print("📂 1. UNITY CATALOG - Estrutura\n")
try:
    spark.sql("USE CATALOG investfacil_catalog")
    schemas = spark.sql("SHOW SCHEMAS").collect()
    print(f"   ✅ Catálogo: investfacil_catalog")
    print(f"   ✅ Schemas encontrados: {len(schemas)}")
    for schema in schemas:
        if schema[0] not in ['information_schema', 'default']:
            print(f"      • {schema[0]}")
    print()
except Exception as e:
    print(f"   ❌ Erro ao verificar catálogo: {str(e)}\n")

# 2. Verificar Tabelas Bronze
print("📊 2. CAMADA BRONZE - Dados Brutos\n")
try:
    tables_bronze = spark.sql("SHOW TABLES IN investfacil_catalog.bronze").collect()
    print(f"   ✅ Tabelas encontradas: {len(tables_bronze)}\n")
    
    # raw_quotes
    count_quotes = spark.sql("SELECT COUNT(*) as cnt FROM investfacil_catalog.bronze.raw_quotes").collect()[0][0]
    tickers_quotes = spark.sql("SELECT COUNT(DISTINCT ticker) as cnt FROM investfacil_catalog.bronze.raw_quotes").collect()[0][0]
    print(f"   📋 raw_quotes:")
    print(f"      • Total registros: {count_quotes}")
    print(f"      • Tickers únicos: {tickers_quotes}")
    
    # raw_fundamentals
    count_fund = spark.sql("SELECT COUNT(*) as cnt FROM investfacil_catalog.bronze.raw_fundamentals").collect()[0][0]
    tickers_fund = spark.sql("SELECT COUNT(DISTINCT ticker) as cnt FROM investfacil_catalog.bronze.raw_fundamentals").collect()[0][0]
    print(f"\n   📋 raw_fundamentals:")
    print(f"      • Total registros: {count_fund}")
    print(f"      • Tickers únicos: {tickers_fund}")
    print()
except Exception as e:
    print(f"   ❌ Erro ao verificar Bronze: {str(e)}\n")

# 3. Verificar Tabelas Silver
print("🔄 3. CAMADA SILVER - Dados Limpos\n")
try:
    # cotacoes
    count_cotacoes = spark.sql("SELECT COUNT(*) as cnt FROM investfacil_catalog.silver.cotacoes").collect()[0][0]
    tickers_cotacoes = spark.sql("SELECT COUNT(DISTINCT ticker) as cnt FROM investfacil_catalog.silver.cotacoes").collect()[0][0]
    print(f"   📋 cotacoes:")
    print(f"      • Total registros: {count_cotacoes}")
    print(f"      • Tickers únicos: {tickers_cotacoes}")
    
    # fundamentos
    count_fundamentos = spark.sql("SELECT COUNT(*) as cnt FROM investfacil_catalog.silver.fundamentos").collect()[0][0]
    tickers_fundamentos = spark.sql("SELECT COUNT(DISTINCT ticker) as cnt FROM investfacil_catalog.silver.fundamentos").collect()[0][0]
    print(f"\n   📋 fundamentos:")
    print(f"      • Total registros: {count_fundamentos}")
    print(f"      • Tickers únicos: {tickers_fundamentos}")
    print()
except Exception as e:
    print(f"   ❌ Erro ao verificar Silver: {str(e)}\n")

# 4. Verificar Tabelas Gold
print("⭐ 4. CAMADA GOLD - Indicadores\n")
try:
    # indicadores_completos
    count_indicadores = spark.sql("SELECT COUNT(*) as cnt FROM investfacil_catalog.gold.indicadores_completos").collect()[0][0]
    
    # Estatísticas dos indicadores
    stats = spark.sql('''
        SELECT 
            COUNT(dividend_yield) as com_dividend_yield,
            COUNT(preco_lucro_pl) as com_pl,
            COUNT(retorno_patrimonio_roe) as com_roe,
            COUNT(volatilidade) as com_volatilidade,
            COUNT(momentum_30d) as com_momentum
        FROM investfacil_catalog.gold.indicadores_completos
    ''').collect()[0]
    
    print(f"   📋 indicadores_completos:")
    print(f"      • Total ações: {count_indicadores}")
    print(f"      • Com Dividend Yield: {stats[0]}")
    print(f"      • Com P/L: {stats[1]}")
    print(f"      • Com ROE: {stats[2]}")
    print(f"      • Com Volatilidade: {stats[3]}")
    print(f"      • Com Momentum 30d: {stats[4]}")
    
    # cotacoes_historico
    count_historico = spark.sql("SELECT COUNT(*) as cnt FROM investfacil_catalog.gold.cotacoes_historico").collect()[0][0]
    dias_historico = spark.sql("SELECT COUNT(DISTINCT data) as cnt FROM investfacil_catalog.gold.cotacoes_historico").collect()[0][0]
    print(f"\n   📋 cotacoes_historico:")
    print(f"      • Total registros: {count_historico}")
    print(f"      • Dias de histórico: {dias_historico}")
    print()
except Exception as e:
    print(f"   ❌ Erro ao verificar Gold: {str(e)}\n")

# 5. Verificar JSONs no S3
print("📤 5. EXPORT S3 - Arquivos JSON\n")
try:
    s3_path = "s3://bucket-aws-databricks/investfacil/export/"
    files = dbutils.fs.ls(s3_path)
    
    print(f"   ✅ Pasta S3: {s3_path}")
    print(f"   ✅ Arquivos encontrados: {len(files)}\n")
    
    for file in files:
        size_kb = file.size / 1024
        size_mb = size_kb / 1024
        if size_mb > 1:
            print(f"      📄 {file.name} ({size_mb:.2f} MB)")
        else:
            print(f"      📄 {file.name} ({size_kb:.2f} KB)")
    print()
except Exception as e:
    print(f"   ❌ Erro ao verificar S3: {str(e)}\n")

# 6. Amostra de Dados Gold (Top 5 por Dividend Yield)
print("📊 6. AMOSTRA DE DADOS - Top 5 Dividend Yield\n")
try:
    top5 = spark.sql('''
        SELECT 
            ticker,
            nome_curto,
            preco_atual,
            ROUND(dividend_yield * 100, 2) as dividend_yield_pct,
            preco_lucro_pl,
            ROUND(volatilidade * 100, 2) as volatilidade_pct
        FROM investfacil_catalog.gold.indicadores_completos
        WHERE dividend_yield IS NOT NULL
        ORDER BY dividend_yield DESC
        LIMIT 5
    ''')
    
    print(top5.toPandas().to_string(index=False))
    print()
except Exception as e:
    print(f"   ❌ Erro ao buscar amostra: {str(e)}\n")

print("="*70)
print("✅ VALIDAÇÃO CONCLUÍDA!")
print("="*70)
print('''
Se todos os itens acima mostraram ✅, o pipeline está funcionando perfeitamente!

📋 Próximos passos:
1. Configurar acesso público ao S3 para o Netlify
2. Construir o front-end que consome os JSONs
3. O Job vai rodar automaticamente todo dia às 19h
''')
print("="*70)
