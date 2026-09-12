# Arquitetura do InvestFácil

## Fluxo de Dados

1. **Ingestão (Bronze)**
   - API: brapi.dev (gratuita)
   - Retry com backoff exponencial
   - Rate limit: 1 req/segundo
   - Auditoria completa: ticker, timestamp, source, raw_payload

2. **Transformação (Silver)**
   - Parse JSON → Schema estruturado
   - Validações de qualidade (preços > 0, datas válidas)
   - Normalização de tipos
   - Deduplicação

3. **Indicadores (Gold)**
   - Dividend Yield
   - P/L (Preço/Lucro)
   - ROE (Retorno sobre Patrimônio)
   - Volatilidade (desvio padrão retornos)
   - Momentum 30 dias
   - Volume médio (liquidez)

4. **Export (S3)**
   - JSON compacto
   - Metadata com timestamp
   - Acesso público via CloudFront

## Decisões Técnicas

### Por que Serverless?
- Zero custo em idle
- Auto-scaling
- Sem gerenciamento de cluster

### Por que JSON no S3?
- Netlify é estático (não roda Spark)
- Zero custo de SQL Warehouse
- Cache via CloudFront

### Por que Arquitetura Medalhão?
- Bronze: auditoria e replay
- Silver: reuso em outros projetos
- Gold: específico para InvestFácil
