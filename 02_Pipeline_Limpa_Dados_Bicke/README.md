Pipeline de Dados - Bike Share Chicago


Neste Pipeline de dados ETL com Databricks Lakeflow (Spark Declarative Pipelines).
Foi implementando a arquitetura Medallion (Bronze → Silver → Gold) para processar dados de compartilhamento de bicicletas.

Arquitetura Medallion

Bronze Layer - Ingestão
Tabela: tb_bronze_bike_eventos
Tipo: STREAMING TABLE
Função: Ingestão de dados brutos do CSV


Características:

- Usa Auto Loader (read_files) para ingestão incremental
- Leitura dos dados diretamente do UC Volume
- Streaming contínuo de novos arquivos

Silver Layer - Transformação
Tabela: tb_silver_bike_eventos
Tipo: STREAMING TABLE
Função: Limpeza e transformação dos dados


Transformações:

- Calcula receita baseada no tipo de usuário (member vs casual)
- Extrai data da viagem
- Filtra viagens inválidas (duração <= 0)
- Padroniza tipos de dados

Gold Layer - Agregação
Tabela: tb_gold_bike_eventos
Tipo: MATERIALIZED VIEW
Função: Agregações para análise e relatórios


Métricas:

- Contagem de viagens por dia/bicicleta
- Receita total por dia/bicicleta
- Primeira e última estação do dia

Importante: Usa MATERIALIZED VIEW (não Streaming Table) porque contém agregações com COUNT(DISTINCT)

Tecnologias
- Databricks Lakeflow (Spark Declarative Pipelines)
- Delta Lake (Lakehouse Storage)
- Unity Catalog (Governança)
- Auto Loader (Ingestão Incremental)
- SQL (Linguagem)
- Configuração do Pipeline

Estrutura de Arquivos

- [Como Executar]
1 - Abra o Pipeline no Databricks Lakeflow
2 - Clique em "Start" para executar todas as camadas
3 - Monitore o progresso na aba de monitoramento
4 - Consulte os dados após a conclusão:


 Pontos Importantes e Boas Práticas Implementadas
 
 - Arquitetura Medallion: Separação clara de responsabilidades
 - Streaming Tables para Bronze/Silver: Processamento incremental eficiente
 - Materialized View para Gold: Agregações com COUNT(DISTINCT) requerem batch mode
 - Auto Loader: Ingestão automática e incremental de novos arquivos
 - Qualidade de Dados: Filtros aplicados na camada Silver

Erros Comuns Evitados
 - Não usar STREAMING TABLE para agregações com COUNT(DISTINCT)
 - Não usar comandos DDL em arquivos de pipeline
 - Não mudar tipo de dataset sem dropar a tabela antes

 Resultados
 - 188 registros agregados na camada Gold
 - Pipeline executando sem erros
 - Processamento incremental funcionando
 - Dados disponíveis para análise via SQL ou dashboards



Databricks Spark Declarative Pipelines
Architecture Medallion
Delta Lake
-- Pipeline funcionando com sucesso!

-- Desenvolvido como parte do aprendizado de Databricks Lakeflow e Arquitetura Medallion.
-- Fabio Medeiros:
fabiolrm78@gmail.com
Tel: 21996637177

