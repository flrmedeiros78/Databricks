# Databricks
Treinamento de databricks oficial 

-- COMANDOS USADO NO DATABRICKS

--- Find Your Data and Create a Delta Table

- NAVEGANDO POR (CATALOG-ESCHEMA-VOLUMES)
- Previewed raw csv data using read_file
- Create Delta Tables Using CREATE TABLE AS SELECT
- CREATE TABLE IF NOT EXISTS AS SELECT
--==========================================================================================
-- 01-Notebook - AULA-01
--==========================================================================================
-- O que você fará:
- CorraSELECT current_catalog(), current_schema(); para confirmar seu ambiente
- UseLIST para ver os arquivos CSV nomyfiles volume
- Visualizar dados brutos com read_files
- Crie aemployees tabela Delta com CREATE TABLE AS SELECT
- Verifique a tabela no SQL e no Catalog Explorer

--LINKS
-- Unity Catalog
https://docs.databricks.com/aws/en/data-governance/unity-catalog
-- Unity Catalog volumes
https://docs.databricks.com/aws/en/volumes/
-- CREATE TABLE [USING]
https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-table-using
-- read_files table-valued function
https://docs.databricks.com/aws/en/sql/language-manual/functions/read_files


USE CATALOG dbacademy;
USE SCHEMA get_started_de;

-- lista o diretório dos arquivos
LIST '/Volumes/dbacademy/get_started_de/myfiles'
SELECT * from read_files -- para visualizar os dados do arquivos

CREATE TABLE IF NOT EXISTS tabela delta
AS SELECT FROM read_files 'caminho do arquivo.txt . csv etc..'
--==========================================================================================
-- 02-Notebook - AULA-02
--==========================================================================================
-- COMANDOS EXECUTADOS 
O que você aprendeu
Navegue pela hierarquia do Catálogo Unity (catálogo → esquema → volume)INSERT INTO
Visualizar dados CSV brutos usando UPDATE ... SET ... WHERE
Crie uma tabela Delta usando DELETE FROM ... WHERE
Verifique tabelas no SQL e no Catalog Explorer



--==========================================================================================
-- 03-Notebook - AULA-03
--==========================================================================================
O que você fará:
CorraDESCRIBE HISTORY para ver todas as versões e suas operações
Consulte a tabela original com VERSION AS OF 0
Consulte o estado após o INSERT com VERSION AS OF 1
Compare contagens de linhas entre versões com UNION ALL
Use a@v0 sintaxe abreviada

-- Compare version side by side
SELECT 'Current' as version, count(1) as row_count FROM delta_employes_01
UNION ALL
SELECT 'Version 0' as version, count(1) as row_count FROM delta_employes_01 VERSION AS OF 0;

-- SELECT DA VERSÃO
SELECT * FROM delta_employes_01@V7;
--========================================================================================
-- 03-Notebook - AULA-03
--========================================================================================



--========================================================================================
-- 04-Notebook - AULA-04 
--========================================================================================
Técnicas de ingestão

Crie tabelas usando CTAS e Upload UI

Aprendizados:

- Execute um CTAS comread_files opções de formato explícitas (cabeçalho, inferSchema)
- Selecione apenas as quatro colunas de dados para um esquema limpo
- Use a interface de usuário de upload do Catalog Explorer para criar uma tabela arrastando e soltando
- ExecuteSHOW TABLES para confirmar que ambas as tabelas existem

Conclusão
O que você aprendeu
Crie tabelas comCTAS opções de formato explícitas para esquemas limpos
Use a interface de upload do Catalog Explorer para ingestão sem código
CTAS é para pipelines e reprodutibilidade; a UI de upload é para importações rápidas e ad hoc
SHOW TABLESlista todas as tabelas no esquema atual

Links de recursos:
CREATE TABLE AS SELECT
https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-table

Upload data to a table
https://docs.databricks.com/aws/en/volumes/volume-files

read_files function
https://docs.databricks.com/aws/en/sql/language-manual/functions/read_files

--========================================================================================
-- 04-Notebook - AULA-05 
--========================================================================================
-- Aprendizados:

- Crie uma tabela vazia com definições de colunas explícitas
- ExecuteCOPY INTO para carregar todos os arquivos CSV do volume (6 linhas)
- Execute novamente o mesmoCOPY INTO — observe 0 linhas afetadas
- DESCRIBE HISTORYVerifique se ocorreu apenas uma operação de carga
- UNION das tabela delta_employes_01 com delta_employes_02 na tabela -> employes_copyinto

https://docs.databricks.com/aws/en/sql/language-manual/delta-copy-into

--========================================================================================
-- Notebook_Employes_Bronze - AULA-06 - Medallion Architecture 
--========================================================================================
-- Tarefas:
- Liste os arquivos CSV no volume
- Criecurrent_employees_bronze e carregue dados com COPY INTO
- Crie e audite carimbos de data/horacurrent_employees_silverUPPER(Role)
- Crie uma visualização temporária comGROUP BY agregação
- Crietotal_roles_gold e preencha com INSERT OVERWRITE
- Explore linhagem, permissões e insights no Catalog Explorer


- Criando uma tabela Bronze e carregando dados com COPY INTO
- Transformando dados em Silver com carUPPER()imbos de data/hora e ROW_NUMBER()
- Criando uma tabela de agregação Gold com INSERT OVERWRITE
- Explorando a linhagem no Catalog Explorer


- Conclusão
-- Aprendizados:

- A Arquitetura Medallion organiza os dados em camadas Bronze (bruta), Prata (limpa) e Ouro (agregada)
- Bronze preserva dados brutos como uma rede de segurança; Prata os padroniza e enriquece; Ouro responde a perguntas comerciais específicas
- INSERT OVERWRITEpermite que você atualize as tabelas Gold de acordo com uma programação
- O Unity Catalog rastreia automaticamente a linhagem, as permissões e a atividade de consulta em todas as camadas



Arquitetura Medallion
https://docs.databricks.com/aws/en/lakehouse/medallion

COPIAR PARA
https://docs.databricks.com/aws/en/sql/language-manual/delta-copy-into

Linhagem do Catálogo Unity
https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-lineage

Gerenciar privilégios no Catálogo Unity
https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/

--========================================================================================
-- Notebook_Employes_Silver_Gold - AULA-07 - Medallion Architecture
--========================================================================================

Crie e execute um trabalho LakeFlow na UI
O notebook de demonstração fornece o contexto, mas esta lição é feita principalmente na UI Jobs & Pipelines. Você criará um novo trabalho, 
adicionará dois cadernos de tarefas com uma dependência entre eles e executará o trabalho para observar a execução automática do pipeline.

-- Tarefas:
- Abra empregos e pipelines e crie um novo emprego
- Adicionar Tarefa 1 (Setup-Bronze) apontando para o caderno de tarefas Bronze
- Adicione a Tarefa 2 (Prata-Ouro) com uma dependência da Tarefa 1
- Explore opções de agendamento e acionamento
- Clique em Executar agora e observe ambas as tarefas serem executadas em ordem
- Revise a saída de execução concluída

Conclusão
-- Aprendizados:

LakeFlow Jobs executa um ou mais notebooks como tarefas com dependências configuráveis
As dependências de tarefas garantem que as etapas downstream só sejam executadas quando as etapas upstream forem bem-sucedidas
Os trabalhos podem ser acionados em um cronograma, pela chegada do arquivo ou manualmente
A automação produz os mesmos resultados de pipeline sem intervenção manual


--Links de recursos:
Crie e gerencie trabalhos do LakeFlow
https://docs.databricks.com/aws/en/jobs/

Gatilhos e agendamento de tarefas
https://docs.databricks.com/aws/en/jobs/triggers

--================================================
Finalizado Treinamento Official Databricks, onde pude ter o aproveitamento de 100% das informações, pois a minha experiência com Ferramentas de ETL, me ajudaram muito.

----------
--MKTEC
Desde 2012 trabalho com ETL Linguagens de programação e Ferramentas nas quais trabalhei até hoje:
FoxPro -- programação estrtuturada
SqlServer -- DDL em banco de dados
Pentaho -- ETL com visualização de transformações
SmartFocus(Viper) -- modelagem de dados e calculos estatisticos
Python
C# 
ASP.NET MVC
--------------
-- MJV
SqlServer
informática Powercenter - IDMC
UNIX/LINUX
FTPS
Ajuda em projetos Usanso Python e Databricks
ShellScript Linux -- Ajuste de processos em produção.

Pude aproveitar 100% do conteúdo pela experiência em ETL desde 2012 - 
--================================================









