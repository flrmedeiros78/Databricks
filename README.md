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
-- COMANDOS EXECUTADOS :
USE CATALOG dbacademy;
USE SCHEMA get_started_de;

-- Aprendizado:
Navegue pela hierarquia do Catálogo Unity (catálogo → esquema → volume)INSERT INTO
Visualizar dados CSV brutos usando UPDATE ... SET ... WHERE
Crie uma tabela Delta usando DELETE FROM ... WHERE
Verifique tabelas no SQL e no Catalog Explorer

-- COMENTÁRIOS:

















