-- 1. Cria o Banco de Dados
CREATE DATABASE IF NOT EXISTS INMET_DB;

-- 2. Cria o Schema (pastinha para organizar as tabelas desse projeto)
CREATE SCHEMA IF NOT EXISTS INMET_DB.PIPELINE;

-- 3. Define que vamos usar esse local para tudo que fizermos agora
USE SCHEMA INMET_DB.PIPELINE;

-- Cria o formato de arquivo baseado no seu script 'etl_bronze.py'
CREATE OR REPLACE FILE FORMAT FORMATO_CSV_INMET
    TYPE = 'CSV'
    FIELD_DELIMITER = ';'       -- Seu arquivo usa ponto e vírgula
    SKIP_HEADER = 9             -- Pula as 8 linhas de cabeçalho inútil
    ENCODING = 'ISO-8859-1'     -- Equivalente ao 'latin-1' do Python
    NULL_IF = ('', 'null', 'NULL') -- Trata vazios como NULL
    REPLACE_INVALID_CHARACTERS = TRUE; -- Evita travar se tiver caractere estranho

-- Cria um Stage (uma "pasta" dentro do Snowflake para receber os arquivos)
CREATE OR REPLACE STAGE STAGE_INMET_RAW
    FILE_FORMAT = FORMATO_CSV_INMET;



-- ETL Bronze
-- Cria tabela Bronze (tudo como texto/VARCHAR para não dar erro na carga)
CREATE OR REPLACE TABLE INMET_BRONZE (
    FILENAME VARCHAR,
    DATA_RAW VARCHAR,
    HORA_RAW VARCHAR,
    PRECIPITACAO VARCHAR,
    PRESSAO_ESTACAO VARCHAR,
    PRESSAO_MAX VARCHAR,
    PRESSAO_MIN VARCHAR,
    RADIACAO VARCHAR,
    TEMP_AR VARCHAR,
    TEMP_ORVALHO VARCHAR,
    TEMP_MAX VARCHAR,
    TEMP_MIN VARCHAR,
    TEMP_ORVALHO_MAX VARCHAR,
    TEMP_ORVALHO_MIN VARCHAR,
    UMID_MAX VARCHAR,
    UMID_MIN VARCHAR,
    UMID_RELATIVA VARCHAR,
    VENTO_DIR VARCHAR,
    VENTO_RAJADA VARCHAR,
    VENTO_VEL VARCHAR,
    LOAD_TIMESTAMP TIMESTAMP_NTZ -- Removemos o default aqui pois vamos passar manualmente abaixo
);

-- Copia os dados
COPY INTO INMET_BRONZE
FROM (
    SELECT 
        METADATA$FILENAME, 
        t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10, 
        t.$11, t.$12, t.$13, t.$14, t.$15, t.$16, t.$17, t.$18, t.$19,
        CURRENT_TIMESTAMP()
    FROM @STAGE_INMET_RAW t
)
FILE_FORMAT = FORMATO_CSV_INMET
ON_ERROR = 'CONTINUE';



-- ETL Silver



CREATE OR REPLACE TABLE INMET_SILVER AS
SELECT
    -- 1. Cidade (Extraída do nome do arquivo)
    TRIM(SPLIT_PART(SPLIT_PART(FILENAME, '/', -1), '_', 5)) AS CIDADE,
    
    -- 2. Data e Hora
    TRY_TO_DATE(DATA_RAW, 'YYYY/MM/DD') AS DATA,
    TRY_TO_TIME(REPLACE(HORA_RAW, ' UTC', ''), 'HH24MI') AS HORA_UTC,
    
    -- 3. Métricas (Convertendo vírgula para ponto e texto para float)
    -- Note que usamos os nomes da tabela BRONZE (ex: UMID_RELATIVA) dentro do REPLACE
    
    TRY_CAST(REPLACE(PRECIPITACAO, ',', '.') AS FLOAT)      AS PRECIPITACAO_TOTAL_MM,
    TRY_CAST(REPLACE(PRESSAO_ESTACAO, ',', '.') AS FLOAT)   AS PRESSAO_ESTACAO_MB,
    TRY_CAST(REPLACE(PRESSAO_MAX, ',', '.') AS FLOAT)       AS PRESSAO_MAX_ANT_MB,
    TRY_CAST(REPLACE(PRESSAO_MIN, ',', '.') AS FLOAT)       AS PRESSAO_MIN_ANT_MB,
    TRY_CAST(REPLACE(RADIACAO, ',', '.') AS FLOAT)          AS RADIACAO_GLOBAL_KJ_M2,
    TRY_CAST(REPLACE(TEMP_AR, ',', '.') AS FLOAT)           AS TEMP_AR_C,
    TRY_CAST(REPLACE(TEMP_ORVALHO, ',', '.') AS FLOAT)      AS TEMP_ORVALHO_C,
    TRY_CAST(REPLACE(TEMP_MAX, ',', '.') AS FLOAT)          AS TEMP_MAX_ANT_C,
    TRY_CAST(REPLACE(TEMP_MIN, ',', '.') AS FLOAT)          AS TEMP_MIN_ANT_C,
    TRY_CAST(REPLACE(TEMP_ORVALHO_MAX, ',', '.') AS FLOAT)  AS TEMP_ORVALHO_MAX_ANT_C,
    TRY_CAST(REPLACE(TEMP_ORVALHO_MIN, ',', '.') AS FLOAT)  AS TEMP_ORVALHO_MIN_ANT_C,
    TRY_CAST(REPLACE(UMID_MAX, ',', '.') AS FLOAT)          AS UMID_MAX_ANT,
    TRY_CAST(REPLACE(UMID_MIN, ',', '.') AS FLOAT)          AS UMID_MIN_ANT,
    
    -- AQUI ESTAVA O ERRO: Lemos UMID_RELATIVA (Bronze) e chamamos de UMIDADE_RELATIVA (Silver)
    TRY_CAST(REPLACE(UMID_RELATIVA, ',', '.') AS FLOAT)     AS UMIDADE_RELATIVA,
    
    TRY_CAST(REPLACE(VENTO_DIR, ',', '.') AS FLOAT)         AS VENTO_DIRECAO_GRAUS,
    TRY_CAST(REPLACE(VENTO_RAJADA, ',', '.') AS FLOAT)      AS VENTO_RAJADA_MAX_MS,
    TRY_CAST(REPLACE(VENTO_VEL, ',', '.') AS FLOAT)         AS VENTO_VELOCIDADE_MS

FROM INMET_BRONZE
WHERE DATA_RAW IS NOT NULL;



-- ETL Gold



CREATE OR REPLACE TABLE INMET_GOLD AS
WITH ESTATISTICAS AS (
    -- Calcula Min e Max de toda a tabela Silver de uma vez só
    SELECT
        MIN(PRECIPITACAO_TOTAL_MM) as min_precip, MAX(PRECIPITACAO_TOTAL_MM) as max_precip,
        MIN(TEMP_AR_C) as min_temp, MAX(TEMP_AR_C) as max_temp,
        MIN(UMIDADE_RELATIVA) as min_umid, MAX(UMIDADE_RELATIVA) as max_umid,
        MIN(VENTO_VELOCIDADE_MS) as min_vento, MAX(VENTO_VELOCIDADE_MS) as max_vento
    FROM INMET_SILVER
)
SELECT
    s.CIDADE,
    s.DATA,
    s.HORA_UTC,
    -- Aplica a fórmula: (Valor - Min) / (Max - Min)
    (s.PRECIPITACAO_TOTAL_MM - e.min_precip) / NULLIF(e.max_precip - e.min_precip, 0) AS PRECIPITACAO_NORM,
    (s.TEMP_AR_C - e.min_temp) / NULLIF(e.max_temp - e.min_temp, 0) AS TEMP_AR_NORM,
    (s.UMIDADE_RELATIVA - e.min_umid) / NULLIF(e.max_umid - e.min_umid, 0) AS UMIDADE_NORM,
    (s.VENTO_VELOCIDADE_MS - e.min_vento) / NULLIF(e.max_vento - e.min_vento, 0) AS VENTO_VEL_NORM

FROM INMET_SILVER s
CROSS JOIN ESTATISTICAS e;



-- Configuração Cron Job



CREATE OR REPLACE STREAM STREAM_INMET_BRONZE 
ON TABLE INMET_BRONZE 
APPEND_ONLY = TRUE;

CREATE OR REPLACE TASK TASK_DIARIA_INMET
    WAREHOUSE = COMPUTE_WH  -- O motor que vai rodar a tarefa
    SCHEDULE = 'USING CRON 0 6 * * * UTC' -- Sintaxe Cron: Minuto 0, Hora 6, Todo dia/mês
AS
    CALL RODAR_PIPELINE_INMET();


ALTER TASK TASK_DIARIA_INMET RESUME;



-- Ajustes Schemas



CREATE OR REPLACE PROCEDURE RODAR_PIPELINE_INMET()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- ==========================================================
    -- 1. CAMADA BRONZE (Ingestão Incremental)
    -- ==========================================================
    -- Não truncamos mais a tabela! Apenas copiamos novos arquivos.
    -- O Snowflake sabe quais arquivos já foram carregados e os ignora automaticamente.
    
    COPY INTO INMET_BRONZE
    FROM (
        SELECT 
            METADATA$FILENAME, 
            t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10, 
            t.$11, t.$12, t.$13, t.$14, t.$15, t.$16, t.$17, t.$18, t.$19,
            CURRENT_TIMESTAMP()
        FROM @STAGE_INMET_RAW t
    )
    FILE_FORMAT = FORMATO_CSV_INMET
    ON_ERROR = 'CONTINUE';

    -- Verificamos se o STREAM tem dados novos. Se não tiver, paramos aqui para economizar.
    IF (SYSTEM$STREAM_HAS_DATA('STREAM_INMET_BRONZE') = FALSE) THEN
        RETURN 'Nenhum dado novo para processar.';
    END IF;

    -- ==========================================================
    -- 2. CAMADA SILVER (Processamento Incremental)
    -- ==========================================================
    -- Lemos do STREAM, transformamos e INSERIMOS na Silver existente.
    
    INSERT INTO INMET_SILVER
    SELECT
        TRIM(SPLIT_PART(SPLIT_PART(FILENAME, '/', -1), '_', 5)) AS CIDADE,
        TRY_TO_DATE(DATA_RAW, 'YYYY/MM/DD') AS DATA,
        TRY_TO_TIME(REPLACE(HORA_RAW, ' UTC', ''), 'HH24MI') AS HORA_UTC,
        TRY_CAST(REPLACE(PRECIPITACAO, ',', '.') AS FLOAT)      AS PRECIPITACAO_TOTAL_MM,
        TRY_CAST(REPLACE(PRESSAO_ESTACAO, ',', '.') AS FLOAT)   AS PRESSAO_ESTACAO_MB,
        TRY_CAST(REPLACE(PRESSAO_MAX, ',', '.') AS FLOAT)       AS PRESSAO_MAX_ANT_MB,
        TRY_CAST(REPLACE(PRESSAO_MIN, ',', '.') AS FLOAT)       AS PRESSAO_MIN_ANT_MB,
        TRY_CAST(REPLACE(RADIACAO, ',', '.') AS FLOAT)          AS RADIACAO_GLOBAL_KJ_M2,
        TRY_CAST(REPLACE(TEMP_AR, ',', '.') AS FLOAT)           AS TEMP_AR_C,
        TRY_CAST(REPLACE(TEMP_ORVALHO, ',', '.') AS FLOAT)      AS TEMP_ORVALHO_C,
        TRY_CAST(REPLACE(TEMP_MAX, ',', '.') AS FLOAT)          AS TEMP_MAX_ANT_C,
        TRY_CAST(REPLACE(TEMP_MIN, ',', '.') AS FLOAT)          AS TEMP_MIN_ANT_C,
        TRY_CAST(REPLACE(TEMP_ORVALHO_MAX, ',', '.') AS FLOAT)  AS TEMP_ORVALHO_MAX_ANT_C,
        TRY_CAST(REPLACE(TEMP_ORVALHO_MIN, ',', '.') AS FLOAT)  AS TEMP_ORVALHO_MIN_ANT_C,
        TRY_CAST(REPLACE(UMID_MAX, ',', '.') AS FLOAT)          AS UMID_MAX_ANT,
        TRY_CAST(REPLACE(UMID_MIN, ',', '.') AS FLOAT)          AS UMID_MIN_ANT,
        TRY_CAST(REPLACE(UMID_RELATIVA, ',', '.') AS FLOAT)     AS UMIDADE_RELATIVA,
        TRY_CAST(REPLACE(VENTO_DIR, ',', '.') AS FLOAT)         AS VENTO_DIRECAO_GRAUS,
        TRY_CAST(REPLACE(VENTO_RAJADA, ',', '.') AS FLOAT)      AS VENTO_RAJADA_MAX_MS,
        TRY_CAST(REPLACE(VENTO_VEL, ',', '.') AS FLOAT)         AS VENTO_VELOCIDADE_MS
    FROM STREAM_INMET_BRONZE -- LER DO STREAM, NÃO DA TABELA
    WHERE DATA_RAW IS NOT NULL;

    -- ==========================================================
    -- 3. CAMADA GOLD (Full Refresh - Necessário para Normalização Global)
    -- ==========================================================
    -- Recalcula Min/Max global e regera a Gold.
    
    CREATE OR REPLACE TABLE INMET_GOLD AS
    WITH ESTATISTICAS AS (
        SELECT
            MIN(PRECIPITACAO_TOTAL_MM) as min_precip, MAX(PRECIPITACAO_TOTAL_MM) as max_precip,
            MIN(TEMP_AR_C) as min_temp, MAX(TEMP_AR_C) as max_temp,
            MIN(UMIDADE_RELATIVA) as min_umid, MAX(UMIDADE_RELATIVA) as max_umid,
            MIN(VENTO_VELOCIDADE_MS) as min_vento, MAX(VENTO_VELOCIDADE_MS) as max_vento
        FROM INMET_SILVER
    )
    SELECT
        s.CIDADE,
        s.DATA,
        s.HORA_UTC,
        (s.PRECIPITACAO_TOTAL_MM - e.min_precip) / NULLIF(e.max_precip - e.min_precip, 0) AS PRECIPITACAO_NORM,
        (s.TEMP_AR_C - e.min_temp) / NULLIF(e.max_temp - e.min_temp, 0) AS TEMP_AR_NORM,
        (s.UMIDADE_RELATIVA - e.min_umid) / NULLIF(e.max_umid - e.min_umid, 0) AS UMIDADE_NORM,
        (s.VENTO_VELOCIDADE_MS - e.min_vento) / NULLIF(e.max_vento - e.min_vento, 0) AS VENTO_VEL_NORM
    FROM INMET_SILVER s
    CROSS JOIN ESTATISTICAS e;

    RETURN 'Pipeline Incremental Executado com Sucesso!';
END;
$$;
