-- 02_create_raw_tables.sql
-- SUBSTITUA SEU_BUCKET pelo nome real do seu bucket.
-- IMPORTANTE: no Amazon Athena, execute uma instrução SQL por vez.

CREATE EXTERNAL TABLE IF NOT EXISTS atividade2_db.clientes_raw (
    cliente_id STRING,
    nome_cliente STRING,
    uf STRING
)
PARTITIONED BY (ingest_date STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '"',
    'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION 's3://SEU_BUCKET/raw/clientes/'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE IF NOT EXISTS atividade2_db.produtos_raw (
    product_id STRING,
    nome_produto STRING,
    categoria STRING,
    preco DOUBLE
)
PARTITIONED BY (ingest_date STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '"',
    'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION 's3://SEU_BUCKET/raw/produtos/'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE IF NOT EXISTS atividade2_db.pedidos_raw (
    pedido_id STRING,
    cliente_id STRING,
    product_id STRING,
    quantidade INT,
    data_pedido STRING
)
PARTITIONED BY (ingest_date STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '"',
    'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION 's3://SEU_BUCKET/raw/pedidos/'
TBLPROPERTIES ('skip.header.line.count'='1');

MSCK REPAIR TABLE atividade2_db.clientes_raw;
MSCK REPAIR TABLE atividade2_db.produtos_raw;
MSCK REPAIR TABLE atividade2_db.pedidos_raw;
