-- 03_create_processed_tables.sql
-- SUBSTITUA SEU_BUCKET pelo nome real do seu bucket.
-- IMPORTANTE: no Amazon Athena, execute uma instrução SQL por vez.

CREATE EXTERNAL TABLE IF NOT EXISTS atividade2_db.pedidos_rejeitados (
    pedido_id STRING,
    cliente_id STRING,
    product_id STRING,
    quantidade INT,
    data_pedido STRING,
    motivo_rejeicao STRING
)
PARTITIONED BY (`data` STRING)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION 's3://SEU_BUCKET/quarantine/pedidos_rejeitados/';

CREATE EXTERNAL TABLE IF NOT EXISTS atividade2_db.fato_vendas (
    pedido_id STRING,
    data_pedido STRING,
    cliente_id STRING,
    nome_cliente STRING,
    uf STRING,
    product_id STRING,
    nome_produto STRING,
    categoria STRING,
    quantidade BIGINT,
    preco DOUBLE,
    valor_total DOUBLE
)
PARTITIONED BY (`data` STRING)
STORED AS PARQUET
LOCATION 's3://SEU_BUCKET/processed/fato_vendas/';

CREATE EXTERNAL TABLE IF NOT EXISTS atividade2_db.vendas_uf_categoria (
    uf STRING,
    categoria STRING,
    quantidade_total BIGINT,
    valor_total_vendas DOUBLE,
    qtd_pedidos BIGINT,
    ticket_medio DOUBLE
)
PARTITIONED BY (`data` STRING)
STORED AS PARQUET
LOCATION 's3://SEU_BUCKET/gold/vendas_uf_categoria/';

MSCK REPAIR TABLE atividade2_db.pedidos_rejeitados;
MSCK REPAIR TABLE atividade2_db.fato_vendas;
MSCK REPAIR TABLE atividade2_db.vendas_uf_categoria;
