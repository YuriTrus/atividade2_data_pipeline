-- 04_auditoria.sql
-- SUBSTITUA DATA_INGESTAO pela data executada, por exemplo: 2026-09-25.
-- IMPORTANTE: no Amazon Athena, execute cada SELECT (ou WITH ... SELECT) separadamente.

-- A) Verificacao visual das tabelas
SELECT *
FROM atividade2_db.pedidos_raw
WHERE ingest_date = 'DATA_INGESTAO'
ORDER BY pedido_id;

SELECT *
FROM atividade2_db.pedidos_rejeitados
WHERE data = 'DATA_INGESTAO'
ORDER BY pedido_id;

SELECT *
FROM atividade2_db.fato_vendas
WHERE data = 'DATA_INGESTAO'
ORDER BY pedido_id;

SELECT *
FROM atividade2_db.vendas_uf_categoria
WHERE data = 'DATA_INGESTAO'
ORDER BY uf, categoria;


-- B) Auditoria de metadados exigida: $path e $file_size
SELECT DISTINCT
    "$path" AS caminho_s3,
    "$file_size" AS tamanho_bytes
FROM atividade2_db.pedidos_raw
WHERE ingest_date = 'DATA_INGESTAO'
ORDER BY caminho_s3;

SELECT DISTINCT
    "$path" AS caminho_s3,
    "$file_size" AS tamanho_bytes
FROM atividade2_db.fato_vendas
WHERE data = 'DATA_INGESTAO'
ORDER BY caminho_s3;

SELECT DISTINCT
    "$path" AS caminho_s3,
    "$file_size" AS tamanho_bytes
FROM atividade2_db.vendas_uf_categoria
WHERE data = 'DATA_INGESTAO'
ORDER BY caminho_s3;


-- C) Conciliação de integridade
-- Regra usada: total Raw = total Silver (válidos) + total Quarantine (inválidos).
WITH totais AS (
    SELECT
        (
            SELECT COUNT(*)
            FROM atividade2_db.pedidos_raw
            WHERE ingest_date = 'DATA_INGESTAO'
        ) AS total_raw,
        (
            SELECT COUNT(*)
            FROM atividade2_db.fato_vendas
            WHERE data = 'DATA_INGESTAO'
        ) AS total_validos,
        (
            SELECT COUNT(*)
            FROM atividade2_db.pedidos_rejeitados
            WHERE data = 'DATA_INGESTAO'
        ) AS total_rejeitados
)
SELECT
    total_raw,
    total_validos,
    total_rejeitados,
    total_validos + total_rejeitados AS total_processado,
    CASE
        WHEN total_raw = total_validos + total_rejeitados THEN 'OK'
        ELSE 'DIVERGENCIA'
    END AS conciliacao
FROM totais;


-- D) Demonstracao das regras de qualidade
SELECT
    pedido_id,
    quantidade,
    cliente_id,
    product_id,
    motivo_rejeicao
FROM atividade2_db.pedidos_rejeitados
WHERE data = 'DATA_INGESTAO'
ORDER BY pedido_id;


-- E) Consulta analitica final da camada Gold
SELECT
    uf,
    categoria,
    quantidade_total,
    valor_total_vendas,
    qtd_pedidos,
    ticket_medio
FROM atividade2_db.vendas_uf_categoria
WHERE data = 'DATA_INGESTAO'
ORDER BY valor_total_vendas DESC;
