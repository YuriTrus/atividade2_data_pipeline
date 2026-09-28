import argparse
import io
import json
from datetime import date

import boto3
import pandas as pd


def csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")


def parquet_bytes(df: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    df.to_parquet(
        buffer,
        index=False,
        engine="pyarrow",
        compression="snappy"
    )
    return buffer.getvalue()


def upload(s3, bucket: str, key: str, body: bytes, content_type: str) -> None:
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType=content_type
    )
    print(f"[OK] s3://{bucket}/{key}")


def read_csv_s3(s3, bucket: str, key: str) -> pd.DataFrame:
    """Relê um CSV diretamente da camada Raw no Amazon S3."""
    response = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(response["Body"].read()))


def gerar_dados():
    clientes = pd.DataFrame([
        {"cliente_id": "C001", "nome_cliente": "Ana Silva", "uf": "SP"},
        {"cliente_id": "C002", "nome_cliente": "Bruno Souza", "uf": "RJ"},
        {"cliente_id": "C003", "nome_cliente": "Carla Lima", "uf": "MG"},
        {"cliente_id": "C004", "nome_cliente": "Diego Alves", "uf": "SP"},
        {"cliente_id": "C005", "nome_cliente": "Elisa Rocha", "uf": "PR"},
        {"cliente_id": "C006", "nome_cliente": "Fabio Costa", "uf": "SC"},
    ])

    produtos = pd.DataFrame([
        {"product_id": "PR001", "nome_produto": "Notebook", "categoria": "Informatica", "preco": 3500.00},
        {"product_id": "PR002", "nome_produto": "Mouse", "categoria": "Informatica", "preco": 120.00},
        {"product_id": "PR003", "nome_produto": "Cadeira", "categoria": "Escritorio", "preco": 850.00},
        {"product_id": "PR004", "nome_produto": "Monitor", "categoria": "Informatica", "preco": 1100.00},
        {"product_id": "PR005", "nome_produto": "Mesa", "categoria": "Escritorio", "preco": 650.00},
    ])

    pedidos = pd.DataFrame([
        {"pedido_id": "P001", "cliente_id": "C001", "product_id": "PR001", "quantidade": 2, "data_pedido": "2026-09-20"},
        {"pedido_id": "P002", "cliente_id": "C002", "product_id": "PR002", "quantidade": 1, "data_pedido": "2026-09-20"},
        {"pedido_id": "P003", "cliente_id": "C003", "product_id": "PR003", "quantidade": 5, "data_pedido": "2026-09-21"},
        {"pedido_id": "P004", "cliente_id": "C004", "product_id": "PR004", "quantidade": 3, "data_pedido": "2026-09-21"},
        {"pedido_id": "P005", "cliente_id": "C005", "product_id": "PR005", "quantidade": 4, "data_pedido": "2026-09-22"},
        {"pedido_id": "P006", "cliente_id": "C006", "product_id": "PR001", "quantidade": 1, "data_pedido": "2026-09-22"},
        {"pedido_id": "P007", "cliente_id": "C001", "product_id": "PR004", "quantidade": 2, "data_pedido": "2026-09-23"},
        {"pedido_id": "P008", "cliente_id": "C002", "product_id": "PR005", "quantidade": 1, "data_pedido": "2026-09-23"},
        {"pedido_id": "P009", "cliente_id": "C003", "product_id": "PR002", "quantidade": 2, "data_pedido": "2026-09-24"},
        {"pedido_id": "P010", "cliente_id": "C004", "product_id": "PR003", "quantidade": 6, "data_pedido": "2026-09-24"},

        # Anomalias intencionais para Data Quality
        {"pedido_id": "P011", "cliente_id": "C001", "product_id": "PR001", "quantidade": 0, "data_pedido": "2026-09-24"},
        {"pedido_id": "P012", "cliente_id": "C002", "product_id": "PR002", "quantidade": -2, "data_pedido": "2026-09-24"},
        {"pedido_id": "P013", "cliente_id": "C003", "product_id": "PR999", "quantidade": 1, "data_pedido": "2026-09-24"},
        {"pedido_id": "P014", "cliente_id": "C999", "product_id": "PR003", "quantidade": 1, "data_pedido": "2026-09-24"},
        {"pedido_id": "P015", "cliente_id": "C999", "product_id": "PR999", "quantidade": -1, "data_pedido": "2026-09-24"},
    ])

    return clientes, produtos, pedidos


def validar_pedidos(pedidos, clientes, produtos):
    clientes_validos = set(clientes["cliente_id"])
    produtos_validos = set(produtos["product_id"])

    validos = []
    rejeitados = []

    for registro in pedidos.to_dict(orient="records"):
        motivos = []

        if int(registro["quantidade"]) <= 0:
            motivos.append("quantidade <= 0")

        if registro["product_id"] not in produtos_validos:
            motivos.append("product_id inexistente")

        if registro["cliente_id"] not in clientes_validos:
            motivos.append("cliente_id inexistente")

        if motivos:
            rejeitado = dict(registro)
            rejeitado["motivo_rejeicao"] = "; ".join(motivos)
            rejeitados.append(rejeitado)
        else:
            validos.append(registro)

    return pd.DataFrame(validos), rejeitados


def main():
    parser = argparse.ArgumentParser(description="Atividade 2 - Pipeline S3/Athena")
    parser.add_argument("--bucket", required=True, help="Nome do bucket S3 existente")
    parser.add_argument("--region", default="us-east-1", help="Regiao AWS do bucket")
    parser.add_argument(
        "--ingest-date",
        default=date.today().isoformat(),
        help="Data da ingestao no formato YYYY-MM-DD"
    )
    args = parser.parse_args()

    s3 = boto3.client("s3", region_name=args.region)

    # Falha rapidamente se o bucket nao existir ou nao houver permissao.
    s3.head_bucket(Bucket=args.bucket)

    clientes, produtos, pedidos = gerar_dados()

    # ---------------------------
    # 1. CAMADA RAW - CSV / Hive
    # ---------------------------
    raw_base = f"raw"
    upload(
        s3, args.bucket,
        f"{raw_base}/clientes/ingest_date={args.ingest_date}/clientes.csv",
        csv_bytes(clientes),
        "text/csv"
    )
    upload(
        s3, args.bucket,
        f"{raw_base}/produtos/ingest_date={args.ingest_date}/produtos.csv",
        csv_bytes(produtos),
        "text/csv"
    )
    upload(
        s3, args.bucket,
        f"{raw_base}/pedidos/ingest_date={args.ingest_date}/pedidos.csv",
        csv_bytes(pedidos),
        "text/csv"
    )

    clientes_key = f"{raw_base}/clientes/ingest_date={args.ingest_date}/clientes.csv"
    produtos_key = f"{raw_base}/produtos/ingest_date={args.ingest_date}/produtos.csv"
    pedidos_key = f"{raw_base}/pedidos/ingest_date={args.ingest_date}/pedidos.csv"

    # Relê os arquivos da própria camada Raw para que o processamento
    # Silver/Gold tenha o S3 como fonte efetiva.
    clientes_raw = read_csv_s3(s3, args.bucket, clientes_key)
    produtos_raw = read_csv_s3(s3, args.bucket, produtos_key)
    pedidos_raw = read_csv_s3(s3, args.bucket, pedidos_key)

    # --------------------------------
    # 2. DATA QUALITY + QUARANTINE
    # --------------------------------
    pedidos_validos, rejeitados = validar_pedidos(
        pedidos_raw, clientes_raw, produtos_raw
    )

    json_lines = "\n".join(
        json.dumps(r, ensure_ascii=False) for r in rejeitados
    ) + "\n"

    upload(
        s3, args.bucket,
        f"quarantine/pedidos_rejeitados/data={args.ingest_date}/rejeitados.json",
        json_lines.encode("utf-8"),
        "application/x-ndjson"
    )

    # ---------------------------
    # 3. SILVER / PROCESSED
    # ---------------------------
    fato_vendas = (
        pedidos_validos
        .merge(clientes_raw, on="cliente_id", how="inner")
        .merge(produtos_raw, on="product_id", how="inner")
    )

    fato_vendas["valor_total"] = (
        fato_vendas["quantidade"] * fato_vendas["preco"]
    ).round(2)

    colunas_silver = [
        "pedido_id",
        "data_pedido",
        "cliente_id",
        "nome_cliente",
        "uf",
        "product_id",
        "nome_produto",
        "categoria",
        "quantidade",
        "preco",
        "valor_total",
    ]
    fato_vendas = fato_vendas[colunas_silver]

    upload(
        s3, args.bucket,
        f"processed/fato_vendas/data={args.ingest_date}/part-00000.parquet",
        parquet_bytes(fato_vendas),
        "application/octet-stream"
    )

    # ---------------------------
    # 4. GOLD
    # ---------------------------
    gold = (
        fato_vendas
        .groupby(["uf", "categoria"], as_index=False)
        .agg(
            quantidade_total=("quantidade", "sum"),
            valor_total_vendas=("valor_total", "sum"),
            qtd_pedidos=("pedido_id", "nunique"),
        )
    )
    gold["ticket_medio"] = (
        gold["valor_total_vendas"] / gold["qtd_pedidos"]
    ).round(2)
    gold["valor_total_vendas"] = gold["valor_total_vendas"].round(2)

    upload(
        s3, args.bucket,
        f"gold/vendas_uf_categoria/data={args.ingest_date}/part-00000.parquet",
        parquet_bytes(gold),
        "application/octet-stream"
    )

    # Prefixo que sera usado pelo Athena para salvar os resultados das queries.
    # Nao e necessario criar a pasta fisicamente; o primeiro resultado a cria.
    print()
    print("========== RESUMO ==========")
    print(f"Bucket: s3://{args.bucket}/")
    print(f"Data de ingestao: {args.ingest_date}")
    print(f"Pedidos Raw: {len(pedidos_raw)}")
    print(f"Pedidos validos/Silver: {len(fato_vendas)}")
    print(f"Pedidos rejeitados: {len(rejeitados)}")
    print(f"Linhas Gold: {len(gold)}")
    print(
        "Conciliacao local: "
        f"{len(pedidos_raw)} = {len(fato_vendas)} + {len(rejeitados)}"
    )

    if len(pedidos_raw) == len(fato_vendas) + len(rejeitados):
        print("Status da conciliacao: OK")
    else:
        raise RuntimeError("Falha na conciliacao de integridade.")


if __name__ == "__main__":
    main()
