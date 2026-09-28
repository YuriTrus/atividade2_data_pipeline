# Atividade 2 — Pipeline de Dados com Amazon S3 e Amazon Athena

## 1. Objetivo

Construir um pipeline completo de dados com:

- ingestão de dados simulados na camada Raw;
- particionamento Hive por data de ingestão;
- Data Quality;
- segregação de registros inválidos em quarentena;
- enriquecimento e criação da camada Silver;
- agregação analítica na camada Gold;
- armazenamento em Parquet com compressão Snappy;
- criação de tabelas externas no Amazon Athena;
- auditoria com as colunas ocultas `$path` e `$file_size`;
- conciliação entre dados brutos, válidos e rejeitados.

## 2. Arquitetura

Fluxo implementado:

`Python -> Amazon S3 Raw -> leitura da Raw -> Data Quality -> Quarantine + Silver -> Gold -> Amazon Athena`

O script primeiro gera a massa simulada e grava os arquivos CSV na camada Raw. Em seguida, os CSVs são **relidos diretamente do S3** e passam pelas regras de qualidade, garantindo que a camada Raw seja a fonte efetiva do processamento Silver e Gold.

Estrutura criada no S3:

```text
s3://SEU_BUCKET/
├── raw/
│   ├── clientes/
│   │   └── ingest_date=YYYY-MM-DD/clientes.csv
│   ├── produtos/
│   │   └── ingest_date=YYYY-MM-DD/produtos.csv
│   └── pedidos/
│       └── ingest_date=YYYY-MM-DD/pedidos.csv
├── quarantine/
│   └── pedidos_rejeitados/
│       └── data=YYYY-MM-DD/rejeitados.json
├── processed/
│   └── fato_vendas/
│       └── data=YYYY-MM-DD/part-00000.parquet
├── gold/
│   └── vendas_uf_categoria/
│       └── data=YYYY-MM-DD/part-00000.parquet
└── athena-results/
```

O prefixo `athena-results/` é utilizado pelo Amazon Athena para armazenar os resultados das consultas.

## 3. Regras de Data Quality

Um pedido é considerado inválido quando:

1. `quantidade <= 0`;
2. `product_id` não existe na tabela de produtos;
3. `cliente_id` não existe na tabela de clientes.

Todos os registros inválidos são segregados na pasta `quarantine/` em formato JSON e recebem o campo `motivo_rejeicao`. Quando um registro possui mais de uma anomalia, todos os motivos são registrados.

## 4. Camada Silver

A camada Silver contém somente pedidos válidos.

São feitos os JOINs:

- pedidos válidos + clientes;
- pedidos válidos + produtos.

Também é calculado:

```text
valor_total = quantidade * preco
```

A saída é armazenada em Parquet com compressão Snappy em `processed/fato_vendas/`.

## 5. Camada Gold

A camada Gold agrega as vendas por:

- `uf`;
- `categoria`.

Métricas calculadas:

- `quantidade_total`;
- `valor_total_vendas`;
- `qtd_pedidos`;
- `ticket_medio`.

A saída também é armazenada em Parquet com compressão Snappy.

## 6. Ambiente e pré-requisitos

A execução apresentada nas evidências foi realizada em **AWS Academy Learner Lab**, utilizando o **AWS CloudShell**, Amazon S3 e Amazon Athena, na região `us-east-1`.

No CloudShell, a sessão já utiliza as credenciais temporárias do laboratório. Para execução local, é necessário possuir credenciais AWS válidas configuradas.

Requisitos do projeto:

- sessão AWS com acesso ao Amazon S3 e Amazon Athena;
- bucket Amazon S3 existente;
- Python 3.10 ou superior;
- dependências de `requirements.txt`.

Para criar um ambiente virtual:

```bash
python -m venv .venv
```

No Windows:

```bash
.venv\Scripts\activate
```

No Linux, macOS ou AWS CloudShell:

```bash
source .venv/bin/activate
```

Depois:

```bash
pip install -r requirements.txt
```

## 7. Executar o pipeline

Formato do comando:

```bash
python src/pipeline.py --bucket SEU_BUCKET --region us-east-1 --ingest-date YYYY-MM-DD
```

Na execução registrada nas evidências foram utilizados:

```text
Bucket: atividade2-yuri-2026-001
Região: us-east-1
Data de ingestão: 2026-09-25
```

Comando correspondente:

```bash
python src/pipeline.py --bucket atividade2-yuri-2026-001 --region us-east-1 --ingest-date 2026-09-25
```

Para a massa de dados incluída neste projeto, o resultado esperado é:

```text
Pedidos Raw: 15
Pedidos validos/Silver: 10
Pedidos rejeitados: 5
Linhas Gold: 8
Conciliacao local: 15 = 10 + 5
Status da conciliacao: OK
```

## 8. Configurar o Athena

No console do Amazon Athena:

1. selecionar a mesma região usada pelo S3;
2. abrir o Query Editor;
3. configurar o local dos resultados como:

```text
s3://SEU_BUCKET/athena-results/
```

4. abrir `sql/01_create_database.sql` e executar a instrução `CREATE DATABASE`;
5. substituir `SEU_BUCKET` pelo nome real do bucket em:
   - `sql/02_create_raw_tables.sql`;
   - `sql/03_create_processed_tables.sql`;
6. executar **uma instrução SQL por vez** no Query Editor. O Athena não aceita múltiplas instruções no mesmo envio e retorna `Only one SQL statement is allowed`;
7. executar separadamente cada `CREATE EXTERNAL TABLE` e, depois, cada `MSCK REPAIR TABLE`;
8. confirmar que as partições foram identificadas.

As tabelas externas criadas são:

```text
clientes_raw
produtos_raw
pedidos_raw
pedidos_rejeitados
fato_vendas
vendas_uf_categoria
```

## 9. Auditoria e conciliação

No arquivo `sql/04_auditoria.sql`, substituir:

```text
DATA_INGESTAO
```

pela data utilizada na execução do pipeline, por exemplo:

```text
2026-09-25
```

As consultas desse arquivo também devem ser executadas **uma por vez** no Query Editor.

A auditoria contempla:

- verificação das tabelas Raw, Quarantine, Silver e Gold;
- pseudo-colunas `$path` e `$file_size`;
- demonstração dos registros rejeitados e seus motivos;
- resultado analítico da camada Gold;
- conciliação de integridade.

Regra de conciliação utilizada:

```text
total_raw = total_validos + total_rejeitados
15 = 10 + 5
```

Resultado esperado:

```text
conciliacao = OK
```

## 10. Evidências da execução

As evidências reais utilizadas na entrega estão na pasta `screenshots/`:

```text
screenshots/
├── 01_cloudshell_execucao_pipeline.jpeg
├── 02_s3_raw_pedidos.jpeg
├── 03_s3_quarantine_pedidos_rejeitados.jpeg
├── 04_s3_silver_fato_vendas.jpeg
├── 05_s3_gold_vendas_uf_categoria.jpeg
├── 06_athena_auditoria_path_file_size.jpeg
├── 07_athena_conciliacao_integridade.jpeg
├── 08_athena_data_quality_rejeitados.jpeg
└── 09_athena_gold_resultado_analitico.jpeg
```

As capturas comprovam:

1. execução do pipeline no AWS CloudShell e envio dos arquivos ao S3;
2. camada Raw particionada por `ingest_date`;
3. quarentena em JSON;
4. camada Silver em Parquet;
5. camada Gold em Parquet;
6. auditoria no Athena com `$path` e `$file_size`;
7. conciliação de integridade retornando `OK`;
8. registros rejeitados com `motivo_rejeicao`;
9. resultado analítico da Gold por UF e categoria.

## 11. Resultado da execução

A execução demonstrada nas capturas produziu:

- 15 pedidos na camada Raw;
- 10 pedidos válidos na camada Silver;
- 5 pedidos inválidos na Quarantine;
- 8 combinações de UF e categoria na camada Gold;
- conciliação de integridade com resultado `OK`.

O pipeline implementa as camadas Raw, Silver e Gold, mantém os registros inválidos em quarentena, enriquece os pedidos válidos com clientes e produtos, calcula `valor_total`, produz métricas agregadas e disponibiliza os dados para auditoria no Amazon Athena.

## 12. Tecnologias

- Python
- boto3
- pandas
- PyArrow
- Amazon S3
- Amazon Athena
- CSV
- JSON
- Apache Parquet
- Snappy
