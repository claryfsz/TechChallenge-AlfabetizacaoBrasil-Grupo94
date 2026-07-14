
#Bronze Layer - Ingestão de dados brutos


import argparse
import logging
import os
from datetime import datetime, timezone

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("bronze_ingest")

TABLES = {
    "uf": {
        "dataset_id": "br_bd_diretorios_brasil",
        "table_id": "uf",
    },
    "municipio": {
        "dataset_id": "br_bd_diretorios_brasil",
        "table_id": "municipio",
    },
    "meta_brasil": {
        "dataset_id": "br_inep_avaliacao_alfabetizacao",
        "table_id": "meta_alfabetizacao_brasil",
    },
    "meta_uf": {
        "dataset_id": "br_inep_avaliacao_alfabetizacao",
        "table_id": "meta_alfabetizacao_uf",
    },
    "meta_municipio": {
        "dataset_id": "br_inep_avaliacao_alfabetizacao",
        "table_id": "meta_alfabetizacao_municipio",
    },
    "indicador_uf": {
        "dataset_id": "br_inep_avaliacao_alfabetizacao",
        "table_id": "uf",
    },
    "indicador_municipio": {
        "dataset_id": "br_inep_avaliacao_alfabetizacao",
        "table_id": "municipio",
    },
}


PARTITIONED_TABLES = {
    "meta_brasil", "meta_uf", "meta_municipio",
    "indicador_uf", "indicador_municipio"
}

BRONZE_DIR = os.path.dirname(__file__)


def fetch_table(dataset_id: str, table_id: str, billing_project: str) -> pd.DataFrame:

    import basedosdados as bd

    logger.info(f"Baixando {dataset_id}.{table_id} da Base dos Dados...")
    df = bd.read_table(
        dataset_id=dataset_id,
        table_id=table_id,
        billing_project_id=billing_project,
    )
    logger.info(f"  -> {len(df)} linhas, {len(df.columns)} colunas")
    return df


def save_bronze(df: pd.DataFrame, name: str) -> str:

    import pyarrow as pa
    import pyarrow.parquet as pq

    ingestion_ts = datetime.now(timezone.utc).isoformat()
    df["_ingested_at"] = ingestion_ts

    if name in PARTITIONED_TABLES and "ano" in df.columns:
        out_dir = os.path.join(BRONZE_DIR, name)
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_to_dataset(table, root_path=out_dir, partition_cols=["ano"])
        logger.info(f"  -> salvo particionado por ano em {out_dir}/ano=<YYYY>/")
        return out_dir  # diretório, não arquivo único

    out_path = os.path.join(BRONZE_DIR, f"{name}.parquet")
    df.to_parquet(out_path, index=False, engine="pyarrow")
    logger.info(f"  -> salvo em {out_path}")
    return out_path


def upload_to_s3(local_path: str, bucket: str, key_prefix: str = "bronze"):

    import boto3

    s3 = boto3.client("s3")

    if os.path.isdir(local_path):
        table_name = os.path.basename(local_path.rstrip("/"))
        for root, _, files in os.walk(local_path):
            for fname in files:
                full_path = os.path.join(root, fname)
                rel_path = os.path.relpath(full_path, local_path)
                key = f"{key_prefix}/{table_name}/{rel_path}"
                s3.upload_file(full_path, bucket, key)
        logger.info(f"  -> diretório particionado enviado para s3://{bucket}/{key_prefix}/{table_name}/")
    else:
        file_name = os.path.basename(local_path)
        key = f"{key_prefix}/{file_name}"
        s3.upload_file(local_path, bucket, key)
        logger.info(f"  -> enviado para s3://{bucket}/{key}")


def main():
    parser = argparse.ArgumentParser(description="Ingestão Bronze - Indicador Criança Alfabetizada")
    parser.add_argument("--billing-project", required=True, help="ID do projeto GCP usado como billing project")
    parser.add_argument("--s3-bucket", default=None, help="Nome do bucket S3 de destino")
    parser.add_argument("--upload-s3", action="store_true", help="Se definido, envia os arquivos para o S3")
    parser.add_argument("--only", nargs="*", default=None, help="Lista de tabelas específicas a baixar (default: todas)")
    args = parser.parse_args()

    tables_to_run = args.only or list(TABLES.keys())

    for name in tables_to_run:
        if name not in TABLES:
            logger.warning(f"Tabela desconhecida: {name}, pulando.")
            continue
        cfg = TABLES[name]
        try:
            df = fetch_table(cfg["dataset_id"], cfg["table_id"], args.billing_project)
            local_path = save_bronze(df, name)
            if args.upload_s3:
                if not args.s3_bucket:
                    logger.error("--s3-bucket é obrigatório quando --upload-s3 é usado")
                    continue
                upload_to_s3(local_path, args.s3_bucket)
        except Exception as e:
            logger.error(f"Falha ao ingerir {name}: {e}")

    logger.info("Ingestão Bronze concluída.")


if __name__ == "__main__":
    main()
