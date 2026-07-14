# -*- coding: utf-8 -*-
"""
Created on Sun Jul 12 20:38:19 2026

@author: luiz.alves
"""

import argparse
import logging
import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("gold_create")


def read_silver_table(silver_dir: str, name: str) -> pd.DataFrame:
    path = os.path.join(silver_dir, name)
    flat_path = os.path.join(silver_dir, f"{name}.parquet")

    if os.path.isdir(path):
        table = pq.read_table(path)
        for i, field in enumerate(table.schema):
            if pa.types.is_dictionary(field.type):
                table = table.set_column(i, field.name, table.column(i).cast(field.type.value_type))
        df = table.to_pandas()
    elif os.path.exists(flat_path):
        df = pd.read_parquet(flat_path)
    else:
        raise FileNotFoundError(f"Tabela silver nao encontrada: {name}")

    logger.info(f"Lido silver/{name}: {len(df)} linhas")
    return df


def save_gold(df, name, gold_dir, partition_by_ano=False, s3_bucket=None):
    if partition_by_ano and "ano" in df.columns:
        out_dir = os.path.join(gold_dir, name)
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_to_dataset(table, root_path=out_dir, partition_cols=["ano"])
        logger.info(f"  -> salvo particionado em {out_dir}/ano=<YYYY>/")
        local_path = out_dir
    else:
        out_path = os.path.join(gold_dir, f"{name}.parquet")
        os.makedirs(gold_dir, exist_ok=True)
        df.to_parquet(out_path, index=False)
        logger.info(f"  -> salvo em {out_path}")
        local_path = out_path

    if s3_bucket:
        upload_to_s3(local_path, s3_bucket)


def upload_to_s3(local_path, bucket, key_prefix="gold"):
    import boto3

    s3 = boto3.client("s3")

    if os.path.isdir(local_path):
        table_name = os.path.basename(local_path.rstrip("/").rstrip("\\"))
        for root, _, files in os.walk(local_path):
            for fname in files:
                full_path = os.path.join(root, fname)
                rel_path = os.path.relpath(full_path, local_path)
                key = f"{key_prefix}/{table_name}/{rel_path}".replace("\\", "/")
                s3.upload_file(full_path, bucket, key)
        logger.info(f"  -> diretorio enviado para s3://{bucket}/{key_prefix}/{table_name}/")
    else:
        file_name = os.path.basename(local_path)
        key = f"{key_prefix}/{file_name}"
        s3.upload_file(local_path, bucket, key)
        logger.info(f"  -> enviado para s3://{bucket}/{key}")


def main():
    parser = argparse.ArgumentParser(description="Criacao da camada Gold")
    parser.add_argument("--silver-dir", default="../silver")
    parser.add_argument("--gold-dir", default="../gold")
    parser.add_argument("--s3-bucket", default=None)
    args = parser.parse_args()

    os.makedirs(args.gold_dir, exist_ok=True)

    silver_municipio = read_silver_table(args.silver_dir, "indicador_meta_municipio")

    gold1 = silver_municipio[[
        "id_municipio", "nome_municipio", "sigla_uf", "ano", "taxa_alfabetizacao"
    ]].rename(columns={
        "nome_municipio": "municipio",
        "sigla_uf": "uf",
        "taxa_alfabetizacao": "indicador",
    }).drop_duplicates()
    save_gold(gold1, "gold_indicador_municipio", args.gold_dir, partition_by_ano=True, s3_bucket=args.s3_bucket)

    gold2 = silver_municipio[[
        "nome_municipio", "sigla_uf", "ano", "meta_alfabetizacao", "taxa_alfabetizacao"
    ]].rename(columns={
        "nome_municipio": "municipio",
        "sigla_uf": "uf",
        "meta_alfabetizacao": "meta",
        "taxa_alfabetizacao": "resultado",
    }).drop_duplicates()
    gold2 = gold2.dropna(subset=["meta"])
    gold2["diferenca"] = gold2["resultado"] - gold2["meta"]
    save_gold(gold2, "gold_meta_vs_resultado", args.gold_dir, partition_by_ano=True, s3_bucket=args.s3_bucket)

    gold3 = (
        silver_municipio.groupby("ano", as_index=False)["taxa_alfabetizacao"]
        .mean()
        .rename(columns={"taxa_alfabetizacao": "indicador"})
        .round(2)
    )
    save_gold(gold3, "gold_evolucao_temporal", args.gold_dir, s3_bucket=args.s3_bucket)

    logger.info("Criacao da camada Gold concluida.")
    logger.info(f"Gold 1 (indicador por municipio): {len(gold1)} linhas")
    logger.info(f"Gold 2 (meta vs resultado): {len(gold2)} linhas")
    logger.info(f"Gold 3 (evolucao temporal): {len(gold3)} linhas -> {gold3.to_dict('records')}")


if __name__ == "__main__":
    main()