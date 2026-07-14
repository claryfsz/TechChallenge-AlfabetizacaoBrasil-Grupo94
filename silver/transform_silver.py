"""
Silver Layer - Limpeza, padronizacao e integracao
"""

import argparse
import logging
import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("silver_transform")



# Leitura auxiliar (tabelas particionadas por ano ficam em subpastas ano=YYYY)

def read_bronze_table(bronze_dir: str, name: str) -> pd.DataFrame:
    path = os.path.join(bronze_dir, name)
    flat_path = os.path.join(bronze_dir, f"{name}.parquet")

    if os.path.isdir(path):

        table = pq.read_table(path)
        for i, field in enumerate(table.schema):
            if pa.types.is_dictionary(field.type):
                table = table.set_column(i, field.name, table.column(i).cast(field.type.value_type))
        df = table.to_pandas()
    elif os.path.exists(flat_path):
        df = pd.read_parquet(flat_path)
    else:
        raise FileNotFoundError(f"Tabela bronze nao encontrada: {name}")

    logger.info(f"Lido bronze/{name}: {len(df)} linhas")
    return df



# Limpeza generica aplicada a qualquer tabela

def clean_table(df: pd.DataFrame, subset_key: list[str] = None) -> pd.DataFrame:
    before = len(df)

    # 1. Remove colunas de metadado de ingestao (nao fazem parte do dado de negocio)
    df = df.drop(columns=[c for c in df.columns if c.startswith("_")], errors="ignore")

    # 2. Padroniza nomes de colunas (minusculo, sem espaco)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # 3. Remove duplicados
    df = df.drop_duplicates()

    # 4. Remove linhas sem a chave principal (nao da pra integrar sem chave)
    if subset_key:
        df = df.dropna(subset=[k for k in subset_key if k in df.columns])

    after = len(df)
    if before != after:
        logger.info(f"  -> limpeza: {before} -> {after} linhas ({before - after} removidas)")
    return df


# Transforma as tabelas de meta (formato largo: uma coluna por ano-meta) em formato longo (uma linha por ano), para poder integrar com o indicador real ano a ano.

META_YEAR_COLS = [
    "meta_alfabetizacao_2024", "meta_alfabetizacao_2025", "meta_alfabetizacao_2026",
    "meta_alfabetizacao_2027", "meta_alfabetizacao_2028", "meta_alfabetizacao_2029",
    "meta_alfabetizacao_2030",
]


def melt_meta(df: pd.DataFrame, id_cols: list[str]) -> pd.DataFrame:
    year_cols = [c for c in META_YEAR_COLS if c in df.columns]
    long_df = df.melt(
        id_vars=[c for c in id_cols if c in df.columns],
        value_vars=year_cols,
        var_name="meta_ano_col",
        value_name="meta_alfabetizacao",
    )
    long_df["ano_meta"] = long_df["meta_ano_col"].str.extract(r"(\d{4})").astype("Int64")
    long_df = long_df.drop(columns=["meta_ano_col"])
    return long_df


def save_silver(df: pd.DataFrame, name: str, silver_dir: str, partition_by_ano: bool = False, s3_bucket: str = None):
    if partition_by_ano and "ano" in df.columns:
        out_dir = os.path.join(silver_dir, name)
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_to_dataset(table, root_path=out_dir, partition_cols=["ano"])
        logger.info(f"  -> salvo particionado em {out_dir}/ano=<YYYY>/")
        local_path = out_dir
    else:
        out_path = os.path.join(silver_dir, f"{name}.parquet")
        os.makedirs(silver_dir, exist_ok=True)
        df.to_parquet(out_path, index=False)
        logger.info(f"  -> salvo em {out_path}")
        local_path = out_path

    if s3_bucket:
        upload_to_s3(local_path, s3_bucket)


def upload_to_s3(local_path: str, bucket: str, key_prefix: str = "silver"):
    """Envia para o S3, preservando estrutura de partição quando for diretório."""
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
    parser = argparse.ArgumentParser(description="Transformacao Silver")
    parser.add_argument("--bronze-dir", default="../bronze")
    parser.add_argument("--silver-dir", default="../silver")
    parser.add_argument("--s3-bucket", default=None, help="Se definido, sobe os resultados para o S3")
    args = parser.parse_args()

    os.makedirs(args.silver_dir, exist_ok=True)

    # --- Dimensoes ---------------------------------------------------------
    dim_uf = clean_table(read_bronze_table(args.bronze_dir, "uf"), subset_key=["sigla"])
    dim_uf = dim_uf.rename(columns={"nome": "nome_uf"})[["sigla", "nome_uf", "id_uf"] if "id_uf" in dim_uf.columns else ["sigla", "nome_uf"]]
    save_silver(dim_uf, "dim_uf", args.silver_dir, s3_bucket=args.s3_bucket)

    dim_municipio = clean_table(read_bronze_table(args.bronze_dir, "municipio"), subset_key=["id_municipio"])
    keep_cols = [c for c in ["id_municipio", "nome", "sigla_uf"] if c in dim_municipio.columns]
    dim_municipio = dim_municipio[keep_cols].rename(columns={"nome": "nome_municipio"})
    save_silver(dim_municipio, "dim_municipio", args.silver_dir, s3_bucket=args.s3_bucket)

    # --- Indicadores (fato: resultado real por ano) -------------------------
    ind_uf = clean_table(read_bronze_table(args.bronze_dir, "indicador_uf"), subset_key=["sigla_uf", "ano"])
    ind_municipio = clean_table(read_bronze_table(args.bronze_dir, "indicador_municipio"), subset_key=["id_municipio", "ano"])

    # --- Metas (fato: meta por ano, formato largo -> longo) -----------------
    meta_brasil = clean_table(read_bronze_table(args.bronze_dir, "meta_brasil"))
    meta_uf = clean_table(read_bronze_table(args.bronze_dir, "meta_uf"), subset_key=["sigla_uf"])
    meta_municipio = clean_table(read_bronze_table(args.bronze_dir, "meta_municipio"), subset_key=["id_municipio"])

    meta_uf_long = melt_meta(meta_uf, id_cols=["sigla_uf", "rede"])
    meta_municipio_long = melt_meta(meta_municipio, id_cols=["id_municipio", "rede"])

    # --- Integracao: indicador real + meta do mesmo ano + nome do territorio
    integrado_uf = ind_uf.merge(dim_uf, left_on="sigla_uf", right_on="sigla", how="left")
    meta_uf_long_join = meta_uf_long.drop(columns=["rede"], errors="ignore")
    integrado_uf = integrado_uf.merge(
        meta_uf_long_join, left_on=["sigla_uf", "ano"], right_on=["sigla_uf", "ano_meta"], how="left"
    )
    save_silver(integrado_uf, "indicador_meta_uf", args.silver_dir, partition_by_ano=True, s3_bucket=args.s3_bucket)

    integrado_municipio = ind_municipio.merge(dim_municipio, on="id_municipio", how="left")
    meta_municipio_long_join = meta_municipio_long.drop(columns=["rede"], errors="ignore")
    integrado_municipio = integrado_municipio.merge(
        meta_municipio_long_join, left_on=["id_municipio", "ano"], right_on=["id_municipio", "ano_meta"], how="left"
    )
    save_silver(integrado_municipio, "indicador_meta_municipio", args.silver_dir, partition_by_ano=True, s3_bucket=args.s3_bucket)

    save_silver(meta_brasil, "meta_brasil", args.silver_dir, partition_by_ano=True, s3_bucket=args.s3_bucket)

    logger.info("Transformacao Silver concluida.")


if __name__ == "__main__":
    main()