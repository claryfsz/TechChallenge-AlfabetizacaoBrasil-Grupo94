"""
Quality Layer - Validações de qualidade de dados
====================================================
Responsável: Colega
Objetivo: Rodar checagens de qualidade sobre os dados (bronze e/ou silver),
gerando um relatório simples que pode ser anexado à documentação ou usado
como gate antes de promover dados para a próxima camada.

Checagens implementadas:
    1. Duplicidade de registros
    2. Valores ausentes (nulos) por coluna
    3. Validação de chave primária (unicidade)
    4. Consistência de chave estrangeira entre tabelas (ex: município -> UF)

Uso:
    python validations.py --input ../bronze/municipio.parquet --key id_municipio
    python validations.py --check-all --data-dir ../bronze
"""

import argparse
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("quality_validations")


@dataclass
class QualityReport:
    table_name: str
    n_rows: int
    n_duplicates: int
    null_counts: dict
    key_is_unique: bool = None
    issues: list = field(default_factory=list)

    def to_dict(self):
        return {
            "table_name": self.table_name,
            "n_rows": self.n_rows,
            "n_duplicates": self.n_duplicates,
            "null_counts": self.null_counts,
            "key_is_unique": self.key_is_unique,
            "issues": self.issues,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }


def check_duplicates(df: pd.DataFrame) -> int:
    return int(df.duplicated().sum())


def check_nulls(df: pd.DataFrame) -> dict:
    return {col: int(cnt) for col, cnt in df.isnull().sum().items() if cnt > 0}


def check_key_uniqueness(df: pd.DataFrame, key: str) -> bool:
    if key not in df.columns:
        return None
    return not df[key].duplicated().any()


def check_referential_consistency(child_df: pd.DataFrame, child_key: str,
                                   parent_df: pd.DataFrame, parent_key: str) -> list:
    """Retorna a lista de valores de child_key que não existem em parent_key."""
    if child_key not in child_df.columns or parent_key not in parent_df.columns:
        return []
    missing = set(child_df[child_key].dropna().unique()) - set(parent_df[parent_key].dropna().unique())
    return sorted(list(missing))[:20]  # limita a amostra no relatório


def run_quality_check(path: str, key: str = None) -> QualityReport:
    df = pd.read_parquet(path)
    table_name = os.path.splitext(os.path.basename(path))[0]

    n_dup = check_duplicates(df)
    nulls = check_nulls(df)
    key_unique = check_key_uniqueness(df, key) if key else None

    issues = []
    if n_dup > 0:
        issues.append(f"{n_dup} linhas duplicadas encontradas")
    if nulls:
        issues.append(f"Colunas com valores nulos: {list(nulls.keys())}")
    if key and key_unique is False:
        issues.append(f"Chave '{key}' não é única")

    report = QualityReport(
        table_name=table_name,
        n_rows=len(df),
        n_duplicates=n_dup,
        null_counts=nulls,
        key_is_unique=key_unique,
        issues=issues,
    )
    return report


def main():
    parser = argparse.ArgumentParser(description="Validações de qualidade de dados")
    parser.add_argument("--input", help="Caminho de um arquivo parquet específico")
    parser.add_argument("--key", help="Nome da coluna chave a validar unicidade")
    parser.add_argument("--check-all", action="store_true", help="Valida todos os .parquet de --data-dir")
    parser.add_argument("--data-dir", default="../bronze", help="Diretório com arquivos parquet")
    parser.add_argument("--report-out", default="quality_report.json")
    args = parser.parse_args()

    reports = []

    if args.check_all:
        for fname in sorted(os.listdir(args.data_dir)):
            if fname.endswith(".parquet"):
                path = os.path.join(args.data_dir, fname)
                logger.info(f"Validando {fname}...")
                reports.append(run_quality_check(path))
    elif args.input:
        reports.append(run_quality_check(args.input, key=args.key))
    else:
        parser.error("Use --input <arquivo> ou --check-all --data-dir <pasta>")

    for r in reports:
        status = "⚠️  COM ISSUES" if r.issues else "✅ OK"
        logger.info(f"{r.table_name}: {r.n_rows} linhas | {r.n_duplicates} duplicadas | {status}")

    with open(args.report_out, "w", encoding="utf-8") as f:
        json.dump([r.to_dict() for r in reports], f, indent=2, ensure_ascii=False)
    logger.info(f"Relatório salvo em {args.report_out}")


if __name__ == "__main__":
    main()
