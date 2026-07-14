
# Simulação de eventos em tempo real

import argparse
import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("streaming_producer")


SAMPLE_MUNICIPIOS = [
    ("Osasco", "SP"), ("Campinas", "SP"), ("Salvador", "BA"),
    ("Recife", "PE"), ("Manaus", "AM"), ("Curitiba", "PR"),
    ("Fortaleza", "CE"), ("Belo Horizonte", "MG"), ("Porto Alegre", "RS"),
    ("Goiânia", "GO"),
]

EVENT_TYPES = ["atualizacao_indicador", "nova_medicao", "atualizacao_meta"]


def generate_event() -> dict:
    municipio, uf = random.choice(SAMPLE_MUNICIPIOS)
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": random.choice(EVENT_TYPES),
        "municipio": municipio,
        "uf": uf,
        "indicador": round(random.uniform(35.0, 85.0), 1),
        "data": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def write_local(events: list, out_dir: str) -> str:
    os.makedirs(out_dir, exist_ok=True)
    batch_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    path = os.path.join(out_dir, f"batch_{batch_id}.jsonl")
    with open(path, "w", encoding="utf-8") as f:
        for ev in events:
            f.write(json.dumps(ev, ensure_ascii=False) + "\n")
    return path


def write_s3(events: list, bucket: str, prefix: str = "streaming"):
    import boto3

    s3 = boto3.client("s3")
    batch_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    key = f"{prefix}/batch_{batch_id}.jsonl"
    body = "\n".join(json.dumps(ev, ensure_ascii=False) for ev in events)
    s3.put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"))
    return f"s3://{bucket}/{key}"


def main():
    parser = argparse.ArgumentParser(description="Simulador de streaming - Indicador Criança Alfabetizada")
    parser.add_argument("--n-events", type=int, default=20, help="Número total de eventos a gerar")
    parser.add_argument("--batch-size", type=int, default=5, help="Eventos por micro-lote")
    parser.add_argument("--interval", type=float, default=2.0, help="Segundos entre micro-lotes")
    parser.add_argument("--sink", choices=["local", "s3"], default="local")
    parser.add_argument("--s3-bucket", default=None)
    parser.add_argument("--out-dir", default=os.path.join(os.path.dirname(__file__), "events"))
    args = parser.parse_args()

    total_sent = 0
    logger.info(f"Iniciando simulação de streaming: {args.n_events} eventos, sink={args.sink}")

    while total_sent < args.n_events:
        batch = [generate_event() for _ in range(min(args.batch_size, args.n_events - total_sent))]

        if args.sink == "local":
            path = write_local(batch, args.out_dir)
            logger.info(f"Micro-lote gravado: {path} ({len(batch)} eventos)")
        else:
            if not args.s3_bucket:
                raise ValueError("--s3-bucket é obrigatório quando --sink=s3")
            uri = write_s3(batch, args.s3_bucket)
            logger.info(f"Micro-lote enviado: {uri} ({len(batch)} eventos)")

        total_sent += len(batch)
        if total_sent < args.n_events:
            time.sleep(args.interval)

    logger.info(f"Simulação concluída: {total_sent} eventos enviados.")


if __name__ == "__main__":
    main()
