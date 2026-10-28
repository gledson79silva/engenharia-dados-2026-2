"""Valida manifestos, hashes, contagens, metadados e escrita sem sobrescrita."""
import argparse
import hashlib
import io
import json
from datetime import datetime, timezone

import pandas as pd
import pyarrow.parquet as pq

from bronze_writer import BronzeWriter
from course_config import list_objects, s3_client


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--testar-escrita", action="store_true")
    args = parser.parse_args()
    client = s3_client()
    if args.testar_escrita:
        writer = BronzeWriter()
        timestamp = datetime.now(timezone.utc)
        resultados = [writer.gravar(pd.DataFrame({"id": [1, 2]}), "_testes",
                                    "append_only", timestamp=timestamp) for _ in range(2)]
        assert resultados[0]["caminho_objeto"] != resultados[1]["caminho_objeto"]
        for resultado in resultados:
            writer.registrar_manifesto({"pipeline": "teste_append_only",
                                       "execucao_em": timestamp.isoformat(), "resultado": resultado})

    manifestos = [o for o in list_objects("bronze", "_manifesto/")
                  if o["Key"].endswith(".json")]
    if not manifestos:
        raise RuntimeError("Nenhum manifesto encontrado")
    pipelines = set()
    for obj in manifestos:
        manifesto = json.loads(client.get_object(Bucket="bronze", Key=obj["Key"])["Body"].read())
        pipelines.add(manifesto["pipeline"])
        resultado = manifesto["resultado"]
        key = resultado["caminho_objeto"].removeprefix("s3://bronze/")
        body = client.get_object(Bucket="bronze", Key=key)["Body"].read()
        assert hashlib.md5(body).hexdigest() == resultado["checksum_md5"], key
        assert len(body) == resultado["tamanho_bytes"], key
        tabela = pq.read_table(io.BytesIO(body))
        assert tabela.num_rows == resultado["registros"], key
        assert {"_fonte", "_ingerido_em", "_versao_schema", "_particao_data"} <= set(tabela.column_names)
        if manifesto["pipeline"] == "pokeapi_bronze":
            assert tabela.num_rows == 50
            assert len(set(tabela.column("id").to_pylist())) == 50
        print(f"OK: {manifesto['pipeline']}: {tabela.num_rows} linhas, MD5 e tamanho conferidos")
    print(f"Manifestos validados: {len(manifestos)}; pipelines: {sorted(pipelines)}")


if __name__ == "__main__":
    main()
