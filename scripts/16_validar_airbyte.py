"""Confere a carga Airbyte no MinIO: linhas, países únicos e metadados."""
import argparse
import io
import json

import pyarrow.parquet as pq
from course_config import ROOT, list_objects, s3_client


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--esperado", type=int, default=254,
                        help="Total de países informado pela API nesta carga")
    args = parser.parse_args()
    prefix = "airbyte/rest-countries-api/countries/"
    files = [o for o in list_objects("bronze", prefix) if o["Key"].endswith(".parquet")]
    if not files:
        raise FileNotFoundError(f"Nenhum Parquet em bronze/{prefix}")
    client = s3_client()
    rows = []
    evidence = []
    for obj in files:
        body = client.get_object(Bucket="bronze", Key=obj["Key"])["Body"].read()
        parquet = pq.ParquetFile(io.BytesIO(body))
        table = parquet.read()
        assert {"_airbyte_raw_id", "_airbyte_extracted_at", "_airbyte_meta"} <= set(table.column_names)
        rows.extend(table.to_pylist())
        evidence.append({"arquivo": obj["Key"], "registros": table.num_rows,
                         "bytes": len(body), "compressao": parquet.metadata.row_group(0).column(0).compression})
    assert len(rows) == args.esperado, (len(rows), args.esperado)
    codes = [json.loads(r["codes"]) if isinstance(r["codes"], str) else r["codes"] for r in rows]
    ids = [c.get("alpha_2") for c in codes if c and c.get("alpha_2")]
    assert len(set(ids)) == len(ids), "Códigos ISO preenchidos estão duplicados"
    names = [json.loads(r["names"]) if isinstance(r["names"], str) else r["names"] for r in rows]
    names = [n["common"] for n in names]
    assert all(names) and len(set(names)) == args.esperado
    assert len({r["_airbyte_raw_id"] for r in rows}) == args.esperado
    assert all(r["_airbyte_extracted_at"] is not None for r in rows)
    result = {"registros": len(rows), "paises_unicos": len(set(names)),
              "codigos_iso_preenchidos": len(ids), "sem_codigo_iso": len(rows) - len(ids),
              "metadados": "OK", "arquivos": evidence}
    output = ROOT / "data/validation"
    output.mkdir(parents=True, exist_ok=True)
    (output / "airbyte_parquet.json").write_text(json.dumps(result, indent=2, ensure_ascii=False))
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
