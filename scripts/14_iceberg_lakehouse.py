"""Complemento Aula 3: catálogo persistente, commits, schema e time travel."""
import io
import json
import os
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.types import StringType

from course_config import ROOT, list_objects, s3_client


def main():
    state = ROOT / "data" / "lakehouse"
    state.mkdir(parents=True, exist_ok=True)
    catalog = SqlCatalog("aula3", **{
        "uri": f"sqlite:///{state / 'catalog.db'}",
        "warehouse": "s3://silver/lakehouse",
        "s3.endpoint": os.getenv("AWS_ENDPOINT_URL", "http://localhost:9000"),
        "s3.access-key-id": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        "s3.secret-access-key": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123"),
        "s3.region": "us-east-1",
        "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
    })
    catalog.create_namespace_if_not_exists("aula3")
    client = s3_client()
    objects = [o for o in list_objects("bronze", "ecommerce_sintetico/") if o["Key"].endswith(".parquet")]
    columns = ["id_pedido", "categoria", "regiao", "valor_total", "status_pedido", "data_pedido"]
    tables = [pq.read_table(io.BytesIO(client.get_object(Bucket="bronze", Key=o["Key"])["Body"].read()), columns=columns)
              for o in objects]
    data = pa.concat_tables(tables)
    schema = pa.schema([pa.field(f.name, pa.string() if pa.types.is_large_string(f.type) else f.type) for f in data.schema])
    data = data.cast(schema)
    assert data.num_rows == 1_000_000
    identifier = f"aula3.pedidos_{uuid4().hex}"
    table = catalog.create_table(identifier, schema=data.schema)
    table.append(data)
    snapshot_initial = table.current_snapshot().snapshot_id
    reader = catalog.load_table(identifier)
    assert reader.scan().count() == data.num_rows
    with table.update_schema() as update:
        update.add_column("versao_laboratorio", StringType())
    row = data.slice(0, 1).to_pylist()[0]
    row["id_pedido"] = 1_000_001
    row["versao_laboratorio"] = "demonstracao"
    new_schema = table.schema().as_arrow()
    table.append(pa.Table.from_pylist([row], schema=new_schema))
    snapshot_current = table.current_snapshot().snapshot_id
    assert snapshot_initial != snapshot_current
    # Um leitor mantém a visão anterior até atualizar seus metadados.
    assert reader.scan().count() == 1_000_000
    reader.refresh()
    assert reader.scan().count() == 1_000_001
    assert table.scan(snapshot_id=snapshot_initial).count() == 1_000_000
    assert table.scan(row_filter="id_pedido == 1000001").to_arrow()["versao_laboratorio"].to_pylist() == ["demonstracao"]
    assert catalog.load_table(identifier).scan().count() == 1_000_001
    result = {"tabela": identifier, "catalogo": str(state / "catalog.db"),
              "metadata_location": table.metadata_location,
              "snapshot_inicial": snapshot_initial, "snapshot_atual": snapshot_current,
              "linhas_iniciais": 1_000_000, "linhas_atuais": 1_000_001,
              "schema_evolution": "OK", "time_travel": "OK", "leitor_isolado": "OK"}
    (state / "ultima_execucao.json").write_text(json.dumps(result, indent=2, ensure_ascii=False))
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
