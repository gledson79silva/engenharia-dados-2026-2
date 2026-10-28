"""Valida a tabela Iceberg existente sem criar tabelas, snapshots ou cargas."""
import json
import os
import sqlite3

import pyarrow.compute as pc
from pyiceberg.table import StaticTable

from course_config import ROOT


def main():
    state = ROOT / "data/lakehouse"
    previous = json.loads((state / "ultima_execucao.json").read_text())
    namespace, name = previous["tabela"].rsplit(".", 1)
    # Abrir o catálogo somente para leitura; nenhum DDL é executado.
    with sqlite3.connect(f"file:{state / 'catalog.db'}?mode=ro", uri=True) as db:
        row = db.execute(
            "SELECT metadata_location FROM iceberg_tables "
            "WHERE catalog_name = ? AND table_namespace = ? AND table_name = ?",
            ("aula3", namespace, name),
        ).fetchone()
    if row is None:
        raise RuntimeError("A tabela registrada na última execução não está no catálogo")
    table = StaticTable.from_metadata(row[0], properties={
        "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        "s3.endpoint": os.getenv("AWS_ENDPOINT_URL", "http://localhost:9000"),
        "s3.access-key-id": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        "s3.secret-access-key": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123"),
        "s3.region": "us-east-1",
    })
    snapshot_initial = previous["snapshot_inicial"]
    snapshot_current = table.current_snapshot().snapshot_id
    assert snapshot_current == previous["snapshot_atual"]
    initial = table.scan(snapshot_id=snapshot_initial, selected_fields=("id_pedido",)).to_arrow()
    current = table.scan(selected_fields=("id_pedido",)).to_arrow()
    assert initial.num_rows == previous["linhas_iniciais"] == 1_000_000
    assert current.num_rows == previous["linhas_atuais"] == 1_000_001
    assert pc.count_distinct(initial["id_pedido"]).as_py() == initial.num_rows
    assert pc.count_distinct(current["id_pedido"]).as_py() == current.num_rows
    assert len(table.metadata.schemas) == 2
    assert str(table.schema().find_field("versao_laboratorio").field_type) == "string"
    demonstration = table.scan(
        row_filter="id_pedido == 1000001", selected_fields=("versao_laboratorio",)
    ).to_arrow()
    assert demonstration["versao_laboratorio"].to_pylist() == ["demonstracao"]
    result = {
        "tabela": previous["tabela"], "snapshot_inicial": snapshot_initial,
        "snapshot_atual": snapshot_current,
        "linhas_iniciais": initial.num_rows, "linhas_atuais": current.num_rows,
        "ids_unicos": "OK", "schemas": len(table.metadata.schemas),
        "schema_evolution": "OK", "time_travel": "OK",
        "novas_tabelas": 0, "novos_snapshots": 0, "status": "OK",
    }
    output = ROOT / "data/validation"
    output.mkdir(parents=True, exist_ok=True)
    (output / "iceberg_revalidacao.json").write_text(json.dumps(result, indent=2, ensure_ascii=False))
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
