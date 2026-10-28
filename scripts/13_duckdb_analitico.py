"""Complemento da Aula 3: SQL sobre Parquet no MinIO, comparado com Spark."""
import json
import os
from urllib.parse import urlparse

import duckdb
from course_config import ROOT


def main():
    output = ROOT / "data" / "validation"
    output.mkdir(parents=True, exist_ok=True)
    extensions = ROOT / ".local" / "duckdb-extensions"
    extensions.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(config={"extension_directory": str(extensions), "threads": "2"})
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    endpoint = urlparse(os.getenv("AWS_ENDPOINT_URL", "http://localhost:9000"))
    # As credenciais ficam apenas na conexão em memória, sem secret persistente.
    literal = lambda value: "'" + value.replace("'", "''") + "'"
    con.execute(f"""CREATE SECRET minio (
        TYPE S3,
        KEY_ID {literal(os.getenv('AWS_ACCESS_KEY_ID', 'minioadmin'))},
        SECRET {literal(os.getenv('AWS_SECRET_ACCESS_KEY', 'minioadmin123'))},
        ENDPOINT {literal(endpoint.netloc)}, URL_STYLE 'path',
        USE_SSL {'true' if endpoint.scheme == 'https' else 'false'}, REGION 'us-east-1'
    )""")
    con.execute("""CREATE VIEW pedidos AS
        SELECT * FROM read_parquet('s3://bronze/ecommerce_sintetico/**/*.parquet', hive_partitioning=true)""")
    total, unique_ids = con.execute("SELECT count(*), count(DISTINCT id_pedido) FROM pedidos").fetchone()
    assert total == unique_ids == 1_000_000
    concluidos = con.execute("SELECT count(*) FROM pedidos WHERE status_pedido='concluido'").fetchone()[0]
    premium = con.execute("SELECT count(*) FROM pedidos WHERE status_pedido='concluido' AND valor_total>500").fetchone()[0]
    receita = con.execute("""SELECT categoria, regiao, count(*) total_pedidos,
        round(sum(valor_total),2) receita_total, round(avg(valor_total),2) ticket_medio,
        count(DISTINCT id_cliente) clientes_unicos
        FROM pedidos WHERE status_pedido='concluido'
        GROUP BY categoria,regiao ORDER BY categoria,regiao""").df()
    processed = con.execute("""SELECT count(*) FROM read_parquet(
        's3://bronze/ecommerce_processado/**/*.parquet', hive_partitioning=true)""").fetchone()[0]
    assert concluidos == processed
    # Referência independente produzida no notebook Spark da mesma execução.
    import pandas as pd
    from pandas.testing import assert_frame_equal
    spark = pd.read_json(output / "spark_receita.json").sort_values(["categoria", "regiao"]).reset_index(drop=True)
    assert_frame_equal(receita, spark[receita.columns], check_dtype=False, rtol=0, atol=0.011)
    plano = con.execute("EXPLAIN SELECT categoria, sum(valor_total) FROM pedidos WHERE status_pedido='concluido' GROUP BY categoria").fetchone()[1]
    (output / "duckdb_plano.txt").write_text(plano)
    receita.to_json(output / "duckdb_receita.json", orient="records", force_ascii=False, indent=2)
    result = {"total": total, "ids_unicos": unique_ids, "concluidos": concluidos,
              "premium": premium, "grupos": len(receita), "comparacao_spark": "OK"}
    (output / "duckdb_resultado.json").write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))
    print(receita.to_string(index=False))
    con.close()


if __name__ == "__main__":
    main()
