"""Lê uma partição Bronze explícita em UTC, sem varrer seu conteúdo histórico."""
import argparse
import io
from datetime import date, datetime, timezone
import pandas as pd
from course_config import s3_client, list_objects


def ler_particao_especifica(sistema, entidade, ano, mes, dia):
    prefixo = f"{sistema}/{entidade}/ano={ano}/mes={mes:02d}/dia={dia:02d}/"
    client = s3_client()
    arquivos = [o["Key"] for o in list_objects("bronze", prefixo)
                if o["Key"].endswith(".parquet")]
    if not arquivos:
        raise FileNotFoundError(f"Nenhum Parquet em bronze/{prefixo}")
    frames = [pd.read_parquet(io.BytesIO(client.get_object(Bucket="bronze", Key=k)["Body"].read()))
              for k in arquivos]
    print(f"Partição: {prefixo}; arquivos lidos: {len(arquivos)}")
    return pd.concat(frames, ignore_index=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sistema", default="restcountries_api")
    parser.add_argument("--entidade", default="countries")
    parser.add_argument("--data", type=date.fromisoformat,
                        default=datetime.now(timezone.utc).date(), help="AAAA-MM-DD em UTC")
    args = parser.parse_args()
    df = ler_particao_especifica(args.sistema, args.entidade,
                                args.data.year, args.data.month, args.data.day)
    assert not df.empty
    assert {"_fonte", "_ingerido_em", "_versao_schema", "_particao_data"} <= set(df.columns)
    assert set(df["_particao_data"]) == {args.data.isoformat()}
    print(f"Registros: {len(df)}; metadados e data da partição: OK")
    print(df.head().to_string(index=False))


if __name__ == "__main__":
    main()
