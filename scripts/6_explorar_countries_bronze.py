"""
Exploração dos dados da REST Countries API na camada Bronze.
Demonstra como ler dados Parquet diretamente do MinIO com Pandas.
"""
 
from course_config import read_dlt_table

df = read_dlt_table("rest_countries", "countries")
if not df.empty:
    print(f"\nShape do dataset: {df.shape}")
    print(f"\nColunas disponíveis:\n{list(df.columns)}")
    print(f"\nDistribuição por região:")
    print(df["region"].value_counts())
    print(f"\nTop 10 países por população:")
    print(
        df[["names__common", "population", "region"]]
        .sort_values("population", ascending=False)
        .head(10)
        .to_string(index=False)
    )

