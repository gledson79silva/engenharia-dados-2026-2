# scripts/comparar_pipelines.py
"""
Compara os dados ingeridos pelos dois pipelines (Countries e PokeAPI)
e demonstra as diferenças de estrutura entre dados cadastrais
e dados com relacionamentos complexos.
"""
 
from course_config import read_dlt_table


print("=" * 65)
print("  COMPARATIVO DOS DADOS INGERIDOS NA CAMADA BRONZE")
print("=" * 65)
 
# REST Countries
df_countries = read_dlt_table("rest_countries", "countries")
print(f"\n🌍 REST Countries API")
print(f"   Registros: {len(df_countries):,}")
print(f"   Colunas:   {len(df_countries.columns)}")
print(f"   Regiões:   {df_countries['region'].nunique()} únicas")
print(f"   Metadados: {[c for c in df_countries.columns if c.startswith('_')]}")

# PokeAPI
df_pokemon = read_dlt_table("pokeapi_dlt", "pokemon")
print(f"\n🎮 PokeAPI")
print(f"   Registros: {len(df_pokemon):,}")
print(f"   Colunas:   {len(df_pokemon.columns)}")
print(f"   Tipos únicos: {df_pokemon['tipo_primario'].nunique()}")
print(f"   Metadados: {[c for c in df_pokemon.columns if c.startswith('_')]}")

print(f"\n{'─' * 65}")
print("  Distribuição de pokémons por tipo primário:")
print(df_pokemon["tipo_primario"].value_counts().head(10).to_string())
print("=" * 65)
