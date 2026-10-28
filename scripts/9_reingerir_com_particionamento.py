# scripts/9_reingerir_com_particionamento.py
"""
Demonstração do uso da classe BronzeWriter para ingestão padronizada.
Reingestão dos dados de Countries e Pokémon com particionamento correto.

REST Countries API v5 (a v3.1 foi descontinuada):
  - Endereço: https://api.restcountries.com/countries/v5
  - Autenticação obrigatória: cabeçalho "Authorization: Bearer <API_KEY>".
    A chave é lida da variável de ambiente RESTCOUNTRIES_API_KEY:
        PowerShell:   $env:RESTCOUNTRIES_API_KEY = "sua_chave"
        Linux/macOS:  export RESTCOUNTRIES_API_KEY="sua_chave"
  - Resposta embrulhada: registros em data.objects, paginação em data.meta.
  - Paginação por limit/offset (máximo de 100 por página no plano gratuito).
  - Seleção de campos com "response_fields" (caminhos com ponto).
"""

import course_config  # carrega .env e mantém o estado dlt neste projeto

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

# Usa o repositório de certificados do sistema operacional (redes com inspeção HTTPS).
# Opcional: se o pacote não estiver instalado, segue com os certificados padrão do Python.
try:
    import truststore
    truststore.inject_into_ssl()
except ImportError:
    pass

import requests
import pandas as pd
import time
from datetime import datetime, timezone
from bronze_writer import BronzeWriter


# ── Configuração da REST Countries API v5 ────────────────────────────────────
RESTCOUNTRIES_BASE_URL = "https://api.restcountries.com"   # atenção: com "api."
RESTCOUNTRIES_ENDPOINT = "/countries/v5"
RESTCOUNTRIES_PAGINA = 100  # máximo permitido no plano gratuito

# Equivalentes v5 dos campos usados na v3.1
CAMPOS_COUNTRIES = [
    "codes.alpha_2",                    # cca2
    "names.common", "names.official",   # name.common / name.official
    "region",
    "continents",
    "capitals",                         # capital
    "currencies",
    "population",
    "area.kilometers",                  # area
    "coordinates.lat", "coordinates.lng",  # latlng
]


def obter_api_key() -> str:
    """Lê a API key da variável de ambiente (nunca escreva a chave no código)."""
    api_key = os.getenv("RESTCOUNTRIES_API_KEY")
    if not api_key:
        raise RuntimeError(
            "Variável de ambiente RESTCOUNTRIES_API_KEY não definida.\n"
            "Obtenha sua chave em https://restcountries.com e defina-a antes de executar:\n"
            '  PowerShell: $env:RESTCOUNTRIES_API_KEY = "sua_chave"\n'
            '  Linux/macOS: export RESTCOUNTRIES_API_KEY="sua_chave"'
        )
    return api_key


def extrair_paises_v5() -> list[dict]:
    """Extrai todos os países da REST Countries API v5, percorrendo as páginas."""
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {obter_api_key()}"})

    paises, offset = [], 0
    while True:
        resposta = session.get(
            f"{RESTCOUNTRIES_BASE_URL}{RESTCOUNTRIES_ENDPOINT}",
            params={
                "response_fields": ",".join(CAMPOS_COUNTRIES),
                "limit": RESTCOUNTRIES_PAGINA,
                "offset": offset,
            },
            timeout=30,
        )
        resposta.raise_for_status()
        dados = resposta.json()["data"]
        pagina, meta = dados["objects"], dados["meta"]

        paises.extend(pagina)
        print(f"  Página offset={offset}: {len(pagina)} países (acumulado: {len(paises)}/{meta['total']})")

        offset += RESTCOUNTRIES_PAGINA
        if not pagina or not meta.get("more", offset < meta["total"]):
            break
        time.sleep(0.5)  # limite da API: 20 requisições a cada 10 segundos

    if not paises or len(paises) != meta["total"]:
        raise RuntimeError("A contagem de países difere do total informado pela API")
    return paises


def capital_principal(capitais: list | None) -> str | None:
    """Retorna a capital principal (attributes.primary) ou, na falta dela, a primeira da lista."""
    if not capitais:
        return None
    principal = next(
        (c for c in capitais if (c.get("attributes") or {}).get("primary")),
        capitais[0],
    )
    return principal.get("name")


def main():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--somente", choices=["countries", "pokemon"])
    args = parser.parse_args()
    writer = BronzeWriter()


    # ── 1. Reingestão da REST Countries API ──────────────────────────────────────
    print("\n" + "─" * 55)
    print("  Reingestão: REST Countries API v5 → Bronze (particionado)")
    print("─" * 55)

    if args.somente != "pokemon":
        paises_raw = extrair_paises_v5()

        # Normalizar os campos aninhados mantendo os MESMOS nomes de colunas da versão
        # anterior, para não quebrar os scripts e consultas que leem esta entidade.
        paises_normalizados = []
        for p in paises_raw:
            codes = p.get("codes") or {}
            names = p.get("names") or {}
            area = p.get("area") or {}
            coords = p.get("coordinates") or {}
            paises_normalizados.append({
                "cca2": codes.get("alpha_2"),
                "nome_comum": names.get("common"),
                "nome_oficial": names.get("official"),
                "regiao": p.get("region"),
                "continentes": p.get("continents"),
                "moedas": p.get("currencies"),
                "populacao": p.get("population"),
                "area_km2": area.get("kilometers"),
                "latlng": [coords.get("lat"), coords.get("lng")] if coords else None,
                "capital": capital_principal(p.get("capitals")),
            })

        df_countries = pd.DataFrame(paises_normalizados)
        assert df_countries["nome_comum"].notna().all()
        assert df_countries["nome_comum"].is_unique
        codigos_validos = df_countries.loc[df_countries["cca2"].fillna("").ne(""), "cca2"]
        assert codigos_validos.is_unique  # Algumas entidades da API não têm código ISO.

        resultado_countries = writer.gravar(
            df=df_countries,
            sistema_origem="restcountries_api",
            entidade="countries",
            versao_schema="2.0",  # origem mudou para a API v5 (mesmas colunas, nova procedência)
        )
        # Registrar manifesto
        writer.registrar_manifesto({
            "pipeline": "rest_countries_bronze",
            "api_versao": "v5",
            "execucao_em": datetime.now(timezone.utc).isoformat(),
            "resultado": resultado_countries,
        })

        print(f"  ✅ {resultado_countries['registros']} países gravados")
        print(f"  📍 {resultado_countries['caminho_objeto']}")

    # ── 2. Reingestão da PokeAPI (primeiros 50 pokémons) ─────────────────────────
    print("\n" + "─" * 55)
    print("  Reingestão: PokeAPI → Bronze (particionado)")
    print("─" * 55)

    if args.somente != "countries":
        BASE_URL = "https://pokeapi.co/api/v2"

        pokemon_lista = []

        # Buscar lista dos primeiros 50 pokémons
        resp_lista = requests.get(f"{BASE_URL}/pokemon?limit=50&offset=0", timeout=15)
        resp_lista.raise_for_status()

        for i, item in enumerate(resp_lista.json()["results"]):
            resp_detalhe = requests.get(item["url"], timeout=10)
            resp_detalhe.raise_for_status()
            d = resp_detalhe.json()

            pokemon_lista.append({
                "id": d["id"],
                "nome": d["name"],
                "altura_dm": d["height"],
                "peso_hg": d["weight"],
                "experiencia_base": d.get("base_experience"),
                "tipo_primario": d["types"][0]["type"]["name"] if d["types"] else None,
                "tipo_secundario": d["types"][1]["type"]["name"] if len(d["types"]) > 1 else None,
                "hp_base": next((s["base_stat"] for s in d["stats"] if s["stat"]["name"] == "hp"), None),
                "ataque_base": next((s["base_stat"] for s in d["stats"] if s["stat"]["name"] == "attack"), None),
                "defesa_base": next((s["base_stat"] for s in d["stats"] if s["stat"]["name"] == "defense"), None),
                "sprite_url": d.get("sprites", {}).get("front_default"),
            })

            if (i + 1) % 10 == 0:
                print(f"  Extraídos: {i + 1}/50 pokémons...")
            time.sleep(0.1)

        df_pokemon = pd.DataFrame(pokemon_lista)

        resultado_pokemon = writer.gravar(
            df=df_pokemon,
            sistema_origem="pokeapi",
            entidade="pokemon",
            versao_schema="1.0",
        )

        writer.registrar_manifesto({
            "pipeline": "pokeapi_bronze",
            "execucao_em": datetime.now(timezone.utc).isoformat(),
            "resultado": resultado_pokemon,
        })

        print(f"  ✅ {resultado_pokemon['registros']} pokémons gravados")
        print(f"  📍 {resultado_pokemon['caminho_objeto']}")

    # ── 3. Relatório final ────────────────────────────────────────────────────────
    print("\n" + "=" * 55)
    print("  RELATÓRIO FINAL — CAMADA BRONZE")
    print("=" * 55)

    for sistema, entidade in [("restcountries_api", "countries"), ("pokeapi", "pokemon")]:
        particoes = writer.listar_particoes(sistema, entidade)
        total_bytes = sum(p["tamanho_bytes"] for p in particoes)
        print(f"\n  [{sistema}/{entidade}]")
        print(f"    Arquivos:  {len(particoes)}")
        print(f"    Tamanho:   {total_bytes / 1024:.1f} KB")
        for p in particoes:
            print(f"    📄 {p['caminho']}")

    print("\n  ✅ Camada Bronze organizada com particionamento correto!")
    print("  Acesse: http://localhost:9001/browser/bronze")


if __name__ == "__main__":
    main()
