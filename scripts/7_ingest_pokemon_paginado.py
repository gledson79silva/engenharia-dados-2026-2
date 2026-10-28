"""
Pipeline de ingestão da PokeAPI para a camada Bronze.
 
Fonte: https://pokeapi.co/api/v2/pokemon
Destino: MinIO (bucket bronze) — formato Parquet com compressão Snappy
Tipo de dado: Entidade com relacionamentos (pokémon → habilidades, tipos, estatísticas)
A PokeAPI demonstra dois padrões fundamentais de ingestão:
  1. Paginação: o endpoint lista retorna 20 itens por página
  2. Enriquecimento: cada item da lista requer uma segunda requisição
     para obter os detalhes completos (padrão "list + detail")
"""
import course_config  # carrega .env e mantém o estado dlt neste projeto

import truststore
truststore.inject_into_ssl()

import dlt
import requests
import time
import logging
from datetime import datetime, timezone
from typing import Iterator
 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
 
import os
os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = "s3://bronze"
os.environ["DESTINATION__FILESYSTEM__CREDENTIALS__AWS_ACCESS_KEY_ID"] = "minioadmin"
os.environ["DESTINATION__FILESYSTEM__CREDENTIALS__AWS_SECRET_ACCESS_KEY"] = "minioadmin123"
os.environ["DESTINATION__FILESYSTEM__CREDENTIALS__ENDPOINT_URL"] = "http://localhost:9000"
os.environ["DESTINATION__FILESYSTEM__CREDENTIALS__REGION_NAME"] = "us-east-1"
 
# Configurações da API
BASE_URL = "https://pokeapi.co/api/v2"
LIMITE_POR_PAGINA = 100   # Máximo permitido pela PokeAPI
TOTAL_POKEMON = 300       # Limitar a 300 para a atividade (existem ~1300 no total)
DELAY_ENTRE_REQUESTS = 0.1  # Segundos entre requisições (respeitar rate limit)

def buscar_detalhes_pokemon(url: str, session: requests.Session) -> dict:
    """
    Busca os detalhes completos de um pokémon pelo URL do recurso.
    Implementa retry simples em caso de falha temporária.
    """
    for tentativa in range(3):
        try:
            resposta = session.get(url, timeout=10)
            resposta.raise_for_status()
            return resposta.json()
        except requests.RequestException as e:
            if tentativa == 2:
                raise
            logger.warning(f"Tentativa {tentativa + 1} falhou para {url}: {e}. Tentando novamente...")
            time.sleep(1)

def normalizar_pokemon(dados: dict, timestamp: str) -> dict:
    """
    Normaliza os dados brutos de um pokémon, achatando estruturas aninhadas
    que seriam difíceis de trabalhar diretamente.
    Na camada Bronze, mantemos o dado original E adicionamos campos normalizados
    para facilitar o uso downstream.
    """
    return {
        # Identificadores
        "id": dados["id"],
        "nome": dados["name"],
        "nome_especie": dados.get("species", {}).get("name"),
 
        # Atributos físicos
        "altura_dm": dados["height"],       # Em decímetros
        "peso_hg": dados["weight"],         # Em hectogramas
        "experiencia_base": dados["base_experience"],
 
        # Tipos (até 2 tipos por pokémon)
        "tipo_primario": dados["types"][0]["type"]["name"] if dados["types"] else None,
        "tipo_secundario": dados["types"][1]["type"]["name"] if len(dados["types"]) > 1 else None,
 
        # Estatísticas base (estrutura original preservada como lista)
        "estatisticas": [
            {"nome": s["stat"]["name"], "valor_base": s["base_stat"]}
            for s in dados["stats"]
        ],
 
        # Habilidades
        "habilidades": [
            {"nome": a["ability"]["name"], "oculta": a["is_hidden"]}
            for a in dados["abilities"]
        ],
 
        # Sprites (imagens)
        "sprite_url": dados.get("sprites", {}).get("front_default"),
 
        # Metadados de rastreabilidade (obrigatórios na camada Bronze)
        "_fonte": "pokeapi_v2",
        "_url_recurso": f"{BASE_URL}/pokemon/{dados['id']}",
        "_ingerido_em": timestamp,
        "_versao_api": "v2",
    }

@dlt.resource(
    name="pokemon",
    write_disposition="append",
    primary_key="id",
)

def extrair_pokemon(total: int = TOTAL_POKEMON) -> Iterator[dict]:
    """
    Extrai pokémons da PokeAPI com paginação automática.
 
    Fluxo de extração:
      1. GET /pokemon?limit=100&offset=0  → lista de 100 pokémons (apenas nome + URL)
      2. Para cada pokémon da lista: GET /pokemon/{id} → detalhes completos
      3. Repetir com offset=100, 200, ... até atingir o total desejado
    """
    timestamp_ingestao = datetime.now(timezone.utc).isoformat()
    session = requests.Session()
    session.headers.update({"User-Agent": "DataEngineeringCourse/1.0"})
 
    total_extraidos = 0
    offset = 0
 
    logger.info(f"Iniciando extração de {total} pokémons da PokeAPI")
 
    while total_extraidos < total:
        # Calcular quantos registros buscar nesta página
        limite_pagina = min(LIMITE_POR_PAGINA, total - total_extraidos)
 
        # Requisição à página de listagem
        url_lista = f"{BASE_URL}/pokemon?limit={limite_pagina}&offset={offset}"
        logger.info(f"Buscando lista: offset={offset}, limite={limite_pagina}")
 
        resposta_lista = session.get(url_lista, timeout=15)
        resposta_lista.raise_for_status()
        dados_lista = resposta_lista.json()
 
        resultados = dados_lista.get("results", [])
        if not resultados:
            logger.info("Sem mais resultados. Encerrando paginação.")
            break
 
        # Para cada item da lista, buscar os detalhes completos
        for item in resultados:
            dados_detalhes = buscar_detalhes_pokemon(item["url"], session)
            pokemon_normalizado = normalizar_pokemon(dados_detalhes, timestamp_ingestao)
            yield pokemon_normalizado
            total_extraidos += 1
 
            # Respeitar o rate limit da API (gratuita e sem autenticação)
            time.sleep(DELAY_ENTRE_REQUESTS)
 
        offset += limite_pagina
        logger.info(f"Progresso: {total_extraidos}/{total} pokémons extraídos")
 
    logger.info(f"Extração concluída. Total: {total_extraidos} pokémons")
 
def executar_pipeline():
    """Configura e executa o pipeline dlt para a PokeAPI."""
 
    pipeline = dlt.pipeline(
        pipeline_name="pokeapi_bronze",
        destination="filesystem",
        dataset_name="pokeapi_dlt",
    )
 
    logger.info("Iniciando pipeline: PokeAPI → MinIO Bronze")
    inicio = datetime.now()
 
    info = pipeline.run(
        extrair_pokemon(),
        loader_file_format="parquet",
    )
 
    duracao = (datetime.now() - inicio).total_seconds()
 
    print("\n" + "=" * 60)
    print("  RELATÓRIO DE EXECUÇÃO — PokeAPI")
    print("=" * 60)
    print(f"  Status:        {'✅ Sucesso' if not info.has_failed_jobs else '❌ Falha'}")
    print(f"  Duração:       {duracao:.2f}s")
    print(f"  Destino:       s3://bronze/pokeapi_dlt/")
    print(f"  Formato:       Parquet + Snappy")
    print()
    print("  Detalhes da carga:")
    print(info)
    print("=" * 60)
 
    return info

if __name__ == "__main__":
    executar_pipeline()
