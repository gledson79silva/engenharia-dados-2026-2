"""
Pipeline de ingestão de dados utilizando o DLT (Data Load Tool) e integração com o MinIO.
Este script demonstra como configurar uma pipeline de ingestão de dados, utilizando o DLT para processar e armazenar dados em diferentes camadas (bronze, silver, gold) no MinIO.
As camadas representam diferentes estágios de processamento dos dados:
- bronze: Dados brutos ingeridos das fontes originais, sem transformação   
"""

import course_config  # carrega .env e mantém o estado dlt neste projeto

import logging
import os
from datetime import datetime, timezone
from typing import Iterator

import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import OffsetPaginator

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

logger = logging.getLogger(__name__)

# Configuração do MinIO - Destino da gravação
# O dlt usa variáveis de ambiente para configurar as nossas credenciais
# No ambiente local, podemos passá-las diretamente no script, mas em produção é recomendado usar variáveis de ambiente ou um arquivo de configuração seguro.
os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = "s3://bronze"
os.environ["DESTINATION__FILESYSTEM__CREDENTIALS__REGION_NAME"] = "us-east-1"
os.environ["DESTINATION__FILESYSTEM__CREDENTIALS__AWS_ACCESS_KEY_ID"] = "minioadmin"
os.environ["DESTINATION__FILESYSTEM__CREDENTIALS__AWS_SECRET_ACCESS_KEY"] = "minioadmin123"
os.environ["DESTINATION__FILESYSTEM__CREDENTIALS__ENDPOINT_URL"] = "http://localhost:9000"

# Configuração da rest countries api
BASE_URL = "https://api.restcountries.com"
ENDPOINT = "/countries/v5"
VERSAO_API = "v5"
TAMANHO_PAGINA = 100 # máximo no plano gratuito no uso da api

CAMPOS_SELECIONADOS = [
    "names.common", "names.official", "codes.alpha_2", "codes.alpha_3",
    "codes.ccn3","region","subregion", "continents",
    "capitals","languages","currencies", "population", "area.kilometers",
    "coordinates.lat", "coordinates.lng","tlds",
    "classification.sovereign",
    "classification.un_member",
    "classification.iso_status",
    "timezones","borders",
    "flag.emoji",
]

def obter_api_key() -> str:
    """
    Lê a API key da variável de ambiente RESTCOUNTRIES_API_KEY.
    A chave NUNCA deve ser escrita no código nem versionada no Git.
    """
    api_key = os.getenv("RESTCOUNTRIES_API_KEY")
    if not api_key:
        raise RuntimeError(
            "Variável de ambiente RESTCOUNTRIES_API_KEY não definida.\n"
            "Obtenha sua chave em https://restcountries.com e defina-a antes de executar:\n"
            '  PowerShell: $env:RESTCOUNTRIES_API_KEY = "sua_chave"\n'
            '  Linux/macOS: export RESTCOUNTRIES_API_KEY="sua_chave"'
        )
    return api_key


@dlt.resource(
    name="countries",
    write_disposition="append",   # Bronze preserva as execuções anteriores
    primary_key="codes__alpha_2",  # ISO 3166-1 alpha-2 (codes.alpha_2 achatado pelo dlt)
)
def extrair_paises() -> Iterator[list[dict]]:
    """
    Extrai todos os países da REST Countries API v5, página a página.
    O decorador @dlt.resource transforma esta função geradora
    em um recurso gerenciado pelo pipeline dlt.
    """
    cliente = RESTClient(
        base_url=BASE_URL,
        auth=BearerTokenAuth(token=obter_api_key()),  # Authorization: Bearer <chave>
        paginator=OffsetPaginator(
            limit=TAMANHO_PAGINA,
            offset_param="offset",
            limit_param="limit",
            total_path="data.meta.total",  # total de países informado pela API
        ),
        data_selector="data.objects",      # onde ficam os registros na resposta
    )
 
    logger.info(f"Iniciando extração de: {BASE_URL}{ENDPOINT}")
    timestamp_ingestao = datetime.now(timezone.utc).isoformat()
    total = 0
 
    for numero_pagina, pagina in enumerate(
        cliente.paginate(
            ENDPOINT,
            params={"response_fields": ",".join(CAMPOS_SELECIONADOS)},
        ),
        start=1,
    ):
        for pais in pagina:
            # Metadados de rastreabilidade (prática obrigatória na camada Bronze)
            pais["_fonte"] = "restcountries_v5"
            pais["_ingerido_em"] = timestamp_ingestao
            pais["_versao_api"] = VERSAO_API
 
        total += len(pagina)
        logger.info(f"Página {numero_pagina}: {len(pagina)} países (acumulado: {total})")
        yield pagina  # enviar a página inteira é mais eficiente que país a país
 
    logger.info(f"Total de países recebidos da API: {total}")

 
def executar_pipeline():
    """Configura e executa o pipeline dlt para a REST Countries API."""
 
    # Criar o pipeline dlt
    pipeline = dlt.pipeline(
        pipeline_name="rest_countries_bronze",
        destination="filesystem",
        dataset_name="rest_countries",  # Subdiretório dentro do bucket bronze
    )
 
    logger.info("Iniciando pipeline: REST Countries v5 → MinIO Bronze")
    inicio = datetime.now()
 
    # Executar a carga
    info = pipeline.run(
        extrair_paises(),
        loader_file_format="parquet",  # Formato Parquet com Snappy (padrão do dlt)
    )
 
    duracao = (datetime.now() - inicio).total_seconds()
 
    # Exibir o relatório de execução
    print("\n" + "=" * 60)
    print("  RELATÓRIO DE EXECUÇÃO — REST Countries API v5")
    print("=" * 60)
    print(f"  Status:        {'✅ Sucesso' if not info.has_failed_jobs else '❌ Falha'}")
    print(f"  Duração:       {duracao:.2f}s")
    print(f"  Destino:       s3://bronze/rest_countries/")
    print(f"  Formato:       Parquet + Snappy")
    print()
    print("  Detalhes da carga:")
    print(info)
    print("=" * 60)
 
    return info
 
 
if __name__ == "__main__":
    executar_pipeline()
