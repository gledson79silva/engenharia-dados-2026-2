"""
Utilitário para gravação padronizada na camada Bronze do Data Lake.
 
Encapsula as boas práticas de:
  - Particionamento por data de ingestão
  - Adição de metadados de rastreabilidade
  - Nomenclatura padronizada de arquivos
  - Registro de manifesto de ingestão
  - Verificação de integridade (contagem de registros)
"""
 
import pandas as pd
import boto3
import json
import io
import hashlib
import logging
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Optional
from uuid import uuid4

logger = logging.getLogger(__name__)
 
 
class BronzeWriter:
    """
    Gerencia a gravação padronizada de dados na camada Bronze do Data Lake.
 
    Uso:
        writer = BronzeWriter()
        resultado = writer.gravar(
            df=meu_dataframe,
            sistema_origem="pokeapi",
            entidade="pokemon",
        )
        print(resultado["caminho_objeto"])
    """

# Configurações padrão do MinIO local
    ENDPOINT = "http://localhost:9000"
    ACCESS_KEY = "minioadmin"
    SECRET_KEY = "minioadmin123"
    BUCKET = "bronze"
 
    def __init__(
        self,
        endpoint: str = ENDPOINT,
        access_key: str = ACCESS_KEY,
        secret_key: str = SECRET_KEY,
        bucket: str = BUCKET,
    ):
        self.bucket = bucket
        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="us-east-1",
        )
 
    def _construir_caminho(
        self,
        sistema_origem: str,
        entidade: str,
        timestamp: datetime,
    ) -> str:
        """
        Constrói o caminho S3 seguindo a convenção de particionamento.
 
        Exemplo de saída:
            pokeapi/pokemon/ano=2024/mes=01/dia=15/pokemon_1705363200.parquet
        """
        particao = (
            f"ano={timestamp.year}/"
            f"mes={timestamp.month:02d}/"
            f"dia={timestamp.day:02d}"
        )
        nome_arquivo = f"{entidade}_{int(timestamp.timestamp())}_{uuid4().hex}.parquet"
        return str(PurePosixPath(sistema_origem) / entidade / particao / nome_arquivo)
    
    def _adicionar_metadados(
        self,
        df: pd.DataFrame,
        sistema_origem: str,
        versao_schema: str,
        timestamp: datetime,
    ) -> pd.DataFrame:
        """
        Adiciona colunas de metadados de rastreabilidade ao DataFrame.
        Estas colunas são prefixadas com '_' para diferenciá-las dos dados originais.
        """
        df = df.copy()
        df["_fonte"] = sistema_origem
        df["_ingerido_em"] = timestamp.isoformat()
        df["_versao_schema"] = versao_schema
        df["_particao_data"] = timestamp.strftime("%Y-%m-%d")
        return df


    def _calcular_checksum(self, buffer: bytes) -> str:
        """Calcula o hash MD5 do arquivo para verificação de integridade."""
        return hashlib.md5(buffer).hexdigest()


    def gravar(
        self,
        df: pd.DataFrame,
        sistema_origem: str,
        entidade: str,
        versao_schema: str = "1.0",
        compressao: str = "snappy",
        timestamp: Optional[datetime] = None,
    ) -> dict:
        """
        Grava um DataFrame Pandas na camada Bronze do MinIO.
 
        Args:
            df:              DataFrame com os dados a serem gravados.
            sistema_origem:  Identificador do sistema de origem (ex: 'pokeapi').
            entidade:        Nome da entidade/tabela (ex: 'pokemon').
            versao_schema:   Versão do schema dos dados (para controle de evolução).
            compressao:      Algoritmo de compressão Parquet ('snappy' ou 'zstd').
            timestamp:       Data/hora de ingestão. Se None, usa o momento atual.
 
        Returns:
            Dicionário com metadados da operação de gravação.
        """
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)
        if timestamp.tzinfo is None:
            raise ValueError("timestamp deve informar o fuso horário")
        timestamp = timestamp.astimezone(timezone.utc)
 
        # Adicionar metadados de rastreabilidade
        df_com_metadados = self._adicionar_metadados(
            df, sistema_origem, versao_schema, timestamp
        )
 
        # Construir o caminho do objeto no MinIO
        caminho_objeto = self._construir_caminho(sistema_origem, entidade, timestamp)
 
        # Serializar para Parquet em memória
        buffer = io.BytesIO()
        df_com_metadados.to_parquet(buffer, index=False, compression=compressao)
        buffer.seek(0)
        dados_bytes = buffer.read()
 
        # Calcular checksum para verificação de integridade
        checksum = self._calcular_checksum(dados_bytes)
 
        # Upload para o MinIO
        self.client.put_object(
            Bucket=self.bucket,
            Key=caminho_objeto,
            Body=dados_bytes,
            ContentType="application/octet-stream",
            IfNoneMatch="*",
            Metadata={
                "fonte": sistema_origem,
                "entidade": entidade,
                "registros": str(len(df)),
                "checksum_md5": checksum,
                "versao_schema": versao_schema,
            },
        )
 
        resultado = {
            "caminho_objeto": f"s3://{self.bucket}/{caminho_objeto}",
            "registros": len(df),
            "colunas": len(df_com_metadados.columns),
            "tamanho_bytes": len(dados_bytes),
            "checksum_md5": checksum,
            "compressao": compressao,
            "timestamp_ingestao": timestamp.isoformat(),
        }
 
        logger.info(
            f"✅ Bronze gravado: {caminho_objeto} "
            f"({resultado['registros']:,} registros, "
            f"{resultado['tamanho_bytes'] / 1024:.1f} KB)"
        )
 
        return resultado

    def registrar_manifesto(self, execucao: dict) -> None:
        """
        Registra um manifesto JSON com os metadados de cada execução de pipeline.
        O manifesto é armazenado em bronze/_manifesto/ e serve como log de auditoria.
        """
        timestamp = datetime.now(timezone.utc)
        chave_manifesto = (
            f"_manifesto/"
            f"ano={timestamp.year}/mes={timestamp.month:02d}/dia={timestamp.day:02d}/"
            f"manifesto_{int(timestamp.timestamp())}_{uuid4().hex}.json"
        )
 
        self.client.put_object(
            Bucket=self.bucket,
            Key=chave_manifesto,
            Body=json.dumps(execucao, indent=2, ensure_ascii=False, default=str),
            ContentType="application/json",
            IfNoneMatch="*",
        )
 
        logger.info(f"📋 Manifesto registrado: {chave_manifesto}")
 
    def listar_particoes(self, sistema_origem: str, entidade: str) -> list:
        """
        Lista todas as partições disponíveis para uma entidade específica.
        Útil para verificar a cobertura temporal dos dados na camada Bronze.
        """
        prefixo = f"{sistema_origem}/{entidade}/"
        particoes = []
        for pagina in self.client.get_paginator("list_objects_v2").paginate(
            Bucket=self.bucket, Prefix=prefixo
        ):
            for obj in pagina.get("Contents", []):
                if obj["Key"].endswith(".parquet") and "/ano=" in obj["Key"]:
                    particoes.append({
                        "caminho": obj["Key"],
                        "tamanho_bytes": obj["Size"],
                        "ultima_modificacao": obj["LastModified"],
                    })
        return particoes
