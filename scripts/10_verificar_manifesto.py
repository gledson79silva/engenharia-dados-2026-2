"""
Lê e exibe os manifestos de ingestão registrados na camada Bronze.
O manifesto é a base para auditoria e rastreabilidade do Data Lake.
"""

import boto3
import json
from course_config import list_objects
 
client = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin123",
    region_name="us-east-1",
)
 
objetos = list_objects("bronze", "_manifesto/")
manifestos = sorted(
    [obj["Key"] for obj in objetos if obj["Key"].endswith(".json")],
    reverse=True,  # Mais recentes primeiro
)
 
print(f"Manifestos de ingestão encontrados: {len(manifestos)}\n")
 
for chave in manifestos[:5]:  # Exibir os 5 mais recentes
    obj = client.get_object(Bucket="bronze", Key=chave)
    manifesto = json.loads(obj["Body"].read().decode("utf-8"))
    print(f"📋 {chave}")
    print(f"   Pipeline:   {manifesto.get('pipeline')}")
    print(f"   Executado:  {manifesto.get('execucao_em')}")
    resultado = manifesto.get("resultado", {})
    print(f"   Registros:  {resultado.get('registros', 'N/A'):,}")
    print(f"   Tamanho:    {resultado.get('tamanho_bytes', 0) / 1024:.1f} KB")
    print(f"   Checksum:   {resultado.get('checksum_md5', 'N/A')}")
    print()
