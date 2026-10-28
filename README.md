# Engenharia de Dados Moderna — Projeto do Curso
 
Repositório de código e configurações desenvolvidos ao longo do curso de
Engenharia de Dados Moderna com Ferramentas Open Source.
 
## Estrutura
- `notebooks/` — Jupyter Notebooks das atividades práticas
- `scripts/` — Scripts Python de ingestão e processamento
- `dbt_project/` — Projeto dbt para transformações Silver e Gold

## Executar o laboratório no macOS

```bash
source .venv311/bin/activate
python --version
python -m pip install -r requirements-lock.txt
python -m pip check
docker compose up -d
docker compose ps
```

Em uma máquina nova, crie primeiro o volume com
`docker volume create engenharia-dados-2026-2_minio_data` e os buckets com
`python scripts/2_setup_minio.py`. Nesta máquina o volume e os buckets já existem.
O volume externo evita que outro diretório Compose conecte um Data Lake diferente.
`docker/compose.yml` inclui a mesma definição da raiz.

Serviços: MinIO API em http://localhost:9000, console em http://localhost:9001,
Jupyter em http://localhost:8888 e Spark UI em http://localhost:4040 enquanto
houver uma SparkSession ativa. Obtenha o link de acesso do Jupyter no seu terminal
com `docker exec spark-jupyter jupyter server list`; não publique o token.
Airbyte é uma instalação separada via `abctl`, em http://localhost:8000.

Copie `.env.example` para `.env` e informe sua chave pessoal da REST Countries.
Os scripts carregam esse arquivo automaticamente, sem imprimir a chave.
Cadastro: https://restcountries.com/sign-up. O arquivo `.env` não entra no Git.

Os requisitos diretos estão em `requirements.txt`; `requirements-lock.txt` registra
o conjunto completo validado no Python 3.11/macOS ARM64. Spark executa na imagem
Docker 3.5.0, com duas CPUs e limite de 2 GiB. Em hosts com pouca RAM, execute
Spark e sincronizações Airbyte em momentos diferentes.

Veja o [relatório de execução e validação](docs/aula3-execucao-validacao.md)
para resultados, comandos e pendências. As práticas DuckDB e Iceberg são
complementos ao roteiro, que detalha somente Spark nesta aula.

Use `iris_teste.ipynb` no Python local `.venv311` (ele acessa `localhost:9000`)
e `1_notebook_spark.ipynb` no Jupyter Docker (ele acessa `minio:9000`). Para
abrir o notebook Iris em um servidor separado, execute no ambiente local
`python -m jupyter lab --no-browser --port=8889` e abra o link exibido no terminal.

O notebook Spark inclui o exemplo validado de join com dimensão de categorias.
Para conferir os snapshots Iceberg já existentes, use
`python -B scripts/17_validar_iceberg.py`; o comando lê o catálogo e as tabelas
existentes e atualiza somente a evidência local de validação.
