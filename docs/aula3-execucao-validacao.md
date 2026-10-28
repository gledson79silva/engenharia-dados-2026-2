# Aula 3 — Execução e validação

Data: 27/10/2026, America/Sao_Paulo. Execução autorizada pelo estudante após
diagnóstico somente de leitura. Fonte principal: `roteiro-aula-aula.md`.

## Ambiente e preservação dos dados

- `.venv311`: Python 3.11.17; dependências completas em `requirements-lock.txt`.
- `.venv`: Python 3.9.6, preservado; não é o ambiente desta disciplina.
- Docker ARM64, 8 CPUs, aproximadamente 5,8 GiB de memória.
- Volume ativo preservado: `engenharia-dados-2026-2_minio_data`.
- O volume `docker_minio_data` foi inspecionado com montagem somente de leitura
  e estava vazio. Nenhum volume foi excluído.
- Compose único na raiz; o arquivo em `docker/` apenas inclui a definição.
- MinIO/Silo: API 9000 e console 9001. Jupyter 8888; Spark UI 4040 existe
  somente durante uma sessão Spark. O notebook encerra a sessão ao terminar.
- Airbyte 2.4.0 via abctl/Kubernetes, porta 8000. Foi parado temporariamente
  para liberar memória durante os testes Spark e reiniciado depois.

## Evidências de execução

| Atividade | Resultado validado |
|---|---|
| Airbyte REST Countries → MinIO | Job 1 terminou `succeeded` após retry; 254 registros; Parquet lido com 254 nomes únicos, metadados e Snappy conferidos |
| Ambiente Python | Verificação dos imports e `pip check` aprovados |
| Parquets locais | Três arquivos, cada um com 1.000.000 de linhas |
| REST Countries via dlt | 254 países em três páginas: 100 + 100 + 54; nenhum job de carga com falha |
| PokeAPI via dlt | 300 Pokémon; tabelas filhas com 1.800 estatísticas e 770 habilidades |
| BronzeWriter Countries | 254 países particionados; manifesto, tamanho e MD5 conferidos |
| BronzeWriter Pokémon | 50 Pokémon; manifesto, tamanho e MD5 conferidos |
| Append-only | Duas escritas com timestamp idêntico geraram chaves distintas; ambas legíveis e com manifestos válidos |
| Spark | Notebook executado em kernel novo, sem erros; 1.000.000 de IDs únicos na entrada |
| Spark join | Left join muitos-para-um com dimensão didática de cinco categorias; 1.000.000 de linhas e IDs únicos preservados; zero pedidos sem correspondência |
| Spark saída | 750.572 pedidos concluídos; IDs únicos, status e partições ano/mês verificados |
| Filtro premium | 697.588 pedidos concluídos com valor acima de 500 |
| DuckDB | SQL diretamente no MinIO; 25 grupos comparados com Spark, tolerância absoluta de 0,011 nos valores arredondados |
| Iceberg | 1.000.000 de linhas iniciais; append de uma linha demonstrativa; total 1.000.001 |
| Iceberg schema/snapshots | Nova coluna, leitor com visão anterior até refresh e time travel com 1.000.000 de linhas aprovados |

O ensaio Iceberg utiliza uma projeção de seis colunas dos pedidos. A linha
adicional tem `id_pedido=1000001` e é uma demonstração em tabela separada;
não foi acrescentada ao dataset Bronze original. O catálogo SQLite fica em
`data/lakehouse/catalog.db` e os arquivos de tabela no bucket Silver, sob
`lakehouse/aula3/`. Isso é uma prática exploratória, sem representar a
transformação definitiva Silver/dbt da próxima aula.

Os arquivos locais de resultados estão em `data/validation/` e
`data/lakehouse/ultima_execucao.json`, ignorados pelo Git. O catálogo SQLite
deve ser preservado junto com os objetos S3 para reutilizar as tabelas Iceberg.

## Correções realizadas

- BronzeWriter usa UTC, UUID e criação condicional para impedir sobrescrita.
- Listagens S3 usam paginação; a comparação dlt lê todos os arquivos da tabela
  principal, sem misturar tabelas filhas nem partições BronzeWriter. A leitura
  seleciona o load ID mais recente para não somar execuções append.
- O teste de particionamento aceita `--data AAAA-MM-DD` em UTC e falha se não
  encontrar arquivos, em vez de declarar sucesso sem dados.
- O notebook Spark usa `local[2]`, valida contagens, IDs e partições, exporta
  referência para DuckDB e libera cache/sessão no final.
- O notebook alternativo PyAirbyte referencia o Compose existente; permanece
  opcional e sem execução, pois não substitui a atividade REST Countries.
- Configuração `.env` compartilhada; estado dlt permanece em `.local/dlt`.
- Cargas dlt futuras usam append; Pokémon dlt usa `pokeapi_dlt/`, separado de
  `pokeapi/pokemon/ano=...` do BronzeWriter.

Na montagem inicial do laboratório, o `replace` original do dlt removeu o Parquet Pokémon
recém-criado pelo BronzeWriter, pois compartilhavam o prefixo. O arquivo foi
reconstruído dos dados já ingeridos e restaurado **somente após conferir o MD5
original**, byte a byte. Os quatro manifestos existentes passaram na conferência
de integridade. A carga dlt foi copiada sem excluir a origem para `pokeapi_dlt/`;
a cópia antiga permanece como evidência e não deve ser somada nas análises.
As próximas cargas usam append; leituras de várias execuções precisam selecionar
um load ID ou tratar duplicações para comparações de entidades.

## Reproduzir as validações

```bash
source .venv311/bin/activate
python -B scripts/1_teste_instalacao_python.py
python -m pip check
docker compose config --quiet
docker compose -f docker/compose.yml config --quiet
python -B scripts/6_explorar_countries_bronze.py
python -B scripts/8_comparar_pipelines.py
python -B scripts/10_verificar_manifesto.py
python -B scripts/11_demonstrar_partition_prunning.py --data 2026-10-27
python -B scripts/12_validar_bronze.py
python -B scripts/13_duckdb_analitico.py
python -B scripts/16_validar_airbyte.py --esperado 254
python -B scripts/17_validar_iceberg.py
```

O arquivo tem `prunning` no nome, conforme o repositório original. A data acima
é a data UTC desta carga; para outra carga, informe sua partição.

Execute `notebooks/1_notebook_spark.ipynb` no Jupyter Docker, do início ao fim,
antes da comparação DuckDB. A célula de gravação usa `overwrite` exclusivamente
no prefixo derivado `bronze/ecommerce_processado/`; a entrada permanece intacta.

```bash
# Nova prática isolada de Iceberg: cria outra tabela e mantém as anteriores.
python -B scripts/14_iceberg_lakehouse.py

# Consultar os jobs de Airbyte; --sync inicia uma nova sincronização manual.
python -B scripts/15_airbyte_countries.py --aguardar

# Gerar novas cargas (acrescentam execuções; requerem acesso à internet).
python -B scripts/5_pipeline_ingestao_dlt.py
python -B scripts/7_ingest_pokemon_paginado.py
python -B scripts/9_reingerir_com_particionamento.py
```

O ensaio append-only pode ser repetido com
`python -B scripts/12_validar_bronze.py --testar-escrita`; ele acrescenta dois
pequenos arquivos de teste e seus manifestos, sem removê-los.

## Airbyte

No diagnóstico existiam dois destinos de nome `minio-bronze-local`, mas nenhuma
source, conexão ou job. Os destinos foram preservados. A source usa o manifesto
versionado em `airbyte/restcountries_manifest.json`, com Bearer Token lido do
`.env`, registros em `data.objects` e paginação offset/limit de 100 registros.
O script de configuração usa a API autenticada e não grava nem imprime segredos.

A API rejeitou inicialmente um manifesto major 7: o catálogo local suporta
major 0 a 4. A definição foi ajustada para **4.3.0**, sem alterar a instalação.
Os componentes Airbyte voltaram a ficar prontos após o reinício. Os pods de
descoberta e de teste da source e do destino S3 concluíram sem erro de processo;
o estado final da sincronização é a evidência para confirmar a carga.
A conexão `countries-to-bronze` foi criada com ID
`044368ed-a68b-4e9d-8b49-14ed88c09fb2`; a primeira sincronização recebeu job ID 1.

O destino existente aponta para `http://host.docker.internal:9000`, bucket
`bronze`, prefixo `airbyte/`, formato Parquet/Snappy. O sucesso deve ser confirmado
pelo estado final do job e pela leitura do Parquet; HTTP 200 da interface,
isoladamente, não comprova sincronização.

A primeira tentativa do job 1 falhou no destino com `PatternSyntaxException`.
O campo `s3_path_format` já cadastrado continha parte de uma tabela Markdown
(`*S3 Path Format** | ...`), que o teste de conexão S3 não verificava.
Foi aplicada uma atualização parcial somente desse campo no destino utilizado,
para `${NAMESPACE}/${STREAM_NAME}/${YEAR}_${MONTH}_${DAY}_${EPOCH}_`, conforme
[a documentação do conector S3](https://docs.airbyte.com/integrations/destinations/s3).
A correção não alterou bucket, endpoint nem credenciais. O Airbyte iniciou
uma nova tentativa automaticamente no mesmo job 1; a tentativa de iniciar outro
job retornou 409 porque esse retry já estava em andamento. O acompanhamento
trata `incomplete` como transição de retry, não como sucesso ou falha final,
e renova o token da API quando necessário.

Resultado final: **job 1 `succeeded`, 254 registros e 20.746 bytes contabilizados
pela API**. O arquivo armazenado tem 5.259.322 bytes (tamanho físico Parquet,
diferente da métrica lógica do job), 254 registros e compressão Snappy:
`bronze/airbyte/rest-countries-api/countries/2026_10_28_1793145910060_0.parquet`.
A data no nome é UTC; a execução ocorreu em 27/10 no fuso de São Paulo.
Foram conferidos nomes únicos, IDs de ingestão únicos e timestamps preenchidos.
A fonte tem 250 códigos ISO preenchidos e **quatro entidades sem código ISO**;
essas quatro foram preservadas. A validação verifica a unicidade dos códigos
preenchidos e dos nomes, sem exigir um ISO inexistente na origem.
Evidências locais: `data/validation/airbyte_jobs.json` e
`data/validation/airbyte_parquet.json`.

## Verificações finais

- Endpoints finais: MinIO 9000/9001, saúde Airbyte 8000 e login Jupyter 8888
  responderam HTTP 200. Spark UI 4040 fica disponível durante uma SparkSession.
- Sintaxe de todos os scripts: aprovada.
- Estrutura nbformat dos três notebooks: válida; nenhuma saída de erro salva.
- `git diff --check`: aprovado.
- Compose da raiz e wrapper em `docker/`: configuração válida.
- `pip check`: aprovado após a instalação.
- `.env` ignorado; chave pessoal ausente de todos os arquivos não ignorados.
- Nenhum arquivo não ignorado acima de 100 MiB; CSV e JSONL grandes continuam
  ignorados e preservados no disco.
- Os scripts atuais `6`, `8`, `10`, `11` (partição Countries de 2026-10-27)
  e `12` passaram na execução final autorizada. A seleção da carga mais recente
  foi exercitada ao vivo: Countries load ID `1793144424.563564` (254 linhas)
  e Pokémon load ID `1793144099.73004` (300 linhas). Os quatro manifestos
  passaram novamente nas verificações de tamanho, contagem e MD5.
- DuckDB e a validação do Parquet Airbyte passaram novamente na versão atual.

## Fechamento da aula: join e revalidação Iceberg

O notebook `notebooks/1_notebook_spark.ipynb` recebeu o bloco **Join: pedidos e
dimensão de categorias**. A dimensão foi criada com SQL `VALUES`, mantendo cinco
chaves únicas; o DataFrame de pedidos foi enriquecido com `left join` e broadcast
da dimensão. Os testes conferiram 1.000.000 de linhas antes e depois, 1.000.000 de
IDs únicos e zero valores ausentes no grupo de categoria.

Para validar o novo bloco foram executadas somente as cinco células iniciais de
configuração/leitura, a célula de join e o encerramento do Spark, em kernel novo.
A execução usou heap de 512 MiB, com os serviços existentes ativos. As saídas da
célula de join foram incorporadas ao notebook; as saídas anteriores foram
preservadas. O notebook completo não foi reexecutado neste fechamento, e sua
célula de gravação `overwrite` não foi acionada.

Evidências locais: `data/validation/spark_join.json` e
`data/validation/spark_join_execucao.ipynb`. Os contadores de execução do bloco
novo pertencem à sessão isolada, não à sessão das células anteriores.

O novo `scripts/17_validar_iceberg.py` abre o SQLite com `mode=ro` e usa a tabela
Iceberg estática a partir dos metadados registrados. Ele leu efetivamente a coluna
de IDs dos snapshots existentes e conferiu:

- Snapshot inicial `7345279312475636455`: 1.000.000 de IDs únicos.
- Snapshot atual `310451212433060214`: 1.000.001 de IDs únicos.
- Dois schemas; a coluna `versao_laboratorio` é string e a linha demonstrativa
  mantém o valor `demonstracao`.
- Zero novas tabelas e zero novos snapshots criados nesta conferência.

A evidência está em `data/validation/iceberg_revalidacao.json`. O resumo deste
fechamento fica em `data/validation/fechamento_aula3.json`. As cargas existentes
foram preservadas; apenas o notebook, os arquivos locais de evidência e a
documentação foram atualizados, além da inclusão do validador Iceberg.

## Limites e próximos estudos

- DuckDB e Iceberg são complementos elaborados para os objetivos anunciados:
  o roteiro local termina no exercício Spark.
- Os testes Iceberg demonstram commits publicados, isolamento de leitura e
  snapshots. Não simulam falha de energia nem disputa entre escritores.
- dbt e Airflow ainda não foram implementados; pertencem às próximas etapas.
- Não foram feitos commit, push, limpeza de volumes ou instalação global.

Para fixar: filtre uma partição histórica; compare `EXPLAIN` de Spark e DuckDB;
consulte os dois snapshots Iceberg e explique por que a contagem mudou sem
alterar o resultado consultado pelo snapshot anterior.

Referências: [PyIceberg](https://py.iceberg.apache.org/api/),
[DuckDB S3](https://duckdb.org/docs/2.0/core_extensions/httpfs/s3api),
[Airbyte API](https://docs.airbyte.com/developers/api-documentation).
