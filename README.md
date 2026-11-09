# NovaMarket — Do Evento ao Insight

Projeto acadêmico de **Big Data and Distributed Computing (UniFECAF, 2026)**. Prova de conceito local em Docker Desktop + Ubuntu/WSL 2, operada pelo VS Code.

## Objetivo

Demonstrar ingestão de eventos, processamento distribuído, tratamento de dados em camadas, agregação e orquestração, além de comparar duas estratégias de join no Spark.

```text
Gerador Python -> Kafka (novamarket-pedidos; 3 partições)
  -> Spark Structured Streaming -> datalake/raw/pedidos (JSON)
  -> Spark batch -> datalake/trusted/pedidos (Parquet)
                 -> datalake/refined/vendas_por_categoria (Parquet)
Airflow (check_input -> process_data -> quality_check -> benchmark)
  -> docker exec no spark-master -> Spark Worker
```

**Componentes:** Kafka 3.9.1, Spark 3.5.5, Airflow 2.10.5 e Python. O Airflow executa sua DAG com Python 3.12, mas dispara o `spark-submit` **dentro do Spark Master** (Python 3.8), evitando o conflito entre as versões do driver e do worker.

## Resultados observados na execução de 09/11/2026

Resultados informados por capturas de tela e logs do ambiente local do aluno:

| Validação | Evidência observada |
|---|---|
| Kafka | Tópico `novamarket-pedidos`, 3 partições e fator de replicação 1 |
| Ingestão | 200 eventos gerados e 200 linhas encontradas nos JSON da RAW |
| Structured Streaming | Aplicação conectada ao cluster, checkpoints e microbatches registrados |
| TRUSTED e REFINED | Arquivos Parquet encontrados nas duas camadas |
| Airflow | Execução concluída com sucesso, segundo confirmação do aluno |
| Benchmark final | Shuffle join: 1,7516 s; broadcast join: 0,4577 s; redução de 73,87% |
| Integridade do benchmark | 4 categorias e total de 149.850.000 em ambas as estratégias |

O benchmark utiliza **300 mil linhas sintéticas**, não os 200 eventos da ingestão. Os tempos representam **uma execução**, sem repetições estatísticas ou extrapolação para produção. O arquivo `benchmark/resultados.json` é gerado localmente e ignorado pelo Git para evitar publicar dados de execução como se fossem reproduzíveis automaticamente.

Os logs e capturas podem ser anexados ao relatório acadêmico. Detalhes e limitações estão em [docs/RELATORIO_TECNICO.md](docs/RELATORIO_TECNICO.md).

## Como executar no VS Code com Ubuntu/WSL 2

Pré-requisitos: Docker Desktop ativo com integração WSL, Docker Compose, Git e espaço em disco suficiente. Execute no terminal Ubuntu, na raiz do repositório.

```bash
docker compose up -d --build
docker compose ps
```

Crie o tópico, se necessário:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --create --if-not-exists \
  --topic novamarket-pedidos --partitions 3 --replication-factor 1
```

**Gerar eventos:** o contêiner Spark Master já possui Python; instale a dependência `kafka-python==2.0.2` nele para o laboratório, ou execute o produtor em um ambiente Python equivalente.

```bash
docker compose exec -u root spark-master python3 -m pip install kafka-python==2.0.2
docker compose exec spark-master python3 /opt/project/producer/gerar_eventos.py \
  --bootstrap kafka:9092 --total 200
```

**Iniciar o streaming** (em outro terminal; demanda recursos e permanece ativo):

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.driver.host=spark-master \
  --conf spark.cores.max=2 --conf spark.executor.cores=2 \
  --conf spark.executor.memory=2g \
  --conf spark.sql.shuffle.partitions=4 \
  --conf spark.jars.ivy=/home/spark/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \
  /opt/project/spark/streaming.py
```

Use `Ctrl+C` para encerrar o streaming antes de executar o batch, se o Worker não tiver núcleos livres.

**Executar o batch manualmente**, quando necessário:

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.driver.host=spark-master \
  --conf spark.cores.max=2 --conf spark.executor.cores=2 \
  --conf spark.executor.memory=2g --conf spark.sql.shuffle.partitions=4 \
  /opt/project/spark/batch.py
```

### Airflow e acesso ao Spark

O ambiente de laboratório permite que o Airflow acesse o socket do Docker para enviar o `spark-submit` para dentro do Spark Master:

```bash
printf 'DOCKER_GID=%s\n' "$(stat -c '%g' /var/run/docker.sock)" > .env
docker compose config
docker compose up -d --no-deps --build --force-recreate airflow
docker compose exec airflow docker ps --filter label=com.docker.compose.service=spark-master
docker compose exec airflow airflow dags list-import-errors
```

Acesse **http://localhost:8081** (Spark UI: **http://localhost:8080**) e acione a DAG `novamarket_pipeline`. Preserve `airflow/state/` antes de recriar o Airflow; a pasta mantém o SQLite e o histórico. Nunca envie senhas, `.env`, SQLite ou logs com credenciais ao GitHub.

**Atenção:** montar `/var/run/docker.sock` dá controle privilegiado sobre o host Docker. Este mecanismo é **somente para ambiente local de desenvolvimento** e não deve ser usado em produção nem em Airflow exposto à internet. Uma instalação de produção exigiria execução remota mais restrita (por exemplo, SparkSubmitOperator com compatibilidade de Python/driver, ou API de submissão).

## Verificação e reexecução

```bash
find datalake/raw/pedidos -name 'part-*.json' -type f -exec cat {} + | wc -l
find datalake/trusted/pedidos -name '*.parquet' -type f
find datalake/refined/vendas_por_categoria -name '*.parquet' -type f
cat benchmark/resultados.json
```

O Spark `overwrite` remove e recria diretórios. Se houver falha de permissões no laboratório, confira o usuário Spark (UID 185), o usuário Airflow (UID 50000), grupo e permissões **do diretório de saída recém-criado** antes de repetir a execução. O Docker pode criar arquivos com proprietário diferente do usuário Ubuntu.

## Limitações

- Kafka com apenas um broker e replicação 1: sem alta disponibilidade.
- Pipeline acadêmico; não demonstra garantias de exactly-once ponta a ponta nem controle transacional sobre banco de destino.
- A comparação shuffle/broadcast é sintética, sem múltiplas rodadas e sem carga de produção.
- Gravação local em Parquet não substitui transações ACID ou versionamento de tabelas.
- Airflow em `standalone`, SQLite e socket Docker: configurações **não adequadas para produção**.
- As evidências visuais devem ser anexadas pelo aluno; as capturas não são armazenadas automaticamente neste repositório.

## Vídeo e entrega acadêmica (09/11/2026)

- **Vídeo demonstrativo:** https://youtu.be/685Qp5Ovofg
- **Relatório acadêmico, apresentação e evidências:** diretório `docs/entrega/` (arquivos binários organizados para envio junto ao projeto).
- **Relatório técnico em Markdown:** [docs/RELATORIO_TECNICO.md](docs/RELATORIO_TECNICO.md).
- **Benchmark observado:** [docs/RESULTADOS_BENCHMARK_2026-11-09.json](docs/RESULTADOS_BENCHMARK_2026-11-09.json).

O vídeo mostra a implementação local; a execução final da DAG foi confirmada com quatro tarefas concluídas no Airflow. Os arquivos de apresentação e as capturas são materiais de evidência, não substituem o código-fonte do pipeline.
