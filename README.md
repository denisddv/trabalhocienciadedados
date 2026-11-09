# NovaMarket — Do Evento ao Insight

Projeto acadêmico de Big Data and Distributed Computing (UniFECAF, 2026).

## Escopo
Produtor de eventos em Python -> tópico Kafka `novamarket-pedidos` -> Spark Structured Streaming -> raw JSON -> Spark batch -> trusted e refined Parquet. DAG Airflow coordena a parte batch e um benchmark compara join convencional com broadcast join.

**Estado da validação:** sintaxe Python aprovada e 2 testes de regras do gerador aprovados em ambiente local. O fluxo distribuído Kafka/Spark/Airflow e as medições de benchmark **não foram executados nem validados** neste ambiente, que não possui Docker ou PySpark. Não há dados de desempenho inventados.

## Como executar
1. Instale Docker Engine e Docker Compose.
2. `docker compose up -d --build`
3. Crie o tópico Kafka: `docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic novamarket-pedidos --partitions 3 --replication-factor 1`
4. Submeta `spark/streaming.py` com pacote `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5`.
5. Execute `producer/gerar_eventos.py` na rede do Docker com a dependência `kafka-python`.
6. Na Airflow UI (porta 8081), inicie a DAG `novamarket_pipeline` após a ingestão.
7. Registre em `benchmark/resultados.json` os tempos efetivamente medidos.

## Limitações
Kafka single broker, streaming e DAG não testados ponta a ponta, sem garantias exactly-once, autenticação e monitoramento de produção. Antes da entrega final, executar em Docker, coletar Spark UI e logs, e atualizar o relatório técnico.

Recomendações: Kafka com replicação, idempotência transacional, contratos de schema, métricas, compactação de arquivos e testes de falha.

## Airflow chama o Spark no próprio contêiner Spark (WSL / Docker Desktop)

Para evitar incompatibilidade Python 3.12 (Airflow) vs 3.8 (Spark Worker), a DAG
faz `docker exec` no Spark Master. A CLI Docker e o socket são disponibilizados
no Airflow. **Acesso ao Docker socket equivale a controle privilegiado do host:
usar somente neste ambiente local de laboratório, nunca em um Airflow público.**

Antes de recriar o Airflow, a partir do Ubuntu WSL:

```bash
cd ~/projetos/trabalhocienciadedados
# Configura o grupo real do socket. .env deve ficar fora do Git.
printf 'DOCKER_GID=%s\n' "$(stat -c '%g' /var/run/docker.sock)" > .env
docker compose config
# Preservar backup do SQLite e conferir airflow/state antes de recriar.
docker compose up -d --no-deps --build --force-recreate airflow
docker compose exec airflow docker ps --filter label=com.docker.compose.service=spark-master
docker compose exec airflow airflow dags list-import-errors
```

Se `docker ps` no Airflow falhar com permissão negada, confira
`docker compose exec airflow id`, `stat -c '%g' /var/run/docker.sock`
e recrie Airflow com `DOCKER_GID` correto. O nome do projeto Compose é
`trabalhocienciadedados`; se mudar o nome do projeto, atualize o filtro
na DAG. Os resultados do benchmark variam entre execuções.

A validação final de ponta a ponta desta versão requer execução no ambiente
do aluno e verificação das quatro tarefas verdes no Airflow.
