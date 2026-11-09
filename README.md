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
