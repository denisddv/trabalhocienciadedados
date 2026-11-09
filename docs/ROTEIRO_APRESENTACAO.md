# Roteiro de apresentação — NovaMarket

Duração sugerida: 4–6 minutos. Gravar no VS Code e navegador com os dados e logs reais do laboratório. Não simular execuções.

## Abertura (30 segundos)

“Este é o projeto NovaMarket, desenvolvido para demonstrar ingestão de eventos, processamento distribuído, armazenamento em camadas e orquestração. Utilizei Kafka, Spark e Airflow em contêineres Docker, a partir do VS Code com Ubuntu/WSL 2.”

**Mostrar:** repositório no GitHub e a arquitetura no README.

## Ingestão e streaming (60 segundos)

“O produtor Python criou 200 eventos de pedidos. O tópico Kafka `novamarket-pedidos` possui três partições. O Spark Structured Streaming leu esses eventos, registrou checkpoints e persistiu os dados brutos em JSON na camada RAW.”

**Mostrar:** `kafka-topics.sh --describe`, contagem dos JSON na RAW e Spark Master com Worker registrado. Evite exibir credenciais nos terminais.

## Data Lake e processamento batch (60 segundos)

“O Spark Batch validou campos como quantidade, valor e data, normalizou os dados e removeu duplicações por identificador. Os pedidos tratados foram gravados em Parquet na TRUSTED. A REFINED contém os indicadores agregados por categoria: pedidos, faturamento e ticket médio.”

**Mostrar:** estrutura `datalake/raw`, `trusted` e `refined`, saídas Parquet e código `spark/batch.py`.

## Orquestração Airflow (60 segundos)

“A DAG `novamarket_pipeline` coordena quatro tarefas: checagem de entrada, processamento, validação da saída e benchmark. Para manter a compatibilidade do Python entre driver e Worker, o Airflow aciona o `spark-submit` dentro do contêiner Spark Master.”

**Mostrar:** tela Graph com quatro tarefas verdes, se disponível.

## Benchmark e interpretação (60 segundos)

“Com 300 mil registros sintéticos, o join convencional levou 1,7516 segundo, enquanto o broadcast join levou 0,4577 segundo. Nesse teste, a redução foi de 73,87%. Ambas as estratégias produziram quatro categorias e total 149.850.000, mostrando equivalência do resultado. O ganho é do experimento de join e não do tempo integral do pipeline.”

**Mostrar:** `docs/RESULTADOS_BENCHMARK_2026-11-09.json` ou `benchmark/resultados.json` na máquina.

## Encerramento (30 segundos)

“Os testes também mostraram desafios operacionais reais, como permissões entre contêineres, recursos do cluster e compatibilidade entre ambientes Python. O projeto é uma prova de conceito e ainda não oferece alta disponibilidade nem garantias transacionais de produção.”

**Lembretes para a entrega:** salvar o vídeo, inserir o link conforme a instrução da disciplina e anexar as capturas necessárias; não gravar senhas, tokens, e-mail pessoal ou credenciais.
