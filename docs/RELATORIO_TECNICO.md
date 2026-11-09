# Relatório técnico — NovaMarket: do evento ao insight

**Curso:** Big Data and Distributed Computing — UniFECAF  
**Projeto:** NovaMarket — prova de conceito acadêmica  
**Data da execução acompanhada:** 9 de novembro de 2026  
**Ambiente:** VS Code com Ubuntu/WSL 2 e Docker Desktop

## 1. Resumo executivo

O trabalho implementa um fluxo de dados que começa com a geração de pedidos em Python, recebe eventos pelo Apache Kafka, processa os eventos com Spark Structured Streaming, grava os dados em um Data Lake e constrói indicadores por categoria com Spark batch. O Apache Airflow orquestra a etapa em lote e o benchmark.

Foram constatados 200 eventos JSON na camada RAW, arquivos Parquet nas camadas TRUSTED e REFINED e uma execução orquestrada bem-sucedida informada pelo aluno. O benchmark final realizado pelo Airflow apresentou tempo de **1,7516 s** no join convencional e **0,4577 s** no broadcast join, correspondendo a **73,87% de redução** neste experimento.

## 2. Problema e objetivo

Em pipelines distribuídos, os desafios incluem movimentação de dados entre partições, consistência de eventos, controle de atraso temporal, qualidade de dados, custo de shuffle e automação das transformações. O objetivo da POC é explorar esses desafios em um fluxo reproduzível de pedidos e mensurar o efeito de uma otimização de join.

## 3. Arquitetura

```text
Python (gerador de pedidos)
          |
          v
Apache Kafka: novamarket-pedidos (3 partições; RF=1)
          |
          v
Spark Structured Streaming
    |                    |
    v                    v
RAW: JSON            janelas de 5 minutos,
200 eventos          watermark de 10 minutos
    |
    v
Spark Batch
    |
    +--> TRUSTED: validação / normalização / deduplicação (Parquet)
    |
    +--> REFINED: pedidos, faturamento e ticket médio
                    por categoria (Parquet)

Apache Airflow: check_input -> process_data -> quality_check -> benchmark
                      (docker exec no Spark Master)
```

Tecnologias observadas: Kafka 3.9.1, Spark 3.5.5, Airflow 2.10.5, Docker Desktop, WSL 2 e Python. A DAG usa Docker CLI para iniciar `spark-submit` dentro do Spark Master, mantendo driver e Worker no Python 3.8 e evitando incompatibilidade com o Python 3.12 do Airflow.

## 4. Metodologia

1. Criar o tópico Kafka com três partições e fator de replicação um.
2. Produzir 200 eventos com identificador, cliente, categoria, quantidade, preço unitário, campanha e horário. O gerador introduz exemplos de duplicação e atraso de eventos.
3. Consumir o tópico pelo Structured Streaming; registrar JSON na camada RAW e calcular métricas temporais com janela de cinco minutos e watermark de dez minutos.
4. Ler os dados brutos no Spark batch, normalizar tipos e valores, rejeitar registros inválidos e aplicar `dropDuplicates(['id_pedido'])`.
5. Gravar a TRUSTED em Parquet e agregar pedidos, faturamento e ticket médio por categoria na REFINED.
6. Orquestrar checagem de entrada, batch, checagem das saídas e benchmark pelo Airflow.
7. Comparar join convencional com broadcast join em dados sintéticos, mantendo a mesma base e verificando a equivalência dos resultados.

A deduplicação por `id_pedido` da POC é simples e não define regras de desempate para possíveis versões divergentes do mesmo pedido.

## 5. Evidências observadas

| Etapa | Evidência da execução acompanhada | Interpretação |
|---|---|---|
| Kafka | Tópico `novamarket-pedidos`, 3 partições, replicação 1 | Ingestão configurada |
| Produtor Python | Mensagem de envio de 200 eventos | Eventos publicados |
| RAW | Contagem `find ... | wc -l` igual a 200; arquivos `part-*.json` | Persistência da ingestão confirmada |
| Spark streaming | Registros de execução, offsets e checkpoints; Spark Worker registrado | Streaming inicializou e consumiu dados |
| TRUSTED | Arquivo `part-*.snappy.parquet` presente | Saída tratada materializada |
| REFINED | Arquivo `part-*.snappy.parquet` presente | Saída agregada materializada |
| Airflow | Usuário confirmou sucesso na execução da DAG após as correções | Orquestração validada pelo relato e resultados locais |
| Benchmark | `resultados.json` lido no terminal | Medição registrada |

A captura da DAG com quatro tarefas verdes deve ser anexada como evidência visual à entrega, caso exigida. Não foi fornecida uma captura final nesta conversa.

## 6. Experimento de desempenho

O arquivo `spark/benchmark.py` cria **300.000 linhas sintéticas** e quatro categorias, materializa um DataFrame em cache e compara duas estratégias, com broadcast automático desabilitado para que a primeira condição represente o cenário convencional. O segundo cenário utiliza `broadcast(categorias)` explicitamente.

### Medição final registrada no Airflow

| Indicador | Join convencional | Broadcast join |
|---|---:|---:|
| Tempo (segundos) | 1,7516 | 0,4577 |
| Categorias | 4 | 4 |
| Soma de valores | 149.850.000 | 149.850.000 |

Redução calculada: `(1,7516 - 0,4577) / 1,7516 * 100 = 73,87%`. O broadcast join foi aproximadamente **3,83 vezes mais rápido** nessa execução.

Uma execução anterior registrou 1,6094 s e 0,4603 s, com redução de 71,40%. Os dois testes apontaram vantagem no cenário analisado, mas **não são suficientes para inferência estatística**. Em outro volume, distribuição de chaves, capacidade de memória ou tamanho de dimensão, o resultado pode ser diferente.

O benchmark testa estratégia de junção, não demonstra automaticamente que todo o pipeline ficou 73,87% mais rápido.

## 7. Problemas encontrados e tratamento

- **Docker Desktop/WSL:** falta de espaço e erros de escrita no armazenamento. Foi liberado espaço e recuperada a execução dos contêineres.
- **Bibliotecas Spark–Kafka:** cache Ivy ausente; após criar diretórios e ajustar permissões, o conector foi baixado.
- **RAW e Parquet:** conflitos de proprietários e permissões entre usuário Ubuntu, Airflow (UID 50000) e Spark Worker (UID 185); houve ajustes de diretórios e grupo compartilhado.
- **Recursos do Spark:** uma aplicação streaming chegou a ocupar todos os 16 núcleos; as execuções seguintes foram limitadas a dois núcleos.
- **Python no Airflow:** o Spark driver dentro do Airflow utilizava Python 3.12 enquanto o Worker utilizava 3.8, gerando `PYTHON_VERSION_MISMATCH`. A DAG foi alterada para executar o Spark driver dentro do contêiner Spark Master.
- **Benchmark:** conflitos na gravação/leitura de `resultados.json`; o script final usa arquivo temporário e substituição atômica, mantendo consistência caso a gravação seja interrompida.

A nova versão final do código de benchmark foi publicada no GitHub, mas **não foi executada novamente após essa atualização nesta conversa**. Os números deste relatório vêm do arquivo JSON efetivamente apresentado pelo aluno.

## 8. Limitações e melhorias

Esta é uma prova de conceito local, não uma arquitetura pronta para produção. O broker Kafka é único, os eventos não possuem tratamento transacional de ponta a ponta, e a saída em arquivos Parquet simples não oferece controle ACID entre múltiplos escritores. O Airflow está em modo standalone com SQLite; o acesso ao Docker socket oferece privilégios elevados e é adequado apenas a um ambiente isolado de laboratório.

Possíveis melhorias: aumentar replicação, definir políticas de idempotência e versionamento de pedidos, ampliar testes de dados inválidos, medir latência e throughput, executar benchmarks com múltiplas repetições, adicionar monitoramento de falhas, controlar número e tamanho de arquivos e substituir o mecanismo privilegiado de orquestração por um executor ou API adequados à produção.

## 9. Conclusão

A POC demonstrou o fluxo entre Kafka, Spark, Data Lake e Airflow, com saídas de dados materializadas e medição experimental de uma otimização do Spark. No benchmark final registrado, o broadcast join apresentou redução de 73,87% no tempo, preservando a soma e a quantidade de categorias. O experimento também mostrou a importância de configurar recursos, permissões, versões do Python e mecanismos de escrita em sistemas distribuídos.

## 10. Checklist de evidências para anexar

- [ ] Imagem do tópico Kafka com três partições
- [ ] Exemplo de evento JSON na RAW e contagem de 200 registros
- [ ] Imagem do painel Spark Master com Worker ativo
- [ ] Lista de arquivos Parquet da TRUSTED e REFINED
- [ ] DAG do Airflow com as quatro tarefas verdes
- [ ] Captura ou cópia do `benchmark/resultados.json` final (73,87%)
- [ ] Links do repositório e do vídeo, se exigidos pela disciplina

**Repositório:** https://github.com/denisddv/trabalhocienciadedados

> Este documento distingue resultados observados nas capturas e logs daquilo que depende de evidência adicional. Não constitui auditoria independente de execução no computador do aluno.
