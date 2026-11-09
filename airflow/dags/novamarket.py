from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# O Airflow apenas orquestra. Driver e executores rodam na imagem Spark
# (Python 3.8), evitando PYTHON_VERSION_MISMATCH do Airflow (Python 3.12).
# Uso de docker.sock é restrito a esta prova de conceito local.
ARGS = {'owner': 'novamarket', 'retries': 1, 'retry_delay': timedelta(minutes=2)}

SPARK_MASTER = (
    '$(docker ps '
    '--filter label=com.docker.compose.project=trabalhocienciadedados '
    '--filter label=com.docker.compose.service=spark-master '
    '--filter status=running '
    '--format "{{.ID}}" | head -n 1)'
)

SPARK_SUBMIT = (
    'set -e; '
    f'CONTAINER={SPARK_MASTER}; '
    'test -n "$CONTAINER" || { echo "Spark Master container not found"; exit 1; }; '
    'docker exec "$CONTAINER" /opt/spark/bin/spark-submit '
    '--master spark://spark-master:7077 '
    '--conf spark.driver.host=spark-master '
    '--conf spark.cores.max=2 '
    '--conf spark.executor.cores=2 '
    '--conf spark.executor.memory=2g '
    '--conf spark.sql.shuffle.partitions=4 '
    '--conf spark.hadoop.fs.permissions.umask-mode=002 '
)

with DAG(
    'novamarket_pipeline',
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=ARGS,
    tags=['bigdata', 'poc'],
) as dag:
    check_input = BashOperator(
        task_id='check_input',
        bash_command='test -d /opt/project/datalake/raw/pedidos',
    )
    process_data = BashOperator(
        task_id='process_data',
        bash_command=SPARK_SUBMIT + '/opt/project/spark/batch.py',
    )
    quality_check = BashOperator(
        task_id='quality_check',
        bash_command=(
            'test -f /opt/project/datalake/trusted/pedidos/_SUCCESS '
            '&& test -f /opt/project/datalake/refined/vendas_por_categoria/_SUCCESS'
        ),
    )
    benchmark = BashOperator(
        task_id='benchmark',
        bash_command=SPARK_SUBMIT + '/opt/project/spark/benchmark.py',
    )

    check_input >> process_data >> quality_check >> benchmark
