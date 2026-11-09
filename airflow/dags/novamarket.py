from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
ARGS={'owner':'novamarket','retries':1,'retry_delay':timedelta(minutes=2)}
BASE = (
    "spark-submit "
    "--master spark://spark-master:7077 "
    "--conf spark.driver.host=airflow "
    "--conf spark.cores.max=2 "
    "--conf spark.executor.cores=2 "
    "--conf spark.executor.memory=2g "
    "--conf spark.sql.shuffle.partitions=4"
)
with DAG('novamarket_pipeline',start_date=datetime(2026,1,1),schedule_interval=None,catchup=False,default_args=ARGS,tags=['bigdata','poc']) as dag:
    check_input=BashOperator(task_id='check_input',bash_command='test -d /opt/project/datalake/raw/pedidos')
    process_data=BashOperator(task_id='process_data',bash_command=BASE+' /opt/project/spark/batch.py')
    quality_check=BashOperator(task_id='quality_check',bash_command='test -d /opt/project/datalake/trusted/pedidos && test -d /opt/project/datalake/refined/vendas_por_categoria')
    benchmark=BashOperator(task_id='benchmark',bash_command=BASE+' /opt/project/spark/benchmark.py')
    check_input >> process_data >> quality_check >> benchmark
