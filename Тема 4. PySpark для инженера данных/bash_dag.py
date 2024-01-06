import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import date, datetime

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F

# задаём базовые аргументы
default_args = {
    'start_date': datetime(2020, 1, 1),
    'owner': 'airflow'
}

# вызываем DAG
dag = DAG("example_bash_dag",
          schedule_interval=None,
          default_args=default_args
          )

# объявляем задачу с Bash-командой
bash_operator = BashOperator(
    task_id='bash_operator_task',
    bash_command='spark-submit --master yarn --num-executors 10 --deploy-mode cluster partition.py 2022-05-31 /user/master/data/events/ /user/vyushmanov/data/events',
    retries=3,
    dag=dag
)

bash_operator




