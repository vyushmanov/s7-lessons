import os
import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import date, datetime

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

# задаём базовые аргументы
default_args = {
    'start_date': datetime(2020, 1, 1),
    'owner': 'airflow'
}

# вызываем DAG
dag = DAG("bash_operator_dag",
          schedule_interval=None,
          default_args=default_args
         )

# объявляем задачу с Bash-командой, которая распечатывает дату
t1 = BashOperator(
    task_id='print_date',
    bash_command="/usr/bin/python3 /lessons/dags/partition.py 2022-06-21 hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/master/data/events hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/vyushmanov/data/events",
        retries=3,
        dag=dag
)

t1