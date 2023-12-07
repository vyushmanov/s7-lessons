import pyspark
from pyspark.sql import SparkSession

date = '2022-06-21'
host = "hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020"
path = f"/user/vyushmanov/data/events/date={date}/event_type=message"

spark = SparkSession.builder \
                    .master("yarn") \
                    .appName(f"7.5.2.2_{date}") \
                    .getOrCreate()

df = spark.read.load(path = f"{host}{path}", format = 'parquet')
df.printSchema()






verified_tags = spark.read.load(path = f"{host}/user/master/data/snapshots/tags_verified/actual", format = 'parquet')
verified_tags.show(5,False)










from datetime import datetime, timedelta
import pyspark.sql.functions as psf


def input_paths(date, depth):
    path_list =[]
    host = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020'
    for i in range(depth):
        start_date = datetime.strptime(date, '%Y-%m-%d') + timedelta(days=-i)
        start_date = datetime.strftime(start_date,'%Y-%m-%d')
        path = f'/user/vyushmanov/data/events/date={start_date}/event_type=message'
        path_list.append(f'{host}{path}')
    return path_list

path_list = input_paths('2022-05-31', 7)

df = spark.read.parquet(*path_list)
df.printSchema()

verified_tags = spark.read.load(path = f"{host}/user/master/data/snapshots/tags_verified/actual", format = 'parquet')

df_exp = df.where("event.message_channel_to is not null")\
        .selectExpr(["event.message_from as user", "explode(event.tags) as tag"])\
        .groupBy("tag")\
        .agg(psf.expr("count(distinct user) as suggested_count"))\
        .where("suggested_count >= 100")

df_res = df_exp.join(verified_tags, on='tag', how='leftanti')
df_res.show()

df_res.write.format('parquet').save(f'{host}/user/vyushmanov/data/analytics/candidates_d7_pyspark')

spark.read.parquet(f'{host}/user/vyushmanov/data/analytics/candidates_d7_pyspark').show()








from pyspark.sql import SparkSession
import pyspark.sql.functions as psf
from datetime import datetime, timedelta

def input_paths(date, depth):
    path_list =[]
    for i in range(depth):
        start_date = datetime.strptime(date, '%Y-%m-%d') + timedelta(days=-i)
        start_date = datetime.strftime(start_date,'%Y-%m-%d')
        path = f'{user_path}/events/date={start_date}/event_type=message'
        path_list.append(f'{host}{path}')
    return path_list


spark = SparkSession \
    .builder \
    .master("yarn") \
    .config("spark.driver.cores", "8") \
    .config("spark.driver.memory", "6g") \
    .appName("7.10.1") \
    .getOrCreate()

host = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020'
user_path = '/user/vyushmanov/data'
date = '2022-05-31'
depth = 84

path_list = input_paths(date, depth)

df = spark.read.parquet(*path_list)
df.printSchema()

verified_tags = spark.read.load(path = f"{host}/user/master/data/snapshots/tags_verified/actual", format = 'parquet')

df_exp = df.where("event.message_channel_to is not null")\
        .selectExpr(["event.message_from as user", "explode(event.tags) as tag"])\
        .groupBy("tag")\
        .agg(psf.expr("count(distinct user) as suggested_count"))\
        .where("suggested_count >= 100")

df_res = df_exp.join(verified_tags, on='tag', how='leftanti')
df_res.show()

df_res.write.format('parquet').save(f'{host}{user_path}/analytics/candidates_d{depth}_pyspark')

spark.read.parquet(f'{host}{user_path}/analytics/candidates_d{depth}_pyspark').show()







import sys
from datetime import date as dt
import os
from datetime import datetime, timedelta

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
import findspark
findspark.init()
findspark.find()

import pyspark
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext
import pyspark.sql.functions as F


def input_paths(date, depth, host, path):
    path_list = []
    for i in range(depth):
        start_date = datetime.strptime(date, '%Y-%m-%d') + timedelta(days=-i)
        start_date = datetime.strftime(start_date, '%Y-%m-%d')
        path = '{}/date={}/event_type=message'.format(path,start_date)
        path_list.append(f'{host}{path}')
    return path_list

def main():
    host = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020'
    username = 'vyushmanov'

    date = sys.argv[1]
    depth = int(sys.argv[2])
    border = sys.argv[3]
    path_source = sys.argv[4].replace('username', username)
    path_verified_tags = sys.argv[5]
    path_target = sys.argv[6].replace('username', username)
    path_list = input_paths(date, depth, host, path_source)

    conf = SparkConf().setAppName("VerifiedTagsCandidatesJob-{}-d{}-cut{}".format(date,depth,border))
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    df = sql.read.parquet(*path_list)
    verified_tags = sql.read.load(path="{}{}".format(host,path_verified_tags)
                                    , format='parquet')
    df_exp = df.where("event.message_channel_to is not null") \
        .selectExpr(["event.message_from as user", "explode(event.tags) as tag"]) \
        .groupBy("tag") \
        .agg(psf.expr("count(distinct user) as suggested_count")) \
        .where(f"suggested_count >= {border}")

    df_res = df_exp.join(verified_tags, on='tag', how='leftanti')
    df_res.write.format('parquet').save('{}/date={}'.format(path_target, date))

if __name__ == "__main__":
    main()






## Версия Яндекс.Практикум
#######################################################################################################################


import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
import datetime

def main():
        date = sys.argv[1]
        days_count = sys.argv[2]
        suggested_cutoff = sys.argv[3]
        base_input_path = sys.argv[4]
        verified_tags_path = sys.argv[5]
        base_output_path = sys.argv[6]

        conf = SparkConf().setAppName(f"VerifiedTagsCandidatesJob-{date}-d{days_count}-cut{suggested_cutoff}")
        sc = SparkContext(conf=conf)
        sql = SQLContext(sc)

        dt = datetime.datetime.strptime(date, '%Y-%m-%d')
        paths = [f"{base_input_path}/date={(dt-datetime.timedelta(days=x)).strftime('%Y-%m-%d')}/event_type=message" for x in range(int(days_count))]
        messages = sql.read.parquet(*paths)

        verified_tags = sql.read.parquet(verified_tags_path)

        candidates = find_candidates(messages, verified_tags, suggested_cutoff)

        candidates.write.parquet(f"{base_output_path}/date={date}")

def find_candidates(messages, verified_tags, suggested_cutoff):
        all_tags = messages\\
        .where("event.message_channel_to is not null")\\
        .selectExpr(["event.message_from as user", "explode(event.tags) as tag"])\\
        .groupBy("tag")\\
        .agg(F.countDistinct("user").alias("suggested_count"))\\
        .where(F.col("suggested_count") >= suggested_cutoff)

        return all_tags.join(verified_tags, "tag", "left_anti")

if __name__ == "__main__":
        main()




## DAG ##
#######################################################################################
import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
from datetime import date, datetime

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

default_args = {
'owner': 'airflow',
'start_date':datetime(2020, 1, 1),
}

dag_spark = DAG(
dag_id = "datalake_etl",
default_args=default_args,
schedule_interval=None,
)

events_partitioned = SparkSubmitOperator(
task_id='events_partitioned',
dag=dag_spark,
application ='/home/username/partition_overwrite.py' ,
conn_id= 'yarn_spark',
application_args = ["2022-05-31", "/user/master/data/events", "/user/username/data/events"],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 1,
executor_memory = '1g'
)

verified_tags_candidates = SparkSubmitOperator(
task_id='verified_tags_candidates_d7',
dag=dag_spark,
application ='/home/username/verified_tags_candidates.py' ,
conn_id= 'yarn_spark',
application_args = ["2022-05-31", "7", "100", "/user/username/data/events", "/user/master/data/snapshots/tags_verified/actual", "/user/username/data/analytics/verified_tags_candidates_d7"],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 1,
executor_memory = '1g'
)

verified_tags_candidates_d84 = SparkSubmitOperator(
task_id='verified_tags_candidates_d84',
dag=dag_spark,
application ='/home/username/verified_tags_candidates.py' ,
conn_id= 'yarn_spark',
application_args = ["2022-05-31", "84", "1000", "/user/username/data/events", "/user/master/data/snapshots/tags_verified/actual", "/user/username/data/analytics/verified_tags_candidates_d84"],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 1,
executor_memory = '1g'
)

events_partitioned >> [verified_tags_candidates_d7, verified_tags_candidates_d84]






import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
from datetime import date, datetime

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME'] = '/usr'
os.environ['SPARK_HOME'] = '/usr/lib/spark'
os.environ['PYTHONPATH'] = '/usr/local/lib/python3.8'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 1, 1),
}

dag_spark = DAG(
    dag_id="datalake_etl",
    default_args=default_args,
    schedule_interval=None,
)

events_partitioned = SparkSubmitOperator(
    task_id='events_partitioned',
    dag=dag_spark,
    application='/home/username/partition_overwrite.py',
    conn_id='yarn_spark',
    application_args=["2022-05-31", "/user/master/data/events", "/user/username/data/events"],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores=1,
    executor_memory='1g'
)

verified_tags_candidates_d7 = SparkSubmitOperator(
    task_id='verified_tags_candidates_d7',
    dag=dag_spark,
    application='/home/username/verified_tags_candidates.py',
    conn_id='yarn_spark',
    application_args=["2022-05-31", "7", "100", "/user/username/data/events",
                      "/user/master/data/snapshots/tags_verified/actual",
                      "/user/username/data/analytics/verified_tags_candidates_d7"],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores=1,
    executor_memory='1g'
)

verified_tags_candidates_d84 = SparkSubmitOperator(
    task_id='verified_tags_candidates_d84',
    dag=dag_spark,
    application='/home/username/verified_tags_candidates.py',
    conn_id='yarn_spark',
    application_args=["2022-05-31", "84", "1000", "/user/username/data/events",
                      "/user/master/data/snapshots/tags_verified/actual",
                      "/user/username/data/analytics/verified_tags_candidates_d84"],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores=1,
    executor_memory='1g'
)

events_partitioned >> [verified_tags_candidates_d7, verified_tags_candidates_d84]