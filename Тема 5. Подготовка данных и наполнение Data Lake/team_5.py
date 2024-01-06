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







## Задание 6 ##
#############################################################################################
## DAG ##
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

get_interests_d7 = SparkSubmitOperator(
    task_id='get_interests_d7',
    dag=dag_spark,
    application='/lessons/dags/interests_def.py',
    conn_id='yarn_spark',
    application_args=['2022-05-25'
                      ,'7'
                      ,'/user/vyushmanov/data/events'
                      ,'/user/vyushmanov/analytics/user_interests_d7'],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores=1,
    executor_memory='1g'
)

get_interests_d28 = SparkSubmitOperator(
    task_id='get_interests_d28',
    dag=dag_spark,
    application='lessons/dags/interests_def.py',
    conn_id='yarn_spark',
    application_args=['2022-05-25'
                      ,'28'
                      ,'/user/vyushmanov/data/events'
                      ,'/user/vyushmanov/analytics/user_interests_d28'],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores=2,
    executor_memory='1g'
)

get_interests_d7 >> get_interests_d28



## interests_def.py ##
################################################################################################
import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as psf
from pyspark.sql.window import Window
from datetime import date, datetime, timedelta

import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'


def main():
    date = sys.argv[1]
    days_count = sys.argv[2]
    events_base_path = sys.argv[3]
    output_base_path = sys.argv[4]

    spark = SparkSession.builder \
        .master("yarn") \
        .appName(f"CalculateUserInterests-{date}-d{days_count}") \
        .getOrCreate()

    host = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020'

    calculate_user_interests(date, days_count, spark, host, events_base_path) \
        .write.mode("overwrite") \
        .parquet(f'{host}{output_base_path}')


def input_paths(date, depth, host, events_base_path):
    path_messages = []
    path_reactions = []
    for i in range(int(depth)):
        start_date = datetime.strptime(date, '%Y-%m-%d') + timedelta(days=-i)
        start_date = datetime.strftime(start_date, '%Y-%m-%d')
        path = f'{events_base_path}/date={start_date}'
        path_messages.append(f'{host}{path}')
        path_reactions.append(f'{host}{path}/event_type=reaction')
    return path_messages, path_reactions


def calculate_user_interests(date, depth, spark, host, events_base_path):
    path_messages, path_reactions = input_paths(date, depth, host, events_base_path)

    df_messages = spark.read \
        .option("basePath", f"{host}{events_base_path}") \
        .parquet(*path_messages)
    window = Window().partitionBy()
    all_tags = df_messages \
        .where("event.message_channel_to is not null") \
        .withColumn('date_', psf.max('date').over(window)) \
        .selectExpr(["event.message_from as user_id", "explode(event.tags) as tag"]) \
        .groupBy("user_id", "tag") \
        .agg(psf.count("tag").alias("tag_top"))
    window = Window().partitionBy("user_id").orderBy(psf.col('tag_top').desc(), psf.col('tag').desc())
    rang_tags = all_tags.withColumn("row_number", psf.row_number().over(window))

    df_top_1 = rang_tags.where(("row_number = 1")).drop('row_number', 'tag_top').withColumnRenamed('tag', 'tag_top_1')
    df_top_2 = rang_tags.where(("row_number = 2")).drop('row_number', 'tag_top').withColumnRenamed('tag', 'tag_top_2')
    df_top_3 = rang_tags.where(("row_number = 3")).drop('row_number', 'tag_top').withColumnRenamed('tag', 'tag_top_3')
    df_res = df_top_1.join(df_top_2, on=['user_id'], how='left') \
        .join(df_top_3, on=['user_id'], how='left')

    df_reaction = spark.read \
        .option("basePath", f"{host}{events_base_path}") \
        .parquet(*path_reactions) \
        .where("event_type='reaction'") \
        .selectExpr(["event.message_id as message_id"
                        , "event.reaction_from as user_id"
                        , "event.reaction_type as reaction_type"])
    # df_reaction.printSchema()

    df_message = spark.read \
        .json(f'{host}/user/master/data/events') \
        .where("event.message_channel_to is not null") \
        .selectExpr(["event.message_id as message_id"
                        , "explode(event.tags) as tag"])
    # df_message.printSchema()

    df_tags = df_reaction \
        .join(df_message, on='message_id', how='inner')
    # df_tags.printSchema()

    df_tags = df_tags \
        .groupBy('reaction_type', "user_id", 'tag') \
        .agg(psf.count("tag").alias("tag_top"))
    window = Window() \
        .partitionBy('reaction_type', "user_id") \
        .orderBy(psf.col('tag_top').desc(), psf.col('tag').desc())
    reaction_rang_tags = df_tags.withColumn("row_number", psf.row_number().over(window))
    df_like_tag_top_1 = reaction_rang_tags.where(("reaction_type = 'like' and row_number = 1")) \
        .drop('reaction_type', 'row_number', 'tag_top') \
        .withColumnRenamed('tag', 'like_tag_top_1')
    df_like_tag_top_2 = reaction_rang_tags.where(("reaction_type = 'like' and row_number = 2")) \
        .drop('reaction_type', 'row_number', 'tag_top') \
        .withColumnRenamed('tag', 'like_tag_top_2')
    df_like_tag_top_3 = reaction_rang_tags.where(("reaction_type = 'like' and row_number = 3")) \
        .drop('reaction_type', 'row_number', 'tag_top') \
        .withColumnRenamed('tag', 'like_tag_top_3')
    df_dislike_tag_top_1 = reaction_rang_tags.where(("reaction_type = 'dislike' and row_number = 1")) \
        .drop('reaction_type', 'row_number', 'tag_top') \
        .withColumnRenamed('tag', 'dislike_tag_top_1')
    df_dislike_tag_top_2 = reaction_rang_tags.where(("reaction_type = 'dislike' and row_number = 2")) \
        .drop('reaction_type', 'row_number', 'tag_top') \
        .withColumnRenamed('tag', 'dislike_tag_top_2')
    df_dislike_tag_top_3 = reaction_rang_tags.where(("reaction_type = 'dislike' and row_number = 3")) \
        .drop('reaction_type', 'row_number', 'tag_top') \
        .withColumnRenamed('tag', 'dislike_tag_top_3')

    df_res = df_res \
        .join(df_like_tag_top_1, on=['user_id'], how='full') \
        .join(df_like_tag_top_2, on=['user_id'], how='full') \
        .join(df_like_tag_top_3, on=['user_id'], how='full') \
        .join(df_dislike_tag_top_1, on=['user_id'], how='full') \
        .join(df_dislike_tag_top_2, on=['user_id'], how='full') \
        .join(df_dislike_tag_top_3, on=['user_id'], how='full') \
        .withColumn('date', psf.to_date(psf.lit('2022-05-04')))

    return df_res


if __name__ == "__main__":
    main()


#!/usr/bin/python3 /lessons/interests_def.py '2022-05-04' '5' '/user/vyushmanov/data/events' '/user/vyushmanov/analytics/user_interests_d5'



### 7.4.5 Задание 1
import pyspark
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

date = '2022-05-25'
depth = 7
host = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020'
path_source = f'/user/vyushmanov/data/events'
path_verified = ''
path_target = '/user/vyushmanov/analytics/user_interests_d7'

spark = SparkSession.builder \
                    .master("yarn") \
                    .appName(f"7.5.4.1_{date}") \
                    .getOrCreate()

def input_paths(date, depth, host, path_source):
    path = []
    for i in range(int(depth)):
        start_date = datetime.strptime(date, '%Y-%m-%d') + timedelta(days=-i)
        start_date = datetime.strftime(start_date, '%Y-%m-%d')
        item = f"{host}{path_source}/date={date}"
        path.append(item)
    return path

paths = input_paths(date, depth, host, path_source)
df_base = spark.read.parquet(*paths).where('event_type="message"')

df_interests = spark.read.parquet(f'{host}{path_interests}')\
        .selectExpr(['user_id'
                     ,'like_tag_top_1 as direct_like_tag_top_1'
                     ,'like_tag_top_2 as direct_like_tag_top_2'
                     ,'like_tag_top_3 as direct_like_tag_top_3'
                     ,'dislike_tag_top_1 as direct_dislike_tag_top_1'
                     ,'dislike_tag_top_2 as direct_dislike_tag_top_2'
                     ,'dislike_tag_top_3 as direct_dislike_tag_top_3'])

df_send_to = df_base.where("event.message_to is not null")\
        .selectExpr(["event.message_from as user_id", "event.message_to as contact_id"])
df_send_from = df_base.where("event.message_to is not null")\
        .selectExpr(["event.message_from as contact_id", "event.message_to as user_id"])
df_contacts = df_send_to.union(df_send_from)

df_res = df_interests.withColumn('id', psf.col('user_id'))\
    .join(df_contacts.withColumn('id', psf.col('contact_id')).drop('user_id'), on='id', how='left')\
    .drop('id','contact_id')

df_message_chan = df_base.where("event.message_channel_to is not null")\
        .selectExpr(["event.channel_id as channel_id"
                     ,"explode(event.tags) as tag"])

df_users_subscriptions = spark.read.parquet(f'{host}{path_source}').where('event_type = "subscription"')\
        .selectExpr(["event.user as id"
                     ,"event.subscription_channel as channel_id"])\
        .orderBy('id')

df_verified_tag = spark.read.parquet(f'{host}/user/master/data/snapshots/tags_verified/actual')

df_channel_tags = df_users_subscriptions\
        .join(df_message_chan, on='channel_id', how='inner')\
        .filter('tag is not null')\
        .groupBy('tag')\
        .agg(psf.count('id'))\
        .join(df_verified_tag, on='tag', how='inner')\
#         .selectExpr(["id as user_id"
#                      ,"tag as verified_tag"])


# Файлы
# run_jobs.sh
# connection_interests.py