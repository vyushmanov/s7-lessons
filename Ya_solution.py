import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import datetime

def input_event_paths(base_path, date, depth):
    dt = datetime.datetime.strptime(date, '%Y-%m-%d')
    return [f"{base_path}/date={(dt-datetime.timedelta(days=x)).strftime('%Y-%m-%d')}" for x in range(int(depth))]

def main():
    date = sys.argv[1]
    days_count = sys.argv[2]
    events_base_path = sys.argv[3]
    interests_base_path = sys.argv[4]
    verified_tags_path = sys.argv[5]
    output_base_path = sys.argv[6]

    conf = SparkConf().setAppName(f"ConnectionInterestsJob-{date}-d{days_count}")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    messages = sql.read\
    .option("basePath", events_base_path)\
    .parquet(input_event_paths(events_base_path, date, days_count))\
    .where("event_type='message'")

    direct_messages = messages.where("event.message_to is not null")
    posts = messages.where("event.message_channel_to is not null")

    interests = sql.read.parquet(f"{interests_base_path}/date={date}")
    subscriptions = sql.read.parquet(events_base_path)\
    .where(f"event_type = 'subscription' and date <= '{date}'")

    verified_tags = sql.read.parquet(verified_tags_path)

    contacts = get_contacts(direct_messages)
    contact_interests = get_contact_interests(contacts, interests).cache()
    subs_interests = get_subs_interests(posts, subscriptions, verified_tags).cache()

    result = join_result(contact_interests, subs_interests)
    result.write.mode("overwrite").parquet(f"{output_base_path}/date={date}")


def get_contacts(direct_messages):
    return direct_messages\
    .select(F.col("event.message_from").alias("from"),
        F.col("event.message_to").alias("to"),
        F.explode(F.array(F.col("event.message_from"), F.col("event.message_to"))).alias("user_id"))\
    .withColumn("contact_id", F.when(F.col("user_id") == F.col("from"), F.col("to")).otherwise(F.col("from")))\
    .select("user_id", "contact_id")\
    .distinct()

def get_contact_interests(contacts, interests):
    return contacts\
    .withColumnRenamed("user_id", "u")\
    .join(interests, F.col("contact_id") == F.col("user_id"))\
    .transform(lambda df: add_tag_usage_count(df, "u"))\
    .transform(lambda df: add_tag_rank(df, "u"))\
    .groupBy("u")\
    .agg([top_direct_tag(c) for c in result_columns()])\
    .withColumnRenamed("u", "user_id")

def get_subs_interests(posts, subscriptions, verified_tags):
    post_tags = posts\
    .select(F.col("event.message_channel_to").alias("channel_id"),
    F.col("event.tags").alias("tags"))

    verified_sub_tags = post_tags\
    .join(subscriptions,
    (F.col("event.subscription_channel") == F.col("channel_id")))\
    .select(F.col("event.user").alias("user_id"), F.explode(F.col("tags")).alias("tag"))\
    .join(verified_tags, "tag", "left_semi")

    return verified_sub_tags\
    .groupBy("user_id", "tag")\
    .agg(F.countDistinct("*").alias("tag_count"))\
    .withColumn("rank", F.row_number().over(Window.partitionBy("user_id").orderBy(F.desc("tag_count"), F.desc("tag"))))\
    .where("rank <= 3")\
    .groupBy("user_id")\
    .pivot("rank", [1, 2, 3])\
    .agg(F.first("tag"))\
    .withColumnRenamed("1", "sub_verified_tag_top_1")\
    .withColumnRenamed("2", "sub_verified_tag_top_2")\
    .withColumnRenamed("3", "sub_verified_tag_top_3")

def join_result(contact_interests, subs_interests):
    return contact_interests\
        .join(subs_interests, "user_id", "full_outer")

def tag_columns(reaction):
    return [f"{reaction}_tag_top_{X}" for X in range(1, 4)]

def result_columns():
    return tag_columns("like") + tag_columns("dislike")

def add_tag_usage_count(df, key):
    res = df
    cols = result_columns()
    for c in cols:
        res = res.withColumn(c+'_count', F.count(c).over(Window.partitionBy(key, c)))
    return res

def add_tag_rank(df, key):
    res = df
    for c in result_columns():

res = res.withColumn(c+"_rank", F.row_number().over(Window.partitionBy(key).orderBy(F.desc(c), F.desc(c+"_count"))))
    return res

def top_direct_tag(column):
    return F.first(F.when(F.col(column+"_rank") == 1, F.col(column)), True).alias('direct_'+column)


if __name__ == "__main__":
    main()

/usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster connection_interests.py 2022-05-25 7 /user/prod/data/events /user/prod/data/analytics/user_interests_d7 /user/master/data/snapshots/tags_verified/actual /user/prod/data/analytics/connection_interests_d7

22:59
import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
from datetime import date, datetime
from airflow.models.baseoperator import chain

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

verified_tags_candidates_d7 = SparkSubmitOperator(
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

user_interests_d7 = SparkSubmitOperator(
task_id='user_interests_d7',
dag=dag_spark,
application ='/home/username/user_interests.py' ,
conn_id= 'yarn_spark',
application_args = ["2022-05-31", "7", "/user/username/data/events", "/user/username/data/analytics/user_interests_d7"],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 1,
executor_memory = '1g'
)

user_interests_d28 = SparkSubmitOperator(
task_id='user_interests_d28',
dag=dag_spark,
application ='/home/username/user_interests.py' ,
conn_id= 'yarn_spark',
application_args = ["2022-05-31", "28", "/user/username/data/events", "/user/username/data/analytics/user_interests_d28"],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 1,
executor_memory = '1g'
)

connection_interests_d7 = SparkSubmitOperator(
task_id='connection_interests_d7',
dag=dag_spark,
application ='/home/username/connection_interests.py' ,
conn_id= 'yarn_spark',
application_args = ["2022-05-31", "7", "/user/username/data/events", "/user/username/data/analytics/user_interests_d7", "/user/master/data/snapshots/tags_verified/actual",  "/user/username/data/analytics/connection_interests_d7"],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 1,
executor_memory = '1g'
)

events_partitioned >> [verified_tags_candidates_d7, verified_tags_candidates_d84]
events_partitioned >> user_interests_d7 >> connection_interests_d7
events_partitioned >> user_interests_d28


