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
12
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