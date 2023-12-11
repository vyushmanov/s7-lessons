import os
from datetime import date, datetime, timedelta

from pyspark.sql import SparkSession
import pyspark.sql.functions as psf
from pyspark.sql.window import Window


def input_paths(date, depth):
    path_list = []
    host = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020'
    for i in range(depth):
        start_date = datetime.strptime(date, '%Y-%m-%d') + timedelta(days=-i)
        start_date = datetime.strftime(start_date, '%Y-%m-%d')
        path = f'/user/vyushmanov/data/events/date={start_date}/event_type=reaction'
        path_list.append(f'{host}{path}')
    return path_list


def calculate_user_interests(date, depth, spark):
    paths = input_paths(date, depth)

    df_messages = spark.read.parquet(*paths)
    all_tags = df_messages \
        .where("event.message_channel_to is not null") \
        .selectExpr(["event.message_from as user_id", "explode(event.tags) as tag"]) \
        .groupBy("user_id", "tag") \
        .agg(psf.count("tag").alias("tag_top"))
    #     all_tags.printSchema()
    window = Window().partitionBy("user_id").orderBy(psf.col('tag_top').desc(), psf.col('tag').desc())
    rang_tags = all_tags.withColumn("row_number", psf.row_number().over(window))
    df_top_1 = rang_tags.where(("row_number = 1")).drop('row_number', 'tag_top').withColumnRenamed('tag', 'tag_top_1')
    df_top_2 = rang_tags.where(("row_number = 2")).drop('row_number', 'tag_top').withColumnRenamed('tag', 'tag_top_2')
    df_top_3 = rang_tags.where(("row_number = 3")).drop('row_number', 'tag_top').withColumnRenamed('tag', 'tag_top_3')
    df_res = df_top_1.join(df_top_2, on='user_id', how='left') \
        .join(df_top_3, on='user_id', how='left')

    df_reaction = spark.read \
        .option("basePath", f"{host}/user/vyushmanov/data/events") \
        .parquet(*paths) \
        .where("event_type='reaction'") \
        .selectExpr(["event.message_id"
                        , "event.reaction_from as user_id"
                        , "event.reaction_type"])
    df_message = spark.read \
        .parquet(f'{host}/user/vyushmanov/data/events') \
        .where("event.message_channel_to is not null") \
        .selectExpr(["event.message_id"
                        , "explode(event.tags) as tag"])

    df_tags = df_reaction.join(df_message, on='message_id', how='inner') \
        .groupBy('reaction_type', "user_id", 'tag') \
        .agg(psf.count("tag").alias("tag_top"))
    window = Window().partitionBy('reaction_type', "user_id").orderBy(psf.col('tag_top').desc(), psf.col('tag').desc())
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
        .join(df_like_tag_top_1, on='user_id', how='full') \
        .join(df_like_tag_top_2, on='user_id', how='full') \
        .join(df_like_tag_top_3, on='user_id', how='full') \
        .join(df_dislike_tag_top_1, on='user_id', how='full') \
        .join(df_dislike_tag_top_2, on='user_id', how='full') \
        .join(df_dislike_tag_top_3, on='user_id', how='full')

    return df_res


spark = SparkSession.builder \
    .master("yarn") \
    .appName("7.5.3.3") \
    .getOrCreate()

host = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020'

calculate_user_interests('2022-04-04', 5, spark).write.mode("overwrite").parquet(
    f'{host}/user/vyushmanov/data/tmp/user_interests_04_04_5')
calculate_user_interests('2022-05-04', 5, spark).write.mode("overwrite").parquet(
    f'{host}/user/vyushmanov/data/tmp/user_interests_05_04_5')
calculate_user_interests('2022-04-04', 1, spark).write.mode("overwrite").parquet(
    f'{host}/user/vyushmanov/data/tmp/user_interests_04_04_1')
