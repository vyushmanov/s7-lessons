import os
from datetime import date, datetime, timedelta

from pyspark.sql import SparkSession
import pyspark.sql.functions as psf
from pyspark.sql.window import Window

def input_paths(date, depth):
    path_list =[]
    host = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020'
    for i in range(depth):
        start_date = datetime.strptime(date, '%Y-%m-%d') + timedelta(days=-i)
        start_date = datetime.strftime(start_date,'%Y-%m-%d')
        path = f'/user/vyushmanov/data/events/date={start_date}/event_type=message'
        path_list.append(f'{host}{path}')
    return path_list

def tag_tops(date, depth, spark):
    paths = input_paths(date, depth)
    print(paths)
    df_messages = spark.read.parquet(*paths)
    all_tags = df_messages\
                .where("event.message_channel_to is not null")\
                .selectExpr(["event.message_from as user_id", "explode(event.tags) as tag"])\
                .groupBy("user_id","tag")\
                .agg(psf.count("tag").alias("tag_top"))
    all_tags.printSchema()
    window = Window().partitionBy("user_id").orderBy(psf.col('tag_top').desc(),psf.col('tag').desc())
    rang_tags = all_tags.withColumn("row_number", psf.row_number().over(window))
    df_top_1 = rang_tags.where(("row_number = 1")).drop('row_number','tag_top').withColumnRenamed('tag', 'tag_top_1')
    df_top_2 = rang_tags.where(("row_number = 2")).drop('row_number','tag_top').withColumnRenamed('tag', 'tag_top_2')
    df_top_3 = rang_tags.where(("row_number = 3")).drop('row_number','tag_top').withColumnRenamed('tag', 'tag_top_3')
    df_res = df_top_1.join(df_top_2, on='user_id', how='left') \
            .join(df_top_3, on='user_id', how='left')
    return df_res

spark = SparkSession.builder \
                    .master("yarn") \
                    .appName("7.5.3.1") \
                    .getOrCreate()


host = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020'

tag_tops('2022-06-04', 5, spark).repartition(1).write.parquet(f'{host}/user/vyushmanov/data/tmp/tag_tops_06_04_5')

tag_tops('2022-05-04', 5, spark).repartition(1).write.parquet(f'{host}/user/vyushmanov/data/tmp/tag_tops_05_04_5')

tag_tops('2022-05-04', 1, spark).repartition(1).write.parquet(f'{host}/user/vyushmanov/data/tmp/tag_tops_05_04_1')
