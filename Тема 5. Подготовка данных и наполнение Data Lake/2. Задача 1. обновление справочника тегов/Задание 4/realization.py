import sys
from datetime import datetime, timedelta
import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
import findspark

findspark.init()
findspark.find()

import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def input_paths(date, depth, host, path):
    path_list = []
    for i in range(depth):
        start_date = datetime.strptime(date, '%Y-%m-%d') + timedelta(days=-i)
        start_date = datetime.strftime(start_date, '%Y-%m-%d')
        path_ = '{}/date={}/event_type=message'.format(path, start_date)
        path_list.append(f'{host}{path_}')
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

    spark = SparkSession \
        .builder \
        .master("yarn") \
        .config("spark.driver.cores", "8") \
        .config("spark.driver.memory", "6g") \
        .appName("7.10.1") \
        .getOrCreate()

    df = spark.read.parquet(*path_list)
    df.printSchema()

    verified_tags = spark.read.load(path="{}{}".format(host, path_verified_tags)
                                    , format='parquet')
    verified_tags.printSchema()

    df_exp = df.where("event.message_channel_to is not null") \
        .selectExpr(["event.message_from as user", "explode(event.tags) as tag"]) \
        .groupBy("tag") \
        .agg(psf.expr("count(distinct user) as suggested_count")) \
        .where(f"suggested_count >= {border}")

    df_res = df_exp.join(verified_tags, on='tag', how='leftanti')
    df_res.write.format('parquet').save('{}{}/date={}'.format(host, path_target, date))


if __name__ == "__main__":
    main()