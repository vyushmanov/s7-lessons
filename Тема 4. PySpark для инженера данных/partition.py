import sys
from datetime import date as dt

import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

findspark.init()
findspark.find()


def main():
    cluster = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020'
    date = sys.argv[1]
    source_dir = sys.argv[2]
    target_dir = sys.argv[3]

    spark = SparkSession \
        .builder \
        .master("yarn") \
        .config("spark.driver.cores", "8") \
        .config("spark.driver.memory", "6g") \
        .appName("7.10.1") \
        .getOrCreate()

    events = spark.read.json(f"{cluster}{source_dir}")
    events.write.partitionBy("date", "event_type") \
        .mode("overwrite") \
        .parquet(f"{cluster}{target_dir}")


if __name__ == '__main__':
    main()