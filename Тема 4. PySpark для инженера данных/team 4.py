# Задание 4.1
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
                    .master("local") \
                    .appName("Learning DataFrames") \
                    .getOrCreate()

data = [('Max', 55),
        ('Yan', 53),
        ('Dmitry', 54),
        ('Ann', 25)
]

columns = ['Name', 'Age']
df = spark.createDataFrame(data=data, schema=columns)
df.printSchema()


# Задание 4.2
usersDF = spark.read.json(path = "hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/master/data/events/date=2022-05-25/part-00005-e1fe6a42-638b-4ad4-adc9-c7d0d312eef3.c000.json")
usersDF.printSchema()


# Задание 4.3
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
                    .master("local") \
                    .appName("PySpark Task 3") \
                    .getOrCreate()

df = spark.read.load(path="hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/master/data/snapshots/channels/actual/*.parquet", format="parquet")

df.write.option("header", True) \
        .partitionBy("channel_type") \
        .mode("append") \
        .parquet("hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/vyushmanov/analytics/test/")
# df_new = spark.read.parquet("hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/vyushmanov/analytics/test/")
df_new = spark.read.load("hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/vyushmanov/analytics/test/"
                         ,format="parquet")

df_new.select('channel_type').orderBy('channel_type').distinct().show()