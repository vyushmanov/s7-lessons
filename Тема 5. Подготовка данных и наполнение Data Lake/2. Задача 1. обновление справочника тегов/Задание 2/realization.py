import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as psf
from datetime import datetime, timedelta

def input_paths(date, depth):
    path_list =[]
    for i in range(depth):
        start_date = datetime.strptime(date, '%Y-%m-%d') + timedelta(days=-i)
        start_date = datetime.strftime(start_date,'%Y-%m-%d')
        path = f'/user/vyushmanov/data/events/date={start_date}/event_type=message'
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

path_list = input_paths('2022-05-31', 7)

df = spark.read.parquet(*path_list)
df.printSchema()

verified_tags = spark.read.load(path = f"{host}/user/master/data/snapshots/tags_verified/actual", format='parquet')

# df_exp = df.filter(psf.col('event.message_channel_to').isNotNull()).\
#         select(psf.explode(psf.col('event.tags')).alias("tag"), psf.col('event.message_from').alias('user')).distinct()
df_exp = df.where("event.message_channel_to is not null")\
        .selectExpr(["event.message_from as user", "explode(event.tags) as tag"])\
        .groupBy("tag")\
        .agg(psf.expr("count(distinct user) as suggested_count"))\
        .where("suggested_count >= 100")

# df_grp = df_exp.groupBy(psf.col('tag')).count().filter(psf.col('suggested_count') > 100)
df_res = df_exp.join(verified_tags, on='tag', how='leftanti')
df_res.show()

df_res.write.format('parquet').save(f'{host}/user/vyushmanov/data/analytics/candidates_d7_pyspark')