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

#######################################################################################################################
# Задание 6.1
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
                    .master("local") \
                    .appName("Learning DataFrames") \
                    .getOrCreate()
# данные первого датафрейма
book = [('Harry Potter and the Goblet of Fire', 'J. K. Rowling', 322),
        ('Nineteen Eighty-Four', 'George Orwell', 382),
        ('Jane Eyre', 'Charlotte Brontë', 159),
        ('Catch-22', 'Joseph Heller',  174),
        ('The Catcher in the Rye', 'J. D. Salinger',  168),
        ('The Wind in the Willows', 'Kenneth Grahame',  259),
        ('The Mayor of Casterbridge', 'Thomas Hardy',  300),
        ('Bad Girls', 'Jacqueline Wilson',  299)
]
# данные второго датафрейма
library = [
        ( 322, "1"),
        ( 250, "2" ),
        (400, "2"),
        (159, "1"),
        (382, "2"),
        (322, "1")
]
# названия атрибутов
columns = ['title', 'author', 'book_id']
columns_library = ['book_id', 'Library_id']
# создаём датафреймы
df = spark.createDataFrame(data=book, schema=columns)
df_library  = spark.createDataFrame(data=library, schema=columns_library )
df_join = df.join(df_library, on='book_id', how='leftanti')
df_join.select('title').show(10,False) # вывод с отменой ограничения количества строк


# Задание 6.2
df_join.select('title').count() # подсчет количества


# Задание 6.3
df_join_exist = df.join(df_library, on='book_id', how='inner')
df_join_exist.select('title').distinct().show(10,False) # уникальные названия


# Задание 6.4
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
                    .master("local") \
                    .appName("Learning DataFrames") \
                    .getOrCreate()
# данные первого датафрейма
book_1 = [('Harry Potter and the Goblet of Fire', 'J. K. Rowling', 322),
        ('Nineteen Eighty-Four', 'George Orwell', 382),
        ('Jane Eyre', 'Charlotte Brontë', 159),
        ('Catch-22', 'Joseph Heller',  174),
        ('The Catcher in the Rye', 'J. D. Salinger',  168),
        ('The Wind in the Willows', 'Kenneth Grahame',  259),
        ('The Mayor of Casterbridge', 'Thomas Hardy',  300),
        ('Bad Girls', 'Jacqueline Wilson',  299)
]
# данные второго датафрейма
book_2 = [
        ('Black Beauty',657 ,'Anna Sewell'),
        ('Artemis Fowl',558,'Eoin Colfer'),
        ('The Magic Faraway Tree', 567,'Enid Blyton'),
        ('The Witches', 567,'Roald Dahl'),
        ('Frankenstein',567 ,'Mary Shelley'),
        ('The Little Prince',557 ,'Antoine de Saint-Exupéry'),
        ('The Truth', 576 ,'Terry Pratchett')
]
# названия атрибутов
columns_1= ['title', 'author', 'book_id']
columns_2 = ['title', 'book_id', 'author']
# создаём датафреймы
df_1 = spark.createDataFrame(data=book_1 , schema=columns_1)
df_2  = spark.createDataFrame(data=book_2 , schema=columns_2)
df_union = df_1.unionByName(df_2)
df_union.show()