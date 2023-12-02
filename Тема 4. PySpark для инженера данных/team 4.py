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

! hdfs dfs -ls hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/master/data/snapshots/channels/actual


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
        (322, "1"),
        (250, "2" ),
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
df_library = spark.createDataFrame(data=library, schema=columns_library )
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



# Задание 7.1
df_cache = df_join.cache()
df_cache.show()
df_cache.explain()


# Задание 7.2
import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

import findspark
findspark.init()
findspark.find()

from pyspark import SparkContext,SparkConf
sc = SparkContext.getOrCreate(SparkConf())
sc.setCheckpointDir(dirName="hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/vyushmanov/analytics/test_check")

df_checkpoint = df_join.checkpoint()
df_checkpoint.show()
df_checkpoint.explain()


# Задание 8.1 Добавление столбца с агрегатом
import pyspark.sql.functions as ps

df_new = df_join.withColumn("current_date", ps.current_date())
df_new.show()


# Задание 8.2 Выделение из метки времени часов, минут, секунд
df_new = df.withColumn("hour", ps.hour(ps.col('event.datetime')))\
    .withColumn('minute', ps.minute(ps.col('event.datetime')))\
    .withColumn('second', ps.second(ps.col('event.datetime')))\
    .orderBy(ps.col('event.datetime').desc())
df_new.show(10, True)


# Функции для работы с NULL-значениями
df_count = df.filter(ps.col('event.message_to').isNull()).count()

df_count = df.filter(ps.col('event.message_to').isNotNull()).count()
df_count


# Задание 8.3 Группировка
from pyspark.sql import SparkSession
import pyspark.sql.functions as ps

spark = SparkSession.builder \
                    .master("local") \
                    .appName("Learning DataFrames") \
                    .getOrCreate()

events = spark.read.json(path = "hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020//user/master/data/events/date=2022-05-31")
events.printSchema()

# Группировка с подсчетом количества
events.filter(ps.col('event_type')=='message').groupBy(ps.col('event.message_from')).count().show()

# Подсчет максимального значения в столбце
events.filter(ps.col('event_type')=='message').groupBy(ps.col('event.message_from')).count().select(ps.max(ps.col('count'))).show()

# Поиск максимальных значений после группировки
events.filter(ps.col('event_type')=='message').groupBy(ps.col('event.message_from')).count().orderBy(ps.col('count').desc()).show()

# собственно решение задания
event_day_max = events.filter((ps.col('event_type')=='reaction')&(ps.col('date')=='2022-05-25')).groupBy(ps.col('event.reaction_from')).count().select(ps.max(ps.col('count'))).show()
# переименование колонки
event_day_max = events.filter((ps.col('event_type')=='reaction')&(ps.col('date')=='2022-05-25')).groupBy(ps.col('event.reaction_from')).count().select(ps.max(ps.col('count'))).show()
event_day_max.withColumnRenamed('max(count)', 'max_count').show()


# Задание 9.1 Оконные функции
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# создаём объект оконной функции
window = Window().orderBy(F.asc('purchase_amount'))

# создаём колонку с рассчитанной статистикой по оконной функции
df_window = df.withColumn("row_number", F.row_number().over(window))

# выводим нужные колонки
df_window.select('dt', 'user_id', 'purchase_amount', 'row_number').show()

# Смещение строк в окне
pyspark.sql.functions.lag(col, offset=1) # предыдущая строка
pyspark.sql.functions.lead (col, offset=1) # информация из следующей строки


# Задание 9.3
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .master("yarn") \
    .appName("Lesson 9 Task 3") \
    .getOrCreate()

events = spark.read.json(
    "hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/master/data/events/date=2022-05-01")

window = Window().partitionBy('event.message_from').orderBy('event.datetime')

dfWithLag = events.withColumn("lag_7", F.lag("event.message_to").over(window))

dfWithLag.filter(F.col('event_type') == 'message') \
    .filter(F.col('lag_7').isNotNull()) \
    .select('event.message_from', 'event.message_to', 'lag_7') \
    .orderBy(F.col('event.message_from').desc()) \
    .show(10, True)

# Задание 9.4
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as ps

spark = SparkSession.builder \
    .master("local") \
    .appName("Learning DataFrames") \
    .getOrCreate()

data = [('2021-01-06', 3744, 63, 322),
        ('2021-01-04', 2434, 21, 382),
        ('2021-01-04', 2434, 32, 159),
        ('2021-01-05', 3744, 32, 159),
        ('2021-01-06', 4342, 32, 159),
        ('2021-01-05', 4342, 12, 259),
        ('2021-01-06', 5677, 12, 259),
        ('2021-01-04', 5677, 23, 499)
        ]

columns = ['dt', 'user_id', 'product_id', 'purchase_amount']

df = spark.createDataFrame(data=data, schema=columns)
df.show()

window = Window().partitionBy(ps.col('user_id'))

df_agg = df.withColumn('max', ps.max('purchase_amount').over(window))\
    .withColumn('min', ps.min('purchase_amount').over(window))\
    .select('user_id', 'max', 'min').distinct()
df_agg.show()


#Задание 10.1
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

import findspark
findspark.init()
findspark.find()

spark = SparkSession \
    .builder \
    .master("yarn") \
    .config("spark.driver.cores", "8") \
    .config("spark.driver.memory", "6g") \
    .appName("7.10.1") \
    .getOrCreate()

events = spark.read.json("hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/master/data/events/")
events.write.partitionBy("date", "event_type") \
    .mode("overwrite") \
    .parquet("hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/vyushmanov/data/events")

spark.read.parquet("hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/vyushmanov/data/events") \
    .orderBy(F.col('event.datetime').desc()).show(10)

! hdfs dfs -ls /user/vyushmanov/data/events


# Задание 10.2
import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext


def main():
    date = sys.argv[1]
    base_input_path = sys.argv[2]
    base_output_path = sys.argv[3]

    conf = SparkConf().setAppName("7.10.2--{}".format(date))
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    events = sql.read.json("{}/date={}".format(base_input_path, date))
    events.write.partitionBy('event_type').format('parquet').save('{}/date={}'.format(base_output_path, date))
    print(date, '\n',base_input_path, '\n', base_output_path)

if __name__ == "__main__":
    main()


# Задание 11.1
spark-submit --master yarn --num-executors 10 --deploy-mode cluster python_scripts.zip 2022-05-31


# Задание 11.2
spark-submit --master yarn --num-executors 10 --deploy-mode cluster python_scripts.zip 2023-10-29 hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/master/data/events/ hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/vyushmanov/data/events



################ Тестирование запуска py-файлов
################ test.py
import sys
from datetime import date as dt

def main():
    client_name = sys.argv[1]
    date = sys.argv[2]
    dir_name = sys.argv[3]

    if str(dt.today()) == date:
        print(client_name)
        current_dir = dir_name
        print('Your current directory is {}'.format(current_dir))
    else:
        print("Come back another day")

if __name__ == "__main__":
    main()

!/usr/bin/python3 test.py 'Bob' '2022-01-01' 'path/to/table/'
!spark-submit test.py 'Bob' '2022-01-01' 'path/to/table/'


################ partition.py
import sys
from datetime import date as dt
import os
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

def main():
    date = sys.argv[1]
    base_input_path = sys.argv[2]
    base_output_path = sys.argv[3]

    conf = SparkConf().setAppName("7.4.12-{}".format(date))
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    events = sql.read.json("{}/date={}".format(base_input_path, date))
    events.write.partitionBy('event_type').format('parquet').save('{}/date={}'.format(base_output_path, date))
    print(date, '\n',base_input_path, '\n', base_output_path)

if __name__ == "__main__":
    main()

!/usr/bin/python3 partition.py '2021-01-01' 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/master/data/events' 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/vyushmanov/data/events'
!spark-submit partition.py '2022-06-06' 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/master/data/events' 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/vyushmanov/data/events'
spark-submit partition.py '2021-01-01' 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/master/data/events' 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/vyushmanov/data/events'

!spark-submit --master yarn --num-executors 10 --deploy-mode cluster /lessons/partition.py  '2022-06-07' 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/master/data/events' 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/vyushmanov/data/events'


## Полезные команды

hdfs dfs -rm -r /user/vyushmanov/data/events/date=2022-06-21 # удаление папки
hdfs dfs -ls /user/vyushmanov/data/events # просмотр содержания папки
echo $PYSPARK_PYTHON # проверка пути к переменным


###### dag_bash.py
####################################################################################
import os
import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import date, datetime

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

# задаём базовые аргументы
default_args = {
    'start_date': datetime(2020, 1, 1),
    'owner': 'airflow'
}

# вызываем DAG
dag = DAG("bash_operator_dag",
          schedule_interval=None,
          default_args=default_args
         )

# объявляем задачу с Bash-командой, которая распечатывает дату
t1 = BashOperator(
    task_id='print_date',
    bash_command="/usr/bin/python3 /lessons/dags/partition.py 2022-06-21 hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/master/data/events hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/vyushmanov/data/events",
        retries=3,
        dag=dag
)

t1


###### dag_spark_submit.py -- не проверен
####################################################################################

import datetime from datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os

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
                        dag_id = "sparkoperator_demo",
                        default_args=default_args,
                        schedule_interval=None,
                        )

# объявляем задачу с помощью SparkSubmitOperator
spark_submit_local = SparkSubmitOperator(
                        task_id='spark_submit_task',
                        dag=dag_spark,
                        application ='/home/user/partition_overwrite.py' ,
                        conn_id= 'yarn_spark',
                        application_args = ["2020-05-01"],
                        conf={
            "spark.driver.maxResultSize": "20g"
        },
                        executor_cores = 2,
                        executor_memory = '2g'
                        )

spark_submit_local