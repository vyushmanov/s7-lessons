
# Смотрим список команд в hdfs
! hdfs dfs -help

# Смотрим состав каталога
! hdfs dfs -ls /user/master/data/events

# Смотрим имя файла в каталоге
! hdfs dfs -ls /user/master/data/events/date=2022-06-21

# Смотрим содержание файла
! hdfs dfs -text /user/master/data/events/date=2022-06-21/part-00173-e1fe6a42-638b-4ad4-adc9-c7d0d312eef3.c000.json

# Копируем файл из HDFS в локальную файловую систему
! hdfs dfs -copyToLocal /user/master/data/events/date=2022-06-21/part-00173-e1fe6a42-638b-4ad4-adc9-c7d0d312eef3.c000.json localfile

# Создание папок
# Контроль содержания папки
!hdfs dfs -mkdir -p /user/vyushmanov/temp
!hdfs dfs -mkdir -p /user/vyushmanov/tmp
! hdfs dfs -ls /user/vyushmanov

# Удаление лишней папки
# Контроль содержания папки
! hdfs dfs -rmdir /user/vyushmanov/temp
! hdfs dfs -ls /user/vyushmanov

# Привоение прав и контроль
! hdfs dfs -chmod 666 /user/vyushmanov/tmp/file.json
! hdfs dfs -ls /user/vyushmanov/tmp

# Смотрим на содержание файла
! hdfs dfs -text /user/vyushmanov/tmp/file.json

# Копируем файл из HDFS в локальную файловую систему
! hdfs dfs -copyToLocal /user/vyushmanov/tmp/file.json file.json


