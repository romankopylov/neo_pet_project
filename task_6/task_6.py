import findspark
findspark.init()

import os
import pyspark
import json
import pandas as pd
import datetime
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from delta import *

# Получаем из файла json путь до дирекотрии с дельтами, наименование таблицы и список полей, являющихся ключами 
with open('./homework/params.json') as f:
    # Загружаем данные из файла
    data = json.load(f)
    
path = data['path']
tab_name = data['tab_name']
keys_list = data['keys_list']
location_delta = data['delta_path']
location_mirror = data['mirror_path']

# Создание SparkSession с подключением модуля Data Lake
builder = pyspark.sql.SparkSession.builder.appName("task_6") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Путем анализа каталогов с директории с дельтами генерируем список дельт
deltas_list = [fname for fname in os.listdir(path) if os.path.isdir(os.path.join(path, fname))]
deltas_list.sort()

# Работа с лог-файлом: 
# Проверяем, существует ли лог-файл
if os.path.exists('./homework/log.csv'):
    # Загружаем данные из файла
    loaded_df = spark.read.csv('./homework/log.csv', header=True, inferSchema=True, sep=";")
    # Фильтруем данные по наименованию таблицы, с которой в данные момент работаем 
    loaded_df = loaded_df.filter(loaded_df['TABLE'] == tab_name)
    # Получаем список ID дельт таблицы, загруженных ранее
    loadedId_list = loaded_df.select('ID').rdd.flatMap(lambda x: x).collect()

    # Удаляем из списка дельт deltas_list загруженные ранее дельты, которые определили на основании лога
    for value in loadedId_list:
        if str(value) in deltas_list:
            deltas_list.remove(str(value))

# На основании списка ключей таблицы генерируем строку с условиями формата "oldData.key1 = newData.key1 AND oldData.key2 = newData.key2..."
condition = ' and '.join(['oldData.{} = newData.{}'.format(x, x) for x in keys_list])
print(condition)

# Создаем пустой датафрейм для записи логов
log_df = pd.DataFrame(columns=['BEGIN', 'END', 'ID', 'TABLE'])

# для каждой дельты и списка дельт выполняем действия:
for delta in deltas_list:
    # Временная метка начала загрузки дельты
    time_begin = datetime.datetime.now()

    # Загружаем дельту в датафрейм из файла csv
    df = spark.read.csv(f'{path}/{delta}/{tab_name}.csv', header=True, inferSchema=True, sep=";")

    # Создаем объект DeltaTable, если он еще не существует
    delta_table = DeltaTable.createIfNotExists(spark) \
        .tableName(f"delta_{tab_name}") \
        .addColumns(df.schema) \
        .location(f"{location_delta}{tab_name}") \
        .execute()

    # Выполняем merge с датафреймом, в который загружена дельта, получем промежуточное стостояние зеркала
    delta_table.alias("oldData") \
        .merge(df.alias("newData"), condition) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

    # Временная метка окончания загрузки дельты
    time_end = datetime.datetime.now()
    # Добавляем строку в таблицу логов с данными о времени, дельте и таблице
    log_df.loc[len(log_df)] = {'BEGIN': time_begin, 'END': time_end, 'ID': delta, 'TABLE': tab_name}
    
# Выгружаем окончательную версию зеркала в фатафрейм и записываем в файл csv
mirr_df = delta_table.toDF()
mirr_df.write.mode("overwrite").csv(f"{location_mirror}mirr_{tab_name}.csv", header=True, sep=";")

# Добавляем данные логов из датафрейма в файл csv
# Проверка существования файла
if not os.path.exists('./homework/log.csv'):
    # Создание файла и запись заголовков и данных
    log_df.to_csv('./homework/log.csv', index=False, sep=";")
else:
    # Добавляем строки в конец файла
    log_df.to_csv('./homework/log.csv', mode='a', header=False, index=False, sep=";")