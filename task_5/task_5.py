import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

path = "/home/roman/homework/"

# Создание SparkSession
spark = SparkSession.builder.getOrCreate()

# Создание списка кортежей с данными
data = [(1, 'swimming', 'summer'),
        (2, 'rhythmic gymnastics', 'summer'),
        (3, 'athletics', 'summer'),
        (4, 'football', 'summer'),
        (5, 'weightlifting', 'summer'),
        (6, 'hockey', 'winter'),
        (7, 'wrestling', 'winter'),
        (8, 'badminton', 'winter'),
        (9, '3x3 basketball', 'winter'),
        (10, 'water polo', 'winter'),]

# Создание DataFrame
schema = "row_id BIGINT, discipline STRING, season STRING"
discDF = spark.createDataFrame(data, schema)

# Сохранение disciplines в csv-файл
discDF.repartition(1).write.csv(f"{path}disciplines.csv", header=True, sep="\t")

# Подсчет количества спортсменов в каждой дисциплине
countDF = athDF.groupBy('Discipline').count().orderBy('count', ascending=False)

# сохранение count_by_discipline в формате parquet в указанную директорию
countDF.write.parquet(f"{path}count_by_disciplines.parquet")

# Преобразование данных колонки discipline в нижний регистр
countDF = countDF.withColumn("discipline", F.lower(countDF.Discipline))

# Объединение датафреймов по колонке "discipline"
resultDF = countDF.join(discDF, on='discipline', how='inner').select('discipline', 'count')

# сохранение result в формате parquet в указанную директорию
resultDF.write.parquet(f"{path}result.parquet")