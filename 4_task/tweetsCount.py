# -*- coding: utf-8 -*-
import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

zk_server = "localhost:2181" # сервер Zookeeper
topic = "tweets-kafka" # топик

# Функция для обновления значений количества слов
def updateTotalCount(currentCount, countState):
    if countState is None:
        countState = 0
    return sum(currentCount, countState)

# Создаем Spark Context
sc = SparkContext(appName="KafkaTwitterWordCount")

sc.setLogLevel("OFF")

# Создаем Streaming Context
ssc = StreamingContext(sc, 10)

# Объявляем checkpoint и указываем директорию в HDFS, где будут храниться значения
ssc.checkpoint("tmp_spark_streaming1")

# Создаем подписчика на поток от Kafka c топиком topic = "tweets-kafka"
kafka_stream = KafkaUtils.createStream(ssc, zk_server, "spark-streaming-consumer", {topic: 1})

# Трансформируем мини-batch 
lines = kafka_stream.map(lambda x: (eval(x[1])['name'], 1)).reduceByKey(lambda x1, x2: x1 + x2)

lines_window_1 = lines.reduceByKeyAndWindow(lambda x1, x2: x1 + x2, None, windowDuration=60, slideDuration=30)
lines_window_1.transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))
lines_window_1.pprint()

lines_window_2 = lines.reduceByKeyAndWindow(lambda x1, x2: x1 + x2, None, windowDuration=600, slideDuration=30)
lines_window_2.transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))
lines_window_2.pprint()

# Запускаем Spark Streaming
ssc.start()

# Ожидаем остановку
ssc.awaitTermination()
