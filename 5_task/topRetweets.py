# -*- coding: utf-8 -*-
import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

zk_server = "localhost:2181" # сервер Zookeeper
topic = "retweets-kafka" # топик

# Функция для обновления значений количества слов
def updateTotalCount(currentCount, countState):
    if countState is None:
        countState = 0
    return sum(currentCount, countState)

# Создаем Spark Context
sc = SparkContext(appName="KafkaTwitterWordCount")

sc.setLogLevel("OFF")

# Создаем Streaming Context
ssc = StreamingContext(sc, 60)

# Объявляем checkpoint и указываем директорию в HDFS, где будут храниться значения
ssc.checkpoint("tmp_spark_streaming1")

# Создаем подписчика на поток от Kafka c топиком topic = "tweets-kafka"
kafka_stream = KafkaUtils.createStream(ssc, zk_server, "spark-streaming-consumer", {topic: 1})

# Трансформируем мини-batch 
retweets_stream = kafka_stream.map(lambda x: (x[1], 1)).reduceByKey(lambda x1, x2: x1 + x2)

retweets_total_count = retweets_stream.updateStateByKey(updateTotalCount)

top_retweets = retweets_total_count.transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))
id_count_tweet = top_retweets.map(lambda x: (eval(x[0])['tweet_id'], x[1]))

id_count_tweet.pprint(5)
# id_count_tweet.transform(lambda rdd: rdd.coalesce(1)).saveAsTextFiles("file:///home/cloudera/712/5_task/output/output")

windowed_data = top_retweets.reduceByKeyAndWindow(max, None, windowDuration=60 * 10, slideDuration=60 * 10)
windowed_data.map(lambda x: x[0]).pprint(5)

# Запускаем Spark Streaming
ssc.start()

# Ожидаем остановку
ssc.awaitTermination()
