from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
from pyspark.streaming.kafka import KafkaUtils
import os
import json
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /Users/mamer/Downloads/spark-3.0.0-preview2-bin-hadoop2.7/bin/Individual/spark-streaming-kafka-0-8-assembly_2.11-2.4.5.jar pyspark-shell'

def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)

def get_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']

def process_rdd(time, rdd):
    print("----------- %s -----------" % str(time))
    try:
        # Get spark sql singleton context from the current context
        sql_context = get_sql_context_instance(rdd.context)

        # convert the RDD to Row RDD
        row_rdd = rdd.map(lambda w: Row(word=w[0], word_count=w[1]))

        # create a DF from the Row RDD
        words_df = sql_context.createDataFrame(row_rdd)
        #print("start printing first 5 rows of words_df")
        #words_df.show(5)
        #print("finish printing words_df")
        #print("Starting 1")
        # Register the dataframe as table
        words_df.registerTempTable("words")
        # get the top 10 hashtags from the table using SQL and print them
        print("\n*** Top 10 Words ***")
        word_counts_df = sql_context.sql("select * from words order by word_count desc limit 10")
        word_counts_df.show()
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)

# create spark configuration

conf = SparkConf()
conf.setAppName("TwitterStreamApp")
# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
# create the Streaming Context from the above spark context with interval size 2 seconds
ssc = StreamingContext(sc, 2)
# setting a checkpoint to allow RDD recovery
ssc.checkpoint("TweetCheckpoint")

print("Yesssssssssssssssssssssssssssssssssssss")
print("Yesssssssssssssssssssssssssssssssssssss")
print("Yesssssssssssssssssssssssssssssssssssss")


kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'spark-streaming', {'twitter':1})
lines = kafkaStream.map(lambda x: x[1])
lines.pprint()
print("ddddddd")

counts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a+b)
counts.pprint()
print("4")

# do the processing for each RDD generated in each interval
counts.foreachRDD(process_rdd)
# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()
