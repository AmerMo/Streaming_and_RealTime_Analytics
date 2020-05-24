from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)

def get_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']

def process_rdd(time, rdd):
    print("----------- %s -----------" % str(time))
    try:
    ####
        # Get spark sql singleton context from the current context
        sql_context = get_sql_context_instance(rdd.context)
    
        # convert the RDD to Row RDD
        row_rdd = rdd.map(lambda w: Row(hashtag=w[0], hashtag_count=w[1]))
    
        # create a DF from the Row RDD
        hashtags_df = sql_context.createDataFrame(row_rdd)
        #print("start printing first 5 rows of words_df")
        #words_df.show(5)
        #print("finish printing words_df")
        #print("Starting 1")
        # Register the dataframe as table
        hashtags_df.registerTempTable("hashtags")
        # get the top 10 hashtags from the table using SQL and print them
        print("\n*** Top 10 Hashtags ***")
        hashtag_counts_df = sql_context.sql("select * from hashtags order by hashtag_count desc limit 10")
        hashtag_counts_df.show()
        #topwords = word_counts_df.map(lambda p: p.word)
        #print("words")
        #print(topwords.collect())
    
    
    ####
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
#TODO
ssc.checkpoint("TweetCheckpoint")
# read data from the port
#TODO
DataRead = ssc.socketTextStream("localhost", 17017)
# split each tweet into words
#TODO
words = DataRead.flatMap(lambda line: line.split(" "))
# filter the words to get only hashtags, then map each hashtag to be a pair of (hashtag,1)
hashtags = words.filter(lambda w: '#' in w).map(lambda x: (x, 1))
# adding the count of each hashtag to its last count
#tags_totals = hashtags.updateStateByKey(aggregate_tags_count)
# do processing for each RDD generated in each interval

tags_totals = hashtags.reduceByKeyAndWindow(lambda x, y: int(x) + int(y), lambda x, y: int(x) - int(y), 30, 10)
#tags_totals.pprint()

# do the processing for each RDD generated in each interval
tags_totals.foreachRDD(process_rdd)
# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()
