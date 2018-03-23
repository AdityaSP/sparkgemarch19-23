from __future__ import print_function
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext

def process(time, rdd):
    print("**************************")
    print(rdd.collect())
    rdd = rdd.map(lambda x : [x])
    if rdd.count() > 0:
        rdddf = sqlc.createDataFrame(rdd)
        rdddf.show()
        rdddf.write.save(mode='append', format='text',path='/tmp/file1')

sc =SparkContext('local[2]','tcpport')
sqlc = SQLContext(sc)
ssc = StreamingContext(sc, 5)

kds = KafkaUtils.createStream(ssc, 'ip-172-31-88-97:2181', 'consumer2', {'mar23': 1})
lines = kds.map(lambda x: x[1])
#lines = ssc.socketTextStream('ip-172-31-81-90', 12345)
#inp = lines.flatMap(lambda x : x.split())

counts = lines.flatMap(lambda line: line.split(" "))\
                  .map(lambda word: (word, 1))\
                  .reduceByKey(lambda a, b: a+b)
lines.foreachRDD(process)
ssc.start()
ssc.awaitTermination()
