"""
spark-submit --packages org.apache.spark:spark-streaming-kafka_2.10:1.5.0,TargetHolding/pyspark-cassandra:0.1.5 --conf spark.cassandra.connection.host=127.0.0.1 example.py my-topic
"""

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import pyspark_cassandra
from pyspark_cassandra import streaming
from datetime import datetime
import sys

# Create a StreamingContext with batch interval of 3 second
sc = SparkContext("spark://MASTER:7077", "myAppName")
ssc = StreamingContext(sc, 3)

topic = sys.argv[1]
kafkaStream = KafkaUtils.createStream(ssc, "MASTER_IP:2181", "topic", {topic: 4})

raw = kafkaStream.flatMap(lambda kafkaS: [kafkaS])
time_now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
clean = raw.map(lambda xs: xs[1].split(","))

# Test timestamp 1 and timestamp 2
# times = clean.map(lambda x: [x[1], time_now])
# times.pprint()

# test subtract new time from old time
# x = clean.map(lambda x:
#             (datetime.strptime(x[1], '%Y-%m-%d %H:%M:%S.%f') -
#             datetime.strptime(time_now, '%Y-%m-%d %H:%M:%S.%f')).seconds)
# x.pprint()


# Match table fields with dictionary keys
# this reads input of format
# partition, timestamp
my_row = clean.map(lambda x: {
      "timestamp":"now()",
      "style":"random('KUNG_FU','WUSHU','DRUNKEN_BOXING')",
      "action":"random('KICK','PUNCH','BLOCK','JUMP')",
      "weapon":"random('BROAD_SWORD','STAFF','CHAIR','ROPE')",
      "target":"random('HEAD','BODY','LEGS','ARMS')",
      "strength":"double(1.0,10.0)"
      "delta": (datetime.strptime(x[1], '%Y-%m-%d %H:%M:%S.%f') -
       datetime.strptime(time_now, '%Y-%m-%d %H:%M:%S.%f')).microseconds,
      
# my_row.pprint()
      parsed = kafka_stream.map(lambda (k, v): json.loads(v))
      summed = parsed.map(lambda event: (event['site_id'], 1)).\
                reduceByKey(lambda x,y: x + y).\
                map(lambda x: {"site_id": x[0], "ts": str(uuid1()), "pageviews": x[1]})

my_row.saveToCassandra("KEYSPACE", "TABLE_NAME")

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
