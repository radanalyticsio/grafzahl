# needs: spark-submit --packages io.radanalytics:spark-streaming-amqp_2.11:0.3.2 [org.postgresql:postgresql:42.1.1]
import argparse
import logging
import os

from flask import Flask, request, jsonify, render_template
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext

import psycopg2
from amqp import AMQPUtils

conn = psycopg2.connect("""
    dbname=salesdb user=daikon password=daikon host=postgresql port=5432
    """)
cur = conn.cursor()

parser = argparse.ArgumentParser(description='Count sales on an AMQ queue')
parser.add_argument('--servers', help='The AMQP server', default='broker-amq-amqp')
parser.add_argument('--port', help = 'The AMQP port', default='61613')
parser.add_argument('--queue', help='Queue to consume', default='salesq')
args = parser.parse_args()

server = os.getenv('SERVERS', args.servers)
port = int(os.getenv('PORT', args.port))
queue = os.getenv('QUEUE', args.queue)

def handleMsg(msg):
    itemID = msg
    return itemID

def storeSale(msg):

    cur.execute("""
        SELECT * FROM sales
        WHERE itemid = %s;
        """,
        (itemID,))
    if(cur.fetchone()==None):
        cur.execute("""
        INSERT INTO sales(itemid, quantity)
        VALUES(%s, %s);
        """,
        (itemID, 1))
    else:
        cur.execute("""
        UPDATE sales
        SET quantit = quantity + 1
        WHERE itemid = %s;
        """,
        (itemID,))
    conn.commit()

app = Flask(__name__)

def createStreamingContext(spark):
    sc = spark.sparkContext 
    ssc = StreamingContext(sc, 1)
    ssc.checkpoint("/tmp/spark-streaming-amqp")

    receiveStream = AMQPUtils.createStream(ssc, "broker-amq-amqp", 5672, "daikon", "daikon", "salesq")

    counts = receiveStream.countByWindow(5,5).collect()
    print(counts)

    return ssc


spark = SparkSession.builder.appName("equoid-data-handler").config("spark.streaming.receiver.writeAheadLog.enable", "true").getOrCreate()
ssc = StreamingContext.getOrCreate("/tmp/spark-streaming-amqp", createStreamingContext(spark))

ssc.start()
ssc.awaitTermination()

def top(request):
#   results = spark.sql("SELECT * FROM results ORDER BY count DESC LIMIT {}" \
#                        .format(int(request.args.get('n') or 10))) \
#                        .collect()
   cur.execute("SELECT * FROM sales ORDER BY quantity DESC LIMIT {}" \
                .format(int(request.args.get('n') or 10)))
   results = cur.fetchall()
   return([x[0] for x in results],[x[1] for x in results])
#   return (map(lambda x: x.value, results), map(lambda x: x['count'], results))

@app.route("/")
def ahahah():
    logging.debug('serving up counts...')
    categories, data = top(request)
    return render_template('index.html',
                           categories=categories,
                           data=data)

@app.route("/data")
def dataonly():
    logging.debug('serving data...')
    categories, data = top(request)
    data.insert(0, "counts")
    return jsonify({"categories": categories, "data": [data]})


#logging.basicConfig(level=logging.DEBUG)

"""
spark \
  .readStream \
   .format("kafka") \
    .option("kafka.bootstrap.servers", servers) \
     .option("subscribe", topic) \
      .load() \
  .selectExpr("CAST(value AS STRING)") \
   .groupBy("value") \
    .count() \
  .writeStream \
   .outputMode("complete") \
    .format("memory") \
     .queryName("results") \
  .start()
"""
app.run(host='0.0.0.0', port=8080)
