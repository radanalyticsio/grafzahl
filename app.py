# needs: spark-submit --packages io.radanalytics:spark-streaming-amqp_2.11:0.3.2 [org.postgresql:postgresql:42.1.1]
import argparse
import logging
import os

from flask import Flask, request, jsonify, render_template
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext

import json
import psycopg2
from amqp import AMQPUtils

conn = psycopg2.connect("""
    dbname=salesdb user=daikon password=daikon host=postgresql port=5432
    """)
curs = conn.cursor()

parser = argparse.ArgumentParser(description='Count sales on an AMQ queue')
parser.add_argument('--servers', help='The AMQP server', default='broker-amq-amqp')
parser.add_argument('--port', help = 'The AMQP port', default='5672')
parser.add_argument('--queue', help='Queue to consume', default='salesq')
args = parser.parse_args()

server = os.getenv('SERVERS', args.servers)
port = int(os.getenv('PORT', args.port))
queue = os.getenv('QUEUE', args.queue)

def getSale(jsonMsg):
    data = json.loads(jsonMsg)
    return data["body"]["section"]

def storeSale(msg)
    _conn = psycopg2.connect("""
        dbname=salesdb user=daikon password=daikon host=postgresql port=5432
        """)
    cur = _conn.cursor()        

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
        SET quantity = quantity + 1
        WHERE itemid = %s;
        """,
        (itemID,))
    conn.commit()
    cur.close()
    conn.close()

def sendSale(rdd):    
    rdd.foreach(lambda record: storeSale(record))

app = Flask(__name__)

#batchIntervalSeconds = 5

#spark = SparkSession.builder \
#        .appName("equoid-data-handler") \
#        .config("spark.streaming.receiver.writeAheadLog.enable", "true") \
#        .getOrCreate()

def makeStream():
    sc = spark.sparkContext 
    ssc = StreamingContext(sc, batchIntervalSeconds)
    ssc.checkpoint("/tmp/spark-streaming-amqp")

    receiveStream = AMQPUtils.createStream(ssc, \
        "broker-amq-amqp", \
        5672, \
        "daikon", \
        "daikon", \
        "salesq")

    sales = receiveStream \
            .map(getSale) \
            .foreachRDD(sendSale)

    return ssc

#counts = msgs.map(lambda item: (item, 1)).reduceByKey(lambda a, b: a + b)
#counts.pprint()

#ssc = StreamingContext.getActiveOrCreate("/tmp/spark-streaming-amqp",makeStream)
#ssc.start()
#ssc.awaitTerminationOrTimeout(batchIntervalSeconds * 2)

def top(request):
    curs.execute("SELECT * FROM sales ORDER BY quantity DESC LIMIT {}" \
        .format(int(request.args.get('n') or 10)))
    results = curs.fetchall()
    return([x[0] for x in results],[x[1] for x in results])

@app.route("/")
def ahahah():
    logging.debug('serving up sales...')
    categories, data = top(request)
    return render_template('index.html',
                           categories=categories,
                           data=data)

@app.route("/data")
def dataonly():
    logging.debug('serving sales...')
    categories, data = top(request)
    data.insert(0, "counts")
    return jsonify({"categories": categories, "data": [data]})


#logging.basicConfig(level=logging.DEBUG)

app.run(host='0.0.0.0', port=8080)
