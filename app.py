# needs: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.1.0 org.postgresql:postgresql:42.1.1

import argparse
import logging
import os

from flask import Flask, request, jsonify, render_template
#from multiprocessing import Process, Value

#from pyspark.sql import SparkSession
import stomp
import psycopg2

class StompListener(stomp.ConnectionListener):
    def __init__(self, conn):
        self.conn = conn
    def on_error(self, headers, message):
        print('received an error "%s"' % message)
    def on_message(self, headers, itemID):
        # Place in postgres
        cur = self.conn.cursor()
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
        self.conn.commit()


conn = psycopg2.connect("""
    dbname=salesdb user=daikon password=daikon host=postgresql port=5432
    """)
cur = conn.cursor()

parser = argparse.ArgumentParser(description='Count words on an AMQ topic')
parser.add_argument('--servers', help='The AMQP server', default='broker-amq-stomp')
parser.add_argument('--port', help = 'The AMQP port', default='61613')
parser.add_argument('--queue', help='Queue to consume', default='salesq')
args = parser.parse_args()

server = os.getenv('SERVERS', args.servers)
port = int(os.getenv('PORT', args.port))
queue = os.getenv('QUEUE', args.queue)

sl = StompListener(conn)
dest = '/queue/' + queue
c = stomp.Connection([(server, port)])
c.set_listener('', sl)
c.start()
c.connect('daikon', 'daikon', wait=True)
c.subscribe(destination=dest, id=1, ack='auto')

app = Flask(__name__)

#spark = SparkSession.builder.appName("grafzhal").getOrCreate()

def top(request):
#   results = spark.sql("SELECT * FROM results ORDER BY count DESC LIMIT {}" \
#                        .format(int(request.args.get('n') or 10))) \
#                        .collect()
   cur.execute("SELECT * FROM sales ORDER BY quantity DESC LIMIT 10")
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
