# needs: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.1.0

import logging

from flask import Flask, request, jsonify, render_template

from pyspark.sql import SparkSession


app = Flask(__name__)

spark = SparkSession.builder.appName("grafzhal").getOrCreate()


def top(request):
   results = spark.sql("SELECT * FROM results ORDER BY count DESC LIMIT {}" \
                       .format(int(request.args.get('n') or 10))) \
                  .collect()
   return (map(lambda x: x.value, results), map(lambda x: x['count'], results))

@app.route("/")
def ahahah():
    logging.debug('serving counts...')
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

spark \
  .readStream \
   .format("kafka") \
    .option("kafka.bootstrap.servers", "kafkanetes-kafka:9092") \
     .option("subscribe", "test") \
      .load() \
  .selectExpr("CAST(value AS STRING)") \
   .groupBy("value") \
    .count() \
  .writeStream \
   .outputMode("complete") \
    .format("memory") \
     .queryName("results") \
  .start()

app.run(host='0.0.0.0', port=8080)
