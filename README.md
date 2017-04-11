# Graf Zahl

Count github event type instances on an Apache Kafka topic and present them to web clients

## Quick start

1. Install app requirements
   ```bash
   pip install -r requirement.txt
   ```

1. Get a copy of Apache Spark (requires 2.1.0+)
   ```bash
   mkdir spark
   curl https://www.apache.org/dist/spark/spark-2.1.0/spark-2.1.0-bin-hadoop2.7.tgz | tar zx -C spark --strip-components=1
   ```
1. [Setup Apache Kafka](https://kafka.apache.org/documentation.html#quickstart)

1. Run the app
   ```bash
   spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.1.0 app.py
   ```

1. [Connect to the app](http://localhost:8080)

1. Publish some words to topic `word-fountain`
