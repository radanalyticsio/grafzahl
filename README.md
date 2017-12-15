# equoid-data-handler 

Count instances of data received from AMQP broker

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
1. Run the app
   ```bash
   spark/bin/spark-submit --jars ./libs/spark-streaming-amqp_2.11-0.3.2-SNAPSHOT.jar app.py
   ```

1. [Connect to the app](http://localhost:8080)

1. Publish some data to queue `salesq`
