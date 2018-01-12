# equoid-data-handler 

Count instances of data received from AMQP broker

## Quick start

1. Start your cluster `oc cluster up`

2. Start app 
```oc new-app --template=oshinko-java-spark-build-dc -p APPLICATION_NAME=equoid-data-handlerh -P GIT_REPO_URI=https://github.com/eldritchjs/equoid-data-handler -p APP_MAIN_CLASS=io.radanalytics.equoid.dataHandler -p APP_ARGS='<AMQP Broker Address> <AMQP Port> <AMQP Username> <AMQP Password>  <AMQP Address> <Infinispan Address>  <Infinispan Port>' -p APP_FILE=equoid-data-handler-1.0-SNAPSHOT.jar -p SPARK_OPTIONS='--driver-java-options=-Dvertx.cacheDirBase=/tmp'
```

3. Publish some data to queue `salesq` per https://github.com/eldritchjs/equoid-publisher
