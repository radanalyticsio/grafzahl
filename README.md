# equoid-data-handler 

Count instances of data received from AMQP broker

## Quickstart on cluster

### DataHandler class
1. Start your cluster `oc cluster up`

2. Start app 
```
oc new-app --template=oshinko-java-spark-build-dc -p APPLICATION_NAME=equoid-data-handler -P GIT_REPO_URI=https://github.com/eldritchjs/equoid-data-handler -p APP_MAIN_CLASS=io.radanalytics.equoid.dataHandler -p APP_ARGS='<AMQP Broker Address> <AMQP Port> <AMQP Username> <AMQP Password>  <AMQP Address> <Infinispan Address>  <Infinispan Port> <k> <epsilon> <confidence>' -p APP_FILE=equoid-data-handler-1.0-SNAPSHOT.jar -p SPARK_OPTIONS='--driver-java-options=-Dvertx.cacheDirBase=/tmp'
```

3. Publish some data to queue `salesq` per https://github.com/eldritchjs/equoid-publisher

### CheckCache test class

1. Start test
```
oc new-app --template=oshinko-java-spark-build-dc -p APPLICATION_NAME=equoid-check-cache -P GIT_REPO_URI=https://github.com/eldritchjs/equoid-data-handler -p APP_MAIN_CLASS=io.radanalytics.equoid.checkCache -p APP_ARGS='<Infinispan Address> <Infinispan Port> <Key> <Iterations>' -p APP_FILE=equoid-data-handler-1.0-SNAPSHOT.jar 
```
2. Check pod log for output either via web console or the following, as data is published to equoid, the value should increase accordingly:
```
oc logs <Equoid-check-cache pod ID> -n equoid
```

## Quickstart locally

A facility for testing/trying out the DataHandler capabilities without an OpenShift cluster is provided, as well. The class `FileDataHandler` incorporates `textFileStream(<data dir>)` to check for files in `data dir` periodically. This is strictly for testing without AMQ-P and OpenShift and is not meant to be a replacement for the relevant components of the Equoid architecture. The testing can take place as follows.

1. Build the classes
`sbt assembly`

2. Recommended: clean out checkpoint area and data files, see for example `src/test/bash/cleanlocal.sh`

3. Start an Infinispan server on `localhost` via Infinispan's provided `bin/standalone.sh` (easiest for testing functionality in the case where an extant server is not available)
 
4. Start `FileDataHandler` (assumes `$SPARK_HOME` is set properly) see for example `src/test/bash/startlocal.sh` 
`$SPARK_HOME/bin/spark-submit --class io.radanalytics.equoid.FileDataHandler --master local[4] target/scala-2.11/equoid-data-handler-assembly-0.1.0-SNAPSHOT.jar`

5. Start copying files to the data directory (default is /tmp/datafiles/)
`src/test/bash/cpdata.sh <Data File> <Output Directory> <Window Size> <Update Interval>`

6. Optional: start `CheckCache` in order to view contents of Infinispan as data is received
`$SPARK_HOME/bin/spark-submit --class io.radanalytics.equoid.CheckCache --master local[4] target/scala-2.11/equoid-data-handler-assembly-0.1.0-SNAPSHOT.jar`

