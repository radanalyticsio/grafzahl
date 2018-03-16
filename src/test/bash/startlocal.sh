#!/bin/bash

$SPARK_HOME/bin/spark-submit --class io.radanalytics.equoid.FileDataHandler --master local[4] target/scala-2.11/equoid-data-handler-assembly-0.1.0-SNAPSHOT.jar

