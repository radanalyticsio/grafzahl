# Topk Grafzahl Postgres Setup #

These instrutions are for testing grafzahl JBoss AM-Q connectivity on Openshift. In the interest of not changing too many components at once, Spark dependencies have been stripped out, with a Postgresql database standing in for Spark Streaming. This will be added back in a future push. 

1. `oc login -u \<username\> https://et0.et.eng.bos.redhat.com:8443`
2. `oc new-project amq-grafzahl`
3. Browse to: https://et0.et.eng.bos.redhat.com:8443/console 
4. Add to project -> Browse catalog -> Red Hat JBoss A-MQ 6.3 (Ephemeral, no SSL) with options
    * MQ_PROTOCOL=amqp,stomp
    * MQ_QUEUES=salesq
    * MQ_TOPICS=salest
    * MQ_USERNAME=daikon
    * MQ_PASSWORD=daikon
5. Add to project -> Browse catalog -> PostgreSQL (Ephemeral) with options
    * POSTGRESQL_USER=daikon
    * POSTGRESQL_PASSWORD=daikon
    * POSTGRESQL_DATABASE=salesdb
6. Back to local bash shell, ``PODNAME=`oc get pods | grep postgresql | awk '{split($0,a," *"); print a[1]}'` ``
7. `oc rsh $PODNAME`
    1. `psql -c 'CREATE TABLE SALES (ITEMID TEXT NOT NULL, QUANTITY INTEGER NOT NULL);' -h postgresql salesdb daikon`
    2. `psql -c 'ALTER TABLE SALES ADD CONSTRAINT ITEMPK PRIMARY KEY (ITEMID);' -h postgresql salesdb daikon`  
    2. `exit`
8. `oc new-app openshift/python-34-centos7:latest~https://github.com/eldritchjs/word-fountain`
9. `oc new-app openshift/python-34-centos7:latest~https://github.com/eldritchjs/grafzahl` (Note: This will complain about python-34-centos7 already existing. The app is still created)
10. `oc expose svc/grafzahl`
11. Browse to grafzahl
 
