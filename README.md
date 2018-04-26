# kafka-connect-mongo

[![Build Status][travis-image]][travis-url]
[![Docker Repository on Quay](https://quay.io/repository/sailxjx/kafka-connect-mongo/status "Docker Repository on Quay")](https://quay.io/repository/sailxjx/kafka-connect-mongo)

Mongo connector (source)

## What's a kafka connector?

* [Ecosystem](https://cwiki.apache.org/confluence/display/KAFKA/Ecosystem)
* [Connect Hub](http://www.confluent.io/product/connectors)

## Config example

```properties
name=mongo-source-connector
connector.class=com.teambition.kafka.connect.mongo.MongoSourceConnector
tasks.max=1
mongo.uri=mongodb://127.0.0.1:27017
batch.size=100
schema.name=mongo_local_schema
topic.prefix=mongo_local
databases=test.users
# If use ssl, add configs on jvm by set environment variables `-Djavax.net.ssl.trustStore=/secrets/truststore.jks -Djavax.net.ssl.trustStorePassword=123456 -Djavax.net.ssl.keyStore=/secrets/keystore.jks -Djavax.net.ssl.keyStorePassword=123456`
#mongo.uri=mongodb://user:pwd@128.0.0.1:27017/?ssl=true&authSource=admin&replicaSet=rs0&sslInvalidHostNameAllowed=true
```

## Schedule export data from mongodb

Simply change the connector class to MongoCronSourceConnector and add a schedule parameter in the source config,
this connector will export all the data from your collection to kafka through the same way of mongo source connect. 

```properties
name=mongo-cron-source-connector
connector.class=com.teambition.kafka.connect.mongo.MongoCronSourceConnector
tasks.max=1
mongo.uri=mongodb://127.0.0.1:27017
batch.size=100
schema.name=mongo_local_schema
topic.prefix=mongo_local
databases=test.users
schedule=0 0 * * * ?  # Execute every hour, in quartz cron pattern
```

## Initial import data from mongo collection

1. Build project `./gradlew clean distTar`
2. Unzip tarball and enter the directory `cd build/distributions && tar -xvf connect-mongo-1.0.tgz && cd connect-mongo-1.0`
3. Create a properties file `touch producer.properties`
4. Fill in the your configs, for example [etc/producer.properties]:
    ```properties
    # Producer properties
    bootstrap.servers=kafka:29092,kafka:39092,kafka:49092
    compression.type=none
    key.serializer=org.apache.kafka.common.serialization.StringSerializer
    value.serializer=org.apache.kafka.common.serialization.StringSerializer
    
    # Mongodb properties
    mongo.uri=mongodb://127.0.0.1:27017
    topic.prefix=mongo_local
    databases=test.emails
    ```
5. Execute the script `./bin/connect-mongo producer.properties`

Tips: the script will use `_id` as the offset for each bulk read, 
so all your messages should have an auto increment field called `_id`.

## Execute import job by gradle (Development only)

1. Edit file `etc/producer.properties`
2. Execute `./gradlew runImport`

## Sink (Experimental)

```properties
name=mongo-sink-connector
connector.class=com.teambition.kafka.connect.mongo.MongoSinkConnector
tasks.max=1
mongo.uri=mongodb://root:root@127.0.0.1:27017/?authSource=admin
topics=topic1,topic2
databases=mydb.topic1,mydb.topic2
```

Now you can only use mongo sink connector as your restore tool, 
you can restore data from kafka which given by mongo source connector.
 
The messages should contain `object` and `id` fields

## LICENSE

Apache License 2.0

[travis-url]: https://travis-ci.org/teambition/kafka-connect-mongo
[travis-image]: http://img.shields.io/travis/teambition/kafka-connect-mongo.svg
