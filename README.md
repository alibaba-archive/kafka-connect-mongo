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
connector.class=org.apache.kafka.connect.mongo.MongoSourceConnector
tasks.max=1
mongo.uri=mongodb://127.0.0.1:27017
batch.size=100
schema.name=mongo_local_schema
topic.prefix=mongo_local
databases=test.users
# If use ssl, the location and password of truststore and keystore files are required
#mongo.uri=mongodb://user:pwd@128.0.0.1:27017/?ssl=true&authSource=admin&replicaSet=rs0&sslInvalidHostNameAllowed=true
#javax.net.ssl.trustStore=truststore.jks
#javax.net.ssl.trustStorePassword=123456
#javax.net.ssl.keyStore=keystore.ks
#javax.net.ssl.keyStorePassword=123456
```

## Initial import data from mongo collection

1. Build project `./gradlew clean distTar`
2. Unzip tarball and enter the directory `cd build/distributions && tar -xvf connect-mongo-1.0.tgz && cd connect-mongo-1.0`
3. Create a properties file `touch producer.properties`
4. Fill in the your configs, for example [etc/producer.properties]:
    ```properties
    # Producer properties
    bootstrap.servers=192.168.0.124:39092
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
so all your documents should have an auto increment field called `_id`.

## Execute import job by gradle (Development only)

1. Edit file `etc/producer.properties`
2. Execute `./gradlew runImport`

## Sink (Experimental)

```properties
name=mongo-sink-connector
connector.class=org.apache.kafka.connect.mongo.MongoSinkConnector
tasks.max=1
mongo.uri=mongodb://root:root@192.168.0.21:27017/?authSource=admin
topics=topic1,topic2
```

Now you can only use mongo sink connector as your restore tool, 
you can restore data from kafka which given by mongo source connector.
 
The messages should contain `databases`, `object` and `id` keys

## LICENSE

Apache License 2.0

[travis-url]: https://travis-ci.org/teambition/kafka-connect-mongo
[travis-image]: http://img.shields.io/travis/teambition/kafka-connect-mongo.svg