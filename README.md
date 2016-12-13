# kafka-connect-mongo

[![Build Status][travis-image]][travis-url]

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

## Import data at the first time

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

## Execute import job by gradle (Development only)

1. Edit file `etc/producer.properties`
2. Execute `./gradlew runImport`

## LICENSE

Apache License 2.0

[travis-url]: https://travis-ci.org/teambition/kafka-connect-mongo
[travis-image]: http://img.shields.io/travis/teambition/kafka-connect-mongo.svg