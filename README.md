# kafka-connect-mongo

[![Build Status][travis-image]][travis-url]
[![Docker Repository on Quay](https://quay.io/repository/sailxjx/kafka-connect-mongo/status "Docker Repository on Quay")](https://quay.io/repository/sailxjx/kafka-connect-mongo)

# WARNING: when upgrade from 1.5.x to 1.6.0, please read the messages below!

1.6.0 changed the package name from org.apache.kafka to com.teambition, so when you upgrade from 1.5.x, you may find your connectors breaked. So please:

1. Save your connectors's configs to local file, you may save those configs to a local curl script like so:
  ```
  curl -X PUT -H "Content-Type: application/json" http://192.168.0.22:38083/connectors/mongo_source_test/config -d '{
    "connector.class": "MongoSourceConnector",
    "databases": "teambition.tasks",
    "initial.import": "true",
    "topic.prefix": "mongo_test",
    "tasks.max": "8",
    "batch.size": "100",
    "schema.name": "mongo_test_schema",
    "name": "mongo_source_test",
    "mongo.uri": "mongodb://root:root@192.168.0.21:27017/?authSource=admin"
  }'
  ```
2. Delete your connectors via `curl -XDELETE http://192.168.0.22:38083/connectors/your_connector_name`, this will not delete your offsets, so you will not worry about lost of your offsets.
3. Upgrade your kafka-connect-mongo cluster to 1.6.0.
4. Recreate your connectors (with the saved curl scripts).

# Mongo connector (source)

## What's a kafka connector?

* [Ecosystem](https://cwiki.apache.org/confluence/display/KAFKA/Ecosystem)
* [Connect Hub](http://www.confluent.io/product/connectors)

## Config example

```properties
name=mongo-source-connector
connector.class=MongoSourceConnector
tasks.max=1
mongo.uri=mongodb://127.0.0.1:27017
batch.size=100
schema.name=mongo_local_schema
topic.prefix=mongo_local
databases=test.users
# If this option is set to true, source connector will analyze the schema from real document type and mapping them to the top level schema types
# WARNING: mongo connector interprets the schema from the structure of document, so it can not ensure the schema always stay consist.
# If you met an `Schema being registered is incompatible with an earlier schema` error given by schema registry, please set the `avro.compatibility.level` option of schema registry to `none`
analyze.schema=false
schema.registry.url=http://127.0.0.1:8080
# If use ssl, add configs on jvm by set environment variables `-Djavax.net.ssl.trustStore=/secrets/truststore.jks -Djavax.net.ssl.trustStorePassword=123456 -Djavax.net.ssl.keyStore=/secrets/keystore.jks -Djavax.net.ssl.keyStorePassword=123456`
#mongo.uri=mongodb://user:pwd@128.0.0.1:27017/?ssl=true&authSource=admin&replicaSet=rs0&sslInvalidHostNameAllowed=true
```
## How the Mongo Source Connector Works

- At startup, load and parse configuration settings
  - determine list of enabled databases
- If analyze.schema is enabled
  - check schema registry for a schema mapped to each enabled database
- If analyze.schema is disabled, prepare a generic oplog schema
- For each enabled database
  - read the stored offset from kafka
  - if initial import is enabled and not complete, do import
    - use offset to page through the collection and queue each document for the next KC `poll()` call
  - query oplog for changes
    - As query results stream in, format them according to the operation
      - insert: publish the oplog to kafka as is
      - delete: publish the oplog to kafka as is
      - update: load the full document from mongo then publish that
    - add them to an in-memory queue waiting for a KC `poll()`
  - each invocation of `poll()` by the kafka-connect framework, flush the in-memory queue to kafka connect, which handles offset tracking

## Schedule export data from mongodb

Simply change the connector class to MongoCronSourceConnector and add a schedule parameter in the source config,
this connector will export all the data from your collection to kafka through the same way of mongo source connect.

```properties
name=mongo-cron-source-connector
connector.class=MongoCronSourceConnector
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
connector.class=MongoSinkConnector
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
