# kafka-connect-mongo

[![Build Status][travis-image]][travis-url]

Mongo connector (source)

## What's a kafka connector?

* [Ecosystem](https://cwiki.apache.org/confluence/display/KAFKA/Ecosystem)
* [Connect Hub](http://www.confluent.io/product/connectors)

## Config example

```
name=mongo-source-connector
connector.class=org.apache.kafka.connect.mongo.MongoSourceConnector
tasks.max=1
mongo.uri=mongodb://127.0.0.1:27017
batch.size=100
schema.name=mongoschema
topic.prefix=mongo
databases=test.users
# If use ssl, the location and password of truststore and keystore files are required
#ssl.truststore.location=truststore.jks
#ssl.truststore.password=123456
#ssl.keystore.location=keystore.jks
#ssl.keystore.password=123456
```

## LICENSE

Apache License 2.0

[travis-url]: https://travis-ci.org/teambition/kafka-connect-mongo
[travis-image]: http://img.shields.io/travis/teambition/kafka-connect-mongo.svg