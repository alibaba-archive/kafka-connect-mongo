# Builds a docker image running Kafka MongoDB Connector.
# Needs to be linked with kafka and schema-registry containers.
# Usage:
#   docker build -t teambition/kafka-connect-mongo kafka-connect-mongo
#   docker run -d --name kafka-connect-mongo --link kafka:kafka --link schema-registry:schema-registry teambition/kafka-connect-mongo

FROM confluentinc/cp-kafka-connect-base:4.1.0

MAINTAINER sailxjx@gmail.com

COPY targets /usr/local/kafka-connect-mongo

HEALTHCHECK --interval=1m --timeout=10s --retries=2 --start-period=20s \
        CMD /usr/local/kafka-connect-mongo/bin/kafka-connect-mongo healthcheck

RUN ln -sf /usr/local/kafka-connect-mongo/lib /usr/share/java/kafka-connect-mongo
