# Builds a docker image running Kafka MongoDB Connector.
# Needs to be linked with kafka and schema-registry containers.
# Usage:
#   docker build -t teambition/kafka-connect-mongo kafka-connect-mongo
#   docker run -d --name kafka-connect-mongo --link kafka:kafka --link schema-registry:schema-registry teambition/kafka-connect-mongo

FROM confluentinc/cp-kafka-connect-base:4.1.0

MAINTAINER sailxjx@gmail.com

ARG APP_NAME

COPY targets /usr/local/$APP_NAME

HEALTHCHECK --interval=1m --timeout=10s --retries=2 --start-period=20s \
        CMD /usr/local/$APP_NAME/bin/$APP_NAME healthcheck

RUN ln -sf /usr/local/$APP_NAME/lib /usr/share/java/$APP_NAME
