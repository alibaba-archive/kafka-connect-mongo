# Builds a docker image running Kafka MongoDB Connector.
# Needs to be linked with kafka and schema-registry containers.
#
# Usage:
#   docker build -t teambition/kafka-connect-mongo kafka-connect-mongo
#   docker run -d --name kafka-connect-mongo --link kafka:kafka --link schema-registry:schema-registry teambition/kafka-connect-mongo

FROM confluentinc/cp-kafka-connect-base

MAINTAINER sailxjx@gmail.com

ENV COMPONENT=kafka-connect-mongo
ENV CLASSPATH="/usr/local/kafka-connect-mongo/lib/*"

COPY targets "/usr/local/kafka-connect-mongo/"
COPY include "/etc/kafka-connect-mongo/"

CMD ["/etc/kafka-connect-mongo/run"]
