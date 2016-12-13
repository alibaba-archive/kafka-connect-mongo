# Builds a docker image running Kafka MongoDB Connector.
# Needs to be linked with kafka and schema-registry containers.
#
# Usage:
#   docker build -t teambition/kafka-connect-mongo kafka-connect-mongo
#   docker run -d --name kafka-connect-mongo --link kafka:kafka --link schema-registry:schema-registry teambition/kafka-connect-mongo

FROM confluentinc/cp-base

MAINTAINER sailxjx@gmail.com

ENV COMPONENT=kafka-connect-mongo

ADD targets /usr/share/java/kafka-connect-mongo/
ADD kafka-connect-mongo-docker.sh /usr/local/bin/

RUN ["mkdir", "-p", "/etc/kafka-connect-mongo"]

CMD ["/usr/local/bin/kafka-connect-mongo-docker.sh"]
