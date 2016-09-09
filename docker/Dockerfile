# Builds a docker image running Kafka MongoDB Connector.
# Needs to be linked with kafka and schema-registry containers.
#
# Usage:
#   docker build -t teambition/kafka-connect-mongo kafka-connect-mongo
#   docker run -d --name kafka-connect-mongo --link kafka:kafka --link schema-registry:schema-registry teambition/kafka-connect-mongo

FROM confluent/platform

MAINTAINER sailxjx@gmail.com

ADD kafka-connect-mongo-docker.sh /usr/local/bin/

RUN ["mkdir", "-p", "/etc/kafka-connect-mongo"]
RUN ["chown", "-R", "confluent:confluent", "/etc/kafka-connect-mongo", "/usr/local/bin/kafka-connect-mongo-docker.sh"]

USER confluent
CMD ["/usr/local/bin/kafka-connect-mongo-docker.sh"]
