# Builds a docker image for the Kafka Connect Mongo.
# Expects links to "kafka" and "schema-registry" containers.
#
# Usage:
#   docker build -t teambition/kafka-connect-mongo kafka-connect-mongo
#   docker run -d --name kafka-connect-mongo --link kafka:kafka --link schema-registry:schema-registry teambition/kafka-connect-mongo

FROM confluent/platform

MAINTAINER sailxjx@gmail.com

COPY rest-proxy-docker.sh /usr/local/bin/

RUN ["chown", "-R", "confluent:confluent", "/etc/kafka-rest", "/usr/local/bin/rest-proxy-docker.sh"]
RUN ["chmod", "+x", "/usr/local/bin/rest-proxy-docker.sh"]

EXPOSE 8082

CMD [ "/usr/local/bin/rest-proxy-docker.sh" ]
