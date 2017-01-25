#!/bin/bash

PACKAGE_VERSION=$(grep -E '^version' ../build.gradle | awk '{print $2}')
PACKAGE_VERSION="${PACKAGE_VERSION//\'}"

BUILD_ENV=$(echo "${PACKAGE_VERSION}" | cut -d "-" -f2)

: ${IMAGE_NAME:="sailxjx/kafka-connect-mongo"}
: ${SCALA_VERSIONS:="2.10 2.11"}
: ${DEFAULT_SCALA_VERSION:="2.11"}
: ${CONFLUENT_PLATFORM_VERSION:="3.0.0"}
: ${KAFKA_VERSION:="0.10.0.0-cp1"}
: ${ZOOKEEPER_VERSION:="3.4.6-cp1"}
: ${DOCKER_BUILD_OPTS:="--rm=true "}
: ${DOCKER_TAG_OPTS:=""}
: ${PACKAGE_URL:="http://packages.confluent.io/archive/3.0"}

#PRIVATE_REPOSITORY=""
#PUSH_TO_DOCKER_HUB=
