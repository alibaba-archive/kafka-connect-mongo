#!/bin/bash

PACKAGE_VERSION=$(grep -E '^version' ../build.gradle | awk '{print $2}')
PACKAGE_VERSION="${PACKAGE_VERSION//\'}"

BUILD_ENV=$(echo "${PACKAGE_VERSION}" | cut -d "-" -f2)

: ${IMAGE_NAME:="kafka-connect-mongo"}
: ${DOCKER_BUILD_OPTS:="--rm=true "}
: ${DOCKER_TAG_OPTS:=""}
