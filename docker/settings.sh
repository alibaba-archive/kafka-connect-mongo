#!/bin/bash

PACKAGE_VERSION=$(grep -E '^version' ../build.gradle | awk '{print $2}')
PACKAGE_VERSION="${PACKAGE_VERSION//\'}"

: ${IMAGE_NAME:="kafka-connect-mongo"}
: ${DOCKER_BUILD_OPTS:="--rm=true "}
: ${DOCKER_TAG_OPTS:=""}
