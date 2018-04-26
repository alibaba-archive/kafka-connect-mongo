#!/usr/bin/env bash

. settings.sh

docker push "docker-registry.teambition.net/library/${IMAGE_NAME}:v${PACKAGE_VERSION}"
docker push "docker-registry.teambition.net/library/${IMAGE_NAME}:latest"

[[ $PACKAGE_VERSION =~ ^[0-9\.]+$ ]] || exit 0

docker push "sailxjx/${IMAGE_NAME}:v${PACKAGE_VERSION}"
docker push "sailxjx/${IMAGE_NAME}:latest"
docker push "quay.io/sailxjx/${IMAGE_NAME}:v${PACKAGE_VERSION}"
docker push "quay.io/sailxjx/${IMAGE_NAME}:latest"
