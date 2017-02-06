#!/usr/bin/env bash

. settings.sh

docker push "docker-registry.teambition.net/library/${IMAGE_NAME}:${PACKAGE_VERSION}"
docker push "docker-registry.teambition.net/library/${IMAGE_NAME}:latest"

[[ $BUILD_ENV == "beta" ]] && exit 0

docker push "sailxjx/${IMAGE_NAME}:${PACKAGE_VERSION}"
docker push "sailxjx/${IMAGE_NAME}:latest"
docker push "quay.io/sailxjx/${IMAGE_NAME}:${PACKAGE_VERSION}"
docker push "quay.io/sailxjx/${IMAGE_NAME}:latest"
