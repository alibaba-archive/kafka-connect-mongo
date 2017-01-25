#!/usr/bin/env bash

. settings.sh

docker push "docker-registry.teambition.net/${IMAGE_NAME}:${PACKAGE_VERSION}"
docker push "docker-registry.teambition.net/${IMAGE_NAME}:latest"

[[ $BUILD_ENV -eq "beta" ]] && exit 0

docker push "${IMAGE_NAME}:${PACKAGE_VERSION}"
docker push "${IMAGE_NAME}:latest"
docker push "quay.io/${IMAGE_NAME}:${PACKAGE_VERSION}"
docker push "quay.io/${IMAGE_NAME}:latest"
