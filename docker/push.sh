#!/usr/bin/env bash

. settings.sh

docker push "${IMAGE_NAME}:${PACKAGE_VERSION}" "${IMAGE_NAME}:latest" "quay.io/${IMAGE_NAME}:${PACKAGE_VERSION}" "quay.io/${IMAGE_NAME}:latest"
