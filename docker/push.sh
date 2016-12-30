#!/usr/bin/env bash

. settings.sh

docker push "quay.io/${IMAGE_NAME}:${PACKAGE_VERSION}"
docker push "quay.io/${IMAGE_NAME}:latest"
