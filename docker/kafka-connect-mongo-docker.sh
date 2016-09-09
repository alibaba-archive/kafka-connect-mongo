#!/bin/bash

CFG_FILE="/etc/kafka-connect-mongo/connect-mongo-source.properties"

# Download the config file, if given a URL
if [ ! -z "$CFG_URL" ]; then
  echo "[RP] Downloading RP config file from ${CFG_URL}"
  curl --location --silent --insecure --output ${CFG_FILE} ${CFG_URL}
  if [ $? -ne 0 ]; then
    echo "[RP] Failed to download ${CFG_URL} exiting."
    exit 1
  fi
fi

CLASSPATH=./share/java/connect-mongo-1.0-SNAPSHOT
exec /usr/bin/connect-standalone /etc/schema-registry/connect-avro-standalone.properties $CFG_FILE
