#!/usr/bin/env bash
set -eu

SOURCE_DIR=$(dirname $0)/..

PACKAGE_NAME=kafka-connect-mongo
PACKAGE_VERSION=$(grep -E '^version' $SOURCE_DIR/build.gradle | awk '{print $2}')
PACKAGE_VERSION="${PACKAGE_VERSION//\'}"

TARGET_DIR=$SOURCE_DIR/target
prepare() {
    echo "Create target dir $TARGET_DIR"
    rm -rf $TARGET_DIR && mkdir $TARGET_DIR
}

cleanup() {
    echo "Cleanup target dir $TARGET_DIR"
    rm -rf $TARGET_DIR
}
trap cleanup EXIT
prepare

# Build source code./connect-mongo
cd $SOURCE_DIR \
    && ./gradlew clean distTar \
    && tar -xvf $SOURCE_DIR/build/distributions/$PACKAGE_NAME-$PACKAGE_VERSION.tgz -C $TARGET_DIR --strip 1
# Start connector
KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:$SOURCE_DIR/etc/log4j.properties" CLASSPATH=$TARGET_DIR/lib/* \
    connect-standalone $SOURCE_DIR/etc/connect-avro-standalone.properties $SOURCE_DIR/etc/connect-mongo-source.properties