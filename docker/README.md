# Build

Run `./build.sh`

You can edit settings.sh to change your environment variables

# Run

`docker run --name kafka-connect-mongo -d teambition/kafka-connect-mongo`

You can change your runtime properties with environment variable called `CFG_URL`

`docker run --name kafka-connect-mongo -e CFG_URL=http://your.host/connect-mongo-source.properties -d teambition/kafka-connect-mongo`

# Fullstack

For the usage of full stack containers of zookeeper, kafka and schema-registry, please visit https://github.com/confluentinc/cp-docker-images.

# License

MIT
