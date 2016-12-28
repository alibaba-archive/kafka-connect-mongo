# Build

`make build`

You can edit settings.sh to change your environment variables

# Run

`docker run -it teambition/kafka-connect-mongo`

This connector will run in distributed mode and use 38083 as the default port, 
you can set modify the port and other configs by environment variables with the `CONNNECT_` prefix

# Run with docker-compose

`docker-compose up -d`

# Fullstack

For the usage of full stack containers of zookeeper, kafka and schema-registry, please visit https://github.com/confluentinc/cp-docker-images.

# License

MIT
