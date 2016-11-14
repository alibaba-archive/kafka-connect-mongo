# Features
* [x] Add message key
* [x] Authorize in MongoDB with password
* [x] Authorize in MongoDB with certification
* [x] Try reconnect when connection lost
* [x] Support kafka 0.10
* [ ] Initial import all data on startup without offset
* [x] Dockerized
* [ ] Validate in docker containers, connect with other containers
* [ ] Implement with confluent 3.1.0

# Test Cases
* [x] Start and stop
* [x] CURD in MongoDB
* [x] Restart and load correct offset
* [x] [Distributed Mode](http://docs.confluent.io/3.0.0/connect/userguide.html#distributed-mode)
* [x] [Resuming from Previous Offsets](http://docs.confluent.io/3.0.0/connect/devguide.html#resuming-from-previous-offsets)
* [x] [Schema Evolution](http://docs.confluent.io/3.0.0/connect/devguide.html#schema-evolution)
