#!/bin/bash

start() {
  local PIDS=`getpids`

  if [[ -n $PIDS ]]
  then
    echo "Processes have started, stop them first.";
    exit
  fi

  echo "Start zookeeper..."
  ./bin/zookeeper-server-start ./etc/kafka/zookeeper.properties >> /dev/null &
  sleep 5

  echo "Start kafka..."
  ./bin/kafka-server-start ./etc/kafka/server.properties >> /dev/null &
  sleep 5

  echo "Start schema registry..."
  ./bin/schema-registry-start ./etc/schema-registry/schema-registry.properties >> /dev/null &
}

stop() {
  local PIDS=`getpids`
  if [[ -z $PIDS ]]
  then
    echo "No job running, exit."
    exit
  fi

  for PID in ${PIDS}; do
    killpid $PID
  done
}

getpids() {
  ps | grep confluent | grep -v grep | awk '{print $1}' | sort -nr
}

killpid() {
  local PID=$1
  echo -n "Killing $PID."
  kill $PID
  while :
  do
    ps -p $PID >> /dev/null
    if [[ $? == 0 ]]
    then
      echo -n .
      sleep 1
    else
      echo ""
      break
    fi
  done
}

waitforstart() {
  local PID=$1
  echo -n "Staring $PID."
  while :
  do
    ps -p $PID >> /dev/null
    if [[ $? == 0 ]]
    then
      echo ""
      break
    else
      echo -n .
      sleep 1
    fi
  done
}

case "$1" in
  start)
  start
  ;;
  stop)
  stop
  ;;
  *)
  echo "Please use start or stop as first argument"
  ;;
esac
