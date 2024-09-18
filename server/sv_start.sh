#!/bin/sh

base_port=8080
num_servers=5
pids=()
mkdir -p logs

cleanup() {
  echo "Cleaning up..."
  for i in $(seq 0 $((num_servers - 1)))
  do
    port=$((base_port + i))
    fuser -k ${port}/tcp
  done
  fuser -k 8090/tcp
  for pid in "${pids[@]}"
  do
    kill $pid
  done
}

trap cleanup SIGINT

# initial cleanup to kill any servers that might be running
cleanup

# initialize sequencer
cd ../sequencer
mvn compile exec:java | ts '%Y-%m-%d %H:%M:%S' > "../server/logs/sequencer.log" &
pids+=($!)

# awaits for the string "Sequencer server started" in the sequencer log to start the servers
cd ../server
while ! grep -q "Sequencer server started" logs/sequencer.log
do
  sleep 1
done

for i in $(seq 0 $((num_servers - 1)))
do
  echo "Starting server $i"
  mvn compile exec:java -Dexec.args="$base_port $i" | ts '%Y-%m-%d %H:%M:%S' > "logs/server_${base_port}_${i}.log" 2>&1 &
  pids+=($!)
done

wait
cleanup