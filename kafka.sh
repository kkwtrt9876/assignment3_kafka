#!/bin/bash

# Directory where Kafka is installed
KAFKA_HOME="/home/i221944/kafka"

# Start ZooKeeper
echo "Starting ZooKeeper..."
$KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties &

# Wait for ZooKeeper to start
sleep 20

# Start Kafka server
echo "Starting Kafka server..."
$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties &

# Wait for Kafka server to start
sleep 20

# Start MongoDB server
echo "Starting MongoDB server..."
systemctl start mongod

# Open new terminal window and start Consumer 1
echo "Starting Consumer 1..."
gnome-terminal -- /bin/bash -c "python3	/home/i221944/kafka_assignement/consumer1.py; exec bash" &

# Open new terminal window and start Consumer 2
echo "Starting Consumer 2..."
gnome-terminal -- /bin/bash -c "python3 /home/i221944/kafka_assignement/consumer2.py; exec bash" &

# Start Consumer 3 in background
echo "Starting Consumer 3..."
gnome-terminal -- /bin/bash -c "python3 /home/i221944/kafka_assignement/consumer3.py; exec bash" &

# Start Producer in background
echo "Starting Producer..."
gnome-terminal -- /bin/bash -c "python3 /home/i221944/kafka_assignement/producer.py; exec bash" &

