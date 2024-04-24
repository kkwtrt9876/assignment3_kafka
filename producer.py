#!/usr/bin/env python3

import json
from kafka import KafkaProducer
from time import sleep

# Define the path to the local JSON file
json_file_path = 'processed.json'

bootstrap_servers = ['localhost:9092']
topic1 = 'topic1'
topic2 = 'topic2'
topic3 = 'topic3'

producer1 = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))


count = 0
with open(json_file_path, 'r') as file:
    # Read each line of the file
    for line in file:
            # Parse the line as JSON
        
        data = json.loads(line)
    
            # Send the data to Kafka topics
        producer1.send(topic1, value=data)
        producer1.send(topic3,value=data)
        producer1.send(topic2,value=data)
        count+=1
        
        
        print(f'Data sent to Kafka of line {count}')
    
    
