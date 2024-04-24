from kafka import KafkaConsumer
from kafka.structs import TopicPartition
from itertools import combinations
import copy
import json

bootstrap_servers = ['localhost:9092']
topic2 = 'topic2'
local_path = '/home/i221944/kafka_assignement/file2.txt'

consumer2 = KafkaConsumer(topic2, bootstrap_servers=bootstrap_servers,group_id='consumer_group_2',
                          value_deserializer=lambda x: json.loads(x.decode('utf-8')))

diction = {}

def create_combinations(lst, r):
    return list(combinations(lst, r))

def hash_function(pair, hash_size): #this creates a hash value for the items
   
    return hash(str(pair)) % hash_size

def combination_creator(consumer2, data, i, hash_size):
    count = 0
    associations = create_combinations(data, i)
    print(len(associations))
    hash_table = [0] * hash_size #hash table is being created based on the hash size
    
    for x in consumer2:
        count += 1
        price_data1 = x.value
        record = price_data1['bucket']

        for item in associations:
            result = all(word in record for word in item)

            if result: #if the result is positive then get the hash value and increment on that hash value
                print(result)
                hash_val = hash_function(item, hash_size)
                hash_table[hash_val] += 1

        print(f'received data of consumer2 object function {count}\t{i}')
        if count == 2678000:
            break

    returned_list = []

    #over here the hash table is being checked for each of the association to get the value above the treshhold
    for item in associations:
        hashkey = hash_function(item,hash_size)
        value = hash_table[hashkey]

        if value > 2500:
            returned_list.append(item)
    
    return returned_list

item_list = []
function_list = []

# First Pass

count = 0
for x in consumer2:
    price_data1 = x.value
    list_data = price_data1['bucket']
    for l in list_data:
        if l in diction:
            diction[l] += 1
        else:
            diction[l] = 1

    count += 1
    print(f'received data of line {count} in consumer2')
    #2678000
    if count == 2678000:
        break

count = 0
for key in diction:
    count += 1
    value = int(diction[key])
    if value >= 2500:
        item_list.append(key)
        function_list.append(key)

diction = {}

consumer2.seek_to_beginning()

# Second Pass

item_list = copy.deepcopy(item_list)
function_list = copy.deepcopy(function_list)

length = 1
i = 1
hash_size = 10000  # Choose an appropriate hash table size

while length != 0:
    i += 1
    returned_associations = combination_creator(consumer2, function_list, i, hash_size)
    length = len(returned_associations)

    if length == 0:
        consumer2.seek_to_beginning()
        break

    for item in returned_associations:
        item_list.append(item)

    function_list = set(item for tuple_ in returned_associations for item in tuple_)

    consumer2.seek_to_beginning()




    
    
    
    
new_associations = create_combinations(item_list,2)

item_list = []

final_associations = []
for items in new_associations:
    check = False
    if isinstance(items[0], tuple):
        for item in items[0] :
            if item in items[1]:
                # print('present')
                # print(items)
                check = True
                break
            
        if check == False:
            list1 = list(items[0])
            list2 = list(items[1])
            
            final_list = []
            final_list.append(list1)
            
            for item in list1:
                list2.append(item)
            
            final_list.append(list2)
            final_associations.append(final_list)
        
    else:
        # print(items[0])
        if items[0] not in items[1]:
            final_list = []
            if isinstance(items[1],tuple):
                list1 = list(items[1])
                final_list.append(items[0])
                list1.append(items[0])
                final_list.append(list1)
                final_associations.append(final_list)
            else:
                final_list = []
                final_list.append(items[0])
                final_list.append(items[1])
                final_associations.append(final_list)
                
            # final_associations.append(items)
        

print("printing the items in associations")
for item in final_associations:
    print(item)        

import pymongo

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")  # Change the connection string as needed

# Select a database
db = client["kafka"]

# Select a collection
collection = db["pcy2"]
consumer2.seek_to_beginning()
count4 = 0
for item in final_associations:
    count4+=1
    count1 = 0
    count2 = 0
    count3 =  0
    for x in consumer2:
        price_data1 = x.value
        line = price_data1['bucket']
        count3+=1
        print(f'checked {count3} for {count4} consumer2')
        result = all(word in item[0] for word in line)
         

        if isinstance(item[0],list):
            result = all(word in item[0] for word in line)
                
            if result:
                count1+=1
        else:
            if item[0] in line:
                count1+=1
            
        if isinstance(item[1],list):
            result = all(word in item[1] for word in line)
                
            if result:
                count2+=1
        else:
            if item[1] in line:
                count2+=1
                    
        if count3 == 2678000 :
            # print('working')
            break
    
    
    if count1 != 0 and count2 != 0:
        value = round((count2/count1),3)
        if value > 0.60:
            data = {"item1": item[0], "item2": item[1], "confidence": value}
            insert_result = collection.insert_one(data)
            # print(insert_result.inserted_id)
            # print(f"{item[0]} ==> {item[1]} {count2/count1:.2f}")
        
            
    consumer2.seek_to_beginning()
            
                    
    
    

        
        
    
    
    
           
    
    
    
    
