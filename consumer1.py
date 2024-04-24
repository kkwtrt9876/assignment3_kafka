#!/usr/bin/env python3

from kafka import KafkaConsumer
from kafka.structs import TopicPartition
from itertools import combinations
import copy

import json

bootstrap_servers = ['localhost:9092']
topic1 = 'topic1'
local_path = '/home/i221944/kafka_assignement/file2.txt'

consumer1 = KafkaConsumer(topic1, bootstrap_servers=bootstrap_servers,group_id='consumer_group_1',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

diction = {}

def create_combinations(lst, r):
    return list(combinations(lst, r))


def combination_creator(consumer1,data,i):
    count = 0
    associations = create_combinations(data,i) #this creates combinations on the basis of value i , 1,2,3
 
    diction = {}
    
    for x in consumer1:
        count+=1
        price_data1 = x.value
        record = price_data1['bucket']
            
        for item in associations:
            result = all(word in record for word in item) #checks wether each of the pair, triple is presnet in the data
            # print(f"parsed {count}")
        
            if result: #if it is present then add it to the dictionary
                print(result)
                if item in diction:
                    diction[item] +=1 
                else:
                    diction[item] = 1
                    
        print(f'recieved data of object function {count}\t{i}')
        if count == 2678000:
            break
        # print(record)
                    
    returnedlist = []
    
    for key in diction: #if the value of each item in the dictionary is greater than 2500 then append into the list
        if diction[key] >=2500:
            print("got items")
            returnedlist.append(key)
            
    return returnedlist
            

item_list = []
function_list = []
    
count =0

for x in consumer1:  #reading the data in the consumer
    
    price_data1 = x.value
    list_data = price_data1['bucket'] #the preprocess file is a json file containing the bucket as a key and list of items as values
    
    for l in list_data: #this loop iterates over the each list and find thes counts of unique
        if l in diction:
            diction[l] +=1
        else:
            diction[l] = 1
        
        
    count+=1
    print(f'recieved data of line {count} in consumer1') #this tells us that how much data is being processed
  
        
    #it breakes the consumer so that no further data comes into it. if producer is started earlier and consumer later, this might raise an error
    #so this break statemne
    if count == 2678000: 
            
        break
        

#the value for each key is bein checked. threshold for the frequent items is bein checked which is 2500
#if it is greater than 2500 means above threshold it is frequent item
for key in diction:
    
    value = int(diction[key])
        
    if value >= 2500:
        item_list.append(key)
        function_list.append(key)
    
            
diction = {}

#this brings the the offset of the consumer to start to reading data of the consumer from the start again    
consumer1.seek_to_beginning()
    

#this is bein done so that the when the function_list is bein changed the item_list is not affected by it
item_list = copy.deepcopy(item_list)
function_list = copy.deepcopy(function_list)


length = 1
i = 1 #this i value is increased to make the doubles, triples , tetra etc

#this loop makes all the doubles , triples, tetra until some data is avaialble to it
while length != 0:
    i+=1 
    
            
    returned_assocaitions = combination_creator(consumer1,function_list,i) #this returns tha pairs , triples based on the value of i
    length = len(returned_assocaitions) #lenght is being checked , if no data is being present means no further pairs , triplets can be made
    
    
    if length == 0:
        consumer1.seek_to_beginning()
        break
    
    for item in returned_assocaitions:
        item_list.append(item) #the items that are being collected are appended to the frequent items list
        
    #over here a set is being formed of the elements in the returned associations , so that it is passed to the function again to make triples , tetra
    function_list = set(item for tuple_ in returned_assocaitions for item in tuple_) 
    
    
    consumer1.seek_to_beginning()
    
    
    
#an association is like this 
#item1 => item2 
#so for this purpose all the item_list is passed to this function , all the items means that single items , doubles , triplets
#and a pair is being made from it to give it a shape of association rule 
#item1 => item2  
new_associations = create_combinations(item_list,2)

item_list = []

#this loop over here checks that if an association e.g
# item1 = a and item2 = b,c
#this checks that wedher item1 is present in item 2 or not 
#if not present then it makes the asscosiation rules from it
final_associations = []
for items in new_associations:
    check = False
    if isinstance(items[0], tuple): #checks wether item1 has multiple items
        for item in items[0] : #check if item is present in item1 is presnt in item2
            if item in items[1]:
                check = True
                break
            
        if check == False: #if the item1 is not present in item2
            list1 = list(items[0]) #make lists of item1 and item2
            list2 = list(items[1])
            
            final_list = []
            final_list.append(list1) #apend item1 list 
            
            #overhere all the list1 of elements are appended into the list2 this is done because
            # a => b,c  , count of [a,b,c] / count of [a] to get the confidence , that is why this is done
            for item in list1: 
                list2.append(item) 
            
            final_list.append(list2)
            #the final list which contains both the lists are then appended into the the final_Association
            #from this now confidence will be calculated
            final_associations.append(final_list)
        
    else:#if multiple items are not present then
        if items[0] not in items[1]:
            final_list = []
            if isinstance(items[1],tuple): #checks wether the second item is tuple or not
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
collection = db["apriori2"]

count4 = 0
#in this part of the code each of the association is being checked 
#the counts are being calculated here and the confidence value is find out

for item in final_associations:
    count4+=1
    count1 = 0
    count2 = 0
    count3 =  0
    for x in consumer1:
        price_data1 = x.value
        line = price_data1['bucket']
        count3+=1
        print(f'checked {count3} for {count4} consumer 1') 
        
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
            # print(item[1])
            if item[1] in line:
                count2+=1
                    
        if count3 == 2678000 :
            # print('working')
            break
    
    
    if count1 != 0 and count2 != 0:
        value = round((count2/count1),3)
        # print(f"{item[0]} ==> {item[1]} {count2/count1:.2f}")
        if value > 0.60: #if the confidence is greater than 0.6 then insert into mongodb
            data = {"item1": item[0], "item2": item[1], "confidence": value}
            insert_result = collection.insert_one(data)
            print(insert_result.inserted_id)
            # print(f"{item[0]} ==> {item[1]} {count2/count1:.2f}")
    
        
            
    consumer1.seek_to_beginning()
            
                    
    
    

        
        
    
    
    
           
    
    
    
    
