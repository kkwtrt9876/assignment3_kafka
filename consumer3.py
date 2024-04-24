#!/usr/bin/env python3

from kafka import KafkaConsumer
from kafka.structs import TopicPartition
from itertools import combinations
import time

import json

bootstrap_servers = ['localhost:9092']
topic3 = 'topic3'
local_path = '/home/i221944/kafka_assignement/file2.txt'

consumer1 = KafkaConsumer(topic3, bootstrap_servers=bootstrap_servers,group_id='consumer_group_3',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

diction = {}

#this is a tree havig count , value , childre
#count += number of times a node is visited
#value = the value of the node
#contains the list of children it will have
class tree:
    def __init__(self,count,value):
        self.count = count
        self.value = value
        self.children = []
    
    def add_Children(self,node):
        self.children.append(node)
        
        
def build_tree(root, transactions):
    for transaction in transactions:
        current_node = root
        if transaction == root.value: #if teh first value is equal to the tree first value then increase the count
            root.count += 1
        else:
            if len(current_node.children) == 0: #if the node doesnt have any children then add the transaction as its child
                new_node = tree(1,transaction)
                current_node.children.append(new_node)
            else:
                found = False
                #in this part each of the child is being checked for the presnece of the transaction value
                #if it is present then the count of node is incremented and currentn node equal to that node
                # and again the current node is checked for the next transaction
                #if it is not found it is added to the children of the current node
                
                while not found:
                # Check if the transaction is in the children of the current node
                    for child in current_node.children:
                        if child.value == transaction:
                        # If found, update its count and move to the child node
                            child.count += 1
                            current_node = child
                            if len(current_node.children) == 0:
                                new_node = tree(1,transaction)
                                current_node.children.append(new_node)
                                current_node = new_node
                                found = True  # Exit the loop
                                break
                    
                    if found == False:
                        new_node = tree(1,transaction)
                        current_node.children.append(new_node)
                        break
                            
                            
                            
                            
def search_helper(root,item):
        
    stack = [root]
    while stack:
        node = stack.pop()  # Pop the last element (top of the stack)
        data = node.value
        count = node.count# Get the data of the node

        # Process the node data as needed
        if data == item:
            return count
        

    # Extend the stack with children nodes
        for child in node.children:
            stack.append(child)
        
        
            

def dfs_paths_to_value(node, target, path=[], paths=set(), count=[], counts=[], data=[]):
    if node is None:
        return

    # Append the current node to the path
    path.append(node.value)

    # If the current node contains the target value, add the current path to the list of paths
    if node.value == target:
        to_add = []
        for i in path:
            if i not in to_add:
                to_add.append(i)

        paths.add(tuple(to_add))

    # Recursively search in the children nodes
    for child in node.children:
        dfs_paths_to_value(child, target, path, paths, count, counts, data)

    # After exploring all children, remove the current node from the path
    path.pop()

    return paths
 

        



item_list = []
function_list = []
new_diction = {}
sorted_dict = {}


count =0
    
for x in consumer1: #reading the data of the consumer
    price_data1 = x.value
    list_data = price_data1['bucket'] #data is in the form of a json file hence the list associated to the key is being printed
    for l in list_data:  #each list is being tranversed and the count of each item is bein find out
        if l in diction:
            diction[l] +=1
        else:
            diction[l] = 1
           
    count+=1
    print(f'recieved data of line {count} in consumer3')
    if count == 2678000 :
        break
        

for key in diction: #checkes over here is the value is greater than the threshold value
    value = int(diction[key])
    if value >= 2500:
        new_diction[key] = diction[key]
  
#this sortes the diction in the form of highest values of the key. key with a highest value will be first in the diction            
sorted_dict = {k: v for k, v in sorted(new_diction.items(), key=lambda item: item[1],reverse=True)}
#freeing memory
new_diction = {}
diction = {}
    
consumer1.seek_to_beginning()
    


ordered_items = []
frequents = []

#over here the keys of the sorted diction are bbeing stored in a list
#so the list will have the key with highest value first and then keys with lower value
for key in sorted_dict:
    frequents.append(key)


#in this part of the code 
#each of the basket is being checked in the preprocess file,
#it is compared with the keys in the dictionary of sorted items
#each of the basket is ordered according to those values presnt in the diction, highest to lowest
count = 0
for x in consumer1:
    count += 1
    print(f"ordering the items {count}")
    price_data1 = x.value
    
    try:
        list_data = price_data1['bucket']
        new_list = []
        
        for item in sorted_dict: #each of the item is checked over here with the bucket.
            if item in list_data:#if it is presnt then it is appended to the list
                new_list.append(item)
        
        if new_list:
            ordered_items.append(new_list) #the collection is then appended into the final list of ordred items
            
        if count == 2678000 :

            break
    
    except KeyError:
        print("KeyError: 'bucket' key not found in price_data1")
        
    


#based on the ordered items a tree is being made. this will contain the elements with the highest values first and the rest will be its children
root = tree(0,0)
for key, item in enumerate(ordered_items):
    print(f"building tree {key}")
    if root.value == 0:
        root = tree(0,item[0])
    else:
        build_tree(root,item)

        

#over here the paths to each of the items is being calculated
#e.g input :- 123 returns [112,115,123]
main_path = []
for item in frequents:
    print("getting the paths")
    paths = dfs_paths_to_value(root, item)
    print(item)
    #each item may have multiple paths to it hence this is done
    #the paths of each of the item is being calculated and stored
    for path in paths: 
        main_path.append(path)



#the counts are for each of the item is being calculated and stored as key value pairs   
counts_diction = {}  
for item in frequents:
    print("getting the counts")
    count = search_helper(root, item)
    print(count)
      
    counts_diction[item] = count
    
        
#here the final paths are being get
final_paths = set()
for key in main_path:
    print("getting the final items")
    total = 0
    for item in key: #each of the item in the path count is taken out and totaled
        total+= counts_diction.get(item, 0)
    
    if total > 2500: #if it is greater than the threshold value then it is considered as valid path
        final_paths.add(key)

    

import pymongo

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")  # Change the connection string as needed

# Select a database
db = client["kafka"]

# Select a collection
collection = db["consumer3"]
collection1 = db['associations1']

#the path is considered to be an association rule
#where by e.g path = [1,2,3,4]
#path[0] will be item1 and path[1:] will be item2
for item in final_paths:
    data = {"item1": item[0], "item2": item[1:]}
    collection1.insert_one(data)
    print(f'{item} ==> {item[1:]}')
    
    


consumer1.seek_to_beginning()        

#in this part of the code each of the path is being checked and calculated the confidence and written to the data base
count4 = 0
for item in final_paths:
    count4+=1
    count1 = 0
    count2 = 0
    count3 =  0
    value = item[0]

    if len(item) == 1:
        continue
    
    for x in consumer1:
        price_data1 = x.value
        try:
            line = price_data1['bucket']
            count3+=1
            print(f'checked {count3} for {count4} out of {len(final_paths)}')
            print(f"{item[0]} ==> {item[1:]}")
            result = all(word in line for word in item)
            if result:
                count2+=1
                
            if value in line:
                count1+=1
             
            if count3 == 2678000 :
            
                break
        except KeyError:
            print("key not prsent")
    
    
    if count1 != 0 and count2 != 0:
        value = round((count2/count1),3)
        data = {"item1": item[0], "item2": item[1:], "confidence": value}
        insert_result = collection.insert_one(data)
        print(insert_result.inserted_id)
        print(f"{item[0]} ==> {item[1:]} {count2/count1:.2f}")
        
        
    consumer1.seek_to_beginning()       
        
        

        
            

            

     
            

            


