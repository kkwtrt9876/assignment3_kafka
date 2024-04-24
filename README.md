# assignment3_kafka
In this assignment the task was to first create a sample of up to 15 gb of the data from the data set that was provided I.e amazon metadata set. After creating this file , preprocessing was done on the data to get the valuable information.
In the preprocessing step I did batch processing. A collection of 100 json objects were passed to function each time from the 15 gb of data. Inside this function the asin id was taken out from the object along with the also bought items.  A list was created containing the asin id of the item and also bought items. A json object was created from this data having the key as bucket and the list as its value. 

The producer send the data to three topics. 3 consumers were being made which collected the data from each of the topic.

In the consumer1 apriori algorithm was implemented. In this algorithm, first the single frequent items were find out. This was done by setting a threshold of 2500. such a high threshold was set because the dataset was large and there were a lot of items. After that the frequent doubles, triples etc were being find out. After getting all the frequent items association rules were being made. The association rules that were wrong were being corrected for example 
a => a.b.c this was considered as wrong association rule. After getting all the association rules confidence was being calculated in which the threshold was set to 0.6.

in consumer2 pcy algorithm was being implemented, everything was same in it just like apriori but it had the hash table , the  items were hashed to a table , and from that table it was checked that they are frequent or not.

In consumer3, I implemented fp growth algorithm. In this consumer first the single frequent items were calculated. After this this these items were being sorted based on there occurances. From highest to the lowest values and stored.
The original data in the consumer was then filtered based on this dictionary. Meaning that each bucket in the consumer was checked if it had the values In the sorted items. As the sorted items were sorted from highest to lowest, each item in the consumer if it had data in the sorted items was also sorted. Inside the code this is mentioned as ordered_items.
After this step a tree was being made. The ordered_items are passed to the tree function. The item with a maximum count is the first node and the rest are its children.
When the next item is passed if the first element is matches with the node, the node count will be incremented. If it doesn’t matches it will be added as the child of the node.
By this way a tree is formed, where each node will have its count, value and children. 
The paths of each frequent item is then calculated inside the tree I.e what is the path of reaching the element 123 inside the tree. Start with 114,117,123 this is the path.
The counts of each frequent item Is also calculated. Meaning that how many time a node was visited.
Using these counts the paths each of the path elements count is calculated and checked if this is greater than threshold. If it is greater than the threshold then it is considered as a valid path and from it association rule is being formed.
Path e.g [1,2,3,4]
association rule :- [1] => [2,3,4]
this rules is used to calculate the confidence and store the data in the database.

The approach in preprocessing was done so that the bucket is being formed which shows the item and the relevant items with that item. So that the frequent items algorithms can be applied on it. Apart from this approach I couldn’t find anything in the data that showed how an item is linked with other items. Hence this approach helped to find the relations between one item and other items , and made it suitable for the frequent items mining algorithms.
The fp growth algorithm was used because the dataset was large and it felt suitable for it because of the use of tree data structure in it. The retrieval of data and finding frequent pair from the tree data structure is comparatively faster as compared to other algorithms on such a large data set.
