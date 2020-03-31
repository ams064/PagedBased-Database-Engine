## 1. Basic information
- Team #:1
- Github Repo Link:https://github.com/UCI-Chenli-teaching/cs222p-winter20-team-1.git
- Student 1 UCI NetID:somania1
- Student 1 Name:Amit Somani

## 2. Catalog information about Index
##### Show your catalog information about an index (tables, columns).
I am re-using the same catalog as in Project 3. No changes have been made to it. While creating an index, I use the file format name : "tableName+ "_" + attributname + ".idx". The ".idx" format helps in identifying an index from a table.

In Tables file, the entry will be same as for all the other files. In columns file, I create an entry with Attribute vector of size 1. The Attribute vector = attribute on which the index is created.

## 3. Block Nested Loop Join (If you have implemented this feature)
##### Describe how your block nested loop join works (especially, how you manage the given buffers.)

In BNLJoin, we first allocate a buffer of size (numPagesPAGE_SIZE) and fill it with tuples from leftIn iterator until the buffer is full. We also create a in-memory hash map for tuples in the buffer. Which allows for faster look up. Now for each record in the buffer, we get the corresponding matching tuple (match on given condition attribute) from rightIn* iterator and retun the tuples. Once all the tuple from the buffer have been exhausted, we refill the buffer as before, re-fill hashMap and repeat the steps until all the tuples have been joined.

The hashMap key is the join attribute value and the "value" is a vector of offsets of all the tuple in the buffer with the join attribute value same as the key. This will allow us get only those records which match the condition and save comparision with all the tuples in the buffer.

## 4. Index Nested Loop Join (If you have implemented this feature)
##### Describe how your grace hash join works.

In this join, we have been passed an IndexScan Iterator(rightIn). I use the leftIn to get a tuple (X) from the outer table. And then look for the matching tuple in right using index scan. I use setIterator(...) to re-set the iterator to find all the records with attribute value equals to the join attribute value in (X) instead of doing a full scan. This will result in very less disk I/O compared to full index scan each time we need to find a tuple to join.

## 5. Grace Hash Join (If you have implemented this feature)
##### Describe how your grace hash join works (especially, in-memory structure).

Stage 1:
We create n = #numPartitions for each side (leftIn and rightIn) rbfm files using the rbfm createFile APIs.
Once the files have been created, we populate the partitions (writing to disk) using the C++ standard library hash function : 

        hashFunction () { 
            (std::hash<int>/std::hash<float>/std::hash<std::string>{})%numPartitions;
        }
        
The above hash function will return the partition number in which the current tuple will go to.

Stage 2:
We load i-th partition of leftIn into the memory and create an in-memory hashMap similar to the one used in BNL join. For each tuple in the i-th left partition, we find the matching tuple in i-th right partition (not loaded into the memory, accessed using rbfm scan iterator). Once all the i-th left parition tuples have been exhausted for join operation. We load (i+1)-thi left partition in the memory and repeat above steps with (i+1)-th right partition.

Stage 3:
After the join operation is over, the intermediate parition files are cleaned up.

Intermediate file format :
Intermediate files created during GHJoin are created using RBFM layer.
The file name is as follows : leftjoinX_Y, rightjoinX_Y.
X = the join counter.
Where Y runs from 0 -> numPartition -1.

## 6. Aggregation
##### Describe how your aggregation (basic, group-based hash) works.

The basic aggregation is performed by reading each tuple using the passed iterator and get the attribute value which needs to be aggregated and perform the required aggregation operation. This will be done for all the values in the iterator and then final result will be returned.

Group based aggregation : In this, we create in-memory hashMap<groupID, aggregatedValue>, for each tuple -> we get its group attribute value and aggregate attribute value. Store it in the hashMap. We do it for all the tuples and return the hashMap (key, value) pairs one-by-one.

## 7. Implementation Detail
##### Have you added your own source file (.cc or .h)?
No
##### Have you implemented any optional features? Then, describe them here.
No
##### Other implementation details:


## 6. Other (optional)
##### Freely use this section to tell us about things that are related to the project 4, but not related to the other sections (optional)
