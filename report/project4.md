## Project 4 Report


### 1. Basic information
 - Team #:102
 - Github Repo Link:https://github.com/UCI-Chenli-teaching/cs222p-fall20-team-102/tree/master
 - Student 1 UCI NetID:hanyanw3
 - Student 1 Name:Hanyan Wang
 - Student 2 UCI NetID (if applicable): jiaminm1
 - Student 2 Name (if applicable): Jiaming Mo


### 2. Catalog information about Index
- Show your catalog information about an index (tables, columns). 
Tables (table-id:int, table-name:varchar(50), file-name:varchar(50), table-flag:int)
Columns(table-id:int, column-name:varchar(50), column-type:int, column-length:int, column-position:int, table-flag: int, hasIndex:int)


### 3. Filter
- Describe how your filter works (especially, how you check the condition.)
getAttributes(): directly call getAttributes() from input iterator.

getNextTuple():
1. It reads in the input iterator, loads its attributes, and gets the condition attribute's index.
2. It calls getNextTuple() from input iterator and retrieves the data of tuple.
3. It checks the value of condition attribute within the data, if it satisfies the condition, return it, otherwise jump to 2.
4. If getNextTuple returns QE_EOF, return QE_EOF.

Before checking the condition, we get the condition index from attributes vector. We read in the null indicator in the retrieved data,
get the null field of the condition attribute with the index. If it is not null, do following steps to attributes before the condition
attribute: check its null field, if it is not null, add its length to offset. Finally, we get the offset of the condition attribute,
then we can read and check it. If it is int type or real type, we can do simple compare. If it is varchar type, we call strcmp to get
the compare result.

### 4. Project
- Describe how your project works.
getAttributes(): It reads in the input iterator and attrNames vector. To get the attributes to project, first it calls getAttributes() in
input iterator and gets the complete attribute vector, then it checks each name in attrName vector, get the corresponding
attribute from the complete attributes vector, and push it into attributes vector.

getNextTuple(): First, for each attribute that we are going to project, we get its index in all attributes, save it in a vector. Then, call
getNextTuple() from input iterator, retrieve the tuple's data, and get the starting offset of each attribute. Finally, we build the null
indicator of the projected attributes, read each projected attribute value with its starting offset, then return the data.


### 5. Block Nested Loop Join
- Describe how your block nested loop join works (especially, how you manage the given buffers.)
we use 2 vectors,outVector and innerVector to store records, outVector will not exceed
numPages * PAGE_SIZE, innerVector will not exceed PAGE_SIZE, for each record in outVector
scan all inner records and compare, repeat the process until we scan all out records.

### 6. Index Nested Loop Join
- Describe how your index nested loop join works. 
for each record we fectch from leftIterator, set the right iterator based on key,
for each satisfying right record, join them.

### 7. Grace Hash Join (If you have implemented this feature)
- Describe how your grace hash join works (especially, in-memory structure).
1. partition left and right tables 
2.for each left partition, create a map<Value,vector<Tuple>>, Value contains the data
of condition attribute
3.for each right partition, look up in the map to join tuples or move to the next

### 8. Aggregation
- Describe how your basic aggregation works.
maintain variables like min, max, sum, count, while scanning, update them,
return corresponding value according to operator

- Describe how your group-based aggregation works. (If you have implemented this feature)
create a map<Value, AggregateByGroupResult>, Value contains group information, 
AggregateByGroupResult maintain variables like min, max, sum, count, 
while scanning, update the map, traverse the map to give final results.

### 9. Implementation Detail
- Have you added your own module or source file (.cc or .h)? 
  Clearly list the changes on files and CMakeLists.txt, if any.
No.


- Other implementation details:



### 10. Member contribution (for team of two)
- Explain how you distribute the workload in team.
Hanyan Wang: write and debug rm.cc, qe.cc
Jiaming Mo: responsible for writing filter, project and debugging all functions.

### 11. Other (optional)
- Freely use this section to tell us about things that are related to the project 4, but not related to the other sections (optional)
We found a pretty strange feedback on ghjoin tests. It passed when we tried with individual test, but failed when we ran the entire qetest_public.
When we tried it on gitHub, it showed Setup timed out in 60000 milliseconds. But SetUp is common in all qe tests, and none of other tests timed out.
We found it a little bit confusing, maybe need some explanation on time requirement for each test.


- Feedback on the project to help improve the project. (optional)
