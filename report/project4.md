## Project 4 Report


### 1. Basic information
 - Team #:102
 - Github Repo Link:https://github.com/UCI-Chenli-teaching/cs222p-fall20-team-102/tree/master
 - Student 1 UCI NetID:hanyanw3
 - Student 1 Name:Hanyan Wang
 - Student 2 UCI NetID (if applicable):
 - Student 2 Name (if applicable):


### 2. Catalog information about Index
- Show your catalog information about an index (tables, columns). 
Tables (table-id:int, table-name:varchar(50), file-name:varchar(50), table-flag:int)
Columns(table-id:int, column-name:varchar(50), column-type:int, column-length:int, column-position:int, table-flag: int, hasIndex:int)


### 3. Filter
- Describe how your filter works (especially, how you check the condition.)



### 4. Project
- Describe how your project works.



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
no


- Other implementation details:



### 10. Member contribution (for team of two)
- Explain how you distribute the workload in team.
Hanyan Wang: write and debug rm.cc, qe.cc


### 11. Other (optional)
- Freely use this section to tell us about things that are related to the project 4, but not related to the other sections (optional)



- Feedback on the project to help improve the project. (optional)
