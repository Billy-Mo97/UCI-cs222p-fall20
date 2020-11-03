## Project 2 Report


### 1. Basic information
 - Team #:102
 - Github Repo Link:https://github.com/UCI-Chenli-teaching/cs222p-fall20-team-102
 - Student 1 UCI NetID:hanyanw3
 - Student 1 Name:Hanyan Wang
 - Student 2 UCI NetID (if applicable): jiaminm1
 - Student 2 Name (if applicable): Jiaming Mo

### 2. Meta-data
- Show your meta-data design (Tables and Columns table) and information about each column.
We keep it same in project description.
Tables (table-id:int, table-name:varchar(50), file-name:varchar(50), table-flag:int)
Columns(table-id:int, column-name:varchar(50), column-type:int, column-length:int, column-position:int, table-flag: int)


### 3. Internal Record Format (in case you have changed from P1, please re-enter here)
- Show your record format design.



- Describe how you store a null field.
same as P1


- Describe how you store a VarChar field.



- Describe how your record design satisfies O(1) field access.
same as P1


### 4. Page Format (in case you have changed from P1, please re-enter here)
- Show your page format design.
left to right: starting from 0, insert record one by one.
right to left: First set free space(short) and slotCount(short), 
then directory(offset(short), slotSize(short)) one by one.


- Explain your slot directory design if applicable.
1. offset(short)
2. slotSize(short)


### 5. Page Management (in case you have changed from P1, please re-enter here)
- How many hidden pages are utilized in your design?
same as P1, one page


- Show your hidden page(s) format design if applicable
same as P1.


### 6. Describe the following operation logic.
- Delete a record
we use offset=-1 to represent a deleted slot,
tombstone:flag=-1(char), pageNum(int), slotNum(short)
1.search the target directory, if offset == -1, which means it has been deleted, return -1
2.check the first byte of record, if is -1, it is a tombstone, get the new Rid from tombstone, call deleteRecord with new Rid, continue to step 3 
3.mark offset as -1, shift records left, change their offsets accordingly, change free space of the page

- Update a record
1.search the target directory, if offset == -1, which means it has been deleted, return -1
2.check the first byte of record, if is -1, it is a tombstone, get the new Rid from tombstone, call updateRecord with new Rid, continue to step 3 
3.if newRecordLen == oldRecordLen, update old record, return
4.if newRecordLen < oldRecordLen, update old record, shift records left, change their offsets accordingly,change free space of the page, return
5.if newRecordLen > oldRecordLen, check if there is enough space, if yes, update old record, shift records right, change their offsets accordingly,change free space of the page, return
6.if newRecordLen > oldRecordLen, and there is no enough space, call insertRecord, create a tombstone with pageNum, slotNum, shift records left,change their offsets accordingly,change free space of the page, return

- Scan on normal records
store fileHandle, recordDescriptor,conditionAttribute,compOp,value,attributeNames into
rbfm_ScanIterator. Maintain cur_page and cur_slot in rbfm_ScanIterator.
when getNextRecord(RID &rid, void *data) is called:
read condition attribute, 
if it is null, continue;
else compare it with the given value and comparator, 
    if satisfy, read output attributes, copy them to data, update cur_page and cur_slot, return
    else, go to next record

- Scan on deleted records
continue to next record


- Scan on updated records
continue to next record


### 7. Implementation Detail
- Other implementation details goes here.
We try to follow al the principles and details introduced in project description and lectures.
However, there is one problem stated in piazza post @82, forced us to insert the "table-flag" tuples
of "Tables" and "Columns" after all other 3+5 tuples, in order to pass catalog_columns_table_check.


### 8. Member contribution (for team of two)
- Explain how you distribute the workload in team.

Hanyan Wang: wrote rbfm, debug rdfm and rm
Jiaming Mo: wrote rbfm, debug rbfm and rm

### 9. Other (optional)
- Freely use this section to tell us about things that are related to the project 1, but not related to the other sections (optional)
We feel that some functions in RBFM layer that are not finished in project 1 deserve some individual test cases.


- Feedback on the project to help improve the project. (optional)
