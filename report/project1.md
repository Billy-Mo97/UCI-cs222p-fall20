## Project 1 Report


### 1. Basic information
 - Team #:102
 - Github Repo Link:https://github.com/UCI-Chenli-teaching/cs222p-fall20-team-102
 - Student 1 UCI NetID:hanyanw3
 - Student 1 Name:Hanyan Wang
 - Student 2 UCI NetID (if applicable):
 - Student 2 Name (if applicable):


### 2. Internal Record Format
- Show your record format design.
1.number of fields(int, 4btyes)
2.null indicator(ceil(recordDescriptor.size() / 8.0) bits)
3.offsets of non null fields pointing to the start of actual data(ints)
4.data

- Describe how you store a null field.
represent it as 1 in the null indicator,
don't store it in data section.

- Describe how you store a VarChar field.
store actual characters. 

- Describe how your record design satisfies O(1) field access.
using number of fields we can get the length of section 1
and 2, using the offset in section 3 to access data.

### 3. Page Format
- Show your page format design.
left to right: starting from 0, insert record one by one.
right to left: First set free space(int) and slotCount(int), 
then directory(offset(int), slotSize(int)) one by one.

- Explain your slot directory design if applicable.
first offset(int), second slotSize(int)

### 4. Page Management
- Show your algorithm of finding next available-space page when inserting a record.
linearly scan page from the first page, check whether it has enough free space,
for each page, free space=PAGE_SIZE-slot size-directory-free space-slotCount.

- How many hidden pages are utilized in your design?



- Show your hidden page(s) format design if applicable



### 5. Implementation Detail
- Other implementation details goes here.



### 6. Member contribution (for team of two)
- Explain how you distribute the workload in team.
Hanyan Wang wrote pfm.cc, rbfm.cc


### 7. Other (optional)
- Freely use this section to tell us about things that are related to the project 1, but not related to the other sections (optional)



- Feedback on the project to help improve the project. (optional)