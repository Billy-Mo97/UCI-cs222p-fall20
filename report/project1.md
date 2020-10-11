## Project 1 Report


### 1. Basic information
 - Team #:102
 - Github Repo Link: https://github.com/UCI-Chenli-teaching/cs222p-fall20-team-102
 - Student 1 UCI NetID: hanyanw3
 - Student 1 Name: Hanyan Wang
 - Student 2 UCI NetID: jiaminm1
 - Student 2 Name: Jiaming Mo


### 2. Internal Record Format
- Show your record format design.
1. number of fields (int, 4 bytes)
2. null indicator (ceil(recordDescriptor.size() / 8.0) bits)
3. offsets of non null fields pointing to the end of each actual field data(ints)
4. actual field data

- Describe how you store a null field.
Represent it as 1 in the null indicator,
don't store it in data section.

- Describe how you store a VarChar field.
Store actual characters. Read the end offsets of its previous field and itself,
then compute its length, read it into memory.

- Describe how your record design satisfies O(1) field access.
Using number of fields in section 1, we can get the length of section 1, 2 and 3.
Then any non null field's offset can be retrieved from section 3, and its data can be accessed
using the offset its previous field and itself.
The process above locate the offset and length of all fields in constant time, which is O(1) time.

### 3. Page Format
- Show your page format design.
left to right: starting from 0, insert record one by one.
right to left: First set free space(int) and slotCount(int), 
then directory(offset(int), slotSize(int)) one by one.

- Explain your slot directory design if applicable.
first offset(int), second slotSize(int)

### 4. Page Management
- Show your algorithm of finding next available-space page when inserting a record.
First check current page's free space if it is equal or more than record's size plus two integer 
size (offset(int), slotSize(int)). If not, linearly scan page from the first page, check whether 
it has enough free space. If no page does, append a new page to the end of file, insert the record.

- How many hidden pages are utilized in your design?
One page. The first page (page 0) is to utilized as hidden page.


- Show your hidden page(s) format design if applicable
Store readPageCount, writePageCount, appendPageCount, numOfPages.
Add an end mark to the last 4 bytes.


### 5. Implementation Detail
- Other implementation details goes here.



### 6. Member contribution (for team of two)
- Explain how you distribute the workload in team.
Hanyan Wang wrote pfm.cc, rbfm.cc
Jiaming Mo wrote pfm.cc, rbfm.cc
Both member contributed equally to debug process.
### 7. Other (optional)
- Freely use this section to tell us about things that are related to the project 1, but not related to the other sections (optional)



- Feedback on the project to help improve the project. (optional)
Jiaming Mo: According to my knowledge, this is the very first time CS222P uses google test to run testcase.
It takes some time to adjust. I wasn't sure the bug mentioned in post @24 on piazza is caused by google test, but without related knowledge, 
I was unable to solve the issue and wasted lots of time. I would appreciate if some introduction and more detailed instruction
of google test is provided. Meanwhile, it didn't happen to our team, but I think some beginners of git may find it bit difficult to understand.
They may want some extra help in lab.
