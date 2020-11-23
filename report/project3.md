## Project 3 Report


### 1. Basic information
 - Team #:102
 - Github Repo Link:https://github.com/UCI-Chenli-teaching/cs222p-fall20-team-102/
 - Student 1 UCI NetID: hanyanw3
 - Student 1 Name: Hanyan Wang
 - Student 2 UCI NetID (if applicable): jiaminm1
 - Student 2 Name (if applicable): Jiaming Mo


### 2. Meta-data page in an index file
- Show your meta-data page of an index design if you have any. 
we use page 0 as hidden page to store readNum, writeNum, appendNum, pageNum, same as project 1 and project 2.
Then we use page 1 as root pointer page to store rootPageNum(int), minLeafPageNum(int),attributeType(AttrType).


### 3. Index Entry Format
- Show your index entry design (structure). 

  - entries on internal nodes:  
  left node pointer
  right node pointer
  key
  keysize
  - entries on leaf nodes:
  key
  rid
  keysize

### 4. Page Format
- Show your internal-page (non-leaf node) design.
left to right:
1.nodeType(char)
2.sizeInPage(short)
3.entries
right to left:
1.entryCount(short)
2.slot offsets(shorts)

- Show your leaf-page (leaf node) design.
left to right:
1.nodeType(char)
2.sizeInPage(short)
3.rightNodePointer
4.overflowPointer
5.entries
right to left:
1.entryCount(short)
2.slot offsets(shorts)

### 5. Describe the following operation logic.
- Split
if cur node is leaf node:
    create a newNode(leaf), newNode.rightPointer=cur.rightPointer, cur.rightPointer=newNode
    copy up the first key in newNode, insert it to the parent node of cur,
    if doesn't exist, create one
if cur node is internal node:
    create a newNode(internal)
    push up the first key of newNode to the parent of cur
    if doesn't exist, create one
- Rotation (if applicable)



- Merge/non-lazy deletion (if applicable)



- Duplicate key span in a page
Start from the mid of page, search from mid to end until we find a different key, say the 
position is i, move entry i till end to the newNode, in this case, same keys remain in the 
original node;
if i == end, we search from mid to front until we find a different key, say the position
is j, move entry j+1 til end to the newNode, in this case, same keys move to newNode;

- Duplicate key span multiple pages (if applicable)



### 6. Implementation Detail
- Have you added your own module or source file (.cc or .h)? 
  Clearly list the changes on files and CMakeLists.txt, if any.



- Other implementation details:
We define two entry class to store each entry of leaf node and internal node in memory. Also, we define LeafNode and InternalNode class
to store nodes in memory. Each time we touch a node in B+ Tree, we check whether it's cached in memory. If not, we read it from pages
in index file. And every time we revise or append a new node in B+ Tree, we immediately flush it into pages index file. This strategy 
allow us to access only the pages we need, and thus reduce the disk I/O.


### 7. Member contribution (for team of two)
- Explain how you distribute the workload in team.
Hanyan Wang:write and debug ix
Jiaming Mo: write and debug ix

### 8. Other (optional)
- Freely use this section to tell us about things that are related to the project 3, but not related to the other sections (optional)



- Feedback on the project to help improve the project. (optional)
We feel that some of the test may need more explanation with it. For example, in split_rotate_and_promote_on_insertion,
we didn't realize that the test shuffle keys vector in random order at first, and may print out different B+ Tree every time we 
ran the test, and we consumed a lot of time on it.