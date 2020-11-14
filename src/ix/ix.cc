#include "src/include/ix.h"
#include <string>
#include <sys/stat.h>
#include <stdio.h>
#include <iostream>

namespace PeterDB {
    IndexManager &IndexManager::instance() {
        static IndexManager _index_manager = IndexManager();
        return _index_manager;
    }

    RC IndexManager::createFile(const std::string &fileName) {
        //Create the index file according to the given file name.
        FILE *file;
        struct stat buffer{};
        //If the file already exist, return -1.
        if ((stat(fileName.c_str(), &buffer) == 0)) {
            //printf("already exist\n");
            return -1;
        } else {
            file = fopen(fileName.c_str(), "w+b");//original: w+b
            if (file == nullptr) {
                printf("null case\n");
                return -1;
            } else {
                //If the file has been successfully created,
                //initiate readPageCount, writePageCount, appendPageCount, numOfPages to 0,
                //root, minLeaf and attrType to -1.
                //write them to the hidden page of the file (first page).
                unsigned readNum = 0, writeNum = 0, appendNum = 0, pageNum = 0;
                int root = -1, minLeaf = -1, nullAttr = -1;
                fseek(file, 0, SEEK_SET);
                fwrite(&readNum, sizeof(unsigned), 1, file);
                fwrite(&writeNum, sizeof(unsigned), 1, file);
                fwrite(&appendNum, sizeof(unsigned), 1, file);
                fwrite(&pageNum, sizeof(unsigned), 1, file);
                fwrite(&root, sizeof(int), 1, file);
                fwrite(&minLeaf, sizeof(int), 1, file);
                fwrite(&nullAttr, sizeof(int), 1, file);
                //Write end mark into the hidden page.
                unsigned end = 0;
                fseek(file, PAGE_SIZE - sizeof(unsigned), SEEK_SET);
                fwrite(&end, sizeof(unsigned), 1, file);
                fclose(file);
                return 0;
            }
        }
    }

    RC IndexManager::destroyFile(const std::string &fileName) {
        if (remove(fileName.c_str()) == 0)return 0;
        else return -1;
    }

    RC IXFileHandle::readHiddenPage() {
        //Read the hidden page of the file,
        //initiate corresponding parameters in IXFileHandle class.
        if (!fileHandle.pointer) return -1;
        fseek(fileHandle.pointer, 0, SEEK_SET);
        char *data = (char *) malloc(PAGE_SIZE);
        size_t readSize = fread(data, 1, PAGE_SIZE, fileHandle.pointer);
        if (readSize != PAGE_SIZE) {
            free(data);
            return -1;
        }
        //Read ixReadPageCounter, ixWritePageCounter, ixAppendPageCounter,
        //numOfPages, root, minLeaf into IXFileHandle.
        //Also, allocate space for bTree in IXFileHandle,
        //read attrType into it.
        short offset = 0;
        memcpy(&ixReadPageCounter, data + offset, sizeof(unsigned));
        offset += sizeof(unsigned);
        memcpy(&ixWritePageCounter, data + offset, sizeof(unsigned));
        offset += sizeof(unsigned);
        memcpy(&ixAppendPageCounter, data + offset, sizeof(unsigned));
        offset += sizeof(unsigned);
        memcpy(&numOfPages, data + offset, sizeof(int));
        offset += sizeof(unsigned);
        memcpy(&root, data + offset, sizeof(int));
        offset += sizeof(int);
        memcpy(&minLeaf, data + offset, sizeof(int));
        offset += sizeof(int);
        bTree = new BTree();
        memcpy(&bTree->attrType, data + offset, sizeof(AttrType));
        offset += sizeof(AttrType);
        free(data);
        return 0;
    }

    RC IXFileHandle::writeHiddenPage() {
        //Write ixReadPageCounter, ixWritePageCounter, ixAppendPageCounter,
        //numOfPages, root, minLeaf into hidden page.
        //Also, write attrType from bTree into hidden page.
        if (!fileHandle.pointer) return -1;
        fseek(fileHandle.pointer, 0, SEEK_SET);
        char *data = (char *) calloc(PAGE_SIZE, 1);
        short offset = 0;
        memcpy(data + offset, &ixReadPageCounter, sizeof(unsigned));
        offset += sizeof(unsigned);
        memcpy(data + offset, &ixWritePageCounter, sizeof(unsigned));
        offset += sizeof(unsigned);
        memcpy(data + offset, &ixAppendPageCounter, sizeof(unsigned));
        offset += sizeof(unsigned);
        memcpy(data + offset, &numOfPages, sizeof(int));
        offset += sizeof(unsigned);
        memcpy(data + offset, &root, sizeof(int));
        offset += sizeof(int);
        memcpy(data + offset, &minLeaf, sizeof(int));
        offset += sizeof(int);
        memcpy(data + offset, &bTree->attrType, sizeof(AttrType));
        offset += sizeof(AttrType);
        size_t writeSize = fwrite(data, 1, PAGE_SIZE, fileHandle.pointer);
        if (writeSize != PAGE_SIZE) {
            free(data);
            return -1;
        }
        fflush(fileHandle.pointer);
        free(data);
        return 0;
    }

    RC IndexManager::openFile(const std::string &fileName, IXFileHandle &ixFileHandle) {
        //Open the index file by given file name.
        //Initiate all the parameters in ixFileHandle class.
        //If the index is not null, initiate the bTree accordingly.
        //Otherwise, set the root and minLeaf to -1.
        PeterDB::PagedFileManager &pfm = PeterDB::PagedFileManager::instance();
        if (pfm.openFile(fileName, ixFileHandle.fileHandle) == -1) return -1;
        if (ixFileHandle.readHiddenPage() == -1) return -1;
        if (ixFileHandle.root != NULLNODE) {
            char *rootPage = (char *) calloc(PAGE_SIZE, 1);
            if(ixFileHandle.readPage(ixFileHandle.root, rootPage) == -1) { return -1; }
            if(ixFileHandle.bTree->generateNode(rootPage, ixFileHandle.bTree->root) == -1) { return -1; }
            ixFileHandle.bTree->root->pageNum = ixFileHandle.root;
            free(rootPage);
            ixFileHandle.bTree->nodeMap[ixFileHandle.root] = ixFileHandle.bTree->root;
        }
        if (ixFileHandle.minLeaf == NULLNODE) ixFileHandle.minLeaf = ixFileHandle.root;
        else {
            char *minPage = (char *) calloc(PAGE_SIZE, 1);
            if(ixFileHandle.readPage(ixFileHandle.minLeaf, minPage) == -1) { return -1; }
            if(ixFileHandle.bTree->generateNode(minPage, ixFileHandle.bTree->minLeaf) == -1) { return -1; }
            ixFileHandle.bTree->minLeaf->pageNum = ixFileHandle.minLeaf;
            free(minPage);
            ixFileHandle.bTree->nodeMap[ixFileHandle.minLeaf] = ixFileHandle.bTree->minLeaf;
        }
        return 0;
    }

    IXFileHandle::IXFileHandle() {
        ixReadPageCounter = 0;
        ixWritePageCounter = 0;
        ixAppendPageCounter = 0;
        numOfPages = 0;
        bTree = NULL;
        root = -1;
        minLeaf = -1;
    }

    IXFileHandle::~IXFileHandle() {}

    RC IXFileHandle::readPage(PageNum pageNum, void *data) {
        ixReadPageCounter++;
        RC res = fileHandle.readPage(pageNum, data);
        return res;
    }

    RC IXFileHandle::writePage(PageNum pageNum, const void *data) {
        ixWritePageCounter++;
        RC res = fileHandle.writePage(pageNum, data);
        return res;
    }

    RC IXFileHandle::appendPage(const void *data) {
        ixAppendPageCounter++;
        RC res = fileHandle.appendPage(data);
        return res;
    }

    RC IndexManager::closeFile(IXFileHandle &ixFileHandle) {
        //Close the ixFileHandle.
        //Before closing the pointer, write all the parameters in ixFileHandle
        //back to hidden page, deallocate the space of bTree.
        if (ixFileHandle.writeHiddenPage() == -1) { return -1; }
        if (PagedFileManager::instance().closeFile(ixFileHandle.fileHandle) == -1) { return -1; }
        delete ixFileHandle.bTree;
        ixFileHandle.bTree = nullptr;
        return 0;
    }

    RC
    IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        LeafEntry leafEntry(attribute.type, key, rid);
        return ixFileHandle.bTree->insertEntry(ixFileHandle, leafEntry);
    }

    RC
    IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        return -1;
    }

    RC IndexManager::printLeafKey(int start, int i, Node *node, const Attribute &attribute, std::ostream &out) {
        //This is a helper function to print out the key in "start" entry, and all rids with this key.
        out << '"';
        if (attribute.type == TypeInt) {
            int key;
            memcpy(&key, (char *) (dynamic_cast<LeafNode *>(node))->leafEntries[start].key, sizeof(int));
            out << key << ":[";
        } else if (attribute.type == TypeReal) {
            float key;
            memcpy(&key, (char *) (dynamic_cast<LeafNode *>(node))->leafEntries[start].key, sizeof(float));
            out << key << ":[";
        } else if (attribute.type == TypeVarChar) {
            int len;
            memcpy(&len, (dynamic_cast<LeafNode *>(node))->leafEntries[start].key, sizeof(int));
            std::string str((char *) (dynamic_cast<LeafNode *>(node))->leafEntries[start].key + sizeof(int),
                            len);
            out << str << ":[";
        }
        for (int j = start; j < i; j++) {
            out << "(" << dynamic_cast<LeafNode *>(node)->leafEntries[j].rid.pageNum << ","
                << dynamic_cast<LeafNode *>(node)->leafEntries[j].rid.slotNum << ")";
            if (j != i - 1) out << ",";
        }
        out << "]";
        out << '"';
        return 0;
    }

    RC IndexManager::leafDFS(IXFileHandle &ixFileHandle, Node *node, const Attribute &attribute,
                             std::ostream &out) const {
        //This is a helper function to print out all the keys and rids in a leaf node.
        int start = 0;
        LeafEntry entry = dynamic_cast<LeafNode *>(node)->leafEntries[0];
        //Starting from the first entry in the node, print out the keys and rids.
        for (int i = 0; i <= dynamic_cast<LeafNode *>(node)->leafEntries.size(); i++) {
            if (i == dynamic_cast<LeafNode *>(node)->leafEntries.size()) {
                if(printLeafKey(start, i, node, attribute, out) == -1) { return -1; }
            } else {
                //"start" marks the beginning of a new key, we iterate until we find a different key.
                //Then, we print out the key, and all rids having this key.
                if (compareKey(attribute.type, entry.key, dynamic_cast<LeafNode *>(node)->leafEntries[i].key) ==
                    0)
                    continue;
                else {
                    if(printLeafKey(start, i, node, attribute, out) == -1) { return -1; }
                    start = i;
                    entry = dynamic_cast<LeafNode *>(node)->leafEntries[i];
                    if (i == dynamic_cast<LeafNode *>(node)->leafEntries.size()) out << ",";
                }
            }
        }
        return 0;
    }


    RC IndexManager::printInternalNode(IXFileHandle &ixFileHandle, Node *node, const Attribute &attribute,
                                       std::ostream &out) {
        //print internal node
        for (int i = 0; i < dynamic_cast<InternalNode *>(node)->internalEntries.size(); i++) {
            out << '"';
            if (attribute.type == TypeInt) {
                int key;
                memcpy(&key, (char *) (dynamic_cast<LeafNode *>(node))->leafEntries[i].key, sizeof(int));
                out << key << '"';
            } else if (attribute.type == TypeReal) {
                float key;
                memcpy(&key, (char *) (dynamic_cast<LeafNode *>(node))->leafEntries[i].key, sizeof(float));
                out << key << '"';
            } else if (attribute.type == TypeVarChar) {
                int len;
                memcpy(&len, (dynamic_cast<LeafNode *>(node))->leafEntries[i].key, sizeof(int));
                std::string str((char *) (dynamic_cast<LeafNode *>(node))->leafEntries[i].key + sizeof(int), len);
                out << str << '"';
            }
            if (i != dynamic_cast<InternalNode *>(node)->internalEntries.size() - 1) out << ",";
        }
        return 0;
    }

    RC IndexManager::DFS(IXFileHandle &ixFileHandle, Node *node, const Attribute &attribute, std::ostream &out) const {
        //Do breadth-first search on B+tree, starting on given node.
        //If the node is not loaded, load it into memory.
        if (node->isLoaded == false) {
            if (ixFileHandle.bTree->loadNode(ixFileHandle, node) == -1) {
                return -1;
            }
        }
        out << "{";
        out << "\"keys\":";
        out << "[";
        //If current node is a leaf node, print it.
        if (node->type == LEAF) {
            if(leafDFS(ixFileHandle, node, attribute, out) == -1) { return -1; }
            out << "]";
        } else {
            //If current node is a internal node,
            //print internal node.
            printInternalNode(ixFileHandle, node, attribute, out);
            out << "]";
            //print its children.
            out << ",";
            out << "\"children\":[";
            for (int i = 0; i < dynamic_cast<InternalNode *>(node)->internalEntries.size(); i++) {
                //For each internal node's entry, do DFS search on its left child.
                if(DFS(ixFileHandle, dynamic_cast<InternalNode *>(node)->internalEntries[i].left, attribute, out) == -1) { return -1; }
                out << ",";
                //If it is the last entry on its node, do DFS search on its right child.
                if (i == dynamic_cast<InternalNode *>(node)->internalEntries.size() - 1) {
                    if(DFS(ixFileHandle, dynamic_cast<InternalNode *>(node)->internalEntries[i].right, attribute, out) == -1) { return -1; }
                }
            }
            out << "]";
        }
        out << "}";
        return 0;
    }

    RC IndexManager::printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) {
        return DFS(ixFileHandle, ixFileHandle.bTree->root, attribute, out);
    }

    IX_ScanIterator::IX_ScanIterator() = default;

    IX_ScanIterator::~IX_ScanIterator() = default;

    RC IndexManager::scan(IXFileHandle &ixFileHandle,
                          const Attribute &attribute,
                          const void *lowKey,
                          const void *highKey,
                          bool lowKeyInclusive,
                          bool highKeyInclusive,
                          IX_ScanIterator &ix_ScanIterator) {
        return -1;
    }

    RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
        return -1;
    }

    RC IX_ScanIterator::close() {
        return -1;
    }

    RC
    IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) const {
        readPageCount = ixReadPageCounter;
        writePageCount = ixWritePageCounter;
        appendPageCount = ixAppendPageCounter;
        return 0;
    }

    RC BTree::generatePageHeader(Node *node, char *page, short &offset) {
        //Both internal and leaf pages share:
        //Node type, node size, node parent,
        memcpy(page + offset, &node->type, sizeof(char)); //internal node or leaf node
        offset += sizeof(char);
        memcpy(page + offset, &node->sizeInPage, sizeof(short));
        offset += sizeof(short);
        int tmp;
        if (node->parent) {
            tmp = node->parent->pageNum;
        } else {
            tmp = NULLNODE;
        }
        memcpy(page + offset, &tmp, sizeof(PageNum));
        offset += sizeof(PageNum);
        return 0;
    }

    RC BTree::setInternalEntryKeyInPage(Node *node, char *page, short &offset, int i) const {
        if (attrType == TypeInt) {
            memcpy(page + offset, dynamic_cast<InternalNode *>(node)->internalEntries[i].key, sizeof(int));
            offset += sizeof(int);
        } else if (attrType == TypeReal) {
            memcpy(page + offset, dynamic_cast<InternalNode *>(node)->internalEntries[i].key, sizeof(float));
            offset += sizeof(float);
        } else if (attrType == TypeVarChar) {
            int strLen;
            memcpy(&strLen, dynamic_cast<InternalNode *>(node)->internalEntries[i].key, sizeof(int));
            memcpy(page + offset, dynamic_cast<InternalNode *>(node)->internalEntries[i].key,
                   sizeof(int) + strLen);
            offset += sizeof(int) + strLen;
        }
        return 0;
    }

    RC BTree::generateInternalPage(Node *node, char *page, short &offset) const {
        //The format of internal page has:
        //left child and key of each node, and last node's right child,
        //entry offset directory, and slot count.
        short slotCount = dynamic_cast<InternalNode *>(node)->internalEntries.size();
        memcpy(page + PAGE_SIZE - sizeof(short), &slotCount, sizeof(short));
        for (int i = 0; i < slotCount; i++) {
            //If the entry is not the last, only write its left child
            int leftPage = dynamic_cast<InternalNode *>(node)->internalEntries[i].left->pageNum;
            memcpy(page + offset, &leftPage, sizeof(PageNum));
            offset += sizeof(PageNum);
            //set entry offset
            memcpy(page + PAGE_SIZE - (i + 2) * sizeof(short), &offset, sizeof(short));
            //set key
            if(setInternalEntryKeyInPage(node, page, offset, i) == -1) { return -1; }
            //If entry is the last, write its right child.
            if (i == slotCount - 1) {
                int rightPage = dynamic_cast<InternalNode *>(node)->internalEntries[i].right->pageNum;
                memcpy(page + offset, &rightPage, sizeof(PageNum));
                offset += sizeof(PageNum);
            }
        }
        return 0;
    }

    RC BTree::setLeafEntryKeyInPage(Node *node, char *page, short &offset, int i) const {
        if (attrType == TypeInt) {
            memcpy(page + offset, dynamic_cast<LeafNode *>(node)->leafEntries[i].key, sizeof(int));
            offset += sizeof(int);
        } else if (attrType == TypeReal) {
            memcpy(page + offset, dynamic_cast<LeafNode *>(node)->leafEntries[i].key, sizeof(float));
            offset += sizeof(float);
        } else if (attrType == TypeVarChar) {
            int strLen;
            memcpy(&strLen, dynamic_cast<LeafNode *>(node)->leafEntries[i].key, sizeof(int));
            memcpy(page + offset, dynamic_cast<LeafNode *>(node)->leafEntries[i].key, sizeof(int) + strLen);
            offset += sizeof(int) + strLen;
        }
        return 0;
    }

    RC BTree::generateLeafPage(Node *node, char *page, short &offset) const {
        //The format of internal page has:
        //right pointer, overflow pointer,
        //key and rid for each entry,
        //entry offset directory, and slot count.

        //set right pointer
        int rightPage;
        if (dynamic_cast<LeafNode *>(node)->rightPointer) {
            rightPage = dynamic_cast<LeafNode *>(node)->rightPointer->pageNum;
        } else {
            rightPage = NULLNODE;
        }
        memcpy(page + offset, &rightPage, sizeof(PageNum));
        offset += sizeof(PageNum);

        //set overflow pointer
        int overflowPage;
        if (dynamic_cast<LeafNode *>(node)->overflowPointer) {
            overflowPage = dynamic_cast<LeafNode *>(node)->overflowPointer->pageNum;
        } else {
            overflowPage = NULLNODE;
        }
        memcpy(page + offset, &overflowPage, sizeof(PageNum));
        offset += sizeof(PageNum);

        //set slotCount
        short slotCount = dynamic_cast<LeafNode *>(node)->leafEntries.size();
        memcpy(page + PAGE_SIZE - sizeof(short), &slotCount, sizeof(short));

        for (int i = 0; i < slotCount; i++) {
            //set entry offset
            memcpy(page + PAGE_SIZE - (i + 2) * sizeof(short), &offset, sizeof(short));
            //set entry key
            setLeafEntryKeyInPage(node, page, offset, i);
            //set rid
            memcpy(page + offset, &dynamic_cast<LeafNode *>(node)->leafEntries[i].rid.pageNum, sizeof(PageNum));
            offset += sizeof(PageNum);
            memcpy(page + offset, &dynamic_cast<LeafNode *>(node)->leafEntries[i].rid.slotNum, sizeof(short));
            offset += sizeof(short);
        }

        return 0;
    }

    RC BTree::generatePage(Node *node, char *data) const {
        //Generate the page according to the node information.
        if (node == nullptr) return -1;
        char *page = (char *) calloc(PAGE_SIZE, 1);
        short offset = 0;
        if(generatePageHeader(node, page, offset) == -1) { return -1; }
        if (node->type == INTERNAL) {
            if(generateInternalPage(node, page, offset) == -1) {
                return -1;
            }
        } else if (node->type == LEAF) {
            if(generateLeafPage(node, page, offset) == -1) {
                return -1;
            }
        }
        memcpy(data, page, PAGE_SIZE);
        free(page);
        return 0;
    }

    RC BTree::generateNodeHeader(Node *res, char *data, short &offset, short &slotCount) {
        //This is a helper function to get some shared information from index file for both internal node and leaf node:
        //type, size, parent and slotCount.
        //Then set isLoaded to true,
        res->type = attrType;
        short size;
        memcpy(&size, data + offset, sizeof(short));
        offset += sizeof(short);
        res->sizeInPage = size;
        int parent;
        memcpy(&parent, data + offset, sizeof(int));
        offset += sizeof(int);
        if (parent != NULLNODE) res->parent = nodeMap[parent];
        else res->parent = nullptr;
        memcpy(&slotCount, data + PAGE_SIZE - sizeof(short), sizeof(short));
        res->isLoaded = true;
        return 0;
    }

    RC BTree::getKeyLen(char *data, short &offset, short &keyLen) const {
        if (attrType == TypeInt) {
            keyLen = sizeof(int);
        } else if (attrType == TypeReal) {
            keyLen = sizeof(float);
        } else if (attrType == TypeVarChar) {
            int strLen;
            memcpy(&strLen, data + offset, sizeof(int));
            keyLen = sizeof(int) + strLen;
        }
        return 0;
    }

    RC BTree::getLeftInternalEntry(InternalEntry &entry, const char *data, short slotOffset) {
        int leftPageNum = *(int *) (data + slotOffset - sizeof(int));
        if (nodeMap.find(leftPageNum) != nodeMap.end()) {
            entry.left = nodeMap[leftPageNum];
        } else {
            Node *left = new Node();
            entry.left = left;
            entry.left->pageNum = leftPageNum;
        }
        return 0;
    }

    RC BTree::getRightInternalEntry(InternalEntry &entry, char *data, short offset, short keyLen) {
        int rightPageNum;
        memcpy(&rightPageNum, data + offset + keyLen, sizeof(int));
        if (nodeMap.find(rightPageNum) != nodeMap.end()) {
            entry.right = nodeMap[rightPageNum];
        } else {
            Node *right = new Node();
            entry.right = right;
            entry.right->pageNum = rightPageNum;
        }
        return 0;
    }

    RC BTree::generateInternalNode(Node *res, char *data, short &offset, short &slotCount) {
        //This is a helper function to initialize following information of internal node:
        //left child, right child of each entry, entries vector in this node.
        for (int i = 0; i < slotCount; i++) {
            short slotOffset;
            memcpy(&slotOffset, data + PAGE_SIZE - (i + 2) * sizeof(short), sizeof(short));
            short keyLen = 0;
            if(getKeyLen(data, offset, keyLen) == -1) { return -1; }
            InternalEntry entry(attrType, data + slotOffset);
            if(getLeftInternalEntry(entry, data, slotOffset) == -1) { return -1; }
            if(getRightInternalEntry(entry, data, offset, keyLen) == -1) { return -1; }
            dynamic_cast<InternalNode *>(res)->internalEntries.push_back(entry);
        }
        return 0;
    }

    RC BTree::getRightPointerInLeafNode(Node *res, char *data, short &offset) {
        PageNum rightPageNum;
        memcpy(&rightPageNum, data + offset, sizeof(PageNum));
        offset += sizeof(PageNum);
        if (rightPageNum != NULLNODE) {
            if (nodeMap.find(rightPageNum) != nodeMap.end()) {
                dynamic_cast<LeafNode *>(res)->rightPointer = nodeMap[rightPageNum];
            } else {
                Node *right = new Node();
                dynamic_cast<LeafNode *>(res)->rightPointer = right;
                dynamic_cast<LeafNode *>(res)->rightPointer->pageNum = rightPageNum;
            }
        } else {
            dynamic_cast<LeafNode *>(res)->rightPointer = nullptr;
        }
        return 0;
    }

    RC BTree::getOverflowPointerInLeafNode(Node *res, char *data, short &offset) {
        PageNum overflowPageNum;
        memcpy(&overflowPageNum, data + offset, sizeof(PageNum));
        offset += sizeof(PageNum);
        if (overflowPageNum != NULLNODE) {
            if (nodeMap.find(overflowPageNum) != nodeMap.end()) {
                dynamic_cast<LeafNode *>(res)->overflowPointer = nodeMap[overflowPageNum];
            } else {
                Node *overflow = new Node();
                dynamic_cast<LeafNode *>(res)->overflowPointer = overflow;
                dynamic_cast<LeafNode *>(res)->overflowPointer->pageNum = overflowPageNum;
            }
        } else {
            dynamic_cast<LeafNode *>(res)->overflowPointer = nullptr;
        }
        return 0;
    }

    RC BTree::getEntriesInLeafNode(Node *res, char *data, short &offset, short &slotCount) {
        for (int i = 0; i < slotCount; i++) {
            short slotOffset;
            memcpy(&slotOffset, data + PAGE_SIZE - (i + 2) * sizeof(short), sizeof(short));
            short keyLen = 0;
            if(getKeyLen(data, offset, keyLen) == -1) { return -1; }
            RID rid;
            memcpy(&rid.pageNum, data + slotOffset + keyLen, sizeof(PageNum));
            memcpy(&rid.slotNum, data + slotOffset + keyLen + sizeof(PageNum), sizeof(short));
            LeafEntry entry(attrType, data + slotOffset, rid);
            dynamic_cast<LeafNode *>(res)->leafEntries.push_back(entry);
        }
        return 0;
    }

    RC BTree::generateLeafNode(Node *res, char *data, short &offset, short &slotCount) {
        //This is a helper function to initialize following information of leaf node:
        //right pointer, overflow pointer, entries vector in this node.

        if(getRightPointerInLeafNode(res, data, offset) == -1) { return -1; }

        if(getOverflowPointerInLeafNode(res, data, offset) == -1) { return -1; }

        if(getEntriesInLeafNode(res, data, offset, slotCount) == -1) { return -1; }

        return 0;
    }

    RC BTree::generateNode(char *data, Node *res) {
        //This is a helper function that loads all the information of a node from index file into memory.
        //These information is stored in node class in memory.
        //Node class can either be internalNode or leafNode.
        //data pointer is pre-loaded with the data of Node in index file.
        res = new Node();
        short offset, slotCount;
        if(generateNodeHeader(res, data, offset, slotCount) == -1) { return -1; }
        if (res->type == INTERNAL) {
            if(generateInternalNode(res, data, offset, slotCount) == -1) { return -1; }
        } else if (res->type == LEAF) {
            if(generateLeafNode(res, data, offset, slotCount) == -1) { return -1; }
        }
        return 0;
    }

    RC BTree::loadNode(IXFileHandle &ixFileHandle, Node *&node) {
        //This is a helper function that check the given node's pageNum in memory.
        //If it already exits in memory, set the node pointer to point to its address.
        //Otherwise, generate the node, load it into the memory and set the pointer.
        if (node == nullptr) return -1;
        PageNum pageNum = node->pageNum;
        if (nodeMap.find(pageNum) != nodeMap.end()) {
            node = nodeMap[pageNum];
        } else {
            char *pageData = (char *) calloc(PAGE_SIZE, 1);
            if (ixFileHandle.readPage(pageNum, pageData) == -1) {
                free(pageData);
                return -1;
            }
            delete node;
            if(generateNode(pageData, node) == -1) { return -1; }
            node->pageNum = pageNum;
            nodeMap[pageNum] = node;
            free(pageData);
        }
        return 0;
    }

    int IndexManager::compareKey(AttrType attrType, const void *v1, const void *v2) {
        //This is a helper function to compare two given keys v1 and v2.
        //It assumes that both v1 and v2 are not null.
        //If v1 > v2, it returns a positive number,
        //if v1 < v2, it returns a negative number,
        //if v1 = v2, it returns zero.
        if (attrType == TypeInt) {
            return *(int *) v1 - *(int *) v2;
        } else if (attrType == TypeReal) {
            return *(float *) v1 - *(float *) v2 > 0 ? 1 : *(float *) v1 - *(float *) v2 == 0 ? 0 : -1;
        } else if (attrType == TypeVarChar) {
            int strLen1 = *(int *) v1;
            int strLen2 = *(int *) v2;
            std::string s1((char *) v1 + sizeof(int), strLen1);
            std::string s2((char *) v2 + sizeof(int), strLen2);
            return s1.compare(s2);
        }
        return 0;
    }

    int BTree::compareKeyInInternalNode(IXFileHandle &ixFileHandle, const LeafEntry &pair, Node *node) {
        //This is a helper function to compare key in internal node.
        //If it finds pair's value less than any entry's key in given internal node, it loads the entry's left child, and returns 1.
        //Otherwise, it loads the last entry's right child, and returns 0.
        PeterDB::IndexManager &idm = PeterDB::IndexManager::instance();
        int size = dynamic_cast<InternalNode *>(node)->internalEntries.size();
        for (int i = 0; i <= size; i++) {
            if (i == size) {
                ixFileHandle.ixReadPageCounter++;
                node = dynamic_cast<InternalNode *>(node)->internalEntries[size - 1].right;
                if (dynamic_cast<InternalNode *>(node)->isLoaded == false) {
                    loadNode(ixFileHandle, node);
                }
            } else {
                if (idm.compareKey(attrType, dynamic_cast<InternalNode *>(node)->internalEntries[i].key, pair.key) < 0) {
                    ixFileHandle.ixReadPageCounter++;
                    node = dynamic_cast<InternalNode *>(node)->internalEntries[i].left;
                    if (dynamic_cast<InternalNode *>(node)->isLoaded == false) {
                        loadNode(ixFileHandle, node);
                    }
                    return 1;
                }
            }
        }
        return 0;
    }

    RC BTree::findLeafNode(IXFileHandle &ixFileHandle, const LeafEntry &pair, Node *node) {
        while (node->type != LEAF) {
            if(compareKeyInInternalNode(ixFileHandle, pair, node) == 1) {
                break;
            }
        }
        return 0;
    }

    RC BTree::generateRootNode(LeafNode *res, const LeafEntry &pair) {
        //This is a helper function that creates root node with one leaf entry.
        //These information is stored in node class in memory.
        res->leafEntries.push_back(pair);
        //This one leaf entry takes up keySize + directory space (short) in page.
        res->sizeInPage += pair.keySize + sizeof(short);
        res->isLoaded = true;
        return 0;
    }

    RC BTree::initiateNullTree(IXFileHandle &ixFileHandle, const LeafEntry &pair) {
        //This is a helper function to initiate a null B+ Tree with its only entry pair.
        //It completes following tasks:
        //Creates a new node with the pair entry.
        //Set the B+ Tree parameters.
        //Create a new page with the new node and write it into the index file.
        LeafNode *node = new LeafNode();
        if(generateRootNode(node, pair) == -1) { return -1; }
        //set tree
        root = node;
        minLeaf = node;
        //generate the page and write it into index file.
        char *new_page = (char *) calloc(PAGE_SIZE, 1);
        if(generatePage(node, new_page) == -1) { return -1; }
        if(ixFileHandle.appendPage(new_page) == -1) { return -1; }
        free(new_page);
        nodeMap[0] = node;
        node->pageNum = 0;
        //set meta fields
        ixFileHandle.root = 0;
        ixFileHandle.minLeaf = 0;
        return 0;
    }

    RC BTree::insertEntry(IXFileHandle &ixFileHandle, const LeafEntry &pair) {
        //This is a helper function to insert pair entry into B+ Tree.

        //If current B+ Tree is empty, initiate it.
        if (root == nullptr) {
            if(initiateNullTree(ixFileHandle, pair) == -1) { return -1; }
        } else {
            Node *targetNode = root;
            if(findLeafNode(ixFileHandle, pair, targetNode) == -1) { return -1; };     //find the inserted leaf node
            if (dynamic_cast<LeafNode *>(targetNode)->isLoaded == false) {
                loadNode(ixFileHandle, targetNode);
            }
            //insert the entry into targetNode
            bool isAdded = false;
            for (auto i = dynamic_cast<LeafNode *>(targetNode)->leafEntries.begin();
                 i < dynamic_cast<LeafNode *>(targetNode)->leafEntries.end(); i++) {
                if (PeterDB::IndexManager::instance().compareKey(attrType, pair.key, (*i).key) < 0) {
                    dynamic_cast<LeafNode *>(targetNode)->leafEntries.insert(i, pair);
                    isAdded = true;
                    break;
                }
            }
            if (isAdded == false) {
                dynamic_cast<LeafNode *>(targetNode)->leafEntries.push_back(pair);
            }
            dynamic_cast<LeafNode *>(targetNode)->sizeInPage += entryLen + sizeof(short);
            if (dynamic_cast<LeafNode *>(targetNode)->sizeInPage <= PAGE_SIZE) {   //insert into targetNode
                //write the updated target node into file
                char *newPage = (char *) calloc(PAGE_SIZE, 1);
                generatePage(targetNode, newPage);
                ixFileHandle.writePage(dynamic_cast<LeafNode *>(targetNode)->pageNum, newPage);
                free(newPage);
                return 0;
            } else {//split leaf node;
                LeafNode *newNode = new LeafNode();
                newNode->isLoaded = true;
                //entry too large
                if (dynamic_cast<LeafNode *>(targetNode)->leafEntries.size() <= 1) return -1;
                //start from mid, find same keys, put them into the same node
                int mid = dynamic_cast<LeafNode *>(targetNode)->leafEntries.size() / 2;
                int end = 0;
                //search towards end
                for (int i = mid; i <= dynamic_cast<LeafNode *>(targetNode)->leafEntries.size(); i++) {
                    if (i == dynamic_cast<LeafNode *>(targetNode)->leafEntries.size()) {
                        end = 1;
                        break;
                    }
                    if (PeterDB::IndexManager::instance().compareKey(attrType,
                                                                     dynamic_cast<LeafNode *>(targetNode)->leafEntries[i].key,
                                                                     dynamic_cast<LeafNode *>(targetNode)->leafEntries[
                                                                             i - 1].key) == 0)
                        continue;
                    else {
                        mid = i;
                        break;
                    }
                }
                if (end == 1) {
                    //search towards front
                    for (int i = mid; i >= 0; i--) {
                        if (i == 0) return -1;//all keys are the same, can't split
                        if (PeterDB::IndexManager::instance().compareKey(attrType,
                                                                         dynamic_cast<LeafNode *>(targetNode)->leafEntries[i].key,
                                                                         dynamic_cast<LeafNode *>(targetNode)->leafEntries[
                                                                                 i - 1].key) == 0)
                            continue;
                        else {
                            mid = i;
                            break;
                        }
                    }
                }
                //split node and update size
                for (int i = mid; i < dynamic_cast<LeafNode *>(targetNode)->leafEntries.size(); i++) {
                    LeafEntry leafEntry(attrType, dynamic_cast<LeafNode *>(targetNode)->leafEntries[i].key,
                                        dynamic_cast<LeafNode *>(targetNode)->leafEntries[i].rid);
                    dynamic_cast<LeafNode *>(newNode)->leafEntries.push_back(leafEntry);
                    int strLen = 0;
                    if (attrType == TypeVarChar) {
                        memcpy(&strLen, (char *) dynamic_cast<LeafNode *>(targetNode)->leafEntries[i].key, 4);
                    }
                    dynamic_cast<LeafNode *>(targetNode)->sizeInPage -=
                            strLen + sizeof(short) + sizeof(int) + sizeof(PageNum) + sizeof(short);
                    newNode->sizeInPage += strLen + sizeof(short) + sizeof(int) + sizeof(PageNum) + sizeof(short);
                }

                for (auto i = dynamic_cast<LeafNode *>(targetNode)->leafEntries.begin() + mid;
                     i != dynamic_cast<LeafNode *>(targetNode)->leafEntries.end(); i++)
                    free(i->key);
                dynamic_cast<LeafNode *>(targetNode)->leafEntries.erase(
                        dynamic_cast<LeafNode *>(targetNode)->leafEntries.begin() + mid,
                        dynamic_cast<LeafNode *>(targetNode)->leafEntries.end());
                //update rightPointer
                newNode->rightPointer = dynamic_cast<LeafNode *>(targetNode)->rightPointer;
                dynamic_cast<LeafNode *>(targetNode)->rightPointer = newNode;
                //write updated targetNode to file
                char *updatedTargetPage = (char *) calloc(PAGE_SIZE, 1);
                generatePage(targetNode, updatedTargetPage);
                ixFileHandle.writePage(dynamic_cast<LeafNode *>(targetNode)->pageNum, updatedTargetPage);
                free(updatedTargetPage);
                //write new leaf node to file
                char *newLeafPage = (char *) calloc(PAGE_SIZE, 1);
                generatePage(newNode, newLeafPage);
                ixFileHandle.appendPage(newLeafPage);
                nodeMap[ixFileHandle.ixAppendPageCounter - 1] = newNode;
                newNode->pageNum = ixFileHandle.ixAppendPageCounter - 1;
                free(newLeafPage);
                //if no parent node, create one
                if (dynamic_cast<LeafNode *>(targetNode)->parent == NULL) {
                    InternalNode *parent = new InternalNode();
                    parent->isLoaded = true;
                    InternalEntry internal_pair(attrType, newNode->leafEntries[0].key, targetNode, newNode);
                    parent->internalEntries.push_back(internal_pair);
                    parent->type = INTERNAL;
                    int strLen = 0;
                    if (attrType == TypeVarChar) {
                        memcpy(&strLen, (char *) dynamic_cast<LeafNode *>(newNode)->leafEntries[0].key, sizeof(int));
                    }
                    parent->sizeInPage += strLen + sizeof(int) + sizeof(short);
                    //write parent node to file
                    char *parentPage = (char *) calloc(PAGE_SIZE, 1);
                    generatePage(parent, parentPage);
                    ixFileHandle.appendPage(parentPage);
                    free(parentPage);
                    nodeMap[ixFileHandle.ixAppendPageCounter - 1] = parent;
                    parent->pageNum = ixFileHandle.ixAppendPageCounter - 1;
                    //set parent
                    dynamic_cast<LeafNode *>(targetNode)->parent = parent;
                    newNode->parent = parent;
                    //set meta fields
                    ixFileHandle.root = ixFileHandle.ixAppendPageCounter - 1;
                    root = parent;
                    return 0;
                } else {  // it has parent node
                    Node *parent = dynamic_cast<LeafNode *>(targetNode)->parent;
                    PageNum parentPageNum = dynamic_cast<LeafNode *>(targetNode)->parent->pageNum;
                    InternalEntry internal_pair(attrType, dynamic_cast<LeafNode *>(newNode)->leafEntries[0].key,
                                                targetNode, newNode);
                    //add entry to internal node
                    bool isInternalAdded = false;
                    int addedIndex = 0;
                    for (auto i = dynamic_cast<InternalNode *>(parent)->internalEntries.begin();
                         i != dynamic_cast<InternalNode *>(parent)->internalEntries.end(); i++) {
                        if (PeterDB::IndexManager::instance().compareKey(attrType, internal_pair.key, (*i).key) < 0) {
                            dynamic_cast<InternalNode *>(parent)->internalEntries.insert(i, internal_pair);
                            isInternalAdded = true;
                            addedIndex = i - dynamic_cast<InternalNode *>(parent)->internalEntries.begin();
                            break;
                        }
                    }
                    int strLen = 0;
                    if (attrType == TypeVarChar) {
                        memcpy(&strLen, (char *) dynamic_cast<LeafNode *>(newNode)->leafEntries[0].key, sizeof(int));
                    }
                    if (isInternalAdded == false) {
                        dynamic_cast<InternalNode *>(parent)->internalEntries.push_back(internal_pair);
                        dynamic_cast<InternalNode *>(parent)->internalEntries[
                                dynamic_cast<InternalNode *>(parent)->internalEntries.size() -
                                2].right = internal_pair.left;
                        dynamic_cast<InternalNode *>(parent)->sizeInPage +=
                                strLen + 2 * sizeof(int) + sizeof(short);//pointer+key+offset
                    } else if (addedIndex == 0) {
                        dynamic_cast<InternalNode *>(parent)->internalEntries[1].left = internal_pair.right;
                        dynamic_cast<InternalNode *>(parent)->sizeInPage += strLen + 2 * sizeof(int) + sizeof(short);
                    } else {
                        dynamic_cast<InternalNode *>(parent)->internalEntries[addedIndex -
                                                                              1].right = internal_pair.left;
                        dynamic_cast<InternalNode *>(parent)->internalEntries[addedIndex +
                                                                              1].left = internal_pair.right;
                        dynamic_cast<InternalNode *>(parent)->sizeInPage += strLen + 2 * sizeof(int) + sizeof(short);
                    }
                    if (dynamic_cast<InternalNode *>(parent)->sizeInPage <= PAGE_SIZE) {
                        char *updatedParentPage = (char *) calloc(PAGE_SIZE, 1);
                        generatePage(parent, updatedParentPage);
                        ixFileHandle.writePage(dynamic_cast<InternalNode *>(parent)->pageNum, updatedParentPage);
                        free(updatedParentPage);
                        return 0;
                    }
                    while (dynamic_cast<InternalNode *>(parent)->sizeInPage > PAGE_SIZE) {   //continuously split parent node
                        Node *newInternal = new InternalNode();
                        newInternal->isLoaded = true;
                        newInternal->sizeInPage += sizeof(int);//right pointer
                        //move half of entries and update size
                        for (int i = dynamic_cast<InternalNode *>(parent)->internalEntries.size() / 2 + 1;
                             i < dynamic_cast<InternalNode *>(parent)->internalEntries.size(); i++) {
                            InternalEntry internalEntry(attrType,
                                                        dynamic_cast<InternalNode *>(parent)->internalEntries[i].key,
                                                        dynamic_cast<InternalNode *>(parent)->internalEntries[i].left,
                                                        dynamic_cast<InternalNode *>(parent)->internalEntries[i].right);
                            dynamic_cast<InternalNode *>(newInternal)->internalEntries.push_back(internalEntry);
                            int strLen = 0;
                            if (attrType == TypeVarChar) {
                                memcpy(&strLen, (char *) dynamic_cast<InternalNode *>(parent)->internalEntries[i].key,
                                       4);
                            }//key, offset, left pointer
                            dynamic_cast<InternalNode *>(parent)->sizeInPage -=
                                    sizeof(int) + strLen + sizeof(short) + sizeof(int);
                            dynamic_cast<InternalNode *>(newInternal)->sizeInPage +=
                                    sizeof(int) + strLen + sizeof(short) + sizeof(int);
                        }
                        //delete half of parent node
                        for (auto i = dynamic_cast<InternalNode *>(parent)->internalEntries.begin() + mid;
                             i != dynamic_cast<InternalNode *>(parent)->internalEntries.end(); i++)
                            free(i->key);
                        dynamic_cast<InternalNode *>(parent)->internalEntries.erase(
                                dynamic_cast<InternalNode *>(parent)->internalEntries.begin() + mid,
                                dynamic_cast<InternalNode *>(parent)->internalEntries.end());
                        //write updated parent to file
                        char *updatedParentPage = (char *) calloc(PAGE_SIZE, 1);
                        generatePage(parent, updatedParentPage);
                        ixFileHandle.writePage(dynamic_cast<InternalNode *>(parent)->pageNum, updatedParentPage);
                        free(updatedParentPage);
                        //write new internal node to file
                        char *newInternalPage = (char *) calloc(PAGE_SIZE, 1);
                        generatePage(newInternal, newInternalPage);
                        ixFileHandle.appendPage(newInternalPage);
                        nodeMap[ixFileHandle.ixAppendPageCounter - 1] = newInternal;
                        newInternal->pageNum = ixFileHandle.ixAppendPageCounter - 1;
                        free(newInternalPage);
                        if (dynamic_cast<InternalNode *>(parent)->parent == NULL) {
                            InternalNode *rootParent = new InternalNode();
                            rootParent->isLoaded = true;
                            InternalEntry rootEntry(attrType,
                                                    dynamic_cast<InternalNode *>(newInternal)->internalEntries[0].key,
                                                    parent, newInternal);
                            rootParent->internalEntries.push_back(rootEntry);
                            int strLen = 0;
                            if (attrType == TypeVarChar) {
                                memcpy(&strLen,
                                       (char *) dynamic_cast<InternalNode *>(newInternal)->internalEntries[0].key,
                                       sizeof(int));
                            }
                            rootParent->sizeInPage += strLen + sizeof(int) + sizeof(short);
                            //write root node to file
                            char *rootPage = (char *) calloc(PAGE_SIZE, 1);
                            generatePage(rootParent, rootPage);
                            ixFileHandle.appendPage(rootPage);
                            free(rootPage);
                            nodeMap[ixFileHandle.ixAppendPageCounter - 1] = rootParent;
                            rootParent->pageNum = ixFileHandle.ixAppendPageCounter - 1;
                            //set parent
                            dynamic_cast<InternalNode *>(parent)->parent = rootParent;
                            newInternal->parent = rootParent;
                            //set meta fields
                            ixFileHandle.root = ixFileHandle.ixAppendPageCounter - 1;
                            root = rootParent;
                            return 0;
                        } else {  // it has parent
                            Node *newParent = dynamic_cast<InternalNode *>(parent)->parent;
                            PageNum newParentPageNum = dynamic_cast<InternalNode *>(parent)->parent->pageNum;
                            InternalEntry new_internal_pair(attrType,
                                                            dynamic_cast<InternalNode *>(newInternal)->internalEntries[0].key,
                                                            parent, newInternal);
                            //add entry to internal node
                            bool isInternalAdded = false;
                            int addedIndex = 0;
                            for (auto i = dynamic_cast<InternalNode *>(newParent)->internalEntries.begin();
                                 i != dynamic_cast<InternalNode *>(newParent)->internalEntries.end(); i++) {
                                if (PeterDB::IndexManager::instance().compareKey(attrType, internal_pair.key,
                                                                                 (*i).key) < 0) {
                                    dynamic_cast<InternalNode *>(newParent)->internalEntries.insert(i, internal_pair);
                                    isInternalAdded = true;
                                    addedIndex = i - dynamic_cast<InternalNode *>(newParent)->internalEntries.begin();
                                    break;
                                }
                            }
                            int strLen = 0;
                            if (attrType == TypeVarChar) {
                                memcpy(&strLen,
                                       (char *) dynamic_cast<InternalNode *>(newInternal)->internalEntries[0].key,
                                       sizeof(int));
                            }
                            if (isInternalAdded == false) {
                                dynamic_cast<InternalNode *>(newParent)->internalEntries.push_back(new_internal_pair);
                                dynamic_cast<InternalNode *>(newParent)->internalEntries[
                                        dynamic_cast<InternalNode *>(newParent)->internalEntries.size() -
                                        2].right = new_internal_pair.left;
                                dynamic_cast<InternalNode *>(newParent)->sizeInPage +=
                                        strLen + 2 * sizeof(int) + sizeof(short); //left pointer + key + offset
                            } else if (addedIndex == 0) {
                                dynamic_cast<InternalNode *>(newParent)->internalEntries[1].left = new_internal_pair.right;
                                dynamic_cast<InternalNode *>(newParent)->sizeInPage +=
                                        strLen + 2 * sizeof(int) + sizeof(short);
                            } else {
                                dynamic_cast<InternalNode *>(newParent)->internalEntries[addedIndex -
                                                                                         1].right = new_internal_pair.left;
                                dynamic_cast<InternalNode *>(newParent)->internalEntries[addedIndex +
                                                                                         1].left = new_internal_pair.right;
                                dynamic_cast<InternalNode *>(newParent)->sizeInPage +=
                                        strLen + 2 * sizeof(int) + sizeof(short);
                            }
                            parent = newParent;
                        }
                    }
                }
            }
        }
        return 0;
    }

    Node::Node() = default;

    Node::~Node() noexcept = default;

    BTree::BTree() {
        root = nullptr;
        minLeaf = nullptr;
        attrType = TypeNull;
    }

    BTree::~BTree() {};

    LeafNode::~LeafNode() {};

    InternalNode::~InternalNode() {
        for (auto i:internalEntries) {

        }
    }

    InternalNode::InternalNode() {
        type = INTERNAL;
        //sizeInPage consists of type(char), sizeInPage(short), parent(PageNum), slotCount(short)
        sizeInPage = sizeof(char) + sizeof(short) + sizeof(PageNum) + sizeof(short);
        parent = nullptr;
        pageNum = -1;
        isDirty = false;
        isLoaded = false;
    }

    LeafNode::LeafNode() {
        type = LEAF;
        //sizeInPage consists of type(char), sizeInPage(short), parent(PageNum), rightPointer(PageNum), overflowPointer(PageNum), slotCount(short)
        sizeInPage = sizeof(char) + sizeof(short) + 3 * sizeof(PageNum) + sizeof(short);
        parent = nullptr;
        pageNum = -1;
        isDirty = false;
        isLoaded = false;
        rightPointer = nullptr;
        overflowPointer = nullptr;
    }

    LeafEntry::LeafEntry() {
        key = nullptr;
        rid.pageNum = -1;
        rid.slotNum = -1;
        keySize = 0;
    }

    LeafEntry::LeafEntry(const AttrType &attrType, const void *key, const RID ridPassed) {
        if (attrType == TypeInt) {
            this->key = malloc(sizeof(int));
            memcpy(this->key, key, sizeof(int));
            keySize = sizeof(int);
        } else if (attrType == TypeReal) {
            this->key = malloc(sizeof(float));
            memcpy(this->key, key, sizeof(float));
            keySize = sizeof(float);
        } else if (attrType == TypeVarChar) {
            int strLen = *(int *) key;
            this->key = malloc(sizeof(int) + strLen);
            memcpy(this->key, key, sizeof(int) + strLen);
            keySize = sizeof(int) + strLen;
        }
        this->rid = ridPassed;
    }

    LeafEntry::~LeafEntry() = default;

    InternalEntry::InternalEntry() {
        key = nullptr;
        left = nullptr;
        right = nullptr;
    }

    InternalEntry::InternalEntry(const AttrType &attribute, const void *key, Node *left, Node *right) {
        if (attribute == TypeInt) {
            this->key = malloc(sizeof(int));
            keySize = sizeof(int);
            this->key = malloc(sizeof(int));
            memcpy(this->key, key, sizeof(int));
        } else if (attribute == TypeReal) {
            this->key = malloc(sizeof(float));
            keySize = sizeof(float);
            this->key = malloc(sizeof(float));
            memcpy(this->key, key, sizeof(float));
        } else if (attribute == TypeVarChar) {
            int strLen = *(int *) key;
            this->key = malloc(sizeof(int) + strLen);
            keySize = sizeof(int) + strLen;
            memcpy(this->key, key, sizeof(int) + strLen);
        }
        this->left = left;
        this->right = right;
    }

    InternalEntry::InternalEntry(const AttrType &attrType, const void *key) {
        if (attrType == TypeInt) {
            this->key = malloc(sizeof(int));
            keySize = sizeof(int);
            memcpy(this->key, key, sizeof(int));
        } else if (attrType == TypeReal) {
            this->key = malloc(sizeof(float));
            keySize = sizeof(float);
            memcpy(this->key, key, sizeof(float));
        } else if (attrType == TypeVarChar) {
            int strLen = *(int *) key;
            this->key = malloc(sizeof(int) + strLen);
            keySize = sizeof(int) + strLen;
            memcpy(this->key, key, sizeof(int) + strLen);
        }
        this->left = nullptr;
        this->right = nullptr;
    }

    InternalEntry::~InternalEntry() {}
} // namespace PeterDB