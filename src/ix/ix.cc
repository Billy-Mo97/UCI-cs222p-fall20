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
                int root = NULLNODE, minLeaf = NULLNODE;
                AttrType nullAttr = TypeNull;
                fseek(file, 0, SEEK_SET);
                fwrite(&readNum, sizeof(unsigned), 1, file);
                fwrite(&writeNum, sizeof(unsigned), 1, file);
                fwrite(&appendNum, sizeof(unsigned), 1, file);
                fwrite(&pageNum, sizeof(unsigned), 1, file);
                fwrite(&root, sizeof(int), 1, file);
                fwrite(&minLeaf, sizeof(int), 1, file);
                fwrite(&nullAttr, sizeof(AttrType), 1, file);
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
        if (pfm.openFile(fileName, ixFileHandle.fileHandle) == -1) { return -1; }
        if (ixFileHandle.readHiddenPage() == -1) return -1;
        if (ixFileHandle.root != NULLNODE) {
            char *rootPage = (char *) calloc(PAGE_SIZE, 1);
            if (ixFileHandle.readPage(ixFileHandle.root, rootPage) == -1) { return -1; }
            if (ixFileHandle.bTree->generateNode(rootPage, ixFileHandle.bTree->root) == -1) { return -1; }
            ixFileHandle.bTree->root->pageNum = ixFileHandle.root;
            free(rootPage);
            ixFileHandle.bTree->nodeMap[ixFileHandle.root] = ixFileHandle.bTree->root;
        }
        if (ixFileHandle.minLeaf == NULLNODE) ixFileHandle.minLeaf = ixFileHandle.root;
        else {
            char *minPage = (char *) calloc(PAGE_SIZE, 1);
            if (ixFileHandle.readPage(ixFileHandle.minLeaf, minPage) == -1) { return -1; }
            if (ixFileHandle.bTree->generateNode(minPage, ixFileHandle.bTree->minLeaf) == -1) { return -1; }
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
        if (ixFileHandle.bTree) {
            return ixFileHandle.bTree->insertEntry(ixFileHandle, leafEntry);
        } else {
            ixFileHandle.bTree = new BTree();
            ixFileHandle.bTree->attrType = attribute.type;
            if (ixFileHandle.root != NULLNODE) {
                char *rootPage = (char *) calloc(PAGE_SIZE, 1);
                if (ixFileHandle.readPage(ixFileHandle.root, rootPage) == -1) { return -1;}
                if (ixFileHandle.bTree->generateNode(rootPage, ixFileHandle.bTree->root) == -1) { return -1; }
                ixFileHandle.bTree->root->pageNum = ixFileHandle.root;
                free(rootPage);
                ixFileHandle.bTree->nodeMap[ixFileHandle.root] = ixFileHandle.bTree->root;
            }
            if (ixFileHandle.minLeaf == NULLNODE) { ixFileHandle.minLeaf = ixFileHandle.root; }
            else {
                char *minPage = (char *) calloc(PAGE_SIZE, 1);
                if (ixFileHandle.readPage(ixFileHandle.minLeaf, minPage) == -1) { return -1; }
                if (ixFileHandle.bTree->generateNode(minPage,  ixFileHandle.bTree->minLeaf) == -1) { return -1; }
                ixFileHandle.bTree->minLeaf->pageNum = ixFileHandle.minLeaf;
                free(minPage);
                ixFileHandle.bTree->nodeMap[ixFileHandle.minLeaf] = ixFileHandle.bTree->minLeaf;
            }
            return ixFileHandle.bTree->insertEntry(ixFileHandle, leafEntry);
        }
        return 0;
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
                if (printLeafKey(start, i, node, attribute, out) == -1) { return -1; }
            } else {
                //"start" marks the beginning of a new key, we iterate until we find a different key.
                //Then, we print out the key, and all rids having this key.
                if (compareKey(attribute.type, entry.key, dynamic_cast<LeafNode *>(node)->leafEntries[i].key) ==
                    0)
                    continue;
                else {
                    if (printLeafKey(start, i, node, attribute, out) == -1) { return -1; }
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
            if (leafDFS(ixFileHandle, node, attribute, out) == -1) { return -1; }
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
                if (DFS(ixFileHandle, dynamic_cast<InternalNode *>(node)->internalEntries[i].left, attribute, out) ==
                    -1) { return -1; }
                out << ",";
                //If it is the last entry on its node, do DFS search on its right child.
                if (i == dynamic_cast<InternalNode *>(node)->internalEntries.size() - 1) {
                    if (DFS(ixFileHandle, dynamic_cast<InternalNode *>(node)->internalEntries[i].right, attribute,
                            out) == -1) { return -1; }
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
    IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount,
                                       unsigned &appendPageCount) const {
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
            if (setInternalEntryKeyInPage(node, page, offset, i) == -1) { return -1; }
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
        if (generatePageHeader(node, page, offset) == -1) { return -1; }
        if (node->type == INTERNAL) {
            if (generateInternalPage(node, page, offset) == -1) {
                return -1;
            }
        } else if (node->type == LEAF) {
            if (generateLeafPage(node, page, offset) == -1) {
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
            if (getKeyLen(data, offset, keyLen) == -1) { return -1; }
            InternalEntry entry(attrType, data + slotOffset);
            if (getLeftInternalEntry(entry, data, slotOffset) == -1) { return -1; }
            if (getRightInternalEntry(entry, data, offset, keyLen) == -1) { return -1; }
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
            if (getKeyLen(data, offset, keyLen) == -1) { return -1; }
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

        if (getRightPointerInLeafNode(res, data, offset) == -1) { return -1; }

        if (getOverflowPointerInLeafNode(res, data, offset) == -1) { return -1; }

        if (getEntriesInLeafNode(res, data, offset, slotCount) == -1) { return -1; }

        return 0;
    }

    RC BTree::generateNode(char *data, Node *res) {
        //This is a helper function that loads all the information of a node from index file into memory.
        //These information is stored in node class in memory.
        //Node class can either be internalNode or leafNode.
        //data pointer is pre-loaded with the data of Node in index file.
        res = new Node();
        short offset, slotCount;
        if (generateNodeHeader(res, data, offset, slotCount) == -1) { return -1; }
        if (res->type == INTERNAL) {
            if (generateInternalNode(res, data, offset, slotCount) == -1) { return -1; }
        } else if (res->type == LEAF) {
            if (generateLeafNode(res, data, offset, slotCount) == -1) { return -1; }
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
            if (generateNode(pageData, node) == -1) { return -1; }
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
                if (idm.compareKey(attrType, dynamic_cast<InternalNode *>(node)->internalEntries[i].key, pair.key) <
                    0) {
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
            if (compareKeyInInternalNode(ixFileHandle, pair, node) == 1) {
                break;
            }
        }
        return 0;
    }

    RC BTree::generateRootNode(LeafNode *res, const LeafEntry &pair) {
        //This is a helper function that creates root node with one leaf entry.
        //These information is stored in node class in memory.
        res->leafEntries.push_back(pair);
        //This one leaf entry takes up keySize + RID + directory space (short) in page.
        res->sizeInPage += pair.keySize + sizeof(RID) + sizeof(short);
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
        if (generateRootNode(node, pair) == -1) { return -1; }
        //set tree
        root = node;
        minLeaf = node;
        //generate the page and write it into index file.
        char *new_page = (char *) calloc(PAGE_SIZE, 1);
        if (generatePage(node, new_page) == -1) { return -1; }
        if (ixFileHandle.appendPage(new_page) == -1) { return -1; }
        free(new_page);
        nodeMap[0] = node;
        node->pageNum = 0;
        //set meta fields
        ixFileHandle.root = 0;
        ixFileHandle.minLeaf = 0;
        return 0;
    }

    RC BTree::insertEntryInLeafNode(Node *targetNode, IXFileHandle &ixFileHandle, const LeafEntry &pair) {
        //This is a helper function that insert pair entry into B+ Tree's leaf node.

        //Find the inserted leaf node, load it into memory if it hasn't been loaded.
        targetNode = root;
        if (findLeafNode(ixFileHandle, pair, targetNode) == -1) { return -1; };
        if (dynamic_cast<LeafNode *>(targetNode)->isLoaded == false) {
            loadNode(ixFileHandle, targetNode);
        }

        //Insert the entry into targetNode
        bool isAdded = false;
        //If we find pair's key less than one entry's key, insert it before that entry.
        for (auto i = dynamic_cast<LeafNode *>(targetNode)->leafEntries.begin();
             i < dynamic_cast<LeafNode *>(targetNode)->leafEntries.end(); i++) {
            if (PeterDB::IndexManager::instance().compareKey(attrType, pair.key, (*i).key) < 0) {
                dynamic_cast<LeafNode *>(targetNode)->leafEntries.insert(i, pair);
                isAdded = true;
                break;
            }
        }
        //Otherwise, push it to the back of vector of entries.
        if (isAdded == false) {
            dynamic_cast<LeafNode *>(targetNode)->leafEntries.push_back(pair);
        }

        //This newly inserted entry takes up keySize + RID + directory space (short) in page.
        dynamic_cast<LeafNode *>(targetNode)->sizeInPage += pair.keySize + sizeof(RID) + sizeof(short);
        return 0;
    }

    RC BTree::writeLeafNodeToFile(IXFileHandle &ixFileHandle, Node *targetNode) {
        //write the updated target node into file
        char *newPage = (char *) calloc(PAGE_SIZE, 1);
        generatePage(targetNode, newPage);
        if (ixFileHandle.writePage(dynamic_cast<LeafNode *>(targetNode)->pageNum, newPage) == -1) { return -1; }
        free(newPage);
        return 0;
    }

    RC BTree::setMidToSplit(Node *targetNode, int &mid) {
        //This is helper function to set the mid point in the target leaf node.
        //The mid point is set to include all the entries around mid point with same key before or after mid point.
        PeterDB::IndexManager &idm = PeterDB::IndexManager::instance();
        int entriesNum = dynamic_cast<LeafNode *>(targetNode)->leafEntries.size();
        mid = entriesNum / 2;
        int end = 0;
        //Search towards end.
        for (int i = mid; i <= entriesNum; i++) {
            if (i == entriesNum) {
                end = 1;
                break;
            }
            LeafEntry curEntry = dynamic_cast<LeafNode *>(targetNode)->leafEntries[i],
                    prevEntry = dynamic_cast<LeafNode *>(targetNode)->leafEntries[i - 1];
            if (idm.compareKey(attrType, curEntry.key, prevEntry.key) == 0)
                continue;
            else {
                mid = i;
                break;
            }
        }
        if (end == 1) {
            //Search towards front.
            for (int i = mid; i >= 0; i--) {
                //All keys are the same, we can't find a reasonable point to split this node.
                if (i == 0) { return -1; }
                LeafEntry curEntry = dynamic_cast<LeafNode *>(targetNode)->leafEntries[i],
                        prevEntry = dynamic_cast<LeafNode *>(targetNode)->leafEntries[i - 1];
                if (idm.compareKey(attrType, curEntry.key, prevEntry.key) == 0)
                    continue;
                else {
                    mid = i;
                    break;
                }
            }
        }
        return 0;
    }

    RC BTree::createNewSplitLeafNode(LeafNode *newNode, LeafNode *targetNode, int &mid) {
        //Split node, push the entries into new node and update size
        for (int i = mid; i < targetNode->leafEntries.size(); i++) {
            LeafEntry leafEntry(attrType, targetNode->leafEntries[i].key, targetNode->leafEntries[i].rid);
            newNode->leafEntries.push_back(leafEntry);
            int entrySizeInPage = leafEntry.keySize + sizeof(RID) + sizeof(short);
            targetNode->sizeInPage -= entrySizeInPage;
            newNode->sizeInPage += entrySizeInPage;
        }

        //Delete the moved entries from old node.
        targetNode->leafEntries.erase(targetNode->leafEntries.begin() + mid, targetNode->leafEntries.end());

        //Update rightPointer of new node and old node.
        newNode->rightPointer = targetNode->rightPointer;
        targetNode->rightPointer = newNode;

        return 0;
    }

    RC BTree::splitLeafNode(IXFileHandle &ixFileHandle, LeafNode *targetNode, LeafNode *newNode) {
        //This is a helper function to split leaf node, write both old and new node to index file.
        newNode = new LeafNode();
        newNode->isLoaded = true;

        //There is only one entry in leaf node, we can't split one single entry.
        if (targetNode->leafEntries.size() <= 1) { return -1; }

        //Find the correct mid point for leaf node to split.
        int mid;
        if (setMidToSplit(targetNode, mid) == 0) { return -1; }

        if (createNewSplitLeafNode(newNode, targetNode, mid) == 0) { return -1; }

        //Write updated targetNode to file
        char *updatedTargetPage = (char *) calloc(PAGE_SIZE, 1);
        generatePage(targetNode, updatedTargetPage);
        ixFileHandle.writePage(targetNode->pageNum, updatedTargetPage);
        free(updatedTargetPage);

        //Write new leaf node to file
        char *newLeafPage = (char *) calloc(PAGE_SIZE, 1);
        if (generatePage(newNode, newLeafPage) == -1) { return -1; };
        if (ixFileHandle.appendPage(newLeafPage) == -1) { return -1; };
        PageNum newPageNum = nodeMap.size();
        nodeMap[newPageNum] = newNode;
        newNode->pageNum = newPageNum;
        free(newLeafPage);

        return 0;
    }

    RC BTree::createParentForSplitLeafNode(IXFileHandle &ixFileHandle, Node *targetNode, LeafNode *newNode) {
        //This is a helper function that creates a parent node for split leaf node.
        //It insert the first entry of new split node into the parent node, set the parameters of the parent node,
        //generate its page and writes it into index file.
        InternalNode *parent = new InternalNode();
        parent->isLoaded = true;
        InternalEntry internal_pair(attrType, newNode->leafEntries[0].key, targetNode, newNode);
        parent->internalEntries.push_back(internal_pair);
        parent->type = INTERNAL;
        int entrySizeInPage = newNode->leafEntries[0].keySize + sizeof(RID) + sizeof(short);
        parent->sizeInPage += entrySizeInPage;

        //Write parent node to file
        char *parentPage = (char *) calloc(PAGE_SIZE, 1);
        if (generatePage(parent, parentPage) == -1) { return -1; }
        if (ixFileHandle.appendPage(parentPage) == -1) { return -1; };
        free(parentPage);

        //set parent node in B+ Tree.
        PageNum parentPageNum = nodeMap.size();
        nodeMap[parentPageNum] = parent;
        parent->pageNum = parentPageNum;
        dynamic_cast<LeafNode *>(targetNode)->parent = parent;
        newNode->parent = parent;
        //set meta fields
        ixFileHandle.root = parentPageNum;
        root = parent;

        return 0;
    }

    RC BTree::updateInternalNode(InternalNode *updateNode, InternalEntry &insertEntry) {
        //This is a helper function to update internal node in B+ Tree by inserting new entry into this node.
        PeterDB::IndexManager &idm = PeterDB::IndexManager::instance();
        bool isAdded = false;
        int addedIndex = 0;
        for (auto i = updateNode->internalEntries.begin();
             i != updateNode->internalEntries.end(); i++) {
            if (idm.compareKey(attrType, insertEntry.key, (*i).key) < 0) {
                updateNode->internalEntries.insert(i, insertEntry);
                isAdded = true;
                addedIndex = i - updateNode->internalEntries.begin();
                break;
            }
        }

        //If the entry is to be inserted to the back of vector.
        if (isAdded == false) {
            updateNode->internalEntries.push_back(insertEntry);
            updateNode->internalEntries[updateNode->internalEntries.size() - 2].right = insertEntry.left;
            //This newly inserted entry takes up keySize + left pointer + directory space (short) in page.
            int entrySizeInPage = insertEntry.keySize + sizeof(PageNum) + sizeof(short);
            updateNode->sizeInPage += entrySizeInPage;
        } else if (addedIndex == 0) { //Or the entry is to be inserted to the beginning of vector.
            updateNode->internalEntries[1].left = insertEntry.right;
            //This newly inserted entry takes up keySize + left pointer + directory space (short) in page.
            int entrySizeInPage = insertEntry.keySize + sizeof(PageNum) + sizeof(short);
            updateNode->sizeInPage += entrySizeInPage;
        } else {//Or it is inserted at middle of the vector
            updateNode->internalEntries[addedIndex - 1].right = insertEntry.left;
            updateNode->internalEntries[addedIndex + 1].left = insertEntry.right;
            //This newly inserted entry takes up keySize + left pointer + directory space (short) in page.
            int entrySizeInPage = insertEntry.keySize + sizeof(PageNum) + sizeof(short);
            updateNode->sizeInPage += entrySizeInPage;
        }

        return 0;
    }

    RC
    BTree::updateParentOfLeaf(IXFileHandle &ixFileHandle, Node *targetNode, LeafNode *newNode, InternalNode *parent) {
        parent = dynamic_cast<InternalNode *>(dynamic_cast<LeafNode *>(targetNode)->parent);
        PageNum parentPageNum = targetNode->parent->pageNum;
        InternalEntry insertEntry(attrType, newNode->leafEntries[0].key, targetNode, newNode);

        if (updateInternalNode(parent, insertEntry) == -1) { return -1; }

        return 0;
    }

    RC BTree::writeParentNodeToFile(IXFileHandle &ixFileHandle, InternalNode *parent) {
        char *updatedParentPage = (char *) calloc(PAGE_SIZE, 1);
        generatePage(parent, updatedParentPage);
        if (ixFileHandle.writePage(dynamic_cast<InternalNode *>(parent)->pageNum, updatedParentPage) ==
            -1) { return -1; }
        free(updatedParentPage);
        return 0;
    }

    RC BTree::splitInternalNode(IXFileHandle &ixFileHandle, InternalNode *targetNode, InternalNode *newInternalNode) {
        //This is a helper function to split internal node, write both old and new node to index file.
        newInternalNode = new InternalNode();
        newInternalNode->isLoaded = true;
        newInternalNode->sizeInPage += sizeof(PageNum); //Size for right pointer
        //Move half of entries in old node and update size
        int internalEntryNum = targetNode->internalEntries.size();
        for (int i = internalEntryNum / 2 + 1; i < internalEntryNum; i++) {
            InternalEntry moveEntry = targetNode->internalEntries[i];
            InternalEntry internalEntry(attrType, moveEntry.key, moveEntry.left, moveEntry.right);
            newInternalNode->internalEntries.push_back(internalEntry);
            //This moved entry takes up keySize + left pointer + directory space (short) in page.
            int entrySizeInPage = internalEntry.keySize + sizeof(PageNum) + sizeof(short);
            targetNode->sizeInPage -= entrySizeInPage;
            newInternalNode->sizeInPage += entrySizeInPage;
        }

        //Delete the moved entries old node
        targetNode->internalEntries.erase(targetNode->internalEntries.begin() + internalEntryNum / 2 + 1,
                                          targetNode->internalEntries.end());

        //write updated parent to file
        char *updatedParentPage = (char *) calloc(PAGE_SIZE, 1);
        if (generatePage(targetNode, updatedParentPage) == -1) { return -1; }
        if (ixFileHandle.writePage(targetNode->pageNum, updatedParentPage) == -1) { return -1; }
        free(updatedParentPage);

        //write new internal node to file
        char *newInternalPage = (char *) calloc(PAGE_SIZE, 1);
        if (generatePage(newInternalNode, newInternalPage) == -1) { return -1; }
        if (ixFileHandle.appendPage(newInternalPage) == -1) { return -1; }
        PageNum newPageNum = nodeMap.size();
        nodeMap[newPageNum] = newInternalNode;
        newInternalNode->pageNum = newPageNum;
        free(newInternalPage);

        return 0;
    }

    RC BTree::createParentForSplitInternalNode(IXFileHandle &ixFileHandle, InternalNode *targetNode,
                                               InternalNode *newNode) {
        InternalNode *rootParent = new InternalNode();
        rootParent->isLoaded = true;
        rootParent->sizeInPage += sizeof(PageNum); //Size for right pointer.

        InternalEntry rootEntry(attrType, newNode->internalEntries[0].key, targetNode, newNode);
        rootParent->internalEntries.push_back(rootEntry);
        //This newly inserted entry takes up keySize + left pointer + directory space (short) in page.
        int entrySizeInPage = newNode->internalEntries[0].keySize + sizeof(PageNum) + sizeof(short);
        rootParent->sizeInPage += entrySizeInPage;

        //write root node to file
        char *rootPage = (char *) calloc(PAGE_SIZE, 1);
        if (generatePage(rootParent, rootPage) == -1) { return -1; }
        if (ixFileHandle.appendPage(rootPage) == -1) { return -1; }
        free(rootPage);

        PageNum newPageNum = nodeMap.size();
        nodeMap[newPageNum] = rootParent;
        rootParent->pageNum = newPageNum;
        //set parent
        targetNode->parent = rootParent;
        newNode->parent = rootParent;
        //set meta fields
        ixFileHandle.root = newPageNum;
        root = rootParent;

        return 0;
    }

    RC BTree::updateParentOfInternal(IXFileHandle &ixFileHandle, Node *targetNode, InternalNode *newNode,
                                     InternalNode *parent) {
        parent = dynamic_cast<InternalNode *>(dynamic_cast<InternalNode *>(targetNode)->parent);
        PageNum parentPageNum = targetNode->parent->pageNum;
        InternalEntry insertEntry(attrType, newNode->internalEntries[0].key, targetNode, newNode);

        if (updateInternalNode(parent, insertEntry) == -1) { return -1; }

        return 0;
    }

    RC BTree::insertEntry(IXFileHandle &ixFileHandle, const LeafEntry &pair) {
        //This is a helper function to insert pair entry into B+ Tree.

        //If current B+ Tree is empty, initiate it with pair entry.
        if (root == nullptr) {
            return initiateNullTree(ixFileHandle, pair);
        } else {
            Node *targetNode;
            if (insertEntryInLeafNode(targetNode, ixFileHandle, pair) == -1) { return -1; }
            //If the inserted leaf node does not exceed space of one page,
            //generate a new page with this node and insert it into index file.
            if (dynamic_cast<LeafNode *>(targetNode)->sizeInPage <= PAGE_SIZE) {
                return writeLeafNodeToFile(ixFileHandle, targetNode);
            } else { //Otherwise, split the leaf node, generate a new page with split node and write it into index file.
                LeafNode *newNode;
                if (splitLeafNode(ixFileHandle, dynamic_cast<LeafNode *>(targetNode), newNode) == -1) { return -1; }

                //If there is no parent for old node, create one and return.
                if (dynamic_cast<LeafNode *>(targetNode)->parent == nullptr) {
                    return createParentForSplitLeafNode(ixFileHandle, targetNode, newNode);
                } else {  // If it has a parent node.
                    InternalNode *parent;
                    if (updateParentOfLeaf(ixFileHandle, targetNode, newNode, parent) == -1) { return -1; }
                    if (dynamic_cast<InternalNode *>(parent)->sizeInPage <= PAGE_SIZE) {
                        return writeParentNodeToFile(ixFileHandle, parent);
                    }
                    while (dynamic_cast<InternalNode *>(parent)->sizeInPage >
                           PAGE_SIZE) {   //continuously split parent node
                        InternalNode *newInternalNode;
                        if (splitInternalNode(ixFileHandle, parent, newInternalNode) == -1) { return -1; }
                        //If parent node does not have any parent, which means it is root node,
                        //create a new root
                        if (dynamic_cast<InternalNode *>(parent)->parent == nullptr) {
                            return createParentForSplitInternalNode(ixFileHandle, parent, newInternalNode);
                        } else {  // it has parent
                            InternalNode *newParent;
                            if (updateParentOfInternal(ixFileHandle, parent, newInternalNode, newParent) ==
                                -1) { return -1; }
                            parent = newParent;
                        }
                    }
                    return writeParentNodeToFile(ixFileHandle, parent);
                }
            }
        }
        return 0;
    }

    Node::Node() {
        type = TypeNull;
        //sizeInPage consists of type(char), sizeInPage(short), parent(PageNum).
        sizeInPage = sizeof(char) + sizeof(short) + sizeof(PageNum);
        parent = nullptr;
        pageNum = -1;
        isDirty = false;
        isLoaded = false;
    }

    Node::~Node() {
        type = TypeNull;
        //sizeInPage consists of type(char), sizeInPage(short), parent(PageNum).
        sizeInPage = sizeof(char) + sizeof(short) + sizeof(PageNum);
        parent = nullptr;
        pageNum = -1;
        isDirty = false;
        isLoaded = false;
    }

    BTree::BTree() {
        root = nullptr;
        minLeaf = nullptr;
        attrType = TypeNull;
    }

    BTree::~BTree() {
        root = nullptr;
        minLeaf = nullptr;
        attrType = TypeNull;
        nodeMap.clear();
    };

    LeafNode::~LeafNode() {
        leafEntries.clear();
        //Set sizeInPage back to initial value
        sizeInPage = sizeof(char) + sizeof(short) + 3 * sizeof(PageNum) + sizeof(short);
        parent = nullptr;
        pageNum = -1;
        isDirty = false;
        isLoaded = false;
        rightPointer = nullptr;
        overflowPointer = nullptr;
    }

    InternalNode::~InternalNode() {
        internalEntries.clear();
        //Set sizeInPage back to initial value
        sizeInPage = sizeof(char) + sizeof(short) + sizeof(PageNum) + sizeof(short);
        parent = nullptr;
        pageNum = -1;
        isDirty = false;
        isLoaded = false;
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

    LeafEntry::~LeafEntry() {
        free(key);
        key = nullptr;
        rid.pageNum = -1;
        rid.slotNum = -1;
        keySize = 0;
    };

    InternalEntry::InternalEntry() {
        key = nullptr;
        left = nullptr;
        right = nullptr;
        keySize = 0;
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
            this->key = malloc(sizeof(int));
            memcpy(this->key, key, sizeof(int));
        } else if (attrType == TypeReal) {
            this->key = malloc(sizeof(float));
            keySize = sizeof(float);
            this->key = malloc(sizeof(float));
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

    InternalEntry::~InternalEntry() {
        free(key);
        key = nullptr;
        left = nullptr;
        right = nullptr;
        keySize = 0;
    }
} // namespace PeterDB