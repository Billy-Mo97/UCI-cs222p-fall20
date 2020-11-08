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
        FILE *file;
        struct stat buffer;
        if ((stat(fileName.c_str(), &buffer) == 0)) {
            //printf("already exist\n");
            return -1;
        } else {
            file = fopen(fileName.c_str(), "w+b");//original: w+b
            if (file == NULL) {
                printf("null case\n");
                return -1;
            } else {
                //If the file has been successfully created,
                //initiate readPageCount, writePageCount, appendPageCount, numOfPages to 0,
                //write them to the hidden page of the file (first page).
                unsigned readNum = 0, writeNum = 0, appendNum = 0, pageNum = 0;
                int root = -1, minLeaf = -1;
                fseek(file, 0, SEEK_SET);
                fwrite(&readNum, sizeof(unsigned), 1, file);
                fwrite(&writeNum, sizeof(unsigned), 1, file);
                fwrite(&appendNum, sizeof(unsigned), 1, file);
                fwrite(&pageNum, sizeof(unsigned), 1, file);
                fwrite(&root, sizeof(int), 1, file);
                fwrite(&minLeaf, sizeof(int), 1, file);
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
        if (!fileHandle.pointer) return -1;
        fseek(fileHandle.pointer, 0, SEEK_SET);
        char* data = (char*)malloc(PAGE_SIZE);
        size_t readSize = fread(data, 1, PAGE_SIZE, fileHandle.pointer);
        if (readSize != PAGE_SIZE){
            free(data);
            return -1;
        }
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
        free(data);
        return 0;
    }

    RC IXFileHandle::writeHiddenPage() {
        if (!fileHandle.pointer) return -1;
        fseek(fileHandle.pointer, 0, SEEK_SET);
        char* data = (char*)calloc(PAGE_SIZE, 1);
        short offset = 0;
        memcpy(data + offset, &ixReadPageCounter, sizeof(unsigned));
        offset += sizeof(unsigned);
        memcpy(data + offset, &ixWritePageCounter, sizeof(unsigned));
        offset += sizeof(unsigned);
        memcpy(data + offset, &ixAppendPageCounter, sizeof(unsigned));
        offset += sizeof(unsigned);
        memcpy(data + offset, &numOfPages, sizeof(int));
        offset += sizeof(unsigned );
        memcpy(data + offset, &root, sizeof(int));
        offset += sizeof(int);
        memcpy(data + offset, &minLeaf, sizeof(int));
        offset += sizeof(int);
        size_t writeSize = fwrite(data, 1, PAGE_SIZE, fileHandle.pointer);
        if (writeSize != PAGE_SIZE){
            free(data);
            return -1;
        }
        fflush(fileHandle.pointer);
        free(data);
        return 0;
    }

    RC IndexManager::openFile(const std::string &fileName, IXFileHandle &ixFileHandle) {
        PeterDB::PagedFileManager &pfm = PeterDB::PagedFileManager::instance();
        if (pfm.openFile(fileName, ixFileHandle.fileHandle) == -1) return -1;
        if (ixFileHandle.readHiddenPage() == -1) return -1;
        return 0;
    }

    IXFileHandle::IXFileHandle(){
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
        if (ixFileHandle.writeHiddenPage() == -1) return -1;
        if (PagedFileManager::instance().closeFile(ixFileHandle.fileHandle) == -1) return -1;
        if (ixFileHandle.bTree) delete ixFileHandle.bTree;
        ixFileHandle.bTree = NULL;
        return 0;
    }

    RC IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        LeafEntry leafEntry(attribute.type, key, rid);
        if(ixFileHandle.bTree){
            return ixFileHandle.bTree->insertEntry(ixFileHandle, leafEntry);
        }else{
            ixFileHandle.bTree = new BTree();
            ixFileHandle.bTree->attrType = attribute.type;
            if(ixFileHandle.root != NULLNODE){
                char* rootPage = (char*)calloc(PAGE_SIZE, 1);
                ixFileHandle.readPage(ixFileHandle.root, rootPage);
                ixFileHandle.bTree->root = ixFileHandle.bTree->generateNode(rootPage);
                ixFileHandle.bTree->root->pageNum = ixFileHandle.root;
                free(rootPage);
                ixFileHandle.bTree->nodeMap[ixFileHandle.root] = ixFileHandle.bTree->root;
            }
            if(ixFileHandle.minLeaf == NULLNODE) ixFileHandle.minLeaf = ixFileHandle.root;
            else{
                char* minPage = (char*)calloc(PAGE_SIZE, 1);
                ixFileHandle.readPage(ixFileHandle.minLeaf, minPage);
                ixFileHandle.bTree->minLeaf = ixFileHandle.bTree->generateNode(minPage);
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

    RC IndexManager::BFS(IXFileHandle &ixfileHandle, Node* node, const Attribute& attribute,std::ostream &out) const {
        if (node->isLoaded == false) {
            ixfileHandle.bTree->loadNode(ixfileHandle, node);
        }
        out << "{";
        out << "\"keys\":";
        out << "[";
        if (node->type == LEAF) {
            int start = 0;
            LeafEntry entry = dynamic_cast<LeafNode*>(node)->leafEntries[0];
            for (int i = 0; i <= dynamic_cast<LeafNode*>(node)->leafEntries.size(); i++) {
                if (i == dynamic_cast<LeafNode*>(node)->leafEntries.size()) {
                    out << '"';
                    if (attribute.type == TypeInt) {
                        int key;
                        memcpy(&key, (char *)(dynamic_cast<LeafNode*>(node))->leafEntries[start].key, sizeof(int));
                        out << key << ":[";
                    }else if(attribute.type == TypeReal){
                        float key;
                        memcpy(&key, (char *)(dynamic_cast<LeafNode*>(node))->leafEntries[start].key, sizeof(float));
                        out << key << ":[";
                    }else if (attribute.type == TypeVarChar) {
                        int len;
                        memcpy(&len, (dynamic_cast<LeafNode*>(node))->leafEntries[start].key, sizeof(int));
                        std::string str((char *)(dynamic_cast<LeafNode*>(node))->leafEntries[start].key + sizeof(int), len);
                        out << str << ":[";
                    }
                    for (int j = start; j < i; j++) {
                        out << "(" << dynamic_cast<LeafNode*>(node)->leafEntries[j].rid.pageNum << "," << dynamic_cast<LeafNode*>(node)->leafEntries[j].rid.slotNum << ")";
                        if(j != i-1) out << ",";
                    }
                    out << "]";
                    out << '"';
                }
                else {
                    if (compareKey(attribute.type, entry.key, dynamic_cast<LeafNode*>(node)->leafEntries[i].key) == 0) continue;
                    else {
                        out << '"';
                        if (attribute.type == TypeInt) {
                            int key;
                            memcpy(&key, (char *)(dynamic_cast<LeafNode*>(node))->leafEntries[start].key, sizeof(int));
                            out << key << ":[";
                        }else if(attribute.type == TypeReal){
                            float key;
                            memcpy(&key, (char *)(dynamic_cast<LeafNode*>(node))->leafEntries[start].key, sizeof(float));
                            out << key << ":[";
                        }else if (attribute.type == TypeVarChar) {
                            int len;
                            memcpy(&len, (dynamic_cast<LeafNode*>(node))->leafEntries[start].key, sizeof(int));
                            std::string str((char *)(dynamic_cast<LeafNode*>(node))->leafEntries[start].key + sizeof(int), len);
                            out << str << ":[";
                        }
                        for (int j = start; j < i; j++) {
                            out << "(" << dynamic_cast<LeafNode*>(node)->leafEntries[j].rid.pageNum << "," << dynamic_cast<LeafNode*>(node)->leafEntries[j].rid.slotNum << ")";
                            if(j != i-1) out << ",";
                        }
                        out << "]";
                        out << '"';
                        start = i;
                        entry = dynamic_cast<LeafNode*>(node)->leafEntries[i];
                        if (i == dynamic_cast<LeafNode*>(node)->leafEntries.size()) out << ",";
                    }
                }
            }
            out << "]";
        }
        else {
            //print internal node
            for (int i = 0; i < dynamic_cast<InternalNode*>(node)->internalEntries.size(); i++) {
                out << '"';
                if (attribute.type == TypeInt) {
                    int key;
                    memcpy(&key, (char *)(dynamic_cast<LeafNode*>(node))->leafEntries[i].key, sizeof(int));
                    out << key << '"';
                }else if(attribute.type == TypeReal){
                    float key;
                    memcpy(&key, (char *)(dynamic_cast<LeafNode*>(node))->leafEntries[i].key, sizeof(float));
                    out << key << '"';
                }else if (attribute.type == TypeVarChar) {
                    int len;
                    memcpy(&len, (dynamic_cast<LeafNode*>(node))->leafEntries[i].key, sizeof(int));
                    std::string str((char *)(dynamic_cast<LeafNode*>(node))->leafEntries[i].key + sizeof(int), len);
                    out << str << '"';
                }
                if (i != dynamic_cast<InternalNode*>(node)->internalEntries.size() - 1) out << ",";
            }
            out << "]";
            //print children
            out << ",";
            out << "\"children\":[";
            for (int i = 0; i < dynamic_cast<InternalNode*>(node)->internalEntries.size(); i++) {
                BFS(ixfileHandle, dynamic_cast<InternalNode*>(node)->internalEntries[i].left, attribute, out);
                out << ",";
                if (i == dynamic_cast<InternalNode*>(node)->internalEntries.size() - 1) {
                    BFS(ixfileHandle, dynamic_cast<InternalNode*>(node)->internalEntries[i].right, attribute, out);
                }
            }
            out << "]";
        }
        out << "}";
        return 0;
    }
    RC IndexManager::printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const {
        return BFS(ixFileHandle, ixFileHandle.bTree->root, attribute, out);
    }

    IX_ScanIterator::IX_ScanIterator() {
    }

    IX_ScanIterator::~IX_ScanIterator() {
    }
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
    IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        readPageCount = ixReadPageCounter;
        writePageCount = ixWritePageCounter;
        appendPageCount = ixAppendPageCounter;
        return 0;
    }
    RC BTree::generatePage(Node *node, char *data) {
        if(node == NULL) return -1;
        char* page = (char*)calloc(PAGE_SIZE, 1);
        short offset = 0;
        memcpy(page + offset, &node->type, sizeof(char));//internal node or leaf node
        offset += sizeof(char);
        memcpy(page + offset, &node->size, sizeof(short));
        offset += sizeof(short);
        int tmp;
        if (node->parent){
            tmp = node->parent->pageNum;
        }else{
            tmp = NULLNODE;
        }
        memcpy(page + offset, &tmp, sizeof(PageNum));
        offset += sizeof(PageNum);
        if (node->type == INTERNAL){
            short slotCount = dynamic_cast<InternalNode*>(node)->internalEntries.size();
            memcpy(page + PAGE_SIZE - sizeof(short), &slotCount, sizeof(short));
            for (int i = 0; i < slotCount; i++){
                //If the entry is not the last, only write its left 
                int leftPage = dynamic_cast<InternalNode*>(node)->internalEntries[i].left->pageNum;
                memcpy(page + offset, &leftPage, sizeof(PageNum));
                offset += sizeof(PageNum);
                //set entry offset
                memcpy(page + PAGE_SIZE - (i + 2) * sizeof(short), &offset, sizeof(short));
                //set key
                if (attrType == TypeInt){
                    memcpy(page + offset, dynamic_cast<InternalNode*>(node)->internalEntries[i].key, sizeof(int));
                    offset += sizeof(int);
                }else if (attrType == TypeReal){
                    memcpy(page + offset, dynamic_cast<InternalNode*>(node)->internalEntries[i].key, sizeof(float));
                    offset += sizeof(float);
                }else if (attrType == TypeVarChar){
                    int strLen; 
                    memcpy(&strLen, dynamic_cast<InternalNode*>(node)->internalEntries[i].key, sizeof(int));
                    memcpy(page + offset, dynamic_cast<InternalNode*>(node)->internalEntries[i].key, sizeof(int) + strLen);
                    offset += sizeof(int) + strLen;
                }
                //If entry is the last, write its right  
                if (i == slotCount - 1){
                    int rightPage = dynamic_cast<InternalNode*>(node)->internalEntries[i].right->pageNum;
                    memcpy(page + offset, &rightPage, sizeof(PageNum));
                    offset += sizeof(PageNum);
                }
            }
        }
        else if (node->type == LEAF){
            //set right pointer
            int rightPage;
            if (dynamic_cast<LeafNode*>(node)->rightPointer){
                rightPage = dynamic_cast<LeafNode*>(node)->rightPointer->pageNum;
            }else{
                rightPage = NULLNODE;
            }
            memcpy(page + offset, &rightPage, sizeof(PageNum));
            offset += sizeof(PageNum);
            //set overflow pointer
            int overflowPage;
            if (dynamic_cast<LeafNode*>(node)->overflowPointer){
                 overflowPage= dynamic_cast<LeafNode*>(node)->overflowPointer->pageNum;
            }else{
                overflowPage = NULLNODE;
            }
            memcpy(page + offset, &overflowPage, sizeof(PageNum));
            offset += sizeof(PageNum);
            //set slotCount
            short slotCount = dynamic_cast<LeafNode*>(node)->leafEntries.size();
            memcpy(page + PAGE_SIZE - sizeof(short), &slotCount, sizeof(short));
            for (int i = 0; i < slotCount; i++){
                //set entry offset
                memcpy(page + PAGE_SIZE - (i + 2) * sizeof(short), &offset, sizeof(short));
                //set entry key
                if (attrType == TypeInt){
                    memcpy(page + offset, dynamic_cast<LeafNode*>(node)->leafEntries[i].key, sizeof(int));
                    offset += sizeof(int);
                }else if (attrType == TypeReal){
                    memcpy(page + offset, dynamic_cast<LeafNode*>(node)->leafEntries[i].key, sizeof(float));
                    offset += sizeof(float);
                }else if (attrType == TypeVarChar)
                {
                    int strLen;
                    memcpy(&strLen, dynamic_cast<LeafNode*>(node)->leafEntries[i].key, sizeof(int));
                    memcpy(page + offset, dynamic_cast<LeafNode*>(node)->leafEntries[i].key, sizeof(int) + strLen);
                    offset += sizeof(int) + strLen;
                }
                //set rid
                memcpy(page + offset, &dynamic_cast<LeafNode*>(node)->leafEntries[i].rid.pageNum, sizeof(PageNum));
                offset += sizeof(PageNum);
                memcpy(page + offset, &dynamic_cast<LeafNode*>(node)->leafEntries[i].rid.slotNum, sizeof(short));
                offset += sizeof(short);
            }
        }
        memcpy(data, page, PAGE_SIZE);
        free(page);
        return 0;
    }
    Node* BTree::generateNode(char* data)
    {
        Node* res;
        char type;
        short offset = 0;
        memcpy(&type, data + offset, sizeof(char));
        offset += sizeof(char);
        if (type == INTERNAL){
            res = new InternalNode();
            res->isLoaded = true;
            short size;
            memcpy(&size, data + offset, sizeof(short));
            offset += sizeof(short);
            res->size = size;
            int parent;
            memcpy(&parent, data + offset, sizeof(int));
            offset += sizeof(int);
            if (parent != NULLNODE) res->parent = nodeMap[parent];
            else res->parent = NULL;
            short slotCount;
            memcpy(&slotCount, data + PAGE_SIZE - sizeof(short), sizeof(short));
            for (int i = 0; i < slotCount; i++){
                short slotOffset;
                memcpy(&slotOffset, data + PAGE_SIZE - (i + 2) * sizeof(short), sizeof(short));
                InternalEntry entry(attrType, data + slotOffset);
                short keyLen = 0;
                if (attrType == TypeInt){
                    keyLen = sizeof(int);
                }else if (attrType == TypeReal){
                    keyLen = sizeof(float);
                }else if (attrType == TypeVarChar){
                    int strLen;
                    memcpy(&strLen, data + offset, sizeof(int));
                    keyLen = sizeof(int) + strLen;
                }
                int leftPageNum = *(int*)(data + slotOffset - sizeof(int));
                if (nodeMap.find(leftPageNum) != nodeMap.end()){
                    entry.left = nodeMap[leftPageNum];
                }else{
                    Node* left = new Node();
                    entry.left = left;
                    entry.left->pageNum = leftPageNum;
                }
                int rightPageNum;
                memcpy(&rightPageNum, data + offset + keyLen, sizeof(int));
                if (nodeMap.find(rightPageNum) != nodeMap.end()){
                    entry.right = nodeMap[rightPageNum];
                }else{
                    Node* right = new Node();
                    entry.right = right;
                    entry.right->pageNum = rightPageNum;
                }
                dynamic_cast<InternalNode*>(res)->internalEntries.push_back(entry);
            }
        }
        else if (type == LEAF){
            res = new LeafNode();
            res->isLoaded = true;
            short size;
            memcpy(&size, data + offset, sizeof(short));
            offset += sizeof(short);
            res->size = size;
            int parent;
            memcpy(&parent, data + offset, sizeof(int));
            offset += sizeof(int);
            if (parent != NULLNODE) res->parent = nodeMap[parent];
            else res->parent = NULL;
            PageNum rightPageNum;
            memcpy(&rightPageNum, data + offset, sizeof(PageNum));
            offset += sizeof(PageNum);
            if (rightPageNum != NULLNODE){
                if (nodeMap.find(rightPageNum) != nodeMap.end()){
                    dynamic_cast<LeafNode*>(res)->rightPointer = nodeMap[rightPageNum];
                }else{
                    Node*right = new Node();
                    dynamic_cast<LeafNode*>(res)->rightPointer = right;
                    dynamic_cast<LeafNode*>(res)->rightPointer->pageNum = rightPageNum;
                }
            }else dynamic_cast<LeafNode*>(res)->rightPointer = NULL;
            PageNum overflowPageNum;
            memcpy(&overflowPageNum, data + offset, sizeof(PageNum));
            offset += sizeof(PageNum);
            if (overflowPageNum != NULLNODE){
                if (nodeMap.find(overflowPageNum) != nodeMap.end()){
                    dynamic_cast<LeafNode*>(res)->overflowPointer = nodeMap[overflowPageNum];
                }else{
                    Node*overflow = new Node();
                    dynamic_cast<LeafNode*>(res)->overflowPointer = overflow;
                    dynamic_cast<LeafNode*>(res)->overflowPointer->pageNum = overflowPageNum;
                }
            }else dynamic_cast<LeafNode*>(res)->rightPointer = NULL;

            short slotCount;
            memcpy(&slotCount, data + PAGE_SIZE - sizeof(short), sizeof(short));
            for (int i = 0; i < slotCount; i++){
                short slotOffset;
                memcpy(&slotOffset, data + PAGE_SIZE - (i + 2) * sizeof(short), sizeof(short));
                LeafEntry entry;
                short keyLen = 0;
                if (attrType == TypeInt){
                    keyLen = sizeof(int);
                }else if (attrType == TypeReal){
                    keyLen = sizeof(float);
                }else if (attrType == TypeVarChar){
                    int strLength;
                    memcpy(&strLength, data + slotOffset, sizeof(int));
                    keyLen = sizeof(int) + strLength;
                }
                entry.key = malloc(keyLen);
                memcpy(entry.key, data + slotOffset, keyLen);
                memcpy(&entry.rid.pageNum, data + slotOffset + keyLen, sizeof(PageNum));
                memcpy(&entry.rid.slotNum, data + slotOffset + keyLen + sizeof(PageNum), sizeof(short));
                dynamic_cast<LeafNode*>(res)->leafEntries.push_back(entry);
            }
        }
        return res;
    }
    RC BTree::loadNode(IXFileHandle &ixfileHandle, Node* &node){
        if (node == NULL) return -1;
        ixfileHandle.ixReadPageCounter++;
        PageNum pageNum = node->pageNum;
        if (nodeMap.find(pageNum) != nodeMap.end()){
            node = nodeMap[pageNum];
        }else{
            char* pageData = (char*)calloc(PAGE_SIZE, 1);
            if (ixfileHandle.readPage(pageNum, pageData) == -1){
                free(pageData);
                return -1;
            }
            delete node;
            node = generateNode(pageData);
            node->pageNum = pageNum;
            nodeMap[pageNum] = node;
            free(pageData);
        }
        return 0;
    }

    int IndexManager::compareKey(AttrType attrType, const void* v1, const void* v2) const{
        if (attrType == TypeInt){
            return *(int*)v1 - *(int*)v2;
        }else if (attrType == TypeReal){
            return *(float*)v1 - *(float*)v2>0?1:*(float*)v1 - *(float*)v2==0?0:-1;
        }else if (attrType == TypeVarChar){
            int strLen1 = *(int*)v1;
            int strLen2 = *(int*)v2;
            std::string s1((char*)v1 + sizeof(int), strLen1);
            std::string s2((char*)v2 + sizeof(int), strLen2);
            return s1.compare(s2);
        }
        return 0;
    }
    
    Node* BTree::findLeafNode(IXFileHandle &ixfileHandle, const LeafEntry &pair, Node* node) {
        while (node->type != LEAF){
            int size = dynamic_cast<InternalNode*>(node)->internalEntries.size();
            if (attrType == TypeInt){            
                int key;
                memcpy(&key, pair.key, sizeof(int));
                for (int i = 0; i <= size; i++){
                    if (i == size){
                        ixfileHandle.ixReadPageCounter++;
                        node = dynamic_cast<InternalNode*>(node)->internalEntries[size - 1].right;
                        if (dynamic_cast<InternalNode*>(node)->isLoaded == false){
                            loadNode(ixfileHandle, node);
                        }
                    }else{
                        int entryKey;
                        memcpy(&entryKey, dynamic_cast<InternalNode*>(node)->internalEntries[i].key, sizeof(int));
                        if (key < entryKey){
                            ixfileHandle.ixReadPageCounter++;
                            node = dynamic_cast<InternalNode*>(node)->internalEntries[i].left;
                            if (dynamic_cast<InternalNode*>(node)->isLoaded == false){
                                loadNode(ixfileHandle, node);
                            }
                            break;
                        }
                    }
                }
            }else if (attrType == TypeReal){
                float key;
                memcpy(&key, (char *)pair.key, sizeof(float));
                for (int i = 0; i <= size; i++){
                    if (i == size) {
                        ixfileHandle.ixReadPageCounter++;
                        node = dynamic_cast<InternalNode*>(node)->internalEntries[size - 1].right;
                        if (dynamic_cast<InternalNode*>(node)->isLoaded == false) {
                            loadNode(ixfileHandle, node);
                        }
                    }
                    else{
                        float entryKey;
                        memcpy(&entryKey, dynamic_cast<InternalNode*>(node)->internalEntries[i].key, sizeof(float));
                        if (key < entryKey){
                            ixfileHandle.ixReadPageCounter++;
                            node = dynamic_cast<InternalNode*>(node)->internalEntries[i].left;
                            if (dynamic_cast<InternalNode*>(node)->isLoaded == false){
                                loadNode(ixfileHandle, node);
                            }
                            break;
                        }
                    }
                }
            }else if (attrType == TypeVarChar) {
                for (int i = 0; i <= size; i++) {
                    if (i == size) {
                        ixfileHandle.ixReadPageCounter++;
                        node = dynamic_cast<InternalNode*>(node)->internalEntries[size - 1].right;
                        if (dynamic_cast<InternalNode*>(node)->isLoaded == false) {
                            loadNode(ixfileHandle, node);
                        }
                    }else{
                        if (PeterDB::IndexManager::instance().compareKey(attrType,pair.key, dynamic_cast<InternalNode*>(node)->internalEntries[i].key) < 0) {
                            ixfileHandle.ixReadPageCounter++;
                            node = dynamic_cast<InternalNode*>(node)->internalEntries[i].left;
                            if (dynamic_cast<InternalNode*>(node)->isLoaded == false) {
                                loadNode(ixfileHandle, node);
                            }
                            break;
                        }
                    }
                }
            }
        }
        return node;
    }

    RC BTree::insertEntry(IXFileHandle &ixFileHandle, const LeafEntry &pair){
        int entryLen = 0;
        if(attrType == TypeVarChar) entryLen = *(int*)pair.key;
        entryLen += sizeof(int) + sizeof(PageNum) + sizeof(short);
        if (root == NULL) {
            //set node 
            LeafNode* node = new LeafNode();
            node->leafEntries.push_back(pair);
            node->size += entryLen + sizeof(short);
            node->isLoaded = true;
            //set tree 
            root = node;
            minLeaf = node;
            char* new_page = (char*)calloc(PAGE_SIZE, 1);
            generatePage(node, new_page);
            ixFileHandle.appendPage(new_page);
            free(new_page);
            nodeMap[ixFileHandle.ixAppendPageCounter - 1] = node;
            node->pageNum = ixFileHandle.ixAppendPageCounter - 1;
            //set meta fields
            ixFileHandle.root = ixFileHandle.ixAppendPageCounter - 1;
            ixFileHandle.minLeaf = ixFileHandle.ixAppendPageCounter - 1;
        }else {
            Node* targetNode = findLeafNode(ixFileHandle, pair, root);     //find the inserted leaf node
            if (dynamic_cast<LeafNode*>(targetNode)->isLoaded == false) {
                loadNode(ixFileHandle, targetNode);
            }
            //insert the entry into targetNode
            bool isAdded = false;
            for (auto i = dynamic_cast<LeafNode*>(targetNode)->leafEntries.begin(); i < dynamic_cast<LeafNode*>(targetNode)->leafEntries.end(); i++) {
                if (PeterDB::IndexManager::instance().compareKey(attrType,pair.key, (*i).key) < 0) {
                    dynamic_cast<LeafNode*>(targetNode)->leafEntries.insert(i, pair);
                    isAdded = true;
                    break;
                }
            }
            if (isAdded == false){
                dynamic_cast<LeafNode*>(targetNode)->leafEntries.push_back(pair);
            }
            dynamic_cast<LeafNode*>(targetNode)->size += entryLen + sizeof(short);
            if (dynamic_cast<LeafNode*>(targetNode)->size <= PAGE_SIZE) {   //insert into targetNode
                //write the updated target node into file
                char* newPage = (char*)calloc(PAGE_SIZE, 1);
                generatePage(targetNode, newPage);
                ixFileHandle.writePage(dynamic_cast<LeafNode*>(targetNode)->pageNum, newPage);
                free(newPage);
                return 0;
            }else {//split leaf node;
                LeafNode* newNode = new LeafNode();
                newNode->isLoaded = true;
                //entry too large
                if (dynamic_cast<LeafNode*>(targetNode)->leafEntries.size() <= 1) return -1;
                //start from mid, find same keys, put them into the same node
                int mid = dynamic_cast<LeafNode*>(targetNode)->leafEntries.size() / 2;
                int end = 0;
                //search towards end
                for (int i = mid; i <= dynamic_cast<LeafNode*>(targetNode)->leafEntries.size(); i++) {
                    if (i == dynamic_cast<LeafNode*>(targetNode)->leafEntries.size()) {
                        end = 1;
                        break;
                    }
                    if (PeterDB::IndexManager::instance().compareKey(attrType,dynamic_cast<LeafNode*>(targetNode)->leafEntries[i].key, dynamic_cast<LeafNode*>(targetNode)->leafEntries[i - 1].key) == 0) continue;
                    else {
                        mid = i;
                        break;
                    }
                }
                if (end == 1) {
                    //search towards front
                    for (int i = mid; i >= 0; i--) {
                        if (i == 0) return -1;//all keys are the same, can't split
                        if (PeterDB::IndexManager::instance().compareKey(attrType,dynamic_cast<LeafNode*>(targetNode)->leafEntries[i].key, dynamic_cast<LeafNode*>(targetNode)->leafEntries[i - 1].key) == 0)continue;
                        else {
                            mid = i;
                            break;
                        }
                    }
                }
                //split node and update size
                for (int i = mid; i < dynamic_cast<LeafNode*>(targetNode)->leafEntries.size(); i++) {
                    LeafEntry leafEntry(attrType, dynamic_cast<LeafNode*>(targetNode)->leafEntries[i].key, dynamic_cast<LeafNode*>(targetNode)->leafEntries[i].rid);
                    dynamic_cast<LeafNode*>(newNode)->leafEntries.push_back(leafEntry);
                    int strLen = 0;
                    if (attrType == TypeVarChar) {
                        memcpy(&strLen, (char *)dynamic_cast<LeafNode*>(targetNode)->leafEntries[i].key, 4);
                    }
                    dynamic_cast<LeafNode*>(targetNode)->size -= strLen + sizeof(short) + sizeof(int) + sizeof(PageNum) + sizeof(short);
                    newNode->size += strLen + sizeof(short) + sizeof(int) + sizeof(PageNum) + sizeof(short);
                }

                for (auto i = dynamic_cast<LeafNode*>(targetNode)->leafEntries.begin() + mid; i != dynamic_cast<LeafNode*>(targetNode)->leafEntries.end(); i++)
                    free(i->key);
                dynamic_cast<LeafNode*>(targetNode)->leafEntries.erase(dynamic_cast<LeafNode*>(targetNode)->leafEntries.begin() + mid, dynamic_cast<LeafNode*>(targetNode)->leafEntries.end());
                //update rightPointer
                newNode->rightPointer = dynamic_cast<LeafNode*>(targetNode)->rightPointer;
                dynamic_cast<LeafNode*>(targetNode)->rightPointer = newNode;
                //write updated targetNode to file
                char* updatedTargetPage = (char*)calloc(PAGE_SIZE, 1);
                generatePage(targetNode, updatedTargetPage);
                ixFileHandle.writePage(dynamic_cast<LeafNode*>(targetNode)->pageNum, updatedTargetPage);
                free(updatedTargetPage);
                //write new leaf node to file
                char* newLeafPage = (char*)calloc(PAGE_SIZE, 1);
                generatePage(newNode, newLeafPage);
                ixFileHandle.appendPage(newLeafPage);
                nodeMap[ixFileHandle.ixAppendPageCounter - 1] = newNode;
                newNode->pageNum = ixFileHandle.ixAppendPageCounter - 1;
                free(newLeafPage);
                //if no parent node, create one
                if (dynamic_cast<LeafNode*>(targetNode)->parent == NULL) {
                    InternalNode* parent = new InternalNode();
                    parent->isLoaded = true;
                    InternalEntry internal_pair(attrType, newNode->leafEntries[0].key, targetNode, newNode);
                    parent->internalEntries.push_back(internal_pair);
                    parent->type = INTERNAL;
                    int strLen = 0;
                    if (attrType == TypeVarChar) {
                        memcpy(&strLen, (char *)dynamic_cast<LeafNode*>(newNode)->leafEntries[0].key, sizeof(int));
                    }
                    parent->size += strLen + sizeof(int) + sizeof(short);
                    //write parent node to file
                    char* parentPage = (char*)calloc(PAGE_SIZE, 1);
                    generatePage(parent,parentPage);
                    ixFileHandle.appendPage(parentPage);
                    free(parentPage);
                    nodeMap[ixFileHandle.ixAppendPageCounter - 1] = parent;
                    parent->pageNum = ixFileHandle.ixAppendPageCounter - 1;
                    //set parent
                    dynamic_cast<LeafNode*>(targetNode)->parent = parent;
                    newNode->parent = parent;
                    //set meta fields
                    ixFileHandle.root = ixFileHandle.ixAppendPageCounter - 1;
                    root = parent;
                    return 0;
                }
                else {  // it has parent node
                    Node* parent = dynamic_cast<LeafNode*>(targetNode)->parent;
                    PageNum parentPageNum = dynamic_cast<LeafNode*>(targetNode)->parent->pageNum;
                    InternalEntry internal_pair(attrType, dynamic_cast<LeafNode*>(newNode)->leafEntries[0].key, targetNode, newNode);
                    //add entry to internal node
                    bool isInternalAdded = false;
                    int addedIndex = 0;
                    for (auto i = dynamic_cast<InternalNode*>(parent)->internalEntries.begin(); i != dynamic_cast<InternalNode*>(parent)->internalEntries.end(); i++) {
                        if (PeterDB::IndexManager::instance().compareKey(attrType,internal_pair.key, (*i).key) < 0) {
                            dynamic_cast<InternalNode*>(parent)->internalEntries.insert(i, internal_pair);
                            isInternalAdded = true;
                            addedIndex = i - dynamic_cast<InternalNode*>(parent)->internalEntries.begin();
                            break;
                        }
                    }
                    int strLen = 0;
                    if (attrType == TypeVarChar) {
                        memcpy(&strLen, (char *)dynamic_cast<LeafNode*>(newNode)->leafEntries[0].key, sizeof(int));
                    }
                    if (isInternalAdded == false) {
                        dynamic_cast<InternalNode*>(parent)->internalEntries.push_back(internal_pair);
                        dynamic_cast<InternalNode*>(parent)->internalEntries[dynamic_cast<InternalNode*>(parent)->internalEntries.size() - 2].right = internal_pair.left;
                        dynamic_cast<InternalNode*>(parent)->size += strLen + 2 * sizeof(int) + sizeof(short);//pointer+key+offset
                    }else if (addedIndex == 0) {
                        dynamic_cast<InternalNode*>(parent)->internalEntries[1].left = internal_pair.right;
                        dynamic_cast<InternalNode*>(parent)->size += strLen + 2 * sizeof(int) + sizeof(short);
                    }else {
                        dynamic_cast<InternalNode*>(parent)->internalEntries[addedIndex - 1].right = internal_pair.left;
                        dynamic_cast<InternalNode*>(parent)->internalEntries[addedIndex + 1].left = internal_pair.right;
                        dynamic_cast<InternalNode*>(parent)->size += strLen + 2 * sizeof(int) + sizeof(short);
                    }
                    if (dynamic_cast<InternalNode*>(parent)->size <= PAGE_SIZE) {
                        char* updatedParentPage = (char*)calloc(PAGE_SIZE, 1);
                        generatePage(parent, updatedParentPage);
                        ixFileHandle.writePage(dynamic_cast<InternalNode*>(parent)->pageNum, updatedParentPage);
                        free(updatedParentPage);
                        return 0;
                    }
                    while (dynamic_cast<InternalNode*>(parent)->size > PAGE_SIZE) {   //continuously split parent node
                        Node* newInternal= new InternalNode();
                        newInternal->isLoaded = true;
                        newInternal->size += sizeof(int);//right pointer
                        //move half of entries and update size
                        for (int i = dynamic_cast<InternalNode*>(parent)->internalEntries.size() / 2 + 1; i < dynamic_cast<InternalNode*>(parent)->internalEntries.size(); i++) {
                            InternalEntry internalEntry(attrType, dynamic_cast<InternalNode*>(parent)->internalEntries[i].key, dynamic_cast<InternalNode*>(parent)->internalEntries[i].left, dynamic_cast<InternalNode*>(parent)->internalEntries[i].right);
                            dynamic_cast<InternalNode*>(newInternal)->internalEntries.push_back(internalEntry);
                            int strLen = 0;
                            if (attrType == TypeVarChar) {
                                memcpy(&strLen, (char *) dynamic_cast<InternalNode *>(parent)->internalEntries[i].key,4);
                            }//key, offset, left pointer
                            dynamic_cast<InternalNode*>(parent)->size -= sizeof(int) + strLen + sizeof(short) + sizeof(int);
                            dynamic_cast<InternalNode*>(newInternal)->size += sizeof(int) + strLen + sizeof(short) + sizeof(int);
                        }
                        //delete half of parent node
                        for (auto i = dynamic_cast<InternalNode*>(parent)->internalEntries.begin() + mid; i != dynamic_cast<InternalNode*>(parent)->internalEntries.end(); i++)
                            free(i->key);
                        dynamic_cast<InternalNode*>(parent)->internalEntries.erase(dynamic_cast<InternalNode*>(parent)->internalEntries.begin() + mid, dynamic_cast<InternalNode*>(parent)->internalEntries.end());
                        //write updated parent to file
                        char* updatedParentPage = (char*)calloc(PAGE_SIZE, 1);
                        generatePage(parent, updatedParentPage);
                        ixFileHandle.writePage(dynamic_cast<InternalNode*>(parent)->pageNum, updatedParentPage);
                        free(updatedParentPage);
                        //write new internal node to file
                        char* newInternalPage = (char*)calloc(PAGE_SIZE, 1);
                        generatePage(newInternal, newInternalPage);
                        ixFileHandle.appendPage(newInternalPage);
                        nodeMap[ixFileHandle.ixAppendPageCounter - 1] = newInternal;
                        newInternal->pageNum = ixFileHandle.ixAppendPageCounter - 1;
                        free(newInternalPage);
                        if (dynamic_cast<InternalNode*>(parent)->parent == NULL) {
                            InternalNode* rootParent = new InternalNode();
                            rootParent->isLoaded = true;
                            InternalEntry rootEntry(attrType, dynamic_cast<InternalNode*>(newInternal)->internalEntries[0].key, parent, newInternal);
                            rootParent->internalEntries.push_back(rootEntry);
                            int strLen = 0;
                            if (attrType == TypeVarChar) {
                                memcpy(&strLen, (char *)dynamic_cast<InternalNode*>(newInternal)->internalEntries[0].key, sizeof(int));
                            }
                            rootParent->size += strLen + sizeof(int) + sizeof(short);
                            //write root node to file
                            char* rootPage = (char*)calloc(PAGE_SIZE, 1);
                            generatePage(rootParent,rootPage);
                            ixFileHandle.appendPage(rootPage);
                            free(rootPage);
                            nodeMap[ixFileHandle.ixAppendPageCounter - 1] = rootParent;
                            rootParent->pageNum = ixFileHandle.ixAppendPageCounter - 1;
                            //set parent
                            dynamic_cast<InternalNode*>(parent)->parent = rootParent;
                            newInternal->parent = rootParent;
                            //set meta fields
                            ixFileHandle.root = ixFileHandle.ixAppendPageCounter - 1;
                            root = rootParent;
                            return 0;
                        }
                        else {  // it has parent
                            Node* newParent = dynamic_cast<InternalNode*>(parent)->parent;
                            PageNum newParentPageNum = dynamic_cast<InternalNode*>(parent)->parent->pageNum;
                            InternalEntry new_internal_pair(attrType, dynamic_cast<InternalNode*>(newInternal)->internalEntries[0].key, parent, newInternal);
                            //add entry to internal node
                            bool isInternalAdded = false;
                            int addedIndex = 0;
                            for (auto i = dynamic_cast<InternalNode*>(newParent)->internalEntries.begin(); i != dynamic_cast<InternalNode*>(newParent)->internalEntries.end(); i++) {
                                if (PeterDB::IndexManager::instance().compareKey(attrType,internal_pair.key, (*i).key) < 0) {
                                    dynamic_cast<InternalNode*>(newParent)->internalEntries.insert(i, internal_pair);
                                    isInternalAdded = true;
                                    addedIndex = i - dynamic_cast<InternalNode*>(newParent)->internalEntries.begin();
                                    break;
                                }
                            }
                            int strLen = 0;
                            if (attrType == TypeVarChar) {
                                memcpy(&strLen, (char *)dynamic_cast<InternalNode*>(newInternal)->internalEntries[0].key, sizeof(int));
                            }
                            if (isInternalAdded == false) {
                                dynamic_cast<InternalNode*>(newParent)->internalEntries.push_back(new_internal_pair);
                                dynamic_cast<InternalNode*>(newParent)->internalEntries[dynamic_cast<InternalNode*>(newParent)->internalEntries.size() - 2].right = new_internal_pair.left;
                                dynamic_cast<InternalNode*>(newParent)->size += strLen + 2 * sizeof(int) + sizeof(short);//leftpointer+key+offset
                            }else if (addedIndex == 0) {
                                dynamic_cast<InternalNode*>(newParent)->internalEntries[1].left = new_internal_pair.right;
                                dynamic_cast<InternalNode*>(newParent)->size += strLen + 2 * sizeof(int) + sizeof(short);
                            }else {
                                dynamic_cast<InternalNode*>(newParent)->internalEntries[addedIndex - 1].right = new_internal_pair.left;
                                dynamic_cast<InternalNode*>(newParent)->internalEntries[addedIndex + 1].left = new_internal_pair.right;
                                dynamic_cast<InternalNode*>(newParent)->size += strLen + 2 * sizeof(int) + sizeof(short);
                            }
                            parent = newParent;
                        }
                    }
                }
            }
        }
        return 0;
    }
    Node::Node() {}
    Node::~Node() noexcept {}
    BTree::BTree() {
        root = NULL;
        minLeaf = NULL;
    }
    BTree::~BTree() {}
    LeafNode::~LeafNode() noexcept {}
    InternalNode::~InternalNode(){
        for(auto i:internalEntries){

        }
    }
    InternalNode::InternalNode(){
        type = INTERNAL;
        size = sizeof(char) + sizeof(short) + sizeof(PageNum) + sizeof(short);
        parent = NULL;
        pageNum = -1;
        isDirty = false;
        isLoaded = false;
    }
    LeafNode::LeafNode() {
        type =  LEAF;
        size = sizeof(char) + sizeof(short) + 3 * sizeof(PageNum) + sizeof(short);
        parent = NULL;
        pageNum = -1;
        isDirty = false;
        isLoaded = false;
        rightPointer =NULL;
        overflowPointer = NULL;
    }
    LeafEntry::LeafEntry(){
        key = NULL;
        rid.pageNum = -1;
        rid.slotNum = -1;
        size = 0;
    }

    LeafEntry::LeafEntry(const AttrType &attrType, const void* key, const RID rid){
        if (attrType == TypeInt){
            this->key = malloc(sizeof(int));
            memcpy(this->key, key, sizeof(int));
            size = sizeof(int);
        }else if (attrType == TypeReal){
            this->key = malloc(sizeof(float));
            memcpy(this->key, key, sizeof(float));
            size = sizeof(float);
        }else if (attrType == TypeVarChar){
            int strLen = *(int*)key;
            this->key = malloc(sizeof(int) + strLen);
            memcpy(this->key, key, sizeof(int) + strLen);
            size = sizeof(int) + strLen;
        }
        this->rid = rid;
    }

    LeafEntry::~LeafEntry(){}

    InternalEntry::InternalEntry(){
        key = NULL;
        left = NULL;
        right = NULL;
    }

    InternalEntry::InternalEntry(const AttrType &attribute, const void* key, Node* left, Node* right){
        if (attribute == TypeInt){
            this->key = malloc(sizeof(int));
            size = sizeof(int);
            memcpy(this->key, key, sizeof(int));
        }else if (attribute == TypeReal){
            this->key = malloc(sizeof(float));
            size = sizeof(float);
            memcpy(this->key, key, sizeof(float));
        }else if (attribute == TypeVarChar){
            int strLen = *(int*)key;
            this->key = malloc(sizeof(int) + strLen);
            size = sizeof(int) + strLen;
            memcpy(this->key, key, sizeof(int) + strLen);
        }
        this->left = left;
        this->right = right;
    }

    InternalEntry::InternalEntry(const AttrType &attrType, const void* key){
        if (attrType == TypeInt){
            this->key = malloc(sizeof(int));
            size = sizeof(int);
            memcpy(this->key, key, sizeof(int));
        }
        else if (attrType == TypeReal){
            this->key = malloc(sizeof(float));
            size = sizeof(float);
            memcpy(this->key, key, sizeof(float));
        }
        else if (attrType == TypeVarChar){
            int strLen = *(int*)key;
            this->key = malloc(sizeof(int) + strLen);
            size = sizeof(int) + strLen;
            memcpy(this->key, key, sizeof(int) + strLen);
        }
    }

    InternalEntry::~InternalEntry(){}
} // namespace PeterDB