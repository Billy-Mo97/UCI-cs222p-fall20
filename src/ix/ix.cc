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
                fseek(file, 0, SEEK_SET);
                fwrite(&readNum, sizeof(unsigned), 1, file);
                fwrite(&writeNum, sizeof(unsigned), 1, file);
                fwrite(&appendNum, sizeof(unsigned), 1, file);
                fwrite(&pageNum, sizeof(unsigned), 1, file);
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
        if (remove(fileName.c_str()) == 0) { return 0; }
        else { return -1; }
    }

    RC IXFileHandle::readHiddenPage() {
        //Read the hidden page of the file,
        //initiate corresponding parameters in IXFileHandle class.
        if (!fileHandle.pointer) { return -1; }
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

        free(data);
        return 0;
    }

    RC IXFileHandle::readRootPointerPage() {
        char *data = (char *) malloc(PAGE_SIZE);
        if (readPage(0, data) == -1) {
            free(data);
            return -1;
        }
        short offset = 0;
        memcpy(&root, data + offset, sizeof(int));
        offset += sizeof(int);
        memcpy(&minLeaf, data + offset, sizeof(int));
        offset += sizeof(int);
        AttrType type;
        memcpy(&type, data + offset, sizeof(AttrType));
        offset += sizeof(AttrType);
        if (type != TypeNull && bTree == nullptr) {
            bTree = new BTree();
            bTree->attrType = type;
        }

        free(data);
        return 0;
    }

    RC IXFileHandle::writeHiddenPage() {
        //Write ixReadPageCounter, ixWritePageCounter, ixAppendPageCounter,
        //numOfPages, root, minLeaf into hidden page.
        //Also, write attrType from bTree into hidden page.
        if (!fileHandle.pointer) { return -1; }
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
        size_t writeSize = fwrite(data, 1, PAGE_SIZE, fileHandle.pointer);
        if (writeSize != PAGE_SIZE) {
            free(data);
            return -1;
        }
        if (writeRootPointerPage() == -1) { return -1; }
        fflush(fileHandle.pointer);
        free(data);
        return 0;
    }

    IXFileHandle::IXFileHandle() {
        ixReadPageCounter = 0;
        ixWritePageCounter = 0;
        ixAppendPageCounter = 0;
        numOfPages = 0;
        bTree = nullptr;
        root = -1;
        minLeaf = -1;
    }

    IXFileHandle::~IXFileHandle() {
        ixReadPageCounter = 0;
        ixWritePageCounter = 0;
        ixAppendPageCounter = 0;
        numOfPages = 0;
        bTree = nullptr;
        root = -1;
        minLeaf = -1;
    }

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
        numOfPages++;
        RC res = fileHandle.appendPage(data);
        return res;
    }

    RC IXFileHandle::appendRootPage(const Attribute &attribute) {
        //This is a helper function to append a new root pointer page into index file.
        char *data = (char *) calloc(PAGE_SIZE, 1);
        short offset = 0;
        int newRoot = 1, newMinLeaf = 1;
        memcpy(data + offset, &newRoot, sizeof(int));
        offset += sizeof(int);
        memcpy(data + offset, &newMinLeaf, sizeof(int));
        offset += sizeof(int);
        memcpy(data + offset, &attribute.type, sizeof(AttrType));
        offset += sizeof(AttrType);
        if (appendPage(data) == -1) { return -1; }
        free(data);
        root = newRoot;
        return 0;
    }

    RC IXFileHandle::writeRootPointerPage() {
        char *data = (char *) calloc(PAGE_SIZE, 1);
        short offset = 0;
        memcpy(data + offset, &root, sizeof(int));
        offset += sizeof(int);
        memcpy(data + offset, &minLeaf, sizeof(int));
        offset += sizeof(int);
        if (bTree != nullptr) {
            memcpy(data + offset, &bTree->attrType, sizeof(AttrType));
            offset += sizeof(AttrType);
        } else {
            AttrType nullType = TypeNull;
            memcpy(data + offset, &nullType, sizeof(AttrType));
            offset += sizeof(AttrType);
        }
        if (writePage(0, data) == -1) {
            free(data);
            return -1;
        }
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
        ixFileHandle.fileName = fileName;
        if (ixFileHandle.readHiddenPage() == -1) { return -1; }
        //If numOfPages is greater than 0, there is a root pointer page, read it.
        if (ixFileHandle.numOfPages > 0) {
            if (ixFileHandle.readRootPointerPage() == -1) { return -1; }
        }
        if (ixFileHandle.root != NULLNODE) {
            if (setRoot(ixFileHandle) == -1) { return -1; }
        }
        if (ixFileHandle.minLeaf != NULLNODE) {
            if (setMinLeaf(ixFileHandle) == -1) { return -1; }
        }
        return 0;
    }


    RC IndexManager::closeFile(IXFileHandle &ixFileHandle) {
        //Close the ixFileHandle.
        //Before closing the pointer, write all the parameters in ixFileHandle
        //back to hidden page, deallocate the space of bTree.
        if (ixFileHandle.writeHiddenPage() == -1) { return -1; }
        if (PagedFileManager::instance().closeFile(ixFileHandle.fileHandle) == -1) { return -1; }
        delete ixFileHandle.bTree;
        ixFileHandle.bTree = nullptr;
        ixFileHandle.fileName = "";
        return 0;
    }

    RC IndexManager::setRoot(IXFileHandle &ixFileHandle) {
        // This is a helper function to help load root pointer in B+ tree.
        // It loads the root page specified by the root pageNum in ixFileHandle.
        char *rootPage = (char *) calloc(PAGE_SIZE, 1);
        if (ixFileHandle.root == NULLNODE) {
            ixFileHandle.bTree->root = nullptr;
            return 0;
        }
        if (ixFileHandle.readPage(ixFileHandle.root, rootPage) == -1) {
            free(rootPage);
            return -1;
        }
        if (ixFileHandle.bTree->generateNode(rootPage, ixFileHandle.bTree->root) == -1) { return -1; }
        ixFileHandle.bTree->root->pageNum = ixFileHandle.root;
        free(rootPage);
        ixFileHandle.bTree->nodeMap[ixFileHandle.root] = ixFileHandle.bTree->root;
        return 0;
    }

    RC IndexManager::getRootAndMinLeaf(IXFileHandle &ixFileHandle) {

        if (ixFileHandle.fileHandle.pointer == nullptr) { return -1; }
        struct stat buffer{};
        std::string fileName = ixFileHandle.fileName;
        if ((stat(ixFileHandle.fileName.c_str(), &buffer) != 0)) {
            return -1;
        }

        if (ixFileHandle.numOfPages < 1) { return 0; }
        char *rootPointerPage = (char *) calloc(PAGE_SIZE, 1);
        if (ixFileHandle.readPage(0, rootPointerPage) == -1) {
            free(rootPointerPage);
            return -1;
        }
        int rootPageNum;
        memcpy(&rootPageNum, rootPointerPage, sizeof(int));
        ixFileHandle.root = rootPageNum;
        int minLeafPageNum;
        memcpy(&minLeafPageNum, rootPointerPage + sizeof(int), sizeof(int));
        ixFileHandle.minLeaf = minLeafPageNum;

        if (ixFileHandle.bTree == nullptr) { return 0; }
        if (rootPageNum == -1 && minLeafPageNum == -1) {
            ixFileHandle.bTree->root = nullptr;
            ixFileHandle.bTree->minLeaf = nullptr;
        } else if (rootPageNum != -1 && minLeafPageNum != -1) {
            Node *rootPointer = new Node();
            rootPointer->pageNum = rootPageNum;
            if (ixFileHandle.bTree->loadNode(ixFileHandle, rootPointer) == -1) { return -1; }
            ixFileHandle.bTree->root = rootPointer;
            char rootType = rootPointer->type;

            Node *minLeafPointer = new Node();
            minLeafPointer->pageNum = minLeafPageNum;
            if (ixFileHandle.bTree->loadNode(ixFileHandle, minLeafPointer) == -1) { return -1; }
            ixFileHandle.bTree->minLeaf = minLeafPointer;
        }
        free(rootPointerPage);
        return 0;
    }


    RC IndexManager::setMinLeaf(IXFileHandle &ixFileHandle) {
        // This is a helper function to help load minLeaf pointer in B+ tree.
        // It loads the minLeaf page specified by the minLeaf pageNum in ixFileHandle.
        char *minPage = (char *) calloc(PAGE_SIZE, 1);
        if (ixFileHandle.minLeaf == -1) {
            ixFileHandle.bTree->minLeaf = nullptr;
            return 0;
        }
        if (ixFileHandle.readPage(ixFileHandle.minLeaf, minPage) == -1) {
            free(minPage);
            return -1;
        }
        if (ixFileHandle.bTree->generateNode(minPage, ixFileHandle.bTree->minLeaf) == -1) { return -1; }
        ixFileHandle.bTree->minLeaf->pageNum = ixFileHandle.minLeaf;
        free(minPage);
        ixFileHandle.bTree->nodeMap[ixFileHandle.minLeaf] = ixFileHandle.bTree->minLeaf;
        return 0;
    }

    RC IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key,
                                 const RID &rid) {
        //First, load the root and min leaf node from root pointer page.
        //If B+ Tree is null, the function will instantly return.
        if (getRootAndMinLeaf(ixFileHandle) == -1) { return -1; }
        LeafEntry leafEntry(attribute.type, key, rid);

        //If bTree pointer is pointing to null, initiate a new B+ Tree.
        if (ixFileHandle.bTree == nullptr) {
            ixFileHandle.bTree = new BTree();
            ixFileHandle.bTree->attrType = attribute.type;
            if (ixFileHandle.root == NULLNODE && ixFileHandle.minLeaf == NULLNODE) {
                if (ixFileHandle.appendRootPage(attribute) == -1) { return -1; }
            }
            if (ixFileHandle.bTree->initiateNullTree(ixFileHandle) == -1) { return -1; }
            InternalEntry *newChildEntry = nullptr;
            int rootPageNum = ixFileHandle.bTree->root->pageNum;
            if (ixFileHandle.bTree->insertEntry(ixFileHandle, ixFileHandle.bTree->root, leafEntry, newChildEntry) ==
                -1) { return -1; }
        } else {
            InternalEntry *newChildEntry = nullptr;
            int rootPageNum = ixFileHandle.bTree->root->pageNum;
            if (ixFileHandle.bTree->insertEntry(ixFileHandle, ixFileHandle.bTree->root, leafEntry, newChildEntry) ==
                -1) { return -1; }
        }
        if (ixFileHandle.writeRootPointerPage() == -1) { return -1; }
        return 0;
    }

    RC BTree::writeLeafNodeToFile(IXFileHandle &ixFileHandle, Node *targetNode) {
        //write the updated target node into file
        char *newPage = (char *) calloc(PAGE_SIZE, 1);
        if (generatePage(targetNode, newPage) == -1) { return -1; }
        if (ixFileHandle.writePage(dynamic_cast<LeafNode *>(targetNode)->pageNum, newPage) == -1) { return -1; }
        free(newPage);
        return 0;
    }

    RC BTree::deleteEntry(IXFileHandle &ixFileHandle, const LeafEntry &pair) {
        // Debug
        Node *targetNode = root;
        ixFileHandle.ixReadPageCounter++;
        int rootPageNum = root->pageNum;
        if (findLeafNode(ixFileHandle, pair, targetNode) == -1) { return -1; };
        int findLeafNodePageNum = targetNode->pageNum;
        if (dynamic_cast<LeafNode *>(targetNode)->isLoaded == false) {
            if (loadNode(ixFileHandle, targetNode) == -1) { return -1; }
        }
        //If we find pair, delete it
        for (auto i = dynamic_cast<LeafNode *>(targetNode)->leafEntries.begin();
             i < dynamic_cast<LeafNode *>(targetNode)->leafEntries.end(); i++) {
            int pos = i - dynamic_cast<LeafNode *>(targetNode)->leafEntries.begin();
            if ((*i).isDeleted) continue;
            int pairPageNum = pair.rid.pageNum;
            short pairSlotNum = pair.rid.slotNum;
            int iPageNum = (*i).rid.pageNum;
            int iSlotNum = (*i).rid.slotNum;
            if (PeterDB::IndexManager::instance().compareKey(attrType, pair.key, (*i).key) == 0) {
                if (pair.rid.pageNum == (*i).rid.pageNum && pair.rid.slotNum == (*i).rid.slotNum) {
                    dynamic_cast<LeafNode *>(targetNode)->sizeInPage -= pair.keySize + sizeof(RID);
                    deleteEntryInFile(ixFileHandle, targetNode, pos);
                    (*i).isDeleted = true;
                    return 0;
                }
            }
        }
        return -1;
    }

    bool BTree::isEntryDeleted(IXFileHandle &ixFileHandle, Node *targetNode, int i) {

        char *newPage = (char *) calloc(PAGE_SIZE, 1);
        if (ixFileHandle.readPage(targetNode->pageNum, newPage) == -1) { return -1; }
        short offset;
        memcpy(&offset, newPage + PAGE_SIZE - sizeof(short) * (i + 2), sizeof(short));
        if (offset == -1) return true;
        return false;
    }

    void shiftSlots(short targetOffset, short begin, short end, char *&page, Node *targetNode) {
        //find first undeleted slot in [begin, end)
        bool flag = false;
        short beginOffset, endOffset, endSlotSize;
        for (short i = begin; i < end; i++) {
            memcpy(&beginOffset, page + PAGE_SIZE - sizeof(short) * (i + 2), sizeof(short));
            if (beginOffset != -1) {
                flag = true;
                break;
            }
        }
        if (!flag) { return; }
        //find last undeleted slot in [begin, end)
        for (short i = end - 1; i >= begin; i--) {
            memcpy(&endOffset, page + PAGE_SIZE - sizeof(short) * (i + 2), sizeof(short));
            if (endOffset != -1) { break; }
        }
        int size = dynamic_cast<LeafNode *>(targetNode)->leafEntries.size();
        endSlotSize = dynamic_cast<LeafNode *>(targetNode)->leafEntries[size - 1].keySize + sizeof(RID);
        short shiftLength = endOffset + endSlotSize - beginOffset;
        memmove(page + targetOffset, page + beginOffset, shiftLength);
        short changeOffset = targetOffset - beginOffset;
        for (short i = begin; i < end; i++) {
            short tmp;
            memcpy(&tmp, page + PAGE_SIZE - sizeof(short) * (i + 2), sizeof(short));
            if (tmp != -1) {
                tmp += changeOffset;
                memcpy(page + PAGE_SIZE - sizeof(short) * (i + 2), &tmp, sizeof(short));
            }
        }//std::cout<<"shift slots to:"<<targetOffset<<std::endl;
    }

    RC BTree::deleteEntryInFile(IXFileHandle &ixFileHandle, Node *targetNode, int i) {
        char *newPage = (char *) calloc(PAGE_SIZE, 1);
        if (generatePage(targetNode, newPage) == -1) { return -1; }
        int deleteFlag = -1;
        short offset;
        memcpy(&offset, newPage + PAGE_SIZE - sizeof(short) * (i + 2), sizeof(short));
        if (offset == -1) return 0;
        memcpy(newPage + PAGE_SIZE - sizeof(short) * (i + 2), &deleteFlag, sizeof(short));
        int slotCount = dynamic_cast<LeafNode *>(targetNode)->leafEntries.size();
        if (i < slotCount - 1) {
            //int shiftDirSize = 2 * sizeof(short) * (slotCount - 1 - slotNum);
            //short shiftStart = PAGE_SIZE - sizeof(short) * (slotNum * 2 + 4)
            shiftSlots(offset, i + 1, slotCount, newPage, targetNode);
        }
        if (ixFileHandle.writePage(dynamic_cast<LeafNode *>(targetNode)->pageNum, newPage) == -1) { return -1; }
        free(newPage);
        return 0;
    }

    RC
    IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        //printf("start deleteEntry\n");
        if (getRootAndMinLeaf(ixFileHandle) == -1) { return -1; }
        //if (getMinLeaf(ixFileHandle) == -1) { return -1; }
        LeafEntry leafEntry(attribute.type, key, rid);
        if (ixFileHandle.bTree) {
            ixFileHandle.bTree->attrType = attribute.type;
            if (ixFileHandle.bTree->deleteEntry(ixFileHandle, leafEntry) == -1) { return -1; }
        } else {
            return -1;
        }
        return 0;
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
            if (j != i - 1) { out << ","; }
        }
        out << "]";
        out << '"';
        return 0;
    }

    RC IndexManager::leafDFS(IXFileHandle &ixFileHandle, Node *node, const Attribute &attribute,
                             std::ostream &out) const {
        //This is a helper function to print out all the keys and rids in a leaf node.
        int start = 0;
        int entrySize = dynamic_cast<LeafNode *>(node)->leafEntries.size();
        if (entrySize == 0) return 0;
        LeafEntry entry;
        bool notDeleted = false;
        for (int i = 0; i < dynamic_cast<LeafNode *>(node)->leafEntries.size(); i++) {
            entry = dynamic_cast<LeafNode *>(node)->leafEntries[i];
            if (entry.isDeleted == false) {
                notDeleted = true;
                break;
            }
        }
        if (!notDeleted) return 0;
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
                    if (i != dynamic_cast<LeafNode *>(node)->leafEntries.size()) out << ",\n";
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
                memcpy(&key, (char *) (dynamic_cast<InternalNode *>(node))->internalEntries[i].key, sizeof(int));
                out << key << '"';
            } else if (attribute.type == TypeReal) {
                float key;
                memcpy(&key, (char *) (dynamic_cast<InternalNode *>(node))->internalEntries[i].key, sizeof(float));
                out << key << '"';
            } else if (attribute.type == TypeVarChar) {
                int len;
                memcpy(&len, (dynamic_cast<InternalNode *>(node))->internalEntries[i].key, sizeof(int));
                std::string str((char *) (dynamic_cast<InternalNode *>(node))->internalEntries[i].key + sizeof(int),
                                len);
                out << str << '"';
            }
            if (i != dynamic_cast<InternalNode *>(node)->internalEntries.size() - 1) out << ",";
        }
        return 0;
    }

    RC IndexManager::DFS(IXFileHandle &ixFileHandle, Node *node, const Attribute &attribute, std::ostream &out) const {
        //Do breadth-first search on B+tree, starting on given node.
        //If the node is not loaded, load it into memory.

        //Debug
        if (node->isLoaded == false) {
            if (ixFileHandle.bTree->loadNode(ixFileHandle, node) == -1) {
                return -1;
            }
        }
        out << "{";
        out << "\"keys\":";
        out << "[\n";

        //If current node is a leaf node, print it.
        if (node->type == LEAF) {
            //std::string str((char *) (dynamic_cast<LeafNode *>(node))->leafEntries[0].key + sizeof(int), 1);
            if (leafDFS(ixFileHandle, node, attribute, out) == -1) { return -1; }
            out << "]";
        } else {
            //If current node is a internal node,
            //print internal node.
            if (printInternalNode(ixFileHandle, node, attribute, out) == -1) { return -1; }
            out << "]";
            //print its children.
            out << ",\n";
            out << "\"children\":[\n";
            for (int i = 0; i < dynamic_cast<InternalNode *>(node)->internalEntries.size(); i++) {
                //For each internal node's entry, do DFS search on its left child.
                if (DFS(ixFileHandle, dynamic_cast<InternalNode *>(node)->internalEntries[i].left, attribute, out) ==
                    -1) { return -1; }
                out << ",\n";
                //If it is the last entry on its node, do DFS search on its right child.
                if (i == dynamic_cast<InternalNode *>(node)->internalEntries.size() - 1) {
                    if (DFS(ixFileHandle, dynamic_cast<InternalNode *>(node)->internalEntries[i].right, attribute,
                            out) == -1) { return -1; }
                    out << "\n";
                }
            }
            out << "]";
        }
        out << "}";
        return 0;
    }

    RC IndexManager::printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) {
        //if (ixFileHandle.readRootPointerPage() == -1) { return -1; }
        if (getRootAndMinLeaf(ixFileHandle) == -1) { return -1; }
        //if (getMinLeaf(ixFileHandle) == -1) { return -1; }
        return DFS(ixFileHandle, ixFileHandle.bTree->root, attribute, out);
    }

    IX_ScanIterator::IX_ScanIterator() {}

    IX_ScanIterator::~IX_ScanIterator() {}

    RC IX_ScanIterator::setValues(const void *lowKey, const void *highKey, bool lowKeyInclusive,
                                  bool highKeyInclusive, IXFileHandle *ixFileHandle) {
        if (lowKey != nullptr) {
            if (this->type == TypeInt) {
                this->lowKey = malloc(sizeof(int));
                memcpy(this->lowKey, lowKey, sizeof(int));
            } else if (this->type == TypeReal) {
                this->lowKey = malloc(sizeof(float));
                memcpy(this->lowKey, lowKey, sizeof(float));
            } else if (this->type == TypeVarChar) {
                int strLen;
                memcpy(&strLen, lowKey, sizeof(int));
                memcpy(this->lowKey, lowKey, sizeof(int) + strLen);
            }
        } else {
            this->lowKey = nullptr;
        }

        if (highKey != nullptr) {
            if (this->type == TypeInt) {
                this->highKey = malloc(sizeof(int));
                memcpy(this->highKey, highKey, sizeof(int));
            } else if (this->type == TypeReal) {
                this->highKey = malloc(sizeof(float));
                memcpy(this->highKey, highKey, sizeof(float));
            } else if (this->type == TypeVarChar) {
                int strLen;
                memcpy(&strLen, highKey, sizeof(int));
                memcpy(this->highKey, highKey, sizeof(int) + strLen);
            }
        } else {
            this->highKey = nullptr;
        }

        this->ixFileHandle = ixFileHandle;
        this->lowKeyInclusive = lowKeyInclusive;
        this->highKeyInclusive = highKeyInclusive;

        return 0;
    }

    RC IndexManager::findInclusiveStartEntry(AttrType type, const void *lowKey, IX_ScanIterator &ix_ScanIterator,
                                             Node *&targetNode, bool &startFound) {
        for (auto i = dynamic_cast<LeafNode *>(targetNode)->leafEntries.begin();
             i < dynamic_cast<LeafNode *>(targetNode)->leafEntries.end(); i++) {
            if (compareKey(type, lowKey, (*i).key) <= 0) {
                ix_ScanIterator.startPageNum = targetNode->pageNum;
                ix_ScanIterator.startEntryIndex = i - dynamic_cast<LeafNode *>(targetNode)->leafEntries.begin();
                startFound = true;
                break;
            }
        }
        return 0;
    }

    RC IndexManager::findExclusiveStartEntry(AttrType type, const void *lowKey, IX_ScanIterator &ix_ScanIterator,
                                             Node *&targetNode, bool &startFound) {
        for (auto i = dynamic_cast<LeafNode *>(targetNode)->leafEntries.begin();
             i < dynamic_cast<LeafNode *>(targetNode)->leafEntries.end(); i++) {
            if (compareKey(type, lowKey, (*i).key) < 0) {
                ix_ScanIterator.startPageNum = targetNode->pageNum;
                ix_ScanIterator.startEntryIndex = i - dynamic_cast<LeafNode *>(targetNode)->leafEntries.begin();
                startFound = true;
                break;
            }
        }
        return 0;
    }


    RC BTree::getMaxLeaf(LeafNode *maxLeafNode) {
        maxLeafNode = dynamic_cast<LeafNode *>(minLeaf);
        while (maxLeafNode->rightPointer != nullptr) {
            maxLeafNode = dynamic_cast<LeafNode *>(maxLeafNode->rightPointer);
        }
        return 0;
    }

    RC IndexManager::scan(IXFileHandle &ixFileHandle,
                          const Attribute &attribute,
                          const void *lowKey,
                          const void *highKey,
                          bool lowKeyInclusive,
                          bool highKeyInclusive,
                          IX_ScanIterator &ix_ScanIterator) {
        if (getRootAndMinLeaf(ixFileHandle) == -1) { return -1; }
        //if (getMinLeaf(ixFileHandle) == -1) { return -1; }
        ix_ScanIterator.type = attribute.type;
        if (ix_ScanIterator.setValues(lowKey, highKey, lowKeyInclusive, highKeyInclusive, &ixFileHandle) ==
            -1) { return -1; }
        RID rid;

        if (lowKey == nullptr) {
            //ixFileHandle.ixReadPageCounter++;
            ix_ScanIterator.startPageNum = ixFileHandle.minLeaf;
            ix_ScanIterator.startEntryIndex = 0;
            ix_ScanIterator.curPageNum = ix_ScanIterator.startPageNum;
            ix_ScanIterator.curEntryIndex = ix_ScanIterator.startEntryIndex;
            return 0;
        }


        LeafEntry lowKeyEntry(attribute.type, lowKey, rid);
        Node *lowNode = ixFileHandle.bTree->root;
        if (ixFileHandle.bTree->findLeafNode(ixFileHandle, lowKeyEntry, lowNode) == -1) { return -1; }
        if (lowNode->isLoaded == false) {
            if (ixFileHandle.bTree->loadNode(ixFileHandle, lowNode) == -1) { return -1; }
        }
        int lowNodePageNum = lowNode->pageNum;
        if (lowKeyInclusive) {
            bool startFound = false;
            if (findInclusiveStartEntry(attribute.type, lowKey, ix_ScanIterator, lowNode, startFound) ==
                -1) { return -1; }
            lowNode = dynamic_cast<LeafNode *>(lowNode)->rightPointer;
            while (!startFound && lowNode != nullptr) {
                if (lowNode->isLoaded == false) {
                    if (ixFileHandle.bTree->loadNode(ixFileHandle, lowNode) == -1) { return -1; }
                }
                if (findInclusiveStartEntry(attribute.type, lowKey, ix_ScanIterator, lowNode, startFound) ==
                    -1) { return -1; }
                lowNode = dynamic_cast<LeafNode *>(lowNode)->rightPointer;
            }
        } else {
            bool startFound = false;
            if (findExclusiveStartEntry(attribute.type, lowKey, ix_ScanIterator, lowNode, startFound) ==
                -1) { return -1; }
            lowNode = dynamic_cast<LeafNode *>(lowNode)->rightPointer;
            while (!startFound && lowNode != nullptr) {
                if (lowNode->isLoaded == false) {
                    if (ixFileHandle.bTree->loadNode(ixFileHandle, lowNode) == -1) { return -1; }
                }
                if (findExclusiveStartEntry(attribute.type, lowKey, ix_ScanIterator, lowNode, startFound) ==
                    -1) { return -1; }
                lowNode = dynamic_cast<LeafNode *>(lowNode)->rightPointer;
            }
        }
        ix_ScanIterator.curPageNum = ix_ScanIterator.startPageNum;
        ix_ScanIterator.curEntryIndex = ix_ScanIterator.startEntryIndex;
        return 0;
    }

    RC IX_ScanIterator::getLeafHeaderFromPage(char *pageData, char &nodeType, short &nodeSize, PageNum &rightPageNum,
                                              PageNum &overflowPageNum) {
        short offset = 0;
        //Check the node type;
        memcpy(&nodeType, pageData + offset, sizeof(char));
        offset += sizeof(char);
        if (nodeType != LEAF) { return -1; }

        memcpy(&nodeSize, pageData + offset, sizeof(short));
        offset += sizeof(short);

        memcpy(&rightPageNum, pageData + offset, sizeof(PageNum));
        offset += sizeof(PageNum);

        memcpy(&overflowPageNum, pageData + offset, sizeof(PageNum));
        offset += sizeof(PageNum);

        return 0;
    }

    RC IX_ScanIterator::getKeyLen(char *pageData, short &offset, short &keyLen) {
        if (type == TypeInt) {
            keyLen = sizeof(int);
        } else if (type == TypeReal) {
            keyLen = sizeof(float);
        } else if (type == TypeVarChar) {
            int strLen;
            memcpy(&strLen, pageData + offset, sizeof(int));
            keyLen = sizeof(int) + strLen;
        }

        return 0;
    }

    int IX_ScanIterator::getLeafEntryFromPage(char *pageData, PageNum &rightPageNum, short &entryCount, RID &rid,
                                              void *key) {
        //First, get the header information from the page.
        char nodeType;
        short nodeSize;
        PageNum overflowPageNum;
        if (getLeafHeaderFromPage(pageData, nodeType, nodeSize, rightPageNum, overflowPageNum) == -1) { return -1; }

        //Then, get the entry slot offset from the end of the page.
        memcpy(&entryCount, pageData + PAGE_SIZE - sizeof(short), sizeof(short));
        short entrySlotOffset;
        memcpy(&entrySlotOffset, pageData + PAGE_SIZE - (sizeof(short) + (curEntryIndex + 1) * sizeof(short)),
               sizeof(short));
        while (entrySlotOffset == -1) {
            curEntryIndex++;
            if (curEntryIndex >= entryCount) {
                curEntryIndex = 0;
                curPageNum = rightPageNum;
                if (curPageNum == -1) { return 1; }
                else { return 2; }
            }
            memcpy(&entrySlotOffset, pageData + PAGE_SIZE - sizeof(short) * (curEntryIndex + 2), sizeof(short));
        }
        //Finally, go to the entry offset and get the key and rid of entry.
        short keyLen;
        if (getKeyLen(pageData, entrySlotOffset, keyLen) == -1) { return -1; }
        memcpy(key, pageData + entrySlotOffset, keyLen);
        //std::cout<<"key:"<<*(int*)key<<" ";
        memcpy(&rid.pageNum, pageData + entrySlotOffset + keyLen, sizeof(PageNum));
        memcpy(&rid.slotNum, pageData + entrySlotOffset + keyLen + sizeof(PageNum), sizeof(short));

        return 0;
    }

    RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
        PeterDB::IndexManager &idm = PeterDB::IndexManager::instance();
        char *pageData = (char *) calloc(PAGE_SIZE, 1);

        // If curPageNum is pointing to -1, all leaf nodes has been scanned, return IX_EOF.
        if (curPageNum == -1) { return IX_EOF; }

        // Read current page that is being scanned.
        if (ixFileHandle->readPage(curPageNum, pageData) == -1) { return -1; }

        short entryCountTest;
        memcpy(&entryCountTest, pageData + PAGE_SIZE - sizeof(short), sizeof(short));

        PageNum nextPageNum;
        short entryCount;
        int result = getLeafEntryFromPage(pageData, nextPageNum, entryCount, rid, key);
        if (result == 1) { return IX_EOF; }
        else if (result == 2) {
            free(pageData);
            return getNextEntry(rid, key);
        }

        //std::cout << "Scanning: page " << curPageNum << std::endl;
        //std::cout << "Scanning, next page: " << nextPageNum << std::endl;

        // Check if key satisfies upper bound.
        // If not, return IX_EOF.
        if (highKey != nullptr) {
            if (highKeyInclusive) {
                if (idm.compareKey(type, key, highKey) > 0) { return IX_EOF; }
            } else {
                if (idm.compareKey(type, key, highKey) >= 0) { return IX_EOF; }
            }
        }
//        short offset;
//        memcpy(&offset, pageData + PAGE_SIZE - sizeof(short) * (curEntryIndex + 2), sizeof(short));
//        while(offset == -1){
//            curEntryIndex++;
//            if (curEntryIndex >= entryCount - 1) {
//                curEntryIndex = 0;
//                curPageNum = nextPageNum;
//            }
//            memcpy(&offset, pageData + PAGE_SIZE - sizeof(short) * (curEntryIndex + 2), sizeof(short));
//        }
        // Set curEntryIndex and curPageNum.
        if (curEntryIndex >= entryCount - 1) {
            curEntryIndex = 0;
            curPageNum = nextPageNum;
        } else {
            curEntryIndex++;
        }

        free(pageData);
        return 0;
    }

    RC IX_ScanIterator::close() {
        if (lowKey != NULL && highKey != NULL &&
            IndexManager::instance().compareKey(this->type, lowKey, highKey) == 0)
            free(lowKey);
        else {
            free(lowKey);
            free(highKey);
        }

        startPageNum = -1;
        startEntryIndex = -1;
        curPageNum = -1;
        curEntryIndex = -1;
        ixFileHandle = nullptr;
        return 0;
    }

    RC
    IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount,
                                       unsigned &appendPageCount) const {
        readPageCount = ixReadPageCounter;
        writePageCount = ixWritePageCounter;
        appendPageCount = ixAppendPageCounter;
        /*readPageCount = fileHandle.readPageCounter;
        writePageCount = fileHandle.writePageCounter;
        appendPageCount = fileHandle.appendPageCounter;*/
        return 0;
    }

    RC BTree::generatePageHeader(Node *node, char *page, short &offset) {
        //Both internal and leaf pages share:
        //Node type, node size.
        memcpy(page + offset, &node->type, sizeof(char)); //internal node or leaf node
        offset += sizeof(char);
        memcpy(page + offset, &node->sizeInPage, sizeof(short));
        offset += sizeof(short);

        return 0;
    }

    RC BTree::setInternalEntryKeyInPage(Node *&node, char *page, short &offset, int i) const {
        //Debug
        int pageNum = node->pageNum;
        int sizeInPage = node->sizeInPage;
        char type = node->type;
        int entrySize = dynamic_cast<InternalNode *>(node)->internalEntries.size();
        int keySize = dynamic_cast<InternalNode *>(node)->internalEntries[i].keySize;
        int leftPageNum = dynamic_cast<InternalNode *>(node)->internalEntries[i].left->pageNum;
        int rightPageNum = dynamic_cast<InternalNode *>(node)->internalEntries[i].right->pageNum;
        //int keyInLeaf;
        //dynamic_cast<LeafNode *>(dynamic_cast<InternalNode *>(node)->internalEntries[i].right)->leafEntries[0].key;
        if (attrType == TypeInt) {
            memcpy(page + offset, dynamic_cast<InternalNode *>(node)->internalEntries[i].key, sizeof(int));
            offset += sizeof(int);
        } else if (attrType == TypeReal) {
            memcpy(page + offset, dynamic_cast<InternalNode *>(node)->internalEntries[i].key, sizeof(float));
            offset += sizeof(float);
        } else if (attrType == TypeVarChar) {
            /*int strLen;
            memcpy(&strLen, dynamic_cast<InternalNode *>(node)->internalEntries[i].key, sizeof(int));*/
            memcpy(page + offset, dynamic_cast<InternalNode *>(node)->internalEntries[i].key,
                   keySize);
            offset += keySize;
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
        PageNum rightPage;
        if (dynamic_cast<LeafNode *>(node)->rightPointer) {
            rightPage = dynamic_cast<LeafNode *>(node)->rightPointer->pageNum;
        } else {
            rightPage = NULLNODE;
        }
        memcpy(page + offset, &rightPage, sizeof(PageNum));
        offset += sizeof(PageNum);

        //set overflow pointer
        PageNum overflowPage;
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
            if (dynamic_cast<LeafNode *>(node)->leafEntries[i].isDeleted == false) {
                //set entry offset
                memcpy(page + PAGE_SIZE - (i + 2) * sizeof(short), &offset, sizeof(short));
                //set entry key
                setLeafEntryKeyInPage(node, page, offset, i);
                //set rid
                PageNum pageNum = dynamic_cast<LeafNode *>(node)->leafEntries[i].rid.pageNum;
                memcpy(page + offset, &dynamic_cast<LeafNode *>(node)->leafEntries[i].rid.pageNum, sizeof(PageNum));
                offset += sizeof(PageNum);
                memcpy(page + offset, &dynamic_cast<LeafNode *>(node)->leafEntries[i].rid.slotNum, sizeof(short));
                offset += sizeof(short);
            } else {
                short deletedFlag = -1;
                //set entry offset
                memcpy(page + PAGE_SIZE - (i + 2) * sizeof(short), &deletedFlag, sizeof(short));
            }
        }

        return 0;
    }

    RC BTree::generatePage(Node *node, char *data) const {
        //Generate the page according to the node information.
        int sizeInPage = node->sizeInPage;
        if (node == nullptr) { return -1; }
        char *page = (char *) calloc(PAGE_SIZE, 1);
        short offset = 0;
        short entryCount;
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

    RC BTree::generateNodeHeader(Node *&res, char *data, short &offset, short &slotCount) {
        //This is a helper function to get some shared information from index file for both internal node and leaf node:
        //type, size and slotCount.
        //Then set isLoaded to true,
        char type;
        memcpy(&type, data + offset, sizeof(char));
        res->type = type;
        offset += sizeof(char);
        //res->type = attrType;
        short size;
        memcpy(&size, data + offset, sizeof(short));
        offset += sizeof(short);
        res->sizeInPage = size;

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

    RC BTree::generateInternalNode(Node *&res, char *data, short &offset, short &slotCount) {
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
            if (slotOffset == -1) {
                LeafEntry deletedEntry;
                deletedEntry.isDeleted = true;
                dynamic_cast<LeafNode *>(res)->leafEntries.push_back(deletedEntry);
            } else {
                short keyLen = 0;
                if (getKeyLen(data, offset, keyLen) == -1) { return -1; }
                RID rid;
                memcpy(&rid.pageNum, data + slotOffset + keyLen, sizeof(PageNum));
                memcpy(&rid.slotNum, data + slotOffset + keyLen + sizeof(PageNum), sizeof(short));
                LeafEntry entry(attrType, data + slotOffset, rid);
                dynamic_cast<LeafNode *>(res)->leafEntries.push_back(entry);
            }
        }
        return 0;
    }

    RC BTree::generateLeafNode(Node *&res, char *data, short &offset, short &slotCount) {
        //This is a helper function to initialize following information of leaf node:
        //right pointer, overflow pointer, entries vector in this node.

        if (getRightPointerInLeafNode(res, data, offset) == -1) { return -1; }

        if (getOverflowPointerInLeafNode(res, data, offset) == -1) { return -1; }

        if (getEntriesInLeafNode(res, data, offset, slotCount) == -1) { return -1; }

        return 0;
    }

    RC BTree::generateNode(char *data, Node *&res) {
        //This is a helper function that loads all the information of a node from index file into memory.
        //These information is stored in node class in memory.
        //Node class can either be internalNode or leafNode.
        //data pointer is pre-loaded with the data of Node in index file.
        res = new Node();
        short offset = 0, slotCount;
        if (generateNodeHeader(res, data, offset, slotCount) == -1) { return -1; }
        if (res->type == INTERNAL) {
            if (generateInternalNode(res, data, offset, slotCount) == -1) { return -1; }
        } else if (res->type == LEAF) {
            LeafNode *temp = new LeafNode();
            temp->type = res->type;
            temp->sizeInPage = res->sizeInPage;
            temp->isLoaded = res->isLoaded;
            delete res;
            res = temp;
            if (generateLeafNode(res, data, offset, slotCount) == -1) { return -1; }
        }
        return 0;
    }

    RC BTree::loadNode(IXFileHandle &ixFileHandle, Node *&node) {
        //This is a helper function that check the given node's pageNum in memory.
        //If it already exits in memory, set the node pointer to point to its address.
        //Otherwise, generate the node, load it into the memory and set the pointer.
        if (node == nullptr) { return -1; }
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

    RC BTree::compareKeyInInternalNode(IXFileHandle &ixFileHandle, const LeafEntry &pair, Node *&node) {
        //This is a helper function to compare key in internal node.
        //If it finds pair's value less than any entry's key in given internal node, it loads the entry's left child, and returns 1.
        //Otherwise, it loads the last entry's right child, and returns 0.
        //Debug
        PeterDB::IndexManager &idm = PeterDB::IndexManager::instance();
        int curNodePageNum = node->pageNum;
        int size = dynamic_cast<InternalNode *>(node)->internalEntries.size();
        if (size == 1) {
            if (idm.compareKey(attrType, pair.key, dynamic_cast<InternalNode *>(node)->internalEntries[0].key) >=
                0) {
                ixFileHandle.ixReadPageCounter++;
                node = dynamic_cast<InternalNode *>(node)->internalEntries[0].right;
            } else {
                ixFileHandle.ixReadPageCounter++;
                node = dynamic_cast<InternalNode *>(node)->internalEntries[0].left;
            }
            if (node->isLoaded == false) {
                if (loadNode(ixFileHandle, node) == -1) { return -1; }
            }
            return 0;
        }
        for (int i = 0; i < size; i++) {
            if (i == size - 1) {
                ixFileHandle.ixReadPageCounter++;
                node = dynamic_cast<InternalNode *>(node)->internalEntries[i].right;
                if (node->isLoaded == false) {
                    if (loadNode(ixFileHandle, node) == -1) { return -1; }
                }
            } else {
                if (idm.compareKey(attrType, pair.key, dynamic_cast<InternalNode *>(node)->internalEntries[i].key) >=
                    0 &&
                    idm.compareKey(attrType, pair.key, dynamic_cast<InternalNode *>(node)->internalEntries[i + 1].key) <
                    0) {
                    ixFileHandle.ixReadPageCounter++;
                    node = dynamic_cast<InternalNode *>(node)->internalEntries[i].right;
                    if (node->isLoaded == false) {
                        if (loadNode(ixFileHandle, node) == -1) { return -1; }
                    }
                    return 0;
                } else if (
                        idm.compareKey(attrType, pair.key, dynamic_cast<InternalNode *>(node)->internalEntries[i].key) <
                        0) {
                    ixFileHandle.ixReadPageCounter++;
                    node = dynamic_cast<InternalNode *>(node)->internalEntries[i].left;
                    if (node->isLoaded == false) {
                        if (loadNode(ixFileHandle, node) == -1) { return -1; }
                    }
                    return 0;
                }
            }
        }
        return 0;
    }

    RC BTree::chooseSubTreeInInternalNode(IXFileHandle &ixFileHandle, const LeafEntry &pair, Node *&node) {
        PeterDB::IndexManager &idm = PeterDB::IndexManager::instance();
        int size = dynamic_cast<InternalNode *>(node)->internalEntries.size();
        if (size == 1) {
            if (idm.compareKey(attrType, pair.key, dynamic_cast<InternalNode *>(node)->internalEntries[0].key) >=
                0) {
                ixFileHandle.ixReadPageCounter++;
                node = dynamic_cast<InternalNode *>(node)->internalEntries[0].right;
            } else {
                ixFileHandle.ixReadPageCounter++;
                node = dynamic_cast<InternalNode *>(node)->internalEntries[0].left;
            }
            if (node->isLoaded == false) {
                if (loadNode(ixFileHandle, node) == -1) { return -1; }
            }
            return 0;
        }
        for (int i = 0; i < size; i++) {
            if (i == size - 1) {
                ixFileHandle.ixReadPageCounter++;
                node = dynamic_cast<InternalNode *>(node)->internalEntries[i].right;
                if (node->isLoaded == false) {
                    if (loadNode(ixFileHandle, node) == -1) { return -1; }
                }
            } else {
                if (idm.compareKey(attrType, pair.key, dynamic_cast<InternalNode *>(node)->internalEntries[i].key) >=
                    0 &&
                    idm.compareKey(attrType, pair.key, dynamic_cast<InternalNode *>(node)->internalEntries[i + 1].key) <
                    0) {
                    ixFileHandle.ixReadPageCounter++;
                    node = dynamic_cast<InternalNode *>(node)->internalEntries[i].right;
                    if (node->isLoaded == false) {
                        if (loadNode(ixFileHandle, node) == -1) { return -1; }
                    }
                    return 0;
                } else if (
                        idm.compareKey(attrType, pair.key, dynamic_cast<InternalNode *>(node)->internalEntries[i].key) <
                        0) {
                    ixFileHandle.ixReadPageCounter++;
                    node = dynamic_cast<InternalNode *>(node)->internalEntries[i].left;
                    if (node->isLoaded == false) {
                        if (loadNode(ixFileHandle, node) == -1) { return -1; }
                    }
                    return 0;
                }
            }
        }
        return 0;
    }

    RC BTree::findLeafNode(IXFileHandle &ixFileHandle, const LeafEntry &pair, Node *&node) {
        while (node->type != LEAF) {
            if (compareKeyInInternalNode(ixFileHandle, pair, node) == -1) {
                return -1;
            }
        }
        return 0;
    }


    RC BTree::initiateNullTree(IXFileHandle &ixFileHandle) {
        //This is a helper function to initiate a null B+ Tree with its only entry pair.
        //It completes following tasks:
        //Creates a new node with the pair entry.
        //Set the B+ Tree parameters.
        //Create a new page with the new node and write it into the index file.
        LeafNode *node = new LeafNode();
        node->isLoaded = true;

        //set tree
        root = node;
        minLeaf = node;

        // Update the root node into Nodemap.
        PageNum newPageNum = nodeMap.size() + 1;
        nodeMap[newPageNum] = node;
        node->pageNum = newPageNum;

        //generate the page and write it into index file.
        char *newPage = (char *) calloc(PAGE_SIZE, 1);
        if (generatePage(node, newPage) == -1) { return -1; }
        if (ixFileHandle.appendPage(newPage) == -1) { return -1; }
        free(newPage);

        //set meta fields
        ixFileHandle.root = newPageNum;
        ixFileHandle.minLeaf = newPageNum;

        return 0;
    }

    RC BTree::writeParentNodeToFile(IXFileHandle &ixFileHandle, InternalNode *&parent) {
        int parentPageNum = parent->pageNum;
        char *updatedParentPage = (char *) calloc(PAGE_SIZE, 1);
        if (generatePage(parent, updatedParentPage) == -1) { return -1; }
        if (ixFileHandle.writePage(dynamic_cast<InternalNode *>(parent)->pageNum, updatedParentPage) ==
            -1) { return -1; }
        free(updatedParentPage);
        return 0;
    }

    bool BTree::checkLeafNodeSpaceForInsertion(LeafNode *L, LeafEntry entry) {
        //This is a helper function to check whether there is enough space on LeafNode L inpage to insert entry.
        //The inserted entry will consume space of keySize, rid and directory in page.
        short newSize = L->sizeInPage + entry.keySize + sizeof(RID) + sizeof(short);
        if (newSize <= PAGE_SIZE) { return true; }
        else { return false; }
    }

    RC BTree::insertEntryInLeafNode(LeafNode *targetNode, IXFileHandle &ixFileHandle, LeafEntry &entry) {
        // This is a helper function to insert entry into leaf node, then write it into index file.
        // Debug

        PeterDB::IndexManager &idm = PeterDB::IndexManager::instance();
        // Insert the entry into targetNode
        bool isAdded = false;
        int insertedIndex = -1;
        // If we find pair's key less than one entry's key, insert it before that entry.
        for (auto i = targetNode->leafEntries.begin(); i < targetNode->leafEntries.end(); i++) {
            if ((*i).isDeleted == true) continue;
            if (idm.compareKey(attrType, entry.key, (*i).key) < 0) {
                if (i != targetNode->leafEntries.begin() && (*(i - 1)).isDeleted) {
                    *(i - 1) = entry;
                    (*(i - 1)).isDeleted = false;
                    isAdded = true;
                    insertedIndex = i - 1 - targetNode->leafEntries.begin();
                    break;
                }
                targetNode->leafEntries.insert(i, entry);
                isAdded = true;
                insertedIndex = i - targetNode->leafEntries.begin();
                break;
            }
        }
        // Otherwise, push it to the back of vector of entries.
        if (isAdded == false) {
            if (targetNode->leafEntries.size() > 0 &&
                targetNode->leafEntries[targetNode->leafEntries.size() - 1].isDeleted) {
                targetNode->leafEntries[targetNode->leafEntries.size() - 1] = entry;
                targetNode->leafEntries[targetNode->leafEntries.size() - 1].isDeleted = false;
                insertedIndex = targetNode->leafEntries.size() - 1;
            } else {
                targetNode->leafEntries.push_back(entry);
                insertedIndex = targetNode->leafEntries.size() - 1;
            }
        }

        int sizeAterInsertion = targetNode->leafEntries.size();
        int insertedPageNum = targetNode->pageNum;
        // This newly inserted entry takes up keySize + RID + directory space (short) in page.
        dynamic_cast<LeafNode *>(targetNode)->sizeInPage += entry.keySize + sizeof(RID) + sizeof(short);

        // Write the updated leaf node into index file.
        char *newPage = (char *) calloc(PAGE_SIZE, 1);

        if (generatePage(targetNode, newPage) == -1) { return -1; }
        if (ixFileHandle.writePage(targetNode->pageNum, newPage) == -1) { return -1; }
        free(newPage);

        return 0;
    }

    RC BTree::setMidToSplit(LeafNode *targetNode, int &mid) {
        //This is helper function to set the mid point in the target leaf node.
        //The mid point is set to include all the entries around mid point with same key before or after mid point.
        PeterDB::IndexManager &idm = PeterDB::IndexManager::instance();
        int entriesNum = targetNode->leafEntries.size();
        mid = entriesNum / 2;
        int end = 0;
        //Search towards end.
        for (int i = mid; i <= entriesNum; i++) {
            if (i == entriesNum) {
                end = 1;
                break;
            }
            LeafEntry curEntry = targetNode->leafEntries[i], prevEntry = targetNode->leafEntries[i - 1];
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
                LeafEntry curEntry = targetNode->leafEntries[i], prevEntry = targetNode->leafEntries[i - 1];
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

        // Update the nodeMap in memory.
        PageNum newPageNum = nodeMap.size() + 1;
        nodeMap[newPageNum] = newNode;
        newNode->pageNum = newPageNum;

        //Update rightPointer of new node and old node.
        newNode->rightPointer = targetNode->rightPointer;
        targetNode->rightPointer = newNode;

        return 0;
    }

    RC BTree::splitLeafNode(IXFileHandle &ixFileHandle, LeafEntry &entry, LeafNode *targetNode, LeafNode *&newNode,
                            InternalEntry *&newChildEntry) {
        //This is a helper function to insert an entry, split leaf node, write both old and new node to index file.

        PeterDB::IndexManager &idm = PeterDB::IndexManager::instance();
        // Insert the entry into targetNode
        bool isAdded = false;
        // If we find pair's key less than one entry's key, insert it before that entry.
        for (auto i = targetNode->leafEntries.begin(); i < targetNode->leafEntries.end(); i++) {
            if (idm.compareKey(attrType, entry.key, (*i).key) < 0) {
                targetNode->leafEntries.insert(i, entry);
                isAdded = true;
                break;
            }
        }
        // Otherwise, push it to the back of vector of entries.
        if (isAdded == false) {
            targetNode->leafEntries.push_back(entry);
        }

        // This newly inserted entry takes up keySize + RID + directory space (short) in page.
        targetNode->sizeInPage += entry.keySize + sizeof(RID) + sizeof(short);

        newNode = new LeafNode();
        newNode->isLoaded = true;

        // There is only one entry in leaf node, we can't split one single entry.
        if (targetNode->leafEntries.size() <= 1) { return -1; }

        // Find the correct mid point for leaf node to split.
        int mid;
        if (setMidToSplit(targetNode, mid) == -1) { return -1; }

        if (createNewSplitLeafNode(newNode, targetNode, mid) == -1) { return -1; }

        // Insert the first key in new node into newChildEntry, set the right pointer of newChildEntry to new node.
        newChildEntry = new InternalEntry(ixFileHandle.bTree->attrType, newNode->leafEntries[0].key, targetNode,
                                          newNode);

        // Write updated targetNode to file.
        char *updatedTargetPage = (char *) calloc(PAGE_SIZE, 1);
        generatePage(targetNode, updatedTargetPage);
        ixFileHandle.writePage(targetNode->pageNum, updatedTargetPage);
        free(updatedTargetPage);

        //std::cout << "Inserting page " << targetNode->pageNum << std::endl;

        // Write new leaf node to file
        char *newLeafPage = (char *) calloc(PAGE_SIZE, 1);
        if (generatePage(newNode, newLeafPage) == -1) { return -1; };
        if (ixFileHandle.appendPage(newLeafPage) == -1) { return -1; };
        free(newLeafPage);

        return 0;
    }

    RC BTree::createNewRoot(IXFileHandle &ixFileHandle, InternalEntry *newChildEntry) {
        // This is a helper function to create a new root node, insert newChildEntry into it, update its sizeInPage
        // update it into nodeMap, and write it into index file.

        InternalNode *newRootNode = new InternalNode();
        newRootNode->isLoaded = true;

        InternalEntry insertEntry(ixFileHandle.bTree->attrType, newChildEntry->key, newChildEntry->left,
                                  newChildEntry->right);
        delete newChildEntry;
        int leftPageNum = insertEntry.left->pageNum;
        int rightPageNum = insertEntry.right->pageNum;
        int leftType = insertEntry.left->type;
        int rightType = insertEntry.right->type;
        //std::cout << "creating new root, left: " << insertEntry.left->pageNum << std::endl;
        //std::cout << "creating new root, right: " << insertEntry.right->pageNum << std::endl;
        newRootNode->internalEntries.push_back(insertEntry);
        // The inserted entry takes up keySize + left pointer pageNum + right pointer pageNum + directory in page.
        int newEntrySizeInPage = insertEntry.keySize + 2 * sizeof(PageNum) + sizeof(short);
        newRootNode->sizeInPage += newEntrySizeInPage;

        // Update it in nodeMap.
        PageNum newPageNum = nodeMap.size() + 1;
        nodeMap[newPageNum] = newRootNode;
        newRootNode->pageNum = newPageNum;

        // Write the new node into index file.
        char *newRootPage = (char *) calloc(PAGE_SIZE, 1);
        if (generatePage(newRootNode, newRootPage) == -1) { return -1; };
        if (ixFileHandle.appendPage(newRootPage) == -1) { return -1; };
        free(newRootPage);

        // Update the root of B+ Tree.
        ixFileHandle.root = newPageNum;
        ixFileHandle.bTree->root = newRootNode;

        // Update the root pointer page.
        if (writeParentNodeToFile(ixFileHandle, newRootNode) == -1) { return -1; }

        return 0;
    }

    bool BTree::checkInternalNodeSpaceForInsertion(InternalNode *N, InternalEntry *newChildEntry) {
        // This is a helper function to check whether Internal node N has enough space to insert newChildEntry.

        // The inserted entry takes up keySize + left pointer pageNum + directory in page.
        short newSize = N->sizeInPage + newChildEntry->keySize + sizeof(PageNum) + sizeof(short);
        if (newSize <= PAGE_SIZE) { return true; }
        else { return false; }
    }

    RC BTree::insertEntryInInternalNode(IXFileHandle ixFileHandle, InternalNode *targetNode,
                                        InternalEntry *newChildEntry) {
        PeterDB::IndexManager &idm = PeterDB::IndexManager::instance();
        char v1 = 0, v2 = 0;
        if (newChildEntry->left->type == LEAF) {
            memcpy(&v1, (char *) dynamic_cast<LeafNode *>(newChildEntry->left)->leafEntries[0].key + sizeof(int), 1);
            memcpy(&v2, (char *) dynamic_cast<LeafNode *>(newChildEntry->right)->leafEntries[0].key + sizeof(int), 1);
        }

        InternalEntry insertEntry(ixFileHandle.bTree->attrType, newChildEntry->key, newChildEntry->left,
                                  newChildEntry->right);
        delete newChildEntry;

        //std::cout << "Inserting internal entry, left: " << insertEntry.left->pageNum << std::endl;
        //std::cout << "Inserting internal entry, right: " << insertEntry.right->pageNum << std::endl;

        bool isAdded = false;
        int addedIndex = 0;
        for (auto i = targetNode->internalEntries.begin(); i != targetNode->internalEntries.end(); i++) {
            if (idm.compareKey(attrType, insertEntry.key, (*i).key) < 0) {
                addedIndex = i - targetNode->internalEntries.begin();
                targetNode->internalEntries.insert(i, insertEntry);
                isAdded = true;
                break;
            }
        }

        // If the entry is to be inserted to the back of vector.
        if (isAdded == false) {
            targetNode->internalEntries.push_back(insertEntry);
            //This newly inserted entry takes up keySize + left pointer + directory space (short) in page.
            int entrySizeInPage = insertEntry.keySize + sizeof(PageNum) + sizeof(short);
            targetNode->sizeInPage += entrySizeInPage;
        } else { // Or it is inserted at middle of the vector
            targetNode->internalEntries[addedIndex + 1].left = insertEntry.right;
            //This newly inserted entry takes up keySize + left pointer + directory space (short) in page.
            int entrySizeInPage = insertEntry.keySize + sizeof(PageNum) + sizeof(short);
            targetNode->sizeInPage += entrySizeInPage;
        }

        // Write the updated leaf node into index file.
        char *newPage = (char *) calloc(PAGE_SIZE, 1);
        if (generatePage(targetNode, newPage) == -1) { return -1; }
        if (ixFileHandle.writePage(targetNode->pageNum, newPage) == -1) { return -1; }
        free(newPage);

        return 0;
    }

    RC BTree::splitInternalNode(IXFileHandle &ixFileHandle, InternalNode *targetNode, InternalNode *&newInternalNode,
                                InternalEntry *&newChildEntry) {
        //This is a helper function to insert newChildEntry into target internal node, split it,  write both old and new node to index file.

        PeterDB::IndexManager &idm = PeterDB::IndexManager::instance();
        InternalEntry insertEntry(ixFileHandle.bTree->attrType, newChildEntry->key, newChildEntry->left,
                                  newChildEntry->right);
        delete newChildEntry;

        bool isAdded = false;
        int addedIndex = 0;
        for (auto i = targetNode->internalEntries.begin(); i != targetNode->internalEntries.end(); i++) {
            if (idm.compareKey(attrType, insertEntry.key, (*i).key) < 0) {
                addedIndex = i - targetNode->internalEntries.begin();
                targetNode->internalEntries.insert(i, insertEntry);
                isAdded = true;
                break;
            }
        }

        // If the entry is to be inserted to the back of vector.
        if (isAdded == false) {
            targetNode->internalEntries.push_back(insertEntry);
            //This newly inserted entry takes up keySize + left pointer + directory space (short) in page.
            int entrySizeInPage = insertEntry.keySize + sizeof(PageNum) + sizeof(short);
            targetNode->sizeInPage += entrySizeInPage;
        } else { // Or it is inserted at middle of the vector
            targetNode->internalEntries[addedIndex + 1].left = insertEntry.right;
            //This newly inserted entry takes up keySize + left pointer + directory space (short) in page.
            int entrySizeInPage = insertEntry.keySize + sizeof(PageNum) + sizeof(short);
            targetNode->sizeInPage += entrySizeInPage;
        }

        newInternalNode = new InternalNode();
        newInternalNode->isLoaded = true;
        newInternalNode->sizeInPage += sizeof(PageNum); // Size for right pointer

        // Move half of entries in old node and update size
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

        // Delete the moved entries old node
        targetNode->internalEntries.erase(targetNode->internalEntries.begin() + internalEntryNum / 2 + 1,
                                          targetNode->internalEntries.end());

        // Set the newChildEntry to the first entry on new node, then erase it from new node.
        newChildEntry = new InternalEntry(attrType, newInternalNode->internalEntries[0].key, targetNode,
                                          newInternalNode);
        newInternalNode->internalEntries.erase(newInternalNode->internalEntries.begin());

        PageNum newPageNum = nodeMap.size() + 1;
        nodeMap[newPageNum] = newInternalNode;
        newInternalNode->pageNum = newPageNum;

        //Write updated parent to file
        char *updatedParentPage = (char *) calloc(PAGE_SIZE, 1);
        if (generatePage(targetNode, updatedParentPage) == -1) { return -1; }
        if (ixFileHandle.writePage(targetNode->pageNum, updatedParentPage) == -1) { return -1; }
        free(updatedParentPage);

        //Write new internal node to file
        char *newInternalPage = (char *) calloc(PAGE_SIZE, 1);
        if (generatePage(newInternalNode, newInternalPage) == -1) { return -1; }
        if (ixFileHandle.appendPage(newInternalPage) == -1) { return -1; }
        free(newInternalPage);

        return 0;
    }

    RC
    BTree::insertEntry(IXFileHandle &ixFileHandle, Node *nodePointer, LeafEntry &entry, InternalEntry *&newChildEntry) {
        if (ixFileHandle.bTree->loadNode(ixFileHandle, nodePointer) == -1) { return -1; }
        if (nodePointer->type == LEAF) {
            char value;
            memcpy(&value, (char *) entry.key + sizeof(int), 1);
            if (checkLeafNodeSpaceForInsertion(dynamic_cast<LeafNode *>(nodePointer), entry)) {
                if (insertEntryInLeafNode(dynamic_cast<LeafNode *>(nodePointer), ixFileHandle, entry) ==
                    -1) { return -1; }
                newChildEntry = nullptr;
                return 0;
            } else {
                LeafNode *newLeafNode;
                if (splitLeafNode(ixFileHandle, entry, reinterpret_cast<LeafNode *&>(nodePointer), newLeafNode,
                                  newChildEntry) == -1) { return -1; }
                //std::cout << "newChildEntry left :" << newChildEntry->left->pageNum << std::endl;
                //std::cout << "newChildEntry right :" << newChildEntry->right->pageNum << std::endl;
                char v1;
                memcpy(&v1, (char *) dynamic_cast<LeafNode *>(newChildEntry->left)->leafEntries[0].key + sizeof(int),
                       1);
                char v2;
                memcpy(&v2, (char *) dynamic_cast<LeafNode *>(newChildEntry->right)->leafEntries[0].key + sizeof(int),
                       1);
                if (ixFileHandle.bTree->root == nodePointer) {
                    if (createNewRoot(ixFileHandle, newChildEntry) == -1) { return -1; }
                }
                return 0;
            }
        } else if (nodePointer->type == INTERNAL) {
            Node *Pi = nodePointer;
            if (chooseSubTreeInInternalNode(ixFileHandle, entry, Pi) == -1) { return -1; }
            //std::cout << "Choosing: " << Pi->pageNum << std::endl;
            if (insertEntry(ixFileHandle, Pi, entry, newChildEntry) == -1) { return -1; }
            if (newChildEntry == nullptr) { return 0; }
            else {
                if (checkInternalNodeSpaceForInsertion(dynamic_cast<InternalNode *>(nodePointer), newChildEntry)) {
                    if (insertEntryInInternalNode(ixFileHandle, dynamic_cast<InternalNode *>(nodePointer),
                                                  newChildEntry) == -1) {
                        return -1;
                    }
                    newChildEntry = nullptr;
                    return 0;
                } else {
                    InternalNode *newInternalNode;
                    if (splitInternalNode(ixFileHandle, dynamic_cast<InternalNode *>(nodePointer), newInternalNode,
                                          newChildEntry) == -1) {
                        return -1;
                    }
                    char v1;
                    memcpy(&v1, (char *) dynamic_cast<InternalNode *>(newChildEntry->left)->internalEntries[0].key +
                                sizeof(int), 1);
                    char v2;
                    memcpy(&v2, (char *) dynamic_cast<InternalNode *>(newChildEntry->right)->internalEntries[0].key +
                                sizeof(int), 1);
                    if (ixFileHandle.bTree->root == nodePointer) {
                        if (createNewRoot(ixFileHandle, newChildEntry) == -1) { return -1; }
                    }
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
        for (auto entry : leafEntries) {
            free(entry.key);
        }
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
        for (auto entry : internalEntries) {
            free(entry.key);
        }
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
        //sizeInPage consists of type(char), sizeInPage(short), slotCount(short)
        sizeInPage = sizeof(char) + sizeof(short) + sizeof(short);
        parent = nullptr;
        pageNum = -1;
        isDirty = false;
        isLoaded = false;
    }

    LeafNode::LeafNode() {
        type = LEAF;
        //sizeInPage consists of type(char), sizeInPage(short),rightPointer(PageNum), overflowPointer(PageNum), slotCount(short)
        sizeInPage = sizeof(char) + sizeof(short) + 2 * sizeof(PageNum) + sizeof(short);
        parent = nullptr;
        pageNum = -1;
        isDirty = false;
        isLoaded = false;
        rightPointer = nullptr;
        overflowPointer = nullptr;
    }

    LeafEntry::LeafEntry() {
        key = nullptr;
        isDeleted = false;
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
        isDeleted = false;
    }

    LeafEntry::~LeafEntry() {
        //free(key);
        key = nullptr;
        isDeleted = false;
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
            int strLen;
            memcpy(&strLen, key, sizeof(int));
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
        //free(key);
        key = nullptr;
        left = nullptr;
        right = nullptr;
        keySize = 0;
    }
} // namespace PeterDB