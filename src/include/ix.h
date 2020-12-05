#ifndef _ix_h_
#define _ix_h_

#include "unordered_map"
#include <vector>
#include <string>

#include "pfm.h"
#include "rbfm.h" // for some type declarations only, e.g., RID and Attribute

# define IX_EOF (-1)  // end of the index scan
# define NULLNODE -1
# define INTERNAL 1
# define LEAF 2

namespace PeterDB {
    class IX_ScanIterator;

    class IXFileHandle;

    class Node;

    class IndexManager {

    public:
        static IndexManager &instance();

        static int compareKey(AttrType attrType, const void *k1, const void *k2) ;

        // Create an index file.
        RC createFile(const std::string &fileName);

        // Delete an index file.
        RC destroyFile(const std::string &fileName);

        // Open an index and return an ixFileHandle.
        RC openFile(const std::string &fileName, IXFileHandle &ixFileHandle);

        // Close an ixFileHandle for an index.
        RC closeFile(IXFileHandle &ixFileHandle);

        // Set root of B+ Tree.
        RC setRoot(IXFileHandle &ixFileHandle);

        // Set minLeaf of B+ Tree.
        RC setMinLeaf(IXFileHandle &ixFileHandle);

        // Get root of B+ Tree from root pointer page.
        RC getRootAndMinLeaf(IXFileHandle &ixFileHandle);

        // Insert an entry into the given index that is indicated by the given ixFileHandle.
        RC insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixFileHandle.
        RC deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Find the inclusive start entry of scanning in given leaf node.
        RC findInclusiveStartEntry(AttrType type, const void *lowKey, IX_ScanIterator &ix_ScanIterator, Node *&targetNode, bool &startFound);

        // Find the exclusive start entry of scanning in given leaf node.
        RC findExclusiveStartEntry(AttrType type, const void *lowKey, IX_ScanIterator &ix_ScanIterator, Node *&targetNode, bool &startFound);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixFileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        RC printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out);

    protected:
        IndexManager() = default;                                                   // Prevent construction
        ~IndexManager() = default;                                                  // Prevent unwanted destruction
        IndexManager(const IndexManager &) = default;                               // Prevent construction by copying
        IndexManager &operator=(const IndexManager &) = default;                    // Prevent assignment

        static RC printLeafKey(int start, int i, Node *node, const Attribute &attribute,
                             std::ostream &out) ;

        RC leafDFS(IXFileHandle &ixFileHandle, Node *node, const Attribute &attribute, std::ostream &out) const;

        static RC printInternalNode(IXFileHandle &ixFileHandle, Node *node, const Attribute &attribute, std::ostream &out) ;

        RC DFS(IXFileHandle &ixFileHandle, Node *node, const Attribute &attribute, std::ostream &out) const;
    };

    class IX_ScanIterator {
    public:
        AttrType type;
        void *lowKey;
        void *highKey;
        bool lowKeyInclusive;
        bool highKeyInclusive;
        PageNum startPageNum = -1;
        int startEntryIndex = -1;
        PageNum curPageNum = -1;
        int curEntryIndex = -1;
        //FileHandle fileHandle;
        IXFileHandle *ixFileHandle;

        // Interface
        RC setValues(const void *lowKey, const void *highKey, bool lowKeyInclusive, bool highKeyInclusive, IXFileHandle *ixFileHandle);

        // Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get header information from leaf page in index file;
        RC getLeafHeaderFromPage(char* pageData, char &nodeType, short &nodeSize, PageNum &rightPageNum, PageNum &overflowPageNum);

        // Get the key length of entry.
        RC getKeyLen(char *pageData, short &offset, short &keyLen);

        // Get leaf entry from leaf page in index file.
        int getLeafEntryFromPage(char* pageData, PageNum &rightPageNum, short &entryCount, RID &rid, void *key);

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();
    };

    class BTree;

    class IXFileHandle {
    public:
        // variables to keep counter for each operation
        unsigned ixReadPageCounter;
        unsigned ixWritePageCounter;
        unsigned ixAppendPageCounter;
        unsigned numOfPages;
        FileHandle fileHandle;
        int root;
        int minLeaf;
        BTree *bTree;
        std::string fileName;

        // Constructor
        IXFileHandle();

        // Destructor
        ~IXFileHandle();

        // Put the current counter values of associated PF FileHandles into variables
        RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) const;

        RC readHiddenPage();

        RC readRootPointerPage();

        RC writeHiddenPage();

        RC readPage(PageNum pageNum, void *data);

        RC writePage(PageNum pageNum, const void *data);

        RC appendPage(const void *data);

        RC appendRootPage(const Attribute &attribute);

        RC writeRootPointerPage();
    };

    class Node {
    public:
        char type;
        short sizeInPage;
        Node *parent;
        PageNum pageNum;
        bool isDirty;
        bool isLoaded;

        Node();

        virtual ~Node();

        bool isOverflow();

        bool isUnderflow();
    };

    class LeafEntry {
    public:
        void *key;
        RID rid;
        short keySize;
        bool isDeleted;
        LeafEntry();

        LeafEntry(const AttrType &attrType, const void *key, const RID rid);

        ~LeafEntry();
    };

    class InternalEntry {
    public:
        void *key;
        Node *left;
        Node *right;
        short keySize;

        InternalEntry();

        InternalEntry(const AttrType &attrType, const void *key);

        InternalEntry(const AttrType &attribute, const void *key, Node *leftChild, Node *rightChild);

        ~InternalEntry();
    };

    class InternalNode : public Node {
    public:
        std::vector<InternalEntry> internalEntries;

        InternalNode();

        ~InternalNode();
    };

    class LeafNode : public Node {
    public:
        Node *rightPointer;
        Node *overflowPointer;
        std::vector<LeafEntry> leafEntries;

        LeafNode();

        ~LeafNode();
    };

    class BTree {
    public:
        Node *root;
        Node *minLeaf;
        AttrType attrType;
        int numOfNodes;
        std::unordered_map<PageNum, Node *> nodeMap;

        BTree();

        ~BTree();
        RC deleteEntryInFile(IXFileHandle &ixFileHandle, Node *targetNode,int i);
        RC deleteEntry(IXFileHandle &ixFileHandle, const LeafEntry &pair);
        bool isEntryDeleted(IXFileHandle &ixFileHandle, Node *targetNode,int i);
        RC generateRootNode(LeafNode *&res);

        RC initiateNullTree(IXFileHandle &ixFileHandle);

        RC insertEntryInLeafNode(LeafNode *targetNode, IXFileHandle &ixFileHandle, LeafEntry &entry);

        RC writeLeafNodeToFile(IXFileHandle &ixFileHandle, Node *targetNode);

        RC setMidToSplit(LeafNode *targetNode, int &mid);

        RC createNewSplitLeafNode(LeafNode *newNode, LeafNode *targetNode, int &mid);

        RC splitLeafNode(IXFileHandle &ixFileHandle, LeafEntry &entry, LeafNode *targetNode, LeafNode *&newNode, InternalEntry *&newChildEntry);

        //RC createParentForSplitLeafNode(IXFileHandle &ixFileHandle, Node *&targetNode, LeafNode *&newNode);

        RC writeParentNodeToFile(IXFileHandle &ixFileHandle, InternalNode *&parent);

        RC splitInternalNode(IXFileHandle &ixFileHandle, InternalNode *targetNode, InternalNode *&newInternalNode, InternalEntry *&newChildEntry);

        bool checkLeafNodeSpaceForInsertion(LeafNode *L, LeafEntry entry);

        //RC setNewChildEntryInLeaf(LeafNode *L2, InternalEntry *newChildEntry);

        RC createNewRoot(IXFileHandle &ixFileHandle, InternalEntry *newChildEntry);

        bool checkInternalNodeSpaceForInsertion(InternalNode *N, InternalEntry *newChildEntry);

        RC insertEntryInInternalNode(IXFileHandle ixFileHandle, InternalNode *targetNode, InternalEntry *newChildEntry);

        RC insertEntry(IXFileHandle &ixFileHandle, Node *nodePointer, LeafEntry &entry, InternalEntry *&newChildEntry);

        RC loadNode(IXFileHandle &ixFileHandle, Node *&node);

        RC compareKeyInInternalNode(IXFileHandle &ixFileHandle, const LeafEntry &pair, Node *&node);

        RC chooseSubTreeInInternalNode(IXFileHandle &ixFileHandle, const LeafEntry &pair, Node *&node);

        RC findLeafNode(IXFileHandle &ixFileHandle, const LeafEntry &pair, Node *&curNode);

        static RC generatePageHeader(Node *node, char *page, short &offset);

        RC setInternalEntryKeyInPage(Node *&node, char *page, short &offset, int i) const;

        RC generateInternalPage(Node *node, char *page, short &offset) const;

        RC setLeafEntryKeyInPage(Node *node, char *page, short &offset, int i) const;

        RC generateLeafPage(Node *node, char *page, short &offset) const;

        RC generatePage(Node *node, char *data) const;

        RC generateNodeHeader(Node *&res, char *data, short &offset, short &slotCount);

        RC getKeyLen(char *data, short &slotOffset, short &keyLen) const;

        RC getLeftInternalEntry(InternalEntry &entry, const char *data, short slotOffset);

        RC getRightInternalEntry(InternalEntry &entry, char *data, short slotOffset, short keyLen);

        RC generateInternalNode(Node *&res, char *data, short &offset, short &slotCount);

        RC getRightPointerInLeafNode(Node *res, char *data, short &offset);

        RC getOverflowPointerInLeafNode(Node *res, char *data, short &offset);

        RC getEntriesInLeafNode(Node *res, char *data, short &offset, short &slotCount);

        RC generateLeafNode(Node *&res, char *data, short &offset, short &slotCount);

        RC generateNode(char *data, Node *&res);

        RC getMaxLeaf(LeafNode *maxLeafNode);
    };

}// namespace PeterDB
#endif // _ix_h_
