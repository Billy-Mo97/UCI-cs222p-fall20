#ifndef _ix_h_
#define _ix_h_
#include "unordered_map"
#include <vector>
#include <string>

#include "pfm.h"
#include "rbfm.h" // for some type declarations only, e.g., RID and Attribute

# define IX_EOF (-1)  // end of the index scan
#define NULLNODE -1
#define INTERNAL 1
#define LEAF 2

namespace PeterDB {
    class IX_ScanIterator;

    class IXFileHandle;
    class Node;
    class IndexManager {

    public:
        static IndexManager &instance();
        int compareKey(AttrType attrType,const void* k1, const void* k2) const;
        // Create an index file.
        RC createFile(const std::string &fileName);

        // Delete an index file.
        RC destroyFile(const std::string &fileName);

        // Open an index and return an ixFileHandle.
        RC openFile(const std::string &fileName, IXFileHandle &ixFileHandle);

        // Close an ixFileHandle for an index.
        RC closeFile(IXFileHandle &ixFileHandle);

        // Insert an entry into the given index that is indicated by the given ixFileHandle.
        RC insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixFileHandle.
        RC deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixFileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        RC printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const;

    protected:
        IndexManager() = default;                                                   // Prevent construction
        ~IndexManager() = default;                                                  // Prevent unwanted destruction
        IndexManager(const IndexManager &) = default;                               // Prevent construction by copying
        IndexManager &operator=(const IndexManager &) = default;                    // Prevent assignment

        RC BFS(IXFileHandle &ixfileHandle, Node *node, const Attribute &attribute, std::ostream &out) const;
    };

    class IX_ScanIterator {
    public:

        // Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

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
        BTree* bTree;
        // Constructor
        IXFileHandle();
        // Destructor
        ~IXFileHandle();
        // Put the current counter values of associated PF FileHandles into variables
        RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);
        RC readHiddenPage();
        RC writeHiddenPage();
        RC readPage(PageNum pageNum, void *data);
        RC writePage(PageNum pageNum, const void *data);
        RC appendPage(const void *data);
    };
    class Node{
    public:
        char type;
        short size;
        Node* parent;
        PageNum pageNum;
        bool isDirty;
        bool isLoaded;
        Node();
        virtual ~Node();
        bool isOverflow();
        bool isUnderflow();
    };

    class LeafEntry{
    public:
        void* key;
        RID rid;
        short size;
        LeafEntry();
        LeafEntry(const AttrType &attrType, const void* key, const RID rid);
        ~LeafEntry();
    };

    class InternalEntry{
    public:
        void* key;
        Node* left;
        Node* right;
        short size;
        InternalEntry();
        InternalEntry(const AttrType &attrType, const void* key);
        InternalEntry(const AttrType &attribute, const void* key, Node* leftChild, Node* rightChild);
        ~InternalEntry();
    };

    class InternalNode : public Node{
    public:
        std::vector<InternalEntry> internalEntries;
        InternalNode();
        ~InternalNode();
    };

    class LeafNode : public Node{
    public:
        Node* rightPointer;
        Node* overflowPointer;
        std::vector<LeafEntry> leafEntries;
        LeafNode();
        ~LeafNode();
    };

    class BTree{
    public:
        Node* root;
        Node* minLeaf;
        AttrType attrType;
        std::unordered_map<PageNum, Node*> nodeMap;
        BTree();
        ~BTree();
        RC insertEntry(IXFileHandle &ixfileHandle, const LeafEntry &pair);
        RC loadNode(IXFileHandle &ixfileHandle, Node* &node);
        Node* findLeafNode(IXFileHandle &ixfileHandle, const LeafEntry &pair, Node* curNode);
        RC generatePage(Node* node, char* data);
        Node* generateNode(char* data);
        //int compareKey(void* v1, void* v2);
//        RC deleteEntry(IXFileHandle &ixfileHandle, const LeafEntry &pair);
//        RC findRecord(IXFileHandle &ixfileHandle, const LeafEntry &pair, std::vector<LeafEntry>::iterator &result);
//        RC findLeaf(IXFileHandle &ixfileHandle, const LeafEntry &pair, LeafNode** &result);

//        int compareEntry(const LeafEntry &pair1, const LeafEntry &pair2);
//        RC doDelete(IXFileHandle &ixfileHandle, Node** node, const LeafEntry &pair);
//        RC removeEntryFromNode(Node** node, const LeafEntry &pair);
//        short getEntrySize(int nodeType, const LeafEntry &pair, bool isLastEntry);
//        RC adjustRoot(IXFileHandle &ixfileHandle);
//        RC getNeighborIndex(Node** node, int &result);
//        RC getNodesMergeSize(Node** node1, Node** node2, short sizeOfParentKey, short &result);
//        short getKeySize(const void* key);
//        RC mergeNodes(IXFileHandle &ixfileHandle, Node** node, Node** neighbor, int neighborIndex, int keyIndex, short mergedNodeSize);
//        RC redistributeNodes(Node** node, Node** neighbor, int neighborIndex, int keyIndex, short keySize);
//        RC refreshNodeSize(Node** node);
//        RC writeNodesBack(IXFileHandle &ixfileHandle);
    };

}// namespace PeterDB
#endif // _ix_h_
