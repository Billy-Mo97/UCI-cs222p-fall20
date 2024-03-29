#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>

#include "src/include/rbfm.h"

namespace PeterDB {
#define RM_EOF (-1)  // end of a scan operator

    typedef enum {
        System = 0, User = 1
    } TableFlagType;

    // RM_ScanIterator is an iterator to go through tuples
    class RM_ScanIterator {
    public:
        RBFM_ScanIterator rbfm_ScanIterator;

        RM_ScanIterator();

        ~RM_ScanIterator();

        // "data" follows the same format as RelationManager::insertTuple()
        RC getNextTuple(RID &rid, void *data);

        RC close();
    };

    // Relation Manager
    class RelationManager {
    public:
        static RelationManager &instance();

        void prepareTableAttribute(std::vector<Attribute> &tableAttributeDescriptor, std::string tableName,
                                   std::string fileName, int &tableDataSize);

        void prepareTableData(int tableId, std::string tableName, std::string fileName, int tableFlag,
                              std::vector<Attribute> &tableAttributeDescriptor, void *tableData);

        void prepareColumnAttribute(std::vector<Attribute> &columnAttributeDescriptor, std::string columnName, int &columnDataSize);

        void prepareColumnData(int tableId, std::string columnName, AttrType columnType, int columnLength,
                               int columnPosition, int tableFlag, std::vector<Attribute> &columnAttributeDescriptor, void *columnData);

        RC checkCatalog();

        RC createCatalog();

        RC deleteCatalog();

        RC createTable(const std::string &tableName, const std::vector<Attribute> &attrs);

        RC deleteTable(const std::string &tableName);

        RC getTableInfo(const std::string &tableName, int &tableId, std::string &fileName);

        RC getAttributes(const std::string &tableName, std::vector<Attribute> &attrs);

        RC insertTuple(const std::string &tableName, const void *data, RID &rid);

        RC deleteTuple(const std::string &tableName, const RID &rid);

        RC updateTuple(const std::string &tableName, const void *data, const RID &rid);

        RC readTuple(const std::string &tableName, const RID &rid, void *data);

        // Print a tuple that is passed to this utility method.
        // The format is the same as printRecord().
        RC printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out);

        RC readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName, void *data);

        // Scan returns an iterator to allow the caller to go through the results one by one.
        // Do not store entire results in the scan iterator.
        RC scan(const std::string &tableName,
                const std::string &conditionAttribute,
                const CompOp compOp,                  // comparison type such as "<" and "="
                const void *value,                    // used in the comparison
                const std::vector<std::string> &attributeNames, // a list of projected attributes
                RM_ScanIterator &rm_ScanIterator);

        // Extra credit work (10 points)
        RC addAttribute(const std::string &tableName, const Attribute &attr);

        RC dropAttribute(const std::string &tableName, const std::string &attributeName);

    protected:
        RelationManager();                                                  // Prevent construction
        ~RelationManager();                                                 // Prevent unwanted destruction
        RelationManager(const RelationManager &);                           // Prevent construction by copying
        RelationManager &operator=(const RelationManager &);                // Prevent assignment

    };

} // namespace PeterDB

#endif // _rm_h_