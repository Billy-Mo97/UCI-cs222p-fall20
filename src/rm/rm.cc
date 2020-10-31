#include "src/include/rm.h"
#include <map>

namespace PeterDB {
    RelationManager &RelationManager::instance() {
        static RelationManager _relation_manager = RelationManager();
        return _relation_manager;
    }

    RelationManager::RelationManager() = default;

    RelationManager::~RelationManager() = default;

    RelationManager::RelationManager(const RelationManager &) = default;

    RelationManager &RelationManager::operator=(const RelationManager &) = default;

    void RelationManager::prepareTableAttribute(std::vector<Attribute> &tableAttributeDescriptor, int &tableDataSize) {
        //Prepare record descriptor to insert into "Tables" table.
        //This function only needs to be called once.
        Attribute tableIdAttr;
        tableIdAttr.name = "table-id";
        tableIdAttr.type = TypeInt;
        tableIdAttr.length = sizeof(int);
        Attribute tableNameAttr;
        tableNameAttr.name = "table-name";
        tableNameAttr.type = TypeVarChar;
        tableNameAttr.length = 50;
        Attribute fileNameAttr;
        fileNameAttr.name = "file-name";
        fileNameAttr.type = TypeVarChar;
        fileNameAttr.length = 50;
        Attribute tableFlagAttr;
        tableFlagAttr.name = "table-flag";
        tableFlagAttr.type = TypeInt;
        tableFlagAttr.length = sizeof(int);
        //Set the data size of table data.
        tableAttributeDescriptor = {tableIdAttr, tableNameAttr, fileNameAttr};
        int nullFieldsIndicatorSize = ceil(tableAttributeDescriptor.size() / 8.0);
        tableDataSize = nullFieldsIndicatorSize + sizeof(int) * 3 + 50 * 2;
        return;
    }

    void RelationManager::prepareTableData(int tableId, std::string tableName, std::string fileName, TableFlagType tableFlag,
                                           std::vector<Attribute> &tableAttributeDescriptor, void *tableData) {
        //Prepare the data and record descriptor to insert into "Tables" table.
        int nullFieldsIndicatorSize = ceil(tableAttributeDescriptor.size() / 8.0);
        unsigned char nullsIndicator[nullFieldsIndicatorSize];
        memset(nullsIndicator, 0, nullFieldsIndicatorSize);
        nullsIndicator[0] = 31; // 00011111
        int dataOffset = 0;
        memcpy(tableData, nullsIndicator, nullFieldsIndicatorSize);
        dataOffset += nullFieldsIndicatorSize;
        memcpy((char *)tableData + dataOffset, &tableId, sizeof(int));
        dataOffset += sizeof(int);
        int tableNameLen = 50;
        memcpy((char *)tableData + dataOffset, &tableNameLen, sizeof(int));
        dataOffset += sizeof(int);
        char tableName_cstr[50];
        memset(tableName_cstr, 0, 50);
        memcpy(tableName_cstr, tableName.c_str(), tableName.size());
        memcpy((char *) tableData + dataOffset, tableName_cstr, tableNameLen);
        dataOffset += tableNameLen;
        int fileNameLen = 50;
        memcpy((char *)tableData + dataOffset, &fileNameLen, sizeof(int));
        dataOffset += sizeof(int);
        char fileName_cstr[50];
        memset(fileName_cstr, 0, 50);
        memcpy(fileName_cstr, fileName.c_str(), fileName.size());
        memcpy((char *)tableData + dataOffset, fileName_cstr, fileNameLen);
        dataOffset += fileNameLen;
        memcpy((char *)tableData + dataOffset, &tableFlag, sizeof(int));
        dataOffset += sizeof(int);
        return;
    }

    void RelationManager::prepareColumnAttribute(std::vector<Attribute> &columnAttributeDescriptor,
                                                 int &columnDataSize) {
        //Prepare record descriptor to insert into "Column" table.
        //This function only needs to be called once.
        Attribute tableIdAttr;
        tableIdAttr.name = "table-id";
        tableIdAttr.type = TypeInt;
        tableIdAttr.length = sizeof(int);
        Attribute columnNameAttr;
        columnNameAttr.name = "column-name";
        columnNameAttr.type = TypeVarChar;
        columnNameAttr.length = 50;
        Attribute columnTypeAttr;
        columnTypeAttr.name = "column-type";
        columnTypeAttr.type = TypeVarChar;
        columnTypeAttr.name = 50;
        Attribute columnLengthAttr;
        columnLengthAttr.name = "column-length";
        columnLengthAttr.type = TypeInt;
        columnLengthAttr.length = sizeof(int);
        Attribute columnPositionAttr;
        columnPositionAttr.name = "column-position";
        columnPositionAttr.type = TypeInt;
        columnPositionAttr.length = sizeof(int);
        Attribute tableFlagAttr;
        tableFlagAttr.name = "table-flag";
        tableFlagAttr.type = TypeInt;
        tableFlagAttr.length = sizeof(int);
        columnAttributeDescriptor = {tableIdAttr, columnNameAttr,columnTypeAttr,
                                     columnLengthAttr, columnPositionAttr, tableFlagAttr};
        int nullFieldsIndicatorSize = ceil(columnAttributeDescriptor.size() / 8.0);
        columnDataSize = nullFieldsIndicatorSize + sizeof(int) * 5 + 50 * 2;
        return;
    }

    void RelationManager::prepareColumnData(int tableId, std::string columnName, AttrType columnType, int columnLength, int columnPosition,
                                            TableFlagType tableFlag, std::vector<Attribute> &columnAttributeDescriptor,
                                            void *columnData) {
        //Prepare the data to insert into "Columns" table.
        int nullFieldsIndicatorSize = ceil(columnAttributeDescriptor.size() / 8.0);
        unsigned char nullsIndicator[nullFieldsIndicatorSize];
        memset(nullsIndicator, 0, nullFieldsIndicatorSize);
        nullsIndicator[0] = 7; // 00000111
        memcpy(columnData, nullsIndicator, nullFieldsIndicatorSize);
        int dataOffset = nullFieldsIndicatorSize;
        memcpy((char *)columnData + dataOffset, &tableId, sizeof(int));
        dataOffset += sizeof(int);
        int columnNameLen = 50;
        memcpy((char *)columnData + dataOffset, &columnNameLen, sizeof(int));
        dataOffset += sizeof(int);
        char columnName_cstr[50];
        memset(columnName_cstr, 0, 50);
        memcpy(columnName_cstr, columnName.c_str(), columnName.size());
        memcpy((char *)columnData + dataOffset, columnName_cstr, columnNameLen);
        dataOffset += columnNameLen;
        memcpy((char *)columnData + dataOffset, &columnType, sizeof(int));
        dataOffset += sizeof(int);
        memcpy((char *)columnData + dataOffset, &columnLength, sizeof(int));
        dataOffset += sizeof(int);
        memcpy((char *)columnData + dataOffset, &columnPosition, sizeof(int));
        dataOffset += sizeof(int);
        memcpy((char *)columnData + dataOffset, &tableFlag, sizeof(int));
        dataOffset += sizeof(int);
        return;
    }

    RC RelationManager::createCatalog() {
        PeterDB::RecordBasedFileManager &rbfm = PeterDB::RecordBasedFileManager::instance();
        //Create "Tables"
        if(rbfm.createFile("Tables") == -1) { return -1; }

        //Prepare a tuple of "Tables", insert it into "Tables" table.
        void *tableData = NULL;
        std::vector<Attribute> tableAttributeDescriptor;
        int tableDataSize;
        prepareTableAttribute(tableAttributeDescriptor, tableDataSize);
        tableData = malloc(tableDataSize);
        prepareTableData(1, "Tables", "Tables", System,
                         tableAttributeDescriptor, tableData);
        PeterDB::FileHandle fileHandle;
        if(rbfm.openFile("Tables", fileHandle) == -1) { return -1; }
        RID rid;
        if(rbfm.insertRecord(fileHandle, tableAttributeDescriptor, tableData, rid) == -1) { return -1; }
        free(tableData);
        tableData = NULL;
        tableAttributeDescriptor.clear();
        if(rbfm.closeFile(fileHandle) == -1) { return -1; }

        //Create "Columns"
        if(rbfm.createFile("Columns") == -1) { return -1; }

        //Prepare a tuple of "Columns", insert it into "Tables" table.
        tableData = malloc(tableDataSize);
        prepareTableData(2, "Columns", "Columns", System,
                         tableAttributeDescriptor, tableData);
        if(rbfm.openFile("Tables", fileHandle) == -1) { return -1; }
        if(rbfm.insertRecord(fileHandle, tableAttributeDescriptor, tableData, rid) == -1) { return -1; }
        if(rbfm.closeFile(fileHandle) == -1) { return -1; }
        free(tableData);
        tableData = NULL;
        //Prepare 4 tuples of "Tables", insert it into "Columns" table.
        if(rbfm.openFile("Columns", fileHandle) == -1) { return -1; }
        void *table_ColumnData1 = NULL, *table_ColumnData2 = NULL, *table_ColumnData3 = NULL, *table_ColumnData4 = NULL;
        std::vector<Attribute> columnAttributeDescriptor;
        int columnDataSize;
        prepareColumnAttribute(columnAttributeDescriptor, columnDataSize);
        table_ColumnData1 = malloc(columnDataSize);
        table_ColumnData2 = malloc(columnDataSize);
        table_ColumnData3 = malloc(columnDataSize);
        table_ColumnData4 = malloc(columnDataSize);
        prepareColumnData(1, "table-id", TypeInt, 4,
                          1, System, columnAttributeDescriptor, table_ColumnData1);
        prepareColumnData(1, "table-name", TypeVarChar, 50,
                          2, System, columnAttributeDescriptor, table_ColumnData2);
        prepareColumnData(1, "fileName", TypeVarChar, 50,
                          3, System, columnAttributeDescriptor, table_ColumnData3);
        prepareColumnData(1, "table-flag", TypeInt, 4,
                          4, System, columnAttributeDescriptor, table_ColumnData4);
        if(rbfm.insertRecord(fileHandle, columnAttributeDescriptor, table_ColumnData1, rid) == -1) { return -1; }
        if(rbfm.insertRecord(fileHandle, columnAttributeDescriptor, table_ColumnData2, rid) == -1) { return -1; }
        if(rbfm.insertRecord(fileHandle, columnAttributeDescriptor, table_ColumnData3, rid) == -1) { return -1; }
        if(rbfm.insertRecord(fileHandle, columnAttributeDescriptor, table_ColumnData4, rid) == -1) { return -1; }
        free(table_ColumnData1);
        free(table_ColumnData2);
        free(table_ColumnData3);
        free(table_ColumnData4);
        table_ColumnData1 = NULL;
        table_ColumnData2 = NULL;
        table_ColumnData3 = NULL;
        table_ColumnData4 = NULL;

        //Prepare 6 tuples of "Columns", insert it into "Columns" table.
        void *column_ColumnData1 = NULL, *column_ColumnData2 = NULL, *column_ColumnData3 = NULL,
                *column_ColumnData4 = NULL, *column_ColumnData5 = NULL, *column_ColumnData6 = NULL;
        column_ColumnData1 = malloc(columnDataSize);
        column_ColumnData2 = malloc(columnDataSize);
        column_ColumnData3 = malloc(columnDataSize);
        column_ColumnData4 = malloc(columnDataSize);
        column_ColumnData5 = malloc(columnDataSize);
        column_ColumnData6 = malloc(columnDataSize);
        prepareColumnData(2, "table-id", TypeInt, 4,
                          1, System, columnAttributeDescriptor, column_ColumnData1);
        prepareColumnData(2, "column-name", TypeVarChar, 50,
                          2, System, columnAttributeDescriptor, column_ColumnData2);
        prepareColumnData(2, "column-type", TypeInt, 4,
                          3, System, columnAttributeDescriptor, column_ColumnData3);
        prepareColumnData(2, "column-length", TypeInt, 4,
                          4, System, columnAttributeDescriptor, column_ColumnData4);
        prepareColumnData(2, "column-position", TypeInt, 4,
                          5, System, columnAttributeDescriptor, column_ColumnData5);
        prepareColumnData(2, "table-flag", TypeInt, 4,
                          6, System, columnAttributeDescriptor, column_ColumnData6);
        if(rbfm.insertRecord(fileHandle, columnAttributeDescriptor, column_ColumnData1, rid) == -1) { return -1; }
        if(rbfm.insertRecord(fileHandle, columnAttributeDescriptor, column_ColumnData2, rid) == -1) { return -1; }
        if(rbfm.insertRecord(fileHandle, columnAttributeDescriptor, column_ColumnData3, rid) == -1) { return -1; }
        if(rbfm.insertRecord(fileHandle, columnAttributeDescriptor, column_ColumnData4, rid) == -1) { return -1; }
        if(rbfm.insertRecord(fileHandle, columnAttributeDescriptor, column_ColumnData5, rid) == -1) { return -1; }
        if(rbfm.insertRecord(fileHandle, columnAttributeDescriptor, column_ColumnData6, rid) == -1) { return -1; }
        if(rbfm.closeFile(fileHandle) == -1) { return -1; }
        free(column_ColumnData1);
        free(column_ColumnData2);
        free(column_ColumnData3);
        free(column_ColumnData4);
        free(column_ColumnData5);
        free(column_ColumnData6);
        column_ColumnData1 = NULL;
        column_ColumnData2 = NULL;
        column_ColumnData3 = NULL;
        column_ColumnData4 = NULL;
        column_ColumnData5 = NULL;
        column_ColumnData6 = NULL;
        return 0;
    }

    RC RelationManager::deleteCatalog() {
        PeterDB::RecordBasedFileManager &rbfm = PeterDB::RecordBasedFileManager::instance();
        if(rbfm.destroyFile("Tables") == -1) { return -1; }
        if(rbfm.destroyFile("Columns") == -1) { return -1; }
        return 0;
    }

    RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
        PeterDB::RecordBasedFileManager &rbfm = PeterDB::RecordBasedFileManager::instance();
        PeterDB::FileHandle fileHandle;

        //First, scan the table to find the maximum id
        RM_ScanIterator rmsi;
        std::vector<std::string> tableIdAttr = {"table-id"};
        if(scan("Tables", "table-id",
                NO_OP, NULL, tableIdAttr, rmsi) == -1) { return -1; }
        int nullIndicatorSize = ceil(tableIdAttr.size() / 8.0);
        void *returnedData = malloc(nullIndicatorSize + sizeof(int));
        RID rid;
        int maxId = 0;
        while(rmsi.getNextTuple(rid, returnedData) != RM_EOF) {
            int dataOffset = nullIndicatorSize;
            int currentId;
            memcpy(&currentId, (char *) returnedData + dataOffset, sizeof(int));
            maxId = maxId > currentId ? maxId : currentId;
        }
        free(returnedData);
        returnedData = NULL;

        //Then, open "Tables" file, prepare the data to insert to "Tables" table
        if(rbfm.openFile("Tables", fileHandle) == -1) { return -1; }
        void *tableData = NULL;
        std::vector<Attribute> tableAttributeDescriptor;
        int tableDataSize;
        prepareTableAttribute(tableAttributeDescriptor, tableDataSize);
        tableData = malloc(tableDataSize);
        //Filename is same as tableName, according to piazza post @87
        int tableId = maxId + 1;
        prepareTableData(tableId, tableName, tableName, User,
                         tableAttributeDescriptor, tableData);
        if(rbfm.insertRecord(fileHandle, tableAttributeDescriptor, tableData, rid) == -1) { return -1; }
        if(rbfm.closeFile(fileHandle) == -1) { return -1; }

        //Finally, for each attribute, insert it into "Columns" table.
        if(rbfm.openFile("Columns", fileHandle) == -1) { return -1; }
        std::vector<Attribute> columnAttributeDescriptor;
        void *columnData = NULL;
        int columnDataSize;
        prepareColumnAttribute(columnAttributeDescriptor, columnDataSize);
        columnData = malloc(columnDataSize);
        for(int i = 0; i < attrs.size(); i++) {
            Attribute attr = attrs[i];
            int columnPosition = i + 1;
            prepareColumnData(tableId, attr.name, attr.type, attr.length, columnPosition,
                              User, columnAttributeDescriptor, columnData);
            if(rbfm.insertRecord(fileHandle, columnAttributeDescriptor, columnData, rid) == -1) {
                return -1;
            }
        }
        if(rbfm.closeFile(fileHandle) == -1) { return -1; }
        return -1;
    }

    RC RelationManager::deleteTable(const std::string &tableName) {
        //First, open "Tables" table.
        PeterDB::RecordBasedFileManager &rbfm = PeterDB::RecordBasedFileManager::instance();
        PeterDB::FileHandle fileHandle;

        //Scan the "Tables" table to search the tableId with tableName
        std::vector<std::string> attributeNames = {"table-id", "file-name"};
        RM_ScanIterator rmsi;
        int tableId;
        std::string fileName;
        if(scan("Tables", "table-name",
                EQ_OP, tableName.c_str(), attributeNames, rmsi) == -1) { return -1; }
        void *returnedData = NULL;
        RID rid;
        if(rmsi.getNextTuple(rid, returnedData) == RM_EOF) { return -1; }
        if(rmsi.close() == -1) { return -1; }
        int nullIndicatorSize = ceil(attributeNames.size() / 8.0);
        int dataOffset = nullIndicatorSize;
        memcpy(&tableId, (char *) returnedData + dataOffset, sizeof(int));
        dataOffset += sizeof(int);
        int fileNameLen;
        memcpy(&fileNameLen, (char *) returnedData + dataOffset, sizeof(int));
        dataOffset += sizeof(int);
        char *fileNameStr = (char *) malloc(fileNameLen + 1);
        memset(fileNameStr, 0, fileNameLen + 1);
        fileName[fileNameLen] = '\0';
        memcpy(fileNameStr, (char *) returnedData + dataOffset, fileNameLen);
        fileName = std::string(fileNameStr);
        free(returnedData);
        returnedData = NULL;
        attributeNames.clear();
        //Delete the scanned record in "Tables" table
        if(rbfm.openFile("Tables", fileHandle) == -1) { return -1; }
        std::vector<Attribute> tableAttributeDescriptor;
        int tableDataSize;
        prepareTableAttribute(tableAttributeDescriptor, tableDataSize);
        if(rbfm.deleteRecord(fileHandle, tableAttributeDescriptor, rid) == -1) { return -1; }
        if(rbfm.closeFile(fileHandle) == -1) { return -1; }

        //Scan the "Columns" table with the tableId.
        if(scan("Tables", "table-id",
                EQ_OP, &tableId, attributeNames, rmsi) == -1) { return -1; }
        returnedData = malloc(4096);
        std::vector<RID> deleteRids;
        while(rmsi.getNextTuple(rid, returnedData) != RM_EOF) { deleteRids.push_back(rid); }
        if(rmsi.close() == -1) { return -1; }
        free(returnedData);
        returnedData = NULL;
        //Delete each one in "Columns" table.
        if(rbfm.openFile("Columns", fileHandle) == -1) { return -1; }
        std::vector<Attribute> columnAttributeDescriptor;
        int columnDataSize;
        prepareColumnAttribute(columnAttributeDescriptor, columnDataSize);
        for(auto delRid : deleteRids) {
            if(rbfm.deleteRecord(fileHandle, columnAttributeDescriptor, delRid) == -1) {
                return -1;
            }
        }
        if(rbfm.closeFile(fileHandle) == -1) { return -1; }
        if(rbfm.destroyFile(fileName) == -1) { return -1; }
        return 0;
    }

    RC RelationManager::getTableInfo(const std::string &tableName, int &tableId, std::string &fileName) {
        //Open "Tables" catalogue to find the filename of the table to insert.
        std::vector<std::string> attributeNames = {"table-id", "file-name"};
        RM_ScanIterator rmsi;
        if(scan("Tables", "table-name",
                EQ_OP, tableName.c_str(), attributeNames, rmsi) == -1) { return -1; }
        void *returnedData = NULL;
        RID rid;
        returnedData = malloc(4096);
        memset(returnedData, 0, 4096);
        if(rmsi.getNextTuple(rid, returnedData) == RM_EOF) { return -1; }
        if(rmsi.close() == -1) { return -1; }
        int nullIndicatorSize = ceil(attributeNames.size() / 8.0);
        int dataOffset = nullIndicatorSize;
        memcpy(&tableId, (char *) returnedData + dataOffset, sizeof(int));
        dataOffset += sizeof(int);
        int fileNameLen;
        memcpy(&fileNameLen, (char *) returnedData + dataOffset, sizeof(int));
        dataOffset += sizeof(int);
        char *fileNameStr = (char *) malloc(fileNameLen + 1);
        memset(fileNameStr, 0, fileNameLen + 1);
        fileName[fileNameLen] = '\0';
        memcpy(fileNameStr, (char *) returnedData + dataOffset, fileNameLen);
        fileName = std::string(fileNameStr);
        free(returnedData);
        attributeNames.clear();
        return 0;
    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
        //This function fetch the attributes from Catalog.
        //First, get the tableId and fileName of the table.
        int tableId;
        std::string fileName;
        if(getTableInfo(tableName, tableId, fileName) == -1) { return -1; }
        //Then, open "Columns" catalogue to find the attributes of the record.
        std::vector <std::string> attributeNames = {"column-name", "column-type", "column-length", "column-position"};
        RM_ScanIterator rmsi;
        if(scan("Columns", "table-id",
                EQ_OP, &tableId, attributeNames, rmsi) == -1) { return -1; }
        std::map<int, Attribute> pos_attr;
        void *returnedData = NULL;
        RID rid;
        returnedData = malloc(4096);
        while(rmsi.getNextTuple(rid, returnedData) != RM_EOF) {
            memset(returnedData, 0, 4096);
            Attribute attr;
            int nullIndicatorSize = ceil(attributeNames.size() / 8.0);
            int dataOffset = nullIndicatorSize;
            int nameLen;
            memcpy(&nameLen, (char *) returnedData + dataOffset, sizeof(int));
            dataOffset += sizeof(int);
            char *columnName = (char *) malloc(nameLen + 1);
            memset(columnName, 0, nameLen + 1);
            columnName[nameLen] = '\0';
            memcpy(columnName, (char *) returnedData + dataOffset, nameLen);
            dataOffset += nameLen;
            std::string name(columnName);
            attr.name = name;
            AttrType type;
            memcpy(&type, (char *) returnedData + dataOffset, sizeof(int));
            dataOffset += sizeof(int);
            attr.type = type;
            AttrLength length;
            memcpy(&length, (char *) returnedData + dataOffset, sizeof(int));
            dataOffset += sizeof(int);
            attr.length = length;
            int columnPos;
            memcpy(&columnPos, (char *) returnedData + dataOffset, sizeof(int));
            dataOffset += sizeof(int);
            pos_attr[columnPos] = attr;
        }
        free(returnedData);
        returnedData = NULL;
        if(rmsi.close() == -1) { return -1; }
        //Push each attribute into vector
        attrs = std::vector<Attribute>(pos_attr.size());
        for(int i = 0; i < pos_attr.size(); i++) { attrs[i] = pos_attr[i]; }
        return 0;
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        //First, get the tableId and fileName from catalog.
        int tableId;
        std::string fileName;
        if(getTableInfo(tableName, tableId, fileName) == -1) { return -1; }
        std::vector<Attribute> recordDescriptor;
        if(getAttributes(tableName, recordDescriptor) == -1) { return -1; }

        //Then, insert the data into the table.
        PeterDB::RecordBasedFileManager &rbfm = PeterDB::RecordBasedFileManager::instance();
        PeterDB::FileHandle fileHandle;
        if(rbfm.openFile(fileName, fileHandle) == -1) { return -1; }
        if(rbfm.insertRecord(fileHandle, recordDescriptor, data, rid) == -1) { return -1; }
        if(rbfm.closeFile(fileHandle) == -1) { return -1; }
        return 0;
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        //First, get the tableId and fileName from catalog.
        int tableId;
        std::string fileName;
        if(getTableInfo(tableName, tableId, fileName) == -1) { return -1; }
        std::vector<Attribute> recordDescriptor;
        if(getAttributes(tableName, recordDescriptor) == -1) { return -1; }

        //Then, delete the record from the table.
        PeterDB::RecordBasedFileManager &rbfm = PeterDB::RecordBasedFileManager::instance();
        PeterDB::FileHandle fileHandle;
        if(rbfm.openFile(fileName, fileHandle) == -1) { return -1; }
        if(rbfm.deleteRecord(fileHandle, recordDescriptor, rid) == -1) { return -1; }
        if(rbfm.closeFile(fileHandle) == -1) { return -1; }
        return 0;
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
        //First, get the tableId and fileName from catalog.
        int tableId;
        std::string fileName;
        if(getTableInfo(tableName, tableId, fileName) == -1) { return -1; }
        std::vector<Attribute> recordDescriptor;
        if(getAttributes(tableName, recordDescriptor) == -1) { return -1; }

        //Then, update the record from the table.
        PeterDB::RecordBasedFileManager &rbfm = PeterDB::RecordBasedFileManager::instance();
        PeterDB::FileHandle fileHandle;
        if(rbfm.openFile(fileName, fileHandle) == -1) { return -1; }
        if(rbfm.updateRecord(fileHandle, recordDescriptor, data, rid) == -1) { return -1; }
        if(rbfm.closeFile(fileHandle) == -1) { return -1; }
        return 0;
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        //First, get the tableId and fileName from catalog.
        int tableId;
        std::string fileName;
        if(getTableInfo(tableName, tableId, fileName) == -1) { return -1; }
        std::vector<Attribute> recordDescriptor;
        if(getAttributes(tableName, recordDescriptor) == -1) { return -1; }

        //Then, read the record from the table.
        PeterDB::RecordBasedFileManager &rbfm = PeterDB::RecordBasedFileManager::instance();
        PeterDB::FileHandle fileHandle;
        if(rbfm.openFile(fileName, fileHandle) == -1) { return -1; }
        if(rbfm.readRecord(fileHandle, recordDescriptor, rid, data) == -1) { return -1; }
        if(rbfm.closeFile(fileHandle) == -1) { return -1; }
        return 0;
    }

    RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out) {
        //Print the tuple using printRecord
        PeterDB::RecordBasedFileManager &rbfm = PeterDB::RecordBasedFileManager::instance();
        if(rbfm.printRecord(attrs, data, out) == -1) { return -1; }
        return 0;
    }

    RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                      void *data) {
        //First, get the tableId and fileName from catalog.
        int tableId;
        std::string fileName;
        if(getTableInfo(tableName, tableId, fileName) == -1) { return -1; }
        std::vector<Attribute> recordDescriptor;
        if(getAttributes(tableName, recordDescriptor) == -1) { return -1; }

        //Then, delete the record from the table.
        PeterDB::RecordBasedFileManager &rbfm = PeterDB::RecordBasedFileManager::instance();
        PeterDB::FileHandle fileHandle;
        if(rbfm.openFile(fileName, fileHandle) == -1) { return -1; }
        if(rbfm.readAttribute(fileHandle, recordDescriptor, rid, attributeName, data) == -1) { return -1; }
        if(rbfm.closeFile(fileHandle) == -1) { return -1; }
        return 0;
    }

    RC RelationManager::scan(const std::string &tableName,
                             const std::string &conditionAttribute,
                             const CompOp compOp,
                             const void *value,
                             const std::vector<std::string> &attributeNames,
                             RM_ScanIterator &rm_ScanIterator) {
        //First, get the tableId and fileName from catalog.
        int tableId;
        std::string fileName;
        if(getTableInfo(tableName, tableId, fileName) == -1) { return -1; }
        std::vector<Attribute> recordDescriptor;
        if(getAttributes(tableName, recordDescriptor) == -1) { return -1; }

        //Then, scan from the table.
        PeterDB::RecordBasedFileManager &rbfm = PeterDB::RecordBasedFileManager::instance();
        PeterDB::FileHandle fileHandle;
        PeterDB::RBFM_ScanIterator rbfmScanIterator;
        if(rbfm.openFile(fileName, fileHandle) == -1) { return -1; }
        if(rbfm.scan(fileHandle, recordDescriptor, conditionAttribute, compOp,
                     value, attributeNames, rbfmScanIterator) == -1) { return -1; }
        if(rbfm.closeFile(fileHandle) == -1) { return -1; }
        return 0;
    }

    RM_ScanIterator::RM_ScanIterator() = default;

    RM_ScanIterator::~RM_ScanIterator() = default;

    RC RM_ScanIterator::getNextTuple(RID &rid, void *data) { return RM_EOF; }

    RC RM_ScanIterator::close() { return -1; }

    // Extra credit work
    RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
        return -1;
    }

    // Extra credit work
    RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
        return -1;
    }

} // namespace PeterDB