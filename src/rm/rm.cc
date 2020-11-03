#include "src/include/rm.h"
#include <map>
#include <iostream>
#include "math.h"
namespace PeterDB {
    RelationManager &RelationManager::instance() {
        static RelationManager _relation_manager = RelationManager();
        return _relation_manager;
    }

    RelationManager::RelationManager() = default;

    RelationManager::~RelationManager() = default;

    RelationManager::RelationManager(const RelationManager &) = default;

    RelationManager &RelationManager::operator=(const RelationManager &) = default;

    void RelationManager::prepareTableAttribute(std::vector<Attribute> &tableAttributeDescriptor, std::string tableName,
                                                std::string fileName,  int &tableDataSize) {
        //Prepare record descriptor to insert into "Tables" table.
        //This function only needs to be called once.
        Attribute tableIdAttr;
        tableIdAttr.name = "table-id";
        tableIdAttr.type = TypeInt;
        tableIdAttr.length = sizeof(int);
        Attribute tableNameAttr;
        tableNameAttr.name = "table-name";
        tableNameAttr.type = TypeVarChar;
        tableNameAttr.length = sizeof(int) + tableName.size();
        Attribute fileNameAttr;
        fileNameAttr.name = "file-name";
        fileNameAttr.type = TypeVarChar;
        fileNameAttr.length = sizeof(int) + fileName.size();
        Attribute tableFlagAttr;
        tableFlagAttr.name = "table-flag";
        tableFlagAttr.type = TypeInt;
        tableFlagAttr.length = sizeof(int);
        //Set the data size of table data.
        tableAttributeDescriptor = {tableIdAttr, tableNameAttr, fileNameAttr, tableFlagAttr};
        int nullFieldsIndicatorSize = ceil(tableAttributeDescriptor.size() / 8.0);
        tableDataSize = nullFieldsIndicatorSize + sizeof(int) * 4 + tableName.size() + fileName.size();
        return;
    }

    void
    RelationManager::prepareTableData(int tableId, std::string tableName, std::string fileName, int tableFlag,
                                      std::vector<Attribute> &tableAttributeDescriptor, void *tableData) {
        //Prepare the data and record descriptor to insert into "Tables" table.
        int nullFieldsIndicatorSize = ceil(tableAttributeDescriptor.size() / 8.0);
        unsigned char nullsIndicator[nullFieldsIndicatorSize];
        memset(nullsIndicator, 0, nullFieldsIndicatorSize);
        nullsIndicator[0] = 15; // 00001111
        int dataOffset = 0;
        memcpy(tableData, nullsIndicator, nullFieldsIndicatorSize);
        dataOffset += nullFieldsIndicatorSize;
        memcpy((char *) tableData + dataOffset, &tableId, sizeof(int));
        dataOffset += sizeof(int);
        int tableNameLen = tableName.size();
        memcpy((char *) tableData + dataOffset, &tableNameLen, sizeof(int));
        dataOffset += sizeof(int);
        char tableName_cstr[tableNameLen];
        memset(tableName_cstr, 0, tableNameLen);
        memcpy(tableName_cstr, tableName.c_str(), tableName.size());
        memcpy((char *) tableData + dataOffset, tableName_cstr, tableNameLen);
        dataOffset += tableNameLen;
        int fileNameLen = fileName.size();
        memcpy((char *) tableData + dataOffset, &fileNameLen, sizeof(int));
        dataOffset += sizeof(int);
        char fileName_cstr[fileNameLen];
        memset(fileName_cstr, 0, fileNameLen);
        memcpy(fileName_cstr, fileName.c_str(), fileName.size());
        memcpy((char *) tableData + dataOffset, fileName_cstr, fileNameLen);
        dataOffset += fileNameLen;
        memcpy((char *) tableData + dataOffset, &tableFlag, sizeof(int));
        dataOffset += sizeof(int);
        return;
    }

    void RelationManager::prepareColumnAttribute(std::vector<Attribute> &columnAttributeDescriptor, std::string columnName,
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
        columnNameAttr.length = sizeof(int) + columnName.size();
        Attribute columnTypeAttr;
        columnTypeAttr.name = "column-type";
        columnTypeAttr.type = TypeInt;
        columnTypeAttr.length = sizeof(int);
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
        columnAttributeDescriptor = {tableIdAttr, columnNameAttr, columnTypeAttr,
                                     columnLengthAttr, columnPositionAttr, tableFlagAttr};
        int nullFieldsIndicatorSize = ceil(columnAttributeDescriptor.size() / 8.0);
        columnDataSize = nullFieldsIndicatorSize + sizeof(int) * 6 + columnName.size();
        return;
    }

    void RelationManager::prepareColumnData(int tableId, std::string columnName, AttrType columnType, int columnLength,
                                            int columnPosition,
                                            int tableFlag, std::vector<Attribute> &columnAttributeDescriptor,
                                            void *columnData) {
        //Prepare the data to insert into "Columns" table.
        int nullFieldsIndicatorSize = ceil(columnAttributeDescriptor.size() / 8.0);
        unsigned char nullsIndicator[nullFieldsIndicatorSize];
        memset(nullsIndicator, 0, nullFieldsIndicatorSize);
        nullsIndicator[0] = 3; // 00000011
        memcpy(columnData, nullsIndicator, nullFieldsIndicatorSize);
        int dataOffset = nullFieldsIndicatorSize;
        memcpy((char *) columnData + dataOffset, &tableId, sizeof(int));
        dataOffset += sizeof(int);
        int columnNameLen = columnName.size();
        memcpy((char *) columnData + dataOffset, &columnNameLen, sizeof(int));
        dataOffset += sizeof(int);
        char columnName_cstr[columnNameLen];
        memset(columnName_cstr, 0, columnNameLen);
        memcpy(columnName_cstr, columnName.c_str(), columnNameLen);
        memcpy((char *) columnData + dataOffset, columnName_cstr, columnNameLen);
        dataOffset += columnNameLen;
        memcpy((char *) columnData + dataOffset, &columnType, sizeof(int));
        dataOffset += sizeof(int);
        memcpy((char *) columnData + dataOffset, &columnLength, sizeof(int));
        dataOffset += sizeof(int);
        memcpy((char *) columnData + dataOffset, &columnPosition, sizeof(int));
        dataOffset += sizeof(int);
        memcpy((char *) columnData + dataOffset, &tableFlag, sizeof(int));
        dataOffset += sizeof(int);
        return;
    }

    RC RelationManager::checkCatalog() {
        //Check whether catalog files exists.
        PeterDB::RecordBasedFileManager &rbfm = PeterDB::RecordBasedFileManager::instance();
        PeterDB::FileHandle fileHandle;
        RC r1 = rbfm.openFile("Tables", fileHandle);
        if(r1 == 0) { rbfm.closeFile(fileHandle); }
        RC r2 = rbfm.openFile("Columns", fileHandle);
        if(r2 == 0) { rbfm.closeFile(fileHandle); }
        if(r1 == 0 && r2 == 0)
            return 0;
        else
            return -1;
    }

    RC RelationManager::createCatalog() {
        PeterDB::RecordBasedFileManager &rbfm = PeterDB::RecordBasedFileManager::instance();
        //Create "Tables"
        //std::cout << "Creating catalog:\n";
        if (rbfm.createFile("Tables") == -1) { return -1; }
        //std::cout << "Create \"Tables\" succeed\n";
        //Prepare a tuple of "Tables", insert it into "Tables" table.
        void *tableData = NULL;
        std::vector<Attribute> tableAttributeDescriptor;
        int tableDataSize;
        prepareTableAttribute(tableAttributeDescriptor, "Tables", "Tables", tableDataSize);
        tableData = malloc(tableDataSize);
        memset(tableData, 0, tableDataSize);
        prepareTableData(1, "Tables", "Tables", System,
                         tableAttributeDescriptor, tableData);
        PeterDB::FileHandle fileHandle;
        if (rbfm.openFile("Tables", fileHandle) == -1) { return -1; }
        //std::cout << "Open \"Tables\" succeed\n";
        RID rid;
        if (rbfm.insertRecord(fileHandle, tableAttributeDescriptor, tableData, rid) == -1) { return -1; }
        //std::cout << "Insert first record into \"Tables\" succeed\n";
        free(tableData);
        tableData = NULL;
        tableAttributeDescriptor.clear();
        if (rbfm.closeFile(fileHandle) == -1) { return -1; }
        //std::cout << "Close \"Tables\" succeed\n";

        //Create "Columns"
        if (rbfm.createFile("Columns") == -1) { return -1; }
        //std::cout << "Create \"Columns\" succeed\n";
        //Prepare a tuple of "Columns", insert it into "Tables" table.
        prepareTableAttribute(tableAttributeDescriptor, "Columns", "Columns", tableDataSize);
        tableData = malloc(tableDataSize); memset(tableData, 0, tableDataSize);
        prepareTableData(2, "Columns", "Columns", 0,
                         tableAttributeDescriptor, tableData);
        //std::cout << "Preparing second record complete.\n";
        if (rbfm.openFile("Tables", fileHandle) == -1) { return -1; }
        //std::cout << "Open \"Tables\" succeed\n";
        if (rbfm.insertRecord(fileHandle, tableAttributeDescriptor, tableData, rid) == -1) { return -1; }
        //std::cout << "Rid of second record in \"Tables\": " << rid.pageNum << ", " << rid.slotNum << std::endl;
        char *printData = (char*) malloc(tableDataSize); memset(printData, 0, tableDataSize);
        rbfm.readRecord(fileHandle, tableAttributeDescriptor, rid, printData);
        rbfm.printRecord(tableAttributeDescriptor, tableData, std::cout);
        //std::cout << "Insert second record into \"Tables\" succeed\n";
        free(printData);
        printData = NULL;
        if (rbfm.closeFile(fileHandle) == -1) { return -1; }
        free(tableData);
        tableData = NULL;
        //Prepare 4 tuples of "Tables", insert it into "Columns" table.
        if (rbfm.openFile("Columns", fileHandle) == -1) { return -1; }
        //std::cout << "Open \"Columns\" succeed\n";
        void *table_ColumnData1 = NULL, *table_ColumnData2 = NULL, *table_ColumnData3 = NULL, *table_ColumnData4 = NULL;
        std::vector<Attribute> table_ColumnAttributeDescriptor1, table_ColumnAttributeDescriptor2, table_ColumnAttributeDescriptor3,
                table_ColumnAttributeDescriptor4;
        int table_ColumnDataSize1, table_ColumnDataSize2, table_ColumnDataSize3, table_ColumnDataSize4;
        prepareColumnAttribute(table_ColumnAttributeDescriptor1, "table-id" ,table_ColumnDataSize1);
        prepareColumnAttribute(table_ColumnAttributeDescriptor2, "table-name" ,table_ColumnDataSize2);
        prepareColumnAttribute(table_ColumnAttributeDescriptor3, "file-name" ,table_ColumnDataSize3);
        prepareColumnAttribute(table_ColumnAttributeDescriptor4, "table-flag" ,table_ColumnDataSize4);
        table_ColumnData1 = malloc(table_ColumnDataSize1); memset(table_ColumnData1, 0, table_ColumnDataSize1);
        table_ColumnData2 = malloc(table_ColumnDataSize2); memset(table_ColumnData2, 0, table_ColumnDataSize2);
        table_ColumnData3 = malloc(table_ColumnDataSize3); memset(table_ColumnData3, 0, table_ColumnDataSize3);
        table_ColumnData4 = malloc(table_ColumnDataSize4); memset(table_ColumnData4, 0, table_ColumnDataSize4);
        prepareColumnData(1, "table-id", TypeInt, 4,
                          1, System, table_ColumnAttributeDescriptor1, table_ColumnData1);
        prepareColumnData(1, "table-name", TypeVarChar, 50,
                          2, System, table_ColumnAttributeDescriptor2, table_ColumnData2);
        prepareColumnData(1, "file-name", TypeVarChar, 50,
                          3, System, table_ColumnAttributeDescriptor3, table_ColumnData3);
        prepareColumnData(1, "table-flag", TypeInt, 4,
                          4, System, table_ColumnAttributeDescriptor4, table_ColumnData4);
        //std::cout << "Preparing 4 tuples of \"Tables\" complete\n";
        if (rbfm.insertRecord(fileHandle, table_ColumnAttributeDescriptor1, table_ColumnData1, rid) == -1) { return -1; }
        if (rbfm.insertRecord(fileHandle, table_ColumnAttributeDescriptor2, table_ColumnData2, rid) == -1) { return -1; }
        if (rbfm.insertRecord(fileHandle, table_ColumnAttributeDescriptor3, table_ColumnData3, rid) == -1) { return -1; }
        //std::cout << "Insert 4 tuples of \"Tables\" succeed.\n";
        free(table_ColumnData1);
        free(table_ColumnData2);
        free(table_ColumnData3);
        table_ColumnData1 = NULL;
        table_ColumnData2 = NULL;
        table_ColumnData3 = NULL;

        //Prepare 6 tuples of "Columns", insert it into "Columns" table.
        int column_ColumnDataSize1, column_ColumnDataSize2, column_ColumnDataSize3, column_ColumnDataSize4,
                column_ColumnDataSize5, column_ColumnDataSize6;
        void *column_ColumnData1 = NULL, *column_ColumnData2 = NULL, *column_ColumnData3 = NULL,
                *column_ColumnData4 = NULL, *column_ColumnData5 = NULL, *column_ColumnData6 = NULL;
        std::vector<Attribute> column_ColumnAttributeDescriptor1, column_ColumnAttributeDescriptor2, column_ColumnAttributeDescriptor3,
                column_ColumnAttributeDescriptor4, column_ColumnAttributeDescriptor5, column_ColumnAttributeDescriptor6;
        prepareColumnAttribute(column_ColumnAttributeDescriptor1, "table-id" ,column_ColumnDataSize1);
        prepareColumnAttribute(column_ColumnAttributeDescriptor2, "column-name" ,column_ColumnDataSize2);
        prepareColumnAttribute(column_ColumnAttributeDescriptor3, "column-type" ,column_ColumnDataSize3);
        prepareColumnAttribute(column_ColumnAttributeDescriptor4, "column-length" ,column_ColumnDataSize4);
        prepareColumnAttribute(column_ColumnAttributeDescriptor5, "column-position" ,column_ColumnDataSize5);
        prepareColumnAttribute(column_ColumnAttributeDescriptor6, "table-flag" ,column_ColumnDataSize6);
        column_ColumnData1 = malloc(column_ColumnDataSize1); memset(column_ColumnData1, 0, column_ColumnDataSize1);
        column_ColumnData2 = malloc(column_ColumnDataSize2); memset(column_ColumnData2, 0, column_ColumnDataSize2);
        column_ColumnData3 = malloc(column_ColumnDataSize3); memset(column_ColumnData3, 0, column_ColumnDataSize3);
        column_ColumnData4 = malloc(column_ColumnDataSize4); memset(column_ColumnData4, 0, column_ColumnDataSize4);
        column_ColumnData5 = malloc(column_ColumnDataSize5); memset(column_ColumnData5, 0, column_ColumnDataSize5);
        column_ColumnData6 = malloc(column_ColumnDataSize6); memset(column_ColumnData6, 0, column_ColumnDataSize6);
        prepareColumnData(2, "table-id", TypeInt, 4,
                          1, System, column_ColumnAttributeDescriptor1, column_ColumnData1);
        prepareColumnData(2, "column-name", TypeVarChar, 50,
                          2, System, column_ColumnAttributeDescriptor2, column_ColumnData2);
        prepareColumnData(2, "column-type", TypeInt, 4,
                          3, System, column_ColumnAttributeDescriptor3, column_ColumnData3);
        prepareColumnData(2, "column-length", TypeInt, 4,
                          4, System, column_ColumnAttributeDescriptor4, column_ColumnData4);
        prepareColumnData(2, "column-position", TypeInt, 4,
                          5, System, column_ColumnAttributeDescriptor5, column_ColumnData5);
        prepareColumnData(2, "table-flag", TypeInt, 4,
                          6, System, column_ColumnAttributeDescriptor6, column_ColumnData6);
        //std::cout << "Preparing 6 tuples of \"Columns\" complete\n";
        if (rbfm.insertRecord(fileHandle, column_ColumnAttributeDescriptor1, column_ColumnData1, rid) == -1) { return -1; }
        if (rbfm.insertRecord(fileHandle, column_ColumnAttributeDescriptor2, column_ColumnData2, rid) == -1) { return -1; }
        if (rbfm.insertRecord(fileHandle, column_ColumnAttributeDescriptor3, column_ColumnData3, rid) == -1) { return -1; }
        if (rbfm.insertRecord(fileHandle, column_ColumnAttributeDescriptor4, column_ColumnData4, rid) == -1) { return -1; }
        if (rbfm.insertRecord(fileHandle, column_ColumnAttributeDescriptor5, column_ColumnData5, rid) == -1) { return -1; }
        if (rbfm.insertRecord(fileHandle, column_ColumnAttributeDescriptor6, column_ColumnData6, rid) == -1) { return -1; }
        if (rbfm.insertRecord(fileHandle, table_ColumnAttributeDescriptor4, table_ColumnData4, rid) == -1) { return -1; }
        //std::cout << "Insert 6 tuples of \"Columns\" succeed.\n";
        if (rbfm.closeFile(fileHandle) == -1) { return -1; }
        free(column_ColumnData1);
        free(column_ColumnData2);
        free(column_ColumnData3);
        free(column_ColumnData4);
        free(column_ColumnData5);
        free(column_ColumnData6);
        free(table_ColumnData4);
        column_ColumnData1 = NULL;
        column_ColumnData2 = NULL;
        column_ColumnData3 = NULL;
        column_ColumnData4 = NULL;
        column_ColumnData5 = NULL;
        column_ColumnData6 = NULL;
        table_ColumnData4 = NULL;
        return 0;
    }

    RC RelationManager::deleteCatalog() {
        //Pre-check: check whether catalog tables exists.
        //if(checkCatalog() == -1) { return -1; }
        PeterDB::RecordBasedFileManager &rbfm = PeterDB::RecordBasedFileManager::instance();
        //std::cout << "deleteCatalog is called.\n";
        RC r1 = rbfm.destroyFile("Tables") == -1;
        RC r2 = rbfm.destroyFile("Columns") == -1;
        if (r1 == -1 || r2 == -1) { return -1; }
        return 0;
    }

    RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
        //Pre-check: check whether catalog tables exists.
        if (checkCatalog() == -1) { return -1; }

        PeterDB::RecordBasedFileManager &rbfm = PeterDB::RecordBasedFileManager::instance();
        PeterDB::FileHandle fileHandle;

        //First, scan the table to find the maximum id
        RM_ScanIterator rmsi;
        std::vector<std::string> tableIdAttr = {"table-id"};
        //std::cout << "Creating table: begin to scan.\n";
        if (scan("Tables", "table-id",
                 NO_OP, NULL, tableIdAttr, rmsi) == -1) { return -1; }
        //std::cout << "Creating table: scan complete.\n";
        int nullIndicatorSize = ceil(tableIdAttr.size() / 8.0);
        void *returnedData = malloc(nullIndicatorSize + sizeof(int));
        memset(returnedData, 0, nullIndicatorSize + sizeof(int));
        RID rid;
        int maxId = 0;
        while (rmsi.getNextTuple(rid, returnedData) != RM_EOF) {
            int dataOffset = nullIndicatorSize;
            int currentId;
            memcpy(&currentId, (char *) returnedData + dataOffset, sizeof(int));
            std::cout << "currentId: " << currentId << std::endl;
            maxId = maxId > currentId ? maxId : currentId;
        }
        //std::cout << "maxId: " << maxId << std::endl;
        rmsi.close();
        free(returnedData);
        returnedData = NULL;

        //Then, open "Tables" file, prepare the data to insert to "Tables" table
        if (rbfm.openFile("Tables", fileHandle) == -1) { return -1; }
        void *tableData = NULL;
        std::vector<Attribute> tableAttributeDescriptor;
        int tableDataSize;
        prepareTableAttribute(tableAttributeDescriptor, tableName, tableName, tableDataSize);
        tableData = malloc(tableDataSize); memset(tableData, 0, tableDataSize);
        //Filename is same as tableName, according to piazza post @87
        int tableId = maxId + 1;
        //std::cout << "Creating table: ready to insert into \"Tables\".\n";
        prepareTableData(tableId, tableName, tableName, User,
                         tableAttributeDescriptor, tableData);
        if (rbfm.insertRecord(fileHandle, tableAttributeDescriptor, tableData, rid) == -1) { return -1; }
        if (rbfm.closeFile(fileHandle) == -1) { return -1; }
        //std::cout << "Creating table: successfully insert into \"Tables\".\n";
        //Finally, for each attribute, insert it into "Columns" table.
        if (rbfm.openFile("Columns", fileHandle) == -1) { return -1; }

        for (int i = 0; i < attrs.size(); i++) {
            Attribute attr = attrs[i];
            std::vector<Attribute> columnAttributeDescriptor;
            void *columnData = NULL;
            int columnDataSize;
            prepareColumnAttribute(columnAttributeDescriptor, attr.name, columnDataSize);
            columnData = malloc(columnDataSize); memset(columnData, 0, columnDataSize);
            int columnPosition = i + 1;
            //std::cout << "Creating table: ready for " << i << " th attr to insert into \"Columns\".\n";
            prepareColumnData(tableId, attr.name, attr.type, attr.length, columnPosition,
                              User, columnAttributeDescriptor, columnData);
            if (rbfm.insertRecord(fileHandle, columnAttributeDescriptor, columnData, rid) == -1) {
                return -1;
            }
            free(columnData);
            columnData = NULL;
        }
        if (rbfm.closeFile(fileHandle) == -1) { return -1; }
        if (rbfm.createFile(tableName) == -1) { return -1; }
        return 0;
    }

    RC RelationManager::deleteTable(const std::string &tableName) {
        //Pre-check: check whether catalog tables exists.
        if (checkCatalog() == -1) { return -1; }
        if(tableName == "Tables" || tableName == "Columns") return -1;
        //First, open "Tables" table.
        PeterDB::RecordBasedFileManager &rbfm = PeterDB::RecordBasedFileManager::instance();
        PeterDB::FileHandle fileHandle;

        //Scan the "Tables" table to search the tableId with tableName
        std::vector<std::string> attributeNames = {"table-id", "file-name"};
        RM_ScanIterator rmsi;
        int tableId;
        std::string fileName;
        char* tableNameData = (char *)malloc(sizeof(int) + tableName.size());
        memset(tableNameData, 0, sizeof(int) + tableName.size());
        int tableNameLen = tableName.size(), tableNameDataOffset= 0;
        memcpy(tableNameData, &tableNameLen, sizeof(int));
        tableNameDataOffset += sizeof(int);
        memcpy(tableNameData + tableNameDataOffset, tableName.c_str(), tableNameLen);
        tableNameDataOffset += tableNameLen;
        if (scan("Tables", "table-name",
                 EQ_OP, tableNameData, attributeNames, rmsi) == -1) { return -1; }
        void *returnedData = malloc(4096); memset(returnedData, 0, 4096);
        RID rid;
        if (rmsi.getNextTuple(rid, returnedData) == RM_EOF) { return -1; }
        //std::cout << "Deleting table: " << tableName << ". Successfully get the tuple.\n";
        if (rmsi.close() == -1) { return -1; }
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
        if (rbfm.openFile("Tables", fileHandle) == -1) { return -1; }
        std::vector<Attribute> tableAttributeDescriptor;
        int tableDataSize;
        prepareTableAttribute(tableAttributeDescriptor, tableName, fileName, tableDataSize);
        if (rbfm.deleteRecord(fileHandle, tableAttributeDescriptor, rid) == -1) { return -1; }
        if (rbfm.closeFile(fileHandle) == -1) { return -1; }

        //Scan the "Columns" table with the tableId.
        //std::cout << "Deleting table: " << tableName << ". Scanning " << tableId << " in \"Columns\".\n";
        if (scan("Columns", "table-id",
                 EQ_OP, &tableId, attributeNames, rmsi) == -1) { return -1; }
        returnedData = malloc(4096); memset(returnedData, 0, 4096);
        std::vector<RID> deleteRids;
        while (rmsi.getNextTuple(rid, returnedData) != RM_EOF) {
            //std::cout << "Deleting " << tableName << " , get tableId on " << rid.pageNum << "," << rid.slotNum << std::endl;
            deleteRids.push_back(rid);
        }
        if (rmsi.close() == -1) { return -1; }
        free(returnedData);
        returnedData = NULL;
        //Delete each one in "Columns" table.
        if (rbfm.openFile("Columns", fileHandle) == -1) { return -1; }
        std::vector<Attribute> columnAttributeDescriptor;
        int columnDataSize;
        //Column attribute descriptor is only used to delete the record, the "column-name" does not
        //represent the real column name.
        prepareColumnAttribute(columnAttributeDescriptor, "column-name", columnDataSize);
        for (auto delRid : deleteRids) {
            if (rbfm.deleteRecord(fileHandle, columnAttributeDescriptor, delRid) == -1) {
                return -1;
            }
        }
        if (rbfm.closeFile(fileHandle) == -1) { return -1; }
        if (rbfm.destroyFile(fileName) == -1) { return -1; }
        free(tableNameData);
        tableNameData = NULL;
        return 0;
    }

    RC RelationManager::getTableInfo(const std::string &tableName, int &tableId, std::string &fileName) {
        //Pre-check: check whether catalog tables exists.
        if (checkCatalog() == -1) { return -1; }

        //If caller is trying to get table info of "Tables" or "Columns",
        //directly return the info without going to catalog.
        if (tableName == "Tables") {
            tableId = 1;
            fileName = "Tables";
            return 0;
        }
        if (tableName == "Columns") {
            tableId = 2;
            fileName = "Columns";
            return 0;
        }

        //Open "Tables" catalogue to find the filename of the table to insert.
        std::vector<std::string> attributeNames = {"table-id", "file-name"};
        RM_ScanIterator rmsi;
        char* tableNameData = (char *) malloc(sizeof(int) + tableName.size());
        memset(tableNameData, 0, sizeof(int) + tableName.size());
        int tableNameLen = tableName.size(), tableNameDataOffset= 0;
        memcpy(tableNameData, &tableNameLen, sizeof(int));
        tableNameDataOffset += sizeof(int);
        memcpy(tableNameData + tableNameDataOffset, tableName.c_str(), tableNameLen);
        tableNameDataOffset += tableNameLen;
        if (scan("Tables", "table-name",
                 EQ_OP, tableNameData, attributeNames, rmsi) == -1) { return -1; }
        void *returnedData = NULL;
        RID rid;
        returnedData = malloc(4096);
        memset(returnedData, 0, 4096);
        if (rmsi.getNextTuple(rid, returnedData) == RM_EOF) { return -1; }
        if (rmsi.close() == -1) { return -1; }
        int nullIndicatorSize = ceil(attributeNames.size() / 8.0);
        int dataOffset = nullIndicatorSize;
        memcpy(&tableId, (char *) returnedData + dataOffset, sizeof(int));
        dataOffset += sizeof(int);
        //std::cout << "tableId: " << tableId << std::endl;
        int fileNameLen;
        memcpy(&fileNameLen, (char *) returnedData + dataOffset, sizeof(int));
        dataOffset += sizeof(int);
        char *fileNameStr = (char *) malloc(fileNameLen + 1);
        memset(fileNameStr, 0, fileNameLen + 1);
        fileName[fileNameLen] = '\0';
        memcpy(fileNameStr, (char *) returnedData + dataOffset, fileNameLen);
        fileName = std::string(fileNameStr);
        free(fileNameStr);
        free(returnedData);
        free(tableNameData);
        fileNameStr = NULL;
        returnedData = NULL;
        tableNameData = NULL;
        attributeNames.clear();
        return 0;
    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
        //Pre-check: check whether catalog tables exists.
        if (checkCatalog() == -1) { return -1; }

        //If caller is trying to get attributes of "Tables" or "Columns",
        //directly return the info without going to catalog.
        if (tableName == "Tables") {
            int tableDataSize;
            prepareTableAttribute(attrs, "Tables", "Tables", tableDataSize);
            return 0;
        }
        if (tableName == "Columns") {
            int columnDataSize;
            prepareColumnAttribute(attrs, "column-name", columnDataSize);
            return 0;
        }

        //This function fetch the attributes from Catalog.
        //First, get the tableId and fileName of the table.
        //std::cout << "Getting attribute starts.\n";
        int tableId;
        std::string fileName;
        if (getTableInfo(tableName, tableId, fileName) == -1) { return -1; }
        //std::cout << "Getting attributes: tableId " << tableId << " filename "<< fileName << std::endl;
        //Then, open "Columns" catalogue to find the attributes of the record.
        std::vector<std::string> attributeNames = {"column-name", "column-type", "column-length", "column-position"};
        RM_ScanIterator rmsi;
        if (scan("Columns", "table-id",
                 EQ_OP, &tableId, attributeNames, rmsi) == -1) { return -1; }
        std::map<int, Attribute> pos_attr;
        void *returnedData = NULL;
        RID rid;
        returnedData = malloc(4096);
        memset(returnedData, 0, 4096);
        while (rmsi.getNextTuple(rid, returnedData) != RM_EOF) {
            //std::cout << "Getting attribute: rid " << rid.pageNum << ", " << rid.slotNum << std::endl;
            Attribute attr;
            int nullIndicatorSize = ceil(attributeNames.size() / 8.0);
            int dataOffset = nullIndicatorSize;
            int nameLen;
            memcpy(&nameLen, (char *) returnedData + dataOffset, sizeof(int));
            //std::cout << "Getting attribute: get attr name len " << nameLen << std::endl;
            dataOffset += sizeof(int);
            char *columnName = (char *) malloc(nameLen + 1);
            memset(columnName, 0, nameLen + 1);
            columnName[nameLen] = '\0';
            memcpy(columnName, (char *) returnedData + dataOffset, nameLen);
            dataOffset += nameLen;
            std::string name(columnName);
            //std::cout << "Getting attribute: get attr name " << name << std::endl;
            free(columnName);
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
            memset(returnedData, 0, 4096);
        }
        free(returnedData);
        returnedData = NULL;
        //std::cout << "Getting attribute: get next tuple complete.\n";
        if (rmsi.close() == -1) { return -1; }
        //std::cout << "Getting attribute: rmsi closed.\n";
        //Push each attribute into vector
        attrs = std::vector<Attribute>(pos_attr.size());
        for (int i = 0; i < pos_attr.size(); i++) { attrs[i] = pos_attr[i + 1]; }
        //std::cout << "Getting attribute complete.\n";
        return 0;
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        //Pre-check: check whether catalog tables exists.
        if (checkCatalog() == -1) { return -1; }

        //First, get the tableId and fileName from catalog.
        int tableId;
        std::string fileName;
        if (getTableInfo(tableName, tableId, fileName) == -1) { return -1; }
        std::vector<Attribute> recordDescriptor;
        if (getAttributes(tableName, recordDescriptor) == -1) { return -1; }

        //Then, insert the data into the table.
        PeterDB::RecordBasedFileManager &rbfm = PeterDB::RecordBasedFileManager::instance();
        PeterDB::FileHandle fileHandle;
        if (rbfm.openFile(fileName, fileHandle) == -1) { return -1; }
        if (rbfm.insertRecord(fileHandle, recordDescriptor, data, rid) == -1) { return -1; }
        if (rbfm.closeFile(fileHandle) == -1) { return -1; }
        return 0;
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        //Pre-check: check whether catalog tables exists.
        if (checkCatalog() == -1) { return -1; }

        //First, get the tableId and fileName from catalog.
        int tableId;
        std::string fileName;
        if (getTableInfo(tableName, tableId, fileName) == -1) { return -1; }
        std::vector<Attribute> recordDescriptor;
        if (getAttributes(tableName, recordDescriptor) == -1) { return -1; }

        //Then, delete the record from the table.
        PeterDB::RecordBasedFileManager &rbfm = PeterDB::RecordBasedFileManager::instance();
        PeterDB::FileHandle fileHandle;
        if (rbfm.openFile(fileName, fileHandle) == -1) { return -1; }
        if (rbfm.deleteRecord(fileHandle, recordDescriptor, rid) == -1) { return -1; }
        if (rbfm.closeFile(fileHandle) == -1) { return -1; }
        return 0;
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
        //Pre-check: check whether catalog tables exists.
        if (checkCatalog() == -1) { return -1; }

        //First, get the tableId and fileName from catalog.
        int tableId;
        std::string fileName;
        if (getTableInfo(tableName, tableId, fileName) == -1) { return -1; }
        std::vector<Attribute> recordDescriptor;
        if (getAttributes(tableName, recordDescriptor) == -1) { return -1; }

        //Then, update the record from the table.
        PeterDB::RecordBasedFileManager &rbfm = PeterDB::RecordBasedFileManager::instance();
        PeterDB::FileHandle fileHandle;
        if (rbfm.openFile(fileName, fileHandle) == -1) { return -1; }
        if (rbfm.updateRecord(fileHandle, recordDescriptor, data, rid) == -1) { return -1; }
        if (rbfm.closeFile(fileHandle) == -1) { return -1; }
        return 0;
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        //Pre-check: check whether catalog tables exists.
        if (checkCatalog() == -1) { return -1; }

        //First, get the tableId and fileName from catalog.
        int tableId;
        std::string fileName;
        if (getTableInfo(tableName, tableId, fileName) == -1) { return -1; }
        std::vector<Attribute> recordDescriptor;
        if (getAttributes(tableName, recordDescriptor) == -1) { return -1; }

        //Then, read the record from the table.
        PeterDB::RecordBasedFileManager &rbfm = PeterDB::RecordBasedFileManager::instance();
        PeterDB::FileHandle fileHandle;
        if (rbfm.openFile(fileName, fileHandle) == -1) { return -1; }
        if (rbfm.readRecord(fileHandle, recordDescriptor, rid, data) == -1) { return -1; }
        if (rbfm.closeFile(fileHandle) == -1) { return -1; }
        return 0;
    }

    RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out) {
        //Print the tuple using printRecord
        PeterDB::RecordBasedFileManager &rbfm = PeterDB::RecordBasedFileManager::instance();
        if (rbfm.printRecord(attrs, data, out) == -1) { return -1; }
        return 0;
    }

    RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                      void *data) {
        //Pre-check: check whether catalog tables exists.
        if (checkCatalog() == -1) { return -1; }

        //First, get the tableId and fileName from catalog.
        int tableId;
        std::string fileName;
        if (getTableInfo(tableName, tableId, fileName) == -1) { return -1; }
        std::vector<Attribute> recordDescriptor;
        if (getAttributes(tableName, recordDescriptor) == -1) { return -1; }
        //std::cout << "Reading attribute: " << "tableId " << tableId << " fileName " << fileName << std::endl;
        //Then, delete the record from the table.
        PeterDB::RecordBasedFileManager &rbfm = PeterDB::RecordBasedFileManager::instance();
        PeterDB::FileHandle fileHandle;
        if (rbfm.openFile(fileName, fileHandle) == -1) { return -1; }
        void *readData = malloc(4096); memset(readData,0,4096);
        if (rbfm.readAttribute(fileHandle, recordDescriptor, rid, attributeName, readData) == -1) { return -1; }
        int dataNullIndicatorSize = 1;
        unsigned char dataNullIndicator[dataNullIndicatorSize];
        dataNullIndicator[0] = 127; // 01111111
        memcpy(data, dataNullIndicator, dataNullIndicatorSize);
        short dataOffset = dataNullIndicatorSize;
        for (auto attr : recordDescriptor) {
            if (attr.name == attributeName) {
                if(attr.type == TypeInt) {
                    memcpy((char *) data + dataOffset, readData, sizeof(int));
                    break;
                } else if (attr.type == TypeReal) {
                    memcpy((char *) data + dataOffset, readData, sizeof(float));
                    break;
                } else if (attr.type == TypeVarChar) {
                    int strLen;
                    memcpy(&strLen, readData, sizeof(int));
                    memcpy((char *) data + dataOffset, readData, sizeof(int) + strLen);
                    break;
                }
            }
        }
        if (rbfm.closeFile(fileHandle) == -1) { return -1; }
        free(readData);
        return 0;
    }

    RC RelationManager::scan(const std::string &tableName,
                             const std::string &conditionAttribute,
                             const CompOp compOp,
                             const void *value,
                             const std::vector<std::string> &attributeNames,
                             RM_ScanIterator &rm_ScanIterator) {
        //Pre-check: check whether catalog tables exists.
        if (checkCatalog() == -1) { return -1; }

        //First, get the tableId and fileName from catalog.
        int tableId;
        std::string fileName;
        if (getTableInfo(tableName, tableId, fileName) == -1) { return -1; }
        std::vector<Attribute> recordDescriptor;
        if (getAttributes(tableName, recordDescriptor) == -1) { return -1; }
        //std::cout << "Scanning: get the tableId and fileName from catalog.\n";

        //Then, scan from the table.
        PeterDB::RecordBasedFileManager &rbfm = PeterDB::RecordBasedFileManager::instance();
        PeterDB::FileHandle fileHandle;
        PeterDB::RBFM_ScanIterator rbfmScanIterator;
        if (rbfm.openFile(fileName, fileHandle) == -1) { return -1; }
        //std::cout << "Scanning: open the table file.\n";
        if (rbfm.scan(fileHandle, recordDescriptor, conditionAttribute, compOp,
                      value, attributeNames, rbfmScanIterator) == -1) { return -1; }
        rm_ScanIterator.rbfm_ScanIterator = rbfmScanIterator;
        return 0;
    }

    RM_ScanIterator::RM_ScanIterator() = default;

    RM_ScanIterator::~RM_ScanIterator() = default;

    RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
        return rbfm_ScanIterator.getNextRecord(rid, data);
    }

    RC RM_ScanIterator::close() {
        //std::cout << "Closing RM_ScanIterator.\n";
        PeterDB::RecordBasedFileManager &rbfm = PeterDB::RecordBasedFileManager::instance();
        if (rbfm_ScanIterator.close() == -1) { return -1; }
        //std::cout << "Closing RM_ScanIterator: RM_ScanIterator closed.\n";
        if (rbfm_ScanIterator.fileHandle.pointer != NULL && rbfm.closeFile(rbfm_ScanIterator.fileHandle) == -1) { return -1; }
        //std::cout << "Closing RM_ScanIterator complete.\n";
        return 0;
    }

    // Extra credit work
    RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
        return -1;
    }

    // Extra credit work
    RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
        return -1;
    }

} // namespace PeterDB