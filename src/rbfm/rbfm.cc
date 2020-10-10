#include "src/include/rbfm.h"
#include "iostream"

namespace PeterDB {
    RecordBasedFileManager &RecordBasedFileManager::instance() {
        static RecordBasedFileManager _rbf_manager = RecordBasedFileManager();
        return _rbf_manager;
    }

    RecordBasedFileManager::RecordBasedFileManager() = default;

    RecordBasedFileManager::~RecordBasedFileManager() = default;

    RecordBasedFileManager::RecordBasedFileManager(const RecordBasedFileManager &) = default;

    RecordBasedFileManager &RecordBasedFileManager::operator=(const RecordBasedFileManager &) = default;

    RC RecordBasedFileManager::createFile(const std::string &fileName) {
        PeterDB::PagedFileManager &pfm = PeterDB::PagedFileManager::instance();
        return pfm.createFile(fileName);
    }

    RC RecordBasedFileManager::destroyFile(const std::string &fileName) {
        PeterDB::PagedFileManager &pfm = PeterDB::PagedFileManager::instance();
        return pfm.destroyFile(fileName);
    }

    RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        PeterDB::PagedFileManager &pfm = PeterDB::PagedFileManager::instance();
        return pfm.openFile(fileName, fileHandle);
    }

    RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
        PeterDB::PagedFileManager &pfm = PeterDB::PagedFileManager::instance();
        return pfm.closeFile(fileHandle);
    }

    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {
        //Change format of record
        int recordSize = 0;
        char *record;
        getFieldInfo(recordDescriptor, data,record, recordSize);
        //std::cout << "recordSize: " << recordSize << std::endl;
        int numOfPages = fileHandle.getNumberOfPages();
        if (numOfPages > 0) {
            //Insert into current page
            RC status = insertRecordInOldPage(fileHandle, recordDescriptor, numOfPages - 1, rid,
                                              record, recordSize);
            if (status == -1) {
#ifdef DEBUG
                std::cerr << "Can not insert record in current page" << std::endl;
#endif          //linearly scan pages with enough space to insert
                for (int i = 0; i < numOfPages - 1; i++) {
                    status = insertRecordInOldPage(fileHandle, recordDescriptor, i, rid, record,
                                                   recordSize);
                    if (status == -1) {
#ifdef DEBUG
                        std::cerr << "Can not insert record in page:" << i << std::endl;
#endif
                    } else {
                        free(record);
                        return 0;
                    }
                }
                //If there is no page with enough space, insert the record in new page.
                status = insertRecordInNewPage(fileHandle, recordDescriptor, rid, record,
                                               recordSize);
                if (status == -1) {
#ifdef DEBUG
                    std::cerr << "Can not insert record in new page:" << std::endl;
#endif
                    free(record);
                    return -1;
                } else {
                    free(record);
                    return 0;
                }
            } else {
                free(record);
                return 0;
            }
        } else {
            RC status = insertRecordInNewPage(fileHandle, recordDescriptor, rid, record,
                                              recordSize);
            if (status == -1) {
#ifdef DEBUG
                std::cerr << "Can not insert record in new page:" << std::endl;
#endif
                free(record);
                return -1;
            } else {
                free(record);
                return 0;
            }
        }
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        //std::cout << "rid:" << rid.pageNum << "," << rid.slotNum << std::endl;
        int pageNum = rid.pageNum, slotNum = rid.slotNum;
        char *page = (char *) malloc(PAGE_SIZE);
        RC status = fileHandle.readPage(pageNum, page);
        if (status == -1) {
#ifdef DEBUG
            std::cerr << "Can not read page " << pageNum << " while read record" << std::endl;
#endif
            free(page);
            return -1;
        }
        //offset: pointer in the page
        //dataOffset: pointer to iterate through data
        int offset;
        memcpy(&offset, page + PAGE_SIZE - sizeof(int) * (2 + 2 * slotNum) - sizeof(int), sizeof(int));
        int recordLen;
        memcpy(&recordLen, page + PAGE_SIZE - sizeof(int) * (2 + 2 * slotNum) - 2 * sizeof(int), sizeof(int));
        int nullBit = 0, dataOffset = 0;
        int nullIndicatorSize = ceil(recordDescriptor.size() / 8.0);
        char *nullIndicator = (char *) malloc(nullIndicatorSize);
        //Skip num of fields, get null indicator
        offset += sizeof(int);
        memcpy(nullIndicator, (char *) page + offset, nullIndicatorSize);
        memcpy(data, (char *) page + offset, nullIndicatorSize);
        dataOffset += nullIndicatorSize;
        offset += nullIndicatorSize;
        //First get the number of non null fields, then skip their offsets.
        int nonNull = 0;
        for (int i = 0; i < recordDescriptor.size(); i++) {
            nullBit = nullIndicator[i / 8] & (1 << (8 - 1 - i % 8));
            if (nullBit == 0) { nonNull++; }
        }
        int fieldOffset = offset;
        offset += nonNull * sizeof(int);
        for (int i = 0; i < recordDescriptor.size(); i++) {
            nullBit = nullIndicator[i / 8] & (1 << (8 - 1 - i % 8));
            if (nullBit == 0) {
                AttrType type = recordDescriptor[i].type;
                if (type == TypeInt) {
                    //int intVal;
                    memcpy((char *)data + dataOffset, (char *) page + offset, sizeof(int));
                    //memcpy(&intVal, (char *) page + offset, sizeof(int));
                    //std::cout << intVal << ", ";
                    dataOffset += sizeof(int);
                    offset += sizeof(int);
                } else if (type == TypeReal) {
                    //float realVal;
                    //memcpy(&realVal, (char *) page + offset, sizeof(float));
                    memcpy((char *)data + dataOffset, (char *) page + offset, sizeof(float));
                    //std::cout << realVal << ", ";
                    dataOffset += sizeof(float);
                    offset += sizeof(float);
                } else if (type == TypeVarChar) {
                    int strLen, prevEnd = offset, curEnd;
                    memcpy(&curEnd, (char *) page + fieldOffset, sizeof(int));
                    strLen = curEnd - prevEnd;
                    memcpy((char *)data + dataOffset, (char *) page + offset, strLen);
                    //char *str = (char *) malloc(strLen + 1);
                    //memcpy(str, (char *) page + offset, strLen);
                    //str[strLen] = '\0';
                    //std::cout << str << ", ";
                    //free(str);
                    offset += strLen;
                    dataOffset += strLen;
                }
                fieldOffset += sizeof(int);
                //std::cout << "offset: " << offset << std::endl;
                //std::cout << "fieldOffset: " << fieldOffset << std::endl;
            }
        }
        free(page);
        free(nullIndicator);
        return 0;
    }

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
        return -1;
    }

    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {
        //Given data (null-fields-indicator + actual data) and record descriptor, print out the data.
        //nullBit: null bit for current attribute.
        //offset: pointer to iterate through the record.
        //First, copy the null indicator from data into nullIndicator.
        int nullBit = 0, offset = 0;
        int nullIndicatorSize = ceil(recordDescriptor.size() / 8.0);
        char *nullIndicator = (char *) malloc(nullIndicatorSize);
        memcpy(nullIndicator, (char *) data + offset, nullIndicatorSize);
        offset += nullIndicatorSize;
        std::cout << "offset: " << offset << std::endl;
        //Then, follow recordDescriptor to iterate through the data.
        //If the attribute is not null, print it out with certain type.
        for (int i = 0; i < recordDescriptor.size(); i++) {
            out << recordDescriptor[i].name << ": ";
            //std::cout << recordDescriptor[i].name << ": ";
            nullBit = nullIndicator[i / 8] & (1 << (8 - 1 - i % 8));
            if (nullBit == 0) {
                AttrType type = recordDescriptor[i].type;
                if (type == TypeInt) {
                    int val;
                    memcpy(&val, (char *) data + offset, sizeof(int));
                    out << val;
                    //std::cout << val;
                    offset += sizeof(int);
                } else if (type == TypeReal) {
                    float val;
                    memcpy(&val, (char *) data + offset, sizeof(float));
                    out << val;
                    //std::cout << val;
                    offset += sizeof(float);
                } else if (type == TypeVarChar) {
                    int strLen;
                    memcpy(&strLen, (char *) data + offset, sizeof(int));
                    offset += sizeof(int);
                    std::cout << "offset: " << offset << std::endl;
                    char *str = (char *) malloc(strLen + 1);
                    memset(str, 0, strLen + 1);
                    str[strLen] = '\0';
                    memcpy(str, (char *) data + offset, strLen);
                    out << str;
                    //std::cout << str;
                    offset += strLen;
                    free(str);
                }
            } else {
                out << "NULL";
                //std::cout << "NULL";
            }
            if (i != recordDescriptor.size() - 1) {
                out << ", ";
                //std::cout << ", ";
            }
            std::cout << "offset: " << offset << std::endl;
        }
        out << std::endl;
        //std::cout << std::endl;
        free(nullIndicator);
        return 0;
    }

    RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, const RID &rid) {
        return -1;
    }

    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {
        return -1;
    }

    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {
        return -1;
    }

    void RecordBasedFileManager::getFieldInfo(const std::vector<Attribute> &recordDescriptor, const void *data,
                                              char *&record, int &recordSize) {
        //Do some pre-preparation the record to insert.
        //The record consists of fieldsNum, null indicator, offsets of fields, and data of the record.
        //This method is to prepare the record to inert into the page.
        //Also, get the record size, assign it to recordSize.
        //dataOffset: point to iterate through data.
        //offset: pointer to iterate through record.
        int dataOffset = 0, offset = 0;
        int nullBit = 0;
        int infoSize = 0;
        int nullIndicatorSize = ceil(recordDescriptor.size() / 8.0);
        char *nullIndicator = (char *) malloc(nullIndicatorSize);
        memcpy(nullIndicator, (char *) data + dataOffset, nullIndicatorSize);
        dataOffset += nullIndicatorSize;
        int fieldsNum = recordDescriptor.size();
        //get num of non null fields
        int nonNull = 0;
        for (int i = 0; i < recordDescriptor.size(); i++) {
            nullBit = nullIndicator[i / 8] & (1 << (8 - 1 - i % 8));
            if (nullBit == 0) { nonNull++; }
        }
        //Record first three parts consist of fieldsNum, null indicator, offsets of fields
        //Allocate memory for record
        infoSize = sizeof(int) + nullIndicatorSize + nonNull * sizeof(int);
        //std::cout << sizeof(int) << "+" << nullIndicatorSize << "+" << nonNull * sizeof(int) << "+" << recordSize << std::endl;
        recordSize += infoSize;
        int dataPointer = nullIndicatorSize;
        for (int i = 0; i < recordDescriptor.size(); i++) {
            nullBit = nullIndicator[i / 8] & (1 << (8 - 1 - i % 8));
            if (nullBit == 0) {
                AttrType type = recordDescriptor[i].type;
                if (type == TypeInt) {
                    dataPointer += sizeof(int);
                    recordSize += sizeof(int);
                } else if (type == TypeReal) {
                    dataPointer += sizeof(float);
                    recordSize += sizeof(float);
                } else if (type == TypeVarChar) {
                    int strLen;
                    memcpy(&strLen, (char *) data + dataPointer, sizeof(int));
                    dataPointer += sizeof(int) + strLen;
                    recordSize += strLen;
                }
            }
        }
        record = (char *) malloc(recordSize);
        //Assign fieldsNum
        memcpy(record + offset, &fieldsNum, sizeof(int));
        offset += sizeof(int);
        //Assign null indicator
        memcpy(record + offset, (char *) nullIndicator, nullIndicatorSize);
        offset += nullIndicatorSize;
        //Assign offsets and recordData.
        //fieldOffset is set to the start of actual data in the record
        int fieldOffset = offset + nonNull * sizeof(int);
        //std::cout << "field offset:" << sizeof(int) << "+" << nullIndicatorSize << "+" << nonNull * sizeof(int) << std::endl;
        for (int i = 0; i < recordDescriptor.size(); i++) {
            nullBit = nullIndicator[i / 8] & (1 << (8 - 1 - i % 8));
            if (nullBit == 0) {
                AttrType type = recordDescriptor[i].type;
                if (type == TypeInt) {
                    memcpy(record + fieldOffset, (char *) data + dataOffset, sizeof(int));
                    fieldOffset += sizeof(int);
                    dataOffset += sizeof(int);
                } else if (type == TypeReal) {
                    memcpy(record + fieldOffset, (char *) data + dataOffset, sizeof(float));
                    fieldOffset += sizeof(float);
                    dataOffset += sizeof(float);
                } else if (type == TypeVarChar) {
                    int strLen;
                    memcpy(&strLen, (char *) data + dataOffset, sizeof(int));
                    dataOffset += sizeof(int);
                    memcpy(record + fieldOffset, (char *) data + dataOffset, strLen);
                    char *str = (char *) malloc(strLen + 1);
                    memset(str, 0, strLen + 1);
                    str[strLen] = '\0';
                    memcpy(str, (char *) record + fieldOffset, strLen);
                    free(str);
                    fieldOffset += strLen;
                    dataOffset += strLen;
                }
                memcpy(record + offset, &fieldOffset, sizeof(int));
                offset += sizeof(int);
                //std::cout << "field offset:" << fieldOffset << std::endl;
            }
        }
        free(nullIndicator);
    }

    int getSlotTable(char *page, int count, int size);

    RC RecordBasedFileManager::insertRecordInNewPage(FileHandle &fileHandle,
                                                     const std::vector<Attribute> &recordDescriptor,
                                                     RID &rid, char *&record, int &recordSize) {
        int slotSize = recordSize;
        char *newPage = (char *) calloc(PAGE_SIZE, 1);
        int slotCount = 0;
        int targetSlot = getSlotTable(newPage, slotCount, slotSize);
        if (targetSlot != 0) {
#ifdef DEBUG
            std::cerr << "Can not get slot table when insert record in a new page" << std::endl;
#endif
            free(newPage);
            return -1;
        } else {
            int offset = 0;
            memcpy(newPage + offset, record, recordSize);
            RC status = fileHandle.appendPage(newPage);
            if (status == -1) {
#ifdef DEBUG
                std::cerr << "Can not append a page while insert a record" << std::endl;
#endif
                free(newPage);
                return -1;
            }
            int totalPage = fileHandle.getNumberOfPages();
            rid.pageNum = totalPage - 1;
            rid.slotNum = 0;
            free(newPage);
            return 0;
        }
    }

    int RecordBasedFileManager::getSlotTable(char *&page, int &slotCount, int &slotSize) {
        //Get the slot count and slot size for the new record.
        //Write the offset and slot size into the slot table of page memory.
        int newSlotCount = slotCount + 1;
        //First set free space and slotCount, then directory(offset, slotSize)
        int freeSpace;
        if (slotCount == 0) {
            freeSpace = PAGE_SIZE;
        } else {
            memcpy(&freeSpace, page + PAGE_SIZE - sizeof(int), sizeof(int));
        }
        freeSpace -= sizeof(int) * 2 + slotSize;
        memcpy(page + PAGE_SIZE - sizeof(int), &freeSpace, sizeof(int));
        memcpy(page + PAGE_SIZE - sizeof(int) * 2, &newSlotCount, sizeof(int));
        if (slotCount == 0) {
            int offset = 0;
            memcpy(page + PAGE_SIZE - sizeof(int) * 3, &offset, sizeof(int));
            memcpy(page + PAGE_SIZE - sizeof(int) * 4, &slotSize, sizeof(int));
        } else {
            int previousOffset;
            memcpy(&previousOffset, page + PAGE_SIZE - sizeof(int) * (2 * slotCount + 2) + sizeof(int), sizeof(int));
            int previousSize;
            memcpy(&previousSize, page + PAGE_SIZE - sizeof(int) * (2 * slotCount + 2), sizeof(int));
            int newOffset = previousOffset + previousSize;
            memcpy(page + PAGE_SIZE - sizeof(int) * (2 * slotCount + 2) - sizeof(int), &newOffset, sizeof(int));
            memcpy(page + PAGE_SIZE - sizeof(int) * (2 * slotCount + 2) - sizeof(int) * 2, &slotSize, sizeof(int));
        }
        return slotCount;
    }

    RC RecordBasedFileManager::insertRecordInOldPage(FileHandle &fileHandle,
                                                     const std::vector<Attribute> &recordDescriptor, int pageNum,
                                                     RID &rid, char *&record,
                                                     int &recordSize) {
        //Insert record into existed page (specified by pageNum).
        //Record is joined by recordInfo and data.
        char *oldPage = (char *) malloc(PAGE_SIZE);
        RC status = fileHandle.readPage(pageNum, oldPage);
        if (status == -1) {
#ifdef DEBUG
            std::cerr << "Can not read page " << pageNum << " while insert record in old page" << std::endl;
#endif
            free(oldPage);
            return -1;
        }
        int freeSpace;
        memcpy(&freeSpace, (char *) oldPage + PAGE_SIZE - sizeof(int), sizeof(int));
        //slot size + directory size(2 int)
        int slotSize = recordSize;
        int needSpace = recordSize + sizeof(int) * 2;
        if (needSpace <= freeSpace) {
            int slotCount;
            memcpy(&slotCount, oldPage + PAGE_SIZE - sizeof(int) * 2, sizeof(int));
            int targetSlot = getSlotTable(oldPage, slotCount, slotSize);
            int lastOffset;
            memcpy(&lastOffset, oldPage + PAGE_SIZE - sizeof(int) * (2 * slotCount + 2) + sizeof(int), sizeof(int));
            int lastLength;
            memcpy(&lastLength, oldPage + PAGE_SIZE - sizeof(int) * (2 * slotCount + 2), sizeof(int));
            int offset = lastOffset + lastLength;
            memcpy(oldPage + offset, record, recordSize);
            status = fileHandle.writePage(pageNum, oldPage);
            if (status == -1) {
#ifdef DEBUG
                std::cerr << "Can not write page " << pageNum << std::endl;
#endif
                free(oldPage);
                return -1;
            }
            rid.pageNum = pageNum;
            rid.slotNum = targetSlot;
            free(oldPage);
            return 0;
        }
        return -1;
    }
} // namespace PeterDB

