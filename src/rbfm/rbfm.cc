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
        return pagedFileManager->createFile(fileName);
    }

    RC RecordBasedFileManager::destroyFile(const std::string &fileName) {
        return pagedFileManager->destroyFile(fileName);
    }

    RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        return pagedFileManager->openFile(fileName,fileHandle);
    }

    RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
        return pagedFileManager->closeFile(fileHandle);
    }

    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {
        //change format of record
        int recordLength,dataSize;
        char* recordInfo;
        getFieldInfo(recordDescriptor,data,recordLength,recordInfo,dataSize);
        int numOfPages = fileHandle.getNumberOfPages();
        if(numOfPages > 0){
            //insert into current page
            RC status = insertRecordInOldPage(fileHandle, recordDescriptor, numOfPages - 1, data, rid,recordLength, recordInfo, dataSize);
            if(status == -1){
#ifdef DEBUG
                std::cerr << "Can not insert record in current page" << std::endl;
#endif          //linearly scan pages to insert
                for(int i = 0;i < numOfPages - 1; i++){
                    status = insertRecordInOldPage(fileHandle, recordDescriptor, i, data, rid,recordLength, recordInfo, dataSize);
                    if(status == -1) {
#ifdef DEBUG
                        std::cerr << "Can not insert record in page:"<< i << std::endl;
#endif
                    }else{
                        free(recordInfo);
                        return 0;
                    }
                }
                //insert in new page
                status = insertRecordInNewPage(fileHandle, recordDescriptor, data, rid, recordLength, recordInfo, dataSize);
                if(status == -1){
#ifdef DEBUG
                    std::cerr << "Can not insert record in new page:"<< std::endl;
#endif
                    free(recordInfo);
                    return -1;
                }else{
                    free(recordInfo);
                    return 0;
                }
            }else{
                free(recordInfo);
                return 0;
            }
        }else{
            RC status = insertRecordInNewPage(fileHandle, recordDescriptor, data, rid, recordLength, recordInfo, dataSize);
            if(status == -1){
#ifdef DEBUG
                std::cerr << "Can not insert record in new page:"<< std::endl;
#endif
                free(recordInfo);
                return -1;
            }else{
                free(recordInfo);
                return 0;
            }
        }
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        int pageNum = rid.pageNum, slotNum = rid.slotNum;
        char *page = (char*)malloc(PAGE_SIZE);
        RC status = fileHandle.readPage(pageNum, page);
        if (status == -1){
#ifdef DEBUG
            std::cerr << "Can not read page " << pageNum << " while read record" << std::endl;
#endif
            free(page);
            return -1;
        }
        int offset;
        memcpy(&offset, page + PAGE_SIZE - sizeof(int) * (2 + 2 * slotNum) - sizeof(int), sizeof(int));
        int dataLen;
        memcpy(&dataLen, page + PAGE_SIZE - sizeof(int) * (2 + 2 * slotNum) - 2 * sizeof(int), sizeof(int));
        int dataEnd = offset + dataLen;
        int nullBit = 0, dataOffset = 0;
        int nullIndicatorSize = ceil(recordDescriptor.size() / 8.0);
        char *nullIndicator = (char*)malloc(nullIndicatorSize);
        //num of fields
        offset += sizeof(int);
        memcpy(nullIndicator, (char *)page + offset, nullIndicatorSize);
        memcpy(data, (char *)page + offset, nullIndicatorSize);
        dataOffset += nullIndicatorSize;
        offset += nullIndicatorSize;
        //offsets of non null fields
        int nonNull=0;
        for (int i = 0; i < recordDescriptor.size(); i++){
            nullBit = nullIndicator[i / 8] & (1 << (8 - 1 - i % 8));
            if(nullBit == 0){nonNull++;}
        }
        offset += nonNull * sizeof(int);
        memcpy((char*)data + dataOffset, (char*)page + offset, dataEnd - offset);
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
        int nullBit = 0, offset = 0;
        int nullIndicatorSize = ceil(recordDescriptor.size() / 8.0);
        char *nullIndicator = (char*)malloc(nullIndicatorSize);
        memcpy(nullIndicator, (char *)data + offset, nullIndicatorSize);
        offset += nullIndicatorSize;
        for (int i = 0; i < recordDescriptor.size(); i++){
            out << recordDescriptor[i].name << ": ";
            nullBit = nullIndicator[i / 8] & (1 << (8 - 1 - i % 8));
            if (nullBit == 0){
                AttrType type = recordDescriptor[i].type;
                if (type == TypeInt){
                    int val;
                    memcpy(&val, (char *)data + offset, sizeof(int));
                    out << val ;
                    offset += sizeof(int);
                }
                else if (type == TypeReal){
                    float val;
                    memcpy(&val, (char *)data + offset, sizeof(float));
                    out << val;
                    offset += sizeof(float);
                }
                else if (type == TypeVarChar){
                    int strLen;
                    memcpy(&strLen, (char *)data + offset, sizeof(int));
                    offset += sizeof(int);
                    char* str = (char*)malloc(strLen + 1);
                    str[strLen] = '\0';
                    memcpy(str, (char *)data + offset, strLen);
                    out << str;
                    offset += strLen;
                    free(str);
                }
            }else{
                out << "NULL";
            }
            if(i != recordDescriptor.size() - 1){
                out << ", ";
            }
        }
        //out << std::endl;
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

    void RecordBasedFileManager::getFieldInfo(const std::vector<Attribute> &recordDescriptor, const void *data, int& recordLength,
                                              char *&recordInfo, int& dataSize) {
        int dataOffset = 0, offset = 0;
        int nullBit = 0;
        int nullIndicatorSize = ceil(recordDescriptor.size() / 8.0);
        char *nullIndicator = (char*)malloc(nullIndicatorSize);
        memcpy(nullIndicator, (char *)data + dataOffset, nullIndicatorSize);
        dataOffset += nullIndicatorSize;
        int fieldsNum = recordDescriptor.size();
        //get num of non null fields
        int nonNull=0;
        for (int i = 0; i < recordDescriptor.size(); i++){
            nullBit = nullIndicator[i / 8] & (1 << (8 - 1 - i % 8));
            if(nullBit == 0){nonNull++;}
        }
        //a record consists of fieldsNum, null indicator, offsets of fields
        recordLength = sizeof(int) + nullIndicatorSize + nonNull * sizeof(int);
        recordInfo = (char*)malloc(recordLength);
        //assign fieldsNum
        memcpy(recordInfo + offset, &fieldsNum, sizeof(int));
        offset += sizeof(int);
        //assign null indicator
        memcpy(recordInfo + offset, (char*)nullIndicator, sizeof(nullIndicatorSize));
        offset += sizeof(nullIndicatorSize);
        //assign offsets
        int fieldOffset = offset + nonNull * sizeof(int);
        for (int i = 0; i < recordDescriptor.size(); i++){
            nullBit = nullIndicator[i / 8] & (1 << (8 - 1 - i % 8));
            if (nullBit == 0){
                AttrType type = recordDescriptor[i].type;
                if (type == TypeInt){
                    fieldOffset += sizeof(int);
                    dataOffset += sizeof(int);
                }
                else if (type == TypeReal){
                    fieldOffset += sizeof(float);
                    dataOffset += sizeof(float);
                }
                else if (type == TypeVarChar){
                    int strLen;
                    memcpy(&strLen, (char *)data + dataOffset, sizeof(int));
                    fieldOffset += sizeof(int);
                    fieldOffset += strLen;
                    dataOffset += sizeof(int);
                    dataOffset += strLen;
                }
                memcpy(recordInfo + offset, &fieldOffset, sizeof(int));
                offset += sizeof(int);
            }
        }
        free(nullIndicator);
        dataSize = dataOffset - nullIndicatorSize;
    }

    int getSlotTable(char *page, int count, int size);

    RC RecordBasedFileManager::insertRecordInNewPage(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data, RID &rid, int &recordLength, char* &recordInfo, int &dataSize)
    {
        int slotSize = recordLength + dataSize;
        char *newPage = (char*)calloc(PAGE_SIZE, 1);
        int slotCount = 0;
        int targetSlot = getSlotTable(newPage, slotCount, slotSize);
        if (targetSlot != 0){
#ifdef DEBUG
            std::cerr << "Can not get slot table when insert record in a new page" << std::endl;
#endif
            free(newPage);
            return -1;
        }else{
            int offset = 0;
            memcpy(newPage + offset, recordInfo, recordLength);
            offset += recordLength;
            int nullIndicatorSize = ceil(recordDescriptor.size() / 8.0);
            memcpy(newPage + offset, (char *)data + nullIndicatorSize, dataSize);
            RC status = fileHandle.appendPage(newPage);
            if (status == -1){
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
    int RecordBasedFileManager::getSlotTable(char* &page, int &slotCount, int &slotSize){
        int newSlotCount = slotCount + 1;
        //first set free space and slotCount, then directory(offset, slotSize)
        int freeSpace;
        if(slotCount == 0){
            freeSpace = PAGE_SIZE;
        }else{
            memcpy(&freeSpace, page + PAGE_SIZE - sizeof(int), sizeof(int));
        }
        freeSpace -= sizeof(int) * 2 + slotSize;
        memcpy(page + PAGE_SIZE - sizeof(int), &freeSpace, sizeof(int));
        memcpy(page + PAGE_SIZE - sizeof(int) * 2, &newSlotCount, sizeof(int));
        if (slotCount == 0){
            int offset = 0;
            memcpy(page + PAGE_SIZE - sizeof(int) * 3, &offset, sizeof(int));
            memcpy(page + PAGE_SIZE - sizeof(int) * 4, &slotSize, sizeof(int));
        }else{
            int previousOffset;
            memcpy(&previousOffset, page + PAGE_SIZE - sizeof(int) * (2 * slotCount + 2) + sizeof(int), sizeof(int));
            int previousSize;
            memcpy(&previousSize, page + PAGE_SIZE - sizeof(int) * (2 * slotCount + 2), sizeof(int));
            memcpy(page + PAGE_SIZE - sizeof(int) * (2 * slotCount + 2) - sizeof(int), &previousOffset, sizeof(int));
            memcpy(page + PAGE_SIZE - sizeof(int) * (2 * slotCount + 2) - sizeof(int) * 2, &previousSize, sizeof(int));
        }
        return slotCount;
    }
    RC RecordBasedFileManager::insertRecordInOldPage(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, int pageNum, const void *data, RID &rid, int &recordLength, char* &recordInfo, int &dataSize){
        char *oldPage = (char*)malloc(PAGE_SIZE);
        RC status = fileHandle.readPage(pageNum, oldPage);
        if (status == -1){
#ifdef DEBUG
            std::cerr << "Can not read page " << pageNum << " while insert record in old page" << std::endl;
#endif
            free(oldPage);
            return -1;
        }
        int freeSpace;
        memcpy(&freeSpace, (char*)oldPage + PAGE_SIZE - sizeof(int), sizeof(int));
        //slot size+ directory size(2 int)
        int slotSize = recordLength + dataSize;
        int needSpace = recordLength + dataSize + sizeof(int) * 2;
        if(needSpace <= freeSpace){
            int slotCount;
            memcpy(&slotCount, oldPage + PAGE_SIZE - sizeof(int) * 2, sizeof(int));
            int targetSlot = getSlotTable(oldPage, slotCount, slotSize);
            int lastOffset;
            memcpy(&lastOffset, oldPage + PAGE_SIZE - sizeof(int) * (2 * slotCount + 2) + sizeof(int), sizeof(int));
            int lastLength;
            memcpy(&lastLength, oldPage + PAGE_SIZE - sizeof(int) * (2 * slotCount + 2), sizeof(int));
            int offset = lastOffset + lastLength;
            memcpy(oldPage + offset, recordInfo, recordLength);
            offset += recordLength;
            int nullIndicatorSize = ceil(recordDescriptor.size() / 8.0);
            memcpy(oldPage + offset, (char *)data + nullIndicatorSize, dataSize);
            status = fileHandle.writePage(pageNum, oldPage);
            if (status == -1){
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

