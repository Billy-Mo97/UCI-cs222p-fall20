#include "src/include/rbfm.h"
#include "iostream"
#include "string.h"
#include "math.h"

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
        short recordSize = 0;
        char *record = (char *) malloc(PAGE_SIZE);//allocate and free in the same function, check if null
        memset(record,0,PAGE_SIZE);
        getFieldInfo(recordDescriptor, data, record, recordSize);
        int numOfPages = fileHandle.getNumberOfPages();
        if (numOfPages > 0) {
            //Insert into current page
            RC status = insertRecordInOldPage(fileHandle, recordDescriptor, numOfPages - 1, rid,
                                              record, recordSize);
            if (status == -1) {
#ifdef DEBUG
                //std::cerr << "Can not insert record in current page" << std::endl;
#endif          //Linearly scan pages with enough space to insert
                for (int i = 0; i < numOfPages - 1; i++) {
                    status = insertRecordInOldPage(fileHandle, recordDescriptor, i, rid, record,
                                                   recordSize);
                    if (status == -1) {
#ifdef DEBUG
                        //std::cerr << "Can not insert record in page:" << i << std::endl;
#endif
                    } else {
                        if (record != nullptr) free(record);
                        return 0;
                    }
                }
                //If there is no page with enough space, insert the record in new page.
                status = insertRecordInNewPage(fileHandle, recordDescriptor, rid, record,
                                               recordSize);
                if (status == -1) {
#ifdef DEBUG
                    //std::cerr << "Can not insert record in new page:" << std::endl;
#endif
                    if (record != nullptr) free(record);
                    return -1;
                } else {
                    if (record != nullptr) free(record);
                    return 0;
                }
            } else {
                if (record != nullptr) free(record);
                return 0;
            }
        } else {
            RC status = insertRecordInNewPage(fileHandle, recordDescriptor, rid, record,
                                              recordSize);
            if (status == -1) {
#ifdef DEBUG
                //std::cerr << "Can not insert record in new page:" << std::endl;
#endif
                if (record != nullptr) free(record);
                return -1;
            } else {
                if (record != nullptr) free(record);
                return 0;
            }
        }
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        int pageNum = rid.pageNum;
        short slotNum = rid.slotNum;
        char *page = (char *) malloc(PAGE_SIZE);memset(page,0,PAGE_SIZE);
        RC status = fileHandle.readPage(pageNum, page);
        short offset, recordLen;
        memcpy(&offset, page + PAGE_SIZE - sizeof(short) * (slotNum * 2 + 2) - sizeof(short), sizeof(short));
        memcpy(&recordLen, page + PAGE_SIZE - sizeof(short) * (slotNum * 2 + 4), sizeof(short));
        //already deleted
        if (offset == -1) { return -1; }
        char flag;
        memcpy(&flag, page + offset, sizeof(char));
        //if is tombstone
        if (flag == -1) {
            RID finalRid;
            memcpy(&finalRid.pageNum, page + offset + sizeof(char), sizeof(int));
            memcpy(&finalRid.slotNum, page + offset + sizeof(char) + sizeof(int), sizeof(short));
            status = readRecord(fileHandle, recordDescriptor, finalRid, data);
            if (status == -1) {
#ifdef DEBUG
                std::cerr << "Cannot read real record when reading record." << std::endl;
#endif
                if (page != nullptr) free(page);
                return -1;
            }
            if (page != nullptr) free(page);
            return status;
        } else {
            //offset: pointer in the page
            //dataOffset: pointer to iterate through data
            //std::cout << "readRecord page:"<<pageNum<<" slot:"<<slotNum<< std::endl;
            short fieldStart = offset;
            int nullBit = 0;
            short dataOffset = 0;
            int nullIndicatorSize = ceil(recordDescriptor.size() / 8.0);
            char *nullIndicator = (char *) malloc(nullIndicatorSize);memset(nullIndicator,0,nullIndicatorSize);
            //Skip num of fields, get null indicator
            offset += sizeof(short);
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
            short fieldOffset = offset;
            offset += nonNull * sizeof(short);
            fieldStart += sizeof(short) + nullIndicatorSize;
            for (int i = 0; i < recordDescriptor.size(); i++) {
                nullBit = nullIndicator[i / 8] & (1 << (8 - 1 - i % 8));
                if (nullBit == 0) {
                    AttrType type = recordDescriptor[i].type;
                    if (type == TypeInt) {
                        //int intVal;
                        memcpy((char *) data + dataOffset, (char *) page + offset, sizeof(int));
                        //memcpy(&intVal, (char *) page + offset, sizeof(int));
                        dataOffset += sizeof(int);
                        offset += sizeof(int);
                    } else if (type == TypeReal) {
                        //float realVal;
                        //memcpy(&realVal, (char *) page + offset, sizeof(float));
                        memcpy((char *) data + dataOffset, (char *) page + offset, sizeof(float));
                        dataOffset += sizeof(float);
                        offset += sizeof(float);
                    } else if (type == TypeVarChar) {
                        short prevEnd, curEnd;
                        int strLen;
                        if (fieldOffset == fieldStart) {
                            prevEnd = sizeof(short) + nullIndicatorSize +
                                      sizeof(short) * nonNull;
                        } else { memcpy(&prevEnd, (char *) page + fieldOffset - sizeof(short), sizeof(short)); }
                        memcpy(&curEnd, (char *) page + fieldOffset, sizeof(short));
                        strLen = curEnd - prevEnd;
                        memcpy((char *) data + dataOffset, &strLen, sizeof(int));
                        dataOffset += sizeof(int);
                        memcpy((char *) data + dataOffset, (char *) page + offset, strLen);
                        offset += strLen;
                        dataOffset += strLen;
                    }
                    fieldOffset += sizeof(short);
                    //std::cout << "fieldOffset: " << fieldOffset << std::endl;
                }
            }
            //std::cout << "readRecord complete page:"<<pageNum<<" slot:"<<slotNum<< std::endl;
            if (page != nullptr) free(page);
            if (nullIndicator != nullptr) free(nullIndicator);
            return 0;
        }
    }

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
        char *page = (char *) malloc(PAGE_SIZE);memset(page,0,PAGE_SIZE);
        int pageNum = rid.pageNum;
        short slotNum = rid.slotNum;
        RC status = fileHandle.readPage(pageNum, page);
        short offset, size;
        memcpy(&offset, page + PAGE_SIZE - sizeof(short) * (slotNum * 2 + 2) - sizeof(short), sizeof(short));
        memcpy(&size, page + PAGE_SIZE - sizeof(short) * (slotNum * 2 + 4), sizeof(short));
        //already deleted
        if (offset == -1) { return -1; }
        char flag;
        memcpy(&flag, page + offset, sizeof(char));
        //if is tombstone
        if (flag == -1) {
            RID finalRid;
            memcpy(&finalRid.pageNum, page + offset + sizeof(char), sizeof(int));
            memcpy(&finalRid.slotNum, page + offset + sizeof(char) + sizeof(int), sizeof(short));
            status = deleteRecord(fileHandle, recordDescriptor, finalRid);
            if (status == -1) {
#ifdef DEBUG
                std::cerr << "Cannot delete real record when deleting record." << std::endl;
#endif
                if (page != nullptr) free(page);
                return -1;
            }
        }
        short freeSpace, slotCount;
        memcpy(&freeSpace, page + PAGE_SIZE - sizeof(short), sizeof(short));
        memcpy(&slotCount, page + PAGE_SIZE - sizeof(short) * 2, sizeof(short));
        //remove all deleted directories in the back
        short deletedCount = 0;
        for (short i = slotCount - 1; i >= 0; i--) {
            short ithOffset;
            memcpy(&ithOffset, page + PAGE_SIZE - sizeof(short) * (i * 2 + 2) - sizeof(short), sizeof(short));
            if (ithOffset == -1) { deletedCount++; }
        }
        //slotCount -= deletedCount;
        //freeSpace += size + deletedCount * sizeof(short) * 2;
        freeSpace += size;
        //memcpy(page + PAGE_SIZE - sizeof(short) * 2, &slotCount, sizeof(short));
        memcpy(page + PAGE_SIZE - sizeof(short), &freeSpace, sizeof(short));
        //if deleted slot is not in the back
        if (slotNum < slotCount - 1) {
            //int shiftDirSize = 2 * sizeof(short) * (slotCount - 1 - slotNum);
            //short shiftStart = PAGE_SIZE - sizeof(short) * (slotNum * 2 + 4)
            shiftSlots(offset, slotNum + 1, slotCount, page);
        }
        int deleteFlag = -1;
        memcpy(page + PAGE_SIZE - sizeof(short) * (slotNum * 2 + 3), &deleteFlag, sizeof(short));
        status = fileHandle.writePage(pageNum, page);
        if (status == -1) {
#ifdef DEBUG
            std::cerr << "Can not write page back when deleting record." << std::endl;
#endif
            if (page != nullptr) free(page);
            return -1;
        }
        if (page != nullptr) free(page);
        std::cout << "delete page:" << pageNum << " slot:" << slotNum << " complete" << std::endl;
        return 0;
    }

    void RecordBasedFileManager::shiftSlots(short targetOffset, short begin, short end, char *&page) {
        //find first undeleted slot in [begin, end)
        bool flag = false;
        short beginOffset, endOffset, endSlotSize;
        for (short i = begin; i < end; i++) {
            memcpy(&beginOffset, page + PAGE_SIZE - sizeof(short) * (4 + i * 2) + sizeof(short), sizeof(short));
            if (beginOffset != -1) {
                flag = true;
                break;
            }
        }
        if (!flag) { return; }
        //find last undeleted slot in [begin, end)
        for (short i = end - 1; i >= begin; i--) {
            memcpy(&endOffset, page + PAGE_SIZE - sizeof(short) * (4 + i * 2) + sizeof(short), sizeof(short));
            memcpy(&endSlotSize, page + PAGE_SIZE - sizeof(short) * (4 + i * 2), sizeof(short));
            if (endOffset != -1) { break; }
        }
        short shiftLength = endOffset + endSlotSize - beginOffset;
        memmove(page + targetOffset, page + beginOffset, shiftLength);
        short changeOffset = targetOffset - beginOffset;
        for (short i = begin; i < end; i++) {
            short tmp;
            memcpy(&tmp, page + PAGE_SIZE - sizeof(short) * (4 + i * 2) + sizeof(short), sizeof(short));
            if (tmp != -1) {
                tmp += changeOffset;
                memcpy(page + PAGE_SIZE - sizeof(short) * (4 + i * 2) + sizeof(short), &tmp, sizeof(short));
            }
        }//std::cout<<"shift slots to:"<<targetOffset<<std::endl;
    }

    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {
        //Given data (null-fields-indicator + actual data) and record descriptor, print out the data.
        //nullBit: null bit for current attribute.
        //offset: pointer to iterate through the record.
        //First, copy the null indicator from data into nullIndicator.
        int nullBit = 0, offset = 0;
        int nullIndicatorSize = ceil(recordDescriptor.size() / 8.0);
        char *nullIndicator = (char *) malloc(nullIndicatorSize);memset(nullIndicator,0,nullIndicatorSize);
        memcpy(nullIndicator, (char *) data + offset, nullIndicatorSize);
        offset += nullIndicatorSize;
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
                    char *str = (char *) malloc(strLen + 1);
                    memset(str, 0, strLen + 1);
                    str[strLen] = '\0';
                    memcpy(str, (char *) data + offset, strLen);
                    out << str;
                    //std::cout << str;
                    offset += strLen;
                    if (str != nullptr) free(str);
                }
            } else {
                out << "NULL";
                //std::cout << "NULL";
            }
            if (i != recordDescriptor.size() - 1) {
                out << ", ";
                //std::cout << ", ";
            }
        }
        out << std::endl;
        //std::cout << std::endl;
        if (nullIndicator != nullptr) free(nullIndicator);
        return 0;
    }

    RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, const RID &rid) {
        short newRecordLen = 0;
        char *newRecord = (char *) malloc(PAGE_SIZE);memset(newRecord,0,PAGE_SIZE);
        getFieldInfo(recordDescriptor, data, newRecord, newRecordLen);
        int pageNum = rid.pageNum;
        short slotNum = rid.slotNum;
        char *page = (char *) malloc(PAGE_SIZE);memset(page,0,PAGE_SIZE);
        RC status = fileHandle.readPage(pageNum, page);
        short slotCount;
        memcpy(&slotCount, page + PAGE_SIZE - 2 * sizeof(short), sizeof(short));
        short offset, recordLen;
        memcpy(&offset, page + PAGE_SIZE - sizeof(short) * (slotNum * 2 + 2) - sizeof(short), sizeof(short));
        memcpy(&recordLen, page + PAGE_SIZE - sizeof(short) * (slotNum * 2 + 4), sizeof(short));
        //already deleted
        if (offset == -1) { return -1; }
        char flag;
        memcpy(&flag, page + offset, sizeof(char));
        //if is tombstone
        if (flag == -1) {
            RID finalRid;
            memcpy(&finalRid.pageNum, page + offset + sizeof(char), sizeof(int));
            memcpy(&finalRid.slotNum, page + offset + sizeof(char) + sizeof(int), sizeof(short));
            status = updateRecord(fileHandle, recordDescriptor, data, finalRid);
            if (status == -1) {
#ifdef DEBUG
                std::cerr << "Cannot update real record when updating record." << std::endl;
#endif
                if (page != nullptr) free(page);
                return -1;
            }
        }
        if (recordLen == newRecordLen) {
            memcpy(page + offset, newRecord, recordLen);
        } else if (recordLen > newRecordLen) {
            memcpy(page + offset, newRecord, newRecordLen);
            short freeSpace;
            memcpy(&freeSpace, page + PAGE_SIZE - sizeof(short), sizeof(short));
            shiftSlots(offset + newRecordLen, slotNum + 1, slotCount, page);
            freeSpace += newRecordLen - recordLen;
            memcpy(page + PAGE_SIZE - sizeof(short), &freeSpace, sizeof(short));
            //update record len in directory
            memcpy(page + PAGE_SIZE - sizeof(short) * (slotNum * 2 + 4), &newRecordLen, sizeof(short));
        } else {
            short freeSpace;
            memcpy(&freeSpace, page + PAGE_SIZE - sizeof(short), sizeof(short));
            if (freeSpace >= newRecordLen - recordLen) {
                shiftSlots(offset + newRecordLen, slotNum + 1, slotCount, page);
                freeSpace -= newRecordLen - recordLen;
                memcpy(page + PAGE_SIZE - sizeof(short), &freeSpace, sizeof(short));
                memcpy(page + PAGE_SIZE - sizeof(short) * (slotNum * 2 + 4), &newRecordLen, sizeof(short));
                memcpy(page + offset, newRecord, newRecordLen);
            } else {//not enough space, use tombstone
                RID newRid;
                insertRecord(fileHandle, recordDescriptor, data, newRid);
                short tombstoneLen = 7;//flag+pageNum+slotNum
                shiftSlots(offset + tombstoneLen, slotNum + 1, slotCount, page);
                freeSpace += recordLen - tombstoneLen;
                memcpy(page + PAGE_SIZE - sizeof(short), &freeSpace, sizeof(short));
                memcpy(page + PAGE_SIZE - sizeof(short) * (slotNum * 2 + 4), &tombstoneLen, sizeof(short));
                char flag = -1;
                int newPageNum = newRid.pageNum;
                short newSlotNum = newRid.slotNum;
                memcpy(page + offset, &flag, sizeof(char));
                offset += sizeof(char);
                memcpy(page + offset, &newPageNum, sizeof(int));
                offset += sizeof(int);
                memcpy(page + offset, &newSlotNum, sizeof(short));
            }
        }
        fileHandle.writePage(pageNum, page);
        if (page != nullptr) free(page);
        if (newRecord != nullptr) free(newRecord);
        return 0;
    }

    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {
        char *record = (char *) malloc(PAGE_SIZE);memset(record,0,PAGE_SIZE);
        RC status = readRecord(fileHandle, recordDescriptor, rid, record);
        printRecord(recordDescriptor, record, std::cout);
        if (status == -1) {
#ifdef DEBUG
            std::cerr << "Cannot read record when reading attribute." << std::endl;
#endif
            if (record != nullptr) free(record);
            return -1;
        }
        int nullIndicatorSize = ceil(recordDescriptor.size() / 8.0);
        char *nullIndicator = (char *) malloc(nullIndicatorSize);memset(nullIndicator,0,nullIndicatorSize);
        short offset = nullIndicatorSize;
        for (int i = 0; i < recordDescriptor.size(); i++) {
            int nullBit = nullIndicator[i / 8] & (1 << (8 - 1 - i % 8));
            if (nullBit == 0) {
                AttrType type = recordDescriptor[i].type;
                std::string name = recordDescriptor[i].name;
                if (type == TypeInt) {
                    int readInt;
                    memcpy(&readInt, (char *) record + offset, sizeof(int));
                    std::cout << "Reading attribute int: " << readInt << std::endl;
                    if (name == attributeName) {
                        memcpy((char *) data, (char *) record + offset, sizeof(int));
                        break;
                    }
                    offset += sizeof(int);
                } else if (type == TypeReal) {
                    int readReal;
                    memcpy(&readReal, (char *) record + offset, sizeof(float));
                    std::cout << "Reading attribute real: " << readReal << std::endl;
                    if (name == attributeName) {
                        memcpy((char *) data, (char *) record + offset, sizeof(float));
                        break;
                    }
                    offset += sizeof(float);
                } else if (type == TypeVarChar) {
                    int strLen;
                    memcpy(&strLen, record + offset, sizeof(int));
                    if (name == attributeName) {
                        memcpy((char *) data, (char *) record + offset, sizeof(int) + strLen);
                        break;
                    }
                    offset += sizeof(int) + strLen;
                }
            }//if attribute is null
            else {
                std::string name = recordDescriptor[i].name;
                if (name == attributeName) {
                    char null = -1;
                    memcpy((char *) data, &null, sizeof(char));
                    break;
                }
            }
        }
        if (nullIndicator != nullptr) free(nullIndicator);
        if (record != nullptr) free(record);
        return 0;
    }

    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {
        rbfm_ScanIterator.curPage = 0;
        rbfm_ScanIterator.curSlot = -1;
        rbfm_ScanIterator.maxPage = fileHandle.numOfPages - 1;
        rbfm_ScanIterator.setFileHandle(fileHandle);
        if (rbfm_ScanIterator.fileHandle.pointer == nullptr) {
            std::cout << "Scanning: fileHandle is pointing to nullptr.\n";
            return -1;
        }
        rbfm_ScanIterator.setRecordDescriptor(recordDescriptor);
        std::cout << "Scanning: successfully set fileHandle and recordDescriptor.\n";
        for (int i = 0; i < recordDescriptor.size(); i++) {
            if (recordDescriptor[i].name == conditionAttribute) {
                rbfm_ScanIterator.setConditionPos(i);
                rbfm_ScanIterator.setConditionAttribute(conditionAttribute);
                break;
            }
        }
        std::cout << "Scanning: successfully set condition attributes and position.\n";
        rbfm_ScanIterator.setValue(value);
        std::cout << "Scanning: successfully set value.\n";
        rbfm_ScanIterator.setAttributeNames(attributeNames);
        rbfm_ScanIterator.setCompOp(compOp);
        std::cout << "Scanning: successfully set all.\n";
        return 0;
    }

    RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
        if (fileHandle.pointer == nullptr) {
            std::cout << "Scanning: fileHandle is pointing to nullptr.\n";
            return -1;
        }
        for (int i = curPage; i <= maxPage; i++) {
            std::cout << "Getting next record: start.\n";
            char *page = (char *) malloc(PAGE_SIZE);memset(page,0,PAGE_SIZE);
            fileHandle.readPage(i, page);
            std::cout << "Getting next record: read page complete.\n";
            short slotCount;
            memcpy(&slotCount, page + PAGE_SIZE - 2 * sizeof(short), sizeof(short));
            int startSlot = 0;
            if (i == curPage) startSlot = curSlot + 1;
            std::cout << "Getting next record: doing condition judge.\n";
            for (int j = startSlot; j < slotCount; j++) {
                std::cout << "i: " << i << std::endl;
                std::cout << "j: " << j << std::endl;
                short slotOffset;
                memcpy(&slotOffset, page + PAGE_SIZE - sizeof(short) * (3 + 2 * j), sizeof(short));
                //if not deleted
                if (slotOffset != -1) {
                    char flag;
                    memcpy(&flag, page + slotOffset, sizeof(char));
                    //if tombstone, continue
                    if (flag == -1) continue;
                    else {
                        char *condition = (char *) malloc(PAGE_SIZE);memset(condition,0,PAGE_SIZE);
                        //read condition
                        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
                        RID ridRead;
                        ridRead.pageNum = i;
                        ridRead.slotNum = j;
                        rbfm.readAttribute(fileHandle, recordDescriptor, ridRead, conditionAttribute, condition);
                        std::cout << "Getting next record: reading condition value.\n";
                        if(value != NULL) {
                            std::cout << "Getting next record: condition value not null" << std::endl;
                            //compare condition with value using compOperator
                            AttrType type = recordDescriptor.at(conditionPos).type;
                            if (type == TypeInt) {
                                int readVal;
                                memcpy(&readVal, condition, sizeof(int));
                                int conditionVal;
                                memcpy(&conditionVal, value, sizeof(int));
                                if (readVal - conditionVal > 0 &&
                                    (compOp == LE_OP || compOp == LT_OP || compOp == EQ_OP))
                                    continue;
                                if (readVal - conditionVal == 0 &&
                                    (compOp == LT_OP || compOp == GT_OP))
                                    continue;
                                if (readVal - conditionVal < 0 &&
                                    (compOp == GE_OP || compOp == GT_OP || compOp == EQ_OP))
                                    continue;
                            } else if (type == TypeReal) {
                                float readVal;
                                memcpy(&readVal, condition, sizeof(float));
                                float conditionVal;
                                memcpy(&conditionVal, value, sizeof(float));
                                if (readVal - conditionVal > 0 &&
                                    (compOp == LE_OP || compOp == LT_OP || compOp == EQ_OP))
                                    continue;
                                if (readVal - conditionVal == 0 &&
                                    (compOp == LT_OP || compOp == GT_OP))
                                    continue;
                                if (readVal - conditionVal < 0 &&
                                    (compOp == GE_OP || compOp == GT_OP || compOp == EQ_OP))
                                    continue;
                            } else if (type == TypeVarChar) {
                                int readStrLen;
                                memcpy(&readStrLen, condition, sizeof(int));
                                char readVal[readStrLen + 1];
                                memset(readVal, 0, readStrLen + 1);
                                readVal[readStrLen] = '\0';
                                memcpy(readVal, (char *) condition + sizeof(int), readStrLen);
                                int conditionStrLen;
                                memcpy(&conditionStrLen, (char *) value, sizeof(int));
                                char conditionVal[conditionStrLen + 1];
                                memset(conditionVal, 0, conditionStrLen + 1);
                                conditionVal[conditionStrLen] = '\0';
                                memcpy(conditionVal, (char *) value + sizeof(int), conditionStrLen);
                                std::string readStr(readVal);
                                std::cout << "Getting next record: condition string:" << conditionVal << std::endl;
                                std::cout << "Getting next record: reading string:" << readStr << std::endl;
                                if (strcmp(readVal, conditionVal) > 0 &&
                                    (compOp == LE_OP || compOp == LT_OP || compOp == EQ_OP))
                                    continue;
                                if (strcmp(readVal, conditionVal) == 0 &&
                                    (compOp == LT_OP || compOp == GT_OP))
                                    continue;
                                if (strcmp(readVal, conditionVal) < 0 &&
                                    (compOp == GE_OP || compOp == GT_OP || compOp == EQ_OP))
                                    continue;
                            }
                        }
                        if (condition != nullptr) {
                            free(condition);
                            condition = nullptr;
                        }
                        //if satisfy, get output attributes, return 0
                        std::vector<int> output;
                        int dataSize = 0;
                        for (int p = 0; p < attributeNames.size(); p++) {
                            for (int q = 0; q < recordDescriptor.size(); q++) {
                                if (recordDescriptor[q].name == attributeNames[p]) {
                                    output.push_back(q);
                                    if (recordDescriptor[q].type == TypeInt) {
                                        dataSize += sizeof(int);
                                    } else if (recordDescriptor[q].type == TypeReal) {
                                        dataSize += sizeof(float);
                                    } else if (recordDescriptor[q].type == TypeVarChar) {
                                        dataSize += sizeof(int) + recordDescriptor[q].length;
                                    }
                                }
                            }
                        }
                        std::cout << "Getting next record: condition judge complete.\n";
                        int nullIndicatorSize = ceil(output.size() / 8.0);
                        char *nullIndicator = (char *) malloc(nullIndicatorSize);memset(nullIndicator,0,nullIndicatorSize);
                        for (int p = 0; p < output.size(); p++) {
                            if (p % 8 == 7) {
                                nullIndicator[p / 8] = 0;
                            } else if (p == output.size() - 1) {
                                std::cout << "nullIndicatorSize = " << nullIndicatorSize << std::endl;
                                int nullSize = 8 - (p % 8 + 1);
                                char nullField = (2 << nullSize - 1) - 1;
                                std::cout << "Getting next record: condition satisfied, nullField = " <<  (2 << nullSize - 1) - 1 << std::endl;
                                nullIndicator[p / 8] = nullField;
                            }
                        }
                        dataSize += nullIndicatorSize;
                        int dataOffset = nullIndicatorSize;
                        //data = malloc(dataSize);
                        memset(data, 0, dataSize);
                        memcpy(data, nullIndicator, nullIndicatorSize);
                        std::cout << "Getting next record: condition satisfied, ridRead " << ridRead.pageNum
                        << ", " << ridRead.slotNum << std::endl;
                        for (int p = 0; p < output.size(); p++) {
                            char *outputData = (char *) malloc(PAGE_SIZE);memset(outputData,0,PAGE_SIZE);
                            std::string outputAttribute = attributeNames[p];
                            std::cout << "Getting next record: condition satisfied, fetching attribute " << outputAttribute << std::endl;
                            AttrType type = recordDescriptor.at(output[p]).type;
                            rbfm.readAttribute(fileHandle, recordDescriptor, ridRead, outputAttribute, outputData);
                            char null;
                            memcpy(&null, outputData, sizeof(char));
                            if (null != -1) {
                                if (type == TypeInt) {
                                    int readInt;
                                    memcpy(&readInt, outputData, sizeof(int));
                                    std::cout << "Getting next record: condition satisfied, fetch " << readInt << std::endl;
                                    memcpy((char *) data + dataOffset, outputData, sizeof(int));
                                    dataOffset += sizeof(int);
                                } else if (type == TypeReal) {
                                    memcpy((char *) data + dataOffset, outputData, sizeof(float));
                                    dataOffset += sizeof(float);
                                } else if (type == TypeVarChar) {
                                    int strLen;
                                    memcpy(&strLen, outputData, sizeof(int));
                                    std::cout << "Getting next record: condition satisfied, fetch string len of " << strLen << std::endl;
                                    memcpy((char *) data + dataOffset, outputData, sizeof(int) + strLen);
                                    dataOffset += sizeof(int) + strLen;
                                }
                            }
                            if (outputData != nullptr) free(outputData);
                            std::cout << "Getting next record: condition satisfied, fetch one attribute done.\n";
                        }
                        curPage = i;
                        curSlot = j;
                        rid.pageNum = i;
                        rid.slotNum = j;
                        if (page != nullptr) free(page);
                        if (nullIndicator != nullptr) free(nullIndicator);
                        std::cout << "Getting next record complete.\n";
                        return 0;
                    }
                }
            }
            if (page != nullptr) free(page);
        }
        return RBFM_EOF;
    }

    void RecordBasedFileManager::getFieldInfo(const std::vector<Attribute> &recordDescriptor, const void *data,
                                              char *&record, short &recordSize) {
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
        char *nullIndicator = (char *) malloc(nullIndicatorSize);memset(nullIndicator,0,nullIndicatorSize);
        memcpy(nullIndicator, (char *) data + dataOffset, nullIndicatorSize);
        dataOffset += nullIndicatorSize;
        short fieldsNum = recordDescriptor.size();
        //get num of non null fields
        int nonNull = 0;
        for (int i = 0; i < recordDescriptor.size(); i++) {
            nullBit = nullIndicator[i / 8] & (1 << (8 - 1 - i % 8));
            if (nullBit == 0) { nonNull++; }
        }
        //Record first three parts consist of fieldsNum, null indicator, offsets of fields
        //Allocate memory for record
        infoSize = sizeof(short) + nullIndicatorSize + nonNull * sizeof(short);
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
        //record = (char *) malloc(recordSize);
        //Assign fieldsNum
        memcpy(record + offset, &fieldsNum, sizeof(short));
        offset += sizeof(short);
        //Assign null indicator
        memcpy(record + offset, (char *) nullIndicator, nullIndicatorSize);
        offset += nullIndicatorSize;
        //Assign offsets and recordData.
        //fieldOffset is set to the start of actual data in the record
        int fieldOffset = offset + nonNull * sizeof(short);
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
                    fieldOffset += strLen;
                    dataOffset += strLen;
                }
                memcpy(record + offset, &fieldOffset, sizeof(short));
                offset += sizeof(short);
                //std::cout << "field offset:" << fieldOffset << std::endl;
            }
        }
        if (nullIndicator != nullptr) free(nullIndicator);
    }

    int getSlotTable(char *page, int count, int size);

    RC RecordBasedFileManager::insertRecordInNewPage(FileHandle &fileHandle,
                                                     const std::vector<Attribute> &recordDescriptor,
                                                     RID &rid, char *&record, short &recordSize) {
        short slotSize = recordSize;
        char *newPage = (char *) malloc(PAGE_SIZE);memset(newPage,0,PAGE_SIZE);
        short slotCount = 0;
        short targetSlot = getSlotTable(newPage, slotCount, slotSize);
        if (targetSlot != 0) {
#ifdef DEBUG
            //std::cerr << "Can not get slot table when insert record in a new page" << std::endl;
#endif
            if (newPage != nullptr) free(newPage);
            return -1;
        } else {
            short offset = 0;
            memcpy(newPage + offset, record, recordSize);
            RC status = fileHandle.appendPage(newPage);
            if (status == -1) {
#ifdef DEBUG
                //std::cerr << "Can not append a page while insert a record" << std::endl;
#endif
                if (newPage != nullptr) free(newPage);
                return -1;
            }
            int totalPage = fileHandle.getNumberOfPages();
            rid.pageNum = totalPage - 1;
            rid.slotNum = 0;
            if (newPage != nullptr) free(newPage);
            return 0;
        }
    }

    short RecordBasedFileManager::getSlotTable(char *&page, short &slotCount, short &slotSize) {
        //Get the slot count and slot size for the new record.
        //Write the offset and slot size into the slot table of page memory.
        short newSlotCount = slotCount + 1;
        //First set free space and slotCount, then directory(offset, slotSize)
        short freeSpace;
        if (slotCount == 0) {
            freeSpace = PAGE_SIZE;
        } else {
            memcpy(&freeSpace, page + PAGE_SIZE - sizeof(short), sizeof(short));
        }
        freeSpace -= slotSize;
        memcpy(page + PAGE_SIZE - sizeof(short), &freeSpace, sizeof(short));
        //memcpy(page + PAGE_SIZE - sizeof(int) * 2, &newSlotCount, sizeof(int));
        if (slotCount == 0) {
            short offset = 0, newFreeSpace = freeSpace - 4 * sizeof(short);
            memcpy(page + PAGE_SIZE - sizeof(short), &newFreeSpace, sizeof(short));
            memcpy(page + PAGE_SIZE - sizeof(short) * 3, &offset, sizeof(short));
            memcpy(page + PAGE_SIZE - sizeof(short) * 4, &slotSize, sizeof(short));
            memcpy(page + PAGE_SIZE - sizeof(short) * 2, &newSlotCount, sizeof(short));
        } else {
            //find whether there is a deleted slot
            for (short i = 0; i < slotCount; i++) {
                short slotOffset = 0;
                memcpy(&slotOffset, page + PAGE_SIZE - sizeof(short) * (2 * i + 4) + sizeof(short), sizeof(short));
                if (slotOffset == -1) {
                    //if it is not the last slot
                    if (i < slotCount - 1) {
                        //find the next undeleted slotOffset to put it in the deleted slot
                        bool flag = false;
                        for (short j = i + 1; j < slotCount; j++) {
                            short nextSlotOffset = 0;
                            memcpy(&nextSlotOffset, page + PAGE_SIZE - sizeof(short) * (2 * j + 4) + sizeof(short),
                                   sizeof(short));
                            if (nextSlotOffset != -1) {
                                memcpy(page + PAGE_SIZE - sizeof(short) * (2 * i + 4) + sizeof(short), &nextSlotOffset,
                                       sizeof(short));
                                flag = true;
                                break;
                            }
                        }
                        if (!flag) {
                            //if there is no undeleted slot, check whether this is the first slot
                            if (i == 0) {
                                short offset = 0;
                                memcpy(page + PAGE_SIZE - sizeof(short) * 3, &offset, sizeof(short));
                                memcpy(page + PAGE_SIZE - sizeof(short) * 4, &slotSize, sizeof(short));
                            } else {
                                short previousOffset;
                                memcpy(&previousOffset, page + PAGE_SIZE - sizeof(short) * (2 * i + 2) + sizeof(short),
                                       sizeof(short));
                                short previousSize;
                                memcpy(&previousSize, page + PAGE_SIZE - sizeof(short) * (2 * i + 2), sizeof(short));
                                short newOffset = previousOffset + previousSize;
                                memcpy(page + PAGE_SIZE - sizeof(short) * (2 * i + 4) + sizeof(short), &newOffset,
                                       sizeof(short));
                                memcpy(page + PAGE_SIZE - sizeof(short) * (2 * i + 4), &slotSize, sizeof(short));
                            }
                        }
                    } else {
                        short previousOffset;
                        memcpy(&previousOffset, page + PAGE_SIZE - sizeof(short) * (2 * i + 2) + sizeof(short),
                               sizeof(short));
                        short previousSize;
                        memcpy(&previousSize, page + PAGE_SIZE - sizeof(short) * (2 * i + 2), sizeof(short));
                        short newOffset = previousOffset + previousSize;
                        memcpy(page + PAGE_SIZE - sizeof(short) * (2 * i + 4) + sizeof(short), &newOffset,
                               sizeof(short));
                        memcpy(page + PAGE_SIZE - sizeof(short) * (2 * i + 4), &slotSize, sizeof(short));
                    }
                    //std::cout<<"use deleted slot,slotCount:"<<slotCount<<" free:"<<freeSpace<<std::endl;
                    return i;
                }
            }// did not find deleted slot, then put in the back
            short previousOffset;
            memcpy(&previousOffset, page + PAGE_SIZE - sizeof(short) * (2 * slotCount + 2) + sizeof(short),
                   sizeof(short));
            short previousSize;
            memcpy(&previousSize, page + PAGE_SIZE - sizeof(short) * (2 * slotCount + 2), sizeof(short));
            short newOffset = previousOffset + previousSize;
            memcpy(page + PAGE_SIZE - sizeof(short) * (2 * slotCount + 2) - sizeof(short), &newOffset, sizeof(short));
            memcpy(page + PAGE_SIZE - sizeof(short) * (2 * slotCount + 2) - sizeof(short) * 2, &slotSize,
                   sizeof(short));
            memcpy(page + PAGE_SIZE - sizeof(short) * 2, &newSlotCount, sizeof(short));
            freeSpace -= 2 * sizeof(short);//new directory
            memcpy(page + PAGE_SIZE - sizeof(short), &freeSpace, sizeof(short));
        }
        //std::cout<<"use back slot,slotCount:"<<newSlotCount<<" free:"<<freeSpace<<std::endl;
        return slotCount;
    }

    RC RecordBasedFileManager::insertRecordInOldPage(FileHandle &fileHandle,
                                                     const std::vector<Attribute> &recordDescriptor, int pageNum,
                                                     RID &rid, char *&record,
                                                     short &recordSize) {
        //Insert record into existed page (specified by pageNum).
        //Record is joined by recordInfo and data.
        char *oldPage = (char *) malloc(PAGE_SIZE);memset(oldPage,0,PAGE_SIZE);
        RC status = fileHandle.readPage(pageNum, oldPage);
        if (status == -1) {
#ifdef DEBUG
            //std::cerr << "Can not read page " << pageNum << " while insert record in old page" << std::endl;
#endif
            if (oldPage != nullptr) free(oldPage);
            return -1;
        }
        short freeSpace;
        memcpy(&freeSpace, (char *) oldPage + PAGE_SIZE - sizeof(short), sizeof(short));
        //slot size + directory size(2 int)
        short slotSize = recordSize;
        short needSpace = recordSize + sizeof(short) * 2;
        if (needSpace <= freeSpace) {
            short slotCount;
            memcpy(&slotCount, oldPage + PAGE_SIZE - sizeof(short) * 2, sizeof(short));
            //std::cout<<"slotCount in old page:"<<slotCount<<std::endl;
            short targetSlot = getSlotTable(oldPage, slotCount, slotSize);
            short newSlotCount;
            memcpy(&newSlotCount, oldPage + PAGE_SIZE - sizeof(short) * 2, sizeof(short));
            //std::cout<<"newSlotCount:"<<newSlotCount<<std::endl;
            //if use deleted slot
            if (slotCount == newSlotCount) {
                short targetOffset;
                memcpy(&targetOffset, oldPage + PAGE_SIZE - sizeof(short) * (2 * targetSlot + 4) + sizeof(short),
                       sizeof(short));
                if (targetSlot < slotCount - 1) {
                    shiftSlots(targetOffset + slotSize, targetSlot + 1, slotCount, oldPage);
                }
                memcpy(oldPage + targetOffset, record, recordSize);
            } else {
                short lastOffset;
                memcpy(&lastOffset, oldPage + PAGE_SIZE - sizeof(short) * (2 * slotCount + 2) + sizeof(short),
                       sizeof(short));
                short lastLength;
                memcpy(&lastLength, oldPage + PAGE_SIZE - sizeof(short) * (2 * slotCount + 2), sizeof(short));
                short offset = lastOffset + lastLength;
                memcpy(oldPage + offset, record, recordSize);
            }
            status = fileHandle.writePage(pageNum, oldPage);
            if (status == -1) {
#ifdef DEBUG
                std::cerr << "Can not write page " << pageNum << std::endl;
#endif
                if (oldPage != nullptr) free(oldPage);
                return -1;
            }
            rid.pageNum = pageNum;
            rid.slotNum = targetSlot;
            if (oldPage != nullptr) free(oldPage);
            return 0;
        }
        return -1;
    }
} // namespace PeterDB

