#include "src/include/qe.h"

namespace PeterDB {
    Filter::Filter(Iterator *input, const Condition &condition) {
        this->input = input;
        this->condition = condition;
    }

    Filter::~Filter() {

    }

    RC Filter::getAttributeName(std::string attr, std::string &attrName) {
        int delimiter = -1;
        for (int i = 0; i < attr.size(); i++) {
            if (attr[i] == '.') {
                delimiter = i;
                break;
            }
        }
        if (delimiter == -1) { return -1; }
        int attrNameLen = attr.size() - 1 - delimiter + 1;
        attrName = attr.substr(delimiter + 1, attrNameLen);
        return 0;
    }

    RC Filter::getAttributeValue(void *data, std::vector<Attribute> attributes, int condIndex, void *value, int &dataLen) {
        int dataOffset = 0;
        int nullIndicatorSize = ceil(attributes.size() / 8.0);
        char *nullIndicator = (char *) malloc(nullIndicatorSize);
        memset(nullIndicator,0, nullIndicatorSize);
        memcpy(nullIndicator, (char *) data + dataOffset, nullIndicatorSize);

        dataOffset += nullIndicatorSize;
        //Then, follow recordDescriptor to iterate through the data.
        //If the attribute is not null, print it out with certain type.
        for (int i = 0; i < attributes.size(); i++) {
            int nullBit = nullIndicator[i / 8] & (1 << (8 - 1 - i % 8));
            if (nullBit == 0) {
                AttrType type = attributes[i].type;
                if (type == TypeInt) {
                    if (i == condIndex) {
                        memcpy(value, (char *) data + dataOffset, sizeof(int));
                    }
                    dataOffset += sizeof(int);
                } else if (type == TypeReal) {
                    if (i == condIndex) {
                        memcpy(value, (char *) data + dataOffset, sizeof(float));
                    }
                    dataOffset += sizeof(float);
                } else if (type == TypeVarChar) {
                    int strLen;
                    memcpy(&strLen, (char *) data + dataOffset, sizeof(int));
                    if (i == condIndex) {
                        memcpy(value, (char *) data + dataOffset, sizeof(int) + strLen);
                    }
                    dataOffset += sizeof(int) + strLen;
                }
            } else if (i == condIndex) {
                return -1;
            }
        }

        dataLen = dataOffset;
        return 0;
    }

    bool Filter::checkCondition(Attribute attr, void *value) {
        AttrType type = attr.type;
        if (type == TypeInt) {
            int readVal;
            memcpy(&readVal, value, sizeof(int));
            int conditionVal;
            memcpy(&conditionVal, condition.rhsValue.data, sizeof(int));
            if (readVal - conditionVal > 0 &&
                (condition.op == LE_OP || condition.op == LT_OP || condition.op == EQ_OP))
                return false;
            if (readVal - conditionVal == 0 &&
                (condition.op  == LT_OP || condition.op == GT_OP))
                return false;
            if (readVal - conditionVal < 0 &&
                (condition.op  == GE_OP || condition.op == GT_OP || condition.op == EQ_OP))
                return false;
        } else if (type == TypeReal) {
            float readVal;
            memcpy(&readVal, value, sizeof(float));
            float conditionVal;
            memcpy(&conditionVal, condition.rhsValue.data, sizeof(float));
            if (readVal - conditionVal > 0 &&
                (condition.op == LE_OP || condition.op == LT_OP || condition.op == EQ_OP))
                return false;
            if (readVal - conditionVal == 0 &&
                (condition.op  == LT_OP || condition.op == GT_OP))
                return false;
            if (readVal - conditionVal < 0 &&
                (condition.op  == GE_OP || condition.op == GT_OP || condition.op == EQ_OP))
                return false;
        } else if (type == TypeVarChar) {
            int readStrLen;
            memcpy(&readStrLen, value, sizeof(int));
            char readVal[readStrLen + 1];
            memset(readVal, 0, readStrLen + 1);
            readVal[readStrLen] = '\0';
            memcpy(readVal, (char *) value + sizeof(int), readStrLen);
            int conditionStrLen;
            memcpy(&conditionStrLen, (char *) condition.rhsValue.data, sizeof(int));
            char conditionVal[conditionStrLen + 1];
            memset(conditionVal, 0, conditionStrLen + 1);
            conditionVal[conditionStrLen] = '\0';
            memcpy(conditionVal, (char *) condition.rhsValue.data + sizeof(int), conditionStrLen);
            if (strcmp(readVal, conditionVal) > 0 &&
                (condition.op == LE_OP || condition.op == LT_OP || condition.op == EQ_OP))
                return false;
            if (strcmp(readVal, conditionVal) == 0 &&
                (condition.op == LT_OP || condition.op == GT_OP))
                return false;
            if (strcmp(readVal, conditionVal) < 0 &&
                (condition.op == GE_OP || condition.op == GT_OP || condition.op == EQ_OP))
                return false;
        }
        return true;
    }

    RC Filter::getNextTuple(void *data) {
        std::vector<Attribute> attributes;
        if (getAttributes(attributes) == -1) { return -1; }
        int condIndex = -1;
        for (int i = 0; i < attributes.size(); i++) {
            std::string condName = condition.lhsAttr;
            //if (getAttributeName(condition.lhsAttr, condName) == -1) { continue; }
            if (condName == attributes[i].name) {
                condIndex = i;
                break;
            }
        }

        if (condIndex == -1) { return QE_EOF; }
        void *returnedData = malloc(PAGE_SIZE);
        memset(returnedData, 0, PAGE_SIZE);
        while (input->getNextTuple(returnedData) != QE_EOF) {
            void *value = malloc(PAGE_SIZE);
            int dataLen;
            if (getAttributeValue(returnedData, attributes, condIndex, value, dataLen) == -1) { return -1; }
            if (checkCondition(attributes[condIndex], value)) {
                memcpy(data, returnedData, dataLen);
                free(returnedData);
                free(value);
                return 0;
            }
            memset(returnedData, 0, PAGE_SIZE);
            free(value);
        }
        free(returnedData);
        return QE_EOF;
    }

    RC Filter::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        if (input == nullptr) { return -1; }
        return input->getAttributes(attrs);
    }

    Project::Project(Iterator *input, const std::vector<std::string> &attrNames) {

    }

    Project::~Project() {

    }

    RC Project::getNextTuple(void *data) {
        return -1;
    }

    RC Project::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }

    BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned int numPages) {

    }

    BNLJoin::~BNLJoin() {

    }

    RC BNLJoin::getNextTuple(void *data) {
        return -1;
    }

    RC BNLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }
    int getAttrIndex(const std::vector<Attribute> &attrs, const std::string &attr){
        for (int i = 0; i < attrs.size(); i++){
            if (attrs[i].name == attr)
                return i;
        }
        return -1;
    }
    Value getAttrValue(const void* data, const int index, const std::vector<Attribute> &attrs){
        int offset = ceil((double)attrs.size() / 8.0);
        for (int i = 0; i < index; i++){
            bool isNULL = ((unsigned char*)data)[i / 8] & (1 << (8 - 1 - i % 8));
            if (!isNULL){
                if (attrs[i].type == TypeInt)
                    offset += sizeof(int);
                else if (attrs[i].type == TypeReal)
                    offset += sizeof(float);
                else if (attrs[i].type == TypeVarChar)
                    offset += sizeof(int) + *(int*)((char*)data + offset);
            }
        }
        Value res;
        res.data = (char*)data + offset;
        res.type = attrs[index].type;
        return res;
    }
    //leftIn getNextTuple:rid,data(nullindicator of returned attributes+real data), rightIn getNextTuple: key, rid
    INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {
        this->leftIn = leftIn;
        this->rightIn = rightIn;
        this->condition = condition;
        leftIn->getAttributes(this->leftAttrs);
        this->leftIndex = getAttrIndex(this->leftAttrs, condition.lhsAttr);
        this->leftBuffer = (char*)calloc(PAGE_SIZE, 1);
        rightIn->getAttributes(this->rightAttrs);
        this->rightIndex = getAttrIndex(this->rightAttrs, condition.rhsAttr);
        this->rightBuffer = (char*)calloc(PAGE_SIZE, 1);
    }

    INLJoin::~INLJoin() {
        free(this->leftBuffer);
        free(this->rightBuffer);
    }

    RC INLJoin::getNextTuple(void *data) {
        while (leftIn->getNextTuple(leftBuffer) != QE_EOF){
            Value leftVal = getAttrValue(this->leftBuffer, this->leftIndex, this->leftAttrs);
            this->rightIn->setIterator(leftVal.data, leftVal.data, true, true);
            if (rightIn->getNextTuple(this->rightBuffer) != QE_EOF)
                return joinLeftAndRight(data);
        }
        return IX_EOF;
    }

    RC INLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        attrs= this->leftAttrs;
        attrs.insert(attrs.end(), this->rightAttrs.begin(), this->rightAttrs.end());
        return 0;
    }
    RC INLJoin::joinLeftAndRight(void *data){
        int nullIndicatorSize = ceil((double)(this->leftAttrs.size() + this->rightAttrs.size()) / 8.0);
        memset(data, 0, nullIndicatorSize);
        int offset = nullIndicatorSize;
        //copy left attributes
        int leftOffset = ceil((double)this->leftAttrs.size() / 8.0);
        for(int i = 0; i < this->leftAttrs.size(); i++){
            char nullField = *((char*)data + i / 8);
            bool isNULL = this->leftBuffer[i / 8] & (1 << (8 - 1 - i % 8));
            if (isNULL) nullField += 1 << (8 - 1 - i % 8);
            else{
                nullField += 0 << (8 - 1 - i % 8);
                if (leftAttrs[i].type == TypeInt){
                    memcpy((char*)data + offset, leftBuffer + leftOffset, sizeof(int));
                    offset += sizeof(int);
                    leftOffset += sizeof(int);
                }else if (leftAttrs[i].type == TypeReal){
                    memcpy((char*)data + offset, leftBuffer + leftOffset, sizeof(float));
                    offset += sizeof(float);
                    leftOffset += sizeof(float);
                }else if (leftAttrs[i].type == TypeVarChar){
                    int strLen = *(int*)(leftBuffer + leftOffset);
                    memcpy((char*)data + offset, leftBuffer + leftOffset, sizeof(int) + strLen);
                    offset += sizeof(int) + strLen;
                    leftOffset += sizeof(int) + strLen;
                }
            }
            ((unsigned char*)data)[i / 8] = nullField;
        }
        //copy right attributes
        int rightOffset = ceil((double)this->rightAttrs.size() / 8.0);
        for (size_t i = 0; i < this->rightAttrs.size(); i++){
            char nullField = ((char*)data)[(i + this->leftAttrs.size()) / 8];
            bool isNULL = this->rightBuffer[i / 8] & (1 << (8 - 1 - i % 8));
            if (isNULL) nullField += 1 << (8 - 1 - (i + this->leftAttrs.size()) % 8);
            else{
                nullField += 0 << (8 - 1 - (i + this->leftAttrs.size()) % 8);
                if (rightAttrs[i].type == TypeInt){
                    memcpy((char*)data + offset, rightBuffer + rightOffset, sizeof(int));
                    offset += sizeof(int);
                    rightOffset += sizeof(int);
                }else if (rightAttrs[i].type == TypeReal){
                    memcpy((char*)data + offset, rightBuffer + rightOffset, sizeof(float));
                    offset += sizeof(float);
                    rightOffset += sizeof(float);
                }else if (rightAttrs[i].type == TypeVarChar){
                    int strLen = *(int*)(rightBuffer + rightOffset);
                    memcpy((char*)data + offset, rightBuffer + rightOffset, sizeof(int) + strLen);
                    offset += sizeof(int) + strLen;
                    rightOffset += sizeof(int) + strLen;
                }
            }
            ((unsigned char*)data)[(i + this->leftAttrs.size()) / 8] = nullField;
        }
        return 0;
    }
    GHJoin::GHJoin(Iterator *leftIn, Iterator *rightIn, const Condition &condition, const unsigned int numPartitions) {

    }

    GHJoin::~GHJoin() {

    }

    RC GHJoin::getNextTuple(void *data) {
        return -1;
    }

    RC GHJoin::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, AggregateOp op) {

    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, const Attribute &groupAttr, AggregateOp op) {

    }

    Aggregate::~Aggregate() {

    }

    RC Aggregate::getNextTuple(void *data) {
        return -1;
    }

    RC Aggregate::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }
} // namespace PeterDB