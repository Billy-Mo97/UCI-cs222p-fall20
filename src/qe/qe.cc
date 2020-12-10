#include "src/include/qe.h"
#include <iostream>

namespace PeterDB {
    Filter::Filter(Iterator *input, const Condition &condition) {
        this->input = input;
        this->condition = condition;
    }

    Filter::~Filter() {

    }

    RC
    Filter::getAttributeValue(void *data, std::vector<Attribute> attributes, int condIndex, void *value, int &dataLen) {
        int dataOffset = 0;
        int nullIndicatorSize = ceil(attributes.size() / 8.0);
        char *nullIndicator = (char *) malloc(nullIndicatorSize);
        memset(nullIndicator, 0, nullIndicatorSize);
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
                (condition.op == LT_OP || condition.op == GT_OP))
                return false;
            if (readVal - conditionVal < 0 &&
                (condition.op == GE_OP || condition.op == GT_OP || condition.op == EQ_OP))
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
                (condition.op == LT_OP || condition.op == GT_OP))
                return false;
            if (readVal - conditionVal < 0 &&
                (condition.op == GE_OP || condition.op == GT_OP || condition.op == EQ_OP))
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
        this->input = input;
        for (auto attrName : attrNames) {
            this->attrNames.push_back(attrName);
        }
    }

    Project::~Project() {

    }

    RC Project::getFieldsStart(std::vector<Attribute> allAttributes, std::vector<int> output, std::map<int, int> &attrMap) {
        std::vector<Attribute> attributes;
        if (getAttributes(attributes) == -1) { return -1; }
        int dataNullIndicatorSize = ceil(allAttributes.size() / 8.0);
        std::vector<int> starts;
        int start = dataNullIndicatorSize;
        for (auto attr : allAttributes) {
            starts.push_back(start);
            AttrType type = attr.type;
            start += attr.length;
        }
        for (int i = 0; i < attributes.size(); i++) {
            Attribute attr = attributes[i];
            int index = output[i];
            attrMap[index] = starts[index];
        }
        return 0;
    }

    RC Project::getAttributeValue(void *data, std::vector<Attribute> allAttributes, std::vector<int> output, void *value, int &dataLen) {
        int dataNullIndicatorSize = ceil(allAttributes.size() / 8.0);
        char *dataNullIndicator = (char *) malloc(dataNullIndicatorSize);
        memset(dataNullIndicator, 0, dataNullIndicatorSize);
        memcpy(dataNullIndicator, data, dataNullIndicatorSize);

        int nullIndicatorSize = ceil(output.size() / 8.0);
        char *nullIndicator = (char *) malloc(nullIndicatorSize);
        memset(nullIndicator,0,nullIndicatorSize);
        char nullField = 0;
        for (int p = 0; p < output.size(); p++) {
            int nullBit = dataNullIndicator[output[p] / 8] & (1 << (8 - 1 - output[p] % 8));
            if (nullBit != 0 ) {
                int rightMost = 8 - (p % 8 + 1);
                nullField += 1 << rightMost;
            }

            if (p % 8 == 7 || p == output.size() - 1) {
                nullIndicator[p / 8] = nullField;
                nullField = 0;
            }
        }

        memcpy(value, nullIndicator, nullIndicatorSize);
        int dataOffset = dataNullIndicatorSize;
        int offset = nullIndicatorSize;

        std::map<int, int> attrMap;
        if (getFieldsStart(allAttributes, output, attrMap) == -1) { return -1; }
        std::vector<Attribute> attributes;
        if (getAttributes(attributes) == -1) { return -1; }
        for (int i = 0; i < attributes.size(); i++) {
            Attribute attr = attributes[i];
            int nullBit = nullIndicator[i / 8] & (1 << (8 - 1 - i % 8));
            if (nullBit == 0) {
                int start = attrMap[output[i]];
                AttrType type = allAttributes[i].type;
                if (type == TypeInt) {
                    memcpy((char *) value + offset, (char *) data + start, sizeof(int));
                    offset += sizeof(int);
                } else if (type == TypeReal) {
                    float val;
                    memcpy(&val,  (char *) data + dataOffset, sizeof(float));
                    memcpy((char *) value + offset, (char *) data + start, sizeof(float));
                    offset += sizeof(float);
                } else if (type == TypeVarChar) {
                    int strLen;
                    memcpy(&strLen, (char *) data + dataOffset, sizeof(int));
                    memcpy((char *) value + offset, (char *) data + start, sizeof(int) + strLen);
                    offset += sizeof(int) + strLen;
                }
            }
        }

        /*for (int i = 0; i < allAttributes.size(); i++) {
            int nullBit = dataNullIndicator[i / 8] & (1 << (8 - 1 - i % 8));
            if (nullBit == 0) {
                AttrType type = allAttributes[i].type;
                if (type == TypeInt) {
                    if (find(output.begin(), output.end(), i) != output.end()) {
                        memcpy((char *) value + offset, (char *) data + dataOffset, sizeof(int));
                        offset += sizeof(int);
                    }
                    dataOffset += sizeof(int);
                } else if (type == TypeReal) {
                    if (find(output.begin(), output.end(), i) != output.end()) {
                        float val;
                        memcpy(&val,  (char *) data + dataOffset, sizeof(float));
                        memcpy((char *) value + offset, (char *) data + dataOffset, sizeof(float));
                        offset += sizeof(float);
                    }
                    dataOffset += sizeof(float);
                } else if (type == TypeVarChar) {
                    int strLen;
                    memcpy(&strLen, (char *) data + dataOffset, sizeof(int));
                    if (find(output.begin(), output.end(), i) != output.end()) {
                        memcpy((char *) value + offset, (char *) data + dataOffset, sizeof(int) + strLen);
                        offset += sizeof(int) + strLen;
                    }
                    dataOffset += sizeof(int) + strLen;
                }
            }
        }*/

        dataLen = offset;
        free(dataNullIndicator);
        free(nullIndicator);

        return 0;
    }

    RC Project::getNextTuple(void *data) {
        std::vector<Attribute> allAttributes;
        if (input->getAttributes(allAttributes) == -1) { return -1; }
        std::vector<int> output;

        for (auto attrName : attrNames) {
            for (int i = 0; i < allAttributes.size(); i++) {
                if (attrName == allAttributes[i].name) {
                    output.push_back(i);
                }
            }
        }


        void *returnedData = malloc(PAGE_SIZE);
        memset(returnedData, 0, PAGE_SIZE);
        if (input->getNextTuple(returnedData) != QE_EOF) {
            void *value = malloc(PAGE_SIZE);
            int dataLen;
            if (getAttributeValue(returnedData, allAttributes, output, value, dataLen) == -1) { return -1; }
            memcpy(data, value, dataLen);
            free(returnedData);
            free(value);
            return 0;
        } else {
            free(returnedData);
            return QE_EOF;
        }
    }

    RC Project::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        if (input == nullptr) { return -1; }
        std::vector<Attribute> allAttributes;
        if (input->getAttributes(allAttributes) == -1) { return -1; }
        for (auto attrName : attrNames) {
            for (auto attr : allAttributes) {
                if (attrName == attr.name) {
                    attrs.push_back(attr);
                }
            }
        }
        return 0;
    }

    int getAttrIndex(const std::vector<Attribute> &attrs, const std::string &attr) {
        for (int i = 0; i < attrs.size(); i++) {
            if (attrs[i].name == attr)
                return i;
        }
        return -1;
    }

    Value getAttrValue(const void *data, const int index, const std::vector<Attribute> &attrs) {
        int offset = ceil((double) attrs.size() / 8.0);
        for (int i = 0; i < index; i++) {
            bool isNULL = ((unsigned char *) data)[i / 8] & (1 << (8 - 1 - i % 8));
            if (!isNULL) {
                if (attrs[i].type == TypeInt)
                    offset += sizeof(int);
                else if (attrs[i].type == TypeReal)
                    offset += sizeof(float);
                else if (attrs[i].type == TypeVarChar)
                    offset += sizeof(int) + *(int *) ((char *) data + offset);
            }
        }
        Value res;
        res.type = attrs[index].type;
        //res.data = (char *) data + offset;
        if (res.type == TypeInt) {
            res.data = malloc(sizeof(int));
            memcpy(res.data, (char *) data + offset, sizeof(int));
        } else if (res.type == TypeReal) {
            res.data = malloc(sizeof(float));
            memcpy(res.data, (char *) data + offset, sizeof(float));
        } else if (res.type == TypeVarChar) {
            int strLen;
            memcpy(&strLen, (char *) data + offset, sizeof(int));
            res.data = malloc(sizeof(int) + strLen);
            memcpy(res.data, (char *) data + offset, sizeof(int) + strLen);
        }
        return res;
    }

    int getDataLength(void *data, std::vector<Attribute> attrs) {
        int offset = 0;
        int nullIndicatorSize = ceil((double) attrs.size() / 8.0);
        char *nullIndicator = (char *) malloc(nullIndicatorSize);
        memcpy(nullIndicator, (char *) data + offset, nullIndicatorSize);
        offset = offset + nullIndicatorSize;
        for (int i = 0; i < attrs.size(); i++) {
            if (!(nullIndicator[i / 8] & (1 << (7 - i % 8)))) {
                if (attrs[i].type == TypeInt) offset += sizeof(int);
                else if (attrs[i].type == TypeReal) offset += sizeof(float);
                else if (attrs[i].type == TypeVarChar) {
                    int strLen;
                    memcpy(&strLen, (char *) data + offset, 4);
                    offset = offset + strLen + 4;
                }
            }
        }
        free(nullIndicator);
        return offset;
    }

    int compareKey(AttrType attrType, const void *v1, const void *v2) {
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

    BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned int numPages) {
        this->leftIn = leftIn;
        this->rightIn = rightIn;
        this->condition = condition;
        this->numPages = numPages;
        leftIn->getAttributes(this->leftAttrs);
        rightIn->getAttributes(this->rightAttrs);
        updateOutVector();
        updateInnerVector();
        outIndex = 0;
        innerIndex = 0;
    }

    BNLJoin::~BNLJoin() {
        for (auto &i:out) free(i.data);
        for (auto &i:inner) free(i.data);
    }

    RC BNLJoin::updateOutVector() {
        outSize = 0;
        for (auto &i : this->out) free(i.data);
        out.clear();
        while (outSize < numPages * PAGE_SIZE) {
            void *data = malloc(PAGE_SIZE);
            if (leftIn->getNextTuple(data) != QE_EOF) {
                int dataLen = getDataLength(data, leftAttrs);
                outSize += dataLen;
                Tuple tuple(data, dataLen);
                out.push_back(tuple);
                free(data);
            } else {
                free(data);
                return -1;
            }
        }
        return 0;
    }

    RC BNLJoin::updateInnerVector() {
        innerSize = 0;
        for (auto &i : this->inner) free(i.data);
        inner.clear();
        while (innerSize < PAGE_SIZE) {
            void *data = malloc(PAGE_SIZE);
            if (rightIn->getNextTuple(data) != QE_EOF) {
                int dataLen = getDataLength(data, rightAttrs);
                innerSize += dataLen;
                Tuple tuple(data, dataLen);
                inner.push_back(tuple);
                free(data);
            } else {
                free(data);
                return -1;
            }
        }
        return 0;
    }

    RC BNLJoin::getNextTuple(void *data) {
        while (true) {
            while (outIndex < out.size()) {
                //find R value
                int outAttrIndex = getAttrIndex(leftAttrs, condition.lhsAttr);
                char *outAttrVal = (char *) calloc(PAGE_SIZE, 1);
                getAttrVal(outAttrVal, out[outIndex].data, outAttrIndex, leftAttrs);
                //find S value
                int innerAttrIndex = getAttrIndex(rightAttrs, condition.rhsAttr);
                AttrType type = rightAttrs[innerAttrIndex].type;
                for (int i = innerIndex; i < inner.size(); i++) {
                    char *innerAttrVal = (char *) calloc(PAGE_SIZE, 1);
                    getAttrVal(innerAttrVal, inner[i].data, innerAttrIndex, rightAttrs);
                    if (compareKey(type, outAttrVal, innerAttrVal) == 0) {
                        joinLeftAndRight(data, out[outIndex].data, out[outIndex].length, inner[i].data,
                                         inner[i].length);
                        free(outAttrVal);
                        free(innerAttrVal);
                        innerIndex = i + 1;
                        return 0;
                    }
                    free(innerAttrVal);
                }
                free(outAttrVal);
                outIndex++;
            }
            if (outIndex >= out.size()) {
                updateInnerVector();
                innerIndex = 0;
                outIndex = 0;
                if (inner.size() == 0) {
                    updateOutVector();
                    innerIndex = 0;
                    outIndex = 0;
                    rightIn->setIterator();
                    updateInnerVector();
                    if (out.size() == 0)
                        return QE_EOF;
                }
            }
        }
    }

    RC BNLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        attrs = this->leftAttrs;
        attrs.insert(attrs.end(), this->rightAttrs.begin(), this->rightAttrs.end());
        return 0;
    }

    RC BNLJoin::getAttrVal(void *val, void *data, int attrIndex, std::vector<Attribute> attrs) {
        int offset = 0;
        int nullIndicatorSize = ceil((double) attrs.size() / 8.0);
        char *nullIndicator = (char *) malloc(nullIndicatorSize);
        memcpy(nullIndicator, (char *) data + offset, nullIndicatorSize);
        offset = offset + nullIndicatorSize;
        for (int i = 0; i < attrs.size(); i++) {
            if (!(nullIndicator[i / 8] & (1 << (7 - i % 8)))) {
                if (attrs[i].type == TypeInt) {
                    if (i == attrIndex) {
                        memcpy(val, (char *) data + offset, sizeof(int));
                        break;
                    }
                    offset += sizeof(int);
                } else if (attrs[i].type == TypeReal) {
                    if (i == attrIndex) {
                        memcpy(val, (char *) data + offset, sizeof(float));
                        break;
                    }
                    offset += sizeof(float);
                } else if (attrs[i].type == TypeVarChar) {
                    int strLen;
                    memcpy(&strLen, (char *) data + offset, 4);
                    if (i == attrIndex) {
                        memcpy(val, (char *) data + offset, strLen + sizeof(int));
                        break;
                    }
                    offset = offset + strLen + 4;
                }
            }
        }
        free(nullIndicator);
        return 0;
    }

    RC BNLJoin::joinLeftAndRight(void *data, void *outData, int outLen, void *innerData, int innerLen) {
        int offset = 0, offset2 = 0;
        int outSize = leftAttrs.size(), innerSize = rightAttrs.size();
        int nullIndicatorSize1 = ceil((double) outSize / 8.0), nullIndicatorSize2 = ceil((double) innerSize / 8.0);
        char *nullIndicator1 = (char *) malloc(nullIndicatorSize1);
        memcpy(nullIndicator1, (char *) outData + offset, nullIndicatorSize1);
        char *nullIndicator2 = (char *) malloc(nullIndicatorSize2);
        memcpy(nullIndicator2, (char *) innerData + offset, nullIndicatorSize2);

        int resNullIndicatorSize = ceil((double) (outSize + innerSize) / 8);
        int resOffset = resNullIndicatorSize;
        memcpy((char *) data, nullIndicator1, nullIndicatorSize1);
        for (int i = outSize; i < outSize + innerSize; i++) {
            if (!(nullIndicator2[(i - outSize) / 8] & (1 << (7 - (i - outSize) % 8)))) {
                ((char *) data)[i / 8] = ((char *) data)[i / 8] & (0 << (7 - (i - nullIndicatorSize1) % 8));
            } else {
                ((char *) data)[i / 8] = ((char *) data)[i / 8] | (1 << (7 - (i - nullIndicatorSize1) % 8));
            }
        }
        memcpy((char *) data + resOffset, (char *) outData + nullIndicatorSize1, outLen - nullIndicatorSize1);
        resOffset += outLen - nullIndicatorSize1;
        memcpy((char *) data + resOffset, (char *) innerData + nullIndicatorSize2, innerLen - nullIndicatorSize2);
        free(nullIndicator1);
        free(nullIndicator2);
        return 0;
    }

    //leftIn getNextTuple:rid,data(nullindicator of returned attributes+real data), rightIn getNextTuple: key, rid
    INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {
        this->leftIn = leftIn;
        this->rightIn = rightIn;
        this->condition = condition;
        leftIn->getAttributes(this->leftAttrs);
        this->leftIndex = getAttrIndex(this->leftAttrs, condition.lhsAttr);
        this->leftBuffer = (char *) calloc(PAGE_SIZE, 1);
        rightIn->getAttributes(this->rightAttrs);
        this->rightIndex = getAttrIndex(this->rightAttrs, condition.rhsAttr);
        this->rightBuffer = (char *) calloc(PAGE_SIZE, 1);
    }

    INLJoin::~INLJoin() {
        free(this->leftBuffer);
        free(this->rightBuffer);
    }

    RC INLJoin::getNextTuple(void *data) {
        while (leftIn->getNextTuple(leftBuffer) != QE_EOF) {
            Value leftVal = getAttrValue(this->leftBuffer, this->leftIndex, this->leftAttrs);
            this->rightIn->setIterator(leftVal.data, leftVal.data, true, true);
            if (rightIn->getNextTuple(this->rightBuffer) != QE_EOF)
                return joinLeftAndRight(data);
        }
        return IX_EOF;
    }

    RC INLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        attrs = this->leftAttrs;
        attrs.insert(attrs.end(), this->rightAttrs.begin(), this->rightAttrs.end());
        return 0;
    }

    RC INLJoin::joinLeftAndRight(void *data) {
        int nullIndicatorSize = ceil((double) (this->leftAttrs.size() + this->rightAttrs.size()) / 8.0);
        memset(data, 0, nullIndicatorSize);
        int offset = nullIndicatorSize;
        //copy left attributes
        int leftOffset = ceil((double) this->leftAttrs.size() / 8.0);
        for (int i = 0; i < this->leftAttrs.size(); i++) {
            char nullField = *((char *) data + i / 8);
            bool isNULL = this->leftBuffer[i / 8] & (1 << (8 - 1 - i % 8));
            if (isNULL) nullField += 1 << (8 - 1 - i % 8);
            else {
                nullField += 0 << (8 - 1 - i % 8);
                if (leftAttrs[i].type == TypeInt) {
                    memcpy((char *) data + offset, leftBuffer + leftOffset, sizeof(int));
                    offset += sizeof(int);
                    leftOffset += sizeof(int);
                } else if (leftAttrs[i].type == TypeReal) {
                    memcpy((char *) data + offset, leftBuffer + leftOffset, sizeof(float));
                    offset += sizeof(float);
                    leftOffset += sizeof(float);
                } else if (leftAttrs[i].type == TypeVarChar) {
                    int strLen = *(int *) (leftBuffer + leftOffset);
                    memcpy((char *) data + offset, leftBuffer + leftOffset, sizeof(int) + strLen);
                    offset += sizeof(int) + strLen;
                    leftOffset += sizeof(int) + strLen;
                }
            }
            ((unsigned char *) data)[i / 8] = nullField;
        }
        //copy right attributes
        int rightOffset = ceil((double) this->rightAttrs.size() / 8.0);
        for (size_t i = 0; i < this->rightAttrs.size(); i++) {
            char nullField = ((char *) data)[(i + this->leftAttrs.size()) / 8];
            bool isNULL = this->rightBuffer[i / 8] & (1 << (8 - 1 - i % 8));
            if (isNULL) nullField += 1 << (8 - 1 - (i + this->leftAttrs.size()) % 8);
            else {
                nullField += 0 << (8 - 1 - (i + this->leftAttrs.size()) % 8);
                if (rightAttrs[i].type == TypeInt) {
                    memcpy((char *) data + offset, rightBuffer + rightOffset, sizeof(int));
                    offset += sizeof(int);
                    rightOffset += sizeof(int);
                } else if (rightAttrs[i].type == TypeReal) {
                    memcpy((char *) data + offset, rightBuffer + rightOffset, sizeof(float));
                    offset += sizeof(float);
                    rightOffset += sizeof(float);
                } else if (rightAttrs[i].type == TypeVarChar) {
                    int strLen = *(int *) (rightBuffer + rightOffset);
                    memcpy((char *) data + offset, rightBuffer + rightOffset, sizeof(int) + strLen);
                    offset += sizeof(int) + strLen;
                    rightOffset += sizeof(int) + strLen;
                }
            }
            ((unsigned char *) data)[(i + this->leftAttrs.size()) / 8] = nullField;
        }
        return 0;
    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, AggregateOp op) {
        this->input = input;
        this->aggAttr = aggAttr;
        this->op = op;
        this->input->getAttributes(this->attrs);
        this->attrIndex = getAttrIndex(this->attrs, aggAttr.name);
        this->end = false;
        this->groupby = false;
    }



    bool Value::operator<(const Value &right) const {
        int leftVal, rightVal;
        if (type == TypeInt) {
            leftVal = *(int *) this->data;
        }
        if (right.type == TypeInt) {
            rightVal = *(int *) right.data;
        }
        return (this->type == right.type) && compareKey(type, this->data, right.data) < 0;
    }

    bool Value::operator==(const Value &right) const {
        int leftVal, rightVal;
        if (type == TypeInt) {
            leftVal = *(int *) this->data;
        }
        if (right.type == TypeInt) {
            rightVal = *(int *) right.data;
        }
        return (this->type == right.type) && compareKey(type, this->data, right.data) == 0;
    }


    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, const Attribute &groupAttr, AggregateOp op) {
        this->input = input;
        this->aggAttr = aggAttr;
        this->groupAttr = groupAttr;
        this->op = op;
        this->input->getAttributes(this->attrs);
        this->attrIndex = getAttrIndex(this->attrs, aggAttr.name);
        this->end = false;
        this->groupby = true;
        char *temp = (char *) malloc(PAGE_SIZE);
        int groupAttrIndex = getAttrIndex(attrs, groupAttr.name);
        while (this->input->getNextTuple(temp) != QE_EOF) {
            Value groupVal = getAttrValue(temp, groupAttrIndex, attrs);
            Value key;
            int keyLength = 0;
            if (groupVal.type == TypeInt) keyLength = sizeof(int);
            else if (groupVal.type == TypeReal) keyLength = sizeof(float);
            else if (groupVal.type == TypeVarChar) keyLength = sizeof(int) + *(int *) groupVal.data;
            key.type = groupVal.type;
            Value resultVal = getAttrValue(temp, attrIndex, attrs);
            AggregateByGroupResult aggregateByGroupResult;
            if (groupResult.find(groupVal) != groupResult.end()) {
                aggregateByGroupResult = groupResult[groupVal];
                key.data = groupVal.data;
            } else {
                key.data = malloc(keyLength);
                memcpy(key.data, groupVal.data, keyLength);
            }
            if (resultVal.type == TypeInt) {
                int val = *(int *) resultVal.data;
                aggregateByGroupResult.sum += val;
                aggregateByGroupResult.count++;
                aggregateByGroupResult.avg = aggregateByGroupResult.sum / aggregateByGroupResult.count;
                aggregateByGroupResult.min = val < aggregateByGroupResult.min ? val : aggregateByGroupResult.min;
                aggregateByGroupResult.max = val > aggregateByGroupResult.max ? val : aggregateByGroupResult.max;
            } else if (resultVal.type == AttrType::TypeReal) {
                float val = *(float *) resultVal.data;
                aggregateByGroupResult.sum += val;
                aggregateByGroupResult.count++;
                aggregateByGroupResult.avg = aggregateByGroupResult.sum / aggregateByGroupResult.count;
                aggregateByGroupResult.min = val < aggregateByGroupResult.min ? val : aggregateByGroupResult.min;
                aggregateByGroupResult.max = val > aggregateByGroupResult.max ? val : aggregateByGroupResult.max;
            }
            int k = *(int *) groupVal.data;
            if (groupResult.find(groupVal) != groupResult.end()) groupResult[key] = aggregateByGroupResult;
            else groupResult.insert(std::make_pair(key, aggregateByGroupResult));

        }
        if (groupResult.size() == 0) this->end = true;
        else groupResultIter = groupResult.begin();
        free(temp);
    }

    Aggregate::~Aggregate() {
        for (auto &i : groupResult) free(i.first.data);
    }

    RC Aggregate::getNextTuple(void *data) {
        if (this->end) return QE_EOF;
        ((char *) data)[0] = 0;
        if (!this->groupby) {
            char *temp = (char *) malloc(PAGE_SIZE);
            float sum = 0, min = std::numeric_limits<float>::max(), max = std::numeric_limits<float>::min();
            int count = 0;
            while (this->input->getNextTuple(temp) != QE_EOF) {
                Value val = getAttrValue(temp, this->attrIndex, this->attrs);
                if (val.type == AttrType::TypeInt) {
                    sum += (float) *(int *) val.data;
                    min = (float) *(int *) val.data < min ? (float) *(int *) val.data : min;
                    max = (float) *(int *) val.data > max ? (float) *(int *) val.data : max;
                } else if (val.type == AttrType::TypeReal) {
                    sum += *(float *) val.data;
                    min = *(float *) val.data < min ? *(float *) val.data : min;
                    max = *(float *) val.data > max ? *(float *) val.data : max;
                }
                count++;
            }
            if (op == AVG) {
                if (count == 0) return QE_EOF;
                float result = sum / count;
                memcpy((char *) data + 1, &result, sizeof(float));
            } else if (op == SUM) {
                if (count == 0) return QE_EOF;
                memcpy((char *) data + 1, &sum, sizeof(float));
            } else if (op == MAX) {
                if (count == 0) return QE_EOF;
                memcpy((char *) data + 1, &max, sizeof(float));
            } else if (op == MIN) {
                if (count == 0) return QE_EOF;
                memcpy((char *) data + 1, &min, sizeof(float));
            } else if (op == COUNT) {
                memcpy((char *) data + 1, &count, sizeof(float));
            }
            free(temp);
            end = true;
        } else {
            if (groupResultIter != groupResult.end()) {
                int keyLength = 0;
                if (groupResultIter->first.type == TypeInt) keyLength = sizeof(int);
                else if (groupResultIter->first.type == TypeReal) keyLength = sizeof(float);
                else if (groupResultIter->first.type == TypeVarChar)
                    keyLength = sizeof(int) +
                                *(int *) groupResultIter->first.data;
                memcpy((char *) data + 1, groupResultIter->first.data, keyLength);
                float count = groupResultIter->second.count;
                if (op == AVG) {
                    if (count == 0) return QE_EOF;
                    memcpy((char *) data + 1 + keyLength, &groupResultIter->second.avg, sizeof(float));
                } else if (op == SUM) {
                    if (count == 0) return QE_EOF;
                    memcpy((char *) data + 1 + keyLength, &groupResultIter->second.sum, sizeof(float));
                } else if (op == MAX) {
                    if (count == 0) return QE_EOF;
                    memcpy((char *) data + 1 + keyLength, &groupResultIter->second.max, sizeof(float));
                } else if (op == MIN) {
                    if (count == 0) return QE_EOF;
                    memcpy((char *) data + 1 + keyLength, &groupResultIter->second.min, sizeof(float));
                } else if (op == COUNT) {
                    memcpy((char *) data + 1 + keyLength, &groupResultIter->second.count, sizeof(float));
                }
                groupResultIter++;
            } else {
                this->end = true;
                return QE_EOF;
            }
        }
        return 0;
    }

    RC Aggregate::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        Attribute attr = this->aggAttr;
        std::string dic[] = {"MIN", "MAX", "COUNT", "SUM", "AVG"};
        if (groupby) {
            attrs.push_back(groupAttr);
            attr.name = dic[op] + "(" + aggAttr.name + ")";
            attr.type = TypeReal;
            attr.length = sizeof(float);
            attrs.push_back(attr);
        } else {
            attr.name = dic[op] + "(" + aggAttr.name + ")";
            attr.type = TypeReal;
            attr.length = sizeof(float);
            attrs.push_back(attr);
        }
        return 0;
    }

    std::string getTableName(std::string s) {
        for (int i = 0; i < s.size(); i++) {
            if (s[i] == '.') return s.substr(0, i);
        }
    }

    GHJoin::GHJoin(Iterator *leftIn, Iterator *rightIn, const Condition &condition, const unsigned int numPartitions) {
        this->leftIn = leftIn;
        this->rightIn = rightIn;
        this->condition = condition;
        this->numPartitions = numPartitions;
        this->leftIn->getAttributes(this->leftAttrs);
        this->rightIn->getAttributes(this->rightAttrs);
        this->leftTableName = getTableName(leftAttrs[0].name);
        this->rightTableName = getTableName(rightAttrs[0].name);
        for (int i = 0; i < leftAttrs.size(); i++) leftAttrNames.push_back(leftAttrs[i].name);
        for (int i = 0; i < rightAttrs.size(); i++) rightAttrNames.push_back(rightAttrs[i].name);
        int rightAttrIndex = getAttrIndex(rightAttrs, condition.rhsAttr);
        this->attrType = rightAttrs[rightAttrIndex].type;
        //partition
        for (int i = 0; i < numPartitions; i++) {
            std::string leftPartition =
                    "left_join" + std::to_string(globalId) + "_" + leftTableName + std::to_string(i);
            std::string rightPartition =
                    "right_join" + std::to_string(globalId) + "_" + rightTableName + std::to_string(i);
            RelationManager::instance().createTable(leftPartition, leftAttrs);
            RelationManager::instance().createTable(rightPartition, rightAttrs);
        }
        insertIntoPartition("left");
        insertIntoPartition("right");
        //build hashmap for out loop
        createLeftMap(0);
        //scan a right tuple
        RelationManager::instance().scan("right_join" + std::to_string(globalId) + "_" + rightTableName + "0", "",
                                         NO_OP, NULL, rightAttrNames, rmScanIterator);
        innerBuffer = malloc(PAGE_SIZE);
        rmScanIterator.getNextTuple(rid, innerBuffer);
    }

    GHJoin::~GHJoin() {
        for (auto &i : map) {
            for (auto &j : i.second)
                free(j.data);
        }
        map.clear();
        for (int i = 0; i < numPartitions; i++) {
            std::string leftPartition =
                    "left_join" + std::to_string(globalId) + "_" + leftTableName + std::to_string(i);
            std::string rightPartition =
                    "right_join" + std::to_string(globalId) + "_" + rightTableName + std::to_string(i);
            RelationManager::instance().deleteTable(leftPartition);
            RelationManager::instance().deleteTable(rightPartition);
        }
    }

    RC GHJoin::getNextTuple(void *data) {
        int rightAttrIndex = getAttrIndex(rightAttrs, condition.rhsAttr);
        while (true) {
            Value val = getAttrValue(innerBuffer, rightAttrIndex, rightAttrs);
            int len = getDataLength(innerBuffer, rightAttrs);
            Tuple tuple(innerBuffer, len);
            if (map.find(val) != map.end()) {
                if (mapVectorIndex < map[val].size()) {
                    joinLeftAndRight(data, map[val][mapVectorIndex].data, map[val][mapVectorIndex].length, innerBuffer,
                                     len);
                    mapVectorIndex++;
                    return 0;
                } else {//since the vector has been traversed, move to the next right tuple
                    free(innerBuffer);
                    innerBuffer = malloc(PAGE_SIZE);
                    mapVectorIndex = 0;
                    if (rmScanIterator.getNextTuple(rid, innerBuffer) != RM_EOF);
                    else {//this right partition has been used, move to the next right partition
                        rmScanIterator.close();
                        rightPartition++;
                        //if all right partitions have been used, create a new left map
                        if (rightPartition >= numPartitions) {
                            rightPartition = 0;
                            //free left map
                            for (auto &i : map) {
                                for (auto &j : i.second)
                                    free(j.data);
                            }
                            map.clear();
                            leftPartition++;
                            if (leftPartition >= numPartitions) {
                                free(innerBuffer);
                                return QE_EOF;
                            } else {
                                createLeftMap(leftPartition);
                                RelationManager::instance().scan(
                                        "right_join" + std::to_string(globalId) + "_" + rightTableName +
                                        std::to_string(rightPartition), "", NO_OP, NULL, rightAttrNames,
                                        rmScanIterator);
                                rmScanIterator.getNextTuple(rid, innerBuffer);
                            }
                        } else {//right partitions not used up, move to next right partition
                            RelationManager::instance().scan(
                                    "right_join" + std::to_string(globalId) + "_" + rightTableName +
                                    std::to_string(rightPartition), "", NO_OP, NULL, rightAttrNames, rmScanIterator);
                            rmScanIterator.getNextTuple(rid, innerBuffer);
                        }
                    }
                }
            } else {//didn't find key in map, move to the next tuple, the following is copied from above
                free(innerBuffer);
                innerBuffer = malloc(PAGE_SIZE);
                mapVectorIndex = 0;
                if (rmScanIterator.getNextTuple(rid, innerBuffer) != RM_EOF);
                else {//this right partition has been used, move to the next right partition
                    rmScanIterator.close();
                    rightPartition++;
                    //if all right partitions have been used, create a new left map
                    if (rightPartition >= numPartitions) {
                        rightPartition = 0;
                        //free left map
                        for (auto &i : map) {
                            for (auto &j : i.second)
                                free(j.data);
                        }
                        map.clear();
                        leftPartition++;
                        if (leftPartition >= numPartitions) {
                            free(innerBuffer);
                            return QE_EOF;
                        } else {
                            createLeftMap(leftPartition);
                            RelationManager::instance().scan(
                                    "right_join" + std::to_string(globalId) + "_" + rightTableName +
                                    std::to_string(rightPartition), "", NO_OP, NULL, rightAttrNames, rmScanIterator);
                            rmScanIterator.getNextTuple(rid, innerBuffer);
                        }
                    } else {//right partitions not used up, move to next right partition
                        RelationManager::instance().scan(
                                "right_join" + std::to_string(globalId) + "_" + rightTableName +
                                std::to_string(rightPartition), "", NO_OP, NULL, rightAttrNames, rmScanIterator);
                        rmScanIterator.getNextTuple(rid, innerBuffer);
                    }
                }
            }
        }
    }

    RC GHJoin::getAttributes(std::vector<Attribute> &attrs) const {
        attrs = this->leftAttrs;
        attrs.insert(attrs.end(), this->rightAttrs.begin(), this->rightAttrs.end());
        return 0;
    }

    void GHJoin::insertIntoPartition(std::string s) {
        std::string tableName;
        std::vector<Attribute> attrs;
        Iterator *iterator;
        int attrIndex;
        if (s == "left") {
            tableName = leftTableName;
            attrs = leftAttrs;
            iterator = leftIn;
            attrIndex = getAttrIndex(attrs, condition.lhsAttr);
        } else if (s == "right") {
            tableName = rightTableName;
            attrs = rightAttrs;
            iterator = rightIn;
            attrIndex = getAttrIndex(attrs, condition.rhsAttr);
        }

        void *data = malloc(PAGE_SIZE);
        while (iterator->getNextTuple(data) != QE_EOF) {
            Value val = getAttrValue(data, attrIndex, attrs);
            if (attrType == TypeInt) {
                int partitionIndex = *(int *) val.data % numPartitions;
                RelationManager::instance().insertTuple(
                        s + "_join" + std::to_string(globalId) + "_" + tableName + std::to_string(partitionIndex), data,
                        rid);
            } else if (attrType == TypeReal) {
                int partitionIndex = (int) *(float *) val.data % numPartitions;
                RelationManager::instance().insertTuple(
                        s + "_join" + std::to_string(globalId) + "_" + tableName + std::to_string(partitionIndex), data,
                        rid);
            } else {
                int strLen = *(int *) val.data;
                char *charArr = (char *) malloc(strLen + 1);
                memcpy(charArr, (char *) val.data + 1, strLen);
                charArr[strLen] = '\0';
                std::string tmp = charArr;
                free(charArr);
                int partitionIndex = std::hash<std::string>{}(tmp) % numPartitions;
                RelationManager::instance().insertTuple(
                        s + "_join" + std::to_string(globalId) + "_" + tableName + std::to_string(partitionIndex), data,
                        rid);
            }
        }
        free(data);
    }

    void GHJoin::createLeftMap(int leftPartition) {
        RelationManager::instance().scan(
                "left_join" + std::to_string(globalId) + "_" + leftTableName + std::to_string(leftPartition), "", NO_OP,
                NULL, leftAttrNames, rmScanIterator);
        void *data = malloc(PAGE_SIZE);
        int attrIndex = getAttrIndex(leftAttrs, condition.lhsAttr);
        while (rmScanIterator.getNextTuple(rid, data) != -1) {
            Value val = getAttrValue(data, attrIndex, leftAttrs);
            int len = getDataLength(data, leftAttrs);
            Tuple tuple(data, len);
            if (map.find(val) != map.end()) {
                map[val].push_back(tuple);
            } else {
                std::vector<Tuple> tupleVector;
                tupleVector.push_back(tuple);
                map.insert(std::make_pair(val, tupleVector));
            }
        }
        free(data);
        rmScanIterator.close();
    }

    RC GHJoin::joinLeftAndRight(void *data, void *outData, int outLen, void *innerData, int innerLen) {
        int offset = 0, offset2 = 0;
        int outSize = leftAttrs.size(), innerSize = rightAttrs.size();
        int nullIndicatorSize1 = ceil((double) outSize / 8.0), nullIndicatorSize2 = ceil((double) innerSize / 8.0);
        char *nullIndicator1 = (char *) malloc(nullIndicatorSize1);
        memcpy(nullIndicator1, (char *) outData + offset, nullIndicatorSize1);
        char *nullIndicator2 = (char *) malloc(nullIndicatorSize2);
        memcpy(nullIndicator2, (char *) innerData + offset, nullIndicatorSize2);

        int resNullIndicatorSize = ceil((double) (outSize + innerSize) / 8);
        int resOffset = resNullIndicatorSize;
        memcpy((char *) data, nullIndicator1, nullIndicatorSize1);
        for (int i = outSize; i < outSize + innerSize; i++) {
            if (!(nullIndicator2[(i - outSize) / 8] & (1 << (7 - (i - outSize) % 8)))) {
                ((char *) data)[i / 8] = ((char *) data)[i / 8] & (0 << (7 - (i - nullIndicatorSize1) % 8));
            } else {
                ((char *) data)[i / 8] = ((char *) data)[i / 8] | (1 << (7 - (i - nullIndicatorSize1) % 8));
            }
        }
        memcpy((char *) data + resOffset, (char *) outData + nullIndicatorSize1, outLen - nullIndicatorSize1);
        resOffset += outLen - nullIndicatorSize1;
        memcpy((char *) data + resOffset, (char *) innerData + nullIndicatorSize2, innerLen - nullIndicatorSize2);
        free(nullIndicator1);
        free(nullIndicator2);
        return 0;
    }
} // namespace PeterDB