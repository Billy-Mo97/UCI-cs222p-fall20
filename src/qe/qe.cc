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
        res.data = (char *) data + offset;
        res.type = attrs[index].type;
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
        this->input = input;
        this->aggAttr = aggAttr;
        this->op = op;
        this->input->getAttributes(this->attrs);
        this->attrIndex = getAttrIndex(this->attrs, aggAttr.name);
        this->end = false;
        this->groupby = false;
    }

    bool Value::operator<(const Value &right) const {
        return (this->type == right.type) && compareKey(type, this->data, right.data) < 0;
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
                else if (groupResultIter->first.type == TypeVarChar) keyLength = sizeof(int) +
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
            attr.name = groupAttr.name + " " + dic[op] + "(" + aggAttr.name + ")";
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
} // namespace PeterDB