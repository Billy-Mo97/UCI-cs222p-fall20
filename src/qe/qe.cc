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
            if (memcmp(value, condition.rhsValue.data, sizeof(int)) == 0) { return true; }
        } else if (type == TypeReal) {
            if (memcmp(value, condition.rhsValue.data, sizeof(float)) == 0) { return true; }
        } else if (type == TypeVarChar) {
            int strLen;
            memcpy(&strLen, (char *) value, sizeof(int));
            if (memcmp(value, condition.rhsValue.data, sizeof(int) + strLen) == 0) { return true; }
        }
        return false;
    }

    RC Filter::getNextTuple(void *data) {
        std::vector<Attribute> attributes;
        if (getAttributes(attributes) == -1) { return -1; }
        int condIndex = -1;
        for (int i = 0; i < attributes.size(); i++) {
            std::string condName;
            if (getAttributeName(condition.lhsAttr, condName) == -1) { continue; }
            if (condName == attributes[i].name) {
                condIndex = i;
                break;
            }
        }
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

    INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {

    }

    INLJoin::~INLJoin() {

    }

    RC INLJoin::getNextTuple(void *data) {
        return -1;
    }

    RC INLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
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