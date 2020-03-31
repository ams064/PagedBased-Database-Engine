#include "qe.h"

/************************* QE Utils Start ***************************************/

/**
 * getColumnData() :  get the value of a given column in a record/tuple.
 * @argument1 : record data.
 * @argument2 : column number to be read.
 * @argument3 : record descriptor/list of columns.
 * @argument4 : buffer containing column data (out parameter).
 *
 * Return : length of the data being returned, -1 if data is NULL.
*/
RC QueryEngineUtils::getColumnData(const void* data, const int columnNum,
                                   const std::vector<Attribute>& attributes,
                                   void* columnData) {

    if(attributes.size() == 0) return -1;

    RT nullBytes = ceil((double)attributes.size()/CHAR_BIT);
    char nullBitField[nullBytes];
    memset(nullBitField, 0, nullBytes);
    memcpy(nullBitField, (char*)data, nullBytes);
    RT recordCounter = nullBytes;

    for(int i = 0 ; i < (int)attributes.size(); i++) {
        int shift = CHAR_BIT - 1 - i%CHAR_BIT;
        int nullbit = nullBitField[i/CHAR_BIT] & (1 << (shift));
        if(nullbit) {
            if(i == columnNum) return -1;
        } else {
            if(attributes[i].type == TypeReal) {
                if(i == columnNum) {
                    memcpy((char*)columnData, (char*)data + recordCounter, sizeof(float));
                    return sizeof(int);
                }
                recordCounter += sizeof(float);
            } else if (attributes[i].type == TypeInt) {
                if(i == columnNum) {
                    memcpy((char*)columnData, (char*)data + recordCounter, sizeof(int));
                    return sizeof(int);
                }
                recordCounter += sizeof(int);
            } else {
                int varcharlen = 0;
                memcpy((char*)&varcharlen, (char*)data + recordCounter, sizeof(int));
                if(i == columnNum) {
                    memcpy((char*)columnData, (char*)data + recordCounter, varcharlen + sizeof(int));
                    return varcharlen;
                }
                recordCounter += varcharlen + sizeof(int);
              }
        }
    }

    return -1;
}

/**
 * getColumnData() :  breaks a record into ColumnsData vector.
 * @argument1 : record data.
 * @argument2 : record descriptor/list of columns.
 * @argument3 : vector containing columns data (out parameter).
 *
 * Return : void.
*/
void QueryEngineUtils::decomposeIntoColumns(const void* data,
                                            const std::vector<Attribute>& attributes,
                                            std::vector<ColumnData>& colData) {

    if(attributes.size() == 0) return;

    RT nullBytes = ceil((double)attributes.size()/CHAR_BIT);
    char nullBitField[nullBytes];
    memset(nullBitField, 0, nullBytes);
    memcpy(nullBitField, (char*)data, nullBytes);
    RT recordCounter = nullBytes;

    for(int i = 0 ; i < (int)attributes.size(); i++) {
        int shift = CHAR_BIT - 1 - i%CHAR_BIT;
        int nullbit = nullBitField[i/CHAR_BIT] & (1 << (shift));
        if(nullbit) {
            ColumnData nullData;
            colData.push_back(nullData);
        } else {
            if(attributes[i].type == TypeReal) {
                colData[i].data = malloc(sizeof(float));
                memcpy((char*)colData[i].data, (char*)data + recordCounter, sizeof(float));
                colData[i].length = sizeof(float);
                recordCounter += sizeof(float);
            } else if (attributes[i].type == TypeInt) {
                colData[i].data = malloc(sizeof(int));
                memcpy((char*)colData[i].data, (char*)data + recordCounter, sizeof(int));
                colData[i].length = sizeof(int);
                recordCounter += sizeof(int);
            } else {
                int varcharlen = 0;
                memcpy(&varcharlen, (char*)data + recordCounter, sizeof(int));
                colData[i].data = malloc(varcharlen + sizeof(int));
                memcpy((char*)colData[i].data, (char*)data + recordCounter, varcharlen + sizeof(int));
                colData[i].length = varcharlen + sizeof(int);
                recordCounter += varcharlen + sizeof(int);
              }
        }
    }

    return;
}

/**
 * getPositionOfAttribute() :  gets the position of a given attribute in a record/tuple.
 * @argument1 : name of the attribute.
 * @argument2 : record descriptor/list of columns.
 * @argument3 : position to be returned (out parameter).
 *
 * Return : void.
*/
void QueryEngineUtils::getPositionOfAttribute(const std::string& attributeName,
                                              const std::vector<Attribute>& attributes,
                                              int& colPos) {
    if(attributes.size() == 0) {
        colPos = -1;
        return;
    }

    for(int i = 0 ; i < (int)attributes.size(); i++) {
        if(attributes[i].name == attributeName) {
            colPos = i;
            return;
        }
    }

    colPos = -1;
    return;
}

/**
 * getTupleSize() :  gets the size of a record/tuple.
 * @argument1 : record data.
 * @argument2 : record descriptor/list of columns.
 *
 * Return : size of the tuple.
*/
int QueryEngineUtils::getTupleSize(const void* data, const std::vector<Attribute>& attributes) {

    if(attributes.size() == 0) return -1;

    RT nullBytes = ceil((double)attributes.size()/CHAR_BIT);
    char nullBitField[nullBytes];
    memset(nullBitField, 0, nullBytes);
    memcpy(nullBitField, (char*)data, nullBytes);
    RT recordCounter = nullBytes;

    for(int i = 0 ; i < (int)attributes.size(); i++) {
        int shift = CHAR_BIT - 1 - i%CHAR_BIT;
        int nullbit = nullBitField[i/CHAR_BIT] & (1 << (shift));
        if(!nullbit) {
            if(attributes[i].type == TypeReal) {
                recordCounter += sizeof(float);
            } else if (attributes[i].type == TypeInt) {
                recordCounter += sizeof(int);
            } else {
                int varcharlen = 0;
                memcpy(&varcharlen, (char*)data + recordCounter, sizeof(int));
                recordCounter += varcharlen + sizeof(int);
            }
        }
    }

    return recordCounter;
}

/**
 * getTupleFromBlock() :  reads a tuple from in-memory block.
 * @argument1 : block data.
 * @argument2 : offset from which to read.
 * @argument3 : record descriptor/list.
 * @argument4 : tuple data to be returned (out parameter).
 *
 * Return : size of the tuple.
*/
int QueryEngineUtils::getTupleFromBlock(const void* block, const int readOffset,
                                        const std::vector<Attribute>& attrs,
                                        void* tuple) {

    int tupleSize = getTupleSize((char*)block + readOffset, attrs);
    memcpy((char*)tuple, (char*)block + readOffset, tupleSize);
    return tupleSize;
}

/**
 * concatTuples() :  Concatenates 2 tuples.
 * @argument1 : left tuple.
 * @argument2 : record descriptor/list of left tuple.
 * @argument3 : right tuple.
 * @argument4 : record descriptor/list of right tuple.
 * @argument5 : concatenated data to be returned (out parameter).
 *
 * Return : void.
*/
void QueryEngineUtils::concatTuples(const void* lTuple, const std::vector<Attribute>& lAttrs,
                                    const void* rTuple, const std::vector<Attribute>& rAttrs,
                                    void* data) {

    int totalAttrs = lAttrs.size() + rAttrs.size();
    int nullBytes = ceil((double)totalAttrs/CHAR_BIT);
    int lNullBytes = ceil((double)lAttrs.size()/CHAR_BIT);
    int rNullBytes = ceil((double)rAttrs.size()/CHAR_BIT);

    char nullBitField[nullBytes];
    char lNullBitField[lNullBytes];
    char rNullBitField[rNullBytes];
    memset(nullBitField, 0, nullBytes);
    memset(lNullBitField, 0, lNullBytes);
    memset(rNullBitField, 0, rNullBytes);

    memcpy((char*)lNullBitField, lTuple, lNullBytes);
    memcpy((char*)rNullBitField, rTuple, rNullBytes);

    int lOffset = lNullBytes;
    int rOffset = rNullBytes;
    int offset = nullBytes;
    int nullIndex = 0;

    // Copying left tuple
    for(int i = 0; i < (int)lAttrs.size(); i++) {
        int shift = CHAR_BIT - 1 - i%CHAR_BIT;
        int nullbit = lNullBitField[i/CHAR_BIT] & (1 << (shift));
        if(nullbit)
            nullBitField[nullIndex/CHAR_BIT] |= (1 << (CHAR_BIT - 1 - nullIndex%CHAR_BIT));
        nullIndex++;
        if(!nullbit) {
            if(lAttrs[i].type == TypeReal) {
                memcpy((char*)data + offset, (char*)lTuple + lOffset, sizeof(float));
                lOffset += sizeof(float);
                offset += sizeof(float);
            } else if (lAttrs[i].type == TypeInt) {
                memcpy((char*)data + offset, (char*)lTuple + lOffset, sizeof(int));
                lOffset += sizeof(int);
                offset += sizeof(int);
            } else {
                int varcharlen = 0;
                memcpy(&varcharlen, (char*)lTuple + lOffset, sizeof(int));
                memcpy((char*)data + offset, (char*)lTuple + lOffset, varcharlen + sizeof(int));
                lOffset += varcharlen + sizeof(int);
                offset += varcharlen + sizeof(int);
            }
        }
    }

    //Copying right tuple
    for(int i = 0; i < (int)rAttrs.size(); i++) {
        int shift = CHAR_BIT - 1 - i%CHAR_BIT;
        int nullbit = rNullBitField[i/CHAR_BIT] & (1 << (shift));
        if(nullbit)
            nullBitField[nullIndex/CHAR_BIT] |= (1 << (CHAR_BIT - 1 - nullIndex%CHAR_BIT));
        nullIndex++;
        if(!nullbit) {
            if(rAttrs[i].type == TypeReal) {
                memcpy((char*)data + offset, (char*)rTuple + rOffset, sizeof(float));
                rOffset += sizeof(float);
                offset += sizeof(float);
            } else if (rAttrs[i].type == TypeInt) {
                memcpy((char*)data + offset, (char*)rTuple + rOffset, sizeof(int));
                rOffset += sizeof(int);
                offset += sizeof(int);
            } else {
                int varcharlen = 0;
                memcpy(&varcharlen, (char*)rTuple + rOffset, sizeof(int));
                memcpy((char*)data + offset, (char*)rTuple + rOffset, varcharlen + sizeof(int));
                rOffset += varcharlen + sizeof(int);
                offset += varcharlen + sizeof(int);
            }
        }
    }

    //Copying null bytes to concateanated tuple
    memcpy((char*)data, (char*)nullBitField, nullBytes);
    return;
}

/**
 * compare() - wrapper to compare 2 data of same types of based on given CompOp.
 * @argument1 : data 1.
 * @argument2 : data 2.
 * @argument3 : flag to check wether data 1 is NULL.
 * @argument4 : flag to check wether data 2 is NULL.
 * @argument5 : Comparison operator.
 *
 * Support (varchar/int/floats).
 *
 * Return : true/false based on comparison.
 */
bool QueryEngineUtils::compare(const void* data1, const void* data2, const int lhsNull,
                               const int rhsNull, const int dataType, const CompOp op) {

    /* Comparison functions defined in rbfm.cc */
    if(lhsNull == -1 || rhsNull == -1) return false;

    if(dataType == TypeReal) {
        return compareTypeReal(data1, data2, op);
    } else if (dataType == TypeInt) {
        return compareTypeInt(data1, data2, op);
    }

    return compareTypeVarChar(data1, data2, op);
}

/**
 * getPartitionToInsert() - get the GHJoin partition to insert.
 * @argument1 : data used to get hash value.
 * @argument2 : type of the data.
 * @argument3 : flag to check wether data is NULL, else has the length of data.
 * @argument4 : num partitions available.
 *
 * Support (varchar/int/floats).
 *
 * Return : -1 if failed, else returns the partition number.
 */
int QueryEngineUtils::getPartitionToInsert(const void* data, int type, int isNull, int numPartitions) {
    if(type == TypeInt) {
        return ((std::hash<int>{}(*(int*)data))%numPartitions);
    } else if(type == TypeReal) {
        return ((std::hash<float>{}(*(float*)data))%numPartitions);
    } else {
        return ((std::hash<std::string>{}(std::string((char*)data + sizeof(int), isNull)))%numPartitions);
    }
    return -1;
}

/**
 * insertInHashTable() - insert into a hash table.
 * @argument1 : hash map.
 * @argument2 : key to be inserted.
 * @argument3 : flag to check wether data is NULL, else has the length of data.
 * @argument4 : value of the key to be inserted.
 * @argument5 : type of the key to be inserted.
 *
 * Support (varchar/int/floats).
 *
 * Return : void.
 */
void QueryEngineUtils::insertInHashTable(void* hashMap, const void* key, int keyLen, int val, int type) {
    if(type == TypeInt) {
        int k = *(int*)key;
        (*(unordered_map<int, std::vector<int>>*)hashMap)[k].push_back(val);
    } else if(type == TypeReal) {
        float k = *(float*)key;
        (*(unordered_map<float, std::vector<int>>*)hashMap)[k].push_back(val);
    } else {
        std::string k((char*)key + sizeof(int), keyLen);
        ( *(unordered_map<std::string, std::vector<int>>*)hashMap)[k].push_back(val);
    }
    return;
}

/**
 * eraseHashTableData() - clear the hash table.
 * @argument1 : hash map.
 * @argument2 : type of keys in hash map.
 *
 * Support (varchar/int/floats).
 *
 * Return : void.
 */
void QueryEngineUtils::eraseHashTableData(void* hashMap, int type) {
    if(type == TypeInt) {
        (*(unordered_map<int, std::vector<int>>*)hashMap).clear();
    } else if(type == TypeReal) {
        (*(unordered_map<float, std::vector<int>>*)hashMap).clear();
    } else {
        (*(unordered_map<std::string, std::vector<int>>*)hashMap).clear();
    }
    return;
}

/**
 * getTupleOffsetFromHashTable() - get the tuple offset from hash map.
 * @argument1 : hash map.
 * @argument2 : key for which to get the tuple offset.
 * @argument3 : length of the key.
 * @argument4 : type of the key.
 *
 * Support (varchar/int/floats).
 *
 * Return : vector of tuple offsets.
 */
std::vector<int> QueryEngineUtils::getTupleOffsetFromHashTable(void* hashMap, const void* key,
                                                               int keyLen, int type) {
    if(type == TypeInt) {
         int k = *(int*)key;
         auto& refHash = *(unordered_map<int, std::vector<int>>*)hashMap;
         if(refHash.find(k) != refHash.end()) {
             return refHash[k];
         }
    } else if(type == TypeReal) {
         float k = *(float*)key;
         auto& refHash = *(unordered_map<float, std::vector<int>>*)hashMap;
         if(refHash.find(k) != refHash.end()) {
             return refHash[k];
         }
    } else {
         std::string k((char*)key + sizeof(int), keyLen);
         auto& refHash = *(unordered_map<std::string, std::vector<int>>*)hashMap;
         if(refHash.find(k) != refHash.end()) {
             return refHash[k];
         }
    }
    return vector<int>();
}

/* I know, i know, this function shouldnt even exist, kinda hacky way to do it */
int QueryEngineUtils::getJoinCounter() {
    std::string fileName = "GhJoin.meta";
    if(RelationManager::instance().isTableExist(fileName)) {
        std::fstream file;
        file.open(fileName.c_str(), std::ios::in | std::ios::out);
        int a = 0;
        file.seekg(0);
        file.read((char*)&a, sizeof(int));
        a++;
        file.seekp(0);
        file.write((char*)&a, sizeof(int));
        return a-1;
    } else {
        int a = 1;
        std::fstream file;
        file.open(fileName.c_str(), std::ios::out);
        file.seekp(0);
        file.write((char*)&a, sizeof(int));
        return a-1;
    }

    return 0;
}

/***************** QE Utils End ************************/

Filter::Filter(Iterator *input, const Condition &condition) {
    this->filterIterator = input;
    this->filterCondition = condition;
    //get the attribbutes
    input->getAttributes(this->filterAttributes);
    //get column position
    QueryEngineUtils::getPositionOfAttribute(this->filterCondition.lhsAttr,
                                             this->filterAttributes,
                                             this->lhsColPos);

    this->lhsAttrVal = malloc(PAGE_SIZE);
    this->dataType = this->filterAttributes[this->lhsColPos].type;

    if(this->filterCondition.bRhsIsAttr) {
        QueryEngineUtils::getPositionOfAttribute(this->filterCondition.rhsAttr,
                                                 this->filterAttributes,
                                                 this->rhsColPos);

        this->rhsAttrVal = malloc(PAGE_SIZE);
    } else {
        this->rhsColPos = INT_MAX;
    }

}

RC Filter::getNextTuple(void* data) {
    if(this->lhsColPos == -1 || this->rhsColPos == -1) return QE_EOF;

    if(this->filterCondition.op == NO_OP) {
        return this->filterIterator->getNextTuple(data);
    }

    while(this->filterIterator->getNextTuple(data) != QE_EOF) {

        RC lhsRet = QueryEngineUtils::getColumnData(data, this->lhsColPos, this->filterAttributes,
                                                    this->lhsAttrVal);

        if(this->rhsColPos != INT_MAX) {
            RC rhsRet = QueryEngineUtils::getColumnData(data, this->rhsColPos, this->filterAttributes,
                                                        this->rhsAttrVal);
            if(QueryEngineUtils::compare(this->lhsAttrVal, this->rhsAttrVal,
                                         lhsRet, rhsRet, this->dataType, this->filterCondition.op)) return 0;
        } else {
            if(QueryEngineUtils::compare(this->lhsAttrVal, this->filterCondition.rhsValue.data,
                                         lhsRet, 0, this->dataType, this->filterCondition.op)) return 0;
        }
    }

    return QE_EOF;
}

void Filter::getAttributes(std::vector<Attribute>& attrs) const {
    attrs = this->filterAttributes;
    return;
}

Project::Project(Iterator *input, const std::vector<std::string> &attrNames) {
    this->projectIterator = input;
    input->getAttributes(this->projectAttributes);
    this->projectData = malloc(PAGE_SIZE);
    for(int i = 0; i < (int)attrNames.size(); i++) {
        int colPos = 0;
        QueryEngineUtils::getPositionOfAttribute(attrNames[i], this->projectAttributes, colPos);
        this->projectColPos.push_back(colPos);
    }

    for(int i = 0 ; i < (int)attrNames.size(); i++) {
        for(int j = 0 ; j < (int)this->projectAttributes.size(); j++) {
            if(this->projectAttributes[j].name == attrNames[i]) {
                this->changedAttrs.push_back(projectAttributes[j]);
            }
        }
    }
}

RC Project::getNextTuple(void* data) {
    bool endFlag = true;
    if(this->projectIterator->getNextTuple(this->projectData) != QE_EOF)
        endFlag = false;
    
    if(endFlag) return QE_EOF;
    std::vector<ColumnData> colData(this->projectAttributes.size());
    QueryEngineUtils::decomposeIntoColumns(this->projectData, this->projectAttributes, colData);

    RT nullBytes = ceil((double)this->projectColPos.size()/CHAR_BIT);
    char nullBitField[nullBytes];
    memset(nullBitField, 0, nullBytes);
    RT colOffset = nullBytes;

    for(int i = 0; i < (int)this->projectColPos.size(); i++) {
        int j = this->projectColPos[i];
        if(colData[j].data == NULL) {
            int shift = CHAR_BIT - 1 - j%CHAR_BIT;
            nullBitField[j/CHAR_BIT] = nullBitField[j/CHAR_BIT] | (1 << (shift));
        }
        memcpy((char*)data + colOffset, (char*)colData[j].data, colData[j].length);
        colOffset += colData[j].length;
    }

    memcpy((char*)data, (char*)nullBitField, nullBytes);
    return 0;
}

void Project::getAttributes(std::vector<Attribute>& attrs) const {
    attrs = this->changedAttrs;
    return;
}


Aggregate::Aggregate(Iterator* input, const Attribute &aggAttr, AggregateOp op) {
    this->aggrIterator = input;
    input->getAttributes(this->aggrAttributes);
    this->aggrOp = op;
    this->aggrAttribute = aggAttr;
    this->aggrData = malloc(PAGE_SIZE);
    this->aggrColData = malloc(sizeof(int));
    this->aggrType = aggAttr.type;
    this->endFlag = false;
    this->isGroupBy = false;
    QueryEngineUtils::getPositionOfAttribute(aggAttr.name, this->aggrAttributes, this->aggrColPos);
    aggrStat[0] = FLT_MAX;
    aggrStat[1] = FLT_MIN;
    aggrStat[2] = 0.0;
    aggrStat[3] = 0.0;
    aggrStat[4] = 0.0;
    nonNullCount = 0.0;
}

Aggregate::Aggregate(Iterator* input, const Attribute &aggAttr, const Attribute &groupAttr, AggregateOp op) {
    this->aggrIterator = input;
    input->getAttributes(this->aggrAttributes);
    this->aggrOp = op;
    this->aggrAttribute = aggAttr;
    this->groupAttr = groupAttr;
    this->isGroupBy = true;
    this->aggrData = malloc(PAGE_SIZE);
    this->aggrColData = malloc(sizeof(int));
    this->aggrType = aggAttr.type;
    this->groupType = groupAttr.type;
    this->groupData = malloc(PAGE_SIZE);
    this->aggrDone = false; 
    this->endFlag = false;
    QueryEngineUtils::getPositionOfAttribute(aggAttr.name, this->aggrAttributes, this->aggrColPos);
    QueryEngineUtils::getPositionOfAttribute(groupAttr.name, this->aggrAttributes, this->groupColPos);
}

Aggregate::~Aggregate() {
    if(this->aggrData != NULL)
        free(aggrData);
    if(this->aggrColData != NULL) 
        free(aggrColData);
    if(this->groupData != NULL) 
        free(groupData);
}

RC Aggregate::groupBy(const float val, const void* data, const int isNull) {
    if(this->groupType == TypeInt) {
        int group = 0;
        memcpy((char*)&group, (char*)data, sizeof(int));
        if(groupsInt.find(group) != groupsInt.end()) {
            if(isNull == 0) {
                groupsInt[group][0] = min(groupsInt[group][0], val);
                groupsInt[group][1] = max(groupsInt[group][1], val);
                groupsInt[group][3] += val;
            }
            groupsInt[group][2]++;
            nonNullCount++;
            groupsInt[group][4] = groupsInt[group][3]/(nonNullCount);
        } else {
            for(int i = 0 ; i < 5; i++) {
                groupsInt[group].push_back(val);
            }
        }
    } else if(this->groupType ==TypeReal) {
        int group = 0;
        memcpy((char*)&group, (char*)data, sizeof(int));
        if(groupsReal.find(group) != groupsReal.end()) {
            if(isNull == 0) {
                groupsReal[group][0] = min(groupsReal[group][0], val);
                groupsReal[group][1] = max(groupsReal[group][1], val);
                groupsReal[group][3] += val;
            }
            groupsReal[group][2]++;
            nonNullCount++;
            groupsReal[group][4] = groupsReal[group][3]/nonNullCount;
        } else {
            for(int i = 0 ; i < 5; i++) {
                groupsReal[group].push_back(val);
            }
        }
    } else {
        std::string group((char*)data + sizeof(int), isNull);
        if(groupsVarChar.find(group) != groupsVarChar.end()) {
            if(isNull == 0) {
                groupsVarChar[group][0] = min(groupsVarChar[group][0], val);
                groupsVarChar[group][1] = max(groupsVarChar[group][1], val);
                groupsVarChar[group][3] += val;
            }
            groupsVarChar[group][2]++;
            nonNullCount++;
            groupsVarChar[group][4] = groupsVarChar[group][3]/nonNullCount;
        } else {
            for(int i = 0 ; i < 5; i++) {
                groupsVarChar[group].push_back(val);
            }
        }

    }
    return 0;
}

RC Aggregate::getGroups(void* data) {
    if(groupsVarChar.size() == 0 && groupsInt.size() == 0 && groupsReal.size() == 0) return -1;
    char nullBytes[1];
    memset(nullBytes, 0 , 1);
    memcpy((char*)data, (char*)nullBytes, 1);
    if(this->groupType == TypeReal) {
        memcpy((char*)data + 1, (char*)&(groupsReal.begin()->first), sizeof(float));
        memcpy((char*)data + 1 + sizeof(float), (char*)&(groupsReal.begin()->second[this->aggrOp]), sizeof(float));
        groupsInt.erase(groupsInt.begin());
    } else if(this->groupType == TypeInt) {
        memcpy((char*)data + 1, (char*)&(groupsInt.begin()->first), sizeof(int));
        memcpy((char*)data + 1 + sizeof(int), (char*)&(groupsInt.begin()->second[this->aggrOp]), sizeof(float));
        groupsInt.erase(groupsInt.begin());
    } else {
        int len = 0;
        len = groupsVarChar.begin()->first.size();
        memcpy((char*)data + 1, (char*)&len, sizeof(int));
        memcpy((char*)data + 1 + sizeof(int), (char*)(groupsVarChar.begin()->first.c_str()), len);
        memcpy((char*)data + 1 + sizeof(int) + len, (char*)&(groupsVarChar.begin()->second[this->aggrOp]), sizeof(float));
        groupsVarChar.erase(groupsVarChar.begin());
    }
    return 0;
}

RC Aggregate::getNextTuple(void* data) {
    // Group by
    if(isGroupBy) {
        if(!aggrDone) {
            while(this->aggrIterator->getNextTuple(this->aggrData) != QE_EOF) {
                int isNull = QueryEngineUtils::getColumnData(this->aggrData, this->aggrColPos,
                                                             this->aggrAttributes, this->aggrColData);
                QueryEngineUtils::getColumnData(this->aggrData, this->groupColPos,
                                                this->aggrAttributes, this->groupData);

                if(this->aggrType == TypeReal && isNull != -1) {
                    float val = 0;
                    memcpy((char*)&val, (char*)this->aggrColData, sizeof(float));
                    groupBy(val, this->groupData, 0);
                } else if(this->aggrType == TypeInt && isNull != -1) {
                    int val_int = 0;
                    memcpy((char*)&val_int, (char*)this->aggrColData, sizeof(int));
                    float val = val_int;
                    groupBy(val, this->groupData, 0);
                } else {
                    aggrStat[2]++;
                    aggrStat[4] = (aggrStat[3])/aggrStat[2];
                    int val = 0;
                    groupBy(val, this->groupData, 1);
                }
            }
        }
        this->aggrDone = true;
        return getGroups(data);
    }

    if(this->endFlag) return -1;

    while(this->aggrIterator->getNextTuple(this->aggrData) != QE_EOF) {
        int isNull = QueryEngineUtils::getColumnData(this->aggrData, this->aggrColPos,
                                                     this->aggrAttributes, this->aggrColData);
        if(this->aggrType == TypeReal && isNull != -1) {
            float val = 0;
            memcpy((char*)&val, (char*)this->aggrColData, sizeof(float));
            aggrStat[0] = min(aggrStat[0], val);
            aggrStat[1] = max(aggrStat[1], val);
            aggrStat[2]++;
            nonNullCount++;
            aggrStat[3] += val;
            aggrStat[4] = (aggrStat[3])/nonNullCount;
        } else if(this->aggrType == TypeInt && isNull != -1) {
            int val_int = 0;
            memcpy((char*)&val_int, (char*)this->aggrColData, sizeof(int));
            float val = val_int;
            aggrStat[0] = min(aggrStat[0], val);
            aggrStat[1] = max(aggrStat[1], val);
            aggrStat[2]++;
            nonNullCount++;
            aggrStat[3] += val;
            aggrStat[4] = (aggrStat[3])/nonNullCount;
        } else {
            aggrStat[2]++;
        }
    }

    char nullBytes[1];
    memset(nullBytes, 0 , 1);
    memcpy((char*)data, (char*)nullBytes, 1);
    memcpy((char*)data + 1, (char*)&aggrStat[this->aggrOp], sizeof(float));

    this->endFlag = true;
    return (aggrStat[2] > 0 ? 0 : -1);
}

void Aggregate::getAttributes(std::vector<Attribute>& attrs) const {
    if(this->isGroupBy) {
        attrs.push_back(this->groupAttr);
    }

    Attribute attr = this->aggrAttribute;
    if(this->aggrOp == MAX){
        attr.name = "MAX(" + aggrAttribute.name + ")";
    } else if(this->aggrOp == MIN){
        attr.name = "MIN(" + aggrAttribute.name + ")";
    } else if(this->aggrOp == SUM){
        attr.name = "SUM(" + aggrAttribute.name + ")";
    } else if(this->aggrOp == AVG){
        attr.name = "AVG(" + aggrAttribute.name + ")";
    } else if(this->aggrOp == COUNT){
        attr.name = "COUNT(" + aggrAttribute.name + ")";
    }

    attrs.push_back(attr);
    return;
}

/****************************** Joins ********************************************/

BNLJoin::BNLJoin(Iterator* leftIn, TableScan* rightIn,
                 const Condition& condition,
                 const unsigned numPages) {

    this->lIterator = leftIn;
    this->rIterator = rightIn;
    lIterator->getAttributes(this->lAttributes);
    rIterator->getAttributes(this->rAttributes);
    this->joinCondition = condition;
    QueryEngineUtils::getPositionOfAttribute(condition.lhsAttr, this->lAttributes, this->lColPos);

    if(condition.bRhsIsAttr)
        QueryEngineUtils::getPositionOfAttribute(condition.rhsAttr, this->rAttributes, this->rColPos);

    this->joinDataType = this->lAttributes[this->lColPos].type;
    this->blockSize = numPages*PAGE_SIZE;
    this->block = malloc(this->blockSize);
    this->lTuple = malloc(PAGE_SIZE);
    this->rTuple = malloc(PAGE_SIZE);
    this->rColData = malloc(PAGE_SIZE);
    this->lastTupleIndex = -1;
    this->lastTupleNum = 0;
    this->lastRemTuple = malloc(PAGE_SIZE);
    this->lastRemTupleSize = 0;


    createHashTable( this->joinDataType);

    this->numTuples = loadDataIntoBlock();
    this->isUsingPrev = false;
    this->isRNull = 0;
    this->LEOF = false;

}

BNLJoin::~BNLJoin() {
    if(lTuple != NULL) free(lTuple);
    if(rTuple != NULL) free(rTuple);
    if(rColData != NULL) free(rColData);
    if(block != NULL) free(block);
    if(lastRemTuple != NULL) free(lastRemTuple);
    deleteHashTable(this->joinDataType);
}

void BNLJoin::createHashTable(int type) {
    if(type == TypeInt) {
        this->hashMap = new unordered_map<int, std::vector<int>>();
    } else if(type == TypeReal) {
        this->hashMap = new unordered_map<float, std::vector<int>>();
    } else {
        this->hashMap = new unordered_map<std::string, std::vector<int>>();
    }
    return;
}

void BNLJoin::deleteHashTable(int type) {
    if(type == TypeInt) {
        delete (unordered_map<int, std::vector<int>>*)hashMap;
    } else if(type == TypeReal) {
         delete (unordered_map<float, std::vector<int>>*)hashMap;
    } else {
         delete (unordered_map<std::string, std::vector<int>>*)hashMap;
    }
    return;
}

int BNLJoin::loadDataIntoBlock() {
    void* data = malloc(PAGE_SIZE);
    void* lColData = malloc(PAGE_SIZE);
    int offset = 0;
    int numTuples = 0;
    bool noEnd = true;

    if(this->lastRemTupleSize != -1) {
        memcpy((char*)this->lastRemTuple, (char*)block + offset, this->lastRemTupleSize);
        offset += this->lastRemTupleSize;
        this->lastRemTupleSize = -1;
        numTuples++;
    }

    while(this->lIterator->getNextTuple(data) != QE_EOF) {
        int tupleSize = QueryEngineUtils::getTupleSize(data, this->lAttributes);
        int isLNull = QueryEngineUtils::getColumnData(data, this->lColPos,
                                                      this->lAttributes, lColData);

        if(offset + tupleSize > this->blockSize) {
            memcpy((char*)this->lastRemTuple, (char*)data, tupleSize);
            this->lastRemTupleSize = tupleSize;
            noEnd = false;
            break;
        }

        /* Ignoring null values */
        if(isLNull == -1) continue;

        memcpy((char*)this->block + offset, (char*)data, tupleSize);
        /* inserting attribute val as key and the tuple offset as vlaue
         * in the hash for faster and efficient lookups */
        QueryEngineUtils::insertInHashTable(this->hashMap, lColData, isLNull, offset, this->joinDataType);
        offset += tupleSize;
        numTuples++;
    }

    if(noEnd) {
        this->LEOF = true;
    }

    free(data);
    free(lColData);
    return numTuples;
}

RC BNLJoin::getNextTuple(void* data) {
    while(this->isUsingPrev || this->rIterator->getNextTuple(this->rTuple) != QE_EOF) {
        if(!this->isUsingPrev) { 
            this->isRNull = QueryEngineUtils::getColumnData(this->rTuple, this->rColPos,
                                                          this->rAttributes, this->rColData);
        }
        if(this->isRNull == -1) continue;

        if(this->lastTupleIndex == -1 || this->lastTupleIndex == (int)this->tupleOffsets.size()) {
            this->tupleOffsets.clear();
            this->tupleOffsets = QueryEngineUtils::getTupleOffsetFromHashTable(this->hashMap,
                                                                               this->rColData,
                                                                               isRNull,
                                                                               this->joinDataType);

            this->lastTupleIndex = 0;
        }

        if(this->tupleOffsets.size() == 0) continue;

        QueryEngineUtils::getTupleFromBlock(this->block, tupleOffsets[this->lastTupleIndex],
                                            this->lAttributes, this->lTuple);
        this->lastTupleIndex++;
        QueryEngineUtils::concatTuples(this->lTuple, this->lAttributes,
                                       this->rTuple, this->rAttributes, data);

        if(this->lastTupleIndex == (int)this->tupleOffsets.size()) {
            this->isUsingPrev = false;
        } else {
            this->isUsingPrev = true;
        }
        return 0;
    }

    if(!this->LEOF) {
        if(this->lastTupleNum > this->numTuples) {
            QueryEngineUtils::eraseHashTableData(hashMap, this->joinDataType);
            this->numTuples = loadDataIntoBlock();
            this->lastTupleIndex = -1;
            this->lastTupleNum = 0;
            this->rIterator->setIterator();
            this->isUsingPrev = false;
        }
    }
    

    return QE_EOF;
}

void BNLJoin::getAttributes(std::vector<Attribute>& attrs) const {
    attrs = this->lAttributes;
    for(int i = 0 ; i < (int)this->rAttributes.size(); i++) {
        attrs.push_back(rAttributes[i]);
    }
    return;
}

INLJoin::INLJoin(Iterator* leftIn, IndexScan* rightIn, const Condition& condition) {
    this->lIterator = leftIn;
    this->rIterator = rightIn;
    lIterator->getAttributes(this->lAttributes);
    rIterator->getAttributes(this->rAttributes);
    this->joinCondition = condition;
    QueryEngineUtils::getPositionOfAttribute(condition.lhsAttr, this->lAttributes, this->lColPos);

    if(condition.bRhsIsAttr)
        QueryEngineUtils::getPositionOfAttribute(condition.rhsAttr, this->rAttributes, this->rColPos);

    this->joinDataType = this->lAttributes[this->lColPos].type;
    this->lTuple = malloc(PAGE_SIZE);
    this->rTuple = malloc(PAGE_SIZE);
    this->lColData = malloc(PAGE_SIZE);
    this->rColData = malloc(PAGE_SIZE);

    this->usingPrevTuple = false;
    this->isLNull = 0;
}

RC INLJoin::getNextTuple(void* data) {
    while(this->usingPrevTuple || this->lIterator->getNextTuple(this->lTuple) != QE_EOF) {
        // get outer tuple attribute
        if(!this->usingPrevTuple) {
            this->isLNull = QueryEngineUtils::getColumnData(this->lTuple, this->lColPos,
                                                            this->lAttributes, this->lColData);
            this->rIterator->setIterator(this->lColData, this->lColData, true, true);

        }

        while(this->rIterator->getNextTuple(this->rTuple) != QE_EOF) {
            int isRNull = QueryEngineUtils::getColumnData(this->rTuple, this->rColPos,
                                                          this->rAttributes, this->rColData);

            if(QueryEngineUtils::compare(this->lColData, this->rColData, this->isLNull, isRNull,
                                         this->joinDataType, this->joinCondition.op)) {

                this->usingPrevTuple = true;
                QueryEngineUtils::concatTuples(this->lTuple, this->lAttributes,
                                               this->rTuple, this->rAttributes, data);
                return 0;
            }
        }

        this->usingPrevTuple = false;
        if(isLNull == -1) continue;

        this->rIterator->setIterator(NULL, NULL, true, true);
    }

    return QE_EOF;
}

void INLJoin::getAttributes(std::vector<Attribute>& attrs) const {
    attrs = this->lAttributes;
    for(int i = 0 ; i < (int)this->rAttributes.size(); i++) {
        attrs.push_back(rAttributes[i]);
    }
    return;
}

GHJoin::GHJoin(Iterator* leftIn, Iterator* rightIn, 
               const Condition& condition,
               const unsigned numPartitions)
               : rbfm(RecordBasedFileManager::instance()) {

    this->lIterator = leftIn;
    this->rIterator = rightIn;
    this->lIterator->getAttributes(this->lAttributes);
    this->rIterator->getAttributes(this->rAttributes);
    this->joinCondition = condition;
    QueryEngineUtils::getPositionOfAttribute(condition.lhsAttr,
                                             this->lAttributes,
                                             this->lColPos);

    if(condition.bRhsIsAttr)
        QueryEngineUtils::getPositionOfAttribute(condition.rhsAttr,
                                                 this->rAttributes,
                                                 this->rColPos);

    this->joinDataType = this->lAttributes[this->lColPos].type;
    this->lTuple = malloc(PAGE_SIZE);
    this->rTuple = malloc(PAGE_SIZE);
    this->rColData = malloc(PAGE_SIZE);

    this->isUsingPrevT = false;
    this->isUsingPrev = false;
    this->isRNull = 0;
    this->numPartitions = numPartitions;
    this->lastPartition = -1;
    this->lastTupleIndex = -1;

    this->lFileHandles = new FileHandle[this->numPartitions];
    this->rFileHandles = new FileHandle[this->numPartitions];

    for(int i = 0; i < (int)lAttributes.size(); i++) {
        this->lAttrNames.push_back(lAttributes[i].name);
    }

    for(int i = 0; i < (int)rAttributes.size(); i++) {
        this->rAttrNames.push_back(rAttributes[i].name);
    }

    joinCounter = QueryEngineUtils::getJoinCounter();
    createIntermediatePartitions(numPartitions);

    populatePartition(this->lIterator, this->lFileHandles, this->lColPos, this->lAttributes);
    populatePartition(this->rIterator, this->rFileHandles, this->rColPos, this->rAttributes);
    createHashTable( this->joinDataType);

    this->joinComplete = false;
}

void GHJoin::createIntermediatePartitions(unsigned numPartitions) {
    for(unsigned i = 0; i < numPartitions; i++) {
        std::string lPartition = "leftjoin" + std::to_string(this->joinCounter) + "_" + std::to_string(i);
        std::string rPartition = "rightjoin" + std::to_string(this->joinCounter) + "_" + std::to_string(i);
        rbfm.createFile(lPartition);
        rbfm.createFile(rPartition);
        rbfm.openFile(lPartition, this->lFileHandles[i]);
        rbfm.openFile(rPartition, this->rFileHandles[i]);
    }
    return;
}

/* uses hashfuncion to populate partition */
void GHJoin::populatePartition(Iterator* itr, FileHandle* fileHandles, int colPos, std::vector<Attribute>& attrs) {
    while(itr->getNextTuple(this->lTuple) != QE_EOF) {
       int isNull = QueryEngineUtils::getColumnData(this->lTuple, colPos,
                                                    attrs, this->rColData);

       if(isNull == -1) continue;
       int partition = QueryEngineUtils::getPartitionToInsert(this->rColData, this->joinDataType,
                                                              isNull, this->numPartitions);
       RID dummyRID;
       rbfm.insertRecord(fileHandles[partition], attrs, this->lTuple, dummyRID);
    }
}

/* deletes ntermediate files creaed during join */
void GHJoin::deleteIntermediatePartitions() {
    for(int i = 0 ; i < this->numPartitions; i++) {
        rbfm.closeFile(this->lFileHandles[i]);
        rbfm.closeFile(this->rFileHandles[i]);
        rbfm.destroyFile(this->rFileHandles[i].fileName);
        rbfm.destroyFile(this->lFileHandles[i].fileName);
    }
    delete[] lFileHandles;
    delete[] rFileHandles;
}

RC GHJoin::loadFileIntoMemory(FileHandle& fileHandle, void* block) {
    rbfm.scan(fileHandle, this->lAttributes, "", NO_OP,
              NULL, this->lAttrNames, this->rbfmSI);

    void* record = malloc(PAGE_SIZE);
    void* lColData = malloc(PAGE_SIZE);
    int offset = 0;
    int numRecords = 0;
    RID dummyRID;

    while(this->rbfmSI.getNextRecord(dummyRID, record) != RBFM_EOF) {
        int recordSize = QueryEngineUtils::getTupleSize(record, this->lAttributes);
        int isLNull = QueryEngineUtils::getColumnData(record, this->lColPos,
                                                      this->lAttributes, lColData);
        /* Ignoring null values */
        if(isLNull == -1) continue;

        memcpy((char*)this->block + offset, (char*)record, recordSize);
        /* inserting attribute val as key and the tuple offset as vlaue
        i* in the hash for faster and efficient lookups */
        QueryEngineUtils::insertInHashTable(this->hashMap, lColData, isLNull, offset, this->joinDataType);
        offset += recordSize;
        numRecords++;
    }
    this->rbfmSI.close();
    rbfm.closeFile(fileHandle);
    free(record);
    free(lColData);
    return numRecords;
}

void GHJoin::createHashTable(int type) {
    if(type == TypeInt) {
        this->hashMap = new unordered_map<int, std::vector<int>>();
    } else if(type == TypeReal) {
        this->hashMap = new unordered_map<float, std::vector<int>>();
    } else {
        this->hashMap = new unordered_map<std::string, std::vector<int>>();
    }
    return;
}

void GHJoin::deleteHashTable(int type) {
    if(type == TypeInt) {
        delete (unordered_map<int, std::vector<int>>*)hashMap;
    } else if(type == TypeReal) {
         delete (unordered_map<float, std::vector<int>>*)hashMap;
    } else {
         delete (unordered_map<std::string, std::vector<int>>*)hashMap;
    }
    return;
}

GHJoin::~GHJoin() {
    if(lTuple != NULL) free(lTuple);
    if(rTuple != NULL) free(rTuple);
    if(rColData != NULL) free(rColData);
    if(block != NULL) free(block);
    deleteHashTable(this->joinDataType);
    if(this->joinComplete)
        deleteIntermediatePartitions();
}

RC GHJoin::getNextTuple(void *data) {
    while(this->lastPartition < this->numPartitions) {
        if(!isUsingPrev) {
            this->lastPartition++;
	    if(this->lastPartition >= this->numPartitions)  {
                this->joinComplete = true;
                return QE_EOF;
            }
            int numPages = this->lFileHandles[this->lastPartition].getNumberOfPages();
            block = realloc(block, numPages*PAGE_SIZE);
            QueryEngineUtils::eraseHashTableData(this->hashMap, this->joinDataType);
            loadFileIntoMemory(this->lFileHandles[this->lastPartition], block);
            rbfm.scan(this->rFileHandles[this->lastPartition], this->rAttributes, "", NO_OP,
                      NULL, this->rAttrNames, this->rbfmSI);
            this->isUsingPrev = true;
            this->lastTupleIndex = -1;
            this->isUsingPrevT = false;
        }

        RID dummyRID;
        while(isUsingPrevT || this->rbfmSI.getNextRecord(dummyRID, this->rTuple) != RBFM_EOF) {
            if(!isUsingPrevT) {
                this->isRNull = QueryEngineUtils::getColumnData(this->rTuple, this->rColPos,
                                                                this->rAttributes, this->rColData);
            }
            if(this->isRNull == -1) continue;

            if(this->lastTupleIndex == -1 || this->lastTupleIndex == (int)this->tupleOffsets.size()) {
                this->tupleOffsets.clear();
                this->tupleOffsets = QueryEngineUtils::getTupleOffsetFromHashTable(this->hashMap,
                                                                                   this->rColData,
                                                                                   isRNull,
                                                                                   this->joinDataType);
                this->lastTupleIndex = 0;
            }

            if(this->tupleOffsets.size() == 0) continue;
            QueryEngineUtils::getTupleFromBlock(this->block, tupleOffsets[this->lastTupleIndex],
                                                this->lAttributes, this->lTuple);
            this->lastTupleIndex++;
            QueryEngineUtils::concatTuples(this->lTuple, this->lAttributes,
                                           this->rTuple, this->rAttributes, data);
            if(this->lastTupleIndex == (int)this->tupleOffsets.size()) {
                this->isUsingPrevT = false;
            } else {
                this->isUsingPrevT = true;
            }
            return 0;
        }
        isUsingPrev = false;
        rbfm.closeFile(this->rFileHandles[this->lastPartition]);
    }
 
    this->joinComplete = true;   
    return QE_EOF;
}

void GHJoin::getAttributes(std::vector<Attribute> &attrs) const {
    attrs = this->lAttributes;
    for(int i = 0 ; i < (int)this->rAttributes.size(); i++) {
        attrs.push_back(rAttributes[i]);
    }
    return;
}

