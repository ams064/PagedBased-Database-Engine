#ifndef _qe_h_
#define _qe_h_

#include "../rbf/rbfm.h"
#include "../rm/rm.h"
#include "../ix/ix.h"

#include <float.h>
#include <map>
#include <chrono>

#define QE_EOF (-1)  // end of the index scan

class RM_ScanIterator;

typedef enum {
    MIN = 0, MAX, COUNT, SUM, AVG
} AggregateOp;

// The following functions use the following
// format for the passed data.
//    For INT and REAL: use 4 bytes
//    For VARCHAR: use 4 bytes for the length followed by the characters

struct Value {
    AttrType type;          // type of value
    void *data;             // value
};

struct Condition {
    std::string lhsAttr;        // left-hand side attribute
    CompOp op;                  // comparison operator
    bool bRhsIsAttr;            // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
    std::string rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
    Value rhsValue;             // right-hand side value if bRhsIsAttr = FALSE
};

struct ColumnData {
    int length;
    void* data;

    ColumnData() {
        data = NULL;
        length = 0;
    }

    ~ColumnData () {
        if(length != 0)
            free(data);
    }
};

class QueryEngineUtils {
public:
    static RC getColumnData(const void* data, const int columnNum,
                            const std::vector<Attribute>& attributes,
                            void* columnData);

    static void getPositionOfAttribute(const std::string& attributeName,
                                       const std::vector<Attribute>& attributes,
                                       int& colPos);

    static int getTupleSize(const void* data, const std::vector<Attribute>& attrs);

    static int getTupleFromBlock(const void* block, const int readOffset,
                                 const std::vector<Attribute>& attrs,
                                 void* tuple);

    static void concatTuples(const void* lTuple, const std::vector<Attribute>& lAttrs,
                             const void* rTuple, const std::vector<Attribute>& rAttrs,
                             void* data);

    static bool compare(const void* data1, const void* data2,const int lhsNull,
                        const int rhsNull, const int dataType, const CompOp op);

    static void decomposeIntoColumns(const void* data,
                                     const std::vector<Attribute>& attributes,
                                     std::vector<ColumnData>& colData);

    static int getPartitionToInsert(const void* data, int type, int isNull, int numPartitions);

    static void insertInHashTable(void* hashMap, const void* key, int keyLen, int val, int type);

    static void eraseHashTableData(void* hashMap, int type);

    static std::vector<int> getTupleOffsetFromHashTable(void* hashMap, const void* key, int keyLen, int type);

    static int getJoinCounter();
};

class Iterator {
    // All the relational operators and access methods are iterators.
public:
    virtual RC getNextTuple(void *data) = 0;

    virtual void getAttributes(std::vector<Attribute> &attrs) const = 0;

    virtual ~Iterator() = default;;
};

class TableScan : public Iterator {
    // A wrapper inheriting Iterator over RM_ScanIterator
public:
    RelationManager &rm;
    RM_ScanIterator *iter;
    std::string tableName;
    std::vector<Attribute> attrs;
    std::vector<std::string> attrNames;
    RID rid{};

    TableScan(RelationManager &rm, const std::string &tableName, const char *alias = NULL) : rm(rm) {
        //Set members
        this->tableName = tableName;

        // Get Attributes from RM
        rm.getAttributes(tableName, attrs);

        // Get Attribute Names from RM
        for (Attribute &attr : attrs) {
            // convert to char *
            attrNames.push_back(attr.name);
        }

        // Call RM scan to get an iterator
        iter = new RM_ScanIterator();
        rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);

        // Set alias
        if (alias) this->tableName = alias;
    };

    // Start a new iterator given the new compOp and value
    void setIterator() {
        iter->close();
        delete iter;
        iter = new RM_ScanIterator();
        rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);
    };

    RC getNextTuple(void *data) override {
        return iter->getNextTuple(rid, data);
    };

    void getAttributes(std::vector<Attribute> &attributes) const override {
        attributes.clear();
        attributes = this->attrs;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        for (Attribute &attribute : attributes) {
            std::string tmp = tableName;
            tmp += ".";
            tmp += attribute.name;
            attribute.name = tmp;
        }
    };

    ~TableScan() override {
        iter->close();
        delete iter;
    };
};

class IndexScan : public Iterator {
    // A wrapper inheriting Iterator over IX_IndexScan
public:
    RelationManager &rm;
    RM_IndexScanIterator *iter;
    std::string tableName;
    std::string attrName;
    std::vector<Attribute> attrs;
    char key[PAGE_SIZE]{};
    RID rid{};

    IndexScan(RelationManager &rm, const std::string &tableName, const std::string &attrName, const char *alias = NULL)
            : rm(rm) {
        // Set members
        this->tableName = tableName;
        this->attrName = attrName;


        // Get Attributes from RM
        rm.getAttributes(tableName, attrs);

        // Call rm indexScan to get iterator
        iter = new RM_IndexScanIterator();
        rm.indexScan(tableName, attrName, NULL, NULL, true, true, *iter);

        // Set alias
        if (alias) this->tableName = alias;
    };

    // Start a new iterator given the new key range
    void setIterator(void *lowKey, void *highKey, bool lowKeyInclusive, bool highKeyInclusive) {
        iter->close();
        delete iter;
        iter = new RM_IndexScanIterator();
        rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive, highKeyInclusive, *iter);
    };

    RC getNextTuple(void *data) override {
        int rc = iter->getNextEntry(rid, key);
        if (rc == 0) {
            rc = rm.readTuple(tableName.c_str(), rid, data);
        }
        return rc;
    };

    void getAttributes(std::vector<Attribute> &attributes) const override {
        attributes.clear();
        attributes = this->attrs;


        // For attribute in std::vector<Attribute>, name it as rel.attr
        for (Attribute &attribute : attributes) {
            std::string tmp = tableName;
            tmp += ".";
            tmp += attribute.name;
            attribute.name = tmp;
        }
    };

    ~IndexScan() override {
        iter->close();
        delete iter;
    };
};

class Filter : public Iterator {
    // Filter operator
private:
    Iterator* filterIterator;
    Condition filterCondition;
    std::vector<Attribute> filterAttributes;
    int lhsColPos;
    int rhsColPos;
    void* lhsAttrVal = NULL;
    void* rhsAttrVal = NULL;
    int dataType;
public:
    Filter(Iterator *input,               // Iterator of input R
           const Condition &condition     // Selection condition
    );

    ~Filter() override {
        if(lhsAttrVal != NULL) free(lhsAttrVal);
        if(rhsAttrVal != NULL) free(rhsAttrVal);
     }

    RC getNextTuple(void *data) override;

    // For attribute in std::vector<Attribute>, name it as rel.attr
    void getAttributes(std::vector<Attribute> &attrs) const override;
};

class Project : public Iterator {
private:
    Iterator* projectIterator;
    std::vector<Attribute> projectAttributes;
    std::vector<Attribute> changedAttrs;
    std::vector<int> projectColPos;
    void* projectData = NULL;
    // Projection operator
public:
    Project(Iterator *input, const std::vector<std::string> &attrNames);

    ~Project() override {
        if(projectData != NULL)
            free(projectData);
     }

    RC getNextTuple(void *data) override;

    // For attribute in std::vector<Attribute>, name it as rel.attr
    void getAttributes(std::vector<Attribute> &attrs) const override ;
};

class BNLJoin : public Iterator {
    // Block nested-loop join operator
private:
    Iterator* lIterator;
    TableScan* rIterator;
    std::vector<Attribute> lAttributes;
    std::vector<Attribute> rAttributes;
    Condition joinCondition;
    int joinDataType;
    int lColPos;
    int rColPos;
    void* lTuple = NULL;
    void* rTuple = NULL;
    void* rColData = NULL;
    int blockSize;
    void* block = NULL;
    int lastTupleIndex;
    int lastTupleNum;
    int numTuples;
    bool isUsingPrev;
    int isRNull;
    std::vector<int> tupleOffsets;

    void* lastRemTuple = NULL;
    int lastRemTupleSize;
    bool LEOF;


    int loadDataIntoBlock();
    void createHashTable(const int type);
    void deleteHashTable(const int type);
public:
    void* hashMap = NULL;
    BNLJoin(Iterator *leftIn,            // Iterator of input R
            TableScan *rightIn,           // TableScan Iterator of input S
            const Condition &condition,   // Join condition
            const unsigned numPages       // # of pages that can be loaded into memory,
            //   i.e., memory block size (decided by the optimizer)
    );

    ~BNLJoin() override;

    RC getNextTuple(void *data) override;

    // For attribute in std::vector<Attribute>, name it as rel.attr
    void getAttributes(std::vector<Attribute> &attrs) const override;
};

class INLJoin : public Iterator {
    // Index nested-loop join operator
private:
    Iterator* lIterator;
    IndexScan* rIterator;
    std::vector<Attribute> lAttributes;
    std::vector<Attribute> rAttributes;
    Condition joinCondition;
    int joinDataType;
    int lColPos;
    int rColPos;
    void* lTuple = NULL;
    void* rTuple = NULL;
    void* lColData = NULL;
    void* rColData = NULL;
    bool usingPrevTuple;
    int isLNull;

public:
    INLJoin(Iterator *leftIn,           // Iterator of input R
            IndexScan *rightIn,          // IndexScan Iterator of input S
            const Condition &condition   // Join condition
    );

    ~INLJoin() override {
        if(lTuple != NULL) free(lTuple);
        if(rTuple != NULL) free(rTuple);
        if(lColData != NULL) free(lColData);
        if(rColData != NULL) free(rColData);
    }

    RC getNextTuple(void *data) override;

    // For attribute in std::vector<Attribute>, name it as rel.attr
    void getAttributes(std::vector<Attribute> &attrs) const override;
};

// Optional for everyone. 10 extra-credit points
class GHJoin : public Iterator {
private:
    Iterator* lIterator;
    Iterator* rIterator;
    std::vector<Attribute> lAttributes;
    std::vector<Attribute> rAttributes;
    std::vector<std::string> lAttrNames;
    std::vector<std::string> rAttrNames;
    Condition joinCondition;
    int joinDataType;
    int lColPos;
    int rColPos;
    void* lTuple = NULL;
    void* rTuple = NULL;
    void* rColData = NULL;
    bool isUsingPrevT;
    bool isUsingPrev;
    int isRNull;
    int numPartitions;
    int lastPartition;
    int lastTupleIndex;
    int lastTupleNum;
    std::vector<int> tupleOffsets;
    void* block = NULL;
    void* hashMap = NULL;
    RecordBasedFileManager& rbfm;
    RBFM_ScanIterator rbfmSI;
    FileHandle* lFileHandles;
    FileHandle* rFileHandles;

    int joinCounter;
    bool joinComplete;

    int loadFileIntoMemory(FileHandle& fileHandle, void* block);
    void createHashTable(const int type);
    void deleteHashTable(const int type);
    void createIntermediatePartitions(unsigned numPartitions);
    void populatePartition(Iterator* itr, FileHandle* fileHandles, int colPos,
                           std::vector<Attribute>& attrs);
    void deleteIntermediatePartitions();

    // Grace hash join operator
public:
    GHJoin(Iterator *leftIn,               // Iterator of input R
           Iterator *rightIn,               // Iterator of input S
           const Condition &condition,      // Join condition (CompOp is always EQ)
           const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
    );

    ~GHJoin() override;

    RC getNextTuple(void *data) override;

    // For attribute in std::vector<Attribute>, name it as rel.attr
    void getAttributes(std::vector<Attribute> &attrs) const override;
};

class Aggregate : public Iterator {
private:
    float aggrStat[5];
    AggregateOp aggrOp;
    Attribute aggrAttribute;
    Iterator* aggrIterator;
    std::vector<Attribute> aggrAttributes;
    int aggrColPos;
    void* aggrColData = NULL;
    void* aggrData = NULL;
    int aggrType;
    bool endFlag;
    float nonNullCount;

    // group - by aggregate
    Attribute groupAttr;
    bool isGroupBy;
    int groupType;
    void* groupData;
    bool aggrDone;
    int groupColPos;

    std::map<int, std::vector<float>> groupsInt;
    std::map<float, std::vector<float>> groupsReal;
    std::map<std::string, std::vector<float>> groupsVarChar;

    RC groupBy(const float val, const void* data, const int isNull);
    RC getGroups(void* data);
    // Aggregation operator
public:
    // Mandatory
    // Basic aggregation
    Aggregate(Iterator *input,          // Iterator of input R
              const Attribute &aggAttr,        // The attribute over which we are computing an aggregate
              AggregateOp op            // Aggregate operation
    );

    // Optional for everyone: 5 extra-credit points
    // Group-based hash aggregation
    Aggregate(Iterator *input,             // Iterator of input R
              const Attribute &aggAttr,           // The attribute over which we are computing an aggregate
              const Attribute &groupAttr,         // The attribute over which we are grouping the tuples
              AggregateOp op              // Aggregate operation
    );

    ~Aggregate() override;

    RC getNextTuple(void *data) override;

    // Please name the output attribute as aggregateOp(aggAttr)
    // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
    // output attrname = "MAX(rel.attr)"
    void getAttributes(std::vector<Attribute> &attrs) const override;
};

#endif
