#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <sys/stat.h>
#include <algorithm>

#include "../rbf/rbfm.h"
#include "../ix/ix.h"

# define RM_EOF (-1)  // end of a scan operator
//System files
#define TABLES_FILE "Tables"
#define COLUMNS_FILE "Columns"

// RM_ScanIterator is an iterator to go through tuples
class RM_ScanIterator {
private:
    RBFM_ScanIterator rbfmScanIterator;
    FileHandle* fileHandle;
    FileHandle fileHandleObj;
    int indicator;
    vector<Attribute> recordDescriptor;
    RecordBasedFileManager& rbfm;
public:
    RM_ScanIterator() : rbfm(RecordBasedFileManager::instance()) {}

    ~RM_ScanIterator() {
        fileHandle = NULL;
    }

    RC initializeScanIterator(const std::string &tableName, const std::string &conditionAttribute,
                              const CompOp compOp, const void* value, 
                              const std::vector<std::string> & attributesNames);

    // "data" follows the same format as RelationManager::insertTuple()
    RC getNextTuple(RID &rid, void *data);

    RC close() {
        if(indicator == 2)
            RecordBasedFileManager::instance().closeFile(fileHandleObj);
        indicator = 1;
        return 0;
    };
};

class RM_IndexScanIterator {
private:
    IX_ScanIterator ixsi;
    IXFileHandle ixFileHandle;
    IndexManager& ixm;
public:
    RM_IndexScanIterator() : ixm(IndexManager::instance()) { }

    ~RM_IndexScanIterator() = default;
    
 
    RC initializeScanIterator(const string &tableName, const string &attributeName,
                              const void *lowKey, const void *highKey,
                              bool lowKeyInclusive, bool highKeyInclusive);

    RC getNextEntry(RID &rid, void *key);

    RC close() {
        ixsi.close();
        this->ixm.closeFile(this->ixFileHandle);
        return 0;
    }

};

// Relation Manager
class RelationManager {
public:

    FileHandle fileHandle;
    std::string currFile;

    static RelationManager &instance();

    RC createCatalog();

    RC deleteCatalog();

    RC createTable(const std::string &tableName, const std::vector<Attribute> &attrs);

    RC deleteTable(const std::string &tableName);

    RC getAttributes(const std::string &tableName, std::vector<Attribute> &attrs);

    RC getAttributesRaw(const std::string &tableName, std::vector<Attribute> &attrs);

    RC insertTuple(const std::string &tableName, const void *data, RID &rid);

    RC deleteTuple(const std::string &tableName, const RID &rid);

    RC updateTuple(const std::string &tableName, const void *data, const RID &rid);

    RC readTuple(const std::string &tableName, const RID &rid, void *data);

    // Print a tuple that is passed to this utility method.
    // The format is the same as printRecord().
    RC printTuple(const std::vector<Attribute> &attrs, const void *data);

    RC readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName, void *data);

    // Scan returns an iterator to allow the caller to go through the results one by one.
    // Do not store entire results in the scan iterator.
    RC scan(const std::string &tableName,
            const std::string &conditionAttribute,
            const CompOp compOp,                  // comparison type such as "<" and "="
            const void *value,                    // used in the comparison
            const std::vector<std::string> &attributeNames, // a list of projected attributes
            RM_ScanIterator &rm_ScanIterator);

    // Extra credit work (10 points)
    RC addAttribute(const std::string &tableName, const Attribute &attr);

    RC dropAttribute(const std::string &tableName, const std::string &attributeName);

    // QE IX related
    RC createIndex(const std::string &tableName, const std::string &attributeName);

    RC destroyIndex(const std::string &tableName, const std::string &attributeName);

    // indexScan returns an iterator to allow the caller to go through qualified entries in index
    RC indexScan(const std::string &tableName,
                 const std::string &attributeName,
                 const void *lowKey,
                 const void *highKey,
                 bool lowKeyInclusive,
                 bool highKeyInclusive,
                 RM_IndexScanIterator &rm_IndexScanIterator);

    vector<Attribute> getAttributesForVersion(const std::string& tableName, const int version);

    RC getLatestTableVersion(const std::string& tableName);

    bool isTableExist(const std::string& tableName);


protected:
    RelationManager();                                                  // Prevent construction
    ~RelationManager();                                                 // Prevent unwanted destruction
    RelationManager(const RelationManager &);                           // Prevent construction by copying
    RelationManager &operator=(const RelationManager &);                // Prevent assignment
private:
    vector<Attribute> tableAttributes;
    vector<Attribute> columnAttributes;
    FileHandle tableFileHandle;
    FileHandle columnFileHandle;
    std::unordered_map<std::string, std::unordered_map<int, ColumnTableInfo>> columnsMap;
    std::unordered_map<std::string, TablesTableInfo> tableMap;
    RecordBasedFileManager& rbfm;

    void initializeTableAttribute(vector<Attribute>& tableAttributes);

    void initializeColumnAttribute(vector<Attribute>& columnAttributes);

    void initializeTableAndColumnFileHandles();

    void initializeColumnMap();

    void initializeTableMap();

    void fillMapFromTableEntry(const void* data, const RID& rid);

    void fillMapFromColumnEntry(const void* data, const RID& rid);

    RC createAndInsertTablesData(const std::string& tableName, int& table_id, const int action = 0);

    RC createAndInsertColumnsData(const std::string& tableName, const std::vector<Attribute>& recordDescriptor, int table_id);

    void correctOrderingOfAttributes();

    void processDescriptor(vector<Attribute>& attrs);

    //getting filename is easy -> following the format that if table name is "tbl_xyz", file name will be "tbl_xyz"
    // Below functions will be called by getAttribute.
    RC getFileNameAndLatestVersionNameFromTablesFile(std::string& fileName, short& version);

    RC getNewTableVersion(const std::string& tableName);

    RC getNewTableId(const std::string& tableName);

    std::string getTableNameFromId(const int tableID);

    void formatRecordWithLatestSchemaChange(std::vector<Attribute>& recordDescriptor, 
                                            std::vector<Attribute>& recordDescriptorLatest, 
                                            std::vector<Attribute>& recordDescriptorLatestOriginal,
                                            void* data);

    RC deleteTableEntryFromCatalog(const std::string& tableName);

    RC deleteTableFile(const std::string tableName);
    // if no version name specified delete all the entries.
    RC deleteEntryFromTables(const std::string tableName);

    RC deleteEntryFromColumns(const std::string tableName);

    RC populateIndexOnAttribute(const std::string& tableName,
                                const::std::string& indexFileName,
                                const std::vector<Attribute>& indexAttrNames);

    bool isSystemTable(const std::string& tableName);

    bool isCatalogInitialized();
};

#endif
