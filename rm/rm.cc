#include "rm.h"

/**
 * initializeScanIterator() - initializes the table iterator.
 * @argument1 : name of the table
 * @argument2 : condition on which to scan.
 * @argument3 : operation on which to scan.
 * @argument4 : value to compare.
 * @argument5 : name of the attribute to be returned.
 
 * Return : 0 on success, -1 on failure.
*/
RC RM_ScanIterator::initializeScanIterator(const std::string &tableName,
                                           const std::string &conditionAttribute,
                                           const CompOp compOp, const void* value,
                                           const std::vector<std::string> & attributesNames) {

    if(RelationManager::instance().currFile == tableName) {
        this->fileHandle = &(RelationManager::instance().fileHandle);
        if(RelationManager::instance().getAttributesRaw(tableName, this->recordDescriptor) == -1) return -1;
        if( rbfm.scan(*(this->fileHandle), this->recordDescriptor, conditionAttribute,
              compOp, value, attributesNames, this->rbfmScanIterator) == -1) return -1;
        this->indicator = 1;
    } else {
        if(rbfm.openFile(tableName, this->fileHandleObj) == -1) return -1;
        if(RelationManager::instance().getAttributesRaw(tableName, this->recordDescriptor) == -1) return -1;
        if(rbfm.scan((this->fileHandleObj), this->recordDescriptor, conditionAttribute,
              compOp, value, attributesNames, this->rbfmScanIterator) == -1) return -1;
        this->indicator = 2;
    }
    return 0;
}

/**
 * getNextTuple() - wrapper on rbfm scan iterator getNextRecord.
 * @argument1 : rid of the next record.
 * @argument2 : data of the next record.
 
 * Return : 0 on success, RM_EOF on file end
*/
RC RM_ScanIterator::getNextTuple(RID& rid, void* data) {
    return this->rbfmScanIterator.getNextRecord(rid, data);
}

/**
 * initializeScanIterator() - initializes the table index iterator.
 * @argument1 : name of the table
 * @argument2 : name of the attribute on which index is there
 * @argument3 : low bound in comparison
 * @argument4 : upper bound in comparison
 * @argument5 : flag to decide if the range include lowKey or not.
 * @argument6 : flag to decide if the range include highKey or not.
 
 * Return : 0 on success, -1 on failure.
*/
RC RM_IndexScanIterator::initializeScanIterator(const string &tableName,
                                                const string &attributeName,
                                                const void *lowKey,
                                                const void *highKey,
                                                bool lowKeyInclusive,
                                                bool highKeyInclusive) {

    std::string indexFileName = tableName + "_" + attributeName + ".idx";
    if(ixm.openFile(indexFileName, this->ixFileHandle) == -1) {
        return -1;
    }

    vector<Attribute> attrs;
    RelationManager::instance().getAttributes(indexFileName, attrs);

    if(ixm.scan(this->ixFileHandle, attrs[0], lowKey, highKey,
                lowKeyInclusive, highKeyInclusive, this->ixsi) == -1) {
        return -1;
    }

    return 0;
}

/**
 * getNextTuple() - wrapper on index scan iterator getNextEntry.
 * @argument1 : rid of the next record.
 * @argument2 : data of the next record.
 
 * Return : 0 on success, RM_EOF on file end
*/
RC RM_IndexScanIterator::getNextEntry(RID& rid, void* data) {
    return this->ixsi.getNextEntry(rid, data);
}

// RM Singleton
RelationManager &RelationManager::instance() {
    static RelationManager _relation_manager = RelationManager();
    return _relation_manager;
}

//C'tor RM
RelationManager::RelationManager() : rbfm(RecordBasedFileManager::instance()) {
    if (isCatalogInitialized()) {
        // order of initializaitons matter here
        initializeTableAttribute(this->tableAttributes);
        initializeColumnAttribute(this->columnAttributes);
        initializeTableAndColumnFileHandles();
        initializeTableMap();
        initializeColumnMap();
        currFile = "";
        rbfm.tableMap = tableMap;
        rbfm.columnsMap = columnsMap;
    }
}

//D'tor RM
RelationManager::~RelationManager() {
    rbfm.closeFile(this->tableFileHandle);
    rbfm.closeFile(this->columnFileHandle);
    if(this->currFile != "") {
        rbfm.closeFile(this->fileHandle);
        this->currFile = "";
    }

}

/**
 * createCatalog() - creates the system catalog tables
 *
 * Return : 0 on success, -1 on file end
*/
RC RelationManager::createCatalog() {
    // Insert the first two tables -> Table and Column
    //Return -1 if files have already been created
    if(rbfm.createFile(TABLES_FILE) == -1 || rbfm.createFile(COLUMNS_FILE) == -1) {
        return -1;
    }

    initializeTableAttribute(this->tableAttributes);
    initializeColumnAttribute(this->columnAttributes);

    rbfm.openFile(TABLES_FILE, this->tableFileHandle);
    rbfm.openFile(COLUMNS_FILE, this->columnFileHandle);

    int table_id = -1;
    //Inserting Tables data into system table and columns
    createAndInsertTablesData(TABLES_FILE, table_id);
    createAndInsertColumnsData(TABLES_FILE, this->tableAttributes, table_id);

    //inserting Columns data into system table and columns
    table_id = -1;
    createAndInsertTablesData(COLUMNS_FILE, table_id);
    createAndInsertColumnsData(COLUMNS_FILE, this->columnAttributes, table_id);

    return 0;
}

/**
 * deleteCatalog() - deletes the system catalog tables
 *
 * Return : 0 on success, -1 on file end
*/
RC RelationManager::deleteCatalog() {
    if(rbfm.destroyFile(TABLES_FILE) == -1 || rbfm.destroyFile(COLUMNS_FILE) == -1) {
        return -1;
    }

    tableMap.clear();
    columnsMap.clear();
    rbfm.tableMap.clear();
    rbfm.columnsMap.clear();
    return 0;
}

/**
 * createTable() - creates a new table.
 * @argument1 : name of the table.
 * @argument2 : columns of the table.
 *
 * Return : 0 on success, -1 on file end
*/
RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
    if(isSystemTable(tableName) || isTableExist(tableName) || attrs.size() == 0) return -1;

    if(rbfm.createFile(tableName) == -1) return -1;


    if(currFile == "") {
        rbfm.openFile(tableName, this->fileHandle);
        this->currFile = tableName;
    } else if(currFile != tableName) {
        rbfm.closeFile(this->fileHandle);
        this->currFile = tableName;
        rbfm.openFile(tableName, this->fileHandle);
    }

    int table_id = -1;
    createAndInsertTablesData(tableName, table_id);
    createAndInsertColumnsData(tableName, attrs, table_id);
    return 0;
}

/**
 * deleteTable() - delete a  table.
 * @argument1 : name of the table.
 *
 *
 * Return : 0 on success, -1 on file end
*/
RC RelationManager::deleteTable(const std::string &tableName) {
    if(isSystemTable(tableName) || !isTableExist(tableName)) return -1;

    deleteTableEntryFromCatalog(tableName);

    if(currFile == tableName) {
        currFile = "";
        rbfm.closeFile(this->fileHandle);
    }
    rbfm.destroyFile(tableName);
    return 0;
}

/**
 * getAttributes() - get columns of a table
 * @argument1 : name of the table.
 * @argument2 : out parameter to return the vector of columns.
 *
 * Return : 0 on success, -1 on file end
*/
RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
    if(!isTableExist(tableName)) {
        return -1;
    }
    int latestVersion = getLatestTableVersion(tableName);
    attrs = getAttributesForVersion(tableName, latestVersion);
    processDescriptor(attrs);

    return 0;
}

/**
 * getAttributesRaw() - get latest columns in the table.
 * @argument1 : name of the table.
 * @argument2 : out parameter to return the vector of columns.
 *
 * Used in case of drop/add attribute.
 *
 * Return : 0 on success, -1 on file end
*/
RC RelationManager::getAttributesRaw(const std::string &tableName, std::vector<Attribute> &attrs) {
    if(!isTableExist(tableName)) {
        return -1;
    }
    int latestVersion = getLatestTableVersion(tableName);
    attrs = getAttributesForVersion(tableName, latestVersion);

    return 0;
}

/**
 * insertTuple() - insert a tuple into table
 * @argument1 : name of the table.
 * @argument2 : data to be inserted.
 * @argument3 : out parameter to get rid of the inserted tuple.
 *
 *
 * Return : 0 on success, -1 on file end
*/
RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
    if(!isTableExist(tableName) || isSystemTable(tableName)) return -1;
    std::vector<Attribute> recordDescriptor;
    getAttributes(tableName, recordDescriptor);

    if(currFile == "") {
        rbfm.openFile(tableName, this->fileHandle);
        this->currFile = tableName;
    } else if(currFile != tableName) {
        rbfm.closeFile(this->fileHandle);
        this->currFile = tableName;
        rbfm.openFile(tableName, this->fileHandle);
    }

    if(rbfm.insertRecord(this->fileHandle, recordDescriptor, data, rid) == -1)  {
        return -1;
    }

    int latestVersion = getLatestTableVersion(tableName);
    if(latestVersion != 1) {
        rbfm.insertVersionOfRecord(this->fileHandle, rid, (RT)latestVersion);
    }

    IXFileHandle ixFileHandle;
    void* key = malloc(PAGE_SIZE);

    for(int i = 0 ;  i < (int)recordDescriptor.size(); i++) {
        std::string indexName = tableName + "_" + recordDescriptor[i].name + ".idx";
        if(this->tableMap.find(indexName) != this->tableMap.end()) {
            IndexManager::instance().openFile(indexName, ixFileHandle);
            this->readAttribute(tableName, rid, recordDescriptor[i].name, key);
            IndexManager::instance().insertEntry(ixFileHandle, recordDescriptor[i], (char*)key + 1, rid);
            IndexManager::instance().closeFile(ixFileHandle);
        }

    }

    free(key);
    return 0;
}

/**
 * deleteTuple() - deletes a tuple from table
 * @argument1 : name of the table.
 * @argument2 : rid of the tuple to be deleted.
 *
 * Return : 0 on success, -1 on file end
*/
RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
    // get the version number from the given rid
    // get the recordDescriptor for the given version of the table
    // call rbfm.deleteRecord(...) with above informations.
    if(!isTableExist(tableName) || isSystemTable(tableName)) return -1;

    std::vector<Attribute> recordDescriptor;
    std::vector<Attribute> recordDescriptorP;

    if(currFile == "") {
        rbfm.openFile(tableName, this->fileHandle);
        this->currFile = tableName;
    } else if(currFile != tableName) {
        rbfm.closeFile(this->fileHandle);
        this->currFile = tableName;
        rbfm.openFile(tableName, this->fileHandle);
    }

    int version = rbfm.getVersionOfRecord(this->fileHandle, rid);
    recordDescriptor = getAttributesForVersion(tableName, version);
    recordDescriptorP = recordDescriptor;
    processDescriptor(recordDescriptorP);


    if(recordDescriptor.size() == 0) return -1;

    if(rbfm.deleteRecord(this->fileHandle, recordDescriptorP, rid) == -1) {
        return -1;
    }

    IXFileHandle ixFileHandle;
    void* key = malloc(PAGE_SIZE);

    for(int i = 0 ;  i < (int)recordDescriptor.size(); i++) {
        std::string indexName = tableName + "_" + recordDescriptor[i].name + ".idx";
        if(this->tableMap.find(indexName) != this->tableMap.end()) {
            IndexManager::instance().openFile(indexName, ixFileHandle);
            this->readAttribute(tableName, rid, recordDescriptor[i].name, key);
            IndexManager::instance().insertEntry(ixFileHandle, recordDescriptor[i], (char*)key + 1, rid);
            IndexManager::instance().closeFile(ixFileHandle);
        }

    }

    free(key);
    return 0;
}

/**
 * updateTuple() - updates a tuple into table
 * @argument1 : name of the table.
 * @argument2 : updated data to be inserted.
 * @argument3 : rid of the tuple to be updated.
 *
 * Return : 0 on success, -1 on file end
*/
RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
    if(!isTableExist(tableName) || isSystemTable(tableName)) return -1;

    std::vector<Attribute> recordDescriptor;
    getAttributes(tableName, recordDescriptor);

    if(currFile == "") {
        rbfm.openFile(tableName, this->fileHandle);
        this->currFile = tableName;
    } else if(currFile != tableName) {
        rbfm.closeFile(this->fileHandle);
        this->currFile = tableName;
        rbfm.openFile(tableName, this->fileHandle);
    }

    if(rbfm.updateRecord(this->fileHandle, recordDescriptor, data, rid) == -1)  {
        rbfm.closeFile(this->fileHandle);
        return -1;
    }

    return 0;
}

/**
 * readTuple() - reads a tuple from the table
 * @argument1 : name of the table.
 * @argument2 : rid of the tuple to be read.
 * @argument3 : out parameter to return the read data.
 *
 * Return : 0 on success, -1 on file end
*/
RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
    // get the version number from the given rid
    // get the recordDescriptor for the given version of the table
    // call rbfm.readRecord(...) with above informations.
    // should handle the case where the record format doesnt conform with the latest schema changes.
    if(!isTableExist(tableName) || isSystemTable(tableName)) return -1;
    std::vector<Attribute> recordDescriptor;
    std::vector<Attribute> recordDescriptorP;
    if(currFile == "") {
        rbfm.openFile(tableName, this->fileHandle);
        this->currFile = tableName;
    } else if(currFile != tableName) {
        rbfm.closeFile(this->fileHandle);
        this->currFile = tableName;
        rbfm.openFile(tableName, this->fileHandle);
    }

    int version = rbfm.getVersionOfRecord(this->fileHandle, rid);
    recordDescriptor = getAttributesForVersion(tableName, version);
    recordDescriptorP = recordDescriptor;
    processDescriptor(recordDescriptorP);

    if(recordDescriptor.size() == 0) return -1;

    if(rbfm.readRecord(this->fileHandle, recordDescriptorP, rid, data) == -1) {
        return -1;
    }

    //If the read record doesnt match the latest schema, format it.
    int latestVersion = getLatestTableVersion(tableName);
    if (version != latestVersion) {
        std::vector<Attribute> recordDescriptorLatest = getAttributesForVersion(tableName, latestVersion);
        std::vector<Attribute> recordDescriptorLatestActual;
        getAttributes(tableName, recordDescriptorLatestActual);
        formatRecordWithLatestSchemaChange(recordDescriptor, recordDescriptorLatest,
                                           recordDescriptorLatestActual, data);
    }

    return 0;
}

/**
 * printTuple() - prints a tuple
 * @argument1 : columns of the tuple
 * @argument2 : data to be printed
 *
 * Return : 0 on success, -1 on file end
*/
RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data) {
    return rbfm.printRecord(attrs, data);
}

/**
 * printTuple() - reads a given column from the tuple
 * @argument1 : name of the table
 * @argument2 : rid of the tuple
 * @argument3 : name of the column to be read.
 * @argument4 : out parameter, data to be returned.
 *
 * Return : 0 on success, -1 on file end
*/
RC RelationManager::readAttribute(const std::string &tableName, const RID &rid,
                                  const std::string &attributeName,
                                  void *data) {
    // get the version number from the given rid
    // get the recordDescriptor for the given version of the table
    // call rbfm.readAttribute(...) with above informations.
    // needs to conform to new data schema.
    if(!isTableExist(tableName) || isSystemTable(tableName)) return -1;

    std::vector<Attribute> recordDescriptor;

    if(currFile == "") {
        rbfm.openFile(tableName, this->fileHandle);
        this->currFile = tableName;
    } else if(currFile != tableName) {
        rbfm.closeFile(this->fileHandle);
        this->currFile = tableName;
        rbfm.openFile(tableName, this->fileHandle);
    }


    int version = rbfm.getVersionOfRecord(this->fileHandle, rid);
    recordDescriptor = getAttributesForVersion(tableName, version);

    if(recordDescriptor.size() == 0) return -1;

    if(rbfm.readAttribute(this->fileHandle, recordDescriptor, rid, attributeName, data) == -1) {
        return -1;
    }

    return 0;
}

/**
 * scan() - Wrapper for RM_ScanIterator::initializeScanIterator()
 * @argument1 : name of the table
 * @argument2 : condition on which to scan.
 * @argument3 : operation on which to scan.
 * @argument4 : value to compare.
 * @argument5 : name of the attribute to be returned.
 * @argument6 : reference of the rm scan iterator.
 
 * Return : 0 on success, -1 on failure.
*/
RC RelationManager::scan(const std::string &tableName,
                         const std::string &conditionAttribute,
                         const CompOp compOp,
                         const void *value,
                         const std::vector<std::string> &attributeNames,
                         RM_ScanIterator &rm_ScanIterator) {

    if(!isTableExist(tableName)) return -1;

    if(rm_ScanIterator.initializeScanIterator(tableName, conditionAttribute,
                                              compOp, value, attributeNames) == -1) return -1;

    return 0;
}

/**
 * dropAttribute() - drop a column from a table
 * @argument1 : name of the table
 * @argument2 : column to be dropped
 *
 * Return : 0 on success, -1 on failure
*/
RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
    int latestVersion = getLatestTableVersion(tableName);
    std::vector<Attribute> recordDescriptor = getAttributesForVersion(tableName, latestVersion);
    for(int  i = 0 ; i < (int)recordDescriptor.size(); i++) {
        if(recordDescriptor[i].name == attributeName) {
            recordDescriptor[i].valid = INVALID;
            break;
         }
    }

    int tableID = -1;
    createAndInsertTablesData(tableName, tableID, UPDATED);
    createAndInsertColumnsData(tableName, recordDescriptor, tableID);
    return 0;    
}

/**
 * addAttribute() - add a column to a table
 * @argument1 : name of the table
 * @argument2 : column to be added
 *
 * Return : 0 on success, -1 on failure
*/
RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
    int latestVersion = getLatestTableVersion(tableName);
    std::vector<Attribute> recordDescriptor = getAttributesForVersion(tableName, latestVersion);
    recordDescriptor.push_back(attr);
    int tableID = -1;

    createAndInsertTablesData(tableName, tableID, UPDATED);
    createAndInsertColumnsData(tableName, recordDescriptor, tableID);
    return 0;
}

/**
 * createIndex() - add a index on given column
 * @argument1 : name of the table
 * @argument2 : column which index is created
 *
 * Return : 0 on success, -1 on failure
*/
RC RelationManager::createIndex(const std::string &tableName, const std::string &attributeName) {
    // No indexes to be created on system table.
    if(isSystemTable(tableName) || !isTableExist(tableName)) return -1;

    std::string indexFileName = tableName + "_" + attributeName + ".idx";

    std::vector<Attribute> attrs, indexAttr;

    this->getAttributes(tableName, attrs);

    for(RT i = 0; i < (RT)attrs.size(); i++) {
        if(attrs[i].name == attributeName) {
            indexAttr.push_back(attrs[i]);
            break;
        }
    }

    // No such attribute to create Index on.
    if(indexAttr.size() == 0) return -1;

    if(IndexManager::instance().createFile(indexFileName) == -1) return -1;

    //create Entry into catalog.
    int table_id = -1;
    createAndInsertTablesData(indexFileName, table_id);
    createAndInsertColumnsData(indexFileName, indexAttr, table_id);

    if(populateIndexOnAttribute(tableName, indexFileName, indexAttr) == -1) return -1;
    return 0;
}

/**
 * destroyIndex() - delete the index on given column
 * @argument1 : name of the table
 * @argument2 : column on which index is deleted.
 *
 * Return : 0 on success, -1 on failure
*/
RC RelationManager::destroyIndex(const std::string &tableName, const std::string &attributeName) {
    std::string indexFileName = tableName + "_" + attributeName + ".idx";
    if(isSystemTable(indexFileName) || !isTableExist(indexFileName)) return -1;

    deleteTableEntryFromCatalog(indexFileName);

    return IndexManager::instance().destroyFile(indexFileName);
}

/**
 * indexScan() - wrapper on initializeScanIterator() for index.
 * @argument1 : name of the table
 * @argument2 : name of the attribute on which index is there
 * @argument3 : low bound in comparison
 * @argument4 : upper bound in comparison
 * @argument5 : flag to decide if the range include lowKey or not.
 * @argument6 : flag to decide if the range include highKey or not.
 * @argument7 : ref. of rm index iteratr object
 
 * Return : 0 on success, -1 on failure.
*/
RC RelationManager::indexScan(const string &tableName,
                             const string &attributeName,
                             const void *lowKey,
                             const void *highKey,
                             bool lowKeyInclusive,
                             bool highKeyInclusive,
                             RM_IndexScanIterator &rm_IndexScanIterator) {

    return rm_IndexScanIterator.initializeScanIterator(tableName, attributeName, lowKey, highKey,
                                                       lowKeyInclusive, highKeyInclusive);
}

/**
 * initializeTableAttribute() - defines the schmea of Tables table.
 * @argument1 : vector of column as out parameter.
 *
 * Return : void.
 */
void RelationManager::initializeTableAttribute(vector<Attribute> &tableAttributes) {
    Attribute attr;

    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength)(sizeof(int));
    tableAttributes.push_back(attr);

    attr.name = "table-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    tableAttributes.push_back(attr);

    attr.name = "file-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    tableAttributes.push_back(attr);

    attr.name = "version";
    attr.type = TypeInt;
    attr.length = (AttrLength)(sizeof(int));
    tableAttributes.push_back(attr);
    return;
}

/**
 * initializeTableAttribute() - defines the schema of Columns table.
 * @argument1 : vector of column as out parameter.
 *
 * Return : void.
 */
void RelationManager::initializeColumnAttribute(vector<Attribute> &columnAttributes) {
    Attribute attr;

    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    columnAttributes.push_back(attr);

    attr.name = "column-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    columnAttributes.push_back(attr);

    attr.name = "column-type";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    columnAttributes.push_back(attr);

    attr.name = "column-length";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    columnAttributes.push_back(attr);

    attr.name = "column-position";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    columnAttributes.push_back(attr);

    attr.name = "version";
    attr.type = TypeInt;
    attr.length = (AttrLength) (sizeof(int));
    columnAttributes.push_back(attr);

    attr.name = "validity";
    attr.type = TypeInt;
    attr.length = (AttrLength)(sizeof(int));
    columnAttributes.push_back(attr);
    return;
}

/**
 * createAndInsertTablesData() - insert a new table's data into Tables table.
 * @argument1 : name of the table.
 * @argument2 : id of the table, out parameter.
 * @argument3 : flag to detect if the table is updated or new one is inserted.
 *
 * Return : 0 on success, -1 on failure.
 */
RC RelationManager::createAndInsertTablesData(const std::string& tableName,
                                              int& table_id, const int action) {

    int nullBytes = ceil((double)this->tableAttributes.size()/CHAR_BIT);
    char nullBitField[nullBytes];
    memset((char*)nullBitField, 0, nullBytes);
    int dataOffset = 0;
    void* data = malloc(nullBytes + 2*sizeof(int) + 2*(sizeof(int) + tableName.length()));
    memcpy((char*)data, (char*)nullBitField, nullBytes);
    dataOffset += nullBytes;

    table_id = getNewTableId(tableName);

    // table id
    memcpy((char*)data + dataOffset, (char*)&table_id, sizeof(int));
    dataOffset += sizeof(int);
    // table name
    int len = tableName.length();
    memcpy((char*)data + dataOffset, (char*)&len, sizeof(int));
    dataOffset += sizeof(int);
    memcpy((char*)data + dataOffset, (char*)tableName.c_str(), len);
    dataOffset += len;

    //file name
    len = (int)tableName.length();
    memcpy((char*)data + dataOffset, (char*)&len, sizeof(int));
    dataOffset += sizeof(int);
    memcpy((char*)data + dataOffset, (char*)tableName.c_str(), len);
    dataOffset += len;

    //latest version name
    int version = getNewTableVersion(tableName);
    //stores the latest version number for any table.
    memcpy((char*)data + dataOffset, (char*)&version, sizeof(int));

    RID tableRID;
    if(action != UPDATED) {
        rbfm.insertRecord(this->tableFileHandle, this->tableAttributes,
                          data, tableRID);
        tableMap[tableName].rid = tableRID;
        tableMap[tableName].tableID = table_id;
        rbfm.tableMap[tableName].rid = tableRID;
        rbfm.tableMap[tableName].tableID = table_id;
    } else {
        //for updated RID and table ID remains the same
        rbfm.updateRecord(tableFileHandle, this->tableAttributes,
                          data, tableMap[tableName].rid);
    }

    tableMap[tableName].version= version;
    rbfm.tableMap[tableName].version = version;
    free(data);
    return 0;
}

/**
 * createAndInsertColumnsData() - insert a new table's data into Columns table.
 * @argument1 : name of the table.
 * @argument2 : columns of the new table
 * @argument3 : id of the new table.
 *
 * Return : 0 on success, -1 on failure.
 */
RC RelationManager::createAndInsertColumnsData(const std::string& tableName, 
                                               const std::vector<Attribute>& recordDescriptor,
                                               int table_id) {

    int version = getNewTableVersion(tableName);
    columnsMap[tableName][version].recordDescriptor = recordDescriptor;
    rbfm.columnsMap[tableName][version].recordDescriptor = recordDescriptor;
    int nullBytes = ceil((double)this->columnAttributes.size()/CHAR_BIT);
    char nullBitField[nullBytes];
    memset(nullBitField, 0 , nullBytes);
    int columnNumber = 0;

    for(auto rec : recordDescriptor) {
        columnNumber++;
        void* data = malloc(nullBytes + 7*sizeof(int) + (int)rec.name.size());
        memcpy((char*)data, (char*)nullBitField, nullBytes);
        int dataOffset = nullBytes;
        memcpy((char*)data + dataOffset, (char*)&table_id, sizeof(int));
        dataOffset += sizeof(int);
        int len  = (int)rec.name.size();
        memcpy((char*)data + dataOffset, (char*)&len, sizeof(int));
        dataOffset += sizeof(int);
        memcpy((char*)data + dataOffset, (char*)rec.name.c_str(), len);
        dataOffset += len;
        memcpy((char*)data + dataOffset, (char*)&rec.type, sizeof(int));
        dataOffset += sizeof(int);
        memcpy((char*)data + dataOffset, (char*)&rec.length, sizeof(int));
        dataOffset += sizeof(int);
        memcpy((char*)data + dataOffset, (char*)&columnNumber, sizeof(int));
        dataOffset += sizeof(int);
        memcpy((char*)data + dataOffset, (char*)&version, sizeof(int));
        dataOffset += sizeof(int);
        memcpy((char*)data + dataOffset, (char*)&rec.valid, sizeof(int));
        RID rid;
        rbfm.insertRecord(this->columnFileHandle, this->columnAttributes, data, rid);
        columnsMap[tableName][version].ridAttribute.push_back(rid);
        free(data);
    }
    rbfm.columnsMap[tableName][version].ridAttribute = columnsMap[tableName][version].ridAttribute;
    return 0;
}

/**
 * initializeTableAndColumnFileHandles() - initializaitons of tableFileHandle and columnFileHandle.
 *
 * The file handles are used to access system catalog.
 *
 * Return : void
 */
void RelationManager::initializeTableAndColumnFileHandles () {
     rbfm.openFile(TABLES_FILE, this->tableFileHandle);
     rbfm.openFile(COLUMNS_FILE, this->columnFileHandle);
}

/**
 * initializeColumnMap() - initializaitons of in memory column map.
 *
 * Return : void.
 */
void RelationManager::initializeColumnMap () {
    std::vector<std::string> attributeNames;
    for(auto itr : this->columnAttributes) {
        attributeNames.push_back(itr.name);
    }
    void* value = NULL;
    RBFM_ScanIterator rbfmScanIterator;
    //passing dummy values to iterate through each record.
    rbfm.scan(this->columnFileHandle, this->columnAttributes, "", NO_OP, value, attributeNames, rbfmScanIterator);
    RID rid;
    void* data = malloc(PAGE_SIZE);
    while(rbfmScanIterator.getNextRecord(rid, data) != RBFM_EOF) {
        fillMapFromColumnEntry(data, rid);
    }
    correctOrderingOfAttributes();
    free(data);
    return;
}

/**
 * fillMapFromColumnEntry() - initializaitons of in-memory column map.
 *
 * Return : void
 */
void RelationManager::fillMapFromColumnEntry(const void* data, const RID& rid) {
    // table record format :
    // nullBytes | (int)id | (int + len) name | (int) type | (int) len | (int) colPos | (int) version | (int)validity |
    int nullBytes = ceil((double)this->columnAttributes.size()/CHAR_BIT);
    int tableID = -1;
    memcpy((char*)&tableID, (char*)data + nullBytes, sizeof(int));
    int dataOffset = nullBytes + sizeof(int);

    std::string tableName = getTableNameFromId(tableID);
    if(tableName.size() == 0) return;
    // Getting the name of the table/file
    int len = 0;
    memcpy((char*)&len, (char*)data + dataOffset, sizeof(int));
    char name[len+1];
    memcpy((char*)name, (char*)data + dataOffset + sizeof(int), len);
    name[len] = '\0';
    dataOffset += len + sizeof(int);

    Attribute attr;
    attr.name.assign(name);
    //setting offset for type
    memcpy((char*)&attr.type, (char*)data + dataOffset, sizeof(int));
    dataOffset += sizeof(int);
    memcpy((char*)&attr.length, (char*)data + dataOffset, sizeof(int));
    dataOffset += sizeof(int);
    memcpy((char*)&attr.colPos, (char*)data + dataOffset, sizeof(int));
    // setting offset to read version number
    dataOffset += sizeof(int);
    int version = 1;
    memcpy((char*)&version, (char*)data + dataOffset, sizeof(int));

    dataOffset += sizeof(int);
    memcpy((char*)&attr.valid, (char*)data + dataOffset, sizeof(int));

    this->columnsMap[tableName][version].recordDescriptor.push_back(attr);
    this->columnsMap[tableName][version].ridAttribute.push_back(rid);

    return;
}

/**
 * correctOrderingOfAttributes() - correcting the order of columns in columnMap
                                   based on column position.
 *
 * Return : void.
 */
void RelationManager::correctOrderingOfAttributes() {

    for(auto itr : this->columnsMap) {
        for(auto uitr : itr.second) {
            sort(uitr.second.recordDescriptor.begin(), uitr.second.recordDescriptor.end(), [] (Attribute& a, Attribute& b) {
                return a.colPos < b.colPos;
            });
        }
    }
    return;
}

/**
 * initializeTableMap() - initializaitons of in memory Table map.
 *
 * Return : void.
 */
void RelationManager::initializeTableMap() {
    std::vector<std::string> attributeNames;
    for(auto itr : this->tableAttributes) {
        attributeNames.push_back(itr.name);
    }
    void* value = NULL;
    RBFM_ScanIterator rbfmScanIterator;
    //passing dummy values to iterate through each record.
    rbfm.scan(this->tableFileHandle, this->tableAttributes, "", NO_OP, value, attributeNames, rbfmScanIterator);

    RID rid;
    void* data = malloc(PAGE_SIZE);
    while(rbfmScanIterator.getNextRecord(rid, data) != RBFM_EOF) {
        fillMapFromTableEntry(data, rid);
    }

    free(data);
    return;
}

/**
 * initializeTableMap() - initializaitons of in memory Table map.
 *
 * Return : void.
 */
void RelationManager::fillMapFromTableEntry(const void* data, const RID& rid) {
    //column data format :
    // | (int) id | (int + len) tableName | (int + len) fileName | (int)version
    int nullBytes = ceil((double)(this->tableAttributes.size())/CHAR_BIT);
    TablesTableInfo info;
    info.rid = rid;
    memcpy((char*)&info.tableID, (char*)data + nullBytes, sizeof(int));
    int dataOffset = nullBytes + sizeof(int);

    // Getting the name of the table/file
    int len = 0;
    memcpy((char*)&len, (char*)data + dataOffset, sizeof(int));
    char name[len+1];
    memcpy((char*)name, (char*)data + dataOffset + sizeof(int), len);
    name[len] = '\0';
    //set offset for version
    dataOffset += 2*(len + sizeof(int));
    memcpy((char*)&info.version, (char*)data + dataOffset, sizeof(int));

    std::string tableName(name);
    this->tableMap[tableName] = info;
    return;
}


/**
 * getNewTableVersion() - get the version for a new table.
 * @argument1 : name of the table.
 *
 * Return : version for the table.
 */
int RelationManager::getNewTableVersion(const std::string& tableName) {
    if(columnsMap.find(tableName) == columnsMap.end()) {
        return 1;
    }
    // would be used when table schemas change.
    return columnsMap[tableName].size() + 1;
}

/**
 * getLatestTableVersion() - get the latest verison for a table.
 * @argument1 : name of the table.
 *
 * Call this only when we know table exists.
 *
 * Return : latest version for the table.
 */
RC RelationManager::getLatestTableVersion(const std::string& tableName) {
    return this->tableMap[tableName].version;
}

/**
 * getNewTableId() - get the ID for a new table.
 * @argument1 : name of the table.
 *
 * Return : ID for the table.
 */
RC RelationManager::getNewTableId(const std::string& tableName) {
    if(tableMap.find(tableName) == tableMap.end()) {
        int max_id = 0;
        for(auto itr : tableMap) {
            max_id = max(max_id,itr.second.tableID);
        }
        return max_id + 1;
    }
    return tableMap[tableName].tableID;
}

/**
 * getAttributesForVersion() - get the columns for a given version of the table.
 * @argument1 : name of the table.
 * @argument2 : version of the table.
 *
 * Return : vector of columns/attribute.
 */
vector<Attribute> RelationManager::getAttributesForVersion(const std::string& tableName, const int version) {
    vector<Attribute> attr;
    if(columnsMap.find(tableName) == columnsMap.end()) return attr;
    return columnsMap[tableName][version].recordDescriptor;
}

/**
 * getTableNameFromId() - get the name of the table with given ID
 *
 * needs tableMap to be initialized before calling
 *
 * Return : name of the table
 */
std::string RelationManager::getTableNameFromId(const int tableID) {
    for(auto itr : tableMap) {
        if(tableID == itr.second.tableID) {
            return itr.first;
        }
    }
    
    return "";
}

/**
 * formatRecordWithLatestSchemaChange() - formats a record with older schema to conform with latest schema.
 * @argument1 : current record descriptor of the record.
 * @argument2 : latest record descriptor without the invalid columns.
 * @argument3 : latest record desciprot with the invalid columns as well.
 *
 * Return : void.
 */
void RelationManager::formatRecordWithLatestSchemaChange(std::vector<Attribute>& recordDescriptor, 
                                                         std::vector<Attribute>& recordDescriptorLatest,
                                                         std::vector<Attribute>& recordDescriptorLatestOriginal, 
                                                         void* data) {

    int nullBytesLat = ceil((double)recordDescriptorLatestOriginal.size()/CHAR_BIT);
    int nullBytes = ceil((double)recordDescriptor.size()/CHAR_BIT);
    char nullField[nullBytes];
    memcpy((char*)nullField, (char*)data, nullBytes);
    char nullFieldLat[nullBytesLat];
    memset((char*)nullFieldLat, 0, nullBytesLat);

    void* formatData = malloc(PAGE_SIZE);
    int dataOffsetLat = nullBytesLat;
    int dataOffset = nullBytes;

    int j = 0;
    for(int i = 0 ; i < (int)recordDescriptor.size(); i++) {
        int nullBit = nullField[i/CHAR_BIT] & (1 << (CHAR_BIT - 1 - i%CHAR_BIT));
        if(nullBit == 0) {
            if (recordDescriptor[i].valid == VALID && recordDescriptorLatest[i].valid == INVALID) {
                if(recordDescriptor[i].type == TypeInt) {
                    dataOffset += sizeof(int);
                } else if(recordDescriptor[i].type == TypeReal) {
                    dataOffset += sizeof(float);
                } else {
                    int len = 0;
                    memcpy((char*)&len, (char*)data + dataOffset, sizeof(int));
                    dataOffset += sizeof(int) + len;
                }
            } else if (recordDescriptor[i].valid == VALID && recordDescriptorLatest[i].valid == VALID) {
                if (recordDescriptor[i].type == TypeInt) {
                    memcpy((char*)formatData + dataOffsetLat, (char*)data + dataOffset, sizeof(int));
                    dataOffset += sizeof(int);
                    dataOffsetLat += sizeof(int);
                } else if (recordDescriptor[i].type == TypeReal) {
                    memcpy((char*)formatData + dataOffsetLat, (char*)data + dataOffset, sizeof(float));
                    dataOffset += sizeof(float);
                    dataOffsetLat += sizeof(float);
                } else {
                    int len = 0; 
                    memcpy((char*)&len, (char*)data + dataOffset, sizeof(int));
                    memcpy((char*)formatData + dataOffsetLat, (char*)data + dataOffset, sizeof(int) + len);
                    dataOffset += sizeof(int) + len;
                    dataOffsetLat += sizeof(int) + len;
                }
               j++;
            }
        } else {
            if (recordDescriptor[i].valid == VALID && recordDescriptorLatest[i].valid == VALID) {
                nullFieldLat[j/CHAR_BIT] |= (1 << (CHAR_BIT - 1 - j%CHAR_BIT));
                j++;
            }
        }
    }


    while(j < (int)recordDescriptorLatestOriginal.size()) {
        int shift = CHAR_BIT - 1 - j%CHAR_BIT;
        nullFieldLat[j/CHAR_BIT] |= (1 << (shift));
        j++;
    }

    memcpy((char*)formatData, (char*)nullFieldLat, nullBytesLat);
    memcpy((char*)data, (char*)formatData, dataOffsetLat);
    free(formatData);
    return ;

}

/**
 * deleteTableEntryFromCatalog() - delete the entry of a table from catalog.
 * @argument1 : name of the table.
 *
 * Return : 0 on success, -1 on failure.
 */
RC RelationManager::deleteTableEntryFromCatalog(const std::string& tableName) {

    RID tableRID = this->tableMap[tableName].rid;
    rbfm.deleteRecord(this->tableFileHandle, this->tableAttributes, tableRID);

    for(auto itr : columnsMap[tableName]) {
        for(auto rids : itr.second.ridAttribute) {
            rbfm.deleteRecord(this->columnFileHandle, this->columnAttributes, rids);
        }
    } 

    this->tableMap.erase(tableName);
    this->columnsMap.erase(tableName);
    rbfm.columnsMap.erase(tableName);
    rbfm.tableMap.erase(tableName);

    return 0;
}

/**
 * populateIndexOnAttribute() - bulk loading of the index data structure (B+ tree in this case).
 * @argument1 : name of the table.
 * @argument2 : name of the index file.
 * @argument3 : record descriptor with only the column on which index is populated.
 *
 * Return : 0 on success, -1 on failure.
 */
RC RelationManager::populateIndexOnAttribute(const std::string& tableName,
                                             const::std::string& indexFileName,
                                             const std::vector<Attribute>& indexAttr) {

    vector<std::string> indexAttrNames;
    indexAttrNames.push_back(indexAttr[0].name);
    RM_ScanIterator rmsi;
    if(this->scan(tableName, "", NO_OP, NULL, indexAttrNames, rmsi) == -1) return -1;

    RID returnedRID;
    void* returnedData = malloc(PAGE_SIZE);

    IXFileHandle ixFileHandle;
    if(IndexManager::instance().openFile(indexFileName, ixFileHandle) == -1) return -1;

    while(rmsi.getNextTuple(returnedRID, returnedData) != RM_EOF) {
        IndexManager::instance().insertEntry(ixFileHandle, indexAttr[0], (char*)returnedData + 1, returnedRID);
    }

    rmsi.close();
    IndexManager::instance().closeFile(ixFileHandle);
    free(returnedData);
    return 0;
}

/**
 * isSystemTable() - if a file is a system file.
 * @argument1 : name of the table.
 *
 * Return : true if system, false otherwise.
 */
bool RelationManager::isSystemTable(const std::string &tableName) {
    return (tableName == TABLES_FILE || tableName == COLUMNS_FILE);
}

/**
 * isTableExist() - checks if a given table exists.
 * @argument1 : name of the table.
 *
 * Return : true if exists, false otherwise.
 */
bool RelationManager::isTableExist(const std::string &tableName) {
   struct stat stFileInfo{};
   return stat(tableName.c_str(), &stFileInfo) == 0;
}

/**
 * isCatalogInitialized() - checks if a system catalog has been initialized.
 *
 * Return : true if initialized, false otherwise.
 */
bool RelationManager::isCatalogInitialized() {
    struct stat stFileInfo{};
    bool f =  stat(TABLES_FILE, &stFileInfo) == 0;
    return f;
}

/**
 * processDescriptor() - removes the invalid column from the record descriptor.
 *
 * Used in drop/add attribute.
 *
 * Return : void.
 */
void RelationManager::processDescriptor(vector<Attribute>& attrs) {
    attrs.erase(std::remove_if(attrs.begin(), attrs.end(),
                       [](Attribute a) { return a.valid ==  INVALID; }), attrs.end());
 }
