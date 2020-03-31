#ifndef _rbfm_h_
#define _rbfm_h_
#include <vector>
#include <climits>
#include <cstring>
#include <math.h>
#include <iostream>
#include <unordered_set>
#include "pfm.h"

using namespace std;

const RT LEFT = -1;
const RT RIGHT = 1;

const RT NULL_POINT = 8000;
const RT MIN_RECORD_SIZE = 6; //in bytes
const int INVAL_TYPE = -1;
const int VALID = -15;
const int INVALID = -16;

// Record ID
struct RID {
    int pageNum;    // page number
    unsigned short slotNum;    // slot number in the page

    bool operator==(const RID& b) const {
        return this->pageNum == b.pageNum && this->slotNum == b.slotNum;
    }

    bool operator >(const RID& b) const {
        if(this->pageNum > b.pageNum) return true;
        else if(this->pageNum == b.pageNum && this->slotNum > b.slotNum) return true;
        return false;
    }

    bool operator!=(const RID& b) const {
        return !(*this == b);
    }

    bool operator>=(const RID& b) const {
        return (*this == b) || (*this > b);
    }
}; 

// Attribute
typedef enum {
    TypeInt = 0, TypeReal, TypeVarChar
} AttrType;

typedef unsigned AttrLength;

struct Attribute {
    std::string name;  // attribute name
    AttrType type;     // attribute type
    AttrLength length; // attribute length
    int colPos;
    int valid;
    Attribute () {
        valid = VALID;
    }
};

struct ColumnTableInfo {
    std::vector<Attribute> recordDescriptor;
    std::vector<RID> ridAttribute;
};

struct TablesTableInfo {
    int version;
    int tableID;
    RID rid;

    TablesTableInfo() {
        version = 1;
    }
};

// Comparison Operator (NOT needed for part 1 of the project)
typedef enum {
    EQ_OP = 0, // no condition// =
    LT_OP,      // <
    LE_OP,      // <=
    GT_OP,      // >
    GE_OP,      // >=
    NE_OP,      // !=
    NO_OP       // no condition
} CompOp;


/********************************************************************
* The scan iterator is NOT required to be implemented for Project 1 *
********************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

//  RBFM_ScanIterator is an iterator to go through records
//  The way to use it is like the following:
//  RBFM_ScanIterator rbfmScanIterator;
//  rbfm.open(..., rbfmScanIterator);
//  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
//    process the data;
//  }
//  rbfmScanIterator.close();

//Utility functions

// Record information retreival helpers
RT getDataSizeWithoutNullBytes(const std::vector<Attribute>& recordDesc, const void* data);

void getDirAndRecPointers(RT& dirSlotPointer, RT& recordSlotPointer, void* pageData);

RT getOffsetAndSizeFromRID(FileHandle& fileHandle, const RID& rid, RT& formattedDataSize, 
                           RID& final_rid, RT& update_flag , RT& initOffset, void* pageData);

RT getFreeSlotInPage(const RT dirSlotPointer, const void* pageData, bool& existingSlot);

bool isValidRID(const RID& nextRID, const void* data);

//Page directory handlers
void addEntryToPageDirectory(PageNum pageNum, RT offset, RID& rid, RT dirSlotPointer, 
                             RT recordSlotPointer, RT formattedDataSize, void* pageData);

void updateDirAndRecPointers(RT dirSlotPointer, RT recordSlotPointer, void* pageData);

RC updateSlotInPageDirectory(RT offset, RT formattedDataSize, RT update_flag, RT slotNum, void* pageData);

RC updatePageDirectoryByOffset(RT startOffset, RT moveOffset, RT dirSlotPointer, RT slotNum, void* pageData);

// Data formatter functions
RC moveRecordsByOffset(RT startOffset, RT moveOffset, RT direction, RT slotNum, RT offset, RT recordSize,
                       RT update_flag, void* pageData);

void* formatDataForStoring(const std::vector<Attribute>& recordDesc, const void* data, RT& formattedDataSize);

void formatDataForReading(RT offset, RT formattedDataSize,
                          const std::vector<Attribute>& recordDescriptor,
                          void* pageData, void* data); 

void* generateNullBitField (const std::vector<Attribute>& recordDesc, const void* data);

void storeDataInFile(FileHandle& fileHandle, PageNum freePage, RT offset, 
                     const void* formattedData, RT formattedDataSize, 
                     RID& rid, void* pageData);

//Math
bool compareTypeInt(const void* data1, const void* data2, const CompOp compOp);

bool compareTypeReal(const void* data1, const void* data2, const CompOp compOp);

bool compareTypeVarChar(const void* data1, const void* data2, const CompOp compOp);

void incrementFreeSlotsInPage(void* pageData);

void decrementFreeSlotsInPage(void* pageData);

class RBFM_ScanIterator {

private:
    FileHandle* fileHandle;
    std::string conditionAttribute;
    void* compValue;
    CompOp compOp;
    AttrType type;
    RID currentRID;
    std::vector<Attribute> recordDescriptor;
    std::vector<std::string> attributeNames;
public:
    RBFM_ScanIterator() {
        compValue = NULL;
    }

    ~RBFM_ScanIterator() {
        if(compValue != NULL)
            free(compValue);
    }

    RC initializeScanIterator(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                              const std::string &conditionAttribute, const CompOp compOp, const void *value,
                              const std::vector<std::string> &attributeNames);

    RID getNextValidRID(RID currentRID, void* data);
    bool recordComparison(const RID& rid, void* data);

    // Never keep the results in the memory. When getNextRecord() is called,
    // a satisfying record needs to be fetched from the file.
    // "data" follows the same format as RecordBasedFileManager::insertRecord().
    RC getNextRecord(RID &rid, void *data);

    RC close() { return -1; };
};

class RecordBasedFileManager {
private:
    RC insertUpdatedRecord(FileHandle& fileHandle, const std::vector<Attribute>& recordDescriptor,
                           const void* newData, const RT recordSize, RID& newRid, const int version);
public:
    std::unordered_map<std::string, std::unordered_map<int, ColumnTableInfo>> columnsMap;
    std::unordered_map<std::string, TablesTableInfo> tableMap;

    static RecordBasedFileManager &instance();                          // Access to the _rbf_manager instance

    RC createFile(const std::string &fileName);                         // Create a new record-based file

    RC destroyFile(const std::string &fileName);                        // Destroy a record-based file

    RC openFile(const std::string &fileName, FileHandle &fileHandle);   // Open a record-based file

    RC closeFile(FileHandle &fileHandle);                               // Close a record-based file

    //  Format of the data passed into the function is the following:
    //  [n byte-null-indicators for y fields] [actual value for the first field] [actual value for the second field] ...
    //  1) For y fields, there is n-byte-null-indicators in the beginning of each record.
    //     The value n can be calculated as: ceil(y / 8). (e.g., 5 fields => ceil(5 / 8) = 1. 12 fields => ceil(12 / 8) = 2.)
    //     Each bit represents whether each field value is null or not.
    //     If k-th bit from the left is set to 1, k-th field value is null. We do not include anything in the actual data part.
    //     If k-th bit from the left is set to 0, k-th field contains non-null values.
    //     If there are more than 8 fields, then you need to find the corresponding byte first,
    //     then find a corresponding bit inside that byte.
    //  2) Actual data is a concatenation of values of the attributes.
    //  3) For Int and Real: use 4 bytes to store the value;
    //     For Varchar: use 4 bytes to store the length of characters, then store the actual characters.
    //  !!! The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute().
    // For example, refer to the Q8 of Project 1 wiki page.

    // Insert a record into a file
    RC insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, 
                    const void *data, RID &rid);

    // Read a record identified by the given rid.
    RC readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, 
                  const RID &rid, void *data);

    // Print the record that is passed to this utility method.
    // This method will be mainly used for debugging/testing.
    // The format is as follows:
    // field1-name: field1-value  field2-name: field2-value ... \n
    // (e.g., age: 24  height: 6.1  salary: 9000
    //        age: NULL  height: 7.5  salary: 7500)
    RC printRecord(const std::vector<Attribute> &recordDescriptor, const void *data);

    /*****************************************************************************************************
    * IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) *
    * are NOT required to be implemented for Project 1                                                   *
    *****************************************************************************************************/
    // Delete a record identified by the given rid.
    RC deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid);

    // Assume the RID does not change after an update
    RC updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data,
                    const RID &rid);

    // Read an attribute given its name and the rid.
    RC readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid,
                     const std::string &attributeName, void *data);

    RC readAttributeOptimized(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid,
                     const std::string &attributeName, void *data, void* pageData);


    RC readAttributes(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid,
                      const std::vector<std::string> &attributeNames, void *data, void* pageData);

    // Scan returns an iterator to allow the caller to go through the results one by one.
    RC scan(FileHandle &fileHandle,
            const std::vector<Attribute> &recordDescriptor,
            const std::string &conditionAttribute,
            const CompOp compOp,                  // comparision type such as "<" and "="
            const void *value,                    // used in the comparison
            const std::vector<std::string> &attributeNames, // a list of projected attributes
            RBFM_ScanIterator &rbfm_ScanIterator);

    // API's for version control of the Table
    RC insertVersionOfRecord(FileHandle& fileHandle, const RID& rid, const RT version);

    RC updateVersionOfRecord(const void* pageData, const RID& rid, const RT version);

    RT getVersionOfRecord(FileHandle& fileHandle, const RID& rid);

    RT getVersionOfRecordWithPage(const void* pageData, const RID& rid);

    vector<Attribute> getAttributesForVersion(const std::string& tableName, const int version);

    RC getLatestTableVersion(const std::string& tableName);

    bool isSystemFile(const std::string& fileName);

protected:
    RecordBasedFileManager();                                                   // Prevent construction
    ~RecordBasedFileManager();                                                  // Prevent unwanted destruction
    RecordBasedFileManager(const RecordBasedFileManager &);                     // Prevent construction by copying
    RecordBasedFileManager &operator=(const RecordBasedFileManager &);          // Prevent assignment
};

#endif // _rbfm_h_
