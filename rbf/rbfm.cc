#include "rbfm.h"
#include "../rm/rm.h"

/**
 * scan() - scan initializer for RBFM based file.
 * @argument1 : filehandle of the file.
 * @argument2 : record descriptor.
 * @argument3 : condition Attribute (for comparison).
 * @argument4 : comparison operation to be formed.
 * @argument5 : value to be compared with.
 * @argument6 : name of the attribute required in the return (output) data.

 * Return : 0 on success, -1 on failure.
*/
RC RBFM_ScanIterator::initializeScanIterator(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const std::string &conditionAttribute, const CompOp compOp,
                                             const void *value, const std::vector<std::string> &attributeNames) {

    if(recordDescriptor.size() == 0 || attributeNames.size() == 0 ) return -1;

    for(auto itr : recordDescriptor) {
        if(itr.name == conditionAttribute) {
            this->type = itr.type;
            if(value == NULL) this->compValue = NULL;
            else if(itr.type == TypeReal) {
                (this->compValue) = malloc(sizeof(float));
                memcpy((char*)(this->compValue), (char*)value, sizeof(float));
            } else if(itr.type == TypeInt) {
                (this->compValue) = malloc(sizeof(int));
                memcpy((char*)(this->compValue), (char*)value, sizeof(int));
            } else {
                int length = 0;
                (this->compValue) = malloc(sizeof(int) + length);
                memcpy((char*)&length, (char*)value, sizeof(int));
                memcpy((char*)(this->compValue), (char*)value, sizeof(int) + length);
            }
            break;
        }
    }

    this->fileHandle = &fileHandle;
    this->recordDescriptor = recordDescriptor;
    this->attributeNames = attributeNames;
    this->conditionAttribute = conditionAttribute;
    this->compOp = compOp;
    this->currentRID.pageNum = -2;
    this->currentRID.slotNum = -1;
    return 0;
}

/**
 * getNextValidRID() - get the next valid(matching scan condition) record using the RBFM iterator
 * @argument1 : RID of the previous record.
 * @argument2 :  page where the record sits.
 *
 * Return : RID of the record matching the condition
*/
RID RBFM_ScanIterator::getNextValidRID (RID currentRID, void* data) {
    RID nextRID;
    nextRID.pageNum = -1;
    nextRID.slotNum = -1;

    if(currentRID.pageNum == -1) return nextRID;
    if(currentRID.pageNum == -2) {
        currentRID.pageNum = 0;
        currentRID.slotNum = 0;
    }

    int totalPages = this->fileHandle->getNumberOfPages();
    for(int i = currentRID.pageNum ; i < totalPages; i++) {
        nextRID.pageNum = i;
        if(this->fileHandle->readPage(nextRID.pageNum, data) == -1) {
            continue;
        }
        RT totalSlots = this->fileHandle->getTotalSlotsInPage(data);
        RT startSlot = i == currentRID.pageNum ? currentRID.slotNum + 1 : 1;
        for(RT j = startSlot; j <= totalSlots; j++) {
            nextRID.slotNum = j;
            if(isValidRID(nextRID, data) && recordComparison(nextRID, data)) {
                return nextRID;
            }
        }
    }
    nextRID.pageNum = -1;
    return nextRID;
}

/**
 * recordComparison() - Compares record based on the iterator condition.
 * @argument1 : RID of the record.
 * @argument2 :  page where the record sits.
 *
 * Return : comparsion result.
*/
bool RBFM_ScanIterator::recordComparison(const RID& rid, void* pageData) {
    if(this->compOp == NO_OP) return true;

    void* attributeValue;
    bool retVal = false;
    if(this->type == TypeReal) {
        attributeValue = malloc(sizeof(float));
        if(RecordBasedFileManager::instance().readAttributeOptimized(*(this->fileHandle), this->recordDescriptor, rid,
                                                         this->conditionAttribute, attributeValue, pageData) == NULL_POINT) {
            free(attributeValue);
            if(this->compValue == NULL) return true;
            return false;
        }

        retVal = compareTypeReal(attributeValue, this->compValue, compOp);
    } else if (this->type == TypeInt) {
        attributeValue = malloc(sizeof(int));
        if(RecordBasedFileManager::instance().readAttributeOptimized(*(this->fileHandle), this->recordDescriptor, rid,
                                                         this->conditionAttribute, attributeValue, pageData) == NULL_POINT) {
            free(attributeValue);
            if(this->compValue == NULL) return true;
            return false;
        }
        retVal =  compareTypeInt(attributeValue, this->compValue, compOp);
    } else {
        attributeValue = malloc(PAGE_SIZE);
        if(RecordBasedFileManager::instance().readAttributeOptimized(*(this->fileHandle), this->recordDescriptor, rid,
                                                         this->conditionAttribute, attributeValue, pageData) == NULL_POINT) {
            free(attributeValue);
            if(this->compValue == NULL) return true;
            return false;
        }
        int length = 0;
        memcpy((char*)&length, (char*)attributeValue, sizeof(int));
        attributeValue = realloc(attributeValue, sizeof(int) + length);
        retVal  = compareTypeVarChar(attributeValue, this->compValue, compOp);
    }
    free(attributeValue);
    return retVal;
}

/**
 * getNextRecord() - Wrapper on getNextValidRecord()
 * @argument1 : RID of the next record
 * @argument2 :  data of the next record.
 *
 * Return : 0 on success, RBFM_EOF on failure
*/
RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
    void* pageData = malloc(PAGE_SIZE);
    this->currentRID = getNextValidRID(this->currentRID, pageData);
    if(this->currentRID.pageNum == -1) {
        free(pageData);
        return RBFM_EOF;
    }

    rid = this->currentRID;
    RecordBasedFileManager::instance().readAttributes(*(this->fileHandle), this->recordDescriptor, this->currentRID,
    this->attributeNames, data, pageData);
    free(pageData);
    return 0;
}

RecordBasedFileManager &RecordBasedFileManager::instance() {
    static RecordBasedFileManager _rbf_manager = RecordBasedFileManager();
    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager() = default;

RecordBasedFileManager::~RecordBasedFileManager() = default;

RecordBasedFileManager::RecordBasedFileManager(const RecordBasedFileManager &) = default;

RecordBasedFileManager &RecordBasedFileManager::operator=(const RecordBasedFileManager &) = default;

/**
 * destroyFile() - creates a given file
 * @argument1 : Name of the file
 *
 * Return : 0 on success, -1 on failure.
*/
RC RecordBasedFileManager::createFile(const std::string &fileName) {
    return PagedFileManager::instance().createFile(fileName);
}

/**
 * destroyFile() - deletes a given file
 * @argument1 : Name of the file
 *
 * Return : 0 on success, -1 on failure.
*/
RC RecordBasedFileManager::destroyFile(const std::string &fileName) {
    return PagedFileManager::instance().destroyFile(fileName);
}

/**
 * openFile() - opens a given file
 * @argument1 : Name of the file
 * @argument2 : Filehandle returned as out parameter by reference.
 *
 * Return : 0 on success, -1 on failure.
*/
RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
    return PagedFileManager::instance().openFile(fileName, fileHandle);
}

/**
 * closeFile() - closes a given file
 * @argument1 : Filehandle of the file.
 *
 * Return : 0 on success, -1 on failure.
*/
RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return PagedFileManager::instance().closeFile(fileHandle);
}

/**
 * insertRecord() - inserts a given record
 * @argument1 : Filehandle of the file.
 * @argument2 : record descriptor.
 * @argument3 : record data to be inserted
 * @argument4 : return RID of the inserted record by reference.
 *
 * Return : 0 on success, -1 on failure.
*/
RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const void *data, RID &rid) {

    //Check if the file is open or not
    if(recordDescriptor.size() == 0) return -1;
    int currentPage = fileHandle.getNumberOfPages() - 1;
    RT formattedDataSize = 0;
    void* formattedData = formatDataForStoring(recordDescriptor, data, formattedDataSize);
    void* pageData = malloc(PAGE_SIZE);
    int retVal = fileHandle.readPage(currentPage, pageData);
    //Check wether the current Page has free space for the given record.
    if (retVal != -1) {
        RT offset = fileHandle.hasEnoughSpace(pageData, formattedDataSize);
        if(offset != -1) {
            storeDataInFile(fileHandle, currentPage, offset, formattedData, formattedDataSize, rid, pageData);
            free(formattedData);
            free(pageData);
            return 0;
        }
    }

    int freePage = fileHandle.findFreePage(formattedDataSize);
    if(freePage != -1) {
        fileHandle.readPage(freePage, pageData);
        RT offset = fileHandle.hasEnoughSpace(pageData, formattedDataSize);
        storeDataInFile(fileHandle, freePage, offset, formattedData, formattedDataSize, rid, pageData);
        free(formattedData);
        free(pageData);
        return 0;
    }

    // if control reaches here -> append a new page   
    int newPage = fileHandle.getNumberOfPages();
    fileHandle.initPageDirectory(pageData);
    storeDataInFile(fileHandle, newPage, 0, formattedData, formattedDataSize, rid, pageData);
    free(pageData);
    free(formattedData);
    
    return 0;
}

/**
 * readRecord() - reads a given record
 * @argument1 : Filehandle of the file.
 * @argument2 : record descriptor.
 * @argument3 : rid of the record to be read.
 *
 * Return : 0 on success, -1 on failure.
*/
RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                      const RID &rid, void *data) {
    if(recordDescriptor.size() == 0) return -1;

    void* pageData = malloc(PAGE_SIZE);
    if(fileHandle.readPage(rid.pageNum, pageData) == -1) {
        free(pageData);
        return -1;
    }

    RID final_rid = rid;
    RT update_flag = 0, formattedDataSize = 0 , initOffset = 0;
    RT offset = getOffsetAndSizeFromRID(fileHandle, rid, formattedDataSize, final_rid, update_flag, initOffset, pageData);
    //Trying to read a deleted record
    if(offset == DELETED) {
        free(pageData);
        return -1;
    }
    // If the record has benn previously updated, read the page where the record has been stored after updation.
    formatDataForReading(offset, formattedDataSize, recordDescriptor, pageData, data);
    free(pageData);
    return 0;
}

/**
 * deleteRecord() - deletes a given record
 * @argument1 : Filehandle of the file.
 * @argument2 : record descriptor.
 * @argument3 : rid of the record to be deleted.
 *
 * Return : 0 on success, -1 on failure.
*/
RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const RID &rid) {
    if(recordDescriptor.size() == 0) return -1;

    RT formattedDataSize = 0, update_flag = 0, offset = 0, initOffset = 0;
    RID final_rid = rid;
    void* pageData = malloc(PAGE_SIZE);
    void* oldData = malloc(PAGE_SIZE);

    if(fileHandle.readPage(rid.pageNum, pageData) == -1) return -1;
    memcpy((char*)oldData, (char*)pageData, PAGE_SIZE);

    offset = getOffsetAndSizeFromRID(fileHandle, rid, formattedDataSize, final_rid, update_flag, initOffset, pageData);
    //record has already been deleted
    if(offset == -1) return -1;
    //record has been updated, need to delete from the page where it is actually stored. we should also delete 
    //the place holder rid's value from the original page.
    if(update_flag == UPDATED) {
        // First remove he placeholder rid's from initial page
        RT startOffset = initOffset + sizeof(int) + sizeof(RT);
        RT moveOffset = sizeof(int) + sizeof(RT);
        moveRecordsByOffset(startOffset, moveOffset, LEFT, rid.slotNum, DELETED, 0 , 0, oldData);
        incrementFreeSlotsInPage(oldData);
        fileHandle.writePage(rid.pageNum, oldData);

        //deleting from the page where updated record actually sits.
        startOffset = offset + formattedDataSize;
        moveOffset = formattedDataSize;
        moveRecordsByOffset(startOffset, moveOffset, LEFT, final_rid.slotNum, DELETED, 0, 0, pageData);
        incrementFreeSlotsInPage(pageData);
        fileHandle.writePage(final_rid.pageNum, pageData);

        free(oldData);
        free(pageData);
        return 0;
    }

    // Record was not preiovusly modified.
    RT startOffset = offset + formattedDataSize;
    RT moveOffset = formattedDataSize;
    moveRecordsByOffset(startOffset, moveOffset, LEFT, final_rid.slotNum, DELETED, 0, 0, pageData);
    //Mark the slot for given record as deleted, hence can be used later.
    incrementFreeSlotsInPage(pageData);
    fileHandle.writePage(final_rid.pageNum, pageData);

    free(oldData);
    free(pageData);
    return 0;
}

/**
 * printRecord() - prints a given record
 * @argument1 : record descriptor.
 * @argument2 : record data
 *
 * Return : 0 on success, -1 on failure.
*/
RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data) {
    if(recordDescriptor.size() == 0) return -1;

    RT nullBytes = ceil((double)recordDescriptor.size()/CHAR_BIT);
    char nullBitField[nullBytes];
    memset(nullBitField, 0, nullBytes);
    memcpy(nullBitField, (char*)data, nullBytes);
    RT recordCounter = nullBytes;
    for(int i = 0 ; i < (int)recordDescriptor.size(); i++) {
        int shift = CHAR_BIT - 1 - i%CHAR_BIT;
        int nullbit = nullBitField[i/CHAR_BIT] & (1 << (shift));
        if(nullbit) {
            std::cout<<recordDescriptor[i].name<<": ";
            std::cout<<"NULL"<<" ";
        } else {
            std::cout<<recordDescriptor[i].name<<": ";
            
            if(recordDescriptor[i].type == TypeReal) {
                float val = 0.0;
                memcpy(&val, (char*)data + recordCounter, sizeof(float));
                recordCounter += sizeof(float);
                std::cout<<val<<" ";
            } else if (recordDescriptor[i].type == TypeInt) {
                int val = 0;
                memcpy(&val, (char*)data + recordCounter, sizeof(int));
                recordCounter += sizeof(int);
                std::cout<<val<<" "; 
            } else {
                int varcharlen = 0;
                memcpy(&varcharlen, (char*)data + recordCounter, sizeof(int));
                recordCounter += sizeof(int);
                char* val = new char[varcharlen+1];
                memcpy(val, (char*)data + recordCounter, varcharlen);
                recordCounter += varcharlen;
                val[varcharlen] = '\0';
                std::cout<<val<<" ";
                delete[] val;
              }
        }
    }

    std::cout<<std::endl;
    return 0;
}

/**
 * updateRecord() - Updates a given record
 * @argument1 : filehandle of the file.
 * @argument2 : record descriptor.
 * @argument3 : updated record data.
 * @argument4 : RID of the record to be updated.
 *
 * Return : 0 on success, -1 on failure.
*/
RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const void *data, const RID &rid) {
    if(recordDescriptor.size() == 0) return -1;

    RT newDataSize = 0;
    void* newData = formatDataForStoring(recordDescriptor, data, newDataSize);

    // Get the original record size.
    RT formattedDataSize = 0, update_flag = 0, initOffset = 0, offset = 0;
    RID finalRid = rid;
    void* pageData = malloc(PAGE_SIZE);
    if(fileHandle.readPage(rid.pageNum, pageData) == -1) return -1;
    void* oldData = malloc(PAGE_SIZE);
    memcpy((char*)oldData, (char*)pageData, PAGE_SIZE);
    
    offset = getOffsetAndSizeFromRID(fileHandle, rid, formattedDataSize, finalRid, update_flag, initOffset, pageData);
    //case 1 : record is already deleted
    if(offset == DELETED) {
        free(newData);
        free(pageData);
        free(oldData);
        return -1;
    }
    // get the latest table schmea
    RT latestVersion = (RT)(getLatestTableVersion(fileHandle.fileName));
    // if the new record leght is less than equal to previous one, we can use the same page
    if(newDataSize <= formattedDataSize) {
        RT moveOffset = formattedDataSize - newDataSize;
        RT startOffset = offset + formattedDataSize;
        memcpy((char*)pageData + offset, (char*)newData, newDataSize);
        moveRecordsByOffset(startOffset, moveOffset, LEFT, finalRid.slotNum, offset, newDataSize, 0, pageData);
        updateVersionOfRecord(pageData, finalRid, latestVersion);
        fileHandle.writePage(finalRid.pageNum, pageData);
    } else {
        // if record is bigger, check if current page has enough space, if not move to new page and place a tombstone
        int recEndOffset = fileHandle.hasEnoughSpace(pageData, newDataSize - formattedDataSize, UPDATED);
        if(recEndOffset != -1) {
            int moveOffset = newDataSize - formattedDataSize;
            int startOffset = offset + formattedDataSize;
            moveRecordsByOffset(startOffset, moveOffset, RIGHT, finalRid.slotNum, offset, newDataSize, 0, pageData);
            memcpy((char*)pageData + offset, (char*)newData, newDataSize);
            updateVersionOfRecord(pageData, finalRid, latestVersion);
            fileHandle.writePage(finalRid.pageNum, pageData);
        } else {
            if(update_flag != UPDATED) {
                //case 3.b page cant hold the new record, find a page with enough space.
                // Also, delete the out dated record
                RID newRid;
                insertUpdatedRecord(fileHandle, recordDescriptor, newData, newDataSize, newRid, latestVersion);
                int pageNum = newRid.pageNum;
                RT slotNum = newRid.slotNum;
                memcpy((char*)pageData + offset, (char*)&pageNum, sizeof(int));
                memcpy((char*)pageData + offset + sizeof(int), (char*)&slotNum, sizeof(RT));
                //UPdate slot of old record
                updateSlotInPageDirectory(offset, sizeof(int) + sizeof(RT), UPDATED, rid.slotNum, pageData);
                RT moveOffset = formattedDataSize - sizeof(int) - sizeof(RT);
                RT startOffset = offset + formattedDataSize;
                moveRecordsByOffset(startOffset, moveOffset, LEFT, rid.slotNum, offset, sizeof(int) + sizeof(RT), UPDATED, pageData);
                fileHandle.writePage(rid.pageNum, pageData);
            } else {
                RID newRid;
                //insert the record in a new page and get the new RID, update tombstone with the new RID.
                insertUpdatedRecord(fileHandle, recordDescriptor, newData, newDataSize, newRid, latestVersion);
                int pageNum = newRid.pageNum;
                RT slotNum = newRid.slotNum;
                memcpy((char*)oldData + initOffset, (char*)&pageNum, sizeof(int));
                memcpy((char*)oldData + initOffset + sizeof(int), (char*)&slotNum, sizeof(RT));
                updateSlotInPageDirectory(initOffset, sizeof(int) + sizeof(RT), UPDATED, rid.slotNum, oldData);
                fileHandle.writePage(rid.pageNum, oldData);

                //delete from page where the record was previously stored.
                RT moveOffset = formattedDataSize;
                RT startOffset = offset + formattedDataSize;
                moveRecordsByOffset(startOffset, moveOffset, LEFT, finalRid.slotNum, DELETED, formattedDataSize, 0, pageData);
                incrementFreeSlotsInPage(pageData);
                fileHandle.writePage(finalRid.pageNum, pageData);
            }
        }
    }

    free(newData);   
    free(pageData);
    free(oldData);
    return 0;
}

/**
 * readAttribute() - Reads a specific attribute from the record.
 * @argument1 : filehandle of the file.
 * @argument2 : record descriptor.
 * @argument3 : RID of the record.
 * @argument4 : attribute name to be projected
 * @argument5 : buffer containin the record (out parameter).
 *
 * Handles the case when a record may have a version (schema) different from the latest version. Used in add/drop attribute (rm.cc).
 *
 * Return : 0 on success, -1 on failure.
*/
RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                         const RID &rid, const std::string &attributeName, void *data) {

    if(recordDescriptor.size() == 0) return -1;

    RT formattedDataSize = 0, update_flag = 0, offset = 0, initOffset = 0;
    RID final_rid = rid;
    void* pageData = malloc(PAGE_SIZE);

    // File read fail.
    if(fileHandle.readPage(rid.pageNum, pageData) == -1) {
        free(pageData);
        return -1;
    }

    offset = getOffsetAndSizeFromRID(fileHandle, rid, formattedDataSize, final_rid, update_flag, initOffset, pageData);
    //record has already been deleted
    if(offset == DELETED) {
        free(pageData);
        return -1;
    }

    RT version = getVersionOfRecordWithPage(pageData, final_rid);
    RT latestVersion = (RT)getLatestTableVersion(fileHandle.fileName);
    // for the case when record is of an outdated schema, need to conform to latest schema.
    if(version != latestVersion) {
        std::vector<Attribute> recordDesc = getAttributesForVersion(fileHandle.fileName, version);
        RT nullBytes = 1;
        char nullInfo[nullBytes];
        memset((char*)nullInfo, 0, nullBytes);
        RT attributeOffset = 0;
        int attributeType = INVAL_TYPE;
        RT i = 0;
        for(i =  0; i < (RT)recordDesc.size(); i++) {
            if(recordDesc[i].name == attributeName && recordDesc[i].valid == VALID) {
                memcpy((char*)&attributeOffset, (char*)pageData + offset + i*sizeof(RT), sizeof(RT));
                attributeType = recordDesc[i].type;
                break;
            }
        }

        if (attributeType == INVAL_TYPE || recordDescriptor[i].valid == INVALID || attributeOffset == NULL_POINT) {
            nullInfo[0] = nullInfo[0] | (1 << (CHAR_BIT-1));
            memcpy((char*)data, (char*)nullInfo, nullBytes);
            free(pageData);
            return 0;
        }

        if(attributeType == TypeReal) {
            memcpy((char*)data + nullBytes, (char*)pageData + offset + attributeOffset - sizeof(float), sizeof(float));
        } else if(attributeType == TypeInt) {
            memcpy((char*)data + nullBytes, (char*)pageData + offset + attributeOffset - sizeof(int), sizeof(int));
        } else {
            int length = 0;
            if(i == 0)
                memcpy((char*)&length, (char*)pageData + offset + recordDesc.size()*sizeof(RT), sizeof(int));
            else {
                RT prevLen = -1;
                for(RT j = i - 1; j >= 0; j--) {
                    memcpy((char*)&prevLen, (char*)pageData + offset + j*sizeof(RT), sizeof(RT));
                    if(prevLen != NULL_POINT) {
                        break;
                    }
                }
                if(prevLen == NULL_POINT) prevLen = recordDesc.size()*sizeof(RT);
                memcpy((char*)&length, (char*)pageData + offset + prevLen, sizeof(int));
            }
            memcpy((char*)data + nullBytes, (char*)pageData + offset + attributeOffset - length - sizeof(int), length + sizeof(int));
        }

        memcpy((char*)data, (char*)nullInfo, nullBytes);

        free(pageData);
        return 0;
    }

    RT nullBytes = 1;
    char nullInfo[nullBytes];
    memset((char*)nullInfo, 0, nullBytes);
    RT attributeOffset = 0;
    int attributeType = INVAL_TYPE;
    RT i = 0;
    for(i =  0; i < (RT)recordDescriptor.size(); i++) {
        if(recordDescriptor[i].name == attributeName) {
            memcpy((char*)&attributeOffset, (char*)pageData + offset + i*sizeof(RT), sizeof(RT));
            attributeType = recordDescriptor[i].type;
            break;
        }
    }

    //No attribute with the given name.
    if(attributeType == INVAL_TYPE) {
        free(pageData);
        return -1;
    }

    if(attributeOffset == NULL_POINT) {
        nullInfo[0] = nullInfo[0] | (1 << (CHAR_BIT-1));
        memcpy((char*)data, (char*)nullInfo, nullBytes);
        free(pageData);
        return 0;
    }

    if(attributeType == TypeReal) {
        memcpy((char*)data + nullBytes, (char*)pageData + offset + attributeOffset - sizeof(float), sizeof(float));
    } else if(attributeType == TypeInt) {
        memcpy((char*)data + nullBytes, (char*)pageData + offset + attributeOffset - sizeof(int), sizeof(int));
    } else {
        int length = 0;
        if(i == 0)
            memcpy((char*)&length, (char*)pageData + offset + recordDescriptor.size()*sizeof(RT), sizeof(RT));
        else {
            RT prevLen = -1;
            for(int j = i - 1; j >= 0; j--) {
                memcpy((char*)&prevLen, (char*)pageData + offset + j*sizeof(RT), sizeof(RT));
                if(prevLen != NULL_POINT) {
                    break;
                }
            }
            if(prevLen == NULL_POINT) prevLen = recordDescriptor.size()*sizeof(RT);
            memcpy((char*)&length, (char*)pageData + offset + prevLen, sizeof(int));
        }
        memcpy((char*)data + nullBytes, (char*)pageData + offset + attributeOffset - length - sizeof(int), length + sizeof(int));
    }

    memcpy((char*)data, (char*)nullInfo, nullBytes);

    free(pageData);
    return 0;
}

/**
 * readAttributeOptimized() - Reads a specific attribute from the record.
 * @argument1 : filehandle of the file.
 * @argument2 : record descriptor.
 * @argument3 : RID of the record.
 * @argument4 : attribute name to be projected
 * @argument5 : buffer containin the record (out parameter).
 * @argument6 : page which contains the data( record).
 *
 * Handles the case when a record may have a version (schema) different from the latest version. Used in add/drop attribute (rm.cc).
 *
 * Return : 0 on success, -1 on failure.
*/
RC RecordBasedFileManager::readAttributeOptimized(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                                  const RID &rid, const std::string &attributeName, void *data, void* pageData) {

    if(recordDescriptor.size() == 0) return -1;

    RT formattedDataSize = 0, update_flag = 0, offset = 0, initOffset = 0, attributeOffset;
    RID final_rid = rid;

    offset = getOffsetAndSizeFromRID(fileHandle, rid, formattedDataSize, final_rid, update_flag, initOffset, pageData);
    RT version = getVersionOfRecordWithPage(pageData, final_rid);
    RT latestVersion = (RT)getLatestTableVersion(fileHandle.fileName);

    if(version != latestVersion) {
        RT attributeOffset = 0;
        int attributeType = INVAL_TYPE;
        std::vector<Attribute> recordDesc = getAttributesForVersion(fileHandle.fileName, version);
        RT i = 0;
        for(i =  0; i < (RT)recordDesc.size(); i++) {
            if(recordDesc[i].name == attributeName && recordDesc[i].valid == VALID) {
                memcpy((char*)&attributeOffset, (char*)pageData + offset + i*sizeof(RT), sizeof(RT));
                attributeType = recordDesc[i].type;
                break;
            }
        }


        if(attributeType == INVAL_TYPE) {
            return NULL_POINT;
        }

        if(recordDescriptor[i].valid == INVALID) { return NULL_POINT; }

        if(attributeOffset == NULL_POINT) {
            return NULL_POINT;
        }

        if (attributeType == TypeReal) {
            memcpy((char*)data, (char*)pageData + offset + attributeOffset - sizeof(float), sizeof(float));
        } else if (attributeType == TypeInt) {
            memcpy((char*)data, (char*)pageData + offset + attributeOffset - sizeof(int), sizeof(int));
        } else {
            int length = 0;
            if(i == 0) {
                memcpy((char*)&length, (char*)pageData + offset + recordDesc.size()*sizeof(RT), sizeof(int));
            } else {
                RT prevLen = 0;
                for(RT j = i - 1; j >= 0; j--) {
                    memcpy((char*)&prevLen, (char*)pageData + offset + j*sizeof(RT), sizeof(RT));
                    if(prevLen != NULL_POINT) {
                        break;
                    }
                }
                if(prevLen == NULL_POINT) prevLen = recordDesc.size()*sizeof(RT);
                memcpy((char*)&length, (char*)pageData + offset + prevLen, sizeof(int));
            }
            memcpy((char*)data, (char*)pageData + offset + attributeOffset - length - sizeof(int), length + sizeof(int));
        }
        return 0;
    }


    RT attributeType = INVAL_TYPE;
    RT i = 0;
    for(i =  0; i < (RT)recordDescriptor.size(); i++) {
        if(recordDescriptor[i].name == attributeName) {
            memcpy((char*)&attributeOffset, (char*)pageData + offset + i*sizeof(RT), sizeof(RT));
            attributeType = recordDescriptor[i].type;
            break;
        }
    }

    if(attributeType == INVAL_TYPE) {
        return -1;
    }

    if(attributeOffset == NULL_POINT) {
        return NULL_POINT;
    }

    if (attributeType == TypeReal) {
        memcpy((char*)data, (char*)pageData + offset + attributeOffset - sizeof(float), sizeof(float));
    } else if (attributeType == TypeInt) {
        memcpy((char*)data, (char*)pageData + offset + attributeOffset - sizeof(int), sizeof(int));
    } else {
        int length = 0;
        if(i == 0) {
            memcpy((char*)&length, (char*)pageData + offset + recordDescriptor.size()*sizeof(RT), sizeof(int));
        } else {
            RT prevLen = 0;           
            for(RT j = i - 1; j >= 0; j--) {
                memcpy((char*)&prevLen, (char*)pageData + offset + j*sizeof(RT), sizeof(RT));
                if(prevLen != NULL_POINT) {
                    break;
                }
            }
            if(prevLen == NULL_POINT) prevLen = recordDescriptor.size()*sizeof(RT);
            memcpy((char*)&length, (char*)pageData + offset + prevLen, sizeof(int));
        }
        memcpy((char*)data, (char*)pageData + offset + attributeOffset - length - sizeof(int), length + sizeof(int));
    }

    return 0;
}

/**
 * readAttributes() - Reads specific attributes from the record.
 * @argument1 : filehandle of the file.
 * @argument2 : record descriptor.
 * @argument3 : RID of the record.
 * @argument4 : attribute names to be projected
 * @argument5 : buffer containin the record (out parameter).
 * @argument6 : page which contains the data( record).
 *
 * Handles the case when a record may have a version (schema) different from the latest version. Used in add/drop attribute (rm.cc).
 *
 * Return : 0 on success, -1 on failure.
*/
RC RecordBasedFileManager::readAttributes(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid,
                                          const std::vector<std::string> &attributeNames, void *data, void* pageData) {

    if(recordDescriptor.size() == 0) return -1;

    RT formattedDataSize = 0, update_flag = 0, offset = 0, initOffset = 0;
    RID final_rid = rid;

    offset = getOffsetAndSizeFromRID(fileHandle, rid, formattedDataSize, final_rid, update_flag, initOffset, pageData);
    //record has already been deleted
    if(offset == DELETED) {
        return -1;
    }
    if(!isSystemFile(fileHandle.fileName)) {
        RT version = getVersionOfRecordWithPage(pageData, final_rid);
        RT latestVersion = (RT)getLatestTableVersion(fileHandle.fileName);
        if(version != latestVersion) {
            std::vector<Attribute> recordDesc = getAttributesForVersion(fileHandle.fileName, version);
            RT sizeWoInval = std::count_if(recordDesc.begin(), recordDesc.end(), [](const Attribute& attr){return attr.valid != INVALID;});
            RT nullBytes = ceil((double)attributeNames.size()/CHAR_BIT);
            char nullBitField[nullBytes];
            memset(nullBitField, 0, nullBytes);
            RT dataOffset = nullBytes;
            std::unordered_set<std::string> s;
            for(RT i = 0 ; i < (RT)attributeNames.size(); i++) {
                s.insert(attributeNames[i]);
            }

            RT i = 0;
            RT j = 0 ;
            while ( i < (RT)recordDesc.size() && s.size() > 0 ) {
                if ( s.find(attributeNames[j]) != s.end() && recordDesc[i].name == attributeNames[j] && recordDescriptor[i].valid == VALID) {
                    RT attributeOffset = 0;
                    memcpy((char*)&attributeOffset, (char*)pageData + offset + i*sizeof(RT), sizeof(RT));
                    if(attributeOffset == NULL_POINT) {
                        nullBitField[j/CHAR_BIT] |= (1 << (CHAR_BIT - 1 - j%CHAR_BIT));
                    } else if (recordDesc[i].type == TypeReal) {
                        memcpy((char*)data + dataOffset, (char*)pageData + offset + attributeOffset - sizeof(float), sizeof(float));
                        dataOffset += sizeof(float);
                    } else if(recordDesc[i].type == TypeInt) {
                        memcpy((char*)data + dataOffset, (char*)pageData + offset + attributeOffset - sizeof(int), sizeof(int));
                        dataOffset += sizeof(int);
                    } else if(recordDesc[i].type == TypeVarChar) {
                        int length = 0;
                        if( i == 0 )
                            memcpy((char*)&length, (char*)pageData + offset + sizeWoInval*sizeof(RT), sizeof(int));
                        else {
                            RT prevLen = 0;
                            for(RT j = i-1; j >= 0; j--) {
                                memcpy((char*)&prevLen, (char*)pageData + offset + (j)*sizeof(RT), sizeof(RT));
                                if(prevLen != NULL_POINT) {
                                    break;
                                }
                            }
                            if(prevLen == NULL_POINT) prevLen = sizeWoInval*sizeof(RT);
                            memcpy((char*)&length, (char*)pageData + offset + prevLen, sizeof(int));
                        }
                        memcpy((char*)data + dataOffset, (char*)pageData + offset + attributeOffset - length - sizeof(int), length + sizeof(int));
                        dataOffset += length + sizeof(int);
                    }
                    s.erase(attributeNames[j]);
                    j++;
                    i = 0;
                } else {
                    i++;
                }
             }

             while( j < (RT)attributeNames.size() ) {
                nullBitField[j/CHAR_BIT] |= (1 << (CHAR_BIT - 1 - j%CHAR_BIT));
             }
            memcpy((char*)data, (char*)nullBitField, nullBytes);
            return 0;
        }
    }

    RT attributeOffset = 0;
    int attributeType = INVAL_TYPE;
    RT nullBytes = ceil((double)attributeNames.size()/CHAR_BIT);
    char nullBitField[nullBytes];
    memset(nullBitField, 0, nullBytes);
    RT dataOffset = nullBytes;
    std::vector<Attribute> recordDescriptorActual = recordDescriptor;
    recordDescriptorActual.erase(std::remove_if(recordDescriptorActual.begin(), recordDescriptorActual.end(),
                       [](Attribute& a) { return a.valid ==  INVALID; }), recordDescriptorActual.end());

    std::unordered_set<std::string> s;
    for(RT i = 0 ; i < (RT)attributeNames.size(); i++) {
        s.insert(attributeNames[i]);
    }
    RT i = 0;
    RT j = 0;
    while( i < (RT)recordDescriptorActual.size() && s.size() > 0 ) {
        if(s.find(attributeNames[j]) != s.end() &&  recordDescriptorActual[i].name == attributeNames[j]) {
            memcpy((char*)&attributeOffset, (char*)pageData + offset + i*sizeof(RT), sizeof(RT));
            attributeType = recordDescriptor[i].type;
            if(attributeOffset == NULL_POINT) {
                 int shift = CHAR_BIT - 1 - j%CHAR_BIT;
                 nullBitField[j/CHAR_BIT] = nullBitField[j/CHAR_BIT] | (1 << (shift));
            } else if(attributeType == TypeReal) {
                memcpy((char*)data + dataOffset, (char*)pageData + offset + attributeOffset - sizeof(float), sizeof(float));
                dataOffset += sizeof(float);
            } else if(attributeType == TypeInt) {
                memcpy((char*)data + dataOffset, (char*)pageData + offset + attributeOffset - sizeof(int), sizeof(int));
                dataOffset += sizeof(int);
            } else {
                int length = 0;
                if( i == 0 )
                    memcpy((char*)&length, (char*)pageData + offset + recordDescriptorActual.size()*sizeof(RT), sizeof(int));
                else {
                    RT prevLen = 0;
                    for(RT j = i-1; j>=0; j--) {
                        memcpy((char*)&prevLen, (char*)pageData + offset + (j)*sizeof(RT), sizeof(RT));
                        if(prevLen != NULL_POINT) {
                            break;
                        }
                    }
                    if(prevLen == NULL_POINT) prevLen = recordDescriptorActual.size()*sizeof(RT);
                    memcpy((char*)&length, (char*)pageData + offset + prevLen, sizeof(int));
                }
                memcpy((char*)data + dataOffset, (char*)pageData + offset + attributeOffset - length - sizeof(int), length + sizeof(int));
                dataOffset += length + sizeof(int);
            }
            s.erase(attributeNames[j]);
            j++;
            i = 0;
        } else {
            i++;
         }
    }

    memcpy((char*)data, (char*)nullBitField, nullBytes);
    return 0;
}

/**
 * scan() - scan initializer for RBFM based file.
 * @argument1 : filehandle of the file.
 * @argument2 : record descriptor.
 * @argument3 : condition Attribute (for comparison).
 * @argument4 : comparison operation to be formed.
 * @argument5 : value to be compared with.
 * @argument6 : name of the attribute required in the return (output) data.
 * @argument7 : rbfm scanIterator object passed by reference.

 * Return : 0 on success, -1 on failure.
*/
RC RecordBasedFileManager::scan(FileHandle &fileHandle,
                                const std::vector<Attribute> &recordDescriptor,
                                const std::string &conditionAttribute,
                                const CompOp compOp, const void *value,
                                const std::vector<std::string> &attributeNames,
                                RBFM_ScanIterator &rbfm_ScanIterator) {

    if(rbfm_ScanIterator.initializeScanIterator(fileHandle, recordDescriptor,
                                                conditionAttribute, compOp,
                                                value, attributeNames) == 0) return 0;

    return -1;
}

/**
 * insertUpdatedRecord() - inserts an updated record to new page.
 * @argument1 : filehandle of the file.
 * @argument2 : record descriptor.
 * @argument3 : data to be inserted.
 * @argument4 : data size 
 * @argument5 : new RID for the data to be inserted.
 * @argument6 : version of the record being inserted (in case of multiple schema table).

 * Return : 0 on success, -1 on failure.
*/
RC RecordBasedFileManager::insertUpdatedRecord(FileHandle& fileHandle,
                                               const std::vector<Attribute>& recordDescriptor,
                                               const void* newData, const RT recordSize,
                                               RID& newRid, const int latestVersion) {

    void* pageData = malloc(PAGE_SIZE);
    int freePage = fileHandle.findFreePage(recordSize);
    if(freePage != -1) {
        fileHandle.readPage(freePage, pageData);
        RT offset = fileHandle.hasEnoughSpace(pageData, recordSize);
        storeDataInFile(fileHandle, freePage, offset, newData, recordSize, newRid, pageData);
        updateVersionOfRecord(pageData, newRid, latestVersion);
        free(pageData);
        return 0;
    }

    int newPage = fileHandle.getNumberOfPages();
    fileHandle.initPageDirectory(pageData);
    storeDataInFile(fileHandle, newPage, 0, newData, recordSize, newRid, pageData);
    updateVersionOfRecord(pageData, newRid, latestVersion);
    free(pageData);

    return 0;
}

/************************* RBFM Version control APIs *******************************************

/**
 * insertVersionOfRecord() - inserts the version of record with given RID.
 * @argument1 : file handle to the file
 * @argument2 : RID of the record
 * @argument3 : new version to be inserted
 *
 * Return : 0
*/
RC RecordBasedFileManager::insertVersionOfRecord(FileHandle& fileHandle, const RID& rid, const RT version) {
    void* pageData = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, pageData);

    memcpy((char*)pageData + PAGE_SIZE - rid.slotNum*SLOT_SIZE* sizeof(RT) - 3*sizeof(RT), (char*)&version, sizeof(RT));

    fileHandle.writePage(rid.pageNum, pageData);
    free(pageData);
    return 0;
}

/**
 * updateVersionOfRecord() - updates the version of record with given RID.
 * @argument1 : page data where the record is stored.
 * @argument2 : RID of the record
 * @argument3 : new version to be updated.
 *
 * Return : 0
*/
RC RecordBasedFileManager::updateVersionOfRecord(const void* pageData, const RID& rid, const RT version) {

    memcpy((char*)pageData + PAGE_SIZE - rid.slotNum*SLOT_SIZE* sizeof(RT) - 3*sizeof(RT), (char*)&version, sizeof(RT));
    return 0;
}

/**
 * getVersionOfRecord() - gets the version of record with given RID.
 * @argument1 : file handle to the file
 * @argument2 : RID of the record
 *
 * Return : version of the record.
*/
RT RecordBasedFileManager::getVersionOfRecord(FileHandle& fileHandle, const RID& rid) {
    void* pageData = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, pageData);
    RT formattedDataSize = 0, update_flag = 0, initOffset = 0, version = 0;
    RID final_rid = rid;
    getOffsetAndSizeFromRID(fileHandle, rid, formattedDataSize, final_rid, update_flag, initOffset, pageData);

    memcpy((char*)&version, (char*)pageData + PAGE_SIZE - final_rid.slotNum*SLOT_SIZE* sizeof(RT) - 3*sizeof(RT),sizeof(RT));

    free(pageData);
    return version;
}

/**
 * getVersionOfRecordWithPage() - gets the version of record with given RID.
 * @argument1 : page data where the record is stored.
 * @argument2 : RID of the record
 *
 * Return : version of the record.
*/
RT RecordBasedFileManager::getVersionOfRecordWithPage(const void* pageData, const RID& rid) {
    RT version = 1;
    memcpy((char*)&version, (char*)pageData + PAGE_SIZE - rid.slotNum*SLOT_SIZE*sizeof(RT) - 3*sizeof(RT),sizeof(RT));
    return version;
}

/**
 * getLatestTableVersion() - get latest table version(or schema)
 * @argument1 : name of the table
 *
 * Return : 1 if table schema has not been changed, else the latest version number.
*/
RC RecordBasedFileManager::getLatestTableVersion(const std::string& tableName) {
    if(tableMap.size() == 0) return 1;
    return this->tableMap[tableName].version;
}

/**
 * getAttributesForVersion() - get the attribute for a given table name and version (or schema).
 * @argument1 : name of the table.
 * @argument2 : version (or schema) number of the table.
 *
 * Used to handle add/drop attributes (see rm.cc).
 *
 * Return : vector of attribute.
*/
vector<Attribute> RecordBasedFileManager::getAttributesForVersion(const std::string& tableName, const int version) {
    vector<Attribute> attr;
    if(columnsMap.find(tableName) == columnsMap.end()) return attr;
    return columnsMap[tableName][version].recordDescriptor;
}

/**
 * isSystemFile() - check if is a system file.
 * @argument1 : filename to be looked
 *
 * Return : true if system file, else false.
*/
bool RecordBasedFileManager::isSystemFile(const std::string& fileName) {
    return (fileName == "Tables" || fileName == "Columns");
}

/**************************** RBFM Utility APIs *****************************************/

/**
 * getDataSizeWithoutNullBytes() - get the size of the data without the null bytes.
 * @argument1 : record descriptor
 * @argument2 : data
 * 
 * Enforces the minimum record size ( = MIN_RECORD_SIZE).
 *
 * Return : 0.
*/
RT getDataSizeWithoutNullBytes(const std::vector<Attribute>& recordDesc, const void* data) {
    RT nullBytes = ceil((double)recordDesc.size()/CHAR_BIT);
    char nullField[nullBytes];
    memset(nullField, 0 , nullBytes);
    memcpy(nullField, (char*)data, nullBytes);
    RT recordSize = nullBytes;
    for(RT i = 0 ; i < (RT)recordDesc.size(); i++) {
        int shift = CHAR_BIT - 1 - i%CHAR_BIT;
        int nullFieldBit = (nullField[i/CHAR_BIT] & (1 << (shift)));

        if(!nullFieldBit) {
            if (recordDesc[i].type == TypeReal) recordSize += sizeof(float);
            else if (recordDesc[i].type == TypeInt) recordSize += sizeof(int);
            else {
                int varcharlen = 0;
                memcpy(&varcharlen,(char*)data + recordSize, sizeof(int));
                recordSize += sizeof(int) + (RT)varcharlen;
            }
        }
    }

    //Return the size of attribute table + the size of data fields, ignoring the null bytes.
    RT size = recordSize + recordDesc.size()*sizeof(RT) - nullBytes;
    return MIN_RECORD_SIZE > size ? MIN_RECORD_SIZE : size;
}

/**
 * getDirAndRecPointers() - given a page, get the directory end and record end pointer.
 * @argument1 : get directory slot pointer by reference.
 * @argument2 : get record slot pointer by reference.
 * @argument3 : The buffer containing page data.
 *
 * Return : void
*/
void getDirAndRecPointers(RT& dirSlotPointer, RT& recordSlotPointer, void* pageData) {
    RT dirPoint, recPoint;
    memcpy((char*)&dirPoint, (char*)pageData + PAGE_SIZE - sizeof(RT), sizeof(RT));
    memcpy((char*)&recPoint, (char*)pageData + PAGE_SIZE - 2*sizeof(RT), sizeof(RT));

    dirSlotPointer = dirPoint;
    recordSlotPointer = recPoint;
    return;
}

/**
 * getOffsetAndSizeFromRID() - given a RID , get the size and offset of the record in the page.
 * @argument1 : filehandle of the file.
 * @argument2 : RID of the record
 * @argument3 : get the data size by reference.
 * @argument4 : get the new rid by reference (in case of an updated record moved to new page).
 * @argument5 : get the update flag by reference
 * @argument6 : get the offset by reference.
 * @argument7 : get the page data (if the record was moved to new page).
 *
 * Initially pagedata has the contents of the page where the rid points too, but if the rid is a tombstone then
 * the returned pagedata will have the contents of the page where the record actually sits.
 *
 * Return : 0.
*/
RT getOffsetAndSizeFromRID(FileHandle& fileHandle, const RID& rid, RT& formattedDataSize, RID& final_rid,
                           RT& update_flag, RT& initOffset, void* pageData)  {
    RT offset = 0;

    RT slotOffset = PAGE_SIZE - SLOT_SIZE*sizeof(RT) - (rid.slotNum-1)*(SLOT_SIZE*sizeof(RT));
    memcpy((char*)&offset, (char*)pageData + slotOffset, sizeof(RT));
    if(offset == DELETED) return DELETED;

    RT uFlag = 0;
    memcpy((char*)&uFlag, (char*)pageData + slotOffset - 2*sizeof(RT), sizeof(RT));

    if(uFlag == UPDATED) {
        int pageNum = 0;
        RT slotNum = 0;
        memcpy((char*)&pageNum, (char*)pageData + offset, sizeof(int));
        memcpy((char*)&slotNum, (char*)pageData + offset + sizeof(int), sizeof(RT));
        final_rid.pageNum = pageNum;
        final_rid.slotNum = slotNum;
        update_flag = uFlag;
        initOffset = offset;
        fileHandle.readPage(final_rid.pageNum, pageData);
        return getOffsetAndSizeFromRID(fileHandle, final_rid, formattedDataSize, final_rid, update_flag, initOffset, pageData);
    }

    RT dataSize = 0;
    memcpy((char*)&dataSize, (char*)pageData + slotOffset - sizeof(RT), sizeof(RT));
    formattedDataSize = dataSize;
    return offset;
}

/**
 * addEntryToPageDirectory() - adds a new entry to the page, can use an existing slot
 * @argument1 : pagenum where the entry is to be added. 
 * @argument2 : offset to be added in the slot.
 * @argument3 : current end of direcotry end pointer.
 * @argument4 : current end of record slot pointer.
 * @argument5 : size to be added to the slot.
 * @argument6 : page data where the entry is made.
 *
 * Return : 0.
*/
void addEntryToPageDirectory(PageNum pageNum, RT offset, RID& rid, RT dirSlotPointer,
                             RT recordSlotPointer, RT formattedDataSize, void* pageData) {
    bool existingSlot = false;
    rid.pageNum = pageNum;
    rid.slotNum = getFreeSlotInPage(dirSlotPointer, pageData, existingSlot);
    RT updatedFlag = 0;
    RT version = 1;
    memcpy((char*)pageData + PAGE_SIZE - rid.slotNum*SLOT_SIZE*sizeof(RT), (char*)&offset, sizeof(RT));
    memcpy((char*)pageData + PAGE_SIZE - rid.slotNum*SLOT_SIZE*sizeof(RT) - sizeof(RT), (char*)&formattedDataSize, sizeof(RT));
    memcpy((char*)pageData + PAGE_SIZE - rid.slotNum*SLOT_SIZE*sizeof(RT) - 2*sizeof(RT), (char*)&updatedFlag, sizeof(RT));
    memcpy((char*)pageData + PAGE_SIZE - rid.slotNum*SLOT_SIZE* sizeof(RT) - 3* sizeof(RT), (char*)&version, sizeof(RT));

    if(!existingSlot) {
        updateDirAndRecPointers(dirSlotPointer - SLOT_SIZE*sizeof(RT), recordSlotPointer + formattedDataSize, pageData);
        return;
    }

    updateDirAndRecPointers(dirSlotPointer, recordSlotPointer + formattedDataSize, pageData);
    decrementFreeSlotsInPage(pageData);
    return;
}

/**
 * getFreeSlotInPage() - get the number of free slot in the page
 * @argument1 : end of the directory slot 
 * @argument2 : page in which to search.
 * @argument3 : ref. variable to check wether an exisiting slot was returned or a new one
                is to be created.
 *
 * Return : 0.
*/
RT getFreeSlotInPage(const RT dirSlotPointer, const void* pageData, bool& existingSlot) {
    RT totalSlots = (PAGE_SIZE - 3*sizeof(RT) - dirSlotPointer)/(SLOT_SIZE*sizeof(RT));
    for(RT i = 1; i <= totalSlots; i++) {
        RT free = 0;
        memcpy((char*)&free, (char*)pageData + PAGE_SIZE - SLOT_SIZE*i*sizeof(RT), sizeof(RT));
        if(free == DELETED) {
            existingSlot = true;
            return i;
        }
    }
    return totalSlots + 1;
}

/**
 * updateDirAndRecPointers() - updates the directory and record slot pointer
 * @argument1 : new directory pointer.
 * @argument2 : new record slot pointer.
 * @argument3 : page data in which the slots are updated.
 *
 *
 * Return : 0.
*/
void updateDirAndRecPointers(RT dirSlotPointer, RT recordSlotPointer, void* pageData) {
    memcpy((char*)pageData + PAGE_SIZE - sizeof(RT), (char*)&dirSlotPointer, sizeof(RT));
    memcpy((char*)pageData + PAGE_SIZE - 2*sizeof(RT), (char*)&recordSlotPointer, sizeof(RT));

    return;
}

/**
 * updateSlotInPageDirectory() - updates the field of a gicen slot int directory.
 * @argument1 : offset to be updated in the slot
 * @argument2 : new record size.
 * @argument3 : update flag
 * @argument4 : slot number to be updated.
 * @argument5 : page data in which the slot is updated.
 *
 *
 * Return : 0.
*/
RC updateSlotInPageDirectory(RT offset, RT formattedDataSize, RT update_flag, RT slotNum, void* pageData) {
    memcpy((char*)pageData + PAGE_SIZE - slotNum*SLOT_SIZE*sizeof(RT), (char*)&offset, sizeof(RT));
    memcpy((char*)pageData + PAGE_SIZE - (slotNum*SLOT_SIZE + 1)*sizeof(RT), (char*)&formattedDataSize, sizeof(RT));
    memcpy((char*)pageData + PAGE_SIZE - (slotNum*SLOT_SIZE + 2)*sizeof(RT), (char*)&update_flag, sizeof(RT));
    return 0;
}

/**
 * updatePageDirectoryByOffset() - updates the offset of all slots in the directory by given offset.
 * @argument1 : start offset 
 * @argument2 : the number of bytes to be moved.
 * @argument3 : end offset of the page directory.
 * @argument4 : page in which the movement is taking place.
 *
 * Used when a record is deleted/updated.
 *
 * Return : 0.
*/
RC updatePageDirectoryByOffset(RT startOffset, RT moveOffset, RT dirSlotPointer, void* pageData) {
    RT numSlots = (PAGE_SIZE - dirSlotPointer - 3*sizeof(RT))/(SLOT_SIZE*sizeof(RT));

   for(RT i = 1 ; i <= numSlots; i++) {
        RT old_offset = 0;
        memcpy((char*)&old_offset, (char*)pageData + PAGE_SIZE - SLOT_SIZE*i*sizeof(RT), sizeof(RT));
        if(old_offset != DELETED) {
            if(old_offset >= startOffset) {
                old_offset = old_offset + moveOffset;
                memcpy((char*)pageData + PAGE_SIZE - SLOT_SIZE*i*sizeof(RT), (char*)&old_offset, sizeof(RT));
            }
        }
    }

    return 0;
}

/**
 * moveRecordsByOffset() - moves the given record in the specified direction by given offset.
 * @argument1 : start offset 
 * @argument2 : the number of bytes to be moved.
 * @argument3 : direction (LEFT/RIGHT) to be moved.
 * @argument4 : slot number from which to move.
 * @argument5 : offset of the record start
 * @argument6 : size of the record.
 * @argument7 : flag to check wether the record was updated.
 * @argument8 : page in which the movement is taking place.
 *
 * Used when a record is deleted/updated.
 *
 * Return : 0
*/
RC moveRecordsByOffset(RT startOffset, RT moveOffset, RT direction,
                       RT slotNum, RT offset, RT recordSize,
                       RT update_flag, void* pageData) {

    if(moveOffset == 0) return 0;
    RT recSlotPointer, dirSlotPointer;
    getDirAndRecPointers(dirSlotPointer, recSlotPointer, pageData);
    RT totalDataBytes = recSlotPointer - startOffset;
    void* data = malloc(totalDataBytes);
    memcpy((char*)data, (char*)pageData + startOffset, totalDataBytes);
    memcpy((char*)pageData + startOffset + direction*moveOffset, (char*)data, totalDataBytes);
    if(direction == LEFT) {
        memset((char*)pageData + recSlotPointer - moveOffset, 0, moveOffset);
    }
 
    updateDirAndRecPointers(dirSlotPointer, recSlotPointer + direction*moveOffset, pageData);
    updateSlotInPageDirectory(offset, recordSize, update_flag, slotNum, pageData);
    updatePageDirectoryByOffset(startOffset, moveOffset*direction, dirSlotPointer, pageData);

    free(data); 
    return 0; 
}

/**
 * formatDataForReading() - formats the data for inserting into file (adds field offset in front).
 * @argument1 : record descriptor.
 * @argument2 : buffer containing the data to be formatted.
 * @argument3 : passed by ref. the size of the returned formatted data.
 *
 *
 * Used to input data ( or insert operation).
 *
 * Return : buffer which will contain the record after formatting.
*/
void* formatDataForStoring(const std::vector<Attribute>& recordDesc, const void* data, RT& formattedDataSize) {
    //stores the end of each field in the offset table
    RT size = getDataSizeWithoutNullBytes(recordDesc, data);
    formattedDataSize = size;
    void* formattedData = malloc(formattedDataSize);
    memset(formattedData, 0, formattedDataSize);
    RT nullBytes = ceil((double)recordDesc.size()/CHAR_BIT);
    char nullfield[nullBytes];
    memset(nullfield, 0, nullBytes);
    memcpy(nullfield, (char*)data, nullBytes);

    RT totaloffset = recordDesc.size()*sizeof(RT);
    RT recordSize = nullBytes;

    for(int i = 0 ; i < (int)recordDesc.size(); i++) {
        int shift = CHAR_BIT - 1 - i%CHAR_BIT;
        int nullFieldBit = nullfield[i/CHAR_BIT] & (1 << (shift));
        if(nullFieldBit) {
            RT null_pointer = NULL_POINT;
            memcpy((char*)formattedData + i*sizeof(RT), &null_pointer, sizeof(RT));
        } else {
            if(recordDesc[i].type == TypeReal) {
                memcpy((char*)formattedData + totaloffset , (char*)data + recordSize, sizeof(float));
                totaloffset += sizeof(float);
                recordSize += sizeof(float);
                memcpy((char*)formattedData + i*sizeof(RT), &totaloffset, sizeof(RT));
            } else if (recordDesc[i].type == TypeInt) {
                memcpy((char*)formattedData +  totaloffset , (char*)data + recordSize, sizeof(int));
                totaloffset += sizeof(int);
                recordSize += sizeof(int);
                memcpy((char*)formattedData + i*sizeof(RT), &totaloffset, sizeof(RT));
            } else {
                int varcharlen = 0;
                memcpy(&varcharlen,(char*)data + recordSize, sizeof(int));
                memcpy((char*)formattedData + totaloffset , (char*)data + recordSize, varcharlen + sizeof(int));
                totaloffset += sizeof(int) + varcharlen;
                recordSize += sizeof(int) + varcharlen;
                memcpy((char*)formattedData + i*sizeof(RT), &totaloffset, sizeof(RT));
            }
        }
    }

    return formattedData;
}

/**
 * formatDataForReading() - formats the data for reading from the file.
 * @argument1 : offset at which the data is stored.
 * @argument2 : formatted data size.
 * @argument3 : record descriptor.
 * @argument4 : buffer containing the page data in which the record is stored.
 * @argument5 : buffer which will contain the record after formatting for reading.
 *
 *
 * Used to read operation, field offset table is removed and null bytes are added in front.
 *
 * Return : @argument 5
*/
void formatDataForReading(RT offset, RT formattedDataSize,
                          const std::vector<Attribute>& recordDescriptor,
                          void* pageData, void* data) {

    RT nullBytes = ceil((double)recordDescriptor.size()/CHAR_BIT);
    void* attrOffsets = malloc(recordDescriptor.size()*sizeof(RT));
    //special case when record size was less than the minimum required record size
    if(formattedDataSize == MIN_RECORD_SIZE) {
        RT actualSize = recordDescriptor.size()*sizeof(RT);
        RT prevOffset = actualSize;
        for(int i = 0 ; i < (int)recordDescriptor.size(); i++) {
            RT lenOfAttribute = 0;
            memcpy((char*)&lenOfAttribute, (char*)pageData + offset + i*sizeof(RT), sizeof(RT));
            if(lenOfAttribute != NULL_POINT) {
              actualSize += lenOfAttribute - prevOffset;
              prevOffset = lenOfAttribute;
            }
        }
        formattedDataSize = actualSize;
    }


    memcpy((char*)attrOffsets, (char*)pageData + offset, recordDescriptor.size()*sizeof(RT));
    void* buffer = malloc(formattedDataSize - recordDescriptor.size()*sizeof(RT));
    memcpy((char*)buffer, (char*)pageData + offset + recordDescriptor.size()*sizeof(RT), formattedDataSize - recordDescriptor.size()*sizeof(RT));

    void* nullBitField = generateNullBitField(recordDescriptor, attrOffsets);
    memcpy((char*)data, (char*)nullBitField, nullBytes);
    memcpy((char*)data + nullBytes, (char*)buffer, formattedDataSize - recordDescriptor.size()*sizeof(RT));
    free(nullBitField);
    free(buffer);
    free(attrOffsets);
    return;
}

/**
 * generateNullBitField() - given a record descriptor and formatted data generate the null bytes.
 * @argument1 : record descriptor.
 * @argument2 : data in the format used to store in the file.
 *
 * Used to output data ( or read).
 *
 * Return : buffer containing the null bytes data filled.
*/
void* generateNullBitField (const std::vector<Attribute>& recordDesc, const void* data) {
    int nullBytes = ceil((double)recordDesc.size()/CHAR_BIT);
    char nullBitField[nullBytes];
    memset(nullBitField, 0, nullBytes);
    int n = recordDesc.size();

    for (int i = 0 ; i < n ; i++) {
        RT offset_val;
        memcpy(&offset_val, (char*)data + i*sizeof(RT), sizeof(RT));
        if(offset_val == NULL_POINT) {
            int shift = CHAR_BIT - 1 - i%CHAR_BIT;
            nullBitField[i/CHAR_BIT] = nullBitField[i/CHAR_BIT] | (1 << (shift));
        }
    }

    void* nulls = malloc(nullBytes);
    memcpy((char*)nulls, nullBitField, nullBytes);
    return nulls;
}

/**
 * storeDataInFile() - stores the given page in the file and updates page parameters
 * @argument1 : FileHandle of the file
 * @argument2 : page number in the file.
 * @argument3 : Offset at which to store the record
 * @argument4 : data to be stored at the offset in the given page.
 * @argument5 : size of the data to be stored.
 * @argument6 : rid to be alloted to the new data being stored.
 * @arguement7 : buffer containing the page data befor data is inserted.
 *
 * Return : true if present, false if the record is deleted or updated (move to other page).
*/
void storeDataInFile(FileHandle& fileHandle, 
                     PageNum freePage, RT offset,
                     const void* formattedData,
                     const RT formattedDataSize,
                     RID& rid, void* pageData) {

    memcpy((char*)pageData + offset, (char*)formattedData, formattedDataSize);

    RT dirSlotPointer, recordSlotPointer;
    getDirAndRecPointers(dirSlotPointer, recordSlotPointer, pageData);
    addEntryToPageDirectory(freePage, offset, rid, dirSlotPointer, recordSlotPointer, formattedDataSize, pageData);

    if(freePage == fileHandle.getNumberOfPages()) {
        fileHandle.appendPage(pageData);
        fileHandle.updateFreeSpaceForPage(freePage, pageData);
        return;
    }
    fileHandle.writePage(freePage, pageData);
    return;
}

/**
 * isValidRID() - checks if a given RID is valid.
 * @argument1 : RID wih page number and slot number.
 * @argument2 : buffer conatining data of the page specified in RID.
 *
 * Used in scan iterator.
 *
 * Return : true if present, false if the record is deleted or updated (move to other page).
*/
bool isValidRID(const RID& nextRID, const void* data) {
    RT offset = 0;
    RT updateFlag = 0;

    memcpy((char*)&offset, (char*)data + PAGE_SIZE - SLOT_SIZE*nextRID.slotNum*sizeof(RT), sizeof(RT));
    if(offset == DELETED) {
        return false;
    }

    memcpy((char*)&updateFlag, (char*)data + PAGE_SIZE - SLOT_SIZE*nextRID.slotNum*sizeof(RT) - 2*sizeof(RT), sizeof(RT));
    if(updateFlag == UPDATED) return false;

    return true;
}

/**
 * decrementFreeSlotsInPage() - decrement the number of  free slots in the page directory.
 * @argument1 : buffer containing data of the page.
 *
 * Used to re-use slot if previous record in a slot was deleted.
 *
 * Return : void
*/
void decrementFreeSlotsInPage(void* pageData) {
    RT freeSlots = 0;
    memcpy((char*)&freeSlots, (char*)pageData + PAGE_SIZE - 3*sizeof(RT), sizeof(RT));
    freeSlots--;
    if(freeSlots < 0) freeSlots = 0;
    memcpy((char*)pageData + PAGE_SIZE - 3*sizeof(RT), (char*)&freeSlots, sizeof(RT));
    return;
}

/**
 * incrementFreeSlotsInPage() - increment the number of  free slots in the page directory.
 * @argument1 : buffer containing data of the page.
 *
 * Used to re-use slot if previous record in a slot was deleted.
 *
 * Return : void
*/
void incrementFreeSlotsInPage(void* pageData) {
    RT freeSlots = 0;
    memcpy((char*)&freeSlots, (char*)pageData + PAGE_SIZE - 3*sizeof(RT), sizeof(RT));

    freeSlots++;
    memcpy((char*)pageData + PAGE_SIZE - 3*sizeof(RT), (char*)&freeSlots, sizeof(RT));
    return;
 }
 
 /*********************************** Type Comparators *****************************/
 
/**
 * compareTypeInt() - compare 2 data of type int based on given operation.
 * @argument1 : data 1
 * @argument2 : data 2
 * @argument3 : comparison operation.
 *
 * Return : comparsion result in boolean
*/
bool compareTypeInt(const void* data1, const void* data2, const CompOp compOp) {
    int d1 = 0;
    int d2 = 0;
    memcpy((char*)&d1, (char*)data1, sizeof(int));
    memcpy((char*)&d2, (char*)data2, sizeof(int));
    if(compOp == EQ_OP) return d1 == d2;
    if(compOp == LT_OP) return d1 < d2;
    if(compOp == LE_OP) return d1 <= d2;
    if(compOp == GT_OP) return d1 > d2;
    if(compOp == GE_OP) return d1 >= d2;
    if(compOp == NE_OP) return d1 != d2;
    return false;
}

/**
 * compareTypeInt() - compare 2 data of type float based on given operation.
 * @argument1 : data 1
 * @argument2 : data 2
 * @argument3 : comparison operation.
 *
 * Return : comparsion result in boolean
*/
bool compareTypeReal(const void* data1, const void* data2, CompOp compOp) {
    float d1 = 0;
    float d2 = 0;
    memcpy((char*)&d1, (char*)data1, sizeof(float));
    memcpy((char*)&d2, (char*)data2, sizeof(float));
    if(compOp == EQ_OP) return d1 == d2;
    if(compOp == LT_OP) return d1 < d2;
    if(compOp == LE_OP) return d1 <= d2;
    if(compOp == GT_OP) return d1 > d2;
    if(compOp == GE_OP) return d1 >= d2;
    if(compOp == NE_OP) return d1 != d2;
    return false;
}

/**
 * compareTypeInt() - compare 2 data of type varchar based on given operation.
 * @argument1 : data 1
 * @argument2 : data 2
 * @argument3 : comparison operation.
 *
 * Return : comparsion result in boolean
*/
bool compareTypeVarChar(const void* data1, const void* data2, CompOp compOp) {
    int len1 = 0;
    int len2 = 0;
    memcpy((char*)&len1, (char*)data1, sizeof(int));
    memcpy((char*)&len2, (char*)data2, sizeof(int));

    char str1[len1+1];
    char str2[len2+1];

    memcpy(str1, (char*)data1 + sizeof(int), len1);
    memcpy(str2, (char*)data2 + sizeof(int), len2);

    str1[len1] = '\0';
    str2[len2] = '\0';
    int retVal = strcmp(str1, str2);

    if(compOp == EQ_OP) return retVal == 0;
    if(compOp == LT_OP && retVal < 0) return retVal < 0;
    if(compOp == LE_OP && retVal <= 0 ) return retVal <= 0;
    if(compOp == GT_OP) return retVal > 0;
    if(compOp == GE_OP) return retVal >= 0;
    if(compOp == NE_OP) return (retVal < 0 || retVal > 0);
    return false;
}