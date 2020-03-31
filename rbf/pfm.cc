#include "pfm.h"

// Buffer manager singleton.
BufferManager &BufferManager::instance() {
    static BufferManager _buf_manager = BufferManager();
    return _buf_manager;
}

BufferManager::BufferManager() = default;

BufferManager::BufferManager(const BufferManager &) = default;

BufferManager &BufferManager::operator=(const BufferManager &) = default;

/**
 * pageInBufffer() - to lookup a page of a file in the cache.
 * @argument1 : name of the file whose page to lookup.
 * @argument2 : page number to be lookedup
 * @argument3 : data in which to return the lookedup up data if exist in cache.
 *
 * Return : 0 on success, -1 page not in cache.
*/
RC BufferManager::pageInBuffer(const std::string& fileName, const int pageNum, void *data) {
    if(buffer.find(fileName) != buffer.end()) {
        if(buffer[fileName].find(pageNum) != buffer[fileName].end()) {
            memcpy((char*)data, (char*)(buffer[fileName][pageNum]).pageData, PAGE_SIZE);
            return 0;
        }
    }
    return -1;
}

/**
 * storeInBuffer() - stores a page into cache.
 * @argument1 : name of the file whose page is being cached.
 * @argument2 : page number to be cached.
 * @argument3 : page data which to be written to cache.
 *
 * Return : 0 on success.
*/
RC BufferManager::storeInBuffer( const std::string& fileName, const int pageNum, const void *data) {
    if(buffer.find(fileName) != buffer.end() && buffer[fileName].find(pageNum) != buffer[fileName].end()) {
        memcpy((char*)(buffer[fileName][pageNum]).pageData, (char*)data, PAGE_SIZE);
        (buffer[fileName][pageNum]).dirtyBit = 1;

    } else if(buffer.find(fileName) != buffer.end()) {
        if(buffer[fileName].size() == MAX_CACHE_PAGE_PER_FILE) {
            writeBackPageToFile(fileName, buffer[fileName].begin()->first, buffer[fileName].begin()->second.pageData, 
                                (buffer.begin()->second).begin()->second.dirtyBit);
            buffer.erase(fileName);
        }
        (buffer[fileName][pageNum]).pageData = malloc(PAGE_SIZE);
        memcpy((char*)(buffer[fileName][pageNum]).pageData, (char*)data, PAGE_SIZE);
        (buffer[fileName][pageNum]).dirtyBit = 0;

    } else if (buffer.size() == MAX_FILES_FOR_CACHE) {
        writeBackPageToFile(buffer.begin()->first, (buffer.begin()->second).begin()->first, 
                            (buffer.begin()->second).begin()->second.pageData, 
                            (buffer.begin()->second).begin()->second.dirtyBit);

        buffer.erase(buffer.begin());
        (buffer[fileName][pageNum]).pageData = malloc(PAGE_SIZE);
        memcpy((char*)(buffer[fileName][pageNum]).pageData, (char*)data, PAGE_SIZE);
        (buffer[fileName][pageNum]).dirtyBit = 0;

    } else {
        (buffer[fileName][pageNum]).pageData = malloc(PAGE_SIZE);
        memcpy((char*)(buffer[fileName][pageNum]).pageData, (char*)data, PAGE_SIZE);
        (buffer[fileName][pageNum]).dirtyBit = 0;
    }
    return 0;
}

/**
 * writeBackPageToFile() - writes back a single page of a file from cache to disk.
 * @argument1 : name of the file whose cached page is to be written to disk.
 * @argument2 : page number to be written.
 * @argument3 : page data which is to be written to disk.
 * @argument4 : provison for dirtyBit currently not being used.
 *
 * Return : 0 on success.
*/
RC BufferManager::writeBackPageToFile(const std::string& fileName, const int pageNum,
                                      const void* data, int dirtyBit) {
    std::fstream file(fileName);
    file.seekp((pageNum + MAX_HIDDEN_PAGES)*PAGE_SIZE);
    file.write((char*)data, PAGE_SIZE);
    return 0;
}

/**
 * writeBackFullBufferToFile() - writes all the cached pages of a file to disk.
 * @argument1 : name of the file whose cached pages are to be written to disk
 *
 * Return : 0 on success.
*/
RC BufferManager::writeBackFullBufferToFile(const std::string &fileName) {
    
    if(buffer.find(fileName) != buffer.end()) {
        for(auto itr =  buffer[fileName].begin(); itr != buffer[fileName].end(); itr++) {
            writeBackPageToFile(fileName, (itr)->first, (itr)->second.pageData, (itr)->second.dirtyBit);
        }
        buffer.erase(buffer.find(fileName));
    }
    return 0;
}

//PFM Singleton
PagedFileManager &PagedFileManager::instance() {
    static PagedFileManager _pf_manager = PagedFileManager();
    return _pf_manager;
}

PagedFileManager::PagedFileManager() = default;

PagedFileManager::~PagedFileManager() = default;

PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;

/**
 * createFile() - create a file on the disk
 * @argument1 : name of the file to be created
 *
 * Return : 0 on success, -1 if file already exists or some other error.
*/
RC PagedFileManager::createFile(const std::string &fileName) {
// 1) Case 1 : If the file with the same name already exists
// 2) Case 2 : If the file doesnt exist create it

    std::ifstream file(fileName.c_str());
    if(file.good()) {
        file.close();
        return -1;
    }

    std::fstream newFile;
    newFile.open(fileName.c_str(), std::ios::out);
    if(!newFile.is_open()) {
        return -1;
    }

    newFile.close();
    return 0;
}

/**
 * destroyFile() - delete a file from the disk
 * @argument1 : name of the file to deleted.
 *
 * Return : 0 on success, -1 if file doesnt exist.
*/
RC PagedFileManager::destroyFile(const std::string &fileName) {
    if(remove(fileName.c_str()) != 0) {
        return -1;
    }

    return 0;
}

/**
 * openFile() - opens a given file.
 * @argument1 : name of the file to be opened.
 * @argument2 : filehandle to be ussed for modifying the opened file.
 *
 * Return : 0
*/
RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
    std::ifstream file(fileName.c_str());
    if(file.good()) {
        file.close();
        if(fileHandle.isOpen()) {
            return -1;
        }
        fileHandle.openFile(fileName);
        return 0;
    }

    return -1;
}

/**
 * closeFile() - closes a given file.
 * @argument1 : filehandle for the file to be closed.
 *
 * Return : 0
*/
RC PagedFileManager::closeFile(FileHandle &fileHandle) {
    fileHandle.closeRoutine();
    return 0;
}

FileHandle::FileHandle() : bm(BufferManager::instance()) {
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
    numPages = 0;
}

FileHandle::~FileHandle() {}

/**
 * openFile() - opens a given file
 * @argument1 : Name of the file to be opened.
 * 
 * Opens a file, if the file is opened for the first time creates the hidden page,
 * reads the performance counter for the file from hidden/header page.
 *
 * Return : none.
*/
void FileHandle::openFile(const std::string& fileName) {
    this->file.open(fileName.c_str(),std::ios::in | std::ios::out);
    if (this->isEmpty()) this->createHiddenPage(fileName);
    this->readCounterFromHiddenPage();
    this->setFileName(fileName);
}

/**
 * closeRoutine() - closes a given file.
 * @argument1 : name of the file to be closed.
 * 
 * Closes a file, updated the performance counter into hidden page,
 * writes back all the cached pages of the file into the disk.
 *
 * Return : none.
*/
void FileHandle::closeRoutine() {
    this->updateCounterInHiddenPage();
    this->bm.writeBackFullBufferToFile(fileName);
    this->file.close();
}

bool FileHandle::isOpen() {
    return this->file.is_open();
}

bool FileHandle::isEmpty() {
    return this->file.peek() == EOF;
}

/**
 * createHiddenPage() - creates the header pages in a file.
 * @argument1 : Name of the file in which the header page is create.
 * 
 * Return : 0 on success.
*/
RC FileHandle::createHiddenPage(const std::string& fileName) {

    std::fstream newFile;
    newFile.open(fileName.c_str(), std::ios::in | std::ios::out);
    newFile.seekp(0, std::ios_base::end);
    void* data = malloc(MAX_HIDDEN_PAGES*PAGE_SIZE);

    for (unsigned i = 0; i < PAGE_SIZE; i++) {
        *((char *) data + i) = i % 96 + 30;
    }

    int counter = 0;
    int pageNum = 0;

    memcpy((char*)data, (char*)&counter, sizeof(int));
    memcpy((char*)data + sizeof(int), (char*)&counter, sizeof(int));
    counter = 1;
    memcpy((char*)data + 2*sizeof(int), (char*)&counter, sizeof(int));
    memcpy((char*)data + 3*sizeof(int), (char*)&pageNum, sizeof(int));

    newFile.write((char*)data, MAX_HIDDEN_PAGES*PAGE_SIZE);
    newFile.close();
    free(data);

    return 0;
}

/**
 * readPage() - reads a given page into buffer.
 * @argument1 : page number to be read.
 * @argument2 : buffer in which to read.
 *
 * Uses a very simple cache mechanism (bufferManager), if the page is
 * already in the buffer it reads from it else reads from the disk and at the same 
 * time stores in the cache.
 * 
 * Return : 0 on success, -1 on failure.
*/
RC FileHandle::readPage(PageNum pageNum, void *data) {
    if(pageNum >= getNumberOfPages() || pageNum < 0) {
        return -1;
    }
    if(!file.is_open()) return -1;

    readPageCounter++;
    if(bm.pageInBuffer(fileName, pageNum, data) == 0) { return 0; }
    pageNum += MAX_HIDDEN_PAGES;
    file.seekg(pageNum*PAGE_SIZE, std::ios_base::beg);
    file.read((char*)data, PAGE_SIZE);

    bm.storeInBuffer(fileName, pageNum - MAX_HIDDEN_PAGES, data);

    return 0;
}

/**
 * writePage() - writes a given page into cache
 * @argument1 : page number to be written.
 * @argument2 : buffer to be written.
 * 
 * Uses a very simple cache mechanism, it the *data ( or buffer) is 
 * written to cache until the cache is full and then only writes back to
 * the disk.
 * 
 * Return : 0 on success, -1 on failure.
*/
RC FileHandle::writePage(PageNum pageNum, const void *data) {
    if(pageNum >= getNumberOfPages()) {
        return -1;
    }

    if(!file.is_open()) return -1;
    updateFreeSpaceForPage(pageNum, data);
    writePageCounter++;
    pageNum += MAX_HIDDEN_PAGES;
    if(bm.storeInBuffer(fileName, pageNum - MAX_HIDDEN_PAGES, data) == 0) return 0;
   /* file.seekp(pageNum*PAGE_SIZE, std::ios_base::beg);
    file.write((char*)data, PAGE_SIZE); */

    return 0;
}

/**
 * writeBackPage() - writes a given page into disk
 * @argument1 : page number to be written
 * @argument2 : buffer to be written.
 * 
 * When the cache is full, this funciton is used to write back to disk.
 *
 * Return : 0 on success, -1 on failure.
*/
RC FileHandle::writeBackPage(int pageNum, const void *data) {
    if(pageNum >= getNumberOfPages()) {
        return -1;
    }

    if(!file.is_open()) return -1;

    pageNum += MAX_HIDDEN_PAGES;
    file.seekp(pageNum*PAGE_SIZE, std::ios_base::beg);
    file.write((char*)data, PAGE_SIZE);

    return 0;
}

/**
 * appendPage() - Appends a new page to the file.
 * @argument1 : buffer of null data to be appended to file.
 * 
 * Return : 0 on success, -1 on failure.
*/
RC FileHandle::appendPage(const void *data) {

    if(!file.is_open()) { 
        return -1; 
    }
    file.seekp(0,std::ios_base::end);
    file.write((char*)data,PAGE_SIZE);
    file.seekp(0,std::ios_base::beg);
    appendPageCounter++;
    numPages++;
    return 0;
}

/**
 * updateFreeSpaceForPage() - Appends a new page to the file.
 * @argument1 : page number for which the free space is to be updated.
 * @argument2 : buffer containing data for the above page number.
 * 
 * Updates the freespace for the given page number into the header page. 
 *
 * Return : 0 on success, -1 on failure.
*/
RC FileHandle::updateFreeSpaceForPage(int pageNum, const void* data) {
    RT dirSlot, recSlot;
    memcpy((char*)&dirSlot, (char*)data + PAGE_SIZE - sizeof(RT), sizeof(RT));
    memcpy((char*)&recSlot, (char*)data + PAGE_SIZE - 2*sizeof(RT), sizeof(RT));

    RT freeSpace = dirSlot - recSlot;
    memcpy((char*)hiddenData + (2*pageNum)*sizeof(RT), (char*)&freeSpace, sizeof(RT));
    memcpy((char*)hiddenData + (2*pageNum+1)*sizeof(RT), (char*)data + PAGE_SIZE - 3*sizeof(RT), sizeof(RT));
    return 0;
}

/**
 * getNumberOfPages() - Gives the number of pages of a file minus the header page.
 * 
 * Return : number of pages.
*/
int FileHandle::getNumberOfPages() {
    return numPages;
}

/**
 * collectCounterValues() - read performance counters.
 * @argument1 : value of number of read disk I/O.
 * @argument2 : value of number of write disk I/O.
 * @argument3 : value of number of pages appended in the disk.
 * 
 * Return : above 3 arguments passed by reference.
*/
RC FileHandle::collectCounterValues(unsigned &readPageCount, 
                                    unsigned &writePageCount, 
                                    unsigned &appendPageCount) {
    readPageCount = readPageCounter;
    writePageCount = writePageCounter;
    appendPageCount = appendPageCounter;
    return 0;
}

/**
 * initPageDirectory() - initialize the basic page structure of RBFM file.
 * 
 * Return : page (*data) with directory initialized to default values.
*/
RC FileHandle::initPageDirectory(void* data) {
    RT dirSlotPointer = PAGE_SIZE - 3*sizeof(RT);
    RT recordSlotPointer = 0;
    RT freeSlots = 0;
    memcpy((char*)data + PAGE_SIZE - sizeof(RT), (char*)&dirSlotPointer, sizeof(RT));
    memcpy((char*)data + PAGE_SIZE - 2*sizeof(RT), (char*)&recordSlotPointer, sizeof(RT));
    memcpy((char*)data + PAGE_SIZE - 3*sizeof(RT), (char*)&freeSlots, sizeof(RT));
    return 0;
}

/**
 * readCounterFromHiddenPage() - reads the performance counter values from 
 *                               hidden/header page upon file open.
 * 
 * Return : 0 on success.
*/
RC FileHandle::readCounterFromHiddenPage(){
    hiddenData = malloc(MAX_HIDDEN_PAGES*PAGE_SIZE - OFFSET_FOR_FS_TABLE*sizeof(int));
    void* counters = malloc(OFFSET_FOR_FS_TABLE*sizeof(int));
    file.seekg(0);
    file.read((char*)counters, OFFSET_FOR_FS_TABLE*sizeof(int));

    file.seekg(4*sizeof(int));
    file.read((char*)hiddenData, MAX_HIDDEN_PAGES*PAGE_SIZE - OFFSET_FOR_FS_TABLE*sizeof(int));

    memcpy((char*)&readPageCounter, (char*)counters, sizeof(int));
    memcpy((char*)&writePageCounter, (char*)counters + sizeof(int), sizeof(int));
    memcpy((char*)&appendPageCounter, (char*)counters + 2*sizeof(int), sizeof(int));
    memcpy((char*)&numPages, (char*)counters + 3*sizeof(int), sizeof(int));
    readPageCounter++;

    free(counters);
    return 0;
}

/**
 * updateCounterInHiddenPage() - updates performance counter to hidden/header page before file close.
 * 
 * Return : 0 on success.
*/
RC FileHandle::updateCounterInHiddenPage() {
    writePageCounter++;
    file.seekp(0);
    file.write((char*)&readPageCounter, sizeof(int));
    file.seekp(sizeof(int));
    file.write((char*)&writePageCounter, sizeof(int));
    file.seekp(2*sizeof(int));
    file.write((char*)&appendPageCounter, sizeof(int));
    file.seekp(3*sizeof(int));
    file.write((char*)&numPages, sizeof(int));

    file.seekp(OFFSET_FOR_FS_TABLE*sizeof(int));
    file.write((char*)hiddenData, MAX_HIDDEN_PAGES*PAGE_SIZE - OFFSET_FOR_FS_TABLE*sizeof(int));

    free(hiddenData);
    hiddenData = NULL;
    return 0;
}

/**
 * getTotalSlotsInPage() - returns number of slots (or records) in a page.
 * @argument1 : buffer with page data.
 * 
 * Return : number of slots/records.
*/
RT FileHandle::getTotalSlotsInPage(const void* data) {
    RT dirSlotPointer = 0;
    memcpy((char*)&dirSlotPointer, (char*)data + PAGE_SIZE - sizeof(RT), sizeof(RT));
    RT totalSlots = (PAGE_SIZE - 2*sizeof(RT) - dirSlotPointer)/(SLOT_SIZE*sizeof(RT));
    return totalSlots;
}

/**
 * findFreePage() - returns first free page with atleast the requiredSpace to insert data.
 * @argument1 : minuimum amount of free space required.
 *
 * Header page stores 2 bytes slots for each page to store the free space, the function first looks in
 * all header pages to check if there are available pages, if not then it will read from the disk the
 * the remaining pages whose slots can not fit in the header pages (they ar bound by MAX_HIDDEN_PAGES),
 * 
 * Return : First page with free space.
*/
int FileHandle::findFreePage(RT requiredSpace) {
    if(numPages == 0) return -1;

    int pagesToStartLoadingFrom = -1;
    getHiddenPagesToLoad(pagesToStartLoadingFrom);
    if(pagesToStartLoadingFrom == -1) {
        RT freeSpace = 0, freeSlots = 0;
        int totalPages = getNumberOfPages();
        int offset = 0;
        for(int i = 0; i < totalPages; i++) {
            memcpy((char*)&freeSpace, (char*)hiddenData + offset, sizeof(RT));
            offset += sizeof(RT);
            memcpy((char*)&freeSlots, (char*)hiddenData + offset, sizeof(RT));
            offset += sizeof(RT);
            if(freeSpace >= requiredSpace && freeSlots > 0 ) return i;
            if(freeSlots == 0 && freeSpace >= (RT)(requiredSpace + SLOT_SIZE*sizeof(RT))) return i;
        }
    } else {
        RT freeSpace = 0, freeSlots = 0;
        int totalPages = getNumberOfPages();
        int offset = 0;
        for(int i = 0; i < (int)((MAX_HIDDEN_PAGES*PAGE_SIZE/(2*sizeof(RT))) - SLOT_SIZE*sizeof(int)) ; i++) {
            memcpy((char*)&freeSpace, (char*)hiddenData + offset, sizeof(RT));
            offset += sizeof(RT);
            memcpy((char*)&freeSlots, (char*)hiddenData + offset, sizeof(RT));
            offset += sizeof(RT);
            if(freeSpace >= requiredSpace && freeSlots > 0 ) return i;
            if(freeSlots == 0 && freeSpace >= (RT)(requiredSpace + SLOT_SIZE*sizeof(RT))) return i;
        }

        void* pageData = malloc(PAGE_SIZE);
        for(int i = pagesToStartLoadingFrom; i < totalPages; i += 2) {
            readPage(i, pageData);
            if(hasEnoughSpace(pageData, requiredSpace) != -1) {
                free(pageData);
                return i;
            }
        }
        free(pageData);
    }
    
    return -1;
}

/**
 * hasEnoughSpace() - checks if a page has enough space
 * @argument1 : buffer with data of the page.
 * @argument2 : minimum required space that is desired.
 * @argument3 : action == UPDATED -> we do not need to consider space occupied by slot, else
 *              include the size of slot
 * Return : number of slots/records.
*/
RT FileHandle::hasEnoughSpace(void* pageData, const RT requiredSpace, const int action) {
    RT dirSlot, recSlot;
    memcpy((char*)&dirSlot, (char*)pageData + PAGE_SIZE - sizeof(RT), sizeof(RT));
    memcpy((char*)&recSlot, (char*)pageData + PAGE_SIZE - 2*sizeof(RT), sizeof(RT));

    if(action == UPDATED) {
        if(dirSlot - recSlot >= requiredSpace) return recSlot;
        return -1;
    }

    RT freeSlots = 0;
    memcpy((char*)&freeSlots, (char*)pageData + PAGE_SIZE - 3*sizeof(RT), sizeof(RT));
    if(freeSlots > 0) {
        if(dirSlot - recSlot >= requiredSpace) return recSlot;
        return -1;
    }

    if(dirSlot - recSlot >= (RT)(requiredSpace + SLOT_SIZE*sizeof(RT))) return recSlot;

    return -1;
}

/**
 * getHiddenPagesToLoad() - used in findFreeSpace() when the number of pages fit in hidden pages.
 * @argument1 : num pages to load by reference.
 *
 * Return : by refernce into @argument1 the number of hidden pages to check for free spacew slots.
*/
int FileHandle::getHiddenPagesToLoad(int& numOfPagesToRead) {
    int totalPages = getNumberOfPages();
    int firstHiddenPageCap = PAGE_SIZE/(2*sizeof(RT)) - OFFSET_FOR_FS_TABLE*sizeof(int);
    if(totalPages > (int)(firstHiddenPageCap + ((MAX_HIDDEN_PAGES - 1)*PAGE_SIZE/(2*sizeof(RT))))) {
        numOfPagesToRead = (firstHiddenPageCap + ((MAX_HIDDEN_PAGES - 1)*PAGE_SIZE/(2*sizeof(RT))));
        return MAX_HIDDEN_PAGES;
    }

    return MAX_HIDDEN_PAGES;
}