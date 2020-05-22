#ifndef _pfm_h_
#define _pfm_h_

#include <fstream>
#include <string>
#include <cstring>
#include <iostream>
#include <math.h>
#include <unordered_map>

using namespace std;

typedef int PageNum;
typedef int RC;
typedef short RT;

const RT PAGE_SIZE = 4096;
const RT MAX_HIDDEN_PAGES  = 6;
const RT OFFSET_FOR_FS_TABLE  = 4; //ONLY A MULTIPLIER WITH SIZEOF(INT)

const RT MAX_CACHE_PAGE_PER_FILE = 1;
const RT MAX_FILES_FOR_CACHE = 1;
const RT SLOT_SIZE  = 4;
const RT DELETED = 30000;
const RT UPDATED = 30001;

class FileHandle;

class CacheInfo {
public:
    void* pageData;
    int dirtyBit;
    std::string  fileName;

    CacheInfo () {
        pageData = NULL;
        dirtyBit = 0;
    }

    ~CacheInfo() {
        if(pageData != NULL) {
            free(pageData);
        }
        dirtyBit = 0;
     }
} ;

class BufferManager {
private:
    std::unordered_map<std::string, std::unordered_map<int, CacheInfo>> buffer;
public:
    static BufferManager &instance();

    RC pageInBuffer(const std::string& fileName, const int pageNum, void* data);
    RC storeInBuffer(const std::string& fileName, const int pageNum, const void* data);
    RC writeBackPageToFile(const std::string& fileName, const int pageNum, const void* data, int dirtyBit);
    RC writeBackFullBufferToFile(const std::string& fileName);

protected:
    BufferManager();
    ~BufferManager() {
        if(buffer.size() > 0)
            buffer.clear();
    }
    BufferManager(const BufferManager &);                         // Prevent construction by copying
    BufferManager &operator=(const BufferManager &);              // Prevent assignment
};

class PagedFileManager {
public:
    static PagedFileManager &instance();                                // Access to the _pf_manager instance

    RC createFile(const std::string &fileName);                         // Create a new file
    RC destroyFile(const std::string &fileName);                        // Destroy a file
    RC openFile(const std::string &fileName, FileHandle &fileHandle);   // Open a file
    RC closeFile(FileHandle &fileHandle);                               // Close a file
protected:
    PagedFileManager();                                                 // Prevent construction
    ~PagedFileManager();                                                // Prevent unwanted destruction
    PagedFileManager(const PagedFileManager &);                         // Prevent construction by copying
    PagedFileManager &operator=(const PagedFileManager &);              // Prevent assignment

};

class FileHandle {
private:
    // variables to keep the counter for each operation
    unsigned readPageCounter;
    unsigned writePageCounter;
    unsigned appendPageCounter;
    unsigned numPages;
    std::fstream file;
    int numHiddenPages;
    void* hiddenData;
    BufferManager& bm;

    int getHiddenPagesToLoad(int& pageToStartLoadingFrom);
    RC writeBackPage(int pageNum, const void* data);

    virtual RC createHiddenPage(const std::string& fileName);
    virtual RC updateCounterInHiddenPage();
    virtual RC readCounterFromHiddenPage();
public:
    std::string fileName;

    FileHandle();                                                       // Default constructor
    virtual ~FileHandle();                                              // Destructor

    virtual void openFile(const std::string& fileName);
    virtual void closeRoutine();
    virtual bool isEmpty();
    virtual bool isOpen();
    virtual RT hasEnoughSpace(void* pageData, const RT requiredSpace, const int action = 0);
    virtual RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount,
                            unsigned &appendPageCount); 

    virtual RC readPage(PageNum pageNum, void *data);                           // Get a specific page
    virtual RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
    virtual RC appendPage(const void *data);                                    // Append a specific page
    virtual int getNumberOfPages();                                        // Get the number of pages in the file
    virtual RC initPageDirectory(void* data);

    RC updateFreeSpaceForPage(int pageNum, const void* data);
    int findFreePage(RT requiredSpace);
    RT getTotalSlotsInPage(const void* data);
    void setFileName(const std::string& name) { fileName = name; }
};

#endif
