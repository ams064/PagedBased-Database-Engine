#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

typedef unsigned short RTS;
const int MAX_HIDDEN_IX_PAGES = 1;
const int PREV_MERGE = -3;
const int PAGE_SCANNED = -1;
const int LAST_ENTRY = -2;

enum NodeType {
    LEAF = 0,
    INTERNAL = 1,
};

class IXFileHandle;
class IndexManager;
class IX_ScanIterator;

class CompositeKey {
private:
    void* key = NULL;
    RID rid;
    RTS keyType;
    RTS keyLen = 0;
public:
    void* getWritableKey();

    RTS getKeyLength() const;

    RID getRID() const;

    void updateSlotNum(RTS slotNum);

    void updatePageNum(int pageNum);

    CompositeKey() {
        key = NULL;
        keyLen = 0;
    }

    CompositeKey(RTS type, const void* data, const RID& r) {
        keyType = type;
        if(keyType == TypeInt) {
            key = malloc(sizeof(int) + sizeof(int) + sizeof(RTS));
            memcpy((char*)key, (char*)data, sizeof(int));
            this->keyLen = sizeof(int);
        } else if (keyType == TypeReal) {
            key = malloc(sizeof(float) + sizeof(int) + sizeof(RTS));
            memcpy((char*)key, (char*)data, sizeof(float));
            this->keyLen = sizeof(float);
        } else {
            int len = 0;
            memcpy((char*)&len, (char*)data, sizeof(int));
            key = malloc(sizeof(int) + len + sizeof(int) + sizeof(RTS));
            memcpy((char*)key, (char*)data, len + sizeof(int));
            this->keyLen = len + sizeof(int);
        }
        rid = r;
    }

   // offset = 0 a >key, rid> is passed, else a whole page is passed.
   CompositeKey(RTS type, const void* data, RTS offset = 0) {
        keyType = type;
        if(keyType == TypeInt) {
            key = malloc(sizeof(int) + sizeof(int) + sizeof(RTS));
            memcpy((char*)key, (char*)data + offset, sizeof(int));
            memcpy((char*)&rid.pageNum, (char*)data + offset + sizeof(int), sizeof(int));
            memcpy((char*)&rid.slotNum, (char*)data + offset + sizeof(int) + sizeof(int), sizeof(RTS));
            this->keyLen = sizeof(int);
        } else if(keyType == TypeReal) {
            key = malloc(sizeof(float) + sizeof(int) + sizeof(RTS));
            memcpy((char*)key, (char*)data + offset, sizeof(float));
            memcpy((char*)&rid.pageNum, (char*)data + offset + sizeof(float), sizeof(int));
            memcpy((char*)&rid.slotNum, (char*)data + offset +  sizeof(float) + sizeof(int), sizeof(RTS));
            this->keyLen = sizeof(float);
        } else {
            int len = 0;
            memcpy((char*)&len, (char*)data + offset, sizeof(int));
            key = malloc(sizeof(int) + len + sizeof(int) + sizeof(RTS));
            memcpy((char*)key, (char*)data + offset, len + sizeof(int));
            memcpy((char*)&rid.pageNum, (char*)data + offset + sizeof(int) + len, sizeof(int));
            memcpy((char*)&rid.slotNum, (char*)data + offset + sizeof(int) + len + sizeof(int), sizeof(RTS));
            this->keyLen = len + sizeof(int);
        }
   }

    /* Assignment and Copy C'tor */
    /* Follows C++ Rule of 3. Note move c'tor and assignement is not reqd. in this case */
    CompositeKey& operator=(const CompositeKey& ckey) {
        if(this->key != NULL) free(this->key);
        if(ckey.key == NULL) {
            this->key = NULL;
            this->keyLen = 0;
            return *this;
        }

        this->keyLen = ckey.keyLen;
        this->key = malloc(ckey.getKeyLength());
        memcpy((char*)this->key, (char*)ckey.key, ckey.getKeyLength());
        this->keyType = ckey.keyType;
        this->rid = ckey.rid;
        return *this;
    }

    CompositeKey(const CompositeKey& ckey) {
        if(this->key != NULL) free(this->key);
        if(ckey.key == NULL) {
            this->key = NULL;
            this->keyLen = 0;
            return;
        }

        this->keyLen = ckey.keyLen;
        this->key = malloc(ckey.getKeyLength());
        memcpy((char*)this->key, (char*)ckey.key, ckey.getKeyLength());
        this->keyType = ckey.keyType;
        this->rid = ckey.rid;
        return;
    }

    ~CompositeKey() {
        if(key != NULL)
            free(key);
    }

    /*Comparator for CompositeKey */
    bool operator==(const CompositeKey& b) const {
        /* used for full scan */
        if(this->key == NULL || b.key == NULL) {
            return true;
        }

        if(keyType == TypeInt) {
            int a_i = 0;
            int b_i = 0;
            memcpy((char*)&a_i, (char*)this->key, sizeof(int));
            memcpy((char*)&b_i, (char*)b.key, sizeof(int));
            if (a_i == b_i) {
                // for insert and deletion
                if(this->rid == b.rid) return true;
                // for scan equality, LE_OP and GE_OP 
                if((this->rid.pageNum == INT_MAX && this->rid.slotNum == 0) ||
                   (b.rid.pageNum == INT_MAX && b.rid.slotNum == 0)) return true;
            }
        } else if(keyType == TypeReal) {
            float a_f = 0;
            float b_f = 0;
            memcpy((char*)&a_f, (char*)this->key, sizeof(float));
            memcpy((char*)&b_f, (char*)b.key, sizeof(float));
            if ( a_f == b_f ) {
                if(this->rid == b.rid) return true;
                if((this->rid.pageNum == INT_MAX && this->rid.slotNum == 0) ||
                   (b.rid.pageNum == INT_MAX && b.rid.slotNum == 0)) return true;
            }
        } else {
            int len_a = 0, len_b = 0;
            memcpy((char*)&len_a, (char*)this->key, sizeof(int));
            memcpy((char*)&len_b, (char*)b.key, sizeof(int));
            if(len_a != len_b) return false;

            RT retVal = -2;
            char str1[len_a + 1];
            char str2[len_b + 1];
            str1[len_a] = '\0';
            str2[len_b] = '\0';
            memcpy((char*)str1, (char*)this->key + sizeof(int), len_a);
            memcpy((char*)str2, (char*)b.key + sizeof(int), len_b);

            retVal = strcmp(str1, str2);

            if (retVal == 0) {
                if(this->rid == b.rid) return true;
                if((this->rid.pageNum == INT_MAX && this->rid.slotNum == 0) ||
                  (b.rid.pageNum == INT_MAX && b.rid.slotNum == 0)) return true;
            }
        }
        return false;
    }

    bool operator>(const CompositeKey& b) const {
        /*used for full scan */
        if(this->key == NULL  || b.key == NULL) return true;

        if(keyType == TypeInt) {
            int a_i = 0;
            int b_i = 0;
            memcpy((char*)&a_i, (char*)this->key, sizeof(int));
            memcpy((char*)&b_i, (char*)b.key, sizeof(int));
            if(a_i > b_i) {
                return true;
            } else if (a_i == b_i) {
                if((this->rid.pageNum == INT_MAX && this->rid.slotNum == USHRT_MAX) || 
                   (b.rid.pageNum == INT_MAX && b.rid.slotNum == USHRT_MAX)) return false;
                return this->rid > b.rid;
            }
        } else if(keyType == TypeReal) {
            float a_f = 0;
            float b_f = 0;
            memcpy((char*)&a_f, (char*)this->key, sizeof(float));
            memcpy((char*)&b_f, (char*)b.key, sizeof(float));
            if (a_f > b_f) {
                return true;
            } else if(a_f == b_f) {
                if((this->rid.pageNum == INT_MAX && this->rid.slotNum == USHRT_MAX) || 
                    (b.rid.pageNum == INT_MAX && b.rid.slotNum == USHRT_MAX)) return false;
                return this->rid > b.rid;
            }
        } else {
            int len_a = 0, len_b = 0;
            memcpy((char*)&len_a, (char*)this->key, sizeof(int));
            memcpy((char*)&len_b, (char*)b.key, sizeof(int));
            //if(len_a > len_b) return true;
            //if(len_b > len_a) return false;

            RT retVal = -2;
            char str1[len_a + 1];
            char str2[len_b + 1];
            str1[len_a] = '\0';
            str2[len_b] = '\0';
            memcpy((char*)str1, (char*)this->key + sizeof(int), len_a);
            memcpy((char*)str2, (char*)b.key + sizeof(int), len_b);
            retVal = strcmp(str1, str2);
            if (retVal > 0) {
                return true;
            } else if(retVal == 0) {
                if((this->rid.pageNum == INT_MAX && this->rid.slotNum == USHRT_MAX) ||
                   (b.rid.pageNum == INT_MAX && b.rid.slotNum == USHRT_MAX)) return false;
                return this->rid > b.rid;
            }
        }
        return false;
    }

    bool operator!=(const CompositeKey& b) const {
        return !(*this == b);
    }

    bool operator<(const CompositeKey& b) const {
        return (!(*this == b) && !(*this > b));
    }

    bool operator>=(const CompositeKey& b) const {
        return (*this == b) || (*this > b);
    }

    bool operator<=(const CompositeKey& b) const {
        return (*this == b) || (*this < b);
    }

    /* Print overload for CompositeKye */
    friend ostream &operator<<( ostream &output, const CompositeKey &cKey ) {
         if(cKey.keyType == TypeInt) {
             int val = 0;
             memcpy((char*)&val, (char*)cKey.key, sizeof(int));
             output<<"\""<<val<<":[("<<cKey.rid.pageNum<<","<<cKey.rid.slotNum<<")]\"";
         } else if(cKey.keyType == TypeReal) {
             float val = 0;
             memcpy((char*)&val, (char*)cKey.key, sizeof(float));
             output<<"\""<<val<<":[("<<cKey.rid.pageNum<<","<<cKey.rid.slotNum<<")]\"";
         } else {
             char* str = new char[cKey.keyLen - sizeof(int)+1];
             memcpy((char*)str, (char*)(cKey.key) + sizeof(int), cKey.keyLen - sizeof(int));
             str[cKey.keyLen - sizeof(int)] = '\0';
             output<<"\""<<str<<":[("<<cKey.rid.pageNum<<","<<cKey.rid.slotNum<<")]\"";
             delete[] str;
         }
         return output;
   }
};

class Node {
private:
    void* data;
public:

    Node(void* data) {
        this->data = data;
    }

    ~Node() { 
        this->data = NULL;
    }

    virtual int getSibling() = 0;

    virtual void setSibling(const int) = 0;

    virtual RC getNextKey(const RTS indexType, const CompositeKey& key, CompositeKey& newKey) = 0;

    virtual int getNextNodePointer(const RTS indexType, const CompositeKey& newKey, 
                                   RTS& keyOffset, int& sibling, int& prevSibling) = 0;

    virtual void getAllPagePointers(const RTS indexType, vector<int>& children) = 0;

    virtual RTS getInsertOffset(const RTS indexType, const CompositeKey& newKey) = 0; // add parameters

    virtual RTS getSplitOffset(const RTS indexType, const RTS totalSpace, RTS& leftEntries,
                               CompositeKey& keyToPushUp) = 0;

    virtual RT findKeyOffset(const RTS indexType, const CompositeKey& findKey) = 0;

    virtual int removeKey(const RTS indexType, const RTS keyOffset) = 0;

    virtual void printKeys(const RTS indexType) = 0;
    // extra parameters marked as default are used only for internal node
    virtual RTS insertEntryInNode(const RTS indexType, CompositeKey& newKey,
                                  int p2 = INT_MAX, int p1 = INT_MAX) = 0;

    RTS getFreeSpace() const;

    RTS getEntries() const;

    RTS getNodeType() const;

    RTS getLastOffset() const;

    int getPageNum() const;

    bool hasEnoughSpace(const RTS requiredSpace) const;

    bool checkIfUnderUtilized() const;

    void getKeyFromOffset(const RTS indexType, RTS offset, CompositeKey& key) const;

    void* getWritableData() const;

    void setFreeSpace(const RTS freeSpace);

    void setEntries(const RTS entries);

    void setNodeType(const RTS type);

    void setLastOffset(const RTS offset);

    void setPageNum(const int pageNum);

    void moveKeysByOffset(const RTS startOffset, const RT moveLength);

    void moveDataToNode(Node& b, RTS numBytes, RTS startOffsetFrom, RTS startOffsetTo);

    void insertKeyAtOffset(const RTS indexType, RTS offset, CompositeKey& key);

    RTS splitNode(const RTS indexType, Node& newNode, CompositeKey& keyToPushUp);

    RTS mergeNodes(const RTS indexType, Node& b);
};

class InternalNode : public Node {
private:
    void* data;

    virtual int getSibling() override {return -1;}

    virtual void setSibling(const int sibling) override {}

    virtual RC getNextKey(const RTS indexType, const CompositeKey& key, CompositeKey& newKey) override { return -1; }

    virtual RT findKeyOffset(const RTS indexType, const CompositeKey& findKey) override { return -1; }

public:
    InternalNode(void* data) : Node(data) {
        this->data = data;
    }

    ~InternalNode() {
        this->data = NULL;
    }

    virtual int getNextNodePointer(const RTS indexType, const CompositeKey& newKey,
                                   RTS& keyOffset, int& sibling, int& prevSibling) override;

    virtual void getAllPagePointers(const RTS indexType, vector<int>& children) override;

    virtual RTS getInsertOffset(const RTS indexType, const CompositeKey& newKey) override;

    virtual RTS getSplitOffset(const RTS indexType, const RTS totalSpace,
                               RTS& leftEntries, CompositeKey& keyToPushUp) override;

    virtual RTS insertEntryInNode(const RTS indexType, CompositeKey& newKey, int p2, int p1) override;

    virtual int removeKey(const RTS indexType, const RTS keyOffset) override;

    virtual void printKeys(const RTS indexType) override;

};

class LeafNode : public Node {
private:
    void* data;

    virtual int getNextNodePointer(const RTS indexType, const CompositeKey& newKey, 
                                   RTS& keyOffset, int& sibling, int& prevSibling) override {
        return -1;
    }

    virtual void getAllPagePointers(const RTS indexType, vector<int>& children) override {}

public:
    LeafNode(void* data) : Node(data) {
        this->data = data;
    }

    ~LeafNode() {
        this->data = NULL;
    }
    virtual int getSibling() override;

    virtual void setSibling(const int sibling) override;

    virtual RC getNextKey(const RTS indexType, const CompositeKey& lowKey, CompositeKey& newKey) override;

    virtual RTS getInsertOffset(const RTS indexType, const CompositeKey& newKey) override;

    virtual RTS getSplitOffset(const RTS indexType, const RTS totalSpace,
                               RTS& leftEntries, CompositeKey& keyToPushUp) override;

    virtual RTS insertEntryInNode(const RTS indexType, CompositeKey& newKey, int p2, int p1) override;

    virtual RT findKeyOffset(const RTS indexType, const CompositeKey& findKey) override;

    virtual int removeKey(const RTS indexType, const RTS keyOffset) override;

    virtual void printKeys(const RTS indexType) override;
};

class IndexManager {
private:
    RC insertEntryRecursively(IXFileHandle& ixFileHandle, const RTS indexType, Node& node, CompositeKey& entry, 
                              int& newChildEntry, CompositeKey& keyToPushUp);

    void depthFirstTraversal(IXFileHandle &ixFileHandle, const RTS indexType, Node& node) const;

    RC deleteEntryRecursively(IXFileHandle& ixFileHandle, const RTS indexType, Node& node,
                              CompositeKey& deleteKey, Node& parent, int parentSibling, 
                              int parentPrevSibling, int parentKeyOffset, int& oldChildPointer);
public:
    static IndexManager &instance();

    // Create an index file.
    RC createFile(const std::string &fileName);

    // Delete an index file.
    RC destroyFile(const std::string &fileName);

    // Open an index and return an ixFileHandle.
    RC openFile(const std::string &fileName, IXFileHandle &ixFileHandle);

    // Close an ixFileHandle for an index.
    RC closeFile(IXFileHandle &ixFileHandle);

    // Insert an entry into the given index that is indicated by the given ixFileHandle.
    RC insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

    // Delete an entry from the given index that is indicated by the given ixFileHandle.
    RC deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

    // Initialize and IX_ScanIterator to support a range search
    RC scan(IXFileHandle &ixFileHandle,
            const Attribute &attribute,
            const void *lowKey,
            const void *highKey,
            bool lowKeyInclusive,
            bool highKeyInclusive,
            IX_ScanIterator &ix_ScanIterator);

    // Print the B+ tree in pre-order (in a JSON record format)
    void printBtree(IXFileHandle &ixFileHandle, const Attribute &attribute) const;

protected:
    IndexManager() = default;                                                   // Prevent construction
    ~IndexManager() = default;                                                  // Prevent unwanted destruction
    IndexManager(const IndexManager &) = default;                               // Prevent construction by copying
    IndexManager &operator=(const IndexManager &) = default;                    // Prevent assignment

};

class IX_ScanIterator {
private:
    IXFileHandle* ixFileHandle;
    CompositeKey lowCKey;
    CompositeKey highCKey;
    RTS indexType;
    int lastPageNum;
    int lastRet;
    void* data;

    RC treeSearch(const RTS indexType, Node& node, const CompositeKey& lowKey);

    RC searchNode(const RTS indexType, const CompositeKey& lowKey);

    void initComparisonKeys(const void* lowKey, const void* highKey,
                            bool lowKeyInclusive, bool highKeyInclusive);

    RC getNextNonEmptySibling(void* data);

public:

    // Constructor
    IX_ScanIterator();

    // Destructor
    ~IX_ScanIterator();

    RC initializeScanIterator(IXFileHandle& ixFileHandle, const Attribute& attribute,
                           const void* lowKey, const void* highKey, bool lowKeyInclusive,
                           bool highKeyInclusive);

    // Get next matching entry
    RC getNextEntry(RID &rid, void *key);

    // Terminate index scan
    RC close();
};

class IXFileHandle : public FileHandle {
private:
    // variables to keep counter for each operation
    void* hiddenData;
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;
    int numPages;
    int root;
    RTS rootType;
    std::fstream file;
    bool changed;

    virtual RC createHiddenPage(const std::string& fileName);

    virtual RC readCounterFromHiddenPage();

    virtual RC updateCounterInHiddenPage();

public:
    std::string fileName;

    IXFileHandle(); //C'tor
    ~IXFileHandle(); //D'tor

    virtual void openFile(const std::string& fileName) override;

    virtual void closeRoutine() override;

    virtual bool isEmpty() override;

    virtual bool isOpen() override;

    virtual RC readPage(PageNum pageNum, void *data) override;

    virtual RC writePage(PageNum pageNum, const void *data) override;

    virtual RC appendPage(const void *data) override;

    virtual int getNumberOfPages() override;
    //Override
    RC initPageDirectory(void* data, RT type);

    int getRoot();

    RTS getNodeType(void* data);

    void setRoot(const int newRoot);
    // To detect if the BTree has changed, used only to optimize disk I/Os in scan
    bool isChanged() { return changed; }

    void resetChanged() { changed = false;}

    void setChanged() { changed = true;}
    // Put the current counter values of associated PF FileHandles into variables
    virtual RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) override;
};

#endif
