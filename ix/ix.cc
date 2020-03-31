#include "ix.h"

void* CompositeKey::getWritableKey() {
    memcpy((char*)(this->key) + keyLen, (char*)&rid.pageNum, sizeof(int));
    memcpy((char*)(this->key) + keyLen + sizeof(int), (char*)&rid.slotNum, sizeof(RTS));
    return this->key;
}

RTS CompositeKey::getKeyLength() const {
    return this->keyLen + sizeof(int) + sizeof(RTS);
}

RID CompositeKey::getRID() const {
    return this->rid;
}

void CompositeKey::updateSlotNum(RTS slotNum) {
    this->rid.slotNum = slotNum;
    return;
}

void CompositeKey::updatePageNum(int pageNum) {
    this->rid.pageNum = pageNum;
    return;
}

/**
 * getFreeSpace() - get free space in a node.
 *
 * Return : free space.
*/
RTS Node::getFreeSpace() const {
    RTS freeSpace = 0;
    memcpy((char*)&freeSpace, (char*)(this->data) + PAGE_SIZE - sizeof(RTS), sizeof(RTS));
    return freeSpace;
}

/**
 * getEntries() - get number of entries in a node.
 *
 * Return : number of entries.
*/
RTS Node::getEntries() const {
    RTS entries = 0;
    memcpy((char*)&entries, (char*)(this->data) + PAGE_SIZE - 2*sizeof(RTS), sizeof(RTS));
    return entries;
}

/**
 * getEntries() - get type of the node (leaf/internal).
 *
 * Return : type.
*/
RTS Node::getNodeType() const {
    RTS type = 0;
    memcpy((char*)&type, (char*)(this->data) + PAGE_SIZE - 3*sizeof(RTS), sizeof(RTS));
    return type;
}

/**
 * getLastOffset() - get offset of the last entry end.
 *
 * Return : last offset.
*/
RTS Node::getLastOffset() const {
    RTS offset = 0;
    memcpy((char*)&offset, (char*)(this->data) + PAGE_SIZE - 4*sizeof(RTS), sizeof(RTS));
    return offset;
}

/**
 * getPageNum() - get the page number of the node.
 *
 * Return : page number.
*/
int Node::getPageNum() const {
    int pageNum = 0;
    memcpy((char*)&pageNum, (char*)(this->data) + PAGE_SIZE - 4*sizeof(RTS) - sizeof(int), sizeof(int));
    return pageNum;
}

/**
 * hasEnoughSpace() - check if the node has atleast the required space.
 * @argument1 : required space
 *
 * Return : true if has enough space, false otherwise.
*/
bool Node::hasEnoughSpace(const RTS requiredSpace) const {
    return getFreeSpace() >= requiredSpace;
}

/**
 * checkIfUnderUtilized() - check if a page has less than 50% occupancy.
 *
 * Return : true if underutilized, false otherwise.
*/
bool Node::checkIfUnderUtilized() const {
    return this->getFreeSpace() > this->getLastOffset();
}

/**
 * getKeyFromOffset() - get the key starting from a given offset.
 * @argument1 : type of the index attribute (int/real/varchar).
 * @argument2 : offset to read key from.
 * @argument3 : CompositeKey passed as reference variable- out parameter.
 *
 * Return : @argument3 passed as reference variable.
*/
void Node::getKeyFromOffset(const RTS indexType, RTS offset, CompositeKey& key) const {
    CompositeKey temp(indexType, this->data, offset);
    key = temp;
    return;
}

/**
 * getWritableData() - get the byte data to be written to the disk.
 *
 * Return : pointer to byte data.
*/
void* Node::getWritableData() const {
    return this->data;
}

/**
 * setFreeSpace() - sets the freespace in a node.
 * @argument1 : new free space.
 *
 * Return : void.
*/
void Node::setFreeSpace(const RTS freeSpace) {
    memcpy((char*)(this->data) + PAGE_SIZE - sizeof(RTS), (char*)&freeSpace, sizeof(RTS));
}

/**
 * setEntries() - sets the number of entries in a node.
 * @argument1 : new entries.
 *
 * Return : void.
*/
void Node::setEntries(const RTS entries) {
    memcpy((char*)(this->data) + PAGE_SIZE - 2*sizeof(RTS), (char*)&entries, sizeof(RTS));
}

/**
 * setNodeType() - sets the type of the node
 * @argument1 : type 
 *
 * Return : void.
*/
void Node::setNodeType(const RTS type) {
    memcpy((char*)(this->data) + PAGE_SIZE - 3*sizeof(RTS), (char*)&type, sizeof(RTS));
}

/**
 * setLastOffset() - sets the last offset in a node
 * @argument1 : last offset to be set. 
 *
 * Return : void.
*/
void Node::setLastOffset(const RTS offset) {
    memcpy((char*)(this->data) + PAGE_SIZE - 4*sizeof(RTS), (char*)&offset, sizeof(RTS));
}

/**
 * setPageNum() - sets the page number of a node.
 * @argument1 : page number to be set
 *
 * Return : void.
*/
void Node::setPageNum(const int pageNum) {
    memcpy((char*)(this->data) + PAGE_SIZE - 4*sizeof(RTS) - sizeof(int), (char*)&pageNum, sizeof(int));
}

/**
 * moveKeysByOffset() - move entries/keys by a given offset.
 * @argument1 : starting point from where to move.
 * @argument2 : move length by which to move the keys.
 *
 * Return : void.
*/
void Node::moveKeysByOffset(const RTS startOffset, const RT moveLength) {
    if((getLastOffset() - startOffset) == 0) return;
    memmove((char*)(this->data) + startOffset + moveLength, (char*)(this->data) + startOffset, getLastOffset() - startOffset);
}

/**
 * moveDataToNode() - move entries/keys from a node to another node.
 * @argument1 : node 2, where the entries are to be moved.
 * @argument2 : number of bytes to be moved.
 * @argument3 : starting point in node 1 from where to move the entries.
 * @argument4 : starting point in node 2 at which to move the entries.
 *
 * Return : void.
*/
void Node::moveDataToNode(Node& b, RTS numBytes, RTS startOffsetFrom, RTS startOffsetTo) {
    memmove((char*)(b.data) + startOffsetTo, (char*)this->data + startOffsetFrom, numBytes);
}

/**
 * insertKeyAtOffset() - inserts a key at a given offset in a node.
 * @argument1 : type of the key.
 * @argument2 : offset at which to insert.
 * @argument3 : composite key to be inserted.
 *
 * Return : void.
*/
void Node::insertKeyAtOffset(const RTS indexType, RTS offset, CompositeKey& key) {
    memcpy((char*)this->data + offset, (char*)(key.getWritableKey()), key.getKeyLength());
    return;
}

/**
 * splitNode() - splits a node into 2 nodes.
 * @argument1 : type of the key
 * @argument2 : reference of the new node created.
 * @argument3 : reference of the key which will be pushed up after split. (out parameter).
 *
 * Return : 0.
*/
RTS Node::splitNode(const RTS indexType, Node& newNode, CompositeKey& keyToPushUp) {
    RTS recordOffset = this->getLastOffset();
    RTS freeSpace = this->getFreeSpace();
    RTS numEntries = this->getEntries();
    RTS leftEntries = 0;
    RTS totalSpace = recordOffset + freeSpace;

    RTS splitOffset = this->getSplitOffset(indexType, totalSpace, leftEntries, keyToPushUp);
    RTS bytesToMove = recordOffset - splitOffset;
    this->moveDataToNode(newNode, bytesToMove, splitOffset, 0);

    this->setFreeSpace(freeSpace + bytesToMove);
    this->setEntries(leftEntries);
    this->setLastOffset(splitOffset);

    RTS newNodeFreeSpace = newNode.getFreeSpace();
    newNode.setFreeSpace(newNodeFreeSpace - bytesToMove);
    newNode.setEntries(numEntries - leftEntries);
    newNode.setLastOffset(bytesToMove);

    return splitOffset - keyToPushUp.getKeyLength();
}

/**
 * mergeNodes() - merge 2 nodes into one.
 * @argument1 : type of the keys present in the node.
 * @argument2 : node to be merged with.
 *
 * Return : 0.
*/
RTS Node::mergeNodes(const RTS indexType, Node& b) {
    RTS recordOffset = this->getLastOffset();
    RTS recordOffset_b = b.getLastOffset();
    RTS freeSpace = this->getFreeSpace();
    RTS numEntries = this->getEntries();
    RTS numEntries_b = b.getEntries();

    RTS bytesToMove = recordOffset_b;
    b.moveDataToNode(*this, bytesToMove, 0, recordOffset);

    this->setFreeSpace(freeSpace - recordOffset_b);
    this->setEntries(numEntries + numEntries_b);
    this->setLastOffset(recordOffset + recordOffset_b);

    b.setEntries(0);
    b.setLastOffset(0);

    return 0;
}

/**
 * getNextNodePointer() - gets the page number to look into while searching for a key.
 * @argument1 : type of the keys present in the node.
 * @argument2 : key to be searched.
 * @argument3 : offset where the key is found (out parameter).
 * @argument4 : next node of the node found (out parameter).
 * @argument5 : prev node of the node found (out parameter).
 *
 * Return : page number in the file.
*/
int InternalNode::getNextNodePointer(const RTS indexType, const CompositeKey& newKey,
                               RTS& keyOffset, int& sibling, int& prevSibling) {
    RTS dataOffset = sizeof(int);
    RTS numEntries = this->getEntries();
    int pageNum = 0;
    for(int i = 0; i < numEntries; i++) {
        CompositeKey key(indexType, this->data, dataOffset);
        if(key >= newKey) {
        memcpy((char*)&pageNum, (char*)(this->data) + dataOffset - sizeof(int), sizeof(int));
        memcpy((char*)&sibling, (char*)(this->data) + dataOffset + key.getKeyLength(), sizeof(int));
        keyOffset = dataOffset;
        return pageNum;
        }
        keyOffset = dataOffset;
        dataOffset += key.getKeyLength() + sizeof(int);
    }

    memcpy((char*)&prevSibling, (char*)(this->data) + keyOffset - sizeof(int), sizeof(int));
    memcpy((char*)&pageNum, (char*)(this->data) + dataOffset - sizeof(int), sizeof(int));
    sibling = INT_MAX;
    return pageNum;
}

/**
 * getAllPagePointers() - gets the page number to look into while searching for a key.
 * @argument1 : type of the keys present in the node..
 * @argument2 : list of pointers to return. (out parameter).
 *
 * Return : all the page pointers in a node.
*/
void InternalNode::getAllPagePointers(const RTS indexType, vector<int>& children) {
    RTS numEntries = this->getEntries();
    if(numEntries == 0) return;

    int pagePointer = 0;
    memcpy((char*)&pagePointer, (char*)this->data, sizeof(int));
    children.push_back(pagePointer);
    RTS dataOffset = sizeof(int);

    for(int i = 0; i < numEntries; i++) {
        CompositeKey key(indexType, this->data, dataOffset);
        dataOffset += key.getKeyLength();
        memcpy((char*)&pagePointer, (char*)this->data + dataOffset, sizeof(int));
        children.push_back(pagePointer);
        dataOffset += sizeof(int);
    }
}

/**
 * getInsertOffset() - gets the offset at which to insert a new composite key.
 * @argument1 : type of the keys present in the node..
 * @argument2 : new composite key to be inserted.
 *
 * Return : offset at which to insert.
*/
RTS InternalNode::getInsertOffset(const RTS indexType, const CompositeKey& newKey) {
    RTS dataOffset = sizeof(int);
    RTS numEntries = this->getEntries();
    if(numEntries == 0) return 0;
    for(int i = 0; i < numEntries; i++) {
        CompositeKey key(indexType, this->data, dataOffset);
        if(key >= newKey) {
            return dataOffset;
        }
        dataOffset += key.getKeyLength() + sizeof(int);
    }
    return dataOffset;
}

/**
 * getSplitOffset() - get offset at which to split a node.
 * @argument1 : type of the keys present in the node.
 * @argument2 : total space in the node.
 * @argument3 : return the number of entries in the node after split. (out parameter).
 * @argument4 : return the key that will be pushed up in B+ tree.
 *
 * Return : get the offset at which to split.
*/
RTS InternalNode::getSplitOffset(const RTS indexType, const RTS totalSpace,
                                 RTS& leftEntries, CompositeKey& keyToPushUp) {
    RTS dataOffset = sizeof(int);
    RTS numEntries = getEntries();
    RTS halfSpace = totalSpace/2;
    for(RTS i = 0; i < numEntries; i++) {
        CompositeKey key(indexType, this->data, dataOffset);
        dataOffset += key.getKeyLength() + sizeof(int);
        if(dataOffset >= halfSpace) {
            keyToPushUp = key;
            leftEntries = i+1;
            return dataOffset - sizeof(int);
        }
    }
    return -1;
}

/**
 * insertEntryInNode() - insert an entry into node.
 * @argument1 : type of the keys present in the node.
 * @argument2 : key to be inserted.
 * @argument3 : previous child pointer used in case of internal node.
 * @argument4 : next child pointer used in case of internal node.
 *
 * Return : 0 on success, -1 otherwise.
*/
RTS InternalNode::insertEntryInNode(const RTS indexType, CompositeKey& newKey,
                                    int p2 = INT_MAX, int p1 = INT_MAX) {
    RTS offset = getInsertOffset(indexType, newKey);
    int keyLen = newKey.getKeyLength();
    // if its the first record in the internal node then we need space for 2 pointers and a key.
    if(offset == 0) {
    keyLen += 2*sizeof(int);
    moveKeysByOffset(offset, keyLen);
    memcpy((char*)data + offset, (char*)&p1, sizeof(int));
    memcpy((char*)data + offset + sizeof(int), (char*)(newKey.getWritableKey()), keyLen - 2*sizeof(int));
    memcpy((char*)data + offset + sizeof(int) + newKey.getKeyLength(), (char*)&p2, sizeof(int));
    } else {
        keyLen += sizeof(int);
        moveKeysByOffset(offset, keyLen);
        memcpy((char*)data + offset, (char*)(newKey.getWritableKey()), keyLen - sizeof(int));
        memcpy((char*)data + offset + keyLen - sizeof(int), (char*)&p2, sizeof(int));
    }

    RTS newFreeSpace = this->getFreeSpace() - keyLen;
    RTS newEntries = this->getEntries() + 1;
    RTS lastOffset = this->getLastOffset() + keyLen;
    this->setFreeSpace(newFreeSpace);
    this->setEntries(newEntries);
    this->setLastOffset(lastOffset);
    return 0;
}

/**
 * removeKey() - remove a key at the offset.
 * @argument1 : type of the keys present in the node.
 * @argument2 : offset in the node from which to remove the key.
 *
 * Return : 0 on success.
*/
int InternalNode::removeKey(const RTS indexType, const RTS keyOffset) {
    CompositeKey toBeRemoved(indexType, this->data, keyOffset);
    RTS numEntries = this->getEntries();
    RTS freeSpace = this->getFreeSpace();
    RTS lastOffset = this->getLastOffset();

    if (numEntries == 0) return -1;
    /* when there is o nly on entry in the internal ode, do not remove the entry, instead make the second pointer as NULL */
    if (numEntries == 1) {
        int newRoot = INT_MAX;
        memcpy((char *) &newRoot, (char *) (this->data) + keyOffset - sizeof(int), sizeof(int));
        this->setFreeSpace(freeSpace + 2 * sizeof(int) + toBeRemoved.getKeyLength());
        this->setEntries(0);
        this->setLastOffset(0);
        /* Return newRoot to mark as first entry, if this page was root, then the root need to be updated*/
        return newRoot;
    }
    this->moveKeysByOffset(keyOffset + toBeRemoved.getKeyLength() + sizeof(int),
                           -(toBeRemoved.getKeyLength() + sizeof(int)));
    this->setEntries(numEntries - 1);
    this->setFreeSpace(freeSpace + toBeRemoved.getKeyLength() + sizeof(int));
    this->setLastOffset(lastOffset - toBeRemoved.getKeyLength() - sizeof(int));
    return -1;
}

/**
 * printKeys() - print all the keys of a node.
 * @argument1 : type of the keys present in the node.
 *
 * Return : 0 on success.
*/
void InternalNode::printKeys(const RTS indexType) {
    RTS dataOffset = sizeof(int);
    RTS numEntries = this->getEntries();
    if (numEntries == 0) return;
    cout << "[";
    for (int i = 0; i < numEntries; i++) {
        CompositeKey key(indexType, this->data, dataOffset);
        std::cout << key;
        if (i < numEntries - 1) {
            std::cout << ",";
        }
        dataOffset += key.getKeyLength() + sizeof(int);
    }
    cout << "],";
}

/*********************Leaf Node helpers *********************************/

/**
 * getSibling() : get the next leaf node.
 *
 * Return : return the leaf node page number.
*/
int LeafNode::getSibling() {
    int sibling;
    memcpy((char*)&sibling, (char*)data + PAGE_SIZE - 4*sizeof(RTS) - 2*sizeof(int), sizeof(int));
    return sibling;
}

/**
 * setSibling() : set the next leaf node.
 *
 * Return : void.
*/
void LeafNode::setSibling(const int sibling) {
    memcpy((char*)data + PAGE_SIZE - 4*sizeof(RTS) - 2*sizeof(int), (char*)&sibling, sizeof(int));
}

/**
 * getNextKey() : get the key which comes after the lowKey
 * @argument1 : type of the keys present in the node.
 * @argument2 : lowKey
 * @argument3 : return the key found. (out parameter).
 *
 * Return : 0 if key is found, PAGE_SCANNED if reached END OF node, LAST_ENTRY 
            while returning the last entry in node.
*/
RC LeafNode::getNextKey(const RTS indexType, const CompositeKey& lowKey, CompositeKey& newKey) {
    RTS dataOffset = 0;
    RTS numEntries = this->getEntries();
    if(numEntries == 0) return PREV_MERGE;

    for(int i = 0 ; i < numEntries; i++) {
        CompositeKey key(indexType, this->data, dataOffset);
        if(key >= lowKey) {
            newKey = key;
            if(i == numEntries - 1) return LAST_ENTRY;
            return 0;
        }
        dataOffset += key.getKeyLength();
    }
    return PAGE_SCANNED;
}

/**
 * insertEntryInNode() - insert an entry into node.
 * @argument1 : type of the keys present in the node.
 * @argument2 : key to be inserted.
 *
 * Return : 0 on success, -1 otherwise.
*/
RTS LeafNode::getInsertOffset(RTS indexType, const CompositeKey& newKey) {
    RTS dataOffset = 0;
    RTS numEntries = this->getEntries();
    for(int i = 0; i < numEntries; i++) {
        CompositeKey key(indexType, this->data, dataOffset);
        if(key >= newKey) {
            return dataOffset;
        }
        dataOffset += key.getKeyLength();
    }
    return dataOffset;
}

/**
 * getSplitOffset() - get offset at which to split a node.
 * @argument1 : type of the keys present in the node.
 * @argument2 : total space in the node.
 * @argument3 : return the number of entries in the node after split. (out parameter).
 * @argument4 : return the key that will be pushed up in B+ tree.
 *
 * Return : get the offset at which to split.
*/
RTS LeafNode::getSplitOffset(const RTS indexType, const RTS totalSpace,
                             RTS& leftEntries, CompositeKey& keyToPushUp) {
    RTS dataOffset = 0;
    RTS numEntries = getEntries();
    RTS halfSpace = totalSpace/2;
    for(RTS i = 0; i < numEntries; i++) {
        CompositeKey key(indexType, this->data, dataOffset);
        dataOffset += key.getKeyLength();
        if(dataOffset >= halfSpace) {
            keyToPushUp = key;
            leftEntries = i+1;
            return dataOffset;
        }
    }
    return -1;
}

/**
 * insertEntryInNode() - insert an entry into node.
 * @argument1 : type of the keys present in the node.
 * @argument2 : key to be inserted.
 * @argument3 : previous child pointer used in case of internal node.
 * @argument4 : next child pointer used in case of internal node.
 *
 * Return : 0 on success, -1 otherwise.
*/
RTS LeafNode::insertEntryInNode(const RTS indexType, CompositeKey& newKey, int p2 = INT_MAX, int p1 = INT_MAX) {
    RTS offset = getInsertOffset(indexType, newKey);
    moveKeysByOffset(offset, newKey.getKeyLength());

    memcpy((char*)data + offset, (char*)(newKey.getWritableKey()), newKey.getKeyLength());

    RTS newFreeSpace = this->getFreeSpace() - newKey.getKeyLength();
    RTS newEntries = this->getEntries() + 1;
    RTS lastOffset = this->getLastOffset() + newKey.getKeyLength();
    this->setFreeSpace(newFreeSpace);
    this->setEntries(newEntries);
    this->setLastOffset(lastOffset);
    return 0;
}

/**
 * findKeyOffset() - find the offset at which a given key is present.
 * @argument1 : type of the keys present in the node.
 * @argument2 : key to be searched.
 *
 * Return : offset at which the key is present, -1 if not present.
*/
RT LeafNode::findKeyOffset(const RTS indexType, const CompositeKey& findKey) {
    int dataOffset = 0;
    RTS numEntries = this->getEntries();
    for(RTS i = 0 ; i < numEntries; i++) {
        CompositeKey key(indexType, this->data, dataOffset);
        if(findKey == key) {
            return dataOffset;
        }
        dataOffset += key.getKeyLength();
    }
    return -1;
}

/**
 * removeKey() - remove a key at the offset.
 * @argument1 : type of the keys present in the node.
 * @argument2 : offset in the node from which to remove the key.
 *
 * Return : 0 on success.
*/
int LeafNode::removeKey(const RTS indexType, const RTS keyOffset) {
    CompositeKey toBeRemoved(indexType, this->data, keyOffset);
    RTS numEntries = this->getEntries();
    RTS freeSpace = this->getFreeSpace();
    RTS lastOffset = this->getLastOffset();

    if(numEntries == 0) return -1;
    /* when there is o nly on entry in the internal ode, do not remove the entry, instead make the second pointer as NULL */
    if(numEntries == 1) {
        int newRoot = INT_MAX;
        this->setFreeSpace(freeSpace + toBeRemoved.getKeyLength());
        this->setEntries(0);
        this->setLastOffset(0);
        /* Return newRoot to mark as first entry, if this page was root, then the root need to be updated*/
        return newRoot;
    }

    this->moveKeysByOffset(keyOffset + toBeRemoved.getKeyLength(), -(toBeRemoved.getKeyLength()));
    this->setEntries(numEntries - 1);
    this->setFreeSpace(freeSpace + toBeRemoved.getKeyLength());
    this->setLastOffset(lastOffset - toBeRemoved.getKeyLength());
    return -1;
}

/**
 * printKeys() - print all the keys of a node.
 * @argument1 : type of the keys present in the node.
 *
 * Return : 0 on success.
*/
void LeafNode::printKeys(const RTS indexType) {
    RTS dataOffset = 0;
    RTS numEntries = this->getEntries();
    if (numEntries == 0) return;
    cout << "[";
    for (int i = 0; i < numEntries; i++) {
        CompositeKey key(indexType, this->data, dataOffset);
        std::cout << key;
        if (i < numEntries - 1) {
            std::cout << ",";
        }
        dataOffset += key.getKeyLength();
    }
    cout << "]";
}

//IX manager singleton
IndexManager &IndexManager::instance() {
    static IndexManager _index_manager = IndexManager();
    return _index_manager;
}

/**
 * createFile() - create an index file.
 * @argument1 : name of the file.
 *
 * Return : 0 on success, -1 on fail.
*/
RC IndexManager::createFile(const std::string &fileName) {
    return PagedFileManager::instance().createFile(fileName);
}

/**
 * destroyFile() - delete an index file.
 * @argument1 : name of the file.
 *
 * Return : 0 on success, -1 on fail.
*/
RC IndexManager::destroyFile(const std::string &fileName) {
    return PagedFileManager::instance().destroyFile(fileName);
}

/**
 * openFile() - open an index file.
 * @argument1 : name of the file.
 * @argument2 : ixfilehandle (out parameter).
 *
 * Return : 0 on success, -1 on fail.
*/
RC IndexManager::openFile(const std::string &fileName, IXFileHandle &ixFileHandle) {
    return PagedFileManager::instance().openFile(fileName, ixFileHandle);
}

/**
 * openFile() - close an index file.
 * @argument1 : ixfilehandle having the file details.
 *
 * Return : 0 on success, -1 on fail.
*/
RC IndexManager::closeFile(IXFileHandle &ixFileHandle) {
    return PagedFileManager::instance().closeFile(ixFileHandle);
}

/**
 * insertEntry() - insert an entry into B tree.
 * @argument1 : ixfilehandle having the Btree details.
 * @argument2 : attribute on which the Btree exists.
 * @argument3 : key to be inserted.
 * @argument4 : rid associated with key.
 *
 * Return : 0 on success, -1 on fail.
*/
RC IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute,
                             const void *key, const RID &rid) {
    int root = ixFileHandle.getRoot();
    // Root or pages in the index file, create one leaf node and make it the root
    void* data = malloc(PAGE_SIZE);
    CompositeKey entry(attribute.type, key, rid);
    int newChildEntry = INT_MAX;

    if(root == INT_MAX) {
        ixFileHandle.initPageDirectory(data, LEAF);
        ixFileHandle.appendPage(data);
        ixFileHandle.setRoot(ixFileHandle.getNumberOfPages()-1);
    } else {
        ixFileHandle.readPage(root, data);
    }


    CompositeKey keyToPushUp;
    RTS indexType = attribute.type;
    if(ixFileHandle.getNodeType(data) == LEAF) {
        LeafNode leafNode(data);
        insertEntryRecursively(ixFileHandle, indexType, leafNode, entry, 
                               newChildEntry, keyToPushUp);
    } else {
        InternalNode internalNode(data);
        insertEntryRecursively(ixFileHandle, indexType, internalNode, entry, 
                               newChildEntry, keyToPushUp);
    }

    free(data);
    return 0;
}

/**
 * insertEntryRecursively() -  helper function to insert an entry into B tree.
 * @argument1 : ixfilehandle having the Btree details.
 * @argument2 : type of the keys present in the node.
 * @argument2 : node in which currently looking to insert.
 * @argument3 : composite key to be inserted.
 * @argument4 : flag to detect if reorganization needs to take place.
 * @argument5 : composite key to be pushed up to upper level if split happened.
 *
 * Return : 0 on success, -1 on fail.
*/
RC IndexManager::insertEntryRecursively(IXFileHandle& ixFileHandle, const RTS indexType, 
                                        Node& node, CompositeKey& entry, int& newChildEntry, 
                                        CompositeKey& keyToPushUp) {
    if(node.getNodeType() == INTERNAL) {
        RTS nextKeyOffset;
        int dummySibling, dummyPrevSibling;
        int nextPointer = node.getNextNodePointer(indexType, entry, nextKeyOffset, 
                                                  dummySibling, dummyPrevSibling);
        void* data = malloc(PAGE_SIZE);
        ixFileHandle.readPage(nextPointer, data);
        if(ixFileHandle.getNodeType(data) == LEAF) {
            LeafNode leafNode(data);
            insertEntryRecursively(ixFileHandle, indexType, leafNode, entry, 
                                   newChildEntry, keyToPushUp);
        } else {
            InternalNode internalNode(data);
            insertEntryRecursively(ixFileHandle, indexType, internalNode, entry, 
                                   newChildEntry, keyToPushUp);
        }

        // No splits happened
        if(newChildEntry == INT_MAX) {
            free(data);
            return 0;
        }
        
        if(node.hasEnoughSpace(keyToPushUp.getKeyLength() + sizeof(int))) {
            node.insertEntryInNode(indexType, keyToPushUp, newChildEntry);
            ixFileHandle.writePage(node.getPageNum(), node.getWritableData());
            newChildEntry = INT_MAX;
            free(data);
            return 0;
        } else {
            void* dataNew = malloc(PAGE_SIZE);
            ixFileHandle.initPageDirectory(dataNew, INTERNAL);
            InternalNode newNode(dataNew);
            CompositeKey newEntry = keyToPushUp;
            int newChildPointer = newChildEntry;
            // Split the node
            node.splitNode(indexType, newNode, keyToPushUp);
            // set newChildEntry to new page num created
            newChildEntry = newNode.getPageNum();
            node.setFreeSpace(node.getFreeSpace() + keyToPushUp.getKeyLength());
            node.setEntries(node.getEntries() - 1);
            node.setLastOffset(node.getLastOffset() - keyToPushUp.getKeyLength());
            // Decide where to insert the new Key
            if(keyToPushUp >= newEntry) {
                node.insertEntryInNode(indexType, newEntry, newChildPointer);
            } else {
                newNode.insertEntryInNode(indexType, newEntry, newChildPointer);
            }

            newChildEntry = newNode.getPageNum();
            ixFileHandle.appendPage(newNode.getWritableData());
            ixFileHandle.writePage(node.getPageNum(), node.getWritableData());
            free(dataNew);
        }

        // Root needs to be updated, if in case the node which got split is root
        if (ixFileHandle.getRoot() == node.getPageNum()) {
            void* dataRoot = malloc(PAGE_SIZE);
            ixFileHandle.initPageDirectory(dataRoot, INTERNAL);
            InternalNode newRoot(dataRoot);
            newRoot.insertEntryInNode(indexType, keyToPushUp, newChildEntry, node.getPageNum());
            ixFileHandle.setRoot(newRoot.getPageNum());
            ixFileHandle.appendPage(newRoot.getWritableData());
            free(dataRoot);
        }
        free(data);
        return 0;

    // node is leaf
    } else {
        if(node.hasEnoughSpace(entry.getKeyLength())) {
            node.insertEntryInNode(indexType, entry);
            ixFileHandle.writePage(node.getPageNum(), node.getWritableData());
            return 0;
        } else {
            void* dataNew = malloc(PAGE_SIZE);
            ixFileHandle.initPageDirectory(dataNew, LEAF);
            LeafNode newNode(dataNew);
            // Split node
            node.splitNode(indexType, newNode, keyToPushUp);
            newChildEntry = newNode.getPageNum();
            newNode.setSibling(node.getSibling());
            node.setSibling(newNode.getPageNum());
            // Decide which node to insert new entry
            if(keyToPushUp >= entry) {
                node.insertEntryInNode(indexType, entry);
            } else {
                newNode.insertEntryInNode(indexType, entry);
            }
            ixFileHandle.appendPage(newNode.getWritableData());
            ixFileHandle.writePage(node.getPageNum(), node.getWritableData());
            free(dataNew);

            // update root, if the node which got split is root
            if (ixFileHandle.getRoot() == node.getPageNum()) {
                void* dataRoot = malloc(PAGE_SIZE);
                ixFileHandle.initPageDirectory(dataRoot, INTERNAL);
                InternalNode newRoot(dataRoot);
                newRoot.insertEntryInNode(indexType, keyToPushUp, newChildEntry, node.getPageNum());
                ixFileHandle.setRoot(newRoot.getPageNum());
                ixFileHandle.appendPage(newRoot.getWritableData());
                free(dataRoot);
            }
            return 0;
        }
    }

    return 0;
}

/**
 * deleteEntry() - delete an entry from B tree.
 * @argument1 : ixfilehandle having the Btree details.
 * @argument2 : attribute on which the Btree exists.
 * @argument3 : key to be deleted.
 * @argument4 : rid associated with key.
 *
 * Return : 0 on success, -1 on fail.
*/
RC IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, 
                             const void *key, const RID &rid) {
    int root = ixFileHandle.getRoot();
    if(root == INT_MAX) return -1;

    void* rootData = malloc(PAGE_SIZE);
    ixFileHandle.readPage(root, rootData);

    RTS indexType = attribute.type;
    CompositeKey deleteKey(indexType, key, rid);
    int oldNodePointer = INT_MAX;
    int retVal = 0;
    if(ixFileHandle.getNodeType(rootData) == LEAF) {
        LeafNode lNode(rootData);
        retVal = deleteEntryRecursively(ixFileHandle, indexType, lNode, deleteKey, lNode,
                                        -1, -1, -1, oldNodePointer);
    } else {
        InternalNode intNode(rootData);
        retVal = deleteEntryRecursively(ixFileHandle, indexType, intNode, deleteKey, intNode, 
                                        -1, -1, -1, oldNodePointer);
    }

    if(retVal == 0) {
        ixFileHandle.setChanged();
    }

    free(rootData);
    return retVal;
}

/**
 * deleteEntryRecursively() - helper to delete an entry from B tree.
 * @argument1 : ixfilehandle having the Btree details.
 * @argument2 : type of the keys present in the node.
 * @argument2 : node in which currently looking to delete.
 * @argument3 : composite key to be delete.
 * @argument4 : parent of the node currently being inspected.
 * @argument5 : sibling of the current node in parent node.
 * @argument6 : previous sibling of the node currently being inspected.
 * @argument7 : flag to check if reorganization is required in each iteration.
 *
 * Return : 0 on success, -1 on fail.
*/
RC IndexManager::deleteEntryRecursively(IXFileHandle& ixFileHandle, const RTS indexType, Node& node, 
                                        CompositeKey& deleteKey, Node& parent, int parentSibling, 
                                        int parentPrevSibling, int parentKeyOffset, int& oldNodePointer) {

     if(node.getNodeType() == INTERNAL) {
        int sibling = 0, prevSibling = INT_MAX;
        RTS keyOffset = 0;
        int nextPointer = node.getNextNodePointer(indexType, deleteKey, keyOffset, sibling, prevSibling);
        void* childData = malloc(PAGE_SIZE);
        ixFileHandle.readPage(nextPointer, childData);
        if(ixFileHandle.getNodeType(childData) == LEAF) {
           LeafNode lNode(childData);
           if(deleteEntryRecursively(ixFileHandle, indexType, lNode, deleteKey, node, sibling, 
                                     prevSibling, keyOffset, oldNodePointer) == -1) {
               free(childData);
               return -1;
           }
        } else {
           InternalNode intNode(childData);
           if(deleteEntryRecursively(ixFileHandle, indexType, intNode, deleteKey, node, sibling, 
                                     prevSibling, keyOffset, oldNodePointer) == -1) {
               free(childData);
               return -1;
           }
        }

        if(oldNodePointer == INT_MAX) {
            free(childData);
            return 0;
        }

        /* If merge happenned in the child nodes of current nodes*/
        int newRoot = node.removeKey(indexType, keyOffset); //Handle cases when there is only one child node left.
        /* If the node is the root of the tree and all the entries 
         * from the root have been removed, update the root */
        if(node.getPageNum() == ixFileHandle.getRoot()) {
            if(newRoot != -1) {
                ixFileHandle.setRoot(newRoot);
                oldNodePointer = INT_MAX;
            } else {
                oldNodePointer = INT_MAX;
                ixFileHandle.writePage(node.getPageNum(), node.getWritableData());
            }
            free(childData);
            return 0;
        }

        /* if < %50 occupancy*/
        if(!node.checkIfUnderUtilized()) {
            oldNodePointer = INT_MAX;
            ixFileHandle.writePage(node.getPageNum(), node.getWritableData());
            free(childData);
            return 0;
        }

        void* mergeWith = malloc(PAGE_SIZE);
        CompositeKey keyToPullDown;
        parent.getKeyFromOffset(indexType, parentKeyOffset, keyToPullDown);

        // If there is no next sibling, try merging with previous sibling
        if(parentSibling == INT_MAX && parentPrevSibling != INT_MAX) {
            ixFileHandle.readPage(parentPrevSibling, mergeWith);
            InternalNode intNode(mergeWith);
            if(intNode.hasEnoughSpace(node.getLastOffset() + keyToPullDown.getKeyLength())) {
                intNode.insertKeyAtOffset(indexType, intNode.getLastOffset(), keyToPullDown);
                intNode.setLastOffset(intNode.getLastOffset() + keyToPullDown.getKeyLength());
                intNode.setFreeSpace(intNode.getFreeSpace() - keyToPullDown.getKeyLength());
                intNode.setEntries(intNode.getEntries() + 1);
                intNode.mergeNodes(indexType, node);
                oldNodePointer = -1;
                ixFileHandle.writePage(intNode.getPageNum(), intNode.getWritableData());
                free(childData);
                free(mergeWith);
                return 0;
            }
        } else { 
            ixFileHandle.readPage(parentSibling, mergeWith);
            InternalNode intNode(mergeWith);
            /* merge with next sibling */
            if(node.hasEnoughSpace(intNode.getLastOffset() + keyToPullDown.getKeyLength())) {
                node.insertKeyAtOffset(indexType, node.getLastOffset(), keyToPullDown);
                node.setLastOffset(node.getLastOffset() + keyToPullDown.getKeyLength());
                node.setFreeSpace(node.getFreeSpace() - keyToPullDown.getKeyLength());
                node.setEntries(node.getEntries() + 1);
                node.mergeNodes(indexType, intNode);
                oldNodePointer = -1;
                ixFileHandle.writePage(node.getPageNum(), node.getWritableData());
                free(childData);
                free(mergeWith);
                return 0;
           /* if merge not possible with next node, try with previous node */
           } else if(parentPrevSibling != INT_MAX) {
               ixFileHandle.readPage(parentPrevSibling, mergeWith);
               InternalNode intNode(mergeWith);
               if(intNode.hasEnoughSpace(node.getLastOffset() + keyToPullDown.getKeyLength())) {
                   intNode.insertKeyAtOffset(indexType, intNode.getLastOffset(), keyToPullDown);
                   intNode.setLastOffset(intNode.getLastOffset() + keyToPullDown.getKeyLength());
                   intNode.setFreeSpace(intNode.getFreeSpace() - keyToPullDown.getKeyLength());
                   intNode.setEntries(intNode.getEntries() + 1);
                   intNode.mergeNodes(indexType, node);
                   oldNodePointer = -1;
                   ixFileHandle.writePage(intNode.getPageNum(), intNode.getWritableData());
                   free(childData);
                   free(mergeWith);
                   return 0;
               }
           }
       }
       /* merge required but not possible, doing a lazy delete */
       ixFileHandle.writePage(node.getPageNum(), node.getWritableData());
       oldNodePointer = INT_MAX;
       free(childData);
       free(mergeWith);
       return 0;
     /* If node is a leaf node */
     } else {
        int keyOffset = node.findKeyOffset(indexType, deleteKey);
        /* ALready deleted or not present */
        if(keyOffset == -1) return -1;
        int newRoot = node.removeKey(indexType, keyOffset); //Handle cases when there is only one child node left.
        if(node.getPageNum() == ixFileHandle.getRoot()) {
            if(newRoot != -1) {
                /* complete Btree deletion has occurred */
                /* deleteEntry(...) should fail after this */
                ixFileHandle.setRoot(newRoot);
                oldNodePointer = INT_MAX;
            } else {
                oldNodePointer = INT_MAX;
                ixFileHandle.writePage(node.getPageNum(), node.getWritableData());
            }
            return 0;
        }

        if(!node.checkIfUnderUtilized()) {
            oldNodePointer = INT_MAX;
            ixFileHandle.writePage(node.getPageNum(), node.getWritableData());
            return 0;
        }

        parentSibling = node.getSibling();
        void* mergeWith = malloc(PAGE_SIZE);
        /*Check if the node is the last node*/
        if(parentSibling == INT_MAX && parentPrevSibling != INT_MAX) {
            ixFileHandle.readPage(parentPrevSibling, mergeWith);
            LeafNode lNode(mergeWith);
            if(lNode.hasEnoughSpace(node.getLastOffset())) {
                lNode.mergeNodes(indexType, node);
                lNode.setSibling(node.getSibling());
                oldNodePointer = -1;
                ixFileHandle.writePage(lNode.getPageNum(), lNode.getWritableData());
                free(mergeWith);
                return 0;
            }
        } else {
            ixFileHandle.readPage(parentSibling, mergeWith);
            LeafNode lNode(mergeWith);
            /* Merge with sibling node */
            if(node.hasEnoughSpace(lNode.getLastOffset())) {
                node.mergeNodes(indexType, lNode);
                node.setSibling(lNode.getSibling());
                oldNodePointer = -1;
                ixFileHandle.writePage(node.getPageNum(), node.getWritableData());
                free(mergeWith);
                return 0;
            /* if merge with sibling not possible try to merge with previous node */
            } else if(parentPrevSibling != INT_MAX) {
                ixFileHandle.readPage(parentPrevSibling, mergeWith);
                LeafNode lNode(mergeWith);
                if(lNode.hasEnoughSpace(node.getLastOffset())) {
                    lNode.mergeNodes(indexType, node);
                    lNode.setSibling(node.getSibling());
                    oldNodePointer = -1;
                    ixFileHandle.writePage(lNode.getPageNum(), lNode.getWritableData());
                    free(mergeWith);
                    return 0;
                }
            }
        }
        /* merge is required but not possible, do a lazy deletion */
        oldNodePointer = INT_MAX;
        ixFileHandle.writePage(node.getPageNum(), node.getWritableData());
        free(mergeWith);
        return 0;
    }
    /*unusual */
    return -1;
}

RC IndexManager::scan(IXFileHandle &ixFileHandle,
                      const Attribute &attribute,
                      const void *lowKey,
                      const void *highKey,
                      bool lowKeyInclusive,
                      bool highKeyInclusive,
                      IX_ScanIterator &ix_ScanIterator) {

    
    return ix_ScanIterator.initializeScanIterator(ixFileHandle, attribute,
                           lowKey, highKey, lowKeyInclusive,
                           highKeyInclusive);
}

/**
* printBtree() : prints the Btree in JSON format.
* @argument1 : ixFileHanlde for the BTree file.
* @argument2 : attribute on which the index exists.
*
* Return : void.
*/
void IndexManager::printBtree(IXFileHandle &ixFileHandle, const Attribute &attribute) const {
    int root = ixFileHandle.getRoot();
    if(root == INT_MAX) return;

    void* data = malloc(PAGE_SIZE);
    ixFileHandle.readPage(root, data);

    RTS indexType = attribute.type;

    if (ixFileHandle.getNodeType(data) == LEAF) {
        LeafNode lNode(data);
        depthFirstTraversal(ixFileHandle, indexType, lNode);
    } else {
        InternalNode intNode(data);
        depthFirstTraversal(ixFileHandle, indexType, intNode);
    }

   free(data);
   return;
}

/**
* printBtree() : depth first traversal of Btree to print.
* @argument1 : ixFileHanlde for the BTree file.
* @argument2 : type of the keys present in the node.
* @argument3 : attribute on which the index exists.
*
* Return : void.
*/
void IndexManager::depthFirstTraversal(IXFileHandle &ixFileHandle, const RTS indexType, Node& node) const {

    if(node.getNodeType() == INTERNAL) {
        std::cout<<"{"<<std::endl;
        std::cout<<"\"keys\":";
        node.printKeys(indexType);
        std::cout<<endl;
        std::cout<<"\"children\":["<<std::endl;
        vector<int> children;
        node.getAllPagePointers(indexType, children);

        void* data = malloc(PAGE_SIZE); 
        for(RTS i = 0 ; i < (RTS)children.size(); i++) {
            ixFileHandle.readPage(children[i], data);
            if(ixFileHandle.getNodeType(data) == INTERNAL) {
                if(i != 0)
                    std::cout<<","<<endl;
                InternalNode intNode(data);
                depthFirstTraversal(ixFileHandle, indexType, intNode);
            } else {
                LeafNode lNode(data);
                if(i != 0) {
                    std::cout<<","<<endl;
                }
                std::cout<<"{";
                std::cout<<"\"keys\":";
                lNode.printKeys(indexType);
                std::cout<<"}";
            }
        }
        free(data);
        std::cout<<"]"<<std::endl;
        std::cout<<"}";
    } else {
       std::cout<<"{"<<std::endl;
       std::cout<<"\"keys\":";
       node.printKeys(indexType);
       std::cout<<"}";
    }
}

IX_ScanIterator::IX_ScanIterator() {
    this->data = NULL;
}

IX_ScanIterator::~IX_ScanIterator() {
}

/**
* initializeScanIterator() : initialization of index scan iterator.
* @argument1 : ixFileHanlde for the BTree file.
* @argument2 : attribute on which the index exists.
* @argument3 : low key for scan.
* @argument4 : high key for scan.
* @argument5 : wether to include lowKey or not.
* @argument6 : wether to include high key.

*
* Return : 0 on successful initialization, -1 of fail.
*/
RC IX_ScanIterator::initializeScanIterator(IXFileHandle& ixFileHandle, const Attribute& attribute,
                                          const void* lowKey, const void* highKey, bool lowKeyInclusive,
                                          bool highKeyInclusive) {

    if(!ixFileHandle.isOpen()) return -1;
    this->ixFileHandle = &ixFileHandle;
    this->indexType = attribute.type;

    initComparisonKeys(lowKey, highKey, lowKeyInclusive, highKeyInclusive);

    /* Initialize first scan entry */
    /*get the first node( or page) */
    this->lastPageNum = searchNode(indexType, lowCKey);
    data = malloc(PAGE_SIZE);
    return 0;
}

/**
* initComparisonKeys() : initialize comparsion keys.
* @argument1 : low key for scan.
* @argument2 : high key for scan.
* @argument3 : wether to include lowKey or not.
* @argument4 : wether to include high key.

*
* Return : 0 on successful initialization, -1 of fail.
*/
void IX_ScanIterator::initComparisonKeys(const void* lowKey, const void* highKey, 
                                         bool lowKeyInclusive, bool highKeyInclusive) {
    if(lowKey != NULL) {
    /* Equality included */
        if(lowKeyInclusive) {
            RID r;
            /* By this, the comparator enforces equality */
            r.pageNum = INT_MAX;
            r.slotNum = 0;
            CompositeKey low(this->indexType, lowKey, r);
            this->lowCKey = low;
        } else {
            /* NO equality */
            RID r;
            /*can be anything */
            r.pageNum = INT_MAX;
            /*can be anything but 0 */
            r.slotNum = USHRT_MAX;
            CompositeKey low(this->indexType, lowKey, r);
            this->lowCKey = low;
        }
    }

    if(highKey != NULL) {
        if(highKeyInclusive) {
            RID r;
            /* By this comparator enforces equality */
            r.pageNum = INT_MAX;
            r.slotNum = 0;
            CompositeKey high(this->indexType, highKey, r);
            this->highCKey = high;
        } else {
            /* NO euqlity */
            RID r;
            /*can be anything */
            r.pageNum = INT_MAX;
            /*can be anything but 0 */
            r.slotNum = USHRT_MAX;
            CompositeKey high(this->indexType, highKey, r);
            this->highCKey = high;
       }
    }
    return;
}

/**
* searchNode() : wrapper on treeSearch method.
* @argument1 : type of the keys present in the node.
* @argument2 : key to be searched.
*
* Return : 0 on successful initialization, -1 of fail.
*/
RC IX_ScanIterator::searchNode(const RTS indexType, const CompositeKey& lowKey) {
    int root = this->ixFileHandle->getRoot();
    if(root == INT_MAX) return -1;

    void* data = malloc(PAGE_SIZE);
    this->ixFileHandle->readPage(root, data);

    int pageNum = -1;

    if(this->ixFileHandle->getNodeType(data) == LEAF) {
        LeafNode lNode(data);
        pageNum = treeSearch(indexType, lNode, lowKey);
    } else {
        InternalNode intNode(data);
        pageNum = treeSearch(indexType, intNode, lowKey);
    }

    free(data);
    return pageNum;
}

/**
* treeSearch() : recursive search of a key in the BTree.
* @argument1 : type of the keys present in the node.
* @argument2 : current node being looked into.
* @argument3 : key to be searched.
*
* Return : 0 on successful initialization, -1 of fail.
*/
RC IX_ScanIterator::treeSearch(const RTS indexType, Node& node, const CompositeKey& lowKey) {
    if(node.getNodeType() == INTERNAL) {
        RTS nextKeyOffset = 0;
        int dummySibling, prevSibling;
        int nextNode = node.getNextNodePointer(indexType, lowKey, nextKeyOffset, 
                                               dummySibling, prevSibling);
        void* data = malloc(PAGE_SIZE);
        this->ixFileHandle->readPage(nextNode, data);
        int pageNum = -1;
        if(this->ixFileHandle->getNodeType(data) == LEAF) {
            LeafNode lNode(data);
            pageNum = treeSearch(indexType, lNode, lowKey);
        } else {
            InternalNode intNode(data);
            pageNum = treeSearch(indexType, intNode, lowKey);
        }
        free(data);
        return pageNum;
    }

    return node.getPageNum();
}

/**
* getNextNonEmptySibling() : get the next non empty leaf node sibling.
* @argument1 : buffer to return the data of the node to be returned (out parameter).
*
* Return : -1 if no sibling found.
*/
RC IX_ScanIterator::getNextNonEmptySibling(void* data) {
    LeafNode currNode(data);
    int entries = currNode.getEntries();
    if(entries != 0) return currNode.getPageNum();

    int sibling = currNode.getSibling();
    int prevPage = -1;

    while(entries == 0) {
        if(sibling == INT_MAX) return INT_MAX;
        this->ixFileHandle->readPage(sibling, data);
        LeafNode lNode(data);
        entries = lNode.getEntries();
        prevPage = sibling;
        sibling = lNode.getSibling();
    }

    return prevPage;
}


/**
* getNextEntry() : get the next <key, rid> pair using iterator.
* @argument1 : rid to be returned (out parameter).
* @argument2 : buffer to return key (out parameter).
*
* Return : IX_EOF if reached EOF, 0 otherwise.
*/
RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
    
    //all pages scanned
    if(this->lastPageNum == INT_MAX || this->lastPageNum == -1) return IX_EOF;

    if(this->lastRet == LAST_ENTRY) {
        this->ixFileHandle->readPage(this->lastPageNum, data);
        this->lastPageNum = getNextNonEmptySibling(data);
        if(this->lastPageNum == INT_MAX) return IX_EOF;
        this->lastRet = 0;
    } else if(this->ixFileHandle->isChanged()) {
        this->ixFileHandle->readPage(this->lastPageNum, data);
        this->ixFileHandle->resetChanged();
    }

    LeafNode lNode(data);
    CompositeKey newKey;
    int retVal = lNode.getNextKey(this->indexType, this->lowCKey, newKey);

    /* Merged with previous node, rescan required */
    if(retVal == PREV_MERGE) {
        this->lastPageNum = searchNode(indexType, this->lowCKey);
        return getNextEntry(rid, key);
    }

 /* Optimization to reduce disk I/Os, basically when we have read the last record, 
 *    update the pageNUm to sibling,
 *   so that next time we just have to read the sibling page instead of first 
 *   reading the last page and then getting the 
 *   sibling */
    if(retVal == LAST_ENTRY) {
        this->lastPageNum = lNode.getSibling();
        this->lastRet = LAST_ENTRY;
    }

    // Page is completely scanned
    if(retVal == PAGE_SCANNED) {
        int sibling = getNextNonEmptySibling(data);
        /* NO sibling EOF */
        if(sibling == INT_MAX) {
            return IX_EOF;
        }

        LeafNode lNode(data);

        int retVal = lNode.getNextKey(this->indexType, this->lowCKey, newKey);
        /* unsual case something is wrong */
        if(retVal == -1) {
            return IX_EOF;
        }
        this->lastPageNum = sibling;
    }

    this->lowCKey = newKey;

    if(newKey.getRID().slotNum == USHRT_MAX) {
        this->lowCKey.updatePageNum(newKey.getRID().pageNum + 1);
    } else {
        this->lowCKey.updateSlotNum(newKey.getRID().slotNum + 1);
    }

    if(this->highCKey >= newKey) {
        rid = newKey.getRID();
        memcpy((char*)key, (char*)(newKey.getWritableKey()), newKey.getKeyLength() - sizeof(int) - sizeof(RTS));
        return 0;
    }

    return IX_EOF;
}

RC IX_ScanIterator::close() {
    CompositeKey nullKey;
    this->lastPageNum = INT_MAX;
    this->lowCKey = nullKey;
    this->highCKey = nullKey;
    this->lastRet = 0;
    if(data != NULL) {
        free(data);
    }
    this->ixFileHandle->setChanged();
    return 0;
}

IXFileHandle::IXFileHandle() {
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
    numPages = 0;
    changed = true;
}

IXFileHandle::~IXFileHandle() { }

/**
 * openFile() - opens a given file
 * @argument1 : Name of the file to be opened.
 * 
 * Opens a file, if the file is opened for the first time creates the hidden page,
 * reads the performance counter for the file from hidden/header page.
 *
 * Return : none.
*/
void IXFileHandle::openFile(const std::string& fileName) {
    this->fileName = fileName;
    this->file.open(fileName.c_str(), std::ios::in | std::ios::out);
    if(this->isEmpty()) this->createHiddenPage(fileName);
    this->readCounterFromHiddenPage();
    this->setChanged();
    return;
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
void IXFileHandle::closeRoutine() {
    this->updateCounterInHiddenPage();
    this->file.close();
    return;
}

bool IXFileHandle::isEmpty() { 
    return this->file.peek() == EOF;
}

bool IXFileHandle::isOpen() {
    return this->file.is_open();
}

/**
 * createHiddenPage() - creates the header pages in a file.
 * @argument1 : Name of the file in which the header page is create.
 * 
 * Return : 0 on success.
*/
RC IXFileHandle::createHiddenPage(const std::string& fileName) {
    std::fstream newFile;
    newFile.open(fileName.c_str(), std::ios::in | std::ios::out);
    newFile.seekp(0, std::ios_base::end);
    void* data = malloc(MAX_HIDDEN_IX_PAGES*PAGE_SIZE);

    int counter = 0;
    int pageNum = 0;

    memcpy((char*)data, (char*)&counter, sizeof(int));
    memcpy((char*)data + sizeof(int), (char*)&counter, sizeof(int));

    counter = 1;
    memcpy((char*)data + 2*sizeof(int), (char*)&counter, sizeof(int));
    memcpy((char*)data + 3*sizeof(int), (char*)&pageNum, sizeof(int));

    int rootDef = INT_MAX, rootTypeDef = INT_MAX;
    memcpy((char*)data + 4*sizeof(int), (char*)&rootDef, sizeof(int));
    memcpy((char*)data + 5*sizeof(int), (char*)&rootTypeDef, sizeof(int));

    newFile.write((char*)data, MAX_HIDDEN_IX_PAGES*PAGE_SIZE);
    newFile.close();
    free(data);

    return 0;
}

/**
 * updateCounterInHiddenPage() - updates performance counter to hidden/header page before file close.
 *
 *  updated Hidden page counters, root, and other attributes.
 * 
 * Return : 0 on success.
*/
RC IXFileHandle::updateCounterInHiddenPage() {
    this->ixWritePageCounter++;
    memcpy((char*)(this->hiddenData), (char*)&(this->ixReadPageCounter), sizeof(int));
    memcpy((char*)(this->hiddenData) + sizeof(int), (char*)&(this->ixWritePageCounter), sizeof(int));
    memcpy((char*)(this->hiddenData) + 2*sizeof(int), (char*)&(this->ixAppendPageCounter), sizeof(int));
    memcpy((char*)(this->hiddenData) + 3*sizeof(int), (char*)&(this->numPages), sizeof(int));
    memcpy((char*)(this->hiddenData) + 4*sizeof(int), (char*)&(this->root), sizeof(int));
    memcpy((char*)(this->hiddenData) + 5*sizeof(int), (char*)&(this->rootType), sizeof(int));

    file.seekp(0);
    file.write((char*)(this->hiddenData), MAX_HIDDEN_IX_PAGES*PAGE_SIZE);

    free(this->hiddenData);
    this->hiddenData = NULL;
    return 0;
}

/**
 * readCounterFromHiddenPage() - reads the performance counter values from 
 *                                hidden/header page upon file open.
 * 
 * Return : 0 on success.
*/
RC IXFileHandle::readCounterFromHiddenPage() {
    this->hiddenData = malloc(MAX_HIDDEN_IX_PAGES*PAGE_SIZE);
    this->file.seekg(0);
    this->file.read((char*)hiddenData, MAX_HIDDEN_IX_PAGES*PAGE_SIZE);

    memcpy((char*)&(this->ixReadPageCounter), (char*)(this->hiddenData), sizeof(int));
    memcpy((char*)&(this->ixWritePageCounter), (char*)(this->hiddenData) + sizeof(int), sizeof(int));
    memcpy((char*)&(this->ixAppendPageCounter), (char*)(this->hiddenData) + 2*sizeof(int), sizeof(int));
    memcpy((char*)&(this->numPages), (char*)(this->hiddenData) + 3*sizeof(int), sizeof(int));
    memcpy((char*)&root, (char*)(this->hiddenData) + 4*sizeof(int), sizeof(int));
    memcpy((char*)&(this->rootType), (char*)(this->hiddenData) + 5*sizeof(int), sizeof(int));

    this->ixReadPageCounter++;

    return 0;
}

/**
 * readPage() - reads a given page into buffer.
 * @argument1 : page number to be read.
 * @argument2 : buffer in which to read.
 * 
 * Return : 0 on success, -1 on failure.
*/
RC IXFileHandle::readPage(PageNum pageNum, void *data) {
    if(pageNum >= this->getNumberOfPages() || pageNum < 0) {
        return -1;
    }
    if(!file.is_open()) return -1;

    this->ixReadPageCounter++;
    pageNum += MAX_HIDDEN_IX_PAGES;
    file.seekg(pageNum*PAGE_SIZE, std::ios_base::beg);
    file.read((char*)data, PAGE_SIZE);
    return 0;
}

/**
 * writePage() - writes a given page into cache
 * @argument1 : page number to be written.
 * @argument2 : buffer to be written.
 * 
 * Return : 0 on success, -1 on failure.
*/
RC IXFileHandle::writePage(PageNum pageNum, const void *data) {
    if(pageNum >= this->getNumberOfPages()) {
        return -1;
    }

    if(!file.is_open()) return -1;
    this->ixWritePageCounter++;
    pageNum += MAX_HIDDEN_IX_PAGES;
    file.seekp(pageNum*PAGE_SIZE, std::ios_base::beg);
    file.write((char*)data, PAGE_SIZE);

    return 0;
}

/**
 * appendPage() - Appends a new page to the file.
 * @argument1 : buffer of null data to be appended to file.
 * 
 * Neeed to call initPageDirectory() before calling this.
 * Return : 0 on success, -1 on failure.
*/
RC IXFileHandle::appendPage(const void *data) {
    if(!file.is_open()) {
        return -1;
    }
    file.seekp(0,std::ios_base::end);
    file.write((char*)data,PAGE_SIZE);
    file.seekp(0,std::ios_base::beg);
    this->ixAppendPageCounter++;
    this->numPages++;
    return 0;
}

/**
 * initPageDirectory() - initialize the basic page structure of RBFM file.
 *
 * calling this function before appenddPage
 * 
 * Return : page (*data) with directory initialized to default values.
*/
RC IXFileHandle::initPageDirectory(void* data, RT type) {
    RTS freeSpace  = 0;
    RTS entries = 0, offset = 0;
    int pageNum = getNumberOfPages();

    if(type == LEAF) {
        freeSpace = PAGE_SIZE - 4*sizeof(RTS) - 2*sizeof(int);
        int sibling = INT_MAX;
        memcpy((char*)data + PAGE_SIZE - 4*sizeof(RTS) - 2*sizeof(int), (char*)&sibling, sizeof(int));
    } else {
        freeSpace = PAGE_SIZE - 4*sizeof(RTS) - sizeof(int);
    }

    memcpy((char*)data + PAGE_SIZE - sizeof(RTS), (char*)&freeSpace, sizeof(RTS));
    memcpy((char*)data + PAGE_SIZE - 2*sizeof(RTS), (char*)&entries, sizeof(RTS));
    memcpy((char*)data + PAGE_SIZE - 3*sizeof(RTS), (char*)&type, sizeof(RTS));
    memcpy((char*)data + PAGE_SIZE - 4*sizeof(RTS), (char*)&offset, sizeof(RTS));
    memcpy((char*)data + PAGE_SIZE - 4*sizeof(RTS) - sizeof(int), (char*)&pageNum, sizeof(int));

    return 0;
}

/**
 * getNodeType() - get the type of a node (leaf/internal).
 * @argument1 : buffer conatining node data. 
 *
 * Return : the type of the node.
*/
RTS IXFileHandle::getNodeType(void* data) {
    RTS type = 0;
    memcpy((char*)&type, (char*)data + PAGE_SIZE - 3*sizeof(RTS), sizeof(RTS));
    return type;
}

/**
 * getNumberOfPages() - Gives the number of pages of a file minus the header page.
 * 
 * Return : number of pages.
*/
int IXFileHandle::getNumberOfPages() {
    return this->numPages;
}

/**
 * getRoot() - get the root of the BTree.
 *
 * Return : root of the BTree.
*/
int IXFileHandle::getRoot() {
    return this->root;
}

/**
 * setRoot() - set the new root of the BTree.
 * @argument1 : page number of the root.
 *
 * Return : void.
*/
void IXFileHandle::setRoot(const int newRoot) {
    this->root = newRoot;
}

/**
 * collectCounterValues() - read performance counters.
 * @argument1 : value of number of read disk I/O.
 * @argument2 : value of number of write disk I/O.
 * @argument3 : value of number of pages appended in the disk.
 * 
 * Return : above 3 arguments passed by reference.
*/
RC IXFileHandle::collectCounterValues(unsigned &readPageCount, 
                                      unsigned &writePageCount, 
                                      unsigned &appendPageCount) {

    readPageCount = ixReadPageCounter;
    writePageCount = ixWritePageCounter;
    appendPageCount = ixAppendPageCounter;
    return 0;
}

