#include "BPlusTree.h"
#include <stdio.h>
#include <cstring>

// RecId BPlusTree::bPlusSearch(int relId, char attrName[ATTR_SIZE], Attribute attrVal, int op) {
//     // declare searchIndex which will be used to store search index for attrName.
//     IndexId searchIndex;
//     int ret = AttrCacheTable::getSearchIndex(relId, attrName, &searchIndex);

//     AttrCatEntry attrCatEntry;
//     AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

//     int block = -1, index = -1;

//     if (searchIndex.block == -1 && searchIndex.index == -1) {
//         // searchIndex is {-1, -1} which means that the search is starting
//         block = attrCatEntry.rootBlock;
//         index = 0;

//         if (block == -1) {
//             return RecId{-1, -1};
//         }

//     } else {
//         /*a valid searchIndex points to an entry in the leaf index of the attribute's
//         B+ Tree which had previously satisfied the op for the given attrVal.*/

//         block = searchIndex.block;
//         index = searchIndex.index + 1;  // search is resumed from the next index.

//         // load block into leaf using IndLeaf::IndLeaf().
//         IndLeaf leaf(block);

//         // declare leafHead which will be used to hold the header of leaf.
//         HeadInfo leafHead;
//         leaf.getHeader(&leafHead);
//         // load header into leafHead using BlockBuffer::getHeader().

//         if (index >= leafHead.numEntries) {
//             /* (all the entries in the block has been searched; search from the
//             beginning of the next leaf index block. */

//             // update block to rblock of current block and index to 0.
//             block = leafHead.rblock;
//             index = 0;
//             if (block == -1) {
//                 // (end of linked list reached - the search is done.)
//                 return RecId{-1, -1};
//             }
//         }
//     }

//     /******  Traverse through all the internal nodes according to value
//              of attrVal and the operator op                             ******/

//     /* (This section is only needed when
//         - search restarts from the root block (when searchIndex is reset by caller)
//         - root is not a leaf
//         If there was a valid search index, then we are already at a leaf block
//         and the test condition in the following loop will fail)
//     */

//     while(StaticBuffer::getStaticBlockType(block) == IND_INTERNAL) {  //use StaticBuffer::getStaticBlockType()

//         // load the block into internalBlk using IndInternal::IndInternal().
//         IndInternal internalBlk(block);

//         HeadInfo intHead;
//         internalBlk.getHeader(&intHead);
//         // load the header of internalBlk into intHead using BlockBuffer::getHeader()

//         // declare intEntry which will be used to store an entry of internalBlk.
//         InternalEntry intEntry;

//         if (op == NE || op == LT || op == LE) {
//             /*
//             - NE: need to search the entire linked list of leaf indices of the B+ Tree,
//             starting from the leftmost leaf index. Thus, always move to the left.

//             - LT and LE: the attribute values are arranged in ascending order in the
//             leaf indices of the B+ Tree. Values that satisfy these conditions, if
//             any exist, will always be found in the left-most leaf index. Thus,
//             always move to the left.
//             */

//             // load entry in the first slot of the block into intEntry
//             // using IndInternal::getEntry().
//             internalBlk.getEntry(&intEntry,0);
//             block = intEntry.lChild;

//         } else {
//             /*
//             - EQ, GT and GE: move to the left child of the first entry that is
//             greater than (or equal to) attrVal
//             (we are trying to find the first entry that satisfies the condition.
//             since the values are in ascending order we move to the left child which
//             might contain more entries that satisfy the condition)
//             */

//             /*
//              traverse through all entries of internalBlk and find an entry that
//              satisfies the condition.
//              if op == EQ or GE, then intEntry.attrVal >= attrVal
//              if op == GT, then intEntry.attrVal > attrVal
//              Hint: the helper function compareAttrs() can be used for comparing
//             */
//            int intIndex = 0;
//            while(intIndex < intHead.numEntries){
//                internalBlk.getEntry(&intEntry,intIndex);
//                int cmpVal = compareAttrs(attrVal,intEntry.attrVal,attrCatEntry.attrType);
//                if(cmpVal < 0)
//                 break;
//                else{
//                    intIndex++;
//                }
//            }

//             if (intIndex != intHead.numEntries) {
//                 // move to the left child of that entry
//                 block =  intEntry.lChild;// left child of the entry

//             } else {
//                 // move to the right child of the last entry of the block
//                 // i.e numEntries - 1 th entry of the block
//                 block =  intEntry.rChild;// right child of last entry
//             }
//         }
//     }

//     // NOTE: `block` now has the block number of a leaf index block.

//     /******  Identify the first leaf index entry from the current position
//                 that satisfies our condition (moving right)             ******/

//     while (block != -1) {
//         // load the block into leafBlk using IndLeaf::IndLeaf().
//         IndLeaf leafBlk(block);
//         HeadInfo leafHead;

//         // load the header to leafHead using BlockBuffer::getHeader().
//         leafBlk.getHeader(&leafHead);
//         // declare leafEntry which will be used to store an entry from leafBlk
//         Index leafEntry;
//         int leafIndex = 0;
//         while (leafIndex < leafHead.numEntries) {

//             // load entry corresponding to block and index into leafEntry
//             // using IndLeaf::getEntry().
//             leafBlk.getEntry(&leafEntry,leafIndex);
//             int cmpVal = compareAttrs(leafEntry.attrVal, attrVal, attrCatEntry.attrType);

//             if (
//                 (op == EQ && cmpVal == 0) ||
//                 (op == LE && cmpVal <= 0) ||
//                 (op == LT && cmpVal < 0) ||
//                 (op == GT && cmpVal > 0) ||
//                 (op == GE && cmpVal >= 0) ||
//                 (op == NE && cmpVal != 0)
//             ) {
//                 // (entry satisfying the condition found)
//                 IndexId newSearchIndex = {block, leafIndex};
//                 AttrCacheTable::setSearchIndex(relId, attrName, &newSearchIndex);
//                 // set search index to {block, index}

//                 // return the recId {leafEntry.block, leafEntry.slot}.
//                 return RecId{leafEntry.block, leafEntry.slot};

//             } else if ((op == EQ || op == LE || op == LT) && cmpVal > 0) {
//                 /*future entries will not satisfy EQ, LE, LT since the values
//                     are arranged in ascending order in the leaves */

//                 return RecId {-1, -1};
//             }

//             // search next index.
//             ++leafIndex;
//         }

//         /*only for NE operation do we have to check the entire linked list;
//         for all the other op it is guaranteed that the block being searched
//         will have an entry, if it exists, satisying that op. */
//         if (op != NE) {
//             break;
//         }

//         // block = next block in the linked list, i.e., the rblock in leafHead.
//         // update index to 0.
//         block = leafHead.rblock;
//         index = 0;
//     }
//     return RecId{-1, -1};
//     // no entry satisying the op was found; return the recId {-1,-1}
// }


RecId BPlusTree::bPlusSearch(int relId, char attrName[ATTR_SIZE], 
                                Attribute attrVal, int op) 
{
    // declare searchIndex which will be used to store search index for attrName.
    IndexId searchIndex;

    // get the search index corresponding to attribute with name attrName
    // using AttrCacheTable::getSearchIndex().
    int ret = AttrCacheTable::getSearchIndex(relId, attrName, &searchIndex);

    AttrCatEntry attrCatEntry;
    // load the attribute cache entry into attrCatEntry using
    // AttrCacheTable::getAttrCatEntry().
    ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    // declare variables block and index which will be used during search
    int block = -1, index = -1;

    if (searchIndex.block == -1 && searchIndex.index == -1) // (search is done for the first time)
    {
        // start the search from the first entry of root.
        block = attrCatEntry.rootBlock;
        index = 0;

        if (block == -1) // attrName doesn't have a B+ tree (block == -1)
            return RecId{-1, -1};

    } 
    else 
    {
        // a valid searchIndex points to an entry in the leaf index of the attribute's
        // B+ Tree which had previously satisfied the op for the given attrVal.

        block = searchIndex.block, index = searchIndex.index + 1;  // search is resumed from the next index.

        // load block into leaf using IndLeaf::IndLeaf().
        IndLeaf leaf(block);

        // declare leafHead which will be used to hold the header of leaf.
        HeadInfo leafHead;

        // load header into leafHead using BlockBuffer::getHeader().
        leaf.getHeader(&leafHead);

        if (index >= leafHead.numEntries) {
            // (all the entries in the block has been searched; search from the
            // beginning of the next leaf index block.

            // update block to rblock of current block and index to 0.
            block = leafHead.rblock, index = 0;

            if (block == -1) {
                // (end of linked list reached - the search is done.)
                return RecId{-1, -1};
            }
        }
    }

    /******  Traverse through all the internal nodes according to value of attrVal and the operator op ******/

    /* 
    * This section is only needed when :
        * search starts from the root block and the root is not a leaf
    * If there was a valid search index, then we are already at a leaf block
    * and the test condition in the following loop will fail
    */

    while(StaticBuffer::getStaticBlockType(block) == IND_INTERNAL) /* block is of type IND_INTERNAL */ // use StaticBuffer::getStaticBlockType()
    {  
        // load the block into internalBlk using IndInternal::IndInternal().
        IndInternal internalBlk(block);

        // load the header of internalBlk into intHead using BlockBuffer::getHeader()
        HeadInfo intHead;
        internalBlk.getHeader(&intHead);

        // declare intEntry which will be used to store an entry of internalBlk.
        InternalEntry intEntry;

        // if (/* op is one of NE, LT, LE */)
        if (op == NE || op == LT || op == LE) 
        {
            /*
            - NE: need to search the entire linked list of leaf indices of the B+ Tree,
            starting from the leftmost leaf index. Thus, always move to the left.

            - LT and LE: the attribute values are arranged in ascending order in the
            leaf indices of the B+ Tree. Values that satisfy these conditions, if
            any exist, will always be found in the left-most leaf index. Thus,
            always move to the left.
            */

            // load entry in the first slot of the block into intEntry
            // using IndInternal::getEntry().
            internalBlk.getEntry(&intEntry, 0);
            block = intEntry.lChild;

        } 
        
        else 
        {
            /*
            - EQ, GT and GE: move to the left child of the first entry that is
            greater than (or equal to) attrVal
            (we are trying to find the first entry that satisfies the condition.
            since the values are in ascending order we move to the left child which
            might contain more entries that satisfy the condition)
            */

            /*
             traverse through all entries of internalBlk and find an entry that
             satisfies the condition.
             if op == EQ or GE, then intEntry.attrVal >= attrVal
             if op == GT, then intEntry.attrVal > attrVal
             Hint: the helper function compareAttrs() can be used for comparing
            */

            int entryindex = 0;
            while (entryindex < intHead.numEntries)
            {
                ret = internalBlk.getEntry(&intEntry, entryindex);
                
                int cmpVal = compareAttrs(intEntry.attrVal, attrVal, attrCatEntry.attrType);
                if (
                    (op == EQ && cmpVal >= 0) ||
                    (op == GE && cmpVal >= 0) ||
                    (op == GT && cmpVal > 0)
                )
                    break;

                entryindex++;
            }

            // if (/* such an entry is found*/) 
            if (entryindex < intHead.numEntries)
            {
                // move to the left child of that entry
                block = intEntry.lChild; // left child of the entry
            }
            else 
            {
                // move to the right child of the last entry of the block
                // i.e numEntries - 1 th entry of the block
                block = intEntry.rChild; // right child of last entry
            }

            // index = 0;
        }
    }

    // NOTE: `block` now has the block number of a leaf index block.

    /******  Identify the first leaf index entry from the current position
                that satisfies our condition (moving right)             ******/

    while (block != -1) {
        // load the block into leafBlk using IndLeaf::IndLeaf().
        IndLeaf leafBlk(block);

        // load the header to leafHead using BlockBuffer::getHeader().
        HeadInfo leafHead;
        leafBlk.getHeader(&leafHead);

        // declare leafEntry which will be used to store an entry from leafBlk
        Index leafEntry;

        // while (/*index < numEntries in leafBlk*/) 
        while (index < leafHead.numEntries)
        {
            // load entry corresponding to block and index into leafEntry
            // using IndLeaf::getEntry().
            leafBlk.getEntry(&leafEntry, index);

            // comparison between leafEntry's attribute value and input attrVal using compareAttrs()
            int cmpVal = compareAttrs(leafEntry.attrVal, attrVal, attrCatEntry.attrType); 

            if (
                (op == EQ && cmpVal == 0) ||
                (op == LE && cmpVal <= 0) ||
                (op == GE && cmpVal >= 0) ||
                (op == LT && cmpVal < 0) ||
                (op == GT && cmpVal > 0) ||
                (op == NE && cmpVal != 0)
            ) {
                // (entry satisfying the condition found)

                // set search index to {block, index}
                searchIndex = IndexId{block, index};
                AttrCacheTable::setSearchIndex(relId, attrName, &searchIndex);

                // return the recId {leafEntry.block, leafEntry.slot}.
                return RecId{leafEntry.block, leafEntry.slot};
            } 
            else if ((op == EQ || op == LE || op == LT) && cmpVal > 0) 
            {
                // future entries will not satisfy EQ, LE, LT since the values
                // are arranged in ascending order in the leaves 

                return RecId {-1, -1};
            }

            // search next index.
            ++index;
        }

        // only for NE operation do we have to check the entire linked list;
        // for all the other op it is guaranteed that the block being searched
        // will have an entry, if it exists, satisying that op. 
        if (op != NE) {
            break;
        }

        // block = next block in the linked list, i.e., the rblock in leafHead.
        // update index to 0.
        block = leafHead.rblock, index = 0;
    }

    // no entry satisying the op was found; return the recId {-1,-1}
    return RecId{-1, -1};
}

int BPlusTree::bPlusCreate(int relId, char attrName[ATTR_SIZE]) {

    if(relId == ATTRCAT_RELID || relId == RELCAT_RELID){
        return E_NOTPERMITTED;
    }

    //printf("BPlus create starting\n");

    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    int attrOffset = attrCatEntry.offset;

    // get the attribute catalog entry of attribute `attrName`
    // using AttrCacheTable::getAttrCatEntry()

    // if getAttrCatEntry fails
    //     return the error code from getAttrCatEntry

    if(ret != SUCCESS)
        return ret;

    //printf("BPlus create middle check\n");

    if (attrCatEntry.rootBlock != -1) {
        return SUCCESS;
    }

    /******Creating a new B+ Tree ******/

    // get a free leaf block using constructor 1 to allocate a new block
    IndLeaf rootBlockBuf;
    // (if the block could not be allocated, the appropriate error code
    //  will be stored in the blockNum member field of the object)

    // declare rootBlock to store the blockNumber of the new leaf block
    int rootBlock = 0;
    rootBlock = rootBlockBuf.getBlockNum();
    //printf("%d \n",rootBlock);
    // if there is no more disk space for creating an index
    if (rootBlock == E_DISKFULL) {
        return E_DISKFULL;
    }

    attrCatEntry.rootBlock = rootBlock;
    AttrCacheTable::setAttrCatEntry(relId, attrOffset, &attrCatEntry);

    //printf("BPlus create middle check 2\n");

    RelCatEntry relCatEntry;

    // load the relation catalog entry into relCatEntry
    // using RelCacheTable::getRelCatEntry().
    RelCacheTable::getRelCatEntry(relId, &relCatEntry);
    int block = relCatEntry.firstBlk;

    /***** Traverse all the blocks in the relation and insert them one
           by one into the B+ Tree *****/
    while (block != -1) {

        // declare a RecBuffer object for `block` (using appropriate constructor)
        RecBuffer recBuffer(block);
        unsigned char slotMap[relCatEntry.numSlotsPerBlk];
        recBuffer.getSlotMap(slotMap);
        // load the slot map into slotMap using RecBuffer::getSlotMap().

        // for every occupied slot of the block
        for(int slotIndex = 0; slotIndex < relCatEntry.numSlotsPerBlk; slotIndex++)
        {
            if(slotMap[slotIndex] == SLOT_OCCUPIED){
                Attribute record[relCatEntry.numAttrs];
                recBuffer.getRecord(record,slotIndex);
                RecId recId{block,slotIndex};
                int retVal = bPlusInsert(relId,attrName,record[attrOffset],recId);
     
                if(retVal == E_DISKFULL){
                    return E_DISKFULL;
                }
            }
            
        }

        //printf("BPlus create end check\n");

        // get the header of the block using BlockBuffer::getHeader()
        HeadInfo headInfo;
        recBuffer.getHeader(&headInfo);
        block = headInfo.rblock;
        // set block = rblock of current block (from the header)
    }

    return SUCCESS;
}

int BPlusTree::bPlusDestroy(int rootBlockNum) {
    if (rootBlockNum < 0 || rootBlockNum >= DISK_BLOCKS) {
        return E_OUTOFBOUND;
    }

    int type = StaticBuffer::getStaticBlockType(rootBlockNum);

    if (type == IND_LEAF) {
        // declare an instance of IndLeaf for rootBlockNum using appropriate
        // constructor
        IndLeaf leafBlock(rootBlockNum);
        leafBlock.releaseBlock();
        // release the block using BlockBuffer::releaseBlock().

        return SUCCESS;

    } else if (type == IND_INTERNAL) {
        // declare an instance of IndInternal for rootBlockNum using appropriate
        // constructor
        IndInternal internalBlk(rootBlockNum);
        // load the header of the block using BlockBuffer::getHeader().
        struct HeadInfo headInfo;
        internalBlk.getHeader(&headInfo);
        for(int entryIndex = 0; entryIndex < headInfo.numEntries; entryIndex++){
            InternalEntry internalEntry;
            internalBlk.getEntry(&internalEntry,entryIndex);
            if(entryIndex == 0)
                BPlusTree::bPlusDestroy(internalEntry.lChild);
            BPlusTree::bPlusDestroy(internalEntry.rChild);
        }
        /*iterate through all the entries of the internalBlk and destroy the lChild
        of the first entry and rChild of all entries using BPlusTree::bPlusDestroy().
        (the rchild of an entry is the same as the lchild of the next entry.
         take care not to delete overlapping children more than once ) */

        // release the block using BlockBuffer::releaseBlock().
        internalBlk.releaseBlock();

        return SUCCESS;

    } else {
        // (block is not an index block.)
        return E_INVALIDBLOCK;
    }
}


int BPlusTree::bPlusInsert(int relId, char attrName[ATTR_SIZE], Attribute attrVal, RecId recId) {
    // get the attribute cache entry corresponding to attrName
    // using AttrCacheTable::getAttrCatEntry().
    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    // if getAttrCatEntry() failed
    //     return the error code

    if(ret !=  SUCCESS)
        return ret;

    int blockNum = attrCatEntry.rootBlock;

    if (blockNum == -1) {
        return E_NOINDEX;
    }


    // find the leaf block to which insertion is to be done using the
    // findLeafToInsert() function

    int leafBlkNum = findLeafToInsert(blockNum, attrVal, attrCatEntry.attrType);
    /* findLeafToInsert(root block num, attrVal, attribute type) */;


    // insert the attrVal and recId to the leaf block at blockNum using the
    // insertIntoLeaf() function.
    // declare a struct Index with attrVal = attrVal, block = recId.block and
    // slot = recId.slot to pass as argument to the function.
    // insertIntoLeaf(relId, attrName, leafBlkNum, Index entry)
    // NOTE: the insertIntoLeaf() function will propagate the insertion to the
    //       required internal nodes by calling the required helper functions
    //       like insertIntoInternal() or createNewRoot()

    struct Index entry;
    entry.attrVal = attrVal;
    entry.block = recId.block;
    
    ret = insertIntoLeaf(relId, attrName, leafBlkNum, entry);

    if (ret == E_DISKFULL) {

        bPlusDestroy(blockNum);
        // destroy the existing B+ tree by passing the rootBlock to bPlusDestroy().
        attrCatEntry.rootBlock = -1;
        AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatEntry);
        // update the rootBlock of attribute catalog cache entry to -1 using
        // AttrCacheTable::setAttrCatEntry().

        return E_DISKFULL;
    }

    return SUCCESS;
}

int BPlusTree::findLeafToInsert(int rootBlock, Attribute attrVal, int attrType) {
    int blockNum = rootBlock;

    while (StaticBuffer::getStaticBlockType(blockNum) != IND_LEAF) {  // use StaticBuffer::getStaticBlockType()

        // declare an IndInternal object for block using appropriate constructor
        IndInternal internalBlk(blockNum);
        struct HeadInfo header;
        internalBlk.getHeader(&header);
        // get header of the block using BlockBuffer::getHeader()

        InternalEntry internalEntry;
        for(int entryIndex = 0; entryIndex < header.numEntries; entryIndex++){
            internalBlk.getEntry(&internalEntry,entryIndex);
            int cmpVal = compareAttrs(internalEntry.attrVal, attrVal, attrType);
            if(cmpVal >= 0){
                blockNum = internalEntry.lChild;
                break;
            }
            else{
                blockNum = internalEntry.rChild;
            }
        }
        /* iterate through all the entries, to find the first entry whose
             attribute value >= value to be inserted.
             NOTE: the helper function compareAttrs() declared in BlockBuffer.h
                   can be used to compare two Attribute values. */

        // if (/*no such entry is found*/) {
        //     // set blockNum = rChild of (nEntries-1)'th entry of the block
        //     // (i.e. rightmost child of the block)

        // } else {
        //     // set blockNum = lChild of the entry that was found
        // }
    }

    return blockNum;
}

int BPlusTree::insertIntoLeaf(int relId, char attrName[ATTR_SIZE], int blockNum, Index indexEntry) {
    // get the attribute cache entry corresponding to attrName
    // using AttrCacheTable::getAttrCatEntry().

    // declare an IndLeaf instance for the block using appropriate constructor
    AttrCatEntry AttrCatEntry;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &AttrCatEntry);

    IndLeaf leafBlock(blockNum);
    HeadInfo blockHeader;
    // store the header of the leaf index block into blockHeader
    // using BlockBuffer::getHeader()
    leafBlock.getHeader(&blockHeader);
    // the following variable will be used to store a list of index entries with
    // existing indices + the new index to insert
    Index indices[blockHeader.numEntries + 1];

    /*
    Iterate through all the entries in the block and copy them to the array indices.
    Also insert `indexEntry` at appropriate position in the indices array maintaining
    the ascending order.
    - use IndLeaf::getEntry() to get the entry
    - use compareAttrs() declared in BlockBuffer.h to compare two Attribute structs
    */
    int newIndex = 0;
    bool insertStatus = false;
    for(int entryIndex = 0; entryIndex < blockHeader.numEntries; entryIndex++, newIndex++){
        leafBlock.getEntry(&indices[newIndex],entryIndex);
        int cmpVal = compareAttrs(indices[entryIndex].attrVal, indexEntry.attrVal, AttrCatEntry.attrType);
        if(cmpVal > 0 && insertStatus == false){
            indices[newIndex] = indexEntry;
            entryIndex--;
            insertStatus = true;
        }
    }
    if (blockHeader.numEntries != MAX_KEYS_LEAF) {
        // (leaf block has not reached max limit)
        blockHeader.numEntries++;
        leafBlock.setHeader(&blockHeader);
        // increment blockHeader.numEntries and update the header of block
        // using BlockBuffer::setHeader().
        for(int entryIndex = 0; entryIndex < blockHeader.numEntries; entryIndex++){
            leafBlock.setEntry(&indices[entryIndex],entryIndex);
        }

        // iterate through all the entries of the array `indices` and populate the
        // entries of block with them using IndLeaf::setEntry().

        return SUCCESS;
    }

    // If we reached here, the `indices` array has more than entries than can fit
    // in a single leaf index block. Therefore, we will need to split the entries
    // in `indices` between two leaf blocks. We do this using the splitLeaf() function.
    // This function will return the blockNum of the newly allocated block or
    // E_DISKFULL if there are no more blocks to be allocated.

    int newRightBlk = splitLeaf(blockNum, indices);

    if(newRightBlk == E_DISKFULL){
        return E_DISKFULL;
    }
    // if splitLeaf() returned E_DISKFULL
    //     return E_DISKFULL

    if (blockHeader.pblock != -1) {  // check pblock in header
        // insert the middle value from `indices` into the parent block using the
        // insertIntoInternal() function. (i.e the last value of the left block)
        InternalEntry internalEntry;
        internalEntry.attrVal = indices[MIDDLE_INDEX_LEAF].attrVal;
        internalEntry.lChild = blockNum;
        internalEntry.rChild = newRightBlk;
        int ret = insertIntoInternal(relId, attrName, blockNum, internalEntry);
        // the middle value will be at index 31 (given by constant MIDDLE_INDEX_LEAF)

        // create a struct InternalEntry with attrVal = indices[MIDDLE_INDEX_LEAF].attrVal,
        // lChild = currentBlock, rChild = newRightBlk and pass it as argument to
        // the insertIntoInternalFunction as follows
        if(ret != SUCCESS)
            return ret;

        // insertIntoInternal(relId, attrName, parent of current block, new internal entry)

    } else {
        int ret = createNewRoot(relId, attrName, indices[MIDDLE_INDEX_LEAF].attrVal, blockNum, newRightBlk);
        // the current block was the root block and is now split. a new internal index
        // block needs to be allocated and made the root of the tree.
        // To do this, call the createNewRoot() function with the following arguments

        // createNewRoot(relId, attrName, indices[MIDDLE_INDEX_LEAF].attrVal,
        //               current block, new right block)
        if(ret != SUCCESS)
            return ret;
    }

    return SUCCESS;
    // if either of the above calls returned an error (E_DISKFULL), then return that
    // else return SUCCESS
}

int BPlusTree::splitLeaf(int leafBlockNum, Index indices[]) {
    // declare rightBlk, an instance of IndLeaf using constructor 1 to obtain new
    // leaf index block that will be used as the right block in the splitting
    
    IndLeaf rightBlk('L');
    //check


    // declare leftBlk, an instance of IndLeaf using constructor 2 to read from
    // the existing leaf block
    IndLeaf leftBlk(leafBlockNum);
    int rightBlkNum = rightBlk.getBlockNum();
    int leftBlkNum = leftBlk.getBlockNum();

    if (rightBlkNum == E_DISKFULL) {
        return E_DISKFULL;
    }

    HeadInfo leftBlkHeader, rightBlkHeader;
    // get the headers of left block and right block using BlockBuffer::getHeader()
    leftBlk.getHeader(&leftBlkHeader);
    rightBlk.getHeader(&rightBlkHeader);

    rightBlkHeader.numEntries = (MAX_KEYS_LEAF + 1) / 2;
    rightBlkHeader.pblock = leftBlkHeader.pblock;
    rightBlkHeader.lblock = leftBlkNum;
    rightBlkHeader.rblock = leftBlkHeader.rblock;
    rightBlk.setHeader(&rightBlkHeader);
    // set rightBlkHeader with the following values
    // - number of entries = (MAX_KEYS_LEAF+1)/2 = 32,
    // - pblock = pblock of leftBlk
    // - lblock = leftBlkNum
    // - rblock = rblock of leftBlk
    // and update the header of rightBlk using BlockBuffer::setHeader()

    leftBlkHeader.numEntries = (MAX_KEYS_LEAF + 1)/2;
    leftBlkHeader.rblock = rightBlkNum;
    leftBlk.setHeader(&leftBlkHeader);    
    // set leftBlkHeader with the following values
    // - number of entries = (MAX_KEYS_LEAF+1)/2 = 32
    // - rblock = rightBlkNum
    // and update the header of leftBlk using BlockBuffer::setHeader() */

    for(int entryIndex = 0; entryIndex < leftBlkHeader.numEntries; entryIndex++){
        leftBlk.setEntry(&indices[entryIndex],entryIndex);
    }

    for(int entryIndex = 0; entryIndex < rightBlkHeader.numEntries; entryIndex++){
        rightBlk.setEntry(&indices[entryIndex + rightBlkHeader.numEntries],entryIndex);
    }
    // set the first 32 entries of leftBlk = the first 32 entries of indices array
    // and set the first 32 entries of newRightBlk = the next 32 entries of
    // indices array using IndLeaf::setEntry().

    return rightBlkNum;
}

int BPlusTree::insertIntoInternal(int relId, char attrName[ATTR_SIZE], int intBlockNum, InternalEntry intEntry) {
    // get the attribute cache entry corresponding to attrName
    // using AttrCacheTable::getAttrCatEntry().

    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    IndInternal intBlk(intBlockNum);
    // declare intBlk, an instance of IndInternal using constructor 2 for the block
    // corresponding to intBlockNum

    HeadInfo blockHeader;
    intBlk.getHeader(&blockHeader);
    // load blockHeader with header of intBlk using BlockBuffer::getHeader().

    // declare internalEntries to store all existing entries + the new entry
    InternalEntry internalEntries[blockHeader.numEntries + 1];

    /*
    Iterate through all the entries in the block and copy them to the array
    `internalEntries`. Insert `indexEntry` at appropriate position in the
    array maintaining the ascending order.
        - use IndInternal::getEntry() to get the entry
        - use compareAttrs() to compare two structs of type Attribute

    Update the lChild of the internalEntry immediately following the newly added
    entry to the rChild of the newly added entry.
    */

    int newIndex = 0;
    int insertStatus = 0;
    for(int entryIndex = 0; entryIndex < blockHeader.numEntries; entryIndex++, newIndex++){
        intBlk.getEntry(&internalEntries[newIndex],entryIndex);
        int cmpVal = compareAttrs(internalEntries[entryIndex].attrVal, intEntry.attrVal, attrCatEntry.attrType);
        if(cmpVal > 0 && insertStatus == 0){
            internalEntries[newIndex] = intEntry;
            entryIndex--;
            insertStatus = newIndex;
        }
    }

    internalEntries[insertStatus].lChild = internalEntries[insertStatus - 1].rChild;

    if (blockHeader.numEntries != MAX_KEYS_INTERNAL) {
        // (internal index block has not reached max limit)
        blockHeader.numEntries++;
        intBlk.setHeader(&blockHeader);
        // increment blockheader.numEntries and update the header of intBlk
        // using BlockBuffer::setHeader().
        for(int entryIndex = 0; entryIndex < blockHeader.numEntries; entryIndex++){
            intBlk.setEntry(&internalEntries[entryIndex],entryIndex);
        }
        // iterate through all entries in internalEntries array and populate the
        // entries of intBlk with them using IndInternal::setEntry().

        return SUCCESS;
    }

    // If we reached here, the `internalEntries` array has more than entries than
    // can fit in a single internal index block. Therefore, we will need to split
    // the entries in `internalEntries` between two internal index blocks. We do
    // this using the splitInternal() function.
    // This function will return the blockNum of the newly allocated block or
    // E_DISKFULL if there are no more blocks to be allocated.

    int newRightBlk = splitInternal(intBlockNum, internalEntries);

    if (newRightBlk == E_DISKFULL) {

        // Using bPlusDestroy(), destroy the right subtree, rooted at intEntry.rChild.
        // This corresponds to the tree built up till now that has not yet been
        // connected to the existing B+ Tree
        bPlusDestroy(intEntry.rChild);

        return E_DISKFULL;
    }

    if (blockHeader.pblock != -1) {  // (check pblock in header)
        // insert the middle value from `internalEntries` into the parent block
        // using the insertIntoInternal() function (recursively).
        // the middle value will be at index 50 (given by constant MIDDLE_INDEX_INTERNAL)
        InternalEntry internalEntry;
        internalEntry.attrVal = internalEntries[MIDDLE_INDEX_INTERNAL].attrVal;
        internalEntry.lChild = intBlockNum;
        internalEntry.rChild = newRightBlk;
        int ret = insertIntoInternal(relId, attrName, blockHeader.pblock, internalEntry);
        
        if(ret != SUCCESS)
            return ret;
        // create a struct InternalEntry with lChild = current block, rChild = newRightBlk
        // and attrVal = internalEntries[MIDDLE_INDEX_INTERNAL].attrVal
        // and pass it as argument to the insertIntoInternalFunction as follows

        // insertIntoInternal(relId, attrName, parent of current block, new internal entry)

    } else {
        int ret = createNewRoot(relId, attrName, internalEntries[MIDDLE_INDEX_INTERNAL].attrVal, intBlockNum, newRightBlk);
        if(ret != SUCCESS)
            return ret;
        // the current block was the root block and is now split. a new internal index
        // block needs to be allocated and made the root of the tree.
        // To do this, call the createNewRoot() function with the following arguments

        // createNewRoot(relId, attrName,
        //               internalEntries[MIDDLE_INDEX_INTERNAL].attrVal,
        //               current block, new right block)
    }

    return SUCCESS;
    // if either of the above calls returned an error (E_DISKFULL), then return that
    // else return SUCCESS
}

int BPlusTree::splitInternal(int intBlockNum, InternalEntry internalEntries[]) {
    // declare rightBlk, an instance of IndInternal using constructor 1 to obtain new
    // internal index block that will be used as the right block in the splitting

    IndInternal rightBlk('I');
    // declare leftBlk, an instance of IndInternal using constructor 2 to read from
    // the existing internal index block

    IndInternal leftBlk(intBlockNum);

    int rightBlkNum = rightBlk.getBlockNum();
    int leftBlkNum = leftBlk.getBlockNum();

    if (rightBlkNum == E_DISKFULL) {
        //(failed to obtain a new internal index block because the disk is full)
        return E_DISKFULL;
    }

    HeadInfo leftBlkHeader, rightBlkHeader;
    // get the headers of left block and right block using BlockBuffer::getHeader();

    leftBlk.getHeader(&leftBlkHeader);
    rightBlk.getHeader(&rightBlkHeader);

    rightBlkHeader.numEntries = (MAX_KEYS_INTERNAL) / 2;
    rightBlkHeader.pblock = leftBlkHeader.pblock;
    rightBlk.setHeader(&rightBlkHeader);

    leftBlkHeader.numEntries = (MAX_KEYS_INTERNAL) / 2;
    leftBlkHeader.rblock = rightBlkNum;
    leftBlk.setHeader(&leftBlkHeader);

    // set rightBlkHeader with the following values
    // - number of entries = (MAX_KEYS_INTERNAL)/2 = 50
    // - pblock = pblock of leftBlk
    // and update the header of rightBlk using BlockBuffer::setHeader()

    // set leftBlkHeader with the following values
    // - number of entries = (MAX_KEYS_INTERNAL)/2 = 50
    // - rblock = rightBlkNum
    // and update the header using BlockBuffer::setHeader()

    /*
    - set the first 50 entries of leftBlk = index 0 to 49 of internalEntries
      array
    - set the first 50 entries of newRightBlk = entries from index 51 to 100
      of internalEntries array using IndInternal::setEntry().
      (index 50 will be moving to the parent internal index block)
    */

    for(int entryIndex = 0; entryIndex < leftBlkHeader.numEntries; entryIndex++){
        leftBlk.setEntry(&internalEntries[entryIndex],entryIndex);
    }

    for(int entryIndex = 0; entryIndex < rightBlkHeader.numEntries; entryIndex++){
        rightBlk.setEntry(&internalEntries[entryIndex + rightBlkHeader.numEntries],entryIndex);
    }

    int type = StaticBuffer::getStaticBlockType(internalEntries[0].lChild);/* block type of a child of any entry of the internalEntries array */;
    //            (use StaticBuffer::getStaticBlockType())

    for (int entryIndex = 0; entryIndex < rightBlkHeader.numEntries; ++entryIndex) {
        // declare an instance of BlockBuffer to access the child block using
        // constructor 2
        BlockBuffer childBlockLeft(internalEntries[entryIndex + rightBlkHeader.numEntries].lChild);
        BlockBuffer childBlockRight(internalEntries[entryIndex + rightBlkHeader.numEntries].rChild);
        
        HeadInfo leftChildHeader, rightChildHeader;
        
        childBlockLeft.getHeader(&leftChildHeader);
        childBlockRight.getHeader(&rightChildHeader);

        leftChildHeader.pblock = rightBlkNum;
        rightChildHeader.pblock = rightBlkNum;

        childBlockLeft.setHeader(&leftChildHeader);
        childBlockRight.setHeader(&rightChildHeader);

        // update pblock of the block to rightBlkNum using BlockBuffer::getHeader()
        // and BlockBuffer::setHeader().
    }

    return rightBlkNum;
}

int BPlusTree::createNewRoot(int relId, char attrName[ATTR_SIZE], Attribute attrVal, int lChild, int rChild) {
    // get the attribute cache entry corresponding to attrName
    // using AttrCacheTable::getAttrCatEntry().

    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    // declare newRootBlk, an instance of IndInternal using appropriate constructor
    // to allocate a new internal index block on the disk

    IndInternal newRootBlk('I');

    int newRootBlkNum = newRootBlk.getBlockNum();

    if (newRootBlkNum == E_DISKFULL) {
        // (failed to obtain an empty internal index block because the disk is full)
        bPlusDestroy(rChild);
        // Using bPlusDestroy(), destroy the right subtree, rooted at rChild.
        // This corresponds to the tree built up till now that has not yet been
        // connected to the existing B+ Tree

        return E_DISKFULL;
    }

    // update the header of the new block with numEntries = 1 using
    // BlockBuffer::getHeader() and BlockBuffer::setHeader()
    HeadInfo newRootBlkHeader;
    newRootBlk.getHeader(&newRootBlkHeader);
    newRootBlkHeader.numEntries = 1;
    newRootBlk.setHeader(&newRootBlkHeader);

    // create a struct InternalEntry with lChild, attrVal and rChild from the
    // arguments and set it as the first entry in newRootBlk using IndInternal::setEntry()

    InternalEntry internalEntry;
    internalEntry.lChild = lChild;
    internalEntry.attrVal = attrVal;
    internalEntry.rChild = rChild;
    newRootBlk.setEntry(&internalEntry,0);

    // declare BlockBuffer instances for the `lChild` and `rChild` blocks using
    // appropriate constructor and update the pblock of those blocks to `newRootBlkNum`
    // using BlockBuffer::getHeader() and BlockBuffer::setHeader()

    BlockBuffer lChildBlock(lChild);
    BlockBuffer rChildBlock(rChild);

    HeadInfo lChildHeader, rChildHeader;

    lChildBlock.getHeader(&lChildHeader);
    rChildBlock.getHeader(&rChildHeader);

    lChildHeader.pblock = newRootBlkNum;
    rChildHeader.pblock = newRootBlkNum;

    lChildBlock.setHeader(&lChildHeader);
    rChildBlock.setHeader(&rChildHeader);

    // update rootBlock = newRootBlkNum for the entry corresponding to `attrName`
    // in the attribute cache using AttrCacheTable::setAttrCatEntry().
    attrCatEntry.rootBlock = newRootBlkNum;
    AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatEntry);

    return SUCCESS;
}