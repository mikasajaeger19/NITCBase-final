#include "AttrCacheTable.h"
#include<iostream>
#include <cstring>
#include<stdio.h>
AttrCacheEntry* AttrCacheTable::attrCache[MAX_OPEN];

/* returns the attrOffset-th attribute for the relation corresponding to relId
NOTE: this function expects the caller to allocate memory for `*attrCatBuf`
*/
int AttrCacheTable::getAttrCatEntry(int relId, char attrName[ATTR_SIZE], AttrCatEntry* attrCatBuf) {
  // check if 0 <= relId < MAX_OPEN and return E_OUTOFBOUND otherwise
  if(relId<0 || relId>=MAX_OPEN)
  	return E_OUTOFBOUND;
  // check if attrCache[relId] == nullptr and return E_RELNOTOPEN if true
  if(attrCache[relId] == nullptr)
  	return E_RELNOTOPEN;
  // traverse the linked list of attribute cache entries
  AttrCacheEntry* entry;
  for (entry = attrCache[relId]; entry != nullptr; entry = entry->next) {
    //if (entry->attrCatEntry.offset == attrOffset)
    if(!strcmp(attrName,entry->attrCatEntry.attrName)) {
	    *attrCatBuf= entry->attrCatEntry;
      break;
	//return SUCCESS;
      // copy entry->attrCatEntry to *attrCatBuf and return SUCCESS;
    }
  }
  if(entry!= nullptr){
    //printf("found\n");
  	return SUCCESS;
  }
  // there is no attribute at this offset
  //printf("not found\n");
  return E_ATTRNOTEXIST;
}

int AttrCacheTable::getAttrCatEntry(int relId, int attrOffset, AttrCatEntry* attrCatBuf) {
  // check if 0 <= relId < MAX_OPEN and return E_OUTOFBOUND otherwise
  if(relId<0 || relId>=MAX_OPEN)
  	return E_OUTOFBOUND;
  // check if attrCache[relId] == nullptr and return E_RELNOTOPEN if true
  if(attrCache[relId] == nullptr)
  	return E_RELNOTOPEN;
  // traverse the linked list of attribute cache entries
  AttrCacheEntry* entry;
  for (entry = attrCache[relId]; entry != nullptr; entry = entry->next) {
    if (entry->attrCatEntry.offset == attrOffset) {
	    *attrCatBuf= entry->attrCatEntry;
	return SUCCESS;
      // copy entry->attrCatEntry to *attrCatBuf and return SUCCESS;
    }
  }
  if(entry!= nullptr)
  	return SUCCESS;
  // there is no attribute at this offset
  return E_ATTRNOTEXIST;
}

int AttrCacheTable::setAttrCatEntry(int relId, char attrName[ATTR_SIZE], AttrCatEntry *attrCatBuf) {

  if(relId < 0 || relId >= MAX_OPEN) {
    return E_OUTOFBOUND;
  }

  if(attrCache[relId] == nullptr) {
    return E_RELNOTOPEN;
  }

  AttrCacheEntry* entry;

  for(entry = attrCache[relId]; entry != nullptr; entry = entry->next)
  {
    if(!strcmp(attrName, entry->attrCatEntry.attrName))
    {
      // copy the attrCatBuf to the corresponding Attribute Catalog entry in
      // the Attribute Cache Table.
      entry->attrCatEntry = *attrCatBuf;
      entry->dirty = true;
      // set the dirty flag of the corresponding Attribute Cache entry in the
      // Attribute Cache Table.

      return SUCCESS;
    }
  }

  return E_ATTRNOTEXIST;
}

int AttrCacheTable::setAttrCatEntry(int relId, int attrOffset, AttrCatEntry *attrCatBuf) {

  if(relId < 0 || relId >= MAX_OPEN) {
    return E_OUTOFBOUND;
  }

  if(attrCache[relId] == nullptr) {
    return E_RELNOTOPEN;
  }

  AttrCacheEntry* entry;
  
  for(entry = attrCache[relId]; entry != nullptr; entry = entry->next)
  {
    if(entry->attrCatEntry.offset == attrOffset)
    {
      // copy the attrCatBuf to the corresponding Attribute Catalog entry in
      // the Attribute Cache Table.
      entry->attrCatEntry = *attrCatBuf;
      entry->dirty = true;

      // set the dirty flag of the corresponding Attribute Cache entry in the
      // Attribute Cache Table.

      return SUCCESS;
    }
  }

  return E_ATTRNOTEXIST;
}

/* Converts a attribute catalog record to AttrCatEntry struct
    We get the record as Attribute[] from the BlockBuffer.getRecord() function.
    This function will convert that to a struct AttrCatEntry type.
*/
void AttrCacheTable::recordToAttrCatEntry(union Attribute record[ATTRCAT_NO_ATTRS],
                                          AttrCatEntry* attrCatEntry) {
  strcpy(attrCatEntry->relName, record[ATTRCAT_REL_NAME_INDEX].sVal);
  strcpy(attrCatEntry->attrName, record[ATTRCAT_ATTR_NAME_INDEX].sVal);
  attrCatEntry->attrType= (int)record[ATTRCAT_ATTR_TYPE_INDEX].nVal;
  //std::cout<<attrCatEntry->attrType<<'\n'
  //attribute has usually only number or string val, bool conversion???
  attrCatEntry->primaryFlag= (bool)record[ATTRCAT_PRIMARY_FLAG_INDEX].nVal;
  attrCatEntry->rootBlock= (int)record[ATTRCAT_ROOT_BLOCK_INDEX].nVal;
  attrCatEntry->offset= (int)record[ATTRCAT_OFFSET_INDEX].nVal;
  // copy the rest of the fields in the record to the attrCacheEntry struct
}

void AttrCacheTable::attrCatEntryToRecord(AttrCatEntry* attrCatEntry,
                                          union Attribute record[ATTRCAT_NO_ATTRS]) {
  strcpy(record[ATTRCAT_REL_NAME_INDEX].sVal, attrCatEntry->relName);
  strcpy(record[ATTRCAT_ATTR_NAME_INDEX].sVal, attrCatEntry->attrName);
  record[ATTRCAT_ATTR_TYPE_INDEX].nVal= attrCatEntry->attrType;
  record[ATTRCAT_PRIMARY_FLAG_INDEX].nVal= attrCatEntry->primaryFlag;
  record[ATTRCAT_ROOT_BLOCK_INDEX].nVal= attrCatEntry->rootBlock;
  record[ATTRCAT_OFFSET_INDEX].nVal= attrCatEntry->offset;
  // copy the rest of the fields in the attrCacheEntry struct to the record
}

int AttrCacheTable::getSearchIndex(int relId, int offset, IndexId* searchIndex) {
  // check if 0 <= relId < MAX_OPEN and return E_OUTOFBOUND otherwise
  
  if(relId<0 || relId>= MAX_OPEN)
  	return E_OUTOFBOUND;

  if(attrCache[relId] == nullptr)
  	return E_RELNOTOPEN;
  // check if relCache[relId] == nullptr and return E_RELNOTOPEN if true

  
  // copy the searchIndex field of the Relation Cache entry corresponding
  //   to input relId to the searchIndex variable.
  AttrCacheEntry* search= attrCache[relId];
  while(offset>0){
  	search= search->next;
  	offset--;
  }

  *searchIndex = search->searchIndex;
  
  return SUCCESS;
}

int AttrCacheTable::getSearchIndex(int relId, char *attrname, IndexId* searchIndex) {
  // check if 0 <= relId < MAX_OPEN and return E_OUTOFBOUND otherwise
  
  if(relId<0 || relId>= MAX_OPEN)
  	return E_OUTOFBOUND;

  if(attrCache[relId] == nullptr)
  	return E_RELNOTOPEN;
  // check if relCache[relId] == nullptr and return E_RELNOTOPEN if true

  
  // copy the searchIndex field of the Relation Cache entry corresponding
  //   to input relId to the searchIndex variable.
  AttrCacheEntry* search= attrCache[relId];
  while(strcmp(attrname, search->attrCatEntry.attrName)){
  	search= search->next;
  }

  *searchIndex = search->searchIndex;
  
  return SUCCESS;
}

// sets the searchIndex for the relation corresponding to relId
int AttrCacheTable::setSearchIndex(int relId, int offset, IndexId* searchIndex) {
  // check if 0 <= relId < MAX_OPEN and return E_OUTOFBOUND otherwise
  
  if(relId<0 || relId>= MAX_OPEN)
  	return E_OUTOFBOUND;

  if(attrCache[relId] == nullptr)
  	return E_RELNOTOPEN;
  // check if relCache[relId] == nullptr and return E_RELNOTOPEN if true

  
  // copy the searchIndex field of the Relation Cache entry corresponding
  //   to input relId to the searchIndex variable.
  AttrCacheEntry* search= attrCache[relId];
  while(offset>0){
  	search= search->next;
  	offset--;
  }

  search->searchIndex = *searchIndex;
  
  return SUCCESS;
}

int AttrCacheTable::setSearchIndex(int relId, char *attrname, IndexId* searchIndex) {
  // check if 0 <= relId < MAX_OPEN and return E_OUTOFBOUND otherwise
  
  if(relId<0 || relId>= MAX_OPEN)
  	return E_OUTOFBOUND;

  if(attrCache[relId] == nullptr)
  	return E_RELNOTOPEN;
  // check if relCache[relId] == nullptr and return E_RELNOTOPEN if true

  
  // copy the searchIndex field of the Relation Cache entry corresponding
  //   to input relId to the searchIndex variable.
  AttrCacheEntry* search= attrCache[relId];
  while(strcmp(attrname, search->attrCatEntry.attrName)){
  	search= search->next;
  }

  search->searchIndex = *searchIndex;
  
  return SUCCESS;
}

int AttrCacheTable::resetSearchIndex(int relId, int offset) {
  // use setSearchIndex to set the search index to {-1, -1}
  
	if (relId < 0 || relId >= MAX_OPEN)
		return E_OUTOFBOUND;

	// check if relCache[relId] == nullptr and return E_RELNOTOPEN if true
	if (AttrCacheTable::attrCache[relId] == nullptr)
		return E_RELNOTOPEN;
		
  AttrCacheEntry* search= attrCache[relId];
  while(offset>0){
  	search= search->next;
  	offset--;
  }
	// use setSearchIndex to set the search index to {-1, -1}
	AttrCacheTable::attrCache[relId]->searchIndex = {-1, -1};
	return SUCCESS;
  
}

int AttrCacheTable::resetSearchIndex(int relId, char *attrname) {
  // use setSearchIndex to set the search index to {-1, -1}
  
	if (relId < 0 || relId >= MAX_OPEN)
		return E_OUTOFBOUND;

	// check if relCache[relId] == nullptr and return E_RELNOTOPEN if true
	if (AttrCacheTable::attrCache[relId] == nullptr)
		return E_RELNOTOPEN;
		
  AttrCacheEntry* search= attrCache[relId];
  while(strcmp(attrname, search->attrCatEntry.attrName)){
  	search= search->next;
  }
	// use setSearchIndex to set the search index to {-1, -1}
	AttrCacheTable::attrCache[relId]->searchIndex = {-1, -1};
	return SUCCESS;
  
}