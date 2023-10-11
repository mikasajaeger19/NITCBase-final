#include "Schema.h"
#include <iostream>
#include <cmath>
#include <cstring>
int Schema::openRel(char relName[ATTR_SIZE]) {
  int ret = OpenRelTable::openRel(relName);

  // the OpenRelTable::openRel() function returns the rel-id if successful
  // a valid rel-id will be within the range 0 <= relId < MAX_OPEN and any
  // error codes will be negative
  if(ret >= 0){
    return SUCCESS;
  }

  //otherwise it returns an error message
  return ret;
}

int Schema::closeRel(char relName[ATTR_SIZE]) {
  if (strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0) {
    return E_NOTPERMITTED;
  }

  // this function returns the rel-id of a relation if it is open or
  // E_RELNOTOPEN if it is not. we will implement this later.
  int relId = OpenRelTable::getRelId(relName);

  if (relId < 0) {
    return E_RELNOTOPEN;
  }
  //check later

  return OpenRelTable::closeRel(relId);
}

int Schema::renameRel(char oldRelName[ATTR_SIZE], char newRelName[ATTR_SIZE]) {

  // We check if the oldRelName is RELCAT_RELNAME or ATTRCAT_RELNAME and return E_NOTPERMITTED if true.
  // We check if the relation with name oldRelName is open and return E_RELNOTOPEN if not.
  // We call BlockAccess::renameRelation(oldRelName, newRelName) and return the return value of that function.

    if(strcmp(oldRelName,RELCAT_RELNAME) == 0 || strcmp(oldRelName,ATTRCAT_RELNAME) == 0)
        return E_NOTPERMITTED;
    
    if(strcmp(newRelName,RELCAT_RELNAME) == 0 || strcmp(newRelName,ATTRCAT_RELNAME) == 0)
        return E_NOTPERMITTED;

    if(OpenRelTable::getRelId(oldRelName) != E_RELNOTOPEN)
        return E_RELOPEN;
    
    int retVal = BlockAccess::renameRelation(oldRelName,newRelName);
    return retVal;
}

int Schema::renameAttr(char relName[ATTR_SIZE], char oldAttrName[ATTR_SIZE], char newAttrName[ATTR_SIZE]) {
  // We check if the relation with name relName is open and return E_RELNOTOPEN if not.
  // We call BlockAccess::renameAttribute(relName, oldAttrName, newAttrName) and return the return value of that function.
    if(strcmp(relName,RELCAT_RELNAME) == 0 || strcmp(relName,ATTRCAT_RELNAME) == 0)
        return E_NOTPERMITTED;

    if(OpenRelTable::getRelId(relName) != E_RELNOTOPEN)
        return E_RELOPEN;

    int retVal = BlockAccess::renameAttribute(relName,oldAttrName,newAttrName);
    return retVal;
}

int Schema::createRel(char relName[], int nAttrs, char attrs[][ATTR_SIZE], int attrtype[]){
  Attribute relNameAsAttribute;
  strcpy(relNameAsAttribute.sVal, relName);

  RecId targetRelId;

  RelCacheTable::resetSearchIndex(RELCAT_RELID);
  targetRelId = BlockAccess::linearSearch(RELCAT_RELID, "RelName", relNameAsAttribute , EQ);

  if(targetRelId.block != -1 && targetRelId.slot != -1){
    return E_RELEXIST;
  }

  for(int i = 0; i < nAttrs; i++){
    for(int attrIndex = i+1; attrIndex < nAttrs; attrIndex++){
      if(strcmp(attrs[i], attrs[attrIndex]) == 0){
        return E_DUPLICATEATTR;
      }
    }
  }

  Attribute relCatRecord[RELCAT_NO_ATTRS];
  strcpy(relCatRecord[RELCAT_REL_NAME_INDEX].sVal, relName);
  relCatRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal = nAttrs;
  relCatRecord[RELCAT_NO_RECORDS_INDEX].nVal = 0;
  relCatRecord[RELCAT_FIRST_BLOCK_INDEX].nVal = -1;
  relCatRecord[RELCAT_LAST_BLOCK_INDEX].nVal = -1;

  int numberSlotsPerBlk = floor((2016 / (ATTR_SIZE * nAttrs + 1)));
  relCatRecord[RELCAT_NO_SLOTS_PER_BLOCK_INDEX].nVal = numberSlotsPerBlk;

  int retVal = BlockAccess::insert(RELCAT_RELID, relCatRecord);
  if(retVal != SUCCESS)
    return retVal;
  
  for(int attrIndex = 0; attrIndex < nAttrs; attrIndex++){
    Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
    strcpy(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal, relName);
    strcpy(attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, attrs[attrIndex]);
    attrCatRecord[ATTRCAT_ATTR_TYPE_INDEX].nVal = attrtype[attrIndex];
    attrCatRecord[ATTRCAT_PRIMARY_FLAG_INDEX].nVal = -1;
    attrCatRecord[ATTRCAT_ROOT_BLOCK_INDEX].nVal = -1;
    attrCatRecord[ATTRCAT_OFFSET_INDEX].nVal = attrIndex;

    retVal = BlockAccess::insert(ATTRCAT_RELID, attrCatRecord);
    if(retVal != SUCCESS){
      Schema::deleteRel(relName);
      return E_DISKFULL;
    }
  }
  return SUCCESS;
}

int Schema::deleteRel(char *relName){

  if(strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0){
    return E_NOTPERMITTED;
  }
  int relId = OpenRelTable::getRelId(relName);
  if(relId != E_RELNOTOPEN){
    return E_RELOPEN;
  }

  int retVal = BlockAccess::deleteRelation(relName);
  std::cout<<"error:"<<retVal<<'\n';
  return retVal;

}

int Schema::createIndex(char relName[ATTR_SIZE],char attrName[ATTR_SIZE]){
    // if the relName is either Relation Catalog or Attribute Catalog,
        // return E_NOTPERMITTED
    //printf("Create Index Schema \n");
    if(strcmp(relName,RELCAT_RELNAME) == 0 || strcmp(relName,ATTRCAT_RELNAME) == 0){
        return E_NOTPERMITTED;
    } 
        // (check if the relation names are either "RELATIONCAT" and "ATTRIBUTECAT".
        // you may use the following constants: RELCAT_NAME and ATTRCAT_NAME)
    int relId = OpenRelTable::getRelId(relName);
    // get the relation's rel-id using OpenRelTable::getRelId() method
    if(relId == E_RELNOTOPEN){
        return E_RELNOTOPEN;
    }
    // if relation is not open in open relation table, return E_RELNOTOPEN
    // (check if the value returned from getRelId function call = E_RELNOTOPEN)
    //printf("Create Index Schema \n");
    // create a bplus tree using BPlusTree::bPlusCreate() and return the value
    return BPlusTree::bPlusCreate(relId, attrName);
}

int Schema::dropIndex(char *relName, char *attrName) {
    // if the relName is either Relation Catalog or Attribute Catalog,
        // return E_NOTPERMITTED
        // (check if the relation names are either "RELATIONCAT" and "ATTRIBUTECAT".
        // you may use the following constants: RELCAT_NAME and ATTRCAT_NAME)

    if(strcmp(relName,RELCAT_RELNAME) == 0 || strcmp(relName,ATTRCAT_RELNAME) == 0){
        return E_NOTPERMITTED;
    }

    int relId = OpenRelTable::getRelId(relName);

    if(relId == E_RELNOTOPEN){
        return E_RELNOTOPEN;
    }
    // get the rel-id using OpenRelTable::getRelId()

    // if relation is not open in open relation table, return E_RELNOTOPEN
    // (check if the value returned from getRelId function call = E_RELNOTOPEN)

    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);
    // get the attribute catalog entry corresponding to the attribute
    // using AttrCacheTable::getAttrCatEntry()

    // if getAttrCatEntry() fails, return E_ATTRNOTEXIST
    if(ret != SUCCESS){
        return E_ATTRNOTEXIST;
    }
    int rootBlock = attrCatEntry.rootBlock;

    if (rootBlock == -1) {
        return E_NOINDEX;
    }

    // destroy the bplus tree rooted at rootBlock using BPlusTree::bPlusDestroy()
    BPlusTree::bPlusDestroy(rootBlock);

    attrCatEntry.rootBlock = -1;
    AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatEntry);
    // set rootBlock = -1 in the attribute cache entry of the attribute using
    // AttrCacheTable::setAttrCatEntry()

    return SUCCESS;
}