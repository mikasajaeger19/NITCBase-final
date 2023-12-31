#include "Algebra.h"
#include<iostream>
#include <cstring>
using namespace std;

bool isNumber(char *str) {
  int len;
  float ignore;
  int ret = sscanf(str, "%f %n", &ignore, &len);
  return ret == 1 && len == strlen(str);
}

int Algebra::insert(char relName[ATTR_SIZE], int nAttrs, char record[][ATTR_SIZE]){
  
  if(strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0)
    return E_NOTPERMITTED;

  int relId = OpenRelTable::getRelId(relName);
  if(relId == E_RELNOTOPEN)
    return E_RELNOTOPEN;

  RelCatEntry relCatEntry;
  RelCacheTable::getRelCatEntry(relId, &relCatEntry);
  if(relCatEntry.numAttrs != nAttrs)
    return E_NATTRMISMATCH;

  Attribute recordValues[nAttrs];

  for(int attrIndex=0; attrIndex < nAttrs; attrIndex++){
    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(relId, attrIndex, &attrCatEntry);
    int attrType = attrCatEntry.attrType;
    if(attrType == NUMBER){
      if(isNumber(record[attrIndex])){
        recordValues[attrIndex].nVal = atof(record[attrIndex]);
      }
      else{
        return E_ATTRTYPEMISMATCH;
      }
    }
    else{
      strcpy((char *) &(recordValues[attrIndex].sVal), record[attrIndex]);
      //strcpy(recordValues[attrIndex].sVal, record[attrIndex]);
    }
  }

  int retVal = BlockAccess::insert(relId, recordValues);
  return retVal;

}

// int Algebra::select(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], char attr[ATTR_SIZE], int op, char strVal[ATTR_SIZE]) {
  
//   int srcRelId = OpenRelTable::getRelId(srcRel);      
//   if (srcRelId == E_RELNOTOPEN) {
//     return E_RELNOTOPEN;
//   }

//   AttrCatEntry attrCatEntry;
//   int ret = AttrCacheTable::getAttrCatEntry(srcRelId, attr,&attrCatEntry);
  
//   if(ret == E_ATTRNOTEXIST)
//   	return E_ATTRNOTEXIST;

//   int type = attrCatEntry.attrType;
//   Attribute attrVal;
//   if (type == NUMBER) {
//     if (isNumber(strVal)) {       // the isNumber() function is implemented below
//       attrVal.nVal = atof(strVal);
//     } else {
//       return E_ATTRTYPEMISMATCH;
//     }
//   } else if (type == STRING) {
//     strcpy(attrVal.sVal, strVal);
//   }

//     RelCatEntry relCatEntry;
//     RelCacheTable::getRelCatEntry(srcRelId,&relCatEntry);

//     int src_nAttrs = relCatEntry.numAttrs;

//     char attr_names[src_nAttrs][ATTR_SIZE];
//     int attr_types[src_nAttrs];
    
//     for (int attrIndex = 0; attrIndex < src_nAttrs; attrIndex++) {
//         AttrCatEntry attrCatEntry;
//         AttrCacheTable::getAttrCatEntry(srcRelId, attrIndex, &attrCatEntry);
//         strcpy(attr_names[attrIndex], attrCatEntry.attrName);
//         attr_types[attrIndex] = attrCatEntry.attrType;
//     }

//     int retVal = Schema::createRel(targetRel, src_nAttrs, attr_names, attr_types);
//     if(retVal != SUCCESS)
//     	return retVal;

//     int openedRelId = OpenRelTable::openRel(targetRel);
    
//     if(openedRelId < 0 || openedRelId >= MAX_OPEN){
//     	Schema::deleteRel(targetRel);
//     	return openedRelId;
//     }
  
//     Attribute record[src_nAttrs];

//     RelCacheTable::resetSearchIndex(srcRelId);
//     AttrCacheTable::resetSearchIndex(srcRelId, attr);

//     while (true) {
//         ret = BlockAccess::search(srcRelId, record, attr, attrVal, op);
//         retVal = BlockAccess::insert(openedRelId, record);
//         if(retVal != SUCCESS){
//           Schema::closeRel(targetRel);
//           Schema::deleteRel(targetRel);
//           return ret;
//         }
//     }

//     Schema::closeRel(targetRel);
//     return SUCCESS;
// }

int Algebra::select(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], 
                    char attr[ATTR_SIZE], int op, char strVal[ATTR_SIZE]) 
{
    // get the srcRel's rel-id (let it be srcRelid), using OpenRelTable::getRelId()
    // if srcRel is not open in open relation table, return E_RELNOTOPEN
    
    int srcRelId = OpenRelTable::getRelId(srcRel); // we'll implement this later
    if (srcRelId == E_RELNOTOPEN) return E_RELNOTOPEN;

    // get the attr-cat entry for attr, using AttrCacheTable::getAttrCatEntry()
    // if getAttrcatEntry() call fails return E_ATTRNOTEXIST
    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(srcRelId, attr, &attrCatEntry);

    if (ret == E_ATTRNOTEXIST) return E_ATTRNOTEXIST;

    /*** Convert strVal to an attribute of data type NUMBER or STRING ***/

    // TODO: Convert strVal (string) to an attribute of data type NUMBER or STRING 
    int type = attrCatEntry.attrType;
    Attribute attrVal;
    if (type == NUMBER)
    {
        if (isNumber(strVal)) // the isNumber() function is implemented below
            attrVal.nVal = atof(strVal);
        else
            return E_ATTRTYPEMISMATCH;
    }
    else if (type == STRING)
        strcpy(attrVal.sVal, strVal);

    /*** Creating and opening the target relation ***/
    // Prepare arguments for createRel() in the following way:
    // get RelcatEntry of srcRel using RelCacheTable::getRelCatEntry()
    RelCatEntry relCatEntryBuffer;
    RelCacheTable::getRelCatEntry(srcRelId, &relCatEntryBuffer);

    int srcNoAttrs =  relCatEntryBuffer.numAttrs;

    /* let attr_names[src_nAttrs][ATTR_SIZE] be a 2D array of type char
        (will store the attribute names of rel). */
    char srcAttrNames [srcNoAttrs][ATTR_SIZE];

    // let attr_types[src_nAttrs] be an array of type int
    int srcAttrTypes [srcNoAttrs];

    /*iterate through 0 to src_nAttrs-1 :
        get the i'th attribute's AttrCatEntry using AttrCacheTable::getAttrCatEntry()
        fill the attr_names, attr_types arrays that we declared with the entries
        of corresponding attributes
    */
    for (int attrIndex = 0; attrIndex < srcNoAttrs; attrIndex++) {
        AttrCatEntry attrCatEntryBuffer;
        AttrCacheTable::getAttrCatEntry(srcRelId, attrIndex, &attrCatEntryBuffer);

        strcpy (srcAttrNames[attrIndex], attrCatEntryBuffer.attrName);
        srcAttrTypes[attrIndex] = attrCatEntryBuffer.attrType;
    }

    /* Create the relation for target relation by calling Schema::createRel()
       by providing appropriate arguments */
    // if the createRel returns an error code, then return that value.

    ret = Schema::createRel(targetRel, srcNoAttrs, srcAttrNames, srcAttrTypes);
    if (ret != SUCCESS) return ret;

    /* Open the newly created target relation by calling OpenRelTable::openRel()
       method and store the target relid */
    /* If opening fails, delete the target relation by calling Schema::deleteRel()
       and return the error value returned from openRel() */
    int targetRelId = OpenRelTable::openRel(targetRel);
    if (targetRelId < 0 || targetRelId >= MAX_OPEN) return targetRelId;

    /*** Selecting and inserting records into the target relation ***/
    /* Before calling the search function, reset the search to start from the
       first using RelCacheTable::resetSearchIndex() */
    // RelCacheTable::resetSearchIndex(srcRelId);

    Attribute record[srcNoAttrs];

    /*
        The BlockAccess::search() function can either do a linearSearch or
        a B+ tree search. Hence, reset the search index of the relation in the
        relation cache using RelCacheTable::resetSearchIndex().
        Also, reset the search index in the attribute cache for the select
        condition attribute with name given by the argument `attr`. Use
        AttrCacheTable::resetSearchIndex().
        Both these calls are necessary to ensure that search begins from the
        first record.
    */

    RelCacheTable::resetSearchIndex(srcRelId);
    AttrCacheTable::resetSearchIndex(srcRelId, attr);

    // read every record that satisfies the condition by repeatedly calling
    // BlockAccess::search() until there are no more records to be read

    while (BlockAccess::search(srcRelId, record, attr, attrVal, op) == SUCCESS) 
    {
        ret = BlockAccess::insert(targetRelId, record);

        // if (insert fails) {
        //     close the targetrel(by calling Schema::closeRel(targetrel))
        //     delete targetrel (by calling Schema::deleteRel(targetrel))
        //     return ret;
        // }

        if (ret != SUCCESS) 
        {
            Schema::closeRel(targetRel);
            Schema::deleteRel(targetRel);
            return ret;
        }
    }

    // Close the targetRel by calling closeRel() method of schema layer
    Schema::closeRel(targetRel);

    return SUCCESS;
}

int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE]) {

    int srcRelId = OpenRelTable::getRelId(srcRel);

   if (srcRelId < 0 || srcRelId >= MAX_OPEN) 
    return E_RELNOTOPEN;


    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(srcRelId, &relCatEntry);

    int numAttrs = relCatEntry.numAttrs;

    char attrNames[numAttrs][ATTR_SIZE];
    int attrTypes[numAttrs];

    for(int attrIndex = 0; attrIndex < numAttrs; attrIndex++){

      AttrCatEntry attrCatEntry;
      AttrCacheTable::getAttrCatEntry(srcRelId, attrIndex, &attrCatEntry);
      strcpy(attrNames[attrIndex], attrCatEntry.attrName);
      attrTypes[attrIndex] = attrCatEntry.attrType;

    } 

    int ret = Schema::createRel(targetRel, numAttrs, attrNames, attrTypes);

    if(ret != SUCCESS)
      return ret;
    
    int targetRelId = OpenRelTable::openRel(targetRel);

    if(targetRelId < 0 || targetRelId >= MAX_OPEN){
      Schema::deleteRel(targetRel);
      return targetRelId;
    }
    
    RelCacheTable::resetSearchIndex(srcRelId);

    Attribute record[numAttrs];

    while (true)
    {

      // check this part POTENTIAL ISSUE
        int retVal = BlockAccess::project(srcRelId, record);

        if(retVal != SUCCESS)
          break;

        ret = BlockAccess::insert(targetRelId, record);

        if (ret != SUCCESS) {
            Schema::closeRel(targetRel);
            Schema::deleteRel(targetRel);
            return ret;
        }
    }

    Schema::closeRel(targetRel);
    return SUCCESS;
}

int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], int tar_nAttrs, char tar_Attrs[][ATTR_SIZE]) {

    int srcRelId = OpenRelTable::getRelId(srcRel);

    if (srcRelId < 0 || srcRelId >= MAX_OPEN) 
      return E_RELNOTOPEN;

    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(srcRelId, &relCatEntry);

    // get the no. of attributes present in relation from the fetched RelCatEntry.
    int numAttrs = relCatEntry.numAttrs;
    int attr_offset[tar_nAttrs];

    int attr_types[tar_nAttrs];

    for(int tar_AttrIndex = 0; tar_AttrIndex < tar_nAttrs; tar_AttrIndex++){
      AttrCatEntry attrCatEntry;
      int ret = AttrCacheTable::getAttrCatEntry(srcRelId, tar_Attrs[tar_AttrIndex], &attrCatEntry);
      if(ret == E_ATTRNOTEXIST)
        return E_ATTRNOTEXIST;
      attr_offset[tar_AttrIndex] = attrCatEntry.offset;
      attr_types[tar_AttrIndex] = attrCatEntry.attrType;
    }

    int retVal = Schema::createRel(targetRel, tar_nAttrs, tar_Attrs, attr_types);

    if(retVal != SUCCESS)
      return retVal;

    int targetRelId = OpenRelTable::openRel(targetRel);

    if(targetRelId < 0 || targetRelId >= MAX_OPEN){
      Schema::deleteRel(targetRel);
      return targetRelId;
    }

    RelCacheTable::resetSearchIndex(srcRelId);

    Attribute record[numAttrs];
    Attribute proj_record[tar_nAttrs];

    while (true) {

        int ret = BlockAccess::project(srcRelId, record);

        if(ret != SUCCESS)
          break;
        Attribute proj_record[tar_nAttrs];

        for(int tar_AttrIndex = 0; tar_AttrIndex < tar_nAttrs; tar_AttrIndex++){
          proj_record[tar_AttrIndex] = record[attr_offset[tar_AttrIndex]];
        }

        ret = BlockAccess::insert(targetRelId, proj_record);

        if (ret != SUCCESS) {
            Schema::closeRel(targetRel);
            Schema::deleteRel(targetRel);
            return ret;
        }
    }

    Schema::closeRel(targetRel);
    return SUCCESS;
}

int Algebra::join(char srcRelation1[ATTR_SIZE], char srcRelation2[ATTR_SIZE], char targetRelation[ATTR_SIZE], char attribute1[ATTR_SIZE], char attribute2[ATTR_SIZE]){

  int srcRelId1 = OpenRelTable::getRelId(srcRelation1), srcRelId2 = OpenRelTable::getRelId(srcRelation2);
  if(srcRelId1 == E_RELNOTOPEN || srcRelId2 == E_RELNOTOPEN)
    return E_RELNOTOPEN;

  AttrCatEntry attrCatEntry1, attrCatEntry2;

  int ret1 = AttrCacheTable::getAttrCatEntry(srcRelId1, attribute1, &attrCatEntry1);
  int ret2 = AttrCacheTable::getAttrCatEntry(srcRelId2, attribute2, &attrCatEntry2);

  if(ret1 == E_ATTRNOTEXIST || ret2 == E_ATTRNOTEXIST)
    return E_ATTRNOTEXIST;

  if(attrCatEntry1.attrType !=  attrCatEntry2.attrType)
    return E_ATTRTYPEMISMATCH;

  RelCatEntry relCatEntry1, relCatEntry2;
  RelCacheTable::getRelCatEntry(srcRelId1, &relCatEntry1);
  RelCacheTable::getRelCatEntry(srcRelId2, &relCatEntry2);
  int numOfAttributes1 = relCatEntry1.numAttrs, numOfAttributes2 =  relCatEntry2.numAttrs;

  for(int attrIndex = 0; attrIndex < numOfAttributes1; attrIndex++){
    AttrCatEntry attrCEntry1, attrCEntry2;
    AttrCacheTable::getAttrCatEntry(srcRelId1, attrIndex, &attrCEntry1);
    if(strcmp(attrCEntry1.attrName, attribute1) == 0)
      continue;
    
    for(int attrIndex2 = 0; attrIndex2 < numOfAttributes2; attrIndex2++){
      AttrCacheTable::getAttrCatEntry(srcRelId2, attrIndex2, &attrCEntry2);

      if(strcmp(attribute2, attrCEntry2.attrName) == 0)
        continue;

      if(strcmp(attrCEntry1.attrName, attrCEntry2.attrName) == 0)
        return E_DUPLICATEATTR;
    }
  }

  if(attrCatEntry2.rootBlock == -1){

    int ret = BPlusTree::bPlusCreate(srcRelId2, attribute2);
    
    if(ret == E_DISKFULL)
      return ret;

  }

  int numOfAttributesInTarget = numOfAttributes1 + numOfAttributes2 - 1;

  char targetRelAttrNames[numOfAttributesInTarget][ATTR_SIZE];
  int targetRelAttrTypes[numOfAttributesInTarget];

  for(int attrIndex = 0; attrIndex < numOfAttributes1; attrIndex++){
    AttrCatEntry attrCEntry;
    AttrCacheTable::getAttrCatEntry(srcRelId1, attrIndex, &attrCEntry);
    strcpy(targetRelAttrNames[attrIndex], attrCEntry.attrName);
    targetRelAttrTypes[attrIndex] = attrCEntry.attrType;
  }

  bool flag = false;
  for(int attrIndex = 0; attrIndex < numOfAttributes2; attrIndex++){
    AttrCatEntry attrCEntry;
    AttrCacheTable::getAttrCatEntry(srcRelId2, attrIndex, &attrCEntry);
    if(strcmp(attrCEntry.attrName, attribute2) == 0){
      flag = true;
      continue;
    }
    if(flag){
      strcpy(targetRelAttrNames[attrIndex + numOfAttributes1 - 1], attrCEntry.attrName);
      targetRelAttrTypes[attrIndex + numOfAttributes1 - 1] = attrCEntry.attrType;
    }
    else{
    strcpy(targetRelAttrNames[attrIndex + numOfAttributes1], attrCEntry.attrName);
    targetRelAttrTypes[attrIndex] = attrCEntry.attrType;
    }
  }

  int ret = Schema::createRel(targetRelation, numOfAttributesInTarget, targetRelAttrNames, targetRelAttrTypes);

  if(ret != SUCCESS)
    return ret;

  int targetRelId = OpenRelTable::openRel(targetRelation);

  if(targetRelId < 0 || targetRelId >= MAX_OPEN){
    Schema::deleteRel(targetRelation);
    return targetRelId;
  }

  Attribute record1[numOfAttributes1], record2[numOfAttributes2];
  Attribute targetRecord[numOfAttributesInTarget];

  RelCacheTable::resetSearchIndex(srcRelId1);
  
  flag = false;
  while(BlockAccess::project(srcRelId1, record1) == SUCCESS){
    
    RelCacheTable::resetSearchIndex(srcRelId2);
    AttrCacheTable::resetSearchIndex(srcRelId2, attribute2);

    while(BlockAccess::search(srcRelId2, record2, attribute2, record1[attrCatEntry1.offset], EQ) == SUCCESS){
      for(int attrIndex = 0; attrIndex < numOfAttributes1; attrIndex++){
        targetRecord[attrIndex] = record1[attrIndex];
      }
      for(int attrIndex = 0; attrIndex < numOfAttributes2; attrIndex++){
        if(attrIndex == attrCatEntry2.offset){
          flag = true;
          continue;
        }
        if(flag)
          targetRecord[attrIndex + numOfAttributes1 - 1] = record2[attrIndex];
        else
          targetRecord[attrIndex + numOfAttributes1] = record2[attrIndex];
      }

      int retVal = BlockAccess::insert(targetRelId, targetRecord);
      if(retVal == E_DISKFULL){
        Schema::closeRel(targetRelation);
        Schema::deleteRel(targetRelation);
        return retVal;
      }
    }
  }

  return SUCCESS;
}