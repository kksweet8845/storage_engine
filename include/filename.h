#ifndef FILENAME_H
#define FILENAME_H


/**
 * Return the filename with corresponding no. prefixed with "dbname"
 */
char* sstable_filename(char* dbname, int lv, int num);

char* skiplist_filename(char* dbname, int immu, int num);


#endif