#include "filename.h"
#include <stdio.h>
#include <stdlib.h>

char* sstable_filename(char* dbname, int lv, int num){
    char* buf = malloc(sizeof(char)* 100);
    if(dbname == NULL)
        dbname = "default";
    sprintf(buf, "./storage/%s_lv%d_%d.ssb", dbname, lv, num);
    return buf;
}
// immu = 1  for immu
char* skiplist_filename(char* dbname, int immu, int num){
    char* buf = malloc(sizeof(char) * 100);
    if(dbname == NULL)
        dbname = "default";
    sprintf(buf, "./storage/%s_skiplist_%d_%d.skp", dbname, immu, num);
    return buf;
}