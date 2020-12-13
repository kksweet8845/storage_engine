#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include "parse_cmd.h"



int parse_cmd(char* str, uint64_t* key1, uint64_t* key2, char** val){

    char* cmds[] = {"PUT", "GET", "SCAN"};

    char* cur = str;

    char* new_str = (char*)malloc(sizeof(char) * 200);
    char* new_cur = new_str;
    memset(new_str, '\0', 200);
    int num_space = 0;
    while(*cur != '\0'){
        if(*cur == ' '){
            cur++;
            new_cur++;
            num_space++;
        }else {
            *new_cur++ = *cur++;
        }
    }
    int i=1;
    // char* cmd, *key, *val;
    char** buf = (char**)malloc(sizeof(char*) * 3);
    buf[0] = new_str;
    cur = new_str;
    while(i < num_space+1){
        if(*cur++ == '\0'){
            buf[i] = cur;
            i++;
        }
    }

    char** endptr = NULL;
    for(int i=0;i<3;i++){
        if(strcmp(cmds[i], buf[0]) == 0){
            switch(i){
                case PUT:
                    *key1 = strtoull(buf[1], endptr, 10);
                    *val = malloc(sizeof(char) * 129);
                    memset(*val, '\0', 129);
                    strcpy(*val, buf[2]);
                    break;
                case GET:
                    *key1 = strtoull(buf[1], endptr, 10);
                    *val = NULL;
                    break;
                case SCAN:
                    *key1 = strtoull(buf[1], endptr, 10);
                    *key2 = strtoull(buf[2], endptr, 10);
                    *val = NULL;
                    break;
            }

            return i;
        }
    }
    return -1;
}