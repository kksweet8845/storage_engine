#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include "list.h"
#include "time.h"
#include <assert.h>
#include "leveldb.h"
// #include <time.h>
#include <unistd.h>
#include "mytime.h"


#define PUT_ 0
#define GET_ 1
#define SCAN_ 2

// #ifdef _GNU_SOURCE
// # undef  _XOPEN_SOURCE
// # define _XOPEN_SOURCE 600
// # undef  _XOPEN_SOURCE_EXTENDED
// # define _XOPEN_SOURCE_EXTENDED 1
// # undef  _LARGEFILE64_SOURCE
// # define _LARGEFILE64_SOURCE 1
// # undef  _BSD_SOURCE
// # define _BSD_SOURCE 1
// # undef  _SVID_SOURCE
// # define _SVID_SOURCE 1
// # undef  _ISOC99_SOURCE
// # define _ISOC99_SOURCE 1
// # undef  _POSIX_SOURCE
// # define _POSIX_SOURCE 1
// # undef  _POSIX_C_SOURCE
// # define _POSIX_C_SOURCE 200112L
// # undef  _ATFILE_SOURCE
// # define _ATFILE_SOURCE 1
// #endif

int parse_cmd(char*, uint64_t*, uint64_t*, char**);
void print_scan(char**, uint64_t, uint64_t, FILE*);


// double diff(struct timespec start, struct timespec end) {
//   struct timespec temp;
//   if ((end.tv_nsec-start.tv_nsec)<0) {
//     temp.tv_sec = end.tv_sec-start.tv_sec-1;
//     temp.tv_nsec = 1000000000+end.tv_nsec-start.tv_nsec;
//   } else {
//     temp.tv_sec = end.tv_sec-start.tv_sec;
//     temp.tv_nsec = end.tv_nsec-start.tv_nsec;
//   }
//   return temp.tv_sec + (double) temp.tv_nsec / 1000000000.0;
// }


int main(int argc, char* argv[]){

    char* inputFilePath = argv[1];
    FILE* fop = fopen(inputFilePath, "r");

    char* token, *filename, *inputFilename;
    printf("%d\n", strlen(inputFilePath));
    char* cur = inputFilePath + strlen(inputFilePath) - 1;
    while(*cur != '/' && cur-- != inputFilePath);
    cur++;
    inputFilename = malloc(sizeof(char) * 100);
    printf("%s\n", cur);
    strcpy(inputFilename, cur);
    filename = strtok(inputFilename, ".");
    char* outputFile = malloc(sizeof(char) * 100);
    memset(outputFile, '\0', 100);
    sprintf(outputFile, "%s.output", filename);

    FILE* out_stream = fopen(outputFile, "w");


    char* cmd = malloc(sizeof(char) * 300);
    uint64_t key1, key2;
    char* value;


    // Initialize db
    db_impl_t db;
    init_db(&db, 4);

    time_t startT = time(NULL);
    srand(time(NULL));
    char* return_val = NULL;
    char** str_list;
    uint64_t i=0;
    double P_ave = 0.0f, G_ave = 0.0f;
    struct timespec P_s, P_e;
    struct timespec G_s, G_e;
    uint64_t P_c = 0, G_c = 0;
    while(fscanf(fop, "%[^\n]\n", cmd) != EOF){
        if(i++ % 1000000 == 0){
            printf("%llu\n", i);
        }
        switch(parse_cmd(cmd, &key1, &key2, &value)){
            case PUT_:
                // printf("PUT %llu %s\n", key1, value);
                P_c++;
                clock_gettime(CLOCK_MONOTONIC, &P_s);
                PUT(key1, value, &db);
                clock_gettime(CLOCK_MONOTONIC, &P_e);
                P_ave += diff(P_s, P_e);
                // printf("PUT %llu\n", i++);
                break;
            case GET_:
                // printf("GET %llu\n", key1);
                // if(key1 == 19692347){
                //     printf("GET %llu\n", key1);
                // }
                G_c++;
                // printf("GET %llu\n", i++);
                clock_gettime(CLOCK_MONOTONIC, &G_s);
                return_val = GET(key1, &db);
                clock_gettime(CLOCK_MONOTONIC, &G_e);
                G_ave += diff(G_s, G_e);
                fprintf(out_stream, "%s\n", return_val == NULL ? "EMPTY" : return_val);
                fflush(out_stream);
                break;
            case SCAN_:
                for(uint64_t key=key1;key<=key2;key++){
                    return_val = GET(key, &db);
                    fprintf(out_stream, "%s\n", return_val == NULL ? "EMPTY" : return_val);
                }
                // printf("SCAN %llu %llu\n", key1, key2);
                // str_list = scan_skiplist(&skiplist_head, key1, key2);
                // print_scan(str_list, key1, key2, out_stream);
                break;
        }
    }
    time_t endT = time(NULL);
    db_end(&db);
    printf("Execution time %d\n", endT- startT);
    // print_skiplist(meta->head, 4);

    printf("PUT : %e sec\n", P_ave / P_c);
    printf("GET : %e sec\n", G_ave / G_c);

    printf("SKIPLIST %e sec\n", db.skiplist_time / db.skiplist_c);
    printf("SKIPLIST IMM %e sec\n", db.skiplist_imm_time / db.skiplist_imm_c);
    printf("SSTABLE %e sec\n", db.sstable_time / db.sstable_c);

    return 0;
}

int parse_cmd(char* str, uint64_t* key1, uint64_t* key2, char** val){

    char* cmds[] = {"PUT", "GET", "SCAN"};

    char* cur = str;

    char* new_str = malloc(sizeof(char) * 200);
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
    char** buf = malloc(sizeof(char*) * 3);
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
                case PUT_:
                    *key1 = strtoull(buf[1], endptr, 10);
                    *val = malloc(sizeof(char) * 129);
                    memset(*val, '\0', 129);
                    strcpy(*val, buf[2]);
                    break;
                case GET_:
                    *key1 = strtoull(buf[1], endptr, 10);
                    *val = NULL;
                    break;
                case SCAN_:
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


void print_scan(char** str_list, uint64_t keyfrom, uint64_t keyto, FILE* stream){

    // printf("==============\n");
    for(uint64_t i=0;i<keyto-keyfrom+1;i++){
        fprintf(stream, "%s\n", str_list[i] == NULL ? "EMPTY" : str_list[i]);
    }
}