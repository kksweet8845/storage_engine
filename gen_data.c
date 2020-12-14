#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>


void generate_random_128Bstring(char* output){


    char* str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!@#$%^&*()_+=<>{}[]|\\";
    int len = strlen(str);
    int index = 0;
    // char* output = malloc(sizeof(char) * 129);
    memset(output, '\0', 129);
    for(int i=0;i<128;i++){
        index = rand() % len;
        output[i] = str[index];
    }
    // return output;
}

void generate_cmd(char* cmd, uint64_t _key) {

    char* cmds[] = { "PUT", "GET", "SCAN" };

    int index;
    unsigned long long int key;
    char* val = malloc(sizeof(char) * 129);
    index = rand() % 2;
    // index = 0; // PUT
    // index = 1; // GET
    if(_key == -1){
        // key = ((uint64_t)rand() << 48) ^ ((uint64_t)rand() << 35) ^ ((uint64_t)rand() << 22) ^ ((uint64_t)rand() << 9) ^ ((uint64_t)rand() << 4);
        key = rand() % ((uint64_t)1 << 20) + 1;
    }else
        key = _key;

    uint64_t key1, key2;
    switch(index){
        case 0:
            generate_random_128Bstring(val);
            memset(cmd, '\0', strlen(cmd));
            sprintf(cmd, "%s %llu %s", cmds[index], key, val);
            break;
        case 1:
            sprintf(cmd, "%s %llu", cmds[index], key);
            break;
        case 2:
            key1 = key;
            key2 = key1 + (rand() % 100);
            sprintf(cmd, "%s %llu %llu", cmds[index], key1, key2);
            break;
    }

}



int main(int argc, char* argv[]){

    if(argc != 3)
        return 1;
    char* outputFilePath = argv[1];
    // int expectedSize = atoi(argv[2]) << 20;
    uint64_t expectedSize = strtoull(argv[2], NULL, 10);
    expectedSize = expectedSize << 20;

    FILE* fop = fopen(outputFilePath, "w");

    int64_t fileSize = 0;
    char* cmd = malloc(sizeof(char) * 200);
    int i=0;
    uint64_t key = 1;
    int phase = 0;
    while(fileSize < expectedSize){
        i++;
        generate_cmd(cmd, -1);
        // key += rand() %10 + 1;
        // printf("%s\n", cmd);
        fileSize += strlen(cmd);
        fprintf(fop, "%s\n", cmd);
        // if(i%30000 == 0){
        //     key = rand() % 10000 + 1;
        // }
    }

}