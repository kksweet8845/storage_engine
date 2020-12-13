#ifndef PARSE_CMD_H
#define PARSE_CMD_H

#include <stdint.h>

#define PUT 0
#define GET 1
#define SCAN 2

int parse_cmd(char* str, uint64_t* key1, uint64_t* key2, char** val);

#endif