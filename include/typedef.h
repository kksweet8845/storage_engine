#ifndef TYPEDEF_H
#define TYPEDEF_H

#include <stdint.h>
#include "list.h"
typedef struct KEY_VAL_PAIR{
    uint64_t key;
    char* val;
    struct list_head list;
} key_val_pair_t;

typedef key_val_pair_t* key_val_pair_ptr_t;

#endif