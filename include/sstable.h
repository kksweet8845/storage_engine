#ifndef SSTABLE_H
#define SSTABLE_H



#include "skiplist.h"
#include "list.h"
#include <stdint.h>
#include <stdio.h>
#include "typedef.h"

#define LV0_FN 4
#define LV1_FN 10
#define LV2_FN 100
#define LV3_FN 1000

#define FOOTER_FIXED_LENGTH 64 // fixed 64 byte
#define MAGICNUMBER 0x12345678
#define SSTBALE_DEFAULT_SIZE ((uint64_t)0x200000)
#define RANGEBIN 15000


typedef struct SSTABLE_NODE {
    char* filename;
    uint64_t keyfrom, keyto;
    int id;
    char* dbname;
    unsigned char empty;
    struct list_head list;
    struct list_head vec;
    uint64_t size;
    uint32_t lv;
} sstable_node_t;

typedef sstable_node_t* sstable_node_ptr_t;

typedef struct SSTABLE_MANAGER {
    int lv;
    int filenum;
    uint64_t fileIndex;
    int max_filenum;
    char* dbname;
    struct list_head sstable_head;
    struct list_head list;
} sstable_manager_t;

typedef sstable_manager_t* sstable_manager_ptr_t;

// typedef struct KEY_VAL_PAIR{
//     uint64_t key;
//     char* val;
//     struct list_head list;
// } key_val_pair_t;

// typedef key_val_pair_t* key_val_pair_ptr_t;

void init_sstable_manager(struct list_head*);
void add_sstable_manager(struct list_head*);

void create_sstable_manager(struct list_head* manager_head, int lv, int n);
sstable_node_ptr_t create_sstable_node(
    uint64_t keyfrom, uint64_t keyto,
    int id, char* dbname, uint32_t lv);
void add_sstable_node(struct list_head* manager_head, int lv, struct list_head* key_val_head);
void add_sstable_node_to_manager(sstable_manager_ptr_t manager, struct list_head* key_val_head, struct list_head* manager_head);
void ssave_to_disk(sstable_manager_ptr_t manager, sstable_node_ptr_t node, struct list_head* manager_head);
void manage_sstable();

void write_sstable(struct list_head*, char*);
int write_data_block(uint64_t key, char* val, FILE* stream);
int write_footer(uint64_t, FILE* stream);


//read
void read_sstable(char* filename, struct list_head* sstable_head);
void read_sstable_and_find(char* filename, struct list_head* sstable_head, uint64_t key);

// find
char* sstable_find_key(struct list_head* manager_head, uint64_t key);
char* _find_key(uint64_t key, struct list_head* head);
char* _find_key_v2(char* filename, uint64_t target, struct list_head* key_val_head);
sstable_manager_ptr_t find_lv(int lv, struct list_head* sstable_manager_head);


// merge
void merge_sstable(struct list_head* sstable_node, struct list_head* sstable_manager_head);
void merge_vec(struct list_head* vec_from, struct list_head* vec_to, uint64_t keyfrom, uint64_t keyto);
void merge_op(struct list_head* left_head, struct list_head* right_head, struct list_head* merge_head);

void update_sstable(sstable_manager_ptr_t manager, sstable_node_ptr_t node, int rn);

void move_sstable(sstable_manager_ptr_t manager, sstable_node_ptr_t node);
void del_sstable(sstable_manager_ptr_t manager, sstable_node_ptr_t node);
void free_sstable(sstable_node_ptr_t node);
int is_key_overlap(int _1_keyfrom, int _1_keyto, int _2_keyfrom, int _2_keyto);
void free_key_val_list(struct list_head*);

// query
int get_list_length(struct list_head* head);
int get_sstable_size(sstable_node_ptr_t node);
int get_list_size(struct list_head*);

struct list_head* get_median_node(struct list_head*);

void sstable_restore_node(char* dbname, int lv, int id, uint64_t keyfrom, uint64_t keyto, struct list_head* manage_head);
// debug
void print_sstable_keyranges(struct list_head* sstable_manager_head);
#endif