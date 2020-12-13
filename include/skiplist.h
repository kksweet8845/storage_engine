#ifndef SKIPLIST_H
#define SKIPLIST_H


#include "list.h"
#include <stdint.h>

#define NEXT 0
#define EQUAL 1
#define PREV 2
#define NONE -1


typedef struct SKIPLIST_NODE {
    int height;
    int total_height;
    uint64_t key;
    unsigned char valid;
    char* val;
    uint64_t size;
    struct list_head list;
    struct list_head lv;
} skiplist_node_t;

typedef skiplist_node_t* skiplist_ptr_t;


typedef struct SKIPLIST_META {
    uint64_t size;
    unsigned char immu;
    // skiplist_ptr_t* head;
    int total_height;
    struct list_head* head;
    struct list_head list;
} skiplist_meta_t;

typedef skiplist_meta_t* skiplist_meta_ptr_t;

/**
 * Initialize the skiplist with a default skiplist
 * @head:   The skiplist list head, which will lead the list of skiplist
 * @height: The height of skiplist
 * @num: Number of skiplist, default is 1
 */
void init_skiplist(struct list_head* head, int height, int num);


/**
 * Insert the key and value into the latest skiplist (unimmutable)
 * @skiplist_head: The skiplist list head, which will lead the list of skiplist
 * @key: The key of data
 * @val: The value of data
 */
void insert_skiplist(struct list_head*, uint64_t, char*);


/**
 * Find the key inside the skiplist.
 * @skiplist_head: The skiplist list head, which leads the list of skiplist
 * @key: The key to be searched
 */
char* find_skiplist(struct list_head*, uint64_t);


/**
 * Scan the skiplist within a range
 * @skiplist_head: The skiplist list head, which leads the list of skiplist
 * @keyfrom: The start of key range
 * @keyto: The end of key range
 */
char** scan_skiplist(struct list_head*, uint64_t, uint64_t);

/**
 * Transform the skiplist into key val pair defined in "typedef.h"
 * @heads: The heads of the skiplist node inside a skiplist
 * @key_val_head: The heads of key val pair, which will lead the list for further usage
 */
void skiplist_to_keyValPair(struct list_head*, struct list_head*);

void skiplist_to_imm(struct list_head*, struct list_head*);


void create_list_head(struct list_head**, int);
int insert(uint64_t, char*, struct list_head*, int);
skiplist_ptr_t find(struct list_head*, uint64_t, int);
skiplist_ptr_t find_exactly(struct list_head*, uint64_t, int);

// utils
skiplist_ptr_t find_closest(struct list_head*, struct list_head*, const uint64_t, int, int*, int);
void update_node(skiplist_ptr_t, char*);
void create_new_node(skiplist_ptr_t*, uint64_t key, char* val, int total_height, int cur_height);
void insert_initial_node(struct list_head* heads, uint64_t key, char* val, int total_height);
void insert_new_node(struct list_head*, uint64_t , char*, int);
// void find_and_insert(struct list_head*, )
void set_val(skiplist_ptr_t, char*);
int get_possibility(int);
int rand_n(int n);
skiplist_ptr_t get_lowest(skiplist_ptr_t);

// debug
void print_skiplist(struct list_head*, int);
void check_skiplist_consistence(struct list_head*, int);

void* skiplist_manager(void*);


#endif