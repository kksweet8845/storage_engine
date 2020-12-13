#ifndef DB_H
#define DB_H


#include "list.h"
#include <stdint.h>
#include <pthread.h>
#include <stdint.h>

#define DEFAULT_SKIPLIST_SIZE ((uint32_t)0x80000)
#define MAX_TABLE_NUM 11

typedef struct DB_ {
    struct list_head SSTABLE_manager_head;
    struct list_head SKIPLIST_meta_head;
    struct list_head SKIPLIST_IMM_meta_head;
    uint64_t MAX_IMM_TABLE_NUM;
    uint64_t CUR_IMM_TABLE_NUM;
    uint64_t MAX_MEMORY_USAGE; // Default 4GB
    volatile unsigned char end;
    pthread_t db_manager_thread;
    double skiplist_time;
    double skiplist_imm_time;
    double sstable_time;
    uint64_t skiplist_c;
    uint64_t skiplist_imm_c;
    uint64_t sstable_c;
} db_impl_t;


typedef db_impl_t* db_impl_ptr_t;

pthread_t db_manager_thread, sstable_manager_thread;

pthread_mutex_t skiplist_mutex;
pthread_mutex_t sstable_mutex;
pthread_mutex_t skiplist_imm_mutex;

pthread_cond_t skiplist_cond;
pthread_cond_t sstable_cond;
pthread_cond_t skiplist_imm_cond;

/**
 * Initialize the db with initializing SSTBALE and SKIPLIST
 */
void init_db(db_impl_ptr_t db, int height);

void init_mutex();

/**
 * PUT the key and val into skiplist
 */
void PUT(uint64_t key, char* val, db_impl_ptr_t db);

/**
 * GET the val with respect to the key.
 */
char* GET(uint64_t key, db_impl_ptr_t db);
char** SCAN(uint64_t key1, uint64_t key2, db_impl_ptr_t db);

void check_memory_usage();

/**
 * Manage the sstable and imm skiplist.
 */
void* db_manager(void*);

void db_end(db_impl_ptr_t);


#endif