#include <stdio.h>
#include <stdlib.h>
#include "skiplist.h"
#include "sstable.h"
#include "leveldb.h"
#include "mytime.h"
#include "filename.h"
#include <sys/stat.h>
#include <string.h>

#define DEFAULT_SKIPLIST 1

// void free_key_val_list(struct list_head* head){
//     key_val_pair_ptr_t item, safe;
//     list_for_each_entry_safe(item, safe, head, list){
//         free(item->val);
//         list_del_init(&item->list);
//         free(item);
//     }
//     INIT_LIST_HEAD(head);
// }

void init_db(db_impl_ptr_t db, int height){

    init_sstable_manager(&db->SSTABLE_manager_head);
    INIT_LIST_HEAD(&db->SKIPLIST_IMM_meta_head);
    INIT_LIST_HEAD(&db->SKIPLIST_meta_head);
    db_prepare(db);
    init_skiplist(&db->SKIPLIST_meta_head, height, DEFAULT_SKIPLIST);
    db->MAX_IMM_TABLE_NUM = MAX_TABLE_NUM;
    db->CUR_IMM_TABLE_NUM = 0;
    db->MAX_MEMORY_USAGE = (1 << 32);
    db->end = 0;

    db->skiplist_imm_time = 0.0;
    db->skiplist_time = 0.0;
    db->sstable_time = 0.0;
    db->skiplist_c = 0;
    db->skiplist_imm_c = 0;
    db->sstable_c = 0;

    init_mutex();
    // printf("%p\n", db);
    pthread_create(&db->db_manager_thread, NULL, db_manager, (void*)db);
    // pthread_create(&db->db_manager_thread, NULL, sstable_manager, (void*)db);
}

void init_mutex(){
    pthread_mutex_init(&skiplist_mutex, NULL);
    pthread_mutex_init(&skiplist_imm_mutex, NULL);
    pthread_mutex_init(&sstable_mutex, NULL);
}


void PUT(uint64_t key, char* val, db_impl_ptr_t db){
    // mutex
    pthread_mutex_lock(&skiplist_mutex);
    insert_skiplist(&db->SKIPLIST_meta_head, key, val);
    pthread_mutex_unlock(&skiplist_mutex);
    // printf("PUT successfully\n");

    // skiplist_meta_ptr_t skiplist_node;
    // struct list_head key_val_head;
    // INIT_LIST_HEAD(&key_val_head);
    // struct list_head* node;
    // skiplist_node = list_first_entry(&db->SKIPLIST_meta_head, skiplist_meta_t, list);
    // if(skiplist_node->size >= DEFAULT_SKIPLIST_SIZE){
    //     // metex
    //     // printf("SKIPLIST move to imm\n");
    //     // printf("length %llu\n", skiplist_node->size / 138);
    //     skiplist_to_imm(&db->SKIPLIST_meta_head, &db->SKIPLIST_IMM_meta_head);
    //     // mutex
    // }

    // int i=0;
    // // pthread_mutex_lock(&skiplist_imm_mutex);
    // list_for_each(node, &db->SKIPLIST_IMM_meta_head) i++;
    // // pthread_mutex_unlock(&skiplist_imm_mutex);
    // if( i >= MAX_TABLE_NUM ){
    //     // save two imm table to disk
    //     // printf("IMM TABLE to sstable\n");
    //     // pthread_mutex_lock(&skiplist_imm_mutex);
    //     skiplist_meta_ptr_t item = list_last_entry(&db->SKIPLIST_IMM_meta_head, skiplist_meta_t, list);
    //     // check_skiplist_consistence(item->head, 4);
    //     // print_skiplist(item->head, 4);
    //     skiplist_to_keyValPair(item->head, &key_val_head);
    //     list_del_init(&item->list);
    //     // pthread_mutex_unlock(&skiplist_imm_mutex);
    //     // pthread_mutex_lock(&sstable_mutex);
    //     add_sstable_node(&db->SSTABLE_manager_head, 0, &key_val_head);
    //     free_key_val_list(&key_val_head);
    //     // pthread_mutex_unlock(&sstable_mutex);
    // }
}


char* GET(uint64_t key, db_impl_ptr_t db){
    struct timespec start, end;
    double d = 0;
    pthread_mutex_lock(&skiplist_mutex);
    db->skiplist_c++;
    clock_gettime(CLOCK_MONOTONIC, &start);
    char* ans = find_skiplist(&db->SKIPLIST_meta_head, key);
    clock_gettime(CLOCK_MONOTONIC, &end);
    d = diff(start, end);
    db->skiplist_time += d;
    // printf("Skiplist %e sec\n", d);
    // printf("SKIP GET %s\n", ans);
    pthread_mutex_unlock(&skiplist_mutex);
    pthread_mutex_lock(&skiplist_imm_mutex);
    if(ans == NULL){
        db->skiplist_imm_c++;
        clock_gettime(CLOCK_MONOTONIC, &start);
        ans = find_skiplist(&db->SKIPLIST_IMM_meta_head, key);
        clock_gettime(CLOCK_MONOTONIC, &end);
        d = diff(start, end);
        db->skiplist_imm_time += d;
        // printf("Skiplist imm %e sec\n", d);
    }
    // ans = ans == NULL ? find_skiplist(&db->SKIPLIST_IMM_meta_head, key) : ans;
    // printf("IMM GET %s\n", ans);
    pthread_mutex_unlock(&skiplist_imm_mutex);

    pthread_mutex_lock(&sstable_mutex);
    if(ans == NULL){
        clock_gettime(CLOCK_MONOTONIC, &start);
        ans = sstable_find_key(&db->SSTABLE_manager_head, key);
        clock_gettime(CLOCK_MONOTONIC, &end);
        d = diff(start, end);
        db->sstable_c++;
        db->sstable_time += d;
    }
    // ans = ans == NULL ? sstable_find_key(&db->SSTABLE_manager_head, key) : ans;
    // printf("sstable %e sec\n", d);
    pthread_mutex_unlock(&sstable_mutex);
    // printf("SSTABLE GET %s\n", ans);
    return ans;
}


char** SCAN(uint64_t key1, uint64_t key2, db_impl_ptr_t db){

    return NULL;
}



void* db_manager(void* arg){

    db_impl_ptr_t db = (db_impl_ptr_t) arg;
    // printf("db manager %p\n", db);
    // change skiplist to imm skiplist

    // save imm skiplist to sstable

    // monitoring
    skiplist_meta_ptr_t skiplist_node;
    struct list_head key_val_head;
    INIT_LIST_HEAD(&key_val_head);
    struct list_head* node;
    FILE* db_log;
    // db_log = fopen("db.log", "w");
    while(db->end == 0){
        pthread_mutex_lock(&skiplist_mutex);
        skiplist_node = list_first_entry(&db->SKIPLIST_meta_head, skiplist_meta_t, list);
        if(skiplist_node->size >= DEFAULT_SKIPLIST_SIZE){
            // metex
            // printf("SKIPLIST move to imm skipliist size %llu KB\n", skiplist_node->size >> 10);
            // printf("length %llu\n", skiplist_node->size / 138);
            pthread_mutex_lock(&skiplist_imm_mutex);
            skiplist_to_imm(&db->SKIPLIST_meta_head, &db->SKIPLIST_IMM_meta_head);
            pthread_mutex_unlock(&skiplist_imm_mutex);
            // mutex
        }
        pthread_mutex_unlock(&skiplist_mutex);
        int i=0;
        // pthread_mutex_lock(&skiplist_imm_mutex);
        list_for_each(node, &db->SKIPLIST_IMM_meta_head) i++;
        // pthread_mutex_unlock(&skiplist_imm_mutex);
        if( i >= MAX_TABLE_NUM ){
            // save two imm table to disk
            // fprintf(db_log, "IMM TABLE to sstable\n");
            pthread_mutex_lock(&skiplist_imm_mutex);
            skiplist_meta_ptr_t item = list_last_entry(&db->SKIPLIST_IMM_meta_head, skiplist_meta_t, list);
            list_del_init(&item->list);
            pthread_mutex_unlock(&skiplist_imm_mutex);
            skiplist_to_keyValPair(item->head, &key_val_head);
            pthread_mutex_lock(&sstable_mutex);
            add_sstable_node(&db->SSTABLE_manager_head, 0, &key_val_head);
            pthread_mutex_unlock(&sstable_mutex);
            free_key_val_list(&key_val_head);
            free_skiplist(item, 4);
        }
    }
    pthread_exit(NULL);
}

int file_exists (char *filename) {
  struct stat buffer;
  return (stat(filename, &buffer) == 0);
}




void db_prepare(db_impl_ptr_t db){
    FILE *fp, *skip_fp;
    if(file_exists("./storage/SKIPLIST.MANIFEST")){

        fp = fopen("./storage/SKIPLIST.MANIFEST", "r");

        char buf[200];
        char dbname[50];
        int immu, num;
        memset(buf, '\0', 200);
        memset(dbname, '\0', 50);
        struct list_head key_val_head;
        INIT_LIST_HEAD(&key_val_head);
        while(fscanf(fp, "./storage/default_skiplist_%d_%d.skp\n", &immu, &num) != EOF){
            // generate skiplist
            sprintf(buf, "./storage/default_skiplist_%d_%d.skp", immu, num);
            printf("Read %s %d\n", buf, immu);
            read_sstable(buf, &key_val_head);
            if(immu == 1){
                gen_skiplist(&db->SKIPLIST_IMM_meta_head, &key_val_head, 4);
            }else {
                gen_skiplist(&db->SKIPLIST_meta_head, &key_val_head, 4);
            }
            memset(buf, '\0', 200);
            memset(dbname, '\0', 50);
            free_key_val_list(&key_val_head);
            INIT_LIST_HEAD(&key_val_head);
        }

        fclose(fp);
    }
    if(file_exists("./storage/SSTABLE.MANIFEST")){
        fp = fopen("./storage/SSTABLE.MANIFEST", "r");

        int lv, id;
        char buf[200];
        uint64_t keyfrom, keyto;
        memset(buf, '\0', 200);
        while(fscanf(fp, "%d %d %s %llu %llu\n", &lv, &id, buf, &keyfrom, &keyto) != EOF){
            printf("Read %s %d %d\n", buf, lv, id);
            sstable_restore_node(NULL, lv, id, keyfrom, keyto, &db->SSTABLE_manager_head);
            memset(buf, '\0', 200);
        }

        fclose(fp);
    }
}

void db_end(db_impl_ptr_t db) {
    db->end = 1;
    pthread_join(db->db_manager_thread, NULL);

    skiplist_meta_ptr_t meta_item, meta_safe;
    struct list_head key_val_head;
    INIT_LIST_HEAD(&key_val_head);

    char** name_list = malloc(sizeof(char*) * 15);
    uint64_t i=0;
    list_for_each_entry_safe(meta_item, meta_safe, &db->SKIPLIST_meta_head, list){
        INIT_LIST_HEAD(&key_val_head);
        skiplist_to_keyValPair(meta_item->head, &key_val_head);
        name_list[i] = skiplist_filename(NULL, 0, i);
        write_sstable(&key_val_head, name_list[i]);
        // add_sstable_node(&db->SSTABLE_manager_head, 0, &key_val_head);
        free_key_val_list(&key_val_head);
        free_skiplist(meta_item, 4);
        i++;
    }
    uint64_t ii=0;
    list_for_each_entry_safe(meta_item, meta_safe, &db->SKIPLIST_IMM_meta_head, list){
        INIT_LIST_HEAD(&key_val_head);
        skiplist_to_keyValPair(meta_item->head, &key_val_head);
        name_list[i] = skiplist_filename(NULL, 1, ii);
        write_sstable(&key_val_head, name_list[i]);
        // add_sstable_node(&db->SSTABLE_manager_head, 0, &key_val_head);
        free_key_val_list(&key_val_head);
        free_skiplist(meta_item, 4);
        i++;
        ii++;
    }

    FILE* fp = fopen("./storage/SKIPLIST.MANIFEST", "w");
    for(uint64_t _i = 0;_i<i;_i++){
        fprintf(fp, "%s\n", name_list[_i]);
    }
    fclose(fp);

    fp = fopen("./storage/SSTABLE.MANIFEST", "w");
    sstable_manager_ptr_t ss_mana_item;
    sstable_node_ptr_t ss_node_item;
    list_for_each_entry(ss_mana_item, &db->SSTABLE_manager_head, list){
        list_for_each_entry(ss_node_item, &ss_mana_item->sstable_head, list){
            fprintf(fp, "%d %d %s %llu %llu\n", ss_node_item->lv, ss_node_item->id, ss_node_item->filename, ss_node_item->keyfrom, ss_node_item->keyto);
        }
    }
    fclose(fp);
    // generate manifest file

}


void* sstable_manager(void* arg){
    db_impl_ptr_t db = (db_impl_ptr_t) arg;

    skiplist_meta_ptr_t skiplist_node;
    struct list_head key_val_head;
    INIT_LIST_HEAD(&key_val_head);
    struct list_head* node;
    FILE* db_log;
    // db_log = fopen("db.log", "w");
    while(db->end == 0){
        int i=0;
        // pthread_mutex_lock(&skiplist_imm_mutex);
        list_for_each(node, &db->SKIPLIST_IMM_meta_head) i++;
        // pthread_mutex_unlock(&skiplist_imm_mutex);
        if( i >= MAX_TABLE_NUM ){
            // save two imm table to disk
            pthread_mutex_lock(&skiplist_imm_mutex);
            skiplist_meta_ptr_t item = list_last_entry(&db->SKIPLIST_IMM_meta_head, skiplist_meta_t, list);
            list_del_init(&item->list);
            pthread_mutex_unlock(&skiplist_imm_mutex);
            skiplist_to_keyValPair(item->head, &key_val_head);
            // pthread_mutex_lock(&sstable_mutex);
            add_sstable_node(&db->SSTABLE_manager_head, 0, &key_val_head);
            // pthread_mutex_unlock(&sstable_mutex);
            free_key_val_list(&key_val_head);
            free_skiplist(item, 4);
        }
    }
    pthread_exit(NULL);
}
