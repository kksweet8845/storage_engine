#include "sstable.h"
#include <stdio.h>
#include "list.h"
#include "skiplist.h"
#include <string.h>
#include "filename.h"
#include <assert.h>
#include <pthread.h>
#include "mytime.h"

static inline char* _strdup(char* str){
    if(str == NULL)
        return NULL;
    char* buf = malloc(sizeof(char) * strlen(str) + 1);
    memset(buf, '\0', strlen(str) + 1);
    strcpy(buf, str);
    return buf;
}


/**
 * initialize the sstable manager, and set four lv 0 managers
 * default lv = 4;
 */
void init_sstable_manager(struct list_head* manager_head){
    INIT_LIST_HEAD(manager_head);
    int n = 4;
    for(int i=0;i<4;i++){
        sstable_manager_ptr_t node = malloc(sizeof(sstable_manager_t));
        node->lv = i;
        node->filenum = 0;
        node->fileIndex = 0;
        node->max_filenum = n;
        node->dbname = NULL;
        if(i == 0) n = 10;
        else n *= 10;
        INIT_LIST_HEAD(&node->sstable_head);
        INIT_LIST_HEAD(&node->list);
        list_add_tail(&node->list, manager_head);
    }
}

void create_sstable_manager(struct list_head* manager_head, int lv, int n){
    sstable_manager_ptr_t node = malloc(sizeof(sstable_manager_t));
    node->lv = lv;
    node->filenum = 0;
    node->fileIndex = 0;
    node->max_filenum = n*10;
    node->dbname = NULL;
    INIT_LIST_HEAD(&node->sstable_head);
    INIT_LIST_HEAD(&node->list);
    list_add_tail(&node->list, manager_head);
}

// void add_sstable_manager(struct list_head* )
sstable_node_ptr_t create_sstable_node(
    uint64_t keyfrom, uint64_t keyto,
    int id, char* dbname, uint32_t lv){

    sstable_node_ptr_t ss_node = malloc(sizeof(sstable_node_t));
    ss_node->id = id;
    ss_node->dbname = _strdup(dbname);
    // ss_node->filename = _strdup(sstable_filename(ss_node->dbname, lv, ss_node->id));
    ss_node->filename = sstable_filename(ss_node->dbname, lv, ss_node->id);
    ss_node->keyfrom = keyfrom;
    ss_node->keyto = keyto;
    ss_node->empty = 1;
    ss_node->size = 0;
    ss_node->lv = lv;
    INIT_LIST_HEAD(&ss_node->list);
    INIT_LIST_HEAD(&ss_node->vec);
    // printf("Create %s\n", ss_node->filename);
    return ss_node;
}

void add_sstable_node(struct list_head* manager_head, int lv, struct list_head* key_val_head){

    assert(lv == 0); // default: lv must be zero.
    sstable_manager_ptr_t item;
    uint64_t min, max;
    list_for_each_entry(item, manager_head, list){
        if(item->lv == lv && item->filenum < item->max_filenum){
            min = list_first_entry(key_val_head, key_val_pair_t, list)->key;
            max = list_last_entry(key_val_head, key_val_pair_t, list)->key;
            // min = (min / RANGEBIN) * RANGEBIN;
            // max = (max / RANGEBIN+1) * RANGEBIN;
            sstable_node_ptr_t ss_node = create_sstable_node(
                min, max,
                item->fileIndex, item->dbname, lv);
            // default write to disk, not store in vec
            write_sstable(key_val_head, ss_node->filename);
            ss_node->size = get_list_size(key_val_head);
            list_add(&ss_node->list, &item->sstable_head);
            item->filenum++;
            item->fileIndex++;
            // printf("keyrange %llu -> %llu\n", ss_node->keyfrom, ss_node->keyto);
            break;
        }else{
            // printf("Perform merge\n");
            struct timespec start, end;
            clock_gettime(CLOCK_MONOTONIC, &start);
            sstable_node_ptr_t ss_node = list_last_entry(&item->sstable_head, sstable_node_t, list);
            merge_sstable(&ss_node->list, manager_head);
            clock_gettime(CLOCK_MONOTONIC, &end);
            printf("%e sec\n", diff(start, end));
            item->filenum--;
            // // printf("%d\n", item->filenum);
            // ss_node = list_last_entry(&item->sstable_head, sstable_node_t, list);
            // merge_sstable(&ss_node->list, manager_head);
            // item->filenum--;
            // printf("%d\n", item->filenum);
            // sstable_node_ptr_t ss_node;
            min = list_first_entry(key_val_head, key_val_pair_t, list)->key;
            max = list_last_entry(key_val_head, key_val_pair_t, list)->key;
            ss_node = create_sstable_node(
                min, max,
                item->fileIndex, item->dbname, lv);
            // default write to disk, not store in vec
            write_sstable(key_val_head, ss_node->filename);
            ss_node->size = get_list_size(key_val_head);
            list_add(&ss_node->list, &item->sstable_head);
            item->filenum++;
            item->fileIndex++;
            break;
        }
    }
}

void add_sstable_node_to_manager(sstable_manager_ptr_t manager, struct list_head* key_val_head, struct list_head* manager_head){

    if(manager->filenum < manager->max_filenum){
        uint64_t min = list_first_entry(key_val_head, key_val_pair_t, list)->key;
        uint64_t max = list_last_entry(key_val_head, key_val_pair_t, list)->key;
        // printf("Upper min %llu, max %llu\n", min, max);
        min = (min / RANGEBIN) * RANGEBIN + 1;
        max = ((max-1) / RANGEBIN + 1) * RANGEBIN;
        // printf("Excessive save min %llu, max %llu\n", min, max);
        sstable_node_ptr_t ss_node = create_sstable_node(
            min, max,
            manager->fileIndex, manager->dbname, manager->lv);
        write_sstable(key_val_head, ss_node->filename);
        ss_node->size = get_list_size(key_val_head);
        list_add(&ss_node->list, &manager->sstable_head);
        free_key_val_list(key_val_head);
        manager->filenum++;
        manager->fileIndex++;
    } else {
        // printf("Perform merge\n");
        sstable_node_ptr_t ss_node = list_last_entry(&manager->sstable_head, sstable_node_t, list);
        merge_sstable(&ss_node->list, manager_head);
        manager->filenum--;
    }

}


void ssave_to_disk(sstable_manager_ptr_t manager, sstable_node_ptr_t node, struct list_head* manager_head){
    // check if need split
    struct list_head tmp_head;
    INIT_LIST_HEAD(&tmp_head);
    list_del_init(&node->list);
    if(node->size > SSTBALE_DEFAULT_SIZE){
        // printf("Node size is excessive\n");
        // perform split operation
        struct list_head* median = get_median_node(&node->vec);
        list_cut_position(&tmp_head, &node->vec, median);
        add_sstable_node_to_manager(manager, &tmp_head, manager_head);
    }
    uint64_t min = list_first_entry(&node->vec, key_val_pair_t, list)->key;
    uint64_t max = list_last_entry(&node->vec, key_val_pair_t, list)->key;
    // printf("Lower min %llu, max %llu\n", min, max);
    min = (min / RANGEBIN) * RANGEBIN+1;
    // printf("min = %llu\n", min);
    max = ((max-1) / RANGEBIN + 1) * RANGEBIN;
    node->keyfrom = min;
    node->keyto = max;
    node->size = get_sstable_size(node);
    write_sstable(&node->vec, node->filename);
    free_key_val_list(&node->vec);
    list_add(&node->list, &manager->sstable_head);
}

void write_sstable(struct list_head* key_val_head, char* filename){

    FILE* fp = fopen(filename, "w");
    // skiplist_meta_ptr_t item;
    key_val_pair_ptr_t item;
    int nb = 0;
    list_for_each_entry(item, key_val_head, list){
        // printf("key %llu\n", item->key);
        if(item->val == NULL)
            continue;
        write_data_block(item->key, item->val, fp);
        // printf("Write %llu %s\n", item->key, item->val);
        nb++;
    }
    // printf("%d\n", nb);
    write_footer(nb, fp);
    fclose(fp);
}

int write_data_block(uint64_t key, char* val, FILE* stream) {
    int written_b = fwrite(&key, 1, sizeof(key), stream);
    // unsigned char len = (unsigned char)strlen(val);
    // written_b += fwrite(&len, 1, sizeof(len), stream);
    written_b += fwrite(val, sizeof(char), 128, stream);
    return written_b;
}


int write_footer(uint64_t nb, FILE* stream){

    int written_b = fwrite(&nb, 1, sizeof(nb), stream);
    // written_b += fwrite(&indexing_block, sizeof(indexing_block), 1, stream);

    char padding = 0xff;
    while(written_b < FOOTER_FIXED_LENGTH - sizeof(MAGICNUMBER)){
        written_b += fwrite(&padding, 1, sizeof(char), stream);
    }
    uint32_t magic_number = MAGICNUMBER;
    written_b += fwrite(&magic_number, 1, sizeof(magic_number), stream);

    return written_b;
}


void read_sstable(char* filename, struct list_head* key_val_head){
    FILE* fp = fopen(filename, "rb");

    FILE* fp_cur = fp;
    fseek(fp_cur, -FOOTER_FIXED_LENGTH, SEEK_END);

    uint32_t magic_number;
    uint64_t data_offset;
    fread(&data_offset, sizeof(uint64_t), 1, fp_cur);
    // printf("%llu\n", *(uint64_t*)buffer);
    // printf("%d\n", FOOTER_FIXED_LENGTH - sizeof(uint32_t));
    fseek(fp_cur, FOOTER_FIXED_LENGTH - sizeof(uint32_t) - sizeof(data_offset), SEEK_CUR);
    fread(&magic_number, sizeof(uint32_t), 1, fp_cur);

    // printf("Read result\n");
    // printf("data_offset: 0x%x, magic_number: 0x%x\n", data_offset, magic_number);

    uint64_t key;
    unsigned char val_len;
    char* val = malloc(sizeof(char)*200);
    // fseek(fp, 0, SEEK_SET);
    fseek(fp_cur, 0, SEEK_SET);
    struct data_block {
        uint64_t key;
        char val[128];
    };

    struct data_block *data_ = malloc(sizeof(struct data_block) * data_offset);
    fread(data_, sizeof(struct data_block), data_offset, fp_cur);

    char* result;
    for(uint64_t i=0;i<data_offset;i++){
        key_val_pair_ptr_t node = malloc(sizeof(key_val_pair_t));
        node->key = data_[i].key;
        result = malloc(sizeof(char) * 129);
        memset(result, '\0', 129);
        strncpy(result, data_[i].val, 128);
        node->val = result;
        INIT_LIST_HEAD(&node->list);
        list_add_tail(&node->list, key_val_head);
    }
    free(data_);
    // fseek(fp_cur, 0, SEEK_SET);

    // for(int i=0;i<data_offset;i++){
    //     fread(&key, 1, sizeof(key), fp_cur);
    //     printf("%llu %llu\n", key, data_[i].key);
    //     assert(key == data_[i].key);
    //     // fread(&val_len, 1, sizeof(val_len), fp_cur);
    //     // printf("val_len %llu\n", val_len);
    //     memset(val, '\0', 200);
    //     fread(val, sizeof(char), 128, fp_cur);
    //     // printf("%llu %s\n", key, val);
    //     key_val_pair_ptr_t node = malloc(sizeof(key_val_pair_t));

    //     node->key = key;
    //     node->val = _strdup(val);
    //     INIT_LIST_HEAD(&node->list);
    //     list_add_tail(&node->list, key_val_head);
    // }
    // printf("read sstable length %llu\n", data_offset);
    free(val);
    fclose(fp);

}

char* _find_key_v2(char* filename, uint64_t target, struct list_head* key_val_head){
    FILE* fp = fopen(filename, "rb");

    FILE* fp_cur = fp;
    fseek(fp_cur, -FOOTER_FIXED_LENGTH, SEEK_END);

    uint32_t magic_number;
    uint64_t data_offset;
    // char* buffer = malloc(sizeof(char) * 200);
    fread(&data_offset, sizeof(uint64_t), 1, fp_cur);
    // printf("%llu\n", *(uint64_t*)buffer);
    // printf("%d\n", FOOTER_FIXED_LENGTH - sizeof(uint32_t));
    fseek(fp_cur, FOOTER_FIXED_LENGTH - sizeof(uint32_t) - sizeof(data_offset), SEEK_CUR);
    fread(&magic_number, sizeof(uint32_t), 1, fp_cur);

    // printf("Read result\n");
    // printf("data_offset: 0x%x, magic_number: 0x%x\n", data_offset, magic_number);

    uint64_t key;
    unsigned char val_len;
    char* val = malloc(sizeof(char)*200);
    // char* buf = malloc(sizeof(char)*200);
    // fseek(fp, 0, SEEK_SET);
    fseek(fp_cur, 0, SEEK_SET);
    struct data_block {
        uint64_t key;
        char val[128];
    };
    // sizeof(&val)
    struct data_block *data_ = malloc(sizeof(struct data_block) * data_offset);
    fread(data_, sizeof(struct data_block), data_offset, fp_cur);
    // fseek(fp_cur, 0, SEEK_SET);
    // struct list_head key_val_head;
    // INIT_LIST_HEAD(&key_val_head);
    // for(int i=0;i<data_offset;i++){
    //     fread(&key, 1, sizeof(key), fp_cur);
    //     // fread(&val_len, 1, sizeof(val_len), fp_cur);
    //     // printf("val_len %llu\n", val_len);
    //     memset(val, '\0', 200);
    //     fread(val, sizeof(char), 128, fp_cur);
    //     // printf("%llu %s\n", key, val);
        key_val_pair_ptr_t node = malloc(sizeof(key_val_pair_t));

        node->key = key;
        node->val = _strdup(val);
        INIT_LIST_HEAD(&node->list);
        list_add_tail(&node->list, &key_val_head);
    // }

    // // check
    // key_val_pair_ptr_t item;
    // uint64_t _i = 0;
    // list_for_each_entry(item, &key_val_head, list){
    //     // printf("size of data block %d\n", sizeof(struct data_block));
    //     // printf("%llu , %llu\n", item->key, data_[_i].key);
    //     assert(item->key == data_[_i++].key);
    // }



    char* result;
    for(uint64_t i=0;i<data_offset;i++){
        if(data_[i].key == target){
            // found
            result = malloc(sizeof(char) * 129);
            memset(result, '\0', 129);
            strncpy(result, data_[i].val, 128);
            for(uint64_t j=0;j<data_offset;j++){
                key_val_pair_ptr_t node = malloc(sizeof(key_val_pair_t));
                node->key = key;
                char* tmp = malloc(sizeof(char) * 129);
                memset(tmp, '\0', 129);
                strncpy(tmp, data_[j].val, 128);
                node->val = tmp;
                INIT_LIST_HEAD(&node->list);
                list_add_tail(&node->list, &key_val_head);
            }
            free(data_);
            fclose(fp);
            return result;
        }else if(data_[i].key > target){
            result = NULL;
            free(data_);
            fclose(fp);
            return result;
        }
    }
    free(data_);
    fclose(fp);
    return NULL;
}

// void* find_worker(void* arg){
//     struct tmp {
//         char* filename;
//         uint64_t target;
//     };
//     struct tmp* info = (struct tmp*) arg;
//     // printf("Open file %s\n", info->filename);
//     char* result = _find_key_v2(info->filename, info->target);
//     pthread_exit(result);
// }


char* sstable_find_key(struct list_head* manager_head, uint64_t key){

    sstable_manager_ptr_t mana_item;
    char* tmp;
    // pthread_t* threads = malloc(sizeof(pthread_t) * 100);
    uint64_t i=0;
    struct tmp {
        char* filename;
        uint64_t target;
    };
    // struct tmp* info = malloc(sizeof(struct tmp) * 100);
    list_for_each_entry(mana_item, manager_head, list){
        sstable_node_ptr_t ssNode_item;
        list_for_each_entry(ssNode_item, &mana_item->sstable_head, list){
            if(ssNode_item->keyfrom > key || ssNode_item->keyto < key )
                continue;
            // if(ssNode_item->empty == 1){
            //     read_sstable(ssNode_item->filename, &ssNode_item->vec);
            //     ssNode_item->empty = 0;
            //     ssNode_item->size = get_sstable_size(ssNode_item);
            //     // printf("Read into memory\n");
            // }
            // info[i].filename = ssNode_item->filename;
            // info[i].target = key;
            // pthread_create(&threads[i], NULL, find_worker, &info[i]);
            // printf("Pthread create %llu\n", i);
            if(ssNode_item->empty == 0){
                tmp = _find_key(key, &ssNode_item->vec);
            }else{
                tmp = _find_key_v2(ssNode_item->filename, key, &ssNode_item->vec);
            }
            // tmp = _find_key(key, &ssNode_item->vec);
            // i++;
            if(tmp != NULL){
                list_del_init(&ssNode_item->list);
                list_add(&ssNode_item->list, &mana_item->sstable_head);
                ssNode_item->empty = 0;
                return tmp;
            }
            // printf("sstable %d not found", ssNode_item->id);
        }
    }
    return NULL;
}


// void read_sstable_and_find(char* filename, struct list_head* sstable_head, uint64_t key){

// }

// void _read(uint64_t offset, char* filename, uint64_t n, uint64_t target){
//     FILE * fp = fopen(filename, "r");
//     uint64_t key;
//     unsigned char val_len;
//     char* val;
//     fseek(fp, offset, SEEK_SET);
//     fread(&key, 1, sizeof(key), fp);
//     if(target == key){
//         fread(&val_len, 1, sizeof(val_len), fp);
//         val = malloc(sizeof(char) * (n+1));
//         memset(val, '\0', n+1);
//         fread(val, sizeof(char), val_len, fp);
//         pthread_exit(val);
//     }
// }



char* _find_key(uint64_t key, struct list_head* head){
    key_val_pair_ptr_t item;
    list_for_each_entry(item, head, list){
        if(item->key != NULL && item->key == key)
            return item->val;
    }
    return NULL;
}


sstable_manager_ptr_t find_lv(int lv, struct list_head* sstable_manager_head){
    sstable_manager_ptr_t item;
    list_for_each_entry(item, sstable_manager_head, list){
        if(item->lv == lv) {
            return item;
        }
    }

    return NULL;
}

void free_key_val_list(struct list_head* head){
    key_val_pair_ptr_t item, safe;
    list_for_each_entry_safe(item, safe, head, list){
        list_del_init(&item->list);
        free(item->val);
        free(item);
    }
    INIT_LIST_HEAD(head);
}

void merge_sstable(struct list_head* sstable_node, struct list_head* sstable_manager_head){

    sstable_node_ptr_t merge_item = list_entry(sstable_node, sstable_node_t, list);
    // printf("Bus error %s %p\n", merge_item->filename, merge_item);
    // sstable_manager_ptr_t cur_lv_manager_item = find_lv(merge_item->lv, sstable_manager_head);
    sstable_manager_ptr_t next_lv_manger_item = find_lv(merge_item->lv+1, sstable_manager_head);

    // if(next_lv_manger_item == NULL){
    //     create_sstable_manager(sstable_manager_head, merge_item->lv+1, cur_lv_manager_item->max_filenum);
    //     next_lv_manger_item = find_lv(merge_item->lv+1, sstable_manager_head);
    // }
    // printf("Node %s merge to lv %d\n", merge_item->filename, next_lv_manger_item->lv);

    sstable_node_ptr_t* be_merged_node_arr = NULL;

    sstable_node_ptr_t item, safe;

    if(next_lv_manger_item->filenum >= next_lv_manger_item->max_filenum){
        // printf("GET merged\n");
        sstable_node_ptr_t next_lv_last_node = list_last_entry(&next_lv_manger_item->sstable_head, sstable_node_t, list);
        merge_sstable(&next_lv_last_node->list, sstable_manager_head);
        next_lv_manger_item->filenum--;
    }

    if(merge_item->empty == 1){
        read_sstable(merge_item->filename, &merge_item->vec);
        merge_item->empty = 0;
    }
    if(list_empty(&next_lv_manger_item->sstable_head)){
        // move to the next lv
        // printf("List empty\n");
        //assign this sstable to next lv (lv >= 1)
        update_sstable(next_lv_manger_item, merge_item, 1);
        list_del_init(&merge_item->list);
        list_add(&merge_item->list, &next_lv_manger_item->sstable_head);
        merge_item->keyfrom = (merge_item->keyfrom / RANGEBIN) * RANGEBIN;
        merge_item->keyto = (merge_item->keyto / RANGEBIN + 1) * RANGEBIN;
        next_lv_manger_item->filenum++;
        // printf("Return\n");
        printf("%s\n", merge_item->filename);
        // Need to create a new file or rename the file
        return;
    }
    struct list_head overlapped_head;
    INIT_LIST_HEAD(&overlapped_head);
    // sstable_manager_ptr_t mana_item;
    // list_for_each_entry(mana_item, sstable_manager_head, list){
    //     if(mana_item->filenum >= mana_item->max_filenum){
    //         // printf("GET merged\n");
    //         sstable_node_ptr_t next_lv_last_node = list_last_entry(&mana_item->sstable_head, sstable_node_t, list);
    //         merge_sstable(&next_lv_last_node->list, sstable_manager_head);
    //         mana_item->filenum--;
    //     }
    //     if(mana_item->lv > merge_item->lv){
    //         if(list_empty(&mana_item->sstable_head)){
    //             update_sstable(mana_item, merge_item, 1);
    //             list_del_init(&merge_item->list);
    //             list_add(&merge_item->list, &mana_item->sstable_head);
    //             merge_item->keyfrom = (merge_item->keyfrom / RANGEBIN) * RANGEBIN;
    //             merge_item->keyto = (merge_item->keyto / RANGEBIN + 1) * RANGEBIN;
    //             mana_item->filenum++;
    //             // printf("Return\n");
    //             printf("%s\n", merge_item->filename);
    //             // Need to create a new file or rename the file
    //             return;
    //         }
    //         list_for_each_entry_safe(item, safe, &mana_item->sstable_head, list) {
    //             // printf("%llu %llu %llu %llu\n", item->keyfrom, item->keyto, merge_item->keyfrom, merge_item->keyto);
    //             if( is_key_overlap(item->keyfrom, item->keyto, merge_item->keyfrom, merge_item->keyto) ){
    //                 // list_del_init(&item->list);
    //                 if(item->empty == 1){
    //                     read_sstable(item->filename, &item->vec);
    //                     item->empty = 0;
    //                 }
    //                 merge_vec(&item->vec, &merge_item->vec, item->keyfrom, item->keyto);
    //                 ssave_to_disk(mana_item, item, sstable_manager_head);
    //                 // list_add(&item->list, )
    //                 // list_add_tail(&item->list, &overlapped_head);
    //                 // next_lv_manger_item->filenum--;
    //             }
    //         }
    //     }
    // }
    // if(!list_empty(&merge_item->vec)){

    // }
    list_for_each_entry_safe(item, safe, &next_lv_manger_item->sstable_head, list) {
        // printf("%llu %llu %llu %llu\n", item->keyfrom, item->keyto, merge_item->keyfrom, merge_item->keyto);
        if( is_key_overlap(item->keyfrom, item->keyto, merge_item->keyfrom, merge_item->keyto) ){
            list_del_init(&item->list);
            // if(item->empty == 1){
            //     read_sstable(item->filename, &item->vec);
            //     item->empty = 0;
            // }
            // merge_vec(&item->vec, &merge_item->vec, item->keyfrom, item->keyto);
            // ssave_to_disk(next_lv_manger_item, item, sstable_manager_head);
            // list_add(&item->list, )
            list_add_tail(&item->list, &overlapped_head);
            next_lv_manger_item->filenum--;
        }
    }
    // if(!list_empty(&merge_item->vec)){
    //     merge_item->lv += 1;
    //     printf("Here\n");
    //     merge_sstable(&merge_item->list, sstable_manager_head);
    // }else{
    //     remove(merge_item->filename);
    //     list_del_init(&merge_item->list);
    //     free_sstable(merge_item);
    //     printf("IJIJIJ\n");
    // }



    remove(merge_item->filename);
    struct list_head vec_head;
    INIT_LIST_HEAD(&vec_head);
    list_for_each_entry_safe(item, safe, &overlapped_head, list){
        if(item->empty == 1){
            read_sstable(item->filename, &item->vec);
            item->empty = 0;
        }
        merge_vec(&item->vec, &vec_head, item->keyfrom, item->keyto);
        remove(item->filename);
        // printf("free sstable %s, range %llu -> %llu\n", item->filename, item->keyfrom, item->keyto);
        list_del_init(&item->list);
        free_sstable(item);
    }
    merge_vec(&merge_item->vec, &vec_head, merge_item->keyfrom, merge_item->keyto);
    struct list_head **ptr;
    key_val_pair_ptr_t vec_item, vec_safe_item;
    uint64_t min = list_first_entry(&vec_head, key_val_pair_t, list)->key;
    uint64_t max = (min / RANGEBIN+1) * RANGEBIN;
    min = (min / RANGEBIN) * RANGEBIN +1;
    struct list_head extracted_key_val_head;
    INIT_LIST_HEAD(&extracted_key_val_head);
    uint64_t key = 0;
    list_for_each_entry_safe(vec_item, vec_safe_item, &vec_head, list){
        key++;
        if(key % RANGEBIN == 0){
            min = list_first_entry(&vec_head, key_val_pair_t, list)->key;
            min = (min / RANGEBIN) * RANGEBIN + 1;
            max = (vec_item->key / RANGEBIN + 1) * RANGEBIN;
            list_cut_position(&extracted_key_val_head, &vec_head, &vec_item->list);
            sstable_node_ptr_t ss_node = create_sstable_node(
                min, max,
                next_lv_manger_item->fileIndex, next_lv_manger_item->dbname, next_lv_manger_item->lv);
            write_sstable(&extracted_key_val_head, ss_node->filename);
            ss_node->size = get_list_size(&extracted_key_val_head);
            list_add(&ss_node->list, &next_lv_manger_item->sstable_head);
            next_lv_manger_item->filenum++;
            next_lv_manger_item->fileIndex++;
            // printf("create node %s to lv %d, range %llu -> %llu\n", ss_node->filename, ss_node->lv, ss_node->keyfrom, ss_node->keyto);
            free_key_val_list(&extracted_key_val_head);
        }
    }
    if(!list_empty(&vec_head)){
        min = list_first_entry(&vec_head, key_val_pair_t, list)->key;
        max = list_last_entry(&vec_head, key_val_pair_t, list)->key;
        min = (min / RANGEBIN) * RANGEBIN + 1;
        max = (max / RANGEBIN + 1) * RANGEBIN;
        sstable_node_ptr_t ss_node = create_sstable_node(
            min, max,
            next_lv_manger_item->fileIndex, next_lv_manger_item->dbname, next_lv_manger_item->lv);
        write_sstable(&vec_head, ss_node->filename);
        ss_node->size = get_list_size(&extracted_key_val_head);
        list_add(&ss_node->list, &next_lv_manger_item->sstable_head);
        next_lv_manger_item->filenum++;
        next_lv_manger_item->fileIndex++;
        // printf("create node %s to lv %d, range %llu -> %llu\n", ss_node->filename, ss_node->lv, ss_node->keyfrom, ss_node->keyto);
    }

    remove(merge_item->filename);
    list_del_init(&merge_item->list);
    free_sstable(merge_item);
    // printf("End of loop of merged\n");
    // if(!list_empty(&merge_item->vec)){
    //     uint64_t min = list_first_entry(&merge_item->vec, key_val_pair_t, list)->key;
    //     uint64_t max = list_last_entry(&merge_item->vec, key_val_pair_t, list)->key;
    //     merge_item->keyfrom = (min / RANGEBIN) * RANGEBIN + 1;
    //     merge_item->keyto = ((max-1) / RANGEBIN + 1) * RANGEBIN;
    //     remove(merge_item->filename);
    //     update_sstable(next_lv_manger_item, merge_item, 0);
    //     ssave_to_disk(next_lv_manger_item, merge_item, sstable_manager_head);
    //     list_del_init(&merge_item->list);
    //     list_add(&merge_item->list, &next_lv_manger_item->sstable_head);
    //     next_lv_manger_item->filenum++;
    // }else {
    //     // delete the node
    //     remove(merge_item->filename);
    //     list_del_init(&merge_item->list);
    //     free_sstable(merge_item);
    // }
    // printf("Finished merged\n");
}

void merge_vec(struct list_head* vec_from, struct list_head* vec_to, uint64_t keyfrom, uint64_t keyto){

    key_val_pair_ptr_t item, safe;
    struct list_head tmp_head, final_head;
    INIT_LIST_HEAD(&tmp_head);
    INIT_LIST_HEAD(&final_head);
    list_for_each_entry_safe(item, safe, vec_from, list){
        if(item->key >= keyfrom && item->key <= keyto){
            list_del_init(&item->list);
            list_add_tail(&item->list, &tmp_head);
        }
    }


    int left_empty, right_empty;
    struct list_head* left_head, *right_head, *merge_head;
    merge_head = &final_head;
    left_head = &tmp_head;
    right_head = vec_to;
    merge_op(left_head, right_head, merge_head);
    assert(list_empty(vec_to));
    INIT_LIST_HEAD(vec_to);
    vec_to->next = merge_head->next;
    vec_to->prev = merge_head->prev;
    merge_head->next->prev = vec_to;
    merge_head->prev->next = vec_to;
}

void merge_op(struct list_head* left_head, struct list_head* right_head, struct list_head* merge_head){

    int left_empty, right_empty;
    left_empty = list_empty(left_head);
    right_empty = list_empty(right_head);

    key_val_pair_ptr_t left, right;
    while( !left_empty && !right_empty ){
        left = list_first_entry(left_head, key_val_pair_t, list);
        right = list_first_entry(right_head, key_val_pair_t, list);
        if(left->key <= right->key){
            list_del_init(&left->list);
            list_add_tail(&left->list, merge_head);
            if(left->key == right->key){
                list_del_init(&right->list);
                free(right->val);
                free(right);
            }
        }else {
            list_del_init(&right->list);
            list_add_tail(&right->list, merge_head);
        }
        left_empty = list_empty(left_head);
        right_empty = list_empty(right_head);
    }

    if(!left_empty && right_empty){
        while(!left_empty) {
            left = list_first_entry(left_head, key_val_pair_t, list);
            list_del_init(&left->list);
            list_add_tail(&left->list, merge_head);
            left_empty = list_empty(left_head);
        }
    }

    if(left_empty && !right_empty){
        while(!right_empty){
            right = list_first_entry(right_head, key_val_pair_t, list);
            list_del_init(&right->list);
            list_add_tail(&right->list, merge_head);
            right_empty = list_empty(right_head);
        }
    }

}


void update_sstable(sstable_manager_ptr_t manager, sstable_node_ptr_t node, int rn) {

    node->id = manager->fileIndex;
    manager->fileIndex++;
    node->lv = manager->lv;
    // TODO need to update the dbname
    char* tmp = _strdup(sstable_filename(manager->dbname, manager->lv, node->id));
    if(rn == 1)
        rename(node->filename, tmp);
    node->filename = tmp;
}


void free_sstable(sstable_node_ptr_t node){
    free(node->filename);
    free(node);
}

int is_key_overlap(int _1_keyfrom, int _1_keyto, int _2_keyfrom, int _2_keyto){
    return (_1_keyfrom < _2_keyto && _1_keyto > _2_keyfrom) ||
            (_2_keyfrom < _1_keyto && _2_keyto > _1_keyfrom);
}

int get_list_length(struct list_head* head){
    struct list_head* node;
    int i=0;
    list_for_each(node, head) i++;
    return i;
}

int get_sstable_size(sstable_node_ptr_t node){
    if(node->empty)
        return -1;
    int len = get_list_length(&node->vec);
    return 64 + (8+1+129) * len;
}

int get_list_size(struct list_head* head){
    int len = get_list_length(head);
    return 64 + (8+1+129) * len;
}

struct list_head* get_median_node(struct list_head* head){
    struct list_head *cur, *h_cur;
    uint64_t max = list_last_entry(head, key_val_pair_t, list)->key;
    uint64_t min = list_first_entry(head, key_val_pair_t, list)->key;
    uint64_t median = (((max-min-1) / RANGEBIN + 1) * RANGEBIN) /2;
    min = (min / RANGEBIN) * RANGEBIN;
    median = (median / RANGEBIN) * RANGEBIN + min;

    key_val_pair_ptr_t item, safe;
    list_for_each_entry_safe(item, safe, head, list){
        if(item->key <= median && safe->key >= median+1) break;
    }
    return &item->list;
}

void print_sstable_keyranges(struct list_head* sstable_manager_head){

    sstable_manager_ptr_t mana_item;
    list_for_each_entry(mana_item, sstable_manager_head, list){
        sstable_node_ptr_t item;
        list_for_each_entry(item, &mana_item->sstable_head, list){
            printf("lv %d, id %d,  keyfrom %llu, keyto %llu\n", item->lv, item->id, item->keyfrom, item->keyto);
        }
    }
}

void sstable_restore_node(char* dbname, int lv, int id, uint64_t keyfrom, uint64_t keyto, struct list_head* manage_head){

    sstable_node_ptr_t ss_node = create_sstable_node(keyfrom, keyto, id, dbname, lv);
    sstable_manager_ptr_t manager_item = find_lv(lv, manage_head);
    list_add(&ss_node->list, &manager_item->sstable_head);
    manager_item->filenum++;
    if(id+1 > manager_item->fileIndex)
        manager_item->fileIndex = id+1;
}