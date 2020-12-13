#include "skiplist.h"
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include "typedef.h"

static inline char* _strdup(char* str){
    if(str == NULL)
        return NULL;
    char* buf = malloc(sizeof(char) * strlen(str) + 1);
    memset(buf, '\0', strlen(str) + 1);
    strcpy(buf, str);
    return buf;
}



void init_skiplist(struct list_head* head, int height, int num){

    INIT_LIST_HEAD(head);
    // skiplist_meta_ptr_t* skiplists = malloc(sizeof(skiplist_meta_t) * num);
    for(int i=0;i<num;i++){
        skiplist_meta_ptr_t skiplist = malloc(sizeof(skiplist_meta_t));
        INIT_LIST_HEAD(&skiplist->list);
        skiplist->size = 0;
        skiplist->immu = 0;
        skiplist->total_height = height;
        create_list_head(&skiplist->head, height);
        // create_list_head(skiplists[i]->head, height);
        // skiplists[i]->size = 0;
        list_add(&skiplist->list, head);
    }
}

void insert_skiplist(struct list_head* skiplist_head, uint64_t key, char* val){

    skiplist_meta_ptr_t meta_item = list_first_entry(skiplist_head, skiplist_meta_t, list);
    int cap = insert(key, val, meta_item->head, meta_item->total_height);
    if(cap == -1){
        printf("Insert error\n");
    }
    meta_item->size += cap;
}

void skiplist_to_imm(struct list_head* skiplist_head, struct list_head* imm_skiplist_head){

    // list_del_init(skiplist_head);
    skiplist_meta_ptr_t skiplist_meta = list_first_entry(skiplist_head, skiplist_meta_t, list);
    list_del_init(&skiplist_meta->list);
    skiplist_meta->immu = 1;
    list_add(&skiplist_meta->list, imm_skiplist_head);

    skiplist_meta_ptr_t skiplist = malloc(sizeof(skiplist_meta_t));
    INIT_LIST_HEAD(&skiplist->list);
    skiplist->size = 0;
    skiplist->immu = 0;
    skiplist->total_height = skiplist_meta->total_height;
    int height = skiplist->total_height;
    create_list_head(&skiplist->head, height);
    // create_list_head(skiplists[i]->head, height);
    // skiplists[i]->size = 0;
    list_add(&skiplist->list, skiplist_head);
}

// TODO NOT FINISHED
char* find_skiplist(struct list_head* skiplist_head, uint64_t key) {

    // TODO traverse thrugh the heads not only first one
    if(list_empty(skiplist_head)){
        return NULL;
    }
    skiplist_meta_ptr_t meta_item = NULL;
    skiplist_ptr_t item;
    list_for_each_entry(meta_item, skiplist_head, list){
        item = find_exactly(meta_item->head, key, meta_item->total_height);
        if(item != NULL)
            return item->val;
    }
    return NULL;
}

char** scan_skiplist(struct list_head* skiplist_head, uint64_t keyfrom, uint64_t keyto){

    skiplist_meta_ptr_t meta_item = list_first_entry(skiplist_head, skiplist_meta_t, list);

    skiplist_ptr_t itemFrom = find(meta_item->head, keyfrom, meta_item->total_height);
    skiplist_ptr_t itemTo = find(meta_item->head, keyto, meta_item->total_height);


    char** str_list = malloc(sizeof(char*) * (keyto-keyfrom+1));

    for(uint64_t i=0;i<keyto-keyfrom+1;i++){
        str_list[i] = NULL;
    }
    skiplist_ptr_t item;
    itemFrom = get_lowest(itemFrom);
    itemTo = get_lowest(itemTo);
    assert(itemFrom->height == 0);
    assert(itemTo->height == 0);
    // printf("from %llu to %llu\n", itemFrom->key, itemTo->key);
    for(skiplist_ptr_t item=itemFrom; item != itemTo; item=list_entry(item->list.next, skiplist_node_t, list)){
        // printf("key %llu \n", item->key);
        if(item->key > keyto || item->key < keyfrom)
            continue;
        char* tmp = malloc(sizeof(char) * strlen(item->val) + 1);
        memset(tmp, '\0', strlen(item->val)+1);
        strcpy(tmp, item->val);
        str_list[item->key-keyfrom] = tmp;
    }
    return str_list;
}


void skiplist_to_keyValPair(struct list_head* heads, struct list_head* key_val_head){

    struct list_head* h = &heads[0];

    skiplist_ptr_t item;
    list_for_each_entry(item, h, list){
        key_val_pair_ptr_t kv = malloc(sizeof(key_val_pair_t));
        INIT_LIST_HEAD(&kv->list);
        kv->key = item->key;
        kv->val = _strdup(item->val);
        list_add_tail(&kv->list, key_val_head);
    }

    // key_val_pair_ptr_t node;
    // list_for_each_entry(node, key_val_head, list){
    //     printf("%llu\n",node->key);
    // }

}



void create_list_head(struct list_head** heads, int height) {

    // skiplist_ptr_t* list_heads;
    struct list_head* list_heads;

    // list_heads = malloc(sizeof(skiplist_ptr_t) * height);
    list_heads = malloc(sizeof(struct list_head) * height);
    skiplist_ptr_t new_node;
    struct list_head lv_head;
    for(int i=0;i<height;i++){
        INIT_LIST_HEAD(&list_heads[i]);
        // list_heads[i] = malloc(sizeof(skiplist_node_t));
        // list_heads[i]->height = i;
        // list_heads[i]->key = 1;
        // list_heads[i]->valid = 0;
        // list_heads[i]->total_height = height;
        // list_heads[i]->val = NULL;
        // INIT_LIST_HEAD(&(list_heads[i]->list));
        // INIT_LIST_HEAD(&(list_heads[i]->lv));
    }
    insert_initial_node(list_heads, 1, NULL, height);
    // int next, prev;
    // for(int i=0;i<height;i++){
    //     next = i-1 >= 0 ? i-1 : i-1+height;
    //     prev = i+1 < height ? i+1 : i+1-height;
    //     list_heads[i]->lv.next = &list_heads[next]->lv;
    //     list_heads[i]->lv.prev = &list_heads[prev]->lv;
    // }
    *heads = list_heads;
}



int insert(uint64_t key, char* val, struct list_head* heads, int height){
    skiplist_ptr_t cur, item, next_item;
    int64_t diff;
    int status;

    // initially
    if(list_empty(&heads[height-1])){
        insert_initial_node(heads, key, val, height);
        return 0;
    }
    next_item = find_closest(heads, heads[height-1].next, key, height-1, &status, 0);
    // item = find_greater_or_equal(heads, key, height-1);

    // if(item == NULL){

    // }
    if(next_item == NULL){
        printf("Wrong algorithm!\n");
        return 1;
    }else {
        switch(status){
            case NEXT:
                // create a new node and insert before it
                insert_new_node(heads, key, val, height);
                // insert_initial_node(heads, key, val, height);
                return 138;
            case EQUAL:
                // update the node
                update_node(next_item, val);
                return 0;
            case PREV:
                // create a new node and insert behind it
                insert_new_node(heads, key, val, height);
                return 138;
        }
    }
    return 0;

}
// TODO Not finished
skiplist_ptr_t find(struct list_head* heads, uint64_t key, int total_height){
    skiplist_ptr_t item, result;

    int status;

    item = find_closest(heads, heads[total_height-1].next, key, total_height-1, &status, 0);
    if(item == NULL){
        printf("Wrong algorithm!\n");
        return NULL;
    }else {
        switch(status){
            case NEXT:
                return item;
            case EQUAL:
                return item;
            case PREV:
                return item;
        }
    }
    return NULL;
}

skiplist_ptr_t find_exactly(struct list_head* heads, uint64_t key, int total_height){
    skiplist_ptr_t item, result;

    int status;

    item = find_closest(heads, heads[total_height-1].next, key, total_height-1, &status, 0);
    if(item == NULL){
        printf("Wrong algorithm!\n");
        return NULL;
    }else {
        switch(status){
            case NEXT:
                return NULL;
            case EQUAL:
                return item;
            case PREV:
                return NULL;
        }
    }
    return NULL;
}


skiplist_ptr_t find_greater_or_equal(
    struct list_head* heads,
    const uint64_t key, int index, struct list_head** prev){


    skiplist_ptr_t entry, next_entry;
    struct list_head* x = &heads[index];
    struct list_head* next = &heads[index];
    while(1){
        next = x->next;
        next_entry = next != &heads[index] ? list_entry(next, skiplist_node_t, list) : NULL;
        if(next_entry != NULL && next_entry->key < key){
            x = next;
        } else {
            if(prev != NULL) prev[index] = x;
            if(index == 0){
                return next;
                return next == &heads[index] ? NULL : next;
            } else {
                if(x == &heads[index--])
                    x = &heads[index];
                else{
                    entry = list_entry(x, skiplist_node_t, list);
                    x = entry->lv.next;
                }
            }
        }
    }
}


skiplist_ptr_t find_closest(
    struct list_head* heads,
    struct list_head* node,
    const uint64_t key, int index, int* status, int expected_index){


    if(index < 0){
        *status = NONE;
        return NULL;
    }
    skiplist_ptr_t entry;
    // first_entry = list_first_entry(&heads[index], skiplist_node_t, list);
    entry = list_entry(node, skiplist_node_t, list);
    // printf("height: %d, expected: %d, entry -> %llu, %s\n",entry->height, expected_index, entry->key, entry->val);
    if(entry->key == key){
        *status = EQUAL;
        return entry;
    }else if(entry->key < key){
        if(list_has_next(&entry->list, &heads[index])){
            skiplist_ptr_t next_entry = list_entry(entry->list.next, skiplist_node_t, list);
            if(next_entry->key == key){
                *status = EQUAL;
                return next_entry;
            }else if(next_entry->key < key){
                return find_closest(heads, &next_entry->list, key, index, status, expected_index);
            }else if(next_entry->key > key){
                if(index == expected_index){
                    *status = NEXT;
                    return next_entry;
                }else{
                    skiplist_ptr_t lv_next_item = list_entry(entry->lv.next, skiplist_node_t, lv);
                    return find_closest(heads, &lv_next_item->list, key, index-1, status, expected_index);
                }
            }
        }else {
            if(index == expected_index){
                *status = PREV;
                // printf("PREV\n");
                return entry;
            }else{
                // printf("Find next lv\n");
                skiplist_ptr_t lv_next_item = list_entry(entry->lv.next, skiplist_node_t, lv);
                return find_closest(heads, &lv_next_item->list, key, index-1, status, expected_index);
            }
        }
    }else if(entry->key > key){
        if(index == expected_index){
            *status = NEXT;
            return entry;
        }else{
            skiplist_ptr_t lv_next_item = list_entry(entry->lv.next, skiplist_node_t, lv);
            return find_closest(heads, &lv_next_item->list, key, index-1, status, expected_index);
        }
    }
    *status = NONE;
    return NULL;

}


void update_node(skiplist_ptr_t node, char* val){
    skiplist_ptr_t item;
    struct list_head lv_head;
    struct list_head *lv_prev = node->lv.prev;

    lv_head.next = &node->lv;
    lv_head.prev = node->lv.prev;
    node->lv.prev = &lv_head;
    lv_prev->next = &lv_head;
    list_for_each_entry(item, &lv_head, lv){
        set_val(item, val);
    }
    lv_head.next->prev = lv_head.prev;
    lv_head.prev->next = lv_head.next;
}

void create_new_node(skiplist_ptr_t* node, uint64_t key, char* val, int total_height, int cur_height){

    skiplist_ptr_t tmp;

    tmp = malloc(sizeof(skiplist_node_t));
    tmp->key = key;
    set_val(tmp, val);
    tmp->height = cur_height;
    tmp->total_height = total_height;
    INIT_LIST_HEAD(&tmp->list);
    INIT_LIST_HEAD(&tmp->lv);

    *node = tmp;
}

void insert_initial_node(struct list_head* heads, uint64_t key, char* val, int total_height){

    skiplist_ptr_t new_node;
    struct list_head lv_head;
    INIT_LIST_HEAD(&lv_head);
    for(int i=total_height-1;i>=0;i--){
        create_new_node(&new_node, key, val, total_height, i);
        list_add(&new_node->list, &heads[i]);
        list_add_tail(&new_node->lv, &lv_head);
    }

    // check
    // skiplist_ptr_t item;
    // int i=total_height;
    // list_for_each_entry(item, &lv_head, lv){
    //     printf("height: %d, %llu, %s\n", --i, item->key, item->val);
    // }
    lv_head.next->prev = lv_head.prev;
    lv_head.prev->next = lv_head.next;
}

void insert_new_node(struct list_head* heads, uint64_t key, char* val, int total_height){

    float pos = (float)(rand() % 1000) / 1000;
    // possibility
    // int total_height = next_node->total_height;
    int total_node = get_possibility(total_height);
    skiplist_ptr_t new_node, cur, item;
    struct list_head* tmp, *cur_lv;
    int status;
    struct list_head lv_head;
    INIT_LIST_HEAD(&lv_head);

    for(int i=0;i<total_node;i++){
        create_new_node(&new_node, key, val, total_height, i);
        skiplist_ptr_t closest_node = find_closest(heads, heads[total_height-1].next, key, total_height-1, &status, i);
        assert(closest_node->height == i);
        if(closest_node == NULL){
            printf("Wrong algorithm!\n");
            return;
        }else {
            switch(status){
                case NEXT:
                    // create a new node and insert before it
                    insert_before_node(&new_node->list, &closest_node->list);
                    break;
                case EQUAL:
                    // update the node
                    printf("Wrong algorithm equal!\n");
                    break;
                case PREV:
                    // create a new node and insert behind it
                    insert_after_node(&new_node->list, &closest_node->list);
                    break;
            }
        }
        list_add(&new_node->lv, &lv_head);
    }
    lv_head.next->prev = lv_head.prev;
    lv_head.prev->next = lv_head.next;
}


void set_val(skiplist_ptr_t node, char* val){
    if(val == NULL){
        node->valid = 0;
        node->val = NULL;
        return;
    }
    char* new_str = malloc(sizeof(char) * strlen(val) + 1);
    memset(new_str, '\0', strlen(val)+1);
    // if(node->valid == 1)
        // free(node->val);
    strcpy(new_str, val);
    node->valid = 1;
    node->val = new_str;
    free(val);
}


int get_possibility(int height){
    for(int i=height-1;i>=0;i--){
        if(rand_n(i) == 0){
            return i+1;
        }
    }
    return 1;
}

int rand_n(int bit){
    int mask = 0xffffffff ^ (0xffffffff << bit);
    int num = rand();
    int val = num & mask;
    // printf("0x%x & 0x%x = 0x%x\n", num, mask, val);
    return val;
}


skiplist_ptr_t get_lowest(skiplist_ptr_t item){

    skiplist_ptr_t tmp;
    tmp = list_entry(item->lv.next, skiplist_node_t, lv);
    while(tmp->height != 0){
        tmp = list_entry(tmp->lv.next, skiplist_node_t, lv);
    }

    return tmp;
}


void print_skiplist(struct list_head* heads, int total_height){

    printf("\n\n*************\n");
    for(int i=total_height-1;i>=0;i--){
        skiplist_ptr_t item;
        printf("H: %d ", i);
        list_for_each_entry(item, &heads[i], list){
            printf("%llu->", item->key);
        }
        printf("NIL\n");
    }

}

void check_skiplist_consistence(struct list_head* heads, int height){

    for(int i=height-1;i>=0;i--){
        skiplist_ptr_t item;
        uint64_t key_prev = -1;
        list_for_each_entry(item, &heads[i], list){
            if(key_prev == -1){
                key_prev = item->key;
            }else{
                // assert(key_prev < item->key);
                if(key_prev > item->key){
                    printf("height: %d, %llu > %llu\n", i, key_prev, item->key);
                    abort();
                }
                key_prev = item->key;
            }
        }
    }

}

void* skiplist_manager(void* arg){

    // change the skiplist to imm

}