#include "sstable.h"
#include <stdio.h>
#include <stdlib.h>
#include "parse_cmd.h"
#include <string.h>
#include "typedef.h"
#include <pthread.h>

static inline char* _strdup(char* str){
    if(str == NULL)
        return NULL;
    char* buf = malloc(sizeof(char) * strlen(str) + 1);
    memset(buf, '\0', strlen(str) + 1);
    strcpy(buf, str);
    return buf;
}

void free_key_val_list(struct list_head* head){
    key_val_pair_ptr_t item, safe;
    list_for_each_entry_safe(item, safe, head, list){
        free(item->val);
        list_del_init(&item->list);
        free(item);
    }
    INIT_LIST_HEAD(head);
}

int main(int argc, char* argv[]){

    char* inputFilePath = argv[1];

    FILE* fp_i = fopen(inputFilePath, "r");

    FILE* fp_o = fopen("output.out", "wb");
    uint64_t key1, key2;
    char* cmd, *value;
    cmd = malloc(sizeof(char) * 300);
    uint64_t written_size = 0;
    int nb = 0;
    struct list_head sstable_manager_head;
    init_sstable_manager(&sstable_manager_head);

    struct list_head key_val_head;
    INIT_LIST_HEAD(&key_val_head);
    key_val_pair_ptr_t key_val_node;
    uint64_t bin = 30000;
    uint64_t index =0;
    while(fscanf(fp_i, "%[^\n]\n", cmd) != EOF){
        switch(parse_cmd(cmd, &key1, &key2, &value)){
            case PUT:
                // printf("PUT %llu %s\n", key1, value);
                key_val_node = malloc(sizeof(key_val_pair_t));
                key_val_node->key = key1;
                key_val_node->val = _strdup(value);
                INIT_LIST_HEAD(&key_val_node->list);
                list_add_tail(&key_val_node->list, &key_val_head);
                index++;
                if(index % bin == 0){
                    add_sstable_node(&sstable_manager_head, 0, &key_val_head);
                    // printf("add successfully\n");
                    free_key_val_list(&key_val_head);
                    // printf("free successfully\n");
                    print_sstable_keyranges(&sstable_manager_head);
                }
                break;
            case GET:
                // printf("GET %llu\n", key1);
                break;
            case SCAN:
                // printf("SCAN %llu %llu\n", key1, key2);
                break;
        }
    }
    // sstable_node_ptr_t ssnode_item;
    // sstable_manager_ptr_t mana_item = list_first_entry(&sstable_manager_head, sstable_manager_t, list);
    // int i=0;
    // list_for_each_entry(ssnode_item, &mana_item->sstable_head, list){
    //     if(i++ < 3){
    //         merge_sstable(ssnode_item, &sstable_manager_head);
    //     }
    // }


    char* ans = sstable_find_key(&sstable_manager_head, 165528);
    printf("ans %s\n", ans);
    // struct list_head sstable_head;
    // printf("Written size %d\n", written_size);
    // int s = write_footer(nb, fp_o);
    // printf("size: %d\n", s);
    // fclose(fp_o);
    // INIT_LIST_HEAD(&sstable_head);
    // read_sstable("output.out", &sstable_head);
    // char* ans = find_key(20, &sstable_head);
    // printf("%s\n", ans);

    return 0;
}