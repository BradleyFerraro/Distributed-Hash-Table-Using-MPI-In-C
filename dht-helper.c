#include <stdio.h>
#include <stdlib.h>
#include "dht-helper.h"


List* create_list() {
    List* new_list = (List*)malloc(sizeof(List));
    new_list->size = 0;
    new_list->head = NULL;
    return new_list;
}
 
void add_to_list(List* list, int key, int value) {
    Node* new_node = (Node*)malloc(sizeof(Node));
    new_node->key = key;
    new_node->value = value;
    new_node->next = list->head;
    list->head = new_node;
    list->size++;
}
 

void delete_from_list( List* list, int key) {
    Node* parent;
    Node* current = list->head;
    // handle first node on the list
    if (current->key == key) {
        list->head = current->next;
        free(current);
        list->size--;
        return; // done, bail out
    }
    //key is not in the first node on the list
    parent = current;
    current = current->next;
    // churn through the list looking for key
    while ((current->key != key) && (current != NULL)) {
            parent = current;
            current = current->next;
    }
    // at this point we are at the key or end of list
    
    if (current == NULL){ // key not found on list, should never happen
        printf("^ERROR^ in delete_from_list, ney not found.");
        return; // bail out
    }
    // remove node and free it    
    parent->next = current->next;
    free(current);
    list->size--;
    return;
}


int lookup(int key, List* list) {
    Node* current_node = list->head;
    while (current_node != NULL){
        if (current_node->key == key) {
            return current_node->value;
        }
        Node* next_node = current_node->next;
        current_node = next_node;
    }
    printf("^ERROR^ Program abort, Key not found on list.\n");
    exit(-1);
}
 

void free_list(List* list) {
    Node* current_node = list->head;
    while (current_node != NULL) {
        Node* next_node = current_node->next;
        free(current_node);
        current_node = next_node;
    }
    free(list);
}
 
void print_list(List* list) {
    Node* current_node = list->head;
    while (current_node != NULL) {
        printf("  (key:%3d value:%4d)\n",current_node->key,current_node->value);
        current_node = current_node->next;
    }
}
 
