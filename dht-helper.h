typedef struct node {
    int key;
    int value;
    struct node* next;
} Node;
 
typedef struct list {
    int size;
    Node* head;
} List;
 
List* create_list(void);
void add_to_list(List* list, int key, int value);
int lookup(int key, List* list);
void free_list(List* list);
void print_list(List* list);
void delete_from_list( List* list, int key);

