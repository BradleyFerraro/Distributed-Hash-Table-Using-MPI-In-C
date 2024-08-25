#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include "dht.h"
#include "dht-helper.h"

static int myStorageId=-1, childRank=-1, myRank, numProcesses;
static struct list* data;


// print out info about this node;
void printme(void){
    printf("  myRank:%d myStorageId:%4d childRank:%d numProcesses:%d \n",myRank,myStorageId,childRank,numProcesses);
    if (myStorageId > 0) print_list(data);
    printf("\n");
}

void debugPrint(int source){
    // I have received a debug print message so print myself
    int dummy;
    MPI_Recv(&dummy, 1, MPI_INT, source, DBGPRINT, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    printme();
    MPI_Send(&dummy, 1, MPI_INT, source, ACK, MPI_COMM_WORLD);

}




// on an END message, the head node is to contact all storage nodes and tell them
void headEnd(void) {
  int i, dummy;
  // the head node knows there is an END message waiting to be received.  Now we just receive it.
  MPI_Recv(&dummy, 1, MPI_INT, numProcesses-1, END, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
  // tell all the storage nodes to END
  for (i = 1; i < numProcesses-1; i++) {
    MPI_Send(&dummy, 1, MPI_INT, i, END, MPI_COMM_WORLD);
  }
    MPI_Finalize();
  exit(0);
}

// on an END message, a storage node just calls MPI_Finalize and exits
void storageEnd(void) {
  int dummy;  // the data is unimportant for an END
  MPI_Recv(&dummy, 1, MPI_INT, 0, END, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
  MPI_Finalize();
  exit(0);
}

// handle the return of a value from a get operation
void retval(int source) {
 int *args;
 args = (int *) malloc (2 * sizeof(int));

    // consume the retval message into the above storage
    MPI_Recv(args, 2, MPI_INT, source, RETVAL, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    if ((myRank != 0) && (myRank != numProcesses-1) ){
        // pass the msg to child
        MPI_Send(args, 2, MPI_INT, childRank, RETVAL, MPI_COMM_WORLD);
    } else if (myRank == 0) { // i am head
        // send return value to the command node
        MPI_Send(args, 2, MPI_INT, numProcesses-1, RETVAL, MPI_COMM_WORLD);

    } else { printf("^ERROR^ in retval\n"); exit(-1); }

  free(args);
}
void getKeyVal(int source) {
  int *argsAdd;
  int key;

  // receive the GET message
  // note that at this point, we've only called MPI_Probe, which only peeks at the message
  // we are receiving the key from whoever sent us the message 
  MPI_Recv(&key, 1, MPI_INT, source, GET, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

  if (key <= myStorageId) {  // I own this key/value pair
    // allocate two integers: the first will be the value, the second will be this storage id
    argsAdd = (int *) malloc (2 * sizeof(int));

    // find the associated value (called "value") using whatever data structure you use
    // you must add this code to find it (omitted here)
      
    argsAdd[0] = lookup(key, data);
    argsAdd[1] = myStorageId;
    // send this value around the ring
    MPI_Send(argsAdd, 2, MPI_INT, childRank, RETVAL, MPI_COMM_WORLD);
    free(argsAdd);
  }
  else {  // I do NOT own this key/value pair; just forward request to next hop
    MPI_Send(&key, 1, MPI_INT, childRank, GET, MPI_COMM_WORLD);
  }
}

void putkeyval(int source) {
    int argsPut[2];
    int key, value, dummy;
    
    // Not &key, should be argsPUT
    MPI_Recv(argsPut, 2, MPI_INT, source, PUT, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    key = argsPut[0];
    value = argsPut[1];
    
    // printf("key:%d myStorageid:%d\n",key,myStorageId);
    if (myRank == 0 || key > myStorageId) {
        // send put along
        MPI_Send(argsPut, 2, MPI_INT, childRank, PUT, MPI_COMM_WORLD );
    }
    else {
        // put into data
        add_to_list(data, key, value);
        // send ack back to source who then sends it along to its child. this is necessary only for the elegant move keys
        // MPI_Send(&dummy, 1, MPI_INT, source, ACK, MPI_COMM_WORLD);
        // if we are not using the elegant move keys then use this ack
        MPI_Send(&dummy, 1, MPI_INT, childRank, ACK, MPI_COMM_WORLD);
    }
}


void ack(int source) {
    int dummy;
    
    MPI_Recv(&dummy, 1, MPI_INT, source, ACK, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    
    if (myRank == 0) {
        // send ack to command
        MPI_Send(&dummy, 1, MPI_INT, numProcesses-1, ACK, MPI_COMM_WORLD );
    }
    else {
        // send ack to child
        MPI_Send(&dummy, 1, MPI_INT, childRank, ACK, MPI_COMM_WORLD);
    }
}

void putKeys(int source) { //add multiple key/value pairs to my list
    // find out how many key/value pairs we will be adding
    int count, dummy, i;
    MPI_Status status;

    MPI_Probe(source, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
    MPI_Get_count(&status, MPI_INT, &count);
    int* args = malloc(sizeof(int) * count);

    MPI_Recv(args, count, MPI_INT, source, PUTKEYS, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    for (i=0;i<count;i+=2){
        //printf(" add Key/value (%d/%d)\n",args[i],args[i+1]);
        add_to_list(data, args[i], args[i+1]);
    }
    MPI_Send(&dummy, 1, MPI_INT, source, ACK, MPI_COMM_WORLD);

    
}

void moveKeys(int toNode, int newId){
    //printf("data list before any moves\n");
    //print_list(data);
    //printf("after print data list before any moves\n");

    // the source will send a message containting the key/vaue pairs to add to our list.
    // build a new list of key/values to move and pass them in a single message to toNode 
    int i, saveKey, argsCount=0, dummy, numKeys;
    struct list* keysToMove=create_list(); // create a new linked list of key/value pairs to move
    Node* current = data->head;
    while (current != NULL) { //build list of key/values to move
        if (current->key <= newId) {
            add_to_list(keysToMove, current->key, current->value); // add the pair to the temp list
            saveKey=current->key;
            current=current->next;
            delete_from_list(data, saveKey); // delete the pair from data
        } else current=current->next;
    }
    // at this point we have a new (potentially empty) list of keys to move and they have been removed from data
    // now allocate the args to pass in the msg
    //print_list(keysToMove);
    if (keysToMove->size != 0) { // not an empty list
        Node* current = keysToMove->head;
        numKeys =keysToMove->size;
        int* args= (int*)malloc(sizeof(int) * keysToMove->size *2); //space for key/value pairs
        for (i=0; (i < numKeys); i++){ //build the args for the msg
                args[i*2] = current->key; saveKey=current->key;
                args[i*2 +1] = current->value;
                argsCount++;
                //printf("args[%i)],args[%i] (%d,%d)\n",i*2,i*2+1,args[i*2],args[i*2+1]);
                current=current->next;
        } //for loop
        free_list(keysToMove); //free memory used for the list
        // send a message to toNode to add the key/values to its list
        //printf ("Sending msg to putkeys wiht %d arguments\n",argsCount*2);
        MPI_Send(args, argsCount*2, MPI_INT, toNode, PUTKEYS, MPI_COMM_WORLD);
        MPI_Recv(&dummy, 1, MPI_INT, toNode, ACK, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        free(args);
        
    } //if not an empty list
    
}

        
        
void add(int source) {
    int argsAdd[2];
    int newNodeRank, newId, dummy;
    
    MPI_Recv(argsAdd, 2, MPI_INT, source, ADD, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    
    newNodeRank = argsAdd[0];
    newId       = argsAdd[1];
    
    if (newId > myStorageId) {
        // send add along
        MPI_Send(argsAdd, 2, MPI_INT, childRank, ADD, MPI_COMM_WORLD );
        // return to the case statement and wait for something to do
    }
    else {
        // tell the idle rank to become a data node with child args[0] and Id args[1]
        argsAdd[0] = myRank; //args[1] is the newId
        MPI_Send(argsAdd, 2, MPI_INT, newNodeRank, MAKENODE, MPI_COMM_WORLD);
        // wait for it to ack that the new node is created
        MPI_Recv(&dummy, 1, MPI_INT, newNodeRank, ACK, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        // the sources's child points to new node
        MPI_Send(&newNodeRank, 1, MPI_INT, source, CHCHILD, MPI_COMM_WORLD);
        MPI_Recv(&dummy, 1, MPI_INT, source, ACK, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        // redistribute data
        //if we have any keys that are greater than our id then send them to newNode.

/* this is the more elegent method of moveing key/values but it it disallowed by the spec
        Node* current = data->head;
        int keyval[2];
        while (current != NULL) {
            if (current->key <= newId) {
                keyval[0] = current->key;
                keyval[1] = current->value;
                MPI_Send(keyval, 2, MPI_INT, newNodeRank, PUT, MPI_COMM_WORLD);
                MPI_Recv(&dummy, 1, MPI_INT, newNodeRank, ACK, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                current=current->next;
                delete_from_list( data, keyval[0]);
            } else current=current->next;
        }
 */ //end elegant solution
        // this is the less elegant solution 
        moveKeys(newNodeRank,newId); // move all appropriate key/value pairs in one msg
        
        // send ack that add is complete
        MPI_Send(&dummy, 1, MPI_INT, source, ACK, MPI_COMM_WORLD);
        }
}


void makenode(int source) {
    int argsmakenode[2];
    int dummy;
    MPI_Recv(argsmakenode, 2, MPI_INT, source, MAKENODE, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    //init the new node
    myStorageId = argsmakenode[1];
    childRank = argsmakenode[0];
    data = create_list();
    MPI_Send(&dummy, 1, MPI_INT, source, ACK, MPI_COMM_WORLD);
}



void removeNode(int source) {
    int args[2];
    int RemoveNodeId, dummy;
    
    MPI_Recv(&RemoveNodeId, 1, MPI_INT, source, REMOVE, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    if (RemoveNodeId==MAX) {printf("^ERROR^ abort, can not remove node with MAX ID \n"); exit(-1);}

    if (myStorageId != RemoveNodeId) {
        // send remove along
        MPI_Send(&RemoveNodeId, 1, MPI_INT, childRank, REMOVE, MPI_COMM_WORLD );
        // return to the case statement and wait for something to do
    }
    else { // we are the node to be removed
        // our child is to receive all our data
        // source's child becomes ourChild
        // send an mpi msg to source telling it to change its child to ourChild
        MPI_Send(&childRank, 1, MPI_INT, source, CHCHILD, MPI_COMM_WORLD);
        // wait for that message to be ack'ed.
        MPI_Recv(&dummy, 1, MPI_INT, source, ACK, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
 
        // at this point we are no longer in the chain of nodes,
        //send all our data to ourChild

/* this is the elegant solution for moving keys but is not allowed by the spec
        Node* current = data->head;
        int keyval[2];
        while (current != NULL) {
                keyval[0] = current->key;
                keyval[1] = current->value;
                MPI_Send(keyval, 2, MPI_INT, childRank, PUT, MPI_COMM_WORLD);
                MPI_Recv(&dummy, 1, MPI_INT, childRank, ACK, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                current=current->next;
                delete_from_list( data, keyval[0]);
        } //end while
*/ // end elegant solution
        
        // this is the less elegant solution for moving keys all at once.
        // here we use MAX for the id because we don't know what our child' id is but all our keys are to be moved there.
        moveKeys(childRank, MAX);

        // make us inactive
        myStorageId=-1; childRank=-1; free(data);
        // send ack that remove is complete;
        MPI_Send(&dummy, 1, MPI_INT, source, ACK, MPI_COMM_WORLD);
    }// end else
}

void chgChild(int source) {
 int newChild, dummy;
  MPI_Recv(&newChild, 1, MPI_INT, source, CHCHILD, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
  childRank=newChild;
  MPI_Send(&dummy, 1, MPI_INT, source, ACK, MPI_COMM_WORLD);
}










// handleMessages repeatedly gets messages and performs the appropriate action
void handleMessages(void) {
  MPI_Status status;
  int count, source, tag;

  while (1) {
    // wait for and peek at the message
    MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
    // this call just gets how many integers are in the message
    MPI_Get_count(&status, MPI_INT, &count);
    // get the source and the tag---which MPI rank sent the message, and
    // what the tag of that message was (the tag is the command)
    source = status.MPI_SOURCE;
    tag = status.MPI_TAG;

      
    // now take the appropriate action
    // code for END and most of GET is given; others require your code
//      printf("MyRank: %d, tag: %d\n",myRank,tag);
      switch(tag) {
      case END:
        if (myRank == 0) headEnd(); else storageEnd();
        break;
      case ADD:
        add(source);
        break;
      case REMOVE:
        removeNode(source);
        break;
      case PUT:
        putkeyval(source);
        break;
      case GET:
        getKeyVal(source);
        break;
      case ACK:
        ack(source);
        break;
      case RETVAL:
        retval(source); // pass the msg along
        break;
      case MAKENODE:
        makenode(source);
        break;
      case CHCHILD:
        chgChild(source);
        break;
      case PUTKEYS:
        putKeys(source);
        break;
      case DBGPRINT:
        debugPrint(source); // debug routine to print out a node
        break;
      default:
        // should never be reached---if it is, something is wrong with your code; just bail out
        printf("ERROR, my id is %d, source is %d, tag is %d, count is %d\n", myRank, source, tag, count);
        exit(1);
    } 
  }
}

int main(int argc, char *argv[]) {
  MPI_Init(&argc, &argv);
    
  // get my rank and the total number of processes
  MPI_Comm_rank(MPI_COMM_WORLD, &myRank);
  MPI_Comm_size(MPI_COMM_WORLD, &numProcesses);

//  DBGnumProcesses = numProcesses; // for debugging we need this available in command

  // set up the head node and the last storage node
  if (myRank == 0) { //head
    myStorageId = 0;
    childRank = numProcesses-2; // storage node id 1000
  }
  else if (myRank == numProcesses-2) { // 3 storage node with id 1000
    myStorageId = MAX;
    childRank = 0; // point to the head
    data=create_list();
  }
    
  // the command node is handled separately
  if (myRank < numProcesses-1) { // I am head or storage
      handleMessages();
  }
  else { // I am command node
    commandNode();  // this is where stuff happens 
  }
  return 0;
}
