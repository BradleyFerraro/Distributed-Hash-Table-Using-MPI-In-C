#define MAX 1000
#define PUT 0
#define GET 1
#define ADD 2
#define REMOVE 3
#define END 4
#define RETVAL 5
#define ACK 6
#define MAKENODE 7  // make a node active
#define CHCHILD 8   // msg to change the childRank of a node
#define PUTKEYS 9   // put for multiple key value pairs
#define DBGPRINT 10 // a msg tag to indicate a debug msg

//#define DEBUG 

void commandNode(void);
void printme(void); // debug print
//int DBGnumProcesses; // debug global variable, needs to be accessable in command
