dht:	dht.o dht-helper.o command.o
	mpicc -o dht dht.o dht-helper.o command.o

dht-helper.o:	dht-helper.c dht-helper.h
	mpicc -c -std=c99 dht-helper.c

dht.o:	dht.c dht-helper.h
	mpicc -c  -std=c99 dht.c

command.o:	command.c
	mpicc -c  -std=c99 command.c

clean:
	rm -f *.o dht 
