CC = gcc -std=c99
CCFLAGS = -Wall -g -I./include -D _GNU_SOURCE


EXEC = main

DEPS = leveldb.o sstable.o skiplist.o filename.o mytime.o

GIT_HOOKS := .git/hooks/applied

all: $(DEPS)
	rm -rf $(EXEC)
	$(CC) -o $(EXEC) $(CCFLAGS) $(EXEC).c $(DEPS) -lpthread
	rm -rf *.o


sstable_test: sstable_test.c parse_cmd.o sstable.o filename.o
	$(CC) $(CCFLAGS) -o $@ $^

# skiplist.o:
# 	$(CC) -c skiplist.c -I./include -lpthread

# leveldb.o: leveldb.c skiplist.o sstable.o
# 	$(CC) $(CCFLAGS) -c -o $@ $^ -lpthread

%.o: %.c
	$(CC) $(CCFLAGS) -c -o $@ $< -lpthread

gen_data:
	rm -rf gen_data
	$(CC) -o gen_data gen_data.c

clean:
	rm -rf $(EXEC) *.o *.stream gen_data

re:
	make clean
	make all