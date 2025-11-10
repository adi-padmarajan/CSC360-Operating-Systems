CC     = gcc
CFLAGS = -Wall -Wextra -O2 -pthread

all: mts

mts: mts.c
	$(CC) $(CFLAGS) -o mts mts.c

clean:
	rm -f mts *.o output.txt
