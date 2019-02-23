simple_database : main.c
	cc -std=gnu99 -o simple_database main.c

draft : draft.c
	cc -std=c99 -o draft draft.c 

clean :
	rm simple_database
