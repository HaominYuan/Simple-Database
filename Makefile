db : main.c
	cc -std=gnu99 -o db main.c

draft : draft.c
	cc -std=c99 -o draft draft.c 

clean :
	rm simple_database
