simple_database : main.c
	cc -o simple_database main.c

draft : draft.c
	cc -o draft draft.c 

clean :
	rm simple_database
