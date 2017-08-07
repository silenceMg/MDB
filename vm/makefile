CC = cc -g
LIBS = -lfl -ll
LEX = flex
YACC = bison

sql:  sql1.h sql1.c sqltext.c scn1.c
	${CC} -o sql sql1.c sql1.h scn1.c sqltext.c ${LIBS}

sql1.c sql1.h:	sql1.y
	${YACC} -vd sql1.y
	mv sql1.tab.h sql1.h
	mv sql1.tab.c sql1.c
	mv sql1.output sql1.out

scn1.o:  sql1.h sql1.c

.SUFFIXES:	.l
.l:
	${LEX} $*.l
	mv lex.yy.c $*.c
clean:
	rm -f sql1.h sql1.c sql1.out
