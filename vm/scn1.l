%{
#include "sql1.h"
#include <string.h>

int lineno = 1;
void yyerror(char *s);

	/* macro to save the text of a SQL token */
#define SV save_str(yytext)

	/* macro to save the text and return a token */
#define TOK(name) { SV;return name; }
%}
%s SQL
%%

EXEC[ \t]+SQL	{ BEGIN SQL; start_save(); }


	/* literal keyword tokens */

<SQL>CLOSE		TOK(CLOSE)
<SQL>COMMIT		TOK(COMMIT)
<SQL>CREATE		TOK(CREATE)
<SQL>DELETE		TOK(DELETE)
<SQL>FROM		TOK(FROM)
<SQL>INDICATOR		TOK(INDICATOR)
<SQL>INSERT		TOK(INSERT)
<SQL>INT(EGER)?		TOK(INTEGER)
<SQL>INTO		TOK(INTO)
<SQL>KEY		TOK(KEY)
<SQL>OPEN		TOK(OPEN)
<SQL>OPTION		TOK(OPTION)
<SQL>PRIMARY		TOK(PRIMARY)
<SQL>ROLLBACK		TOK(ROLLBACK)
<SQL>SELECT		TOK(SELECT)
<SQL>TABLE		TOK(TABLE)
<SQL>UPDATE		TOK(UPDATE)
<SQL>VALUES		TOK(VALUES)
<SQL>WHERE		TOK(WHERE)
<SQL>WORK		TOK(WORK)

	/* punctuation */

<SQL>"="	|
<SQL>"<>" 	|
<SQL>"<"	|
<SQL>">"	|
<SQL>"<="	|
<SQL>">="		TOK(COMPARISON)

<SQL>[-+*/(),.;]	TOK(yytext[0])

<SQL>[-+*/(),.;]	TOK(yytext[0])


	/* names */
<SQL>[A-Za-z][A-Za-z0-9_]*	TOK(NAME)

	/* parameters */
<SQL>":"[A-Za-z][A-Za-z0-9_]*	{
			save_param(yytext+1);
			return PARAMETER;
		}

	/* numbers */

<SQL>[0-9]+	|
<SQL>[0-9]+"."[0-9]* |
<SQL>"."[0-9]*		TOK(INTNUM)


	/* strings */

<SQL>'[^'\n]*'	{
		int c = input();

		unput(c);	/* just peeking */
		if(c != '\'') {
			SV;return STRING;
		} else
			yymore();
	}
		
<SQL>'[^'\n]*$	{	yyerror("Unterminated string"); }

<SQL>\n		{ save_str(" ");lineno++; }
\n		{ lineno++; ECHO; }

<SQL>[ \t\r]+	save_str(" ");	/* white space */

<SQL>"--".*	;	/* comment */

.		ECHO;	/* random non-SQL text */
%%

void
yyerror(char *s)
{
	printf("%d: %s at %s\n", lineno, s, yytext);
}

main(int ac, char **av)
{
	if(ac > 1 && (yyin = fopen(av[1], "r")) == NULL) {
		perror(av[1]);
		exit(1);
	}

	if(!yyparse())
		fprintf(stderr, "Embedded SQL parse worked\n");
	else
		fprintf(stderr, "Embedded SQL parse failed\n");
} /* main */

/* leave SQL lexing mode */
un_sql()
{
	BEGIN INITIAL;
} /* un_sql */
