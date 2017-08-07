	/* symbolic tokens */

%union {
	int intval;
	char *strval;
	int subtok;
}
	
%token NAME
%token STRING
%token INTNUM APPROXNUM 
%token INTO
%token INDICATOR
%token WORK
	/* operators */
%left '+' '-'
%left '*' '/'
%nonassoc UMINUS
%left <subtok> COMPARISON /* = <> < > <= >= */
	/* literal keyword tokens */

%token FROM INSERT INTEGER PRIMARY TABLE UPDATE VALUES WHERE ROLLBACK SELECT DELETE CREATE CHARACTER COMMIT OPTION PARAMETER KEY OPEN CLOSE

%%

sql_list:
		sql ';'
	|	sql_list sql ';'
	;


	/* schema definition language */
	/* Note: other ``sql:'' rules appear later in the grammar */
sql:	base_table_def
	;
	
base_table_def:
		CREATE TABLE table '(' base_table_element_commalist ')'
	;

base_table_element_commalist:
		base_table_element
	|	base_table_element_commalist ',' base_table_element
	;

base_table_element:
		column_def
	;

column_def:
		column data_type 
	;

column_commalist:
		column
	|	column_commalist ',' column
	;

opt_column_commalist:
		/* empty */
	|	'(' column_commalist ')'
	;

	/* manipulative statements */

sql:		manipulative_statement
	;

manipulative_statement:
		commit_statement
	|	delete_statement_searched
	|	insert_statement
	|	rollback_statement
	|	select_statement
	;

commit_statement:
		COMMIT 
	;

delete_statement_searched:
		DELETE FROM table opt_where_clause
	;

insert_statement:
		INSERT INTO table opt_column_commalist values {sql_insert()}
	;

values:
		VALUES '(' insert_atom_commalist ')'
	;

insert_atom_commalist:
		atom
	|	insert_atom_commalist ',' atom
	;

rollback_statement:
		ROLLBACK WORK
	;

select_statement:
		SELECT column table_exp
	|   SELECT atom table_exp
	;

opt_where_clause:
		/* empty */
	|	where_clause
	;

	/* query expressions */

table_exp:
		from_clause opt_where_clause
	;

from_clause:
		FROM table
	;

where_clause:
		WHERE search_condition
	;

	/* search conditions */

search_condition:
		'(' search_condition ')'
	|    predicate
	;

predicate:
        comparison_predicate
    ;

comparison_predicate:
        scalar_exp COMPARISON scalar_exp
    ;

atom_commalist:
		atom
	|	atom_commalist ',' atom
	;

	/* scalar expressions */

scalar_exp:
		scalar_exp '+' scalar_exp
	|	scalar_exp '-' scalar_exp
	|	scalar_exp '*' scalar_exp
	|	scalar_exp '/' scalar_exp
	|	'+' scalar_exp %prec UMINUS
	|	'-' scalar_exp %prec UMINUS
	|	atom
	|   column
    ;

atom:
		parameter_ref
	|	literal
	;

parameter_ref:
		parameter
	|	parameter parameter
	|	parameter INDICATOR parameter
	;

literal:
		STRING
	|	INTNUM
	;

	/* miscellaneous */

table:
		NAME
	|	NAME '.' NAME
	;

		/* data types */

data_type:
		CHARACTER
	|	INTEGER
	;

	/* the various things you can name */

column:		NAME
	;

parameter:
	     PARAMETER
	;

%%
