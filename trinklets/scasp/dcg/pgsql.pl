% PostgreSQL-Compatible SQL DCG Parser
% This parser handles the major SQL constructs with PostgreSQL extensions

% Main entry points
sql_statement(Statement) -->
    select_statement(Statement), !.
sql_statement(Statement) -->
    insert_statement(Statement), !.
sql_statement(Statement) -->
    update_statement(Statement), !.
sql_statement(Statement) -->
    delete_statement(Statement), !.
sql_statement(Statement) -->
    create_table_statement(Statement), !.
sql_statement(Statement) -->
    drop_table_statement(Statement), !.
sql_statement(Statement) -->
    create_index_statement(Statement), !.

% SELECT Statement
select_statement(select(Distinct, Columns, From, Where, GroupBy, Having, OrderBy, Limit)) -->
    [select],
    optional_distinct(Distinct),
    select_list(Columns),
    optional_from_clause(From),
    optional_where_clause(Where),
    optional_group_by_clause(GroupBy),
    optional_having_clause(Having),
    optional_order_by_clause(OrderBy),
    optional_limit_clause(Limit).

% DISTINCT clause
optional_distinct(distinct) --> [distinct], !.
optional_distinct(all) --> [all], !.
optional_distinct(none) --> [].

% Select list
select_list([*]) --> [*], !.
select_list(Columns) --> select_item_list(Columns).

select_item_list([Item]) --> select_item(Item).
select_item_list([Item|Rest]) -->
    select_item(Item), [','], select_item_list(Rest).

select_item(column_alias(Expr, Alias)) -->
    expression(Expr), [as], identifier(Alias), !.
select_item(column_alias(Expr, Alias)) -->
    expression(Expr), identifier(Alias), !.
select_item(Expr) --> expression(Expr).

% FROM clause
optional_from_clause(From) --> [from], from_list(From), !.
optional_from_clause(none) --> [].

from_list([Item]) --> from_item(Item).
from_list([Item|Rest]) -->
    from_item(Item), [','], from_list(Rest).

from_item(table_alias(Table, Alias)) -->
    table_reference(Table), [as], identifier(Alias), !.
from_item(table_alias(Table, Alias)) -->
    table_reference(Table), identifier(Alias), !.
from_item(join(Left, JoinType, Right, Condition)) -->
    from_item(Left), join_type(JoinType), [join],
    from_item(Right), [on], expression(Condition), !.
from_item(Table) --> table_reference(Table).

join_type(inner) --> [inner], !.
join_type(left) --> [left], !.
join_type(right) --> [right], !.
join_type(full) --> [full], !.
join_type(cross) --> [cross], !.
join_type(inner) --> [].

table_reference(table(Schema, Table)) -->
    identifier(Schema), ['.'], identifier(Table), !.
table_reference(table(Table)) --> identifier(Table).
table_reference(subquery(Query)) --> ['('], select_statement(Query), [')'].

% WHERE clause
optional_where_clause(Where) --> [where], expression(Where), !.
optional_where_clause(none) --> [].

% GROUP BY clause
optional_group_by_clause(GroupBy) -->
    [group], [by], expression_list(GroupBy), !.
optional_group_by_clause(none) --> [].

% HAVING clause
optional_having_clause(Having) --> [having], expression(Having), !.
optional_having_clause(none) --> [].

% ORDER BY clause
optional_order_by_clause(OrderBy) -->
    [order], [by], order_item_list(OrderBy), !.
optional_order_by_clause(none) --> [].

order_item_list([Item]) --> order_item(Item).
order_item_list([Item|Rest]) -->
    order_item(Item), [','], order_item_list(Rest).

order_item(order(Expr, Dir)) -->
    expression(Expr), order_direction(Dir), !.
order_item(order(Expr, asc)) --> expression(Expr).

order_direction(asc) --> [asc].
order_direction(desc) --> [desc].

% LIMIT clause
optional_limit_clause(limit(Count, Offset)) -->
    [limit], integer(Count), [offset], integer(Offset), !.
optional_limit_clause(limit(Count)) -->
    [limit], integer(Count), !.
optional_limit_clause(none) --> [].

% INSERT Statement
insert_statement(insert(Table, Columns, Values)) -->
    [insert], [into], table_reference(Table),
    optional_column_list(Columns),
    values_clause(Values).

optional_column_list(Columns) -->
    ['('], identifier_list(Columns), [')'], !.
optional_column_list(none) --> [].

values_clause(values(ValuesList)) -->
    [values], values_list(ValuesList), !.
values_clause(select(Query)) --> select_statement(Query).

values_list([Values]) --> ['('], expression_list(Values), [')'].
values_list([Values|Rest]) -->
    ['('], expression_list(Values), [')'], [','], values_list(Rest).

% UPDATE Statement
update_statement(update(Table, SetClause, Where)) -->
    [update], table_reference(Table),
    [set], set_clause_list(SetClause),
    optional_where_clause(Where).

set_clause_list([Clause]) --> set_clause(Clause).
set_clause_list([Clause|Rest]) -->
    set_clause(Clause), [','], set_clause_list(Rest).

set_clause(assign(Column, Value)) -->
    identifier(Column), ['='], expression(Value).

% DELETE Statement
delete_statement(delete(Table, Where)) -->
    [delete], [from], table_reference(Table),
    optional_where_clause(Where).

% CREATE TABLE Statement
create_table_statement(create_table(Table, Columns, Options)) -->
    [create], [table], table_reference(Table),
    ['('], column_definition_list(Columns), [')'],
    optional_table_options(Options).

column_definition_list([Column]) --> column_definition(Column).
column_definition_list([Column|Rest]) -->
    column_definition(Column), [','], column_definition_list(Rest).

column_definition(column(Name, Type, Constraints)) -->
    identifier(Name), data_type(Type),
    column_constraints(Constraints).

data_type(integer) --> [integer], !.
data_type(int) --> [int], !.
data_type(bigint) --> [bigint], !.
data_type(smallint) --> [smallint], !.
data_type(varchar(Length)) --> [varchar], ['('], integer(Length), [')'], !.
data_type(varchar) --> [varchar], !.
data_type(char(Length)) --> [char], ['('], integer(Length), [')'], !.
data_type(char) --> [char], !.
data_type(text) --> [text], !.
data_type(boolean) --> [boolean], !.
data_type(bool) --> [bool], !.
data_type(date) --> [date], !.
data_type(time) --> [time], !.
data_type(timestamp) --> [timestamp], !.
data_type(timestamptz) --> [timestamptz], !.
data_type(numeric(Precision, Scale)) -->
    [numeric], ['('], integer(Precision), [','], integer(Scale), [')'], !.
data_type(numeric(Precision)) -->
    [numeric], ['('], integer(Precision), [')'], !.
data_type(numeric) --> [numeric], !.
data_type(decimal) --> [decimal], !.
data_type(real) --> [real], !.
data_type(float) --> [float], !.
data_type(double) --> [double], [precision], !.

column_constraints([]) --> [].
column_constraints([Constraint|Rest]) -->
    column_constraint(Constraint), column_constraints(Rest).

column_constraint(not_null) --> [not], [null], !.
column_constraint(primary_key) --> [primary], [key], !.
column_constraint(unique) --> [unique], !.
column_constraint(default(Value)) --> [default], literal(Value), !.
column_constraint(references(Table, Column)) -->
    [references], table_reference(Table),
    ['('], identifier(Column), [')'], !.

optional_table_options([]) --> [].

% DROP TABLE Statement
drop_table_statement(drop_table(Table, IfExists)) -->
    [drop], [table], if_exists(IfExists), table_reference(Table).

if_exists(true) --> [if], [exists], !.
if_exists(false) --> [].

% CREATE INDEX Statement
create_index_statement(create_index(IndexName, Table, Columns, Unique)) -->
    [create], unique_flag(Unique), [index], identifier(IndexName),
    [on], table_reference(Table),
    ['('], identifier_list(Columns), [')'].

unique_flag(true) --> [unique], !.
unique_flag(false) --> [].

% Expression parsing
expression(Expr) --> or_expression(Expr).

or_expression(or(Left, Right)) -->
    and_expression(Left), [or], or_expression(Right), !.
or_expression(Expr) --> and_expression(Expr).

and_expression(and(Left, Right)) -->
    not_expression(Left), [and], and_expression(Right), !.
and_expression(Expr) --> not_expression(Expr).

not_expression(not(Expr)) --> [not], comparison_expression(Expr), !.
not_expression(Expr) --> comparison_expression(Expr).

comparison_expression(Expr) -->
    additive_expression(Left), comparison_op(Op), additive_expression(Right),
    { Expr =.. [Op, Left, Right] }, !.
comparison_expression(in(Expr, List)) -->
    additive_expression(Expr), [in], ['('], expression_list(List), [')'], !.
comparison_expression(between(Expr, Low, High)) -->
    additive_expression(Expr), [between], additive_expression(Low),
    [and], additive_expression(High), !.
comparison_expression(like(Expr, Pattern)) -->
    additive_expression(Expr), [like], additive_expression(Pattern), !.
comparison_expression(is_null(Expr)) -->
    additive_expression(Expr), [is], [null], !.
comparison_expression(is_not_null(Expr)) -->
    additive_expression(Expr), [is], [not], [null], !.
comparison_expression(Expr) --> additive_expression(Expr).

comparison_op('=') --> ['='].
comparison_op('!=') --> ['!='].
comparison_op('<>') --> ['<>'].
comparison_op('<') --> ['<'].
comparison_op('>') --> ['>'].
comparison_op('<=') --> ['<='].
comparison_op('>=') --> ['>='].

additive_expression(Expr) -->
    multiplicative_expression(Left), additive_op(Op), additive_expression(Right),
    { Expr =.. [Op, Left, Right] }, !.
additive_expression(Expr) --> multiplicative_expression(Expr).

additive_op('+') --> ['+'].
additive_op('-') --> ['-'].
additive_op('||') --> ['||'].  % String concatenation

multiplicative_expression(Expr) -->
    unary_expression(Left), multiplicative_op(Op), multiplicative_expression(Right),
    { Expr =.. [Op, Left, Right] }, !.
multiplicative_expression(Expr) --> unary_expression(Expr).

multiplicative_op('*') --> ['*'].
multiplicative_op('/') --> ['/'].
multiplicative_op('%') --> ['%'].

unary_expression(minus(Expr)) --> ['-'], primary_expression(Expr), !.
unary_expression(plus(Expr)) --> ['+'], primary_expression(Expr), !.
unary_expression(Expr) --> primary_expression(Expr).

primary_expression(Expr) --> ['('], expression(Expr), [')'], !.
primary_expression(function_call(Name, Args)) -->
    identifier(Name), ['('], function_args(Args), [')'], !.
primary_expression(column(Schema, Table, Column)) -->
    identifier(Schema), ['.'], identifier(Table), ['.'], identifier(Column), !.
primary_expression(column(Table, Column)) -->
    identifier(Table), ['.'], identifier(Column), !.
primary_expression(column(Column)) --> identifier(Column), !.
primary_expression(subquery(Query)) --> ['('], select_statement(Query), [')'], !.
primary_expression(Literal) --> literal(Literal).

function_args([]) --> [].
function_args(Args) --> expression_list(Args).

% Literals
literal(string(S)) --> string_literal(S), !.
literal(integer(I)) --> integer(I), !.
literal(float(F)) --> float(F), !.
literal(boolean(true)) --> [true], !.
literal(boolean(false)) --> [false], !.
literal(null) --> [null].

% Helper predicates
expression_list([Expr]) --> expression(Expr).
expression_list([Expr|Rest]) -->
    expression(Expr), [','], expression_list(Rest).

identifier_list([Id]) --> identifier(Id).
identifier_list([Id|Rest]) -->
    identifier(Id), [','], identifier_list(Rest).

% Terminal parsers
identifier(Id) --> [Id], { atom(Id) }.
string_literal(S) --> [S], { atom(S), atom_chars(S, ['\''|_]) }.
integer(I) --> [I], { integer(I) }.
float(F) --> [F], { float(F) }.

% Utility predicates for testing
parse_sql(TokenList, AST) :-
    phrase(sql_statement(AST), TokenList).

% Example usage and test cases
test_select :-
    Tokens = [select, name, ',', age, from, users, where, age, '>', 18],
    phrase(sql_statement(AST), Tokens),
    write('SELECT AST: '), write(AST), nl.

test_insert :-
    Tokens = [insert, into, users, '(', name, ',', age, ')', values, '(', '\'John\'', ',', 25, ')'],
    phrase(sql_statement(AST), Tokens),
    write('INSERT AST: '), write(AST), nl.

test_create_table :-
    Tokens = [create, table, users, '(',
              id, integer, primary, key, ',',
              name, varchar, '(', 100, ')', not, null, ',',
              age, integer,
              ')'],
    phrase(sql_statement(AST), Tokens),
    write('CREATE TABLE AST: '), write(AST), nl.

% Run all tests
run_tests :-
    write('Testing SQL DCG Parser:'), nl,
    test_select,
    test_insert,
    test_create_table,
    write('All tests completed.'), nl.
