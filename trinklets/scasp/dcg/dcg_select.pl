:- use_module(library(dcg/basics)).
:- use_module(library(dcg/high_order)).

:- use_module(library(scasp)).

query('SELECT', Columns, Sources, Query) :- phrase(query_select(Columns, Sources), Codes, []),
                                   string_codes(Query, Codes).


query_select(Columns, Sources) --> "SELECT ",
                                   projection(Columns),
                                   source(Sources).

% ---------- Projection ----------
projection(Columns) --> { var(Columns), not table_columns_are_known(t2), Columns = [] }, "*", !.
projection([]) --> "*", !.
projection([H|Tail]) --> foreach(member(X, [H|Tail]), column(X), ", ").

column(String) --> String.

% ---------- source for fetching  ----------

source(table(TableName)) --> " FROM ", table_name(TableName).


% example
% ?- phrase(table_name("tbl1"), C, []), string_codes(S, C).
% C = [116, 98, 108, 49],
% S = "tbl1".
%
% ?- phrase(table_name(T), `tbl1`, []).
% T = "tbl1".
%
% ?- phrase(table_name("tbl1"), `tbl1`, []).
% true.

table_name(TableName) --> { nonvar(TableName), string_codes(TableName, C) }, C, !.
table_name(TableName) --> { var(TableName) }, string_without(" \n;", Codes), { string_codes(TableName, Codes) }.

table_column(t1, "c1").
table_column(t1, "c2").
table_column(t1, "c3").
table_column(t1, "c4").
table_column(t1, "c5").
-table_column(T,C) :- not table_column(T,C).

table_columns_are_known(T) :- table_column(T, _).
-table_columns_are_known(T) :- not table_columns_are_known(T).
