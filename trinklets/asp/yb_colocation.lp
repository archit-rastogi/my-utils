%% 1. co-located table cannot be created in a non-colocated database.
%% 2. Table created in a colocated database are colocated by default.
%% 3. One could opt-out specific tables out of colocated database.

%% Domain
db_count(2).
database(1..C) :- db_count(C).
table(1..3).

bool_setting(true).
bool_setting(false).
bool_setting(empty).

%% GENERATE: associate all tables with database
{ db_table(Db, Table) } = 1 :- database(Db), table(Table).


%% Define
{ with_colocation(database(Db), X) : db_table(Db, _), bool_setting(X) } = C :- db_count(C).

{ with_colocation(db_table(Db, Table), X): db_table(Db, Table), bool_setting(X)} = C :- C = #count { 1, Db, Table : db_table(Db, Table) }.
%% Or, add negative predicate:
% -with_colocation(database(Db1), X1) :- bool_setting(X1), db_table(Db1, _), with_colocation(database(Db1), X2), X1 != X2.

is_colocated(database(Db), false) :- db_table(Db, _), not with_colocation(database(Db), true).
is_colocated(database(Db), true) :- db_table(Db, _), not is_colocated(database(Db), false).

%% Test: Constraints

%% Only one configuration per database.

%% Using aggregate constraint. If same db is configured twice, then count is 1, hence, filtered.
%% :- #count { 1, Db : with_colocation(database(Db), _ )} = 1.

%% Or, using inequality:
:- with_colocation(database(Db), X), with_colocation(database(Db), Y), X != Y.
:- with_colocation(db_table(Db, Table), X), with_colocation(db_table(Db, Table), Y), X != Y.
:- is_colocated(database(Db), false), with_colocation(db_table(DB, Table), true).
