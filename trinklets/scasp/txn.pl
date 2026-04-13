:- use_module(library(scasp)).

% Op/2, Op/3 -> read, update, delete, lock, begin, commit, rollback
% stmt is a set of (sequence of ops).
% txn is temporal sequence of stmt.

% event calculus
% database state is a Fluent.
% txn is also a fluent.
% a statement is executed at time t.
% no two statements can be executed at the same time t.

txn(TxnId, Start) :-
    TxnId #> 0, TxnId #< 2,
    Start #> 0.

% when does a txn ends ? on commit, on rollback, or explicit connection termination.

% a statement always run in a txn
stmt(Sid, TxnId, Start) :-
    get_stmt_id(Sid, TxnId),
    txn(TxnId, TxnStart),
    Start #> TxnStart.

% if there are no statements before, then sid = 1
% else sid = previous_sid + 1
get_stmt_id(TxnId, 1) :- not some_other_stmt(1, TxnId, _).
get_stmt_id(TxnId, Sid) :- some_other_stmt(PreStmtSid, TxnId, _), Sid #> 0, Sid #< 2, Sid = PreStmtSid + 1.

stmt(Sid, _, Start) :- not some_other_stmt(Sid, _, Start).
% any 2 different stmt cannot have start time at the same time
some_other_stmt(Sid, _, Start) :- stmt(Sid2, _, Start), Sid \= Sid2.

stmt_query(_, _, [a, b]).
stmt_query(_, _, [c, d]).
stmt_query(_, _, [e, f]).

txn(1, 1).
