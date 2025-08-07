:- use_module(library(scasp)).

:- style_check(-singleton).
:- set_prolog_flag(scasp_unknown, fail).
:- style_check(-discontiguous).

:- discontiguous (-)/0, g/1, '-g'/1.

%% d(1).
%% -d(2).
%% p(X) :- not -d(X).
%% q(X) :- not d(X).
%% r(X) :- d(X).

%% train.
%% cross :- not train.
%%
%% %% alternate worlds ?
%% glfag(x, 1).
%% glfag(x, 2).
%%
%%
%% p(Some) :- r(Some).
%% p(Some) :- not s(Some), r(Some).
%%
%% r(2).
%% r(1).
%% s(X) :- not t(X).
%% t(X) :- not s(X).
%%
%% %% #abducible g(X).
%%
%% g(X) :- not h(X).
%% h(X) :- not g(X).
%%
%% g(1).
%% g(2).
%% -g(3).


% BEC1 - StoppedIn(t1,f,t2)
stoppedIn(T1, Fluent, T2) :-
    T1 #< T, T #< T2,
    terminates(Event, Fluent, T),
    happens(Event, T).

stoppedIn(T1, Fluent, T2) :-
    T1 #< T, T #< T2,
    releases(Event, Fluent, T),
    happens(Event, T).

% BEC2 - StartedIn(t1,f,t2)
startedIn(T1, Fluent, T2) :-
    T1 #< T, T #< T2,
    initiates(Event, Fluent, T),
    happens(Event, T).

startedIn(T1, Fluent, T2) :-
    T1 #< T, T #< T2,
    releases(Event, Fluent, T),
    happens(Event, T).

% BEC3 - HoldsAt(f,t)
holdsAt(Fluent2, T2) :-
    initiates(Event, Fluent1, T1),
    happens(Event, T1),
    trajectory(Fluent1, T1, Fluent2, T2),
    not stoppedIn(T1, Fluent1, T2).
% BEC4 - HoldsAt(f,t)
holdsAt(Fluent, T) :-
    0 #< T,
    initiallyP(Fluent),
    not stoppedIn(0, Fluent, T).

% BEC5 - not HoldsAt(f,t)
-holdsAt(Fluent, T) :-
    0 #< T,
    initiallyN(Fluent),
    not startedIn(0, Fluent, T).

% BEC6 - HoldsAt(f,t)
holdsAt(Fluent, T) :-
    T1 #< T,
    initiates(Event, Fluent, T1),
    happens(Event, T1),
    not stoppedIn(T1, Fluent, T).

% BEC7 - not HoldsAt(f,t)
-holdsAt(Fluent, T) :-
    T1 #< T,
    terminates(Event, Fluent, T1),
    happens(Event, T1),
    not startedIn(T1, Fluent, T).


/**
tx1:
   begin
   read id(1), balance(1000)
   read id(2), balance(1000)
   write id(1), balance(900)
   write id(2), balance(1100)
   commit

entity(id(Term), [attr(balance)])  <--- read attr balance
entity(id(Term), [attr(balance, Value), attr(name, like(Value))])  <-- write to row with new balance, where name like someValue
entity(id(primary_key(Term)), [attr(balance, default(Value))]) <-- create entity with a default

operations:
   - create
   - delete
   - read
   - write

statement:
   - start_time
   - one or more operations

txn:
   - start_time
   - one or more statements
   - commit or rollbac

*/

happens(get_balance_acc(1, 1000), T).
happens(get_balance_acc(2, 1000), T).

happens(credit_acc(X), T) :- happens(get_balance_acc(X, _, T1)), T1 #< T.
happens(debit_acc(X), T) :- happens(get_balance_acc(X, _, T1)), T1 #< T.

happens(txn, Start, End) :- happens(begin, Start),
                   happens(commit, End),
                   End #> Start.

credit_debit_txn(Debit, Credit, T1, T2, End) :- happens(credit_acc(Credit), T1),
                                           happens(debit_acc(Debit), T2),
                                           happens(txn, Start, End),
                                           Start #< T1, Start #< T2,
                                           End #> T1, End #> T2.

transfer_money(Debit, Credit, T) :- credit_debit_txn(Debit, Credit, T1, T2, T),
                                    T2 #< T1.

transfer_money(Debit, Credit, T) :- credit_debit_txn(Debit, Credit, T1, T2, T),
                                    T1 #< T2.
