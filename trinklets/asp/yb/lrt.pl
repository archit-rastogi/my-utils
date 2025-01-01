% :- use_module(library(scasp)).
:- use_module(library(dcg/basics)).

% t_select(Columns) --> [].

% keywords(Select) --> "SELECT", {Select = "SELECT"}.
% keywords(As) --> "AS", {As = "AS"}.
% keywords(From) --> "FROM", {From = "FROM"}.
% keywords(Where) --> "WHERE", {Where = "WHERE"}.


member(X, [ X|Xs ] ).
member(X, [ _|Xs ] ):- member(X, Xs).
list( [ 1,2,3,4,5 ] ).


% scope rules
scope(a).
scope(b).
scope(c).
scope(d).
