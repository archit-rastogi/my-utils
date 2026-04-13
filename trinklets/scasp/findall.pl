:- use_module(library(scasp)).

:- style_check(-discontiguous).
:- style_check(-singleton).
:- set_prolog_flag(scasp_unknown, fail).

% s(CASP) program
s(1) :- p.
s(2) :- p.
s(3) :- q.
p :- not q.
q :- not p.

% Prolog predicate to use findall/3 with scasp/1
get_all_s_values(L) :-
    findall(V, scasp(s(V), [model(_)]), L).

/** <examples>
?- get_all_s_values(L).
?- scasp(p), get_all_s_values(L).
*/
