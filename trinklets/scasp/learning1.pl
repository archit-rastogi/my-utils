:- use_module(library(scasp)).

:- style_check(-singleton).
:- set_prolog_flag(scasp_unknown, fail).

d(1).
p(X) :- not d(X).

backed_up.
restored.
