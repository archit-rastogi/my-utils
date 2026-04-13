% s(CASP) Programming
:- use_module(library(scasp)).
% Uncomment to suppress warnings
:- style_check(-discontiguous).
% :- style_check(-singleton).
:- set_prolog_flag(scasp_unknown, fail).

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


trajectory(tablet_size(TabletId, SizeAtT1), T1, tablet_size(TabletId, SizeAtT2), T2) :-
    T1 #< T2,
    not stoppedIn(T1, workload_is_running, T2),
    not stoppedIn(T1, tablet_exists(TabletId), T2),
    growth_rate(Rate),
    SizeAtT2 #= SizeAtT1 + (Rate * (T2 - T1)).

initiates(split(TabletId), tablet_exists(ChildTabletId), T) :-
    T #> 0,
    eligible_to_split(TabletId, T),
    child_tablets(TabletId, ChildTabletId).

% terminate parent tablet when child tablets have been created
terminates(split(TabletId), tablet_exists(TabletId), T) :-
    T1 #> 0, T1 #< T,
    child_tablets(TabletId, ChildTabletId),
    holdsAt(tablet_exists(ChildTabletId), T1).

eligible_to_split(TabletId, T) :-
    T #> 0,
    holdsAt(tablet_size(TabletId, Size), T),
    split_threshold(Threshold),
    Size #= Threshold.

% a split intiates two new child tablets
child_tablets(TabletId, ChildTabletId) :-
    ChildTabletId #= TabletId * 2.
child_tablets(TabletId, ChildTabletId) :-
    ChildTabletId #= TabletId * 2 + 1.

initiates(load_data, workload_is_running, T) :-
    T1 #< T,
    not holdsAt(workload_is_running, T1).
terminates(stop_load, workload_is_running, T) :-
    T1 #< T,
    not -holdsAt(workload_is_running, T1).

% scenario
growth_rate(100).        % KB per second
split_threshold(131072).

initiallyP(tablet_exists(1)).
initiallyP(tablet_size(1, 0)).
initiallyN(workload_is_running).

happens(load_data, 5).
happens(stop_load, 10000).
% happens(split(_), _).
