% s(CASP) Example: Two non-overlapping meetings in any order
% This program generates different permutations of meeting schedules
:- use_module(library(scasp)).

:- style_check(-singleton).
:- set_prolog_flag(scasp_unknown, fail).
:- style_check(-discontiguous).

% Define meeting durations (fixed for this example)
meeting_duration(team_meeting, 2).
meeting_duration(client_call, 1).

% Define a meeting with start time, duration, and calculated end time
meeting(Name, Start, Duration, End) :-
    member(Name, [team_meeting, client_call]),
    meeting_duration(Name, Duration),
    Start #>= 0,          % Meetings start at time 0 or later
    Start #=< 10,         % Meetings must start before time 10
    End #= Start + Duration.

% Non-overlapping constraint: two meetings cannot overlap
% Either meeting1 ends before meeting2 starts, OR
% meeting2 ends before meeting1 starts
non_overlapping(Name1, Start1, End1, Name2, Start2, End2) :-
    End1 #=< Start2.      % meeting1 completely before meeting2

non_overlapping(Name1, Start1, End1, Name2, Start2, End2) :-
    End2 #=< Start1.      % meeting2 completely before meeting1

% Main predicate to schedule both meetings
schedule_meetings(TeamStart, TeamEnd, ClientStart, ClientEnd) :-
    meeting(team_meeting, TeamStart, TeamDur, TeamEnd),
    meeting(client_call, ClientStart, ClientDur, ClientEnd),
    non_overlapping(team_meeting, TeamStart, TeamEnd,
                   client_call, ClientStart, ClientEnd).

% Query predicate to show readable results
show_schedule(Schedule) :-
    schedule_meetings(TS, TE, CS, CE),
    (TS #< CS ->
        Schedule = [meeting(team_meeting, TS, TE), meeting(client_call, CS, CE)]
    ;   Schedule = [meeting(client_call, CS, CE), meeting(team_meeting, TS, TE)]
    ).

% Alternative formulation showing explicit ordering
explicit_ordering(Order, TeamStart, TeamEnd, ClientStart, ClientEnd) :-
    meeting(team_meeting, TeamStart, 2, TeamEnd),
    meeting(client_call, ClientStart, 1, ClientEnd),
    (   (TeamEnd #=< ClientStart, Order = team_first)
    ;   (ClientEnd #=< TeamStart, Order = client_first)
    ).

% Example with specific time constraints
morning_schedule(TeamStart, TeamEnd, ClientStart, ClientEnd) :-
    meeting(team_meeting, TeamStart, 2, TeamEnd),
    meeting(client_call, ClientStart, 1, ClientEnd),
    TeamStart #=< 6,      % Team meeting in morning (before 6)
    ClientStart #=< 6,    % Client call also in morning
    non_overlapping(team_meeting, TeamStart, TeamEnd,
                   client_call, ClientStart, ClientEnd).

% Show gaps between meetings
schedule_with_gap(Gap, TeamStart, TeamEnd, ClientStart, ClientEnd) :-
    meeting(team_meeting, TeamStart, 2, TeamEnd),
    meeting(client_call, ClientStart, 1, ClientEnd),
    (   (TeamEnd #=< ClientStart, Gap #= ClientStart - TeamEnd)
    ;   (ClientEnd #=< TeamStart, Gap #= TeamStart - ClientEnd)
    ),
    Gap #>= 1.            % At least 1 time unit gap between meetings

enumerate(Ss):-
	findall(schedule(TS,TE,CS,5),schedule_with_gap(Gap, TS, TE, CS, 5), Ss).

%% Example Queries:
%%
%% 1. Generate all possible schedules:
%% ?- schedule_meetings(TS, TE, CS, CE).
%%
%% 2. Show schedules in readable format:
%% ?- show_schedule(Schedule).
%%
%% 3. Show explicit ordering:
%% ?- explicit_ordering(Order, TS, TE, CS, CE).
%%
%% 4. Morning schedules only:
%% ?- morning_schedule(TS, TE, CS, CE).
%%
%% 5. Schedules with specific gaps:
%% ?- schedule_with_gap(Gap, TS, TE, CS, CE), Gap #= 2.
%%
%% 6. Find schedules where team meeting starts at time 0:
%% ?- schedule_meetings(0, TE, CS, CE).
%%
%% 7. Count different orderings:
