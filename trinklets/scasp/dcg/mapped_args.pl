% Main predicate to call a predicate with mapped arguments
% call_with_mapped_args(+PredicateName, +ArgSpecs, +TermList)
% where ArgSpecs is a list of functor/arity specifications
call_with_mapped_args(PredName, ArgSpecs, TermList) :-
    map_arguments(ArgSpecs, TermList, MappedArgs),
    Pred =.. [PredName|MappedArgs],
    call(Pred).

% Map argument specifications to actual terms or free variables
% map_arguments(+ArgSpecs, +TermList, -MappedArgs)
map_arguments([], _, []).
map_arguments([Spec|RestSpecs], TermList, [Arg|RestArgs]) :-
    (   find_matching_term(Spec, TermList, MatchedTerm)
    ->  Arg = MatchedTerm
    ;   true  % Leave Arg as free variable
    ),
    map_arguments(RestSpecs, TermList, RestArgs).

% Find a term in the list that matches the specification
% find_matching_term(+Spec, +TermList, -Term)
find_matching_term(Functor/Arity, TermList, Term) :-
    member(Term, TermList),
    functor(Term, Functor, Arity).

% Alternative version that removes used terms to avoid double-mapping
call_with_mapped_args_unique(PredName, ArgSpecs, TermList) :-
    map_arguments_unique(ArgSpecs, TermList, MappedArgs, _),
    Pred =.. [PredName|MappedArgs],
    call(Pred).

% Map arguments ensuring each term is used only once
% map_arguments_unique(+ArgSpecs, +TermList, -MappedArgs, -RemainingTerms)
map_arguments_unique([], TermList, [], TermList).
map_arguments_unique([Spec|RestSpecs], TermList, [Arg|RestArgs], FinalRemainingTerms) :-
    (   find_and_remove_matching_term(Spec, TermList, MatchedTerm, RemainingTerms)
    ->  Arg = MatchedTerm,
        NextTermList = RemainingTerms
    ;   NextTermList = TermList  % Leave Arg as free variable, keep all terms
    ),
    map_arguments_unique(RestSpecs, NextTermList, RestArgs, FinalRemainingTerms).

% Find matching term and remove it from the list
% find_and_remove_matching_term(+Spec, +TermList, -Term, -RemainingTerms)
find_and_remove_matching_term(Functor/Arity, TermList, Term, RemainingTerms) :-
    select(Term, TermList, RemainingTerms),
    functor(Term, Functor, Arity).

% Example predicate for testing
test_pred(A, B, C, D) :-
    format('Called with: A=~w, B=~w, C=~w, D=~w~n', [A, B, C, D]).

% Example usage and test cases
run_examples :-
    format('=== Example 1 ===~n'),
    % pred(A1: a/2, B1:b/3, B2:b/2, C1:c/0)
    % List: [c, b(1,2,3), a(5,6), d(4)]
    ArgSpecs1 = [a/2, b/3, b/2, c/0],
    TermList1 = [c, b(1,2,3), a(5,6), d(4)],
    call_with_mapped_args(test_pred, ArgSpecs1, TermList1),

    format('~n=== Example 2 ===~n'),
    % Different order and missing terms
    ArgSpecs2 = [x/1, b/3, a/2],
    TermList2 = [a(5,6), b(1,2,3), c],
    call_with_mapped_args(test_pred, ArgSpecs2, TermList2),

    format('~n=== Example 3 - Unique mapping ===~n'),
    % Using unique mapping version
    ArgSpecs3 = [b/2, b/3, a/2, c/0],
    TermList3 = [b(x,y), b(1,2,3), a(5,6), c],
    call_with_mapped_args_unique(test_pred, ArgSpecs3, TermList3),

    format('~n=== Example 4 - Multiple b/3 terms ===~n'),
    ArgSpecs4 = [b/3, b/3, a/2],
    TermList4 = [b(1,2,3), b(x,y,z), a(5,6)],
    call_with_mapped_args(test_pred, ArgSpecs4, TermList4).

% Utility predicate to create argument specifications from a template
% This helps when you have a predicate definition like pred(A1:a/2, B1:b/3, ...)
parse_pred_spec(PredSpec, PredName, ArgSpecs) :-
    PredSpec =.. [PredName|ArgDefs],
    extract_specs(ArgDefs, ArgSpecs).

extract_specs([], []).
extract_specs([_Var:Spec|RestDefs], [Spec|RestSpecs]) :-
    extract_specs(RestDefs, RestSpecs).

% Example with predicate specification parsing
run_spec_example :-
    format('~n=== Specification Parsing Example ===~n'),
    % Define predicate specification
    PredSpec = test_pred(a1:a/2, b1:b/3, b2:b/2, c1:c/0),
    parse_pred_spec(PredSpec, PredName, ArgSpecs),
    format('Parsed predicate: ~w~n', [PredName]),
    format('Argument specs: ~w~n', [ArgSpecs]),

    TermList = [c, b(1,2,3), a(5,6), d(4)],
    call_with_mapped_args(PredName, ArgSpecs, TermList).

% Advanced version with type checking and error handling
call_with_mapped_args_safe(PredName, ArgSpecs, TermList) :-
    (   atom(PredName) ->
        true
    ;   throw(error(type_error(atom, PredName), context(call_with_mapped_args_safe/3, 'Predicate name must be an atom')))
    ),
    (   is_list(ArgSpecs) ->
        true
    ;   throw(error(type_error(list, ArgSpecs), context(call_with_mapped_args_safe/3, 'Argument specifications must be a list')))
    ),
    (   is_list(TermList) ->
        true
    ;   throw(error(type_error(list, TermList), context(call_with_mapped_args_safe/3, 'Term list must be a list')))
    ),
    map_arguments(ArgSpecs, TermList, MappedArgs),
    Pred =.. [PredName|MappedArgs],
    (   catch(call(Pred), Error, (format('Error calling predicate: ~w~n', [Error]), fail))
    ->  true
    ;   format('Predicate ~w failed~n', [PredName]),
        fail
    ).
