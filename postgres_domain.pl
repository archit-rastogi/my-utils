% choice is determined by otehr constraints, will have to see how it will work or not!
% TODO: may be we should decouple domain choices and database-specific schema design
% so we have a pipeline: domain -> database schema -> sql
% schema definition as a start state -> acions[create, create, ..., alters, alters, dmls, alters, ..., drop, ....] -> end state is the created schema on the cluster

get_ddl(Schema, [
            name(TblName),
            Cols,
            TableConstraints,
            Colocation,
            WithOids,
            Tablespace,
            SplitTablets
        ]). %% may be use dcg to create sql
%% choose a column order

% there is a planner which can split table defintiions into a sequence of sub-steps that includes create and alter
define_table(Schema, [
                 name(TblName),
                 Cols,
                 TableConstraints,
                 Colocation,
                 WithOids,
                 Tablespace,
                 SplitTablets
             ]) :-
    choose_table_name(Schema, TblName),
    choose_columns(Schema, TblName, Cols),
    choose_constraints(Schema, TblName,TableConstraints),
    pick_colocation(Schema, TblName,Colocation),
    choose_tablespace(Schema, TblName,Tablespace),
    choose_tablet_distribution(Schema, TblName,SplitTablets).


choose_table_name(Schema, TblName) :-
    %% predicate to return a unique name
    TblName = 't1'.


%% return atleast 1 col.
%% empty list should never unify.
%% how to choose data types for columns ?
%%% atleast 2 primitive data types
%%% atleast 1 data type that can be a primary key
%%% number of columns should be less, more, etc.
%%% size of row should be small, medium, large, etc.


choose_columns(Schema, TblName, [H|T]) :- guess_number_of_columns(Schema, TblName, Length),
                                          Length #>= 1,
                                          define_column([H]), choose_columns(T).

define_column(
    [
        Name,
        DataType,
        DataTypeArgs
    ]
).


% modelling various data types
% data type has a name, one or more aliases, one or more arguments, and other properties.
data_types(
    [
        'bigint',
        'bigserial',
        'bit',
        'bit varying',
        'boolean',
        'character',
        'character varying',
n        'date',
        'double precsion',
        'integer',
        'numeric',
        'real',
        'smallint',
        'smallserial',
        'serial',
        'text',
        'time',
        'timestamp',
        'uuid',
        'interval',
        'decimal',
        'real'
    ]).


%% primitive data type
data_type_is_primitive(
    [
        'bigint',
        'bit',
        'bit varying',
        'boolean',
        'char',
        'character varying',
        'character',
        'varchar',
        'date',
        'double precision',
        'integer',
        'interval',
        'numeric',
        'decimal',
        'real',
        'smallint',
        'time'
    ]
).
