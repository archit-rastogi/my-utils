column("pk", data_type("integer");
        "ol_int_nokey", data_type("integer");
        "col_int_key", data_type("integer");
        "col_date_key", data_type("date");
        "col_date_nokey", data_type("date");
        "col_time_key", data_type("time without time zone");
        "col_time_nokey", data_type("time without time zone");
        "col_datetime_key", data_type("timestamp without time zone");
        "col_datetime_nokey", data_type("timestamp without time zone");
        "col_varchar_key", data_type("character varying(1)");
        "col_varchar_nokey", data_type("character varying(1)")
      ).

table(a;aa;b;bb;c;cc).

% Assume all columns belong to all tables
table_column(TableName, ColName) :- column(ColName, _), table(TableName).

% todo: Will figure out later how to specify nested levels.

{ projection(table_column(T, C), 0) : table_column(T, C) } >= 1.

% Every Db table is a potential entity at level 0.
{ entity(table(T), 0) : table(T) } = 1.

% A projection is valid if the column belongs to the source at same level.
:- projection(table_column(T, _), L1), entity(table(T), L2), L1 != L2.
:- projection(table_column(T1, _), L1), entity(table(T2), L1), T1 != T2.


% column(Name, Datatype)
% data_type(TypeName)
% table(Name).

#show projection/2.
#show entity/2.
