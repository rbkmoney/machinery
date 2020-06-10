%%%
%%% Storage schema behaviour.

-module(machinery_mg_schema).


%% Behaviour definition

-type schema() :: module().

-type t() ::
    {args,
        init |
        repair |
        call
    } |
    {response,
        call |
        {repair,
            success |
            failure
        }
    } |
    {event, Version} |
    {aux_state, Version}.
-type v(T) ::
    T.

-type vt() :: aux_state | event.
-type version() :: undefined | integer().

-type context() :: #{
    machine_ref := machinery:ref(),
    machine_ns := machinery:namespace(),
    created_at => machinery:timestamp(),
    atom() => term()
}.

-callback marshal(t(), v(_), context()) ->
    {machinery_msgpack:t(), context()}.

-callback unmarshal(t(), machinery_msgpack:t(), context()) ->
    {v(_), context()}.

-callback get_version(vt()) ->
    version().

-export_type([schema/0]).
-export_type([t/0]).
-export_type([v/1]).
-export_type([vt/0]).
-export_type([version/0]).
-export_type([context/0]).

%% API

-export([marshal/4]).
-export([unmarshal/4]).
-export([get_version/2]).

-spec marshal(schema(), t(), v(_), context()) ->
    {machinery_msgpack:t(), context()}.
marshal(Schema, T, V, C) ->
    Schema:marshal(T, V, C).

-spec unmarshal(schema(), t(), machinery_msgpack:t(), context()) ->
    {v(_), context()}.
unmarshal(Schema, T, V, C) ->
    Schema:unmarshal(T, V, C).

-spec get_version(schema(), vt()) ->
    version().
get_version(Schema, T) ->
    Schema:get_version(T).