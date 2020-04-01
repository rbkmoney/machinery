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
    response |
    {event, Version} |
    {aux_state, Version}.
-type v(T) ::
    T.

-type vt() :: aux_state | event.
-type version() :: undefined | integer().

-callback marshal(t(), v(_)) ->
    machinery_msgpack:t().

-callback unmarshal(t(), machinery_msgpack:t()) ->
    v(_).

-callback get_version(vt()) ->
    version().

-export_type([schema/0]).
-export_type([t/0]).
-export_type([v/1]).
-export_type([vt/0]).
-export_type([version/0]).

%% API

-export([marshal/3]).
-export([unmarshal/3]).
-export([get_version/2]).

-spec marshal(schema(), t(), v(_)) ->
    machinery_msgpack:t().
marshal(Schema, T, V) ->
    Schema:marshal(T, V).

-spec unmarshal(schema(), t(), machinery_msgpack:t()) ->
    v(_).
unmarshal(Schema, T, V) ->
    Schema:unmarshal(T, V).

-spec get_version(schema(), vt()) ->
    version().
get_version(Schema, T) ->
    Schema:get_version(T).