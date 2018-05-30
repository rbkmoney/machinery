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
    event |
    aux_state.
-type v(T) ::
    T.

-callback marshal(t(), v(_)) ->
    machinery_msgpack:t().

-callback unmarshal(t(), machinery_msgpack:t()) ->
    v(_).

-export_type([schema/0]).
-export_type([t/0]).
-export_type([v/1]).

%% API

-export([marshal/3]).
-export([unmarshal/3]).

-spec marshal(schema(), t(), v(_)) ->
    machinery_msgpack:t().
marshal(Schema, T, V) ->
    Schema:marshal(T, V).

-spec unmarshal(schema(), t(), machinery_msgpack:t()) ->
    v(_).
unmarshal(Schema, T, V) ->
    Schema:unmarshal(T, V).
