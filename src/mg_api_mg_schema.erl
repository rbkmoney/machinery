%%%
%%% Storage schema behaviour.

-module(mg_api_mg_schema).

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
    mg_api_msgpack:t().

-callback unmarshal(t(), mg_api_msgpack:t()) ->
    v(_).

-export_type([schema/0]).
-export_type([t/0]).
-export_type([v/1]).

%%

-export([marshal/3]).
-export([unmarshal/3]).

-spec marshal(schema(), t(), v(_)) ->
    mg_api_msgpack:t().

-spec unmarshal(schema(), t(), mg_api_msgpack:t()) ->
    v(_).

marshal(Schema, T, V) ->
    Schema:marshal(T, V).

unmarshal(Schema, T, V) ->
    Schema:unmarshal(T, V).
