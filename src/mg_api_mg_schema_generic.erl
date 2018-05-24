%%%
%%% Storage schema adapted to store arbitrary persistent Erlang terms.
%%%
%%% Excluding, as expected: pids, refs, ports.

-module(mg_api_mg_schema_generic).

-export([marshal/1]).
-export([unmarshal/1]).

%% Storage schema behaviour

-behaviour(mg_api_mg_schema).

-export([marshal/2]).
-export([unmarshal/2]).

-import(mg_api_msgpack, [
    nil/0,
    wrap/1,
    unwrap/1
]).

%%

-type t()  :: mg_api_mg_schema:t().
-type v(T) :: mg_api_mg_schema:v(T).

-spec marshal(t(), v(eterm())) ->
    mg_api_msgpack:t().

-spec unmarshal(t(), mg_api_msgpack:t()) ->
    v(eterm()).

marshal(_T, V) ->
    marshal(V).

unmarshal(_T, V) ->
    unmarshal(V).

%%

-type eterm() ::
    atom()   |
    number() |
    tuple()  |
    binary() |
    list()   |
    map().

-spec marshal(eterm()) ->
    mg_api_msgpack:t().

-spec unmarshal(mg_api_msgpack:t()) ->
    eterm().

marshal(undefined) ->
    nil();
marshal(V) when is_boolean(V) ->
    wrap(V);
marshal(V) when is_atom(V) ->
    wrap(atom_to_binary(V, utf8));
marshal(V) when is_number(V) ->
    wrap(V);
marshal(V) when is_binary(V) ->
    wrap({binary, V});
marshal([]) ->
    wrap([]);
marshal(V) when is_list(V) ->
    wrap([marshal(lst) | lists:map(fun marshal/1, V)]);
marshal(V) when is_tuple(V) ->
    wrap([marshal(tup) | lists:map(fun marshal/1, tuple_to_list(V))]);
marshal(V) when is_map(V) ->
    wrap([marshal(map), wrap(genlib_map:truemap(fun (Ke, Ve) -> {marshal(Ke), marshal(Ve)} end, V))]);
marshal(V) ->
    error({badarg, V}).

unmarshal(M) ->
    unmarshal_v(unwrap(M)).

unmarshal_v(nil) ->
    undefined;
unmarshal_v(V) when is_boolean(V) ->
    V;
unmarshal_v(V) when is_binary(V) ->
    binary_to_existing_atom(V, utf8);
unmarshal_v(V) when is_number(V) ->
    V;
unmarshal_v({binary, V}) ->
    V;
unmarshal_v([]) ->
    [];
unmarshal_v([Ty | Vs]) ->
    unmarshal_v(unmarshal(Ty), Vs).

unmarshal_v(lst, Vs) ->
    lists:map(fun unmarshal/1, Vs);
unmarshal_v(tup, Es) ->
    list_to_tuple(unmarshal_v(lst, Es));
unmarshal_v(map, [V]) ->
    genlib_map:truemap(fun (Ke, Ve) -> {unmarshal(Ke), unmarshal(Ve)} end, unwrap(V)).
