-module(machinery_mg_codec).

-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").

-export([marshal/2]).
-export([unmarshal/2]).

-define(MICROS_PER_SEC, (1000 * 1000)).

%%

-type type() :: atom() | {enum, list(atom())} | {schema, module(), any(), any()}.

-type encoded_value() :: encoded_value(any()).
-type encoded_value(T) :: T.

-type decoded_value() :: decoded_value(any()).
-type decoded_value(T) :: T.

-spec marshal(type(), decoded_value()) -> encoded_value().
marshal(id, V) ->
    marshal(string, V);
marshal(event_id, V) ->
    marshal(integer, V);
marshal(namespace, V) ->
    marshal(atom, V);
marshal(tag, V) ->
    marshal(string, V);
marshal(ref, V) when is_binary(V) ->
    {id, marshal(id, V)};
marshal(ref, {tag, V}) ->
    {tag, marshal(tag, V)};
marshal(descriptor, {NS, Ref, Range}) ->
    #mg_stateproc_MachineDescriptor{
        'ns' = marshal(namespace, NS),
        'ref' = marshal(ref, Ref),
        'range' = marshal(range, Range)
    };
marshal(range, {Cursor, Limit, Direction}) ->
    #mg_stateproc_HistoryRange{
        'after' = maybe_marshal(event_id, Cursor),
        'limit' = maybe_marshal(limit, Limit),
        'direction' = marshal(direction, Direction)
    };
marshal(limit, V) ->
    marshal(integer, V);
marshal(direction, V) ->
    marshal({enum, [forward, backward]}, V);
marshal({enum, Choices = [_ | _]} = T, V) when is_atom(V) ->
    _ = lists:member(V, Choices) orelse erlang:error(badarg, [T, V]),
    V;
marshal({schema, Schema, T, Context}, V) ->
    machinery_mg_schema:marshal(Schema, T, V, Context);
marshal(timestamp, {DateTime, USec}) ->
    Ts = genlib_time:daytime_to_unixtime(DateTime) * ?MICROS_PER_SEC + USec,
    Str = calendar:system_time_to_rfc3339(Ts, [{unit, microsecond}, {offset, "Z"}]),
    erlang:list_to_binary(Str);
marshal(atom, V) when is_atom(V) ->
    atom_to_binary(V, utf8);
marshal(string, V) when is_binary(V) ->
    V;
marshal(integer, V) when is_integer(V) ->
    V;
marshal(T, V) ->
    erlang:error(badarg, [T, V]).

%%

-spec unmarshal(type(), encoded_value()) -> decoded_value().
unmarshal(id, V) ->
    unmarshal(string, V);
unmarshal(event_id, V) ->
    unmarshal(integer, V);
unmarshal(namespace, V) ->
    unmarshal(atom, V);
unmarshal(tag, V) ->
    unmarshal(string, V);
%%
%% No unmarshalling for the decriptor required by the protocol so far.
%%
%% unmarshal(
%%     descriptor,
%%     #mg_stateproc_MachineDescriptor{
%%         ns = NS,
%%         ref = {'id', ID},
%%         range = Range
%%     }
%% ) ->
%%     {unmarshal(namespace, NS), unmarshal(id, ID), unmarshal(range, Range)};

unmarshal(
    range,
    #mg_stateproc_HistoryRange{
        'after' = Cursor,
        'limit' = Limit,
        'direction' = Direction
    }
) ->
    {
        maybe_unmarshal(event_id, Cursor),
        maybe_unmarshal(limit, Limit),
        unmarshal(direction, Direction)
    };
unmarshal(limit, V) ->
    unmarshal(integer, V);
unmarshal(direction, V) ->
    unmarshal({enum, [forward, backward]}, V);
unmarshal({enum, Choices = [_ | _]} = T, V) when is_atom(V) ->
    case lists:member(V, Choices) of
        true ->
            V;
        false ->
            erlang:error(badarg, [T, V])
    end;
unmarshal({schema, Schema, T, Context}, V) ->
    machinery_mg_schema:unmarshal(Schema, T, V, Context);
unmarshal(timestamp, V) when is_binary(V) ->
    ok = assert_is_utc(V),
    Str = erlang:binary_to_list(V),
    try
        Micros = calendar:rfc3339_to_system_time(Str, [{unit, microsecond}]),
        Datetime = calendar:system_time_to_universal_time(Micros, microsecond),
        {Datetime, Micros rem ?MICROS_PER_SEC}
    catch
        error:Reason ->
            erlang:error(badarg, [timestamp, V, Reason])
    end;
unmarshal(atom, V) when is_binary(V) ->
    binary_to_existing_atom(V, utf8);
unmarshal(string, V) when is_binary(V) ->
    V;
unmarshal(integer, V) when is_integer(V) ->
    V;
unmarshal(T, V) ->
    erlang:error(badarg, [T, V]).

%%

maybe_unmarshal(_Type, undefined) ->
    undefined;
maybe_unmarshal(Type, Value) ->
    unmarshal(Type, Value).

maybe_marshal(_Type, undefined) ->
    undefined;
maybe_marshal(Type, Value) ->
    marshal(Type, Value).

-spec assert_is_utc(binary()) -> ok | no_return().
assert_is_utc(Rfc3339) ->
    Size0 = erlang:byte_size(Rfc3339),
    Size1 = Size0 - 1,
    Size6 = Size0 - 6,
    case Rfc3339 of
        <<_:Size1/bytes, "Z">> ->
            ok;
        <<_:Size6/bytes, "+00:00">> ->
            ok;
        <<_:Size6/bytes, "-00:00">> ->
            ok;
        _ ->
            erlang:error(badarg, [timestamp, Rfc3339, badoffset])
    end.
