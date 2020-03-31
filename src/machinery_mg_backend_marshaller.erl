%%%
%%% Copyright 2020 RBKmoney
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%

-module(machinery_mg_backend_marshaller).

-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").

-behaviour(machinery_backend_marshaller).

%% API
-export([marshal/2, unmarshal/2]).

-define(MICROS_PER_SEC, (1000 * 1000)).

%% Marshalling

%% No marshalling for the machine required by the protocol so far.
%%
%% marshal(
%%     {machine, Schema},
%%     #{
%%         ns              := NS,
%%         id              := ID,
%%         history         := History
%%     }
%% ) ->
%%     #mg_stateproc_Machine{
%%         'ns'            = marshal(namespace, NS),
%%         'id'            = marshal(id, ID),
%%         'history'       = marshal({history, Schema}, History),
%%         % TODO
%%         % There are required fields left
%%         'history_range' = marshal(range, {undefined, undefined, forward})
%%     };

-spec marshal(machinery_backend_marshaller:kind(), machinery_backend_marshaller:args()) ->
    machinery_backend_marshaller:result().

marshal(descriptor, {NS, Ref, Range}) ->
    #mg_stateproc_MachineDescriptor{
        'ns'        = marshal(namespace, NS),
        'ref'       = marshal(ref, Ref),
        'range'     = marshal(range, Range)
    };

marshal(range, {Cursor, Limit, Direction}) ->
    #mg_stateproc_HistoryRange{
        'after'     = marshal({maybe, event_id}, Cursor),
        'limit'     = marshal(limit, Limit),
        'direction' = marshal(direction, Direction)
    };

marshal({history, Schema}, V) ->
    marshal({list, {event, Schema}}, V);
marshal({event, Schema}, {EventID, CreatedAt, Body}) ->
    #mg_stateproc_Event{
        'id'         = marshal(event_id, EventID),
        'created_at' = marshal(timestamp, CreatedAt),
        'data'       = marshal({schema, Schema, event}, Body)
    };

marshal({signal, Schema}, {init, Args}) ->
    {init, #mg_stateproc_InitSignal{arg = marshal({schema, Schema, {args, init}}, Args)}};

marshal({signal, _Schema}, timeout) ->
    {timeout, #mg_stateproc_TimeoutSignal{}};

marshal({signal, Schema}, {repair, Args}) ->
    {repair, #mg_stateproc_RepairSignal{arg = marshal({maybe, {schema, Schema, {args, repair}}}, Args)}};

marshal({signal_result, Schema}, #{} = V) ->
    #mg_stateproc_SignalResult{
        change = marshal({state_change, Schema}, V),
        action = marshal(action, maps:get(action, V, []))
    };

marshal({call_result, Schema}, {Response, #{} = V}) ->
    #mg_stateproc_CallResult{
        response = marshal({schema, Schema, response}, Response),
        change   = marshal({state_change, Schema}, V),
        action   = marshal(action, maps:get(action, V, []))
    };

marshal({state_change, Schema}, #{} = V) ->
    #mg_stateproc_MachineStateChange{
        events = [
            #mg_stateproc_Content{data = Event}
            || Event <- marshal({list, {schema, Schema, event}}, maps:get(events, V, []))
        ],
        % TODO
        % Provide this to logic handlers as well
        aux_state = #mg_stateproc_Content{
            data = marshal({schema, Schema, aux_state}, maps:get(aux_state, V, undefined))
        }
    };

marshal(action, V) when is_list(V) ->
    lists:foldl(fun apply_action/2, #mg_stateproc_ComplexAction{}, V);

marshal(action, V) ->
    marshal(action, [V]);

marshal(timer, {timeout, V}) ->
    {timeout, marshal(integer, V)};

marshal(timer, {deadline, V}) ->
    {deadline, marshal(timestamp, V)};

marshal(namespace, V) ->
    marshal(atom, V);

marshal(ref, V) when is_binary(V) ->
    {id, marshal(id, V)};

marshal(ref, {tag, V}) ->
    {tag, marshal(tag, V)};

marshal(id, V) ->
    marshal(string, V);

marshal(tag, V) ->
    marshal(string, V);

marshal(event_id, V) ->
    marshal(integer, V);

marshal(limit, V) ->
    marshal({maybe, integer}, V);

marshal(direction, V) ->
    marshal({enum, [forward, backward]}, V);

marshal({schema, Schema, T}, V) ->
    % TODO
    % Marshal properly
    machinery_mg_schema:marshal(Schema, T, V);

marshal(timestamp, {DateTime, USec}) ->
    Ts = genlib_time:daytime_to_unixtime(DateTime) * ?MICROS_PER_SEC + USec,
    Str = calendar:system_time_to_rfc3339(Ts, [{unit, microsecond}, {offset, "Z"}]),
    erlang:list_to_binary(Str);

marshal({list, T}, V) when is_list(V) ->
    [marshal(T, E) || E <- V];

marshal({maybe, _}, undefined) ->
    undefined;

marshal({maybe, T}, V) ->
    marshal(T, V);

marshal({enum, Choices = [_ | _]} = T, V) when is_atom(V) ->
    _ = lists:member(V, Choices) orelse erlang:error(badarg, [T, V]),
    V;

marshal(atom, V) when is_atom(V) ->
    atom_to_binary(V, utf8);

marshal(string, V) when is_binary(V) ->
    V;

marshal(integer, V) when is_integer(V) ->
    V;

marshal(T, V) ->
    erlang:error(badarg, [T, V]).

apply_action({set_timer, V}, CA) ->
    CA#mg_stateproc_ComplexAction{
        timer = {set_timer, #mg_stateproc_SetTimerAction{timer = marshal(timer, V)}}
    };

apply_action({set_timer, T, Range}, CA) ->
    CA#mg_stateproc_ComplexAction{
        timer = {set_timer, #mg_stateproc_SetTimerAction{
            timer   = marshal(timer, T),
            range   = marshal(range, Range)
        }}
    };

apply_action({set_timer, T, Range, HandlingTimeout}, CA) ->
    CA#mg_stateproc_ComplexAction{
        timer = {set_timer, #mg_stateproc_SetTimerAction{
            timer   = marshal(timer, T),
            range   = marshal(range, Range),
            timeout = marshal(integer, HandlingTimeout)
        }}
    };

apply_action(unset_timer, CA) ->
    CA#mg_stateproc_ComplexAction{
        timer = {unset_timer, #mg_stateproc_UnsetTimerAction{}}
    };

apply_action(continue, CA) ->
    CA#mg_stateproc_ComplexAction{
        timer = {set_timer, #mg_stateproc_SetTimerAction{timer = {timeout, 0}}}
    };

apply_action(remove, CA) ->
    CA#mg_stateproc_ComplexAction{
        remove = #mg_stateproc_RemoveAction{}
    };

apply_action({tag, Tag}, CA) ->
    CA#mg_stateproc_ComplexAction{
        tag = #mg_stateproc_TagAction{
            tag = marshal(tag, Tag)
        }
    }.

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

-spec unmarshal(machinery_backend_marshaller:kind(), machinery_backend_marshaller:args()) ->
    machinery_backend_marshaller:result().

unmarshal(
    range,
    #mg_stateproc_HistoryRange{
        'after'         = Cursor,
        'limit'         = Limit,
        'direction'     = Direction
    }
) ->
    {unmarshal({maybe, event_id}, Cursor), unmarshal(limit, Limit), unmarshal(direction, Direction)};

unmarshal(
    {machine, Schema},
    #mg_stateproc_Machine{
        'ns'            = NS,
        'id'            = ID,
        'history'       = History,
        'history_range' = Range,
        'aux_state'     = #mg_stateproc_Content{format_version = _Version, data = AuxState}
    }
) ->
    #{
        ns              => unmarshal(namespace, NS),
        id              => unmarshal(id, ID),
        history         => unmarshal({history, Schema}, History),
        range           => unmarshal(range, Range),
        aux_state       => unmarshal({maybe, {schema, Schema, aux_state}}, AuxState)
    };

unmarshal({history, Schema}, V) ->
    unmarshal({list, {event, Schema}}, V);

unmarshal(
    {event, Schema},
    #mg_stateproc_Event{
        'id'         = EventID,
        'created_at' = CreatedAt,
        'data'       = Payload
    }
) ->
    {unmarshal(event_id, EventID), unmarshal(timestamp, CreatedAt), unmarshal({schema, Schema, event}, Payload)};

unmarshal({signal, Schema}, {init, #mg_stateproc_InitSignal{arg = Args}}) ->
    {init, unmarshal({schema, Schema, {args, init}}, Args)};

unmarshal({signal, _Schema}, {timeout, #mg_stateproc_TimeoutSignal{}}) ->
    timeout;

unmarshal({signal, Schema}, {repair, #mg_stateproc_RepairSignal{arg = Args}}) ->
    {repair, unmarshal({maybe, {schema, Schema, {args, repair}}}, Args)};

unmarshal(namespace, V) ->
    unmarshal(atom, V);

unmarshal(id, V) ->
    unmarshal(string, V);

unmarshal(event_id, V) ->
    unmarshal(integer, V);

unmarshal(limit, V) ->
    unmarshal({maybe, integer}, V);

unmarshal(direction, V) ->
    unmarshal({enum, [forward, backward]}, V);

unmarshal({schema, Schema, T}, V) ->
    machinery_mg_schema:unmarshal(Schema, T, V);

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

unmarshal({list, T}, V) when is_list(V) ->
    [unmarshal(T, E) || E <- V];

unmarshal({maybe, _}, undefined) ->
    undefined;

unmarshal({maybe, T}, V) ->
    unmarshal(T, V);

unmarshal({enum, Choices = [_ | _]} = T, V) when is_atom(V) ->
    case lists:member(V, Choices) of
        true ->
            V;
        false ->
            erlang:error(badarg, [T, V])
    end;

unmarshal(atom, V) when is_binary(V) ->
    binary_to_existing_atom(V, utf8);

unmarshal(string, V) when is_binary(V) ->
    V;

unmarshal(integer, V) when is_integer(V) ->
    V;

unmarshal(T, V) ->
    erlang:error(badarg, [T, V]).

-spec assert_is_utc(binary()) ->
    ok | no_return().
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
