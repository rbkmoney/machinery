%%%
%%% Machinery machinegun backend

%% TODO
%%
%%  - There's marshalling scattered around which is common enough for _any_ thrift interface.

-module(machinery_mg_backend).
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").

-type namespace()       :: machinery:namespace().
-type ref()             :: machinery:ref().
-type id()              :: machinery:id().
-type range()           :: machinery:range().
-type args(T)           :: machinery:args(T).
-type response(T)       :: machinery:response(T).
-type error(T)          :: machinery:error(T).
-type machine(E, A)     :: machinery:machine(E, A).
-type logic_handler(T)  :: machinery:logic_handler(T).

-define(BACKEND_CORE_OPTS,
    schema          := machinery_mg_schema:schema()
).

-define(MICROS_PER_SEC, (1000 * 1000)).

%% Server types
-type backend_config() :: #{
    ?BACKEND_CORE_OPTS
}.

-type handler_config() :: #{
    path            := woody:path(),
    backend_config  := backend_config()
}.

-type handler(A)       :: {logic_handler(A), handler_config()}. %% handler server spec

-type handler_opts() :: machinery:handler_opts(#{
    woody_ctx    := woody_context:ctx()
}).

-type backend_handler_opts() :: #{
    handler      := logic_handler(_),
    ?BACKEND_CORE_OPTS
}.

%% Client types
-type backend_opts() :: machinery:backend_opts(#{
    woody_ctx       := woody_context:ctx(),
    client          := machinery_mg_client:woody_client(),
    ?BACKEND_CORE_OPTS
}).

-type backend() :: {?MODULE, backend_opts()}.

-type backend_opts_static() :: #{
    client          := machinery_mg_client:woody_client(),
    ?BACKEND_CORE_OPTS
}.

-export_type([backend_config/0]).
-export_type([handler_config/0]).
-export_type([logic_handler/1]).
-export_type([handler/1]).
-export_type([handler_opts/0]).
-export_type([backend_opts/0]).
-export_type([backend/0]).

%% API
-export([get_routes/2]).
-export([get_handler/2]).
-export([new/2]).

%% Machinery backend
-behaviour(machinery_backend).

-export([start/4]).
-export([call/5]).
-export([repair/5]).
-export([get/4]).

%% Woody handler
-behaviour(woody_server_thrift_handler).

-export([handle_function/4]).

%% API

-spec get_routes([handler(_)], machinery_utils:route_opts()) ->
    machinery_utils:woody_routes().
get_routes(Handlers, Opts) ->
    machinery_utils:get_woody_routes(Handlers, fun get_handler/2, Opts).

-spec get_handler(handler(_), machinery_utils:route_opts()) ->
    machinery_utils:woody_handler().
get_handler({LogicHandler, #{path := Path, backend_config := Config}}, _) ->
    {Path, {
        {mg_proto_state_processing_thrift, 'Processor'},
        {?MODULE, get_backend_handler_opts(LogicHandler, Config)}
    }}.

-spec new(woody_context:ctx(), backend_opts_static()) ->
    backend().
new(WoodyCtx, Opts = #{client := _, schema := _}) ->
    {?MODULE, Opts#{woody_ctx => WoodyCtx}}.

%% Machinery backend

-spec start(namespace(), id(), args(_), backend_opts()) ->
    ok | {error, exists}.
start(NS, ID, Args, Opts) ->
    Client = get_client(Opts),
    Schema = get_schema(Opts),
    InitArgs = marshal({schema, Schema, {args, init}}, Args),
    case machinery_mg_client:start(marshal(namespace, NS), marshal(id, ID), InitArgs, Client) of
        {ok, ok} ->
            ok;
        {exception, #mg_stateproc_MachineAlreadyExists{}} ->
            {error, exists};
        {exception, #mg_stateproc_NamespaceNotFound{}} ->
            error({namespace_not_found, NS});
        {exception, #mg_stateproc_MachineFailed{}} ->
            error({failed, NS, ID})
    end.

-spec call(namespace(), ref(), range(), args(_), backend_opts()) ->
    {ok, response(_)} | {error, notfound}.
call(NS, Ref, Range, Args, Opts) ->
    Client = get_client(Opts),
    Schema = get_schema(Opts),
    Descriptor = {NS, Ref, Range},
    CallArgs = marshal({schema, Schema, {args, call}}, Args),
    case machinery_mg_client:call(marshal(descriptor, Descriptor), CallArgs, Client) of
        {ok, Response} ->
            {ok, unmarshal({schema, Schema, {response, call}}, Response)};
        {exception, #mg_stateproc_MachineNotFound{}} ->
            {error, notfound};
        {exception, #mg_stateproc_NamespaceNotFound{}} ->
            error({namespace_not_found, NS});
        {exception, #mg_stateproc_MachineFailed{}} ->
            error({failed, NS, Ref})
    end.

-spec repair(namespace(), ref(), range(), args(_), backend_opts()) ->
    {ok, response(_)} | {error, {failed, error(_)} | notfound | working}.
repair(NS, Ref, Range, Args, Opts) ->
    Client = get_client(Opts),
    Schema = get_schema(Opts),
    Descriptor = {NS, Ref, Range},
    CallArgs = marshal({schema, Schema, {args, repair}}, Args),
    case machinery_mg_client:repair(marshal(descriptor, Descriptor), CallArgs, Client) of
        {ok, Response} ->
            {ok, unmarshal({schema, Schema, {response, {repair, success}}}, Response)};
        {exception, #mg_stateproc_RepairFailed{reason = Reason}} ->
            {error, {failed, unmarshal({schema, Schema, {response, {repair, failure}}}, Reason)}};
        {exception, #mg_stateproc_MachineNotFound{}} ->
            {error, notfound};
        {exception, #mg_stateproc_MachineAlreadyWorking{}} ->
            {error, working};
        {exception, #mg_stateproc_NamespaceNotFound{}} ->
            error({namespace_not_found, NS});
        {exception, #mg_stateproc_MachineFailed{}} ->
            error({failed, NS, Ref})
    end.

-spec get(namespace(), ref(), range(), backend_opts()) ->
    {ok, machine(_, _)} | {error, notfound}.
get(NS, Ref, Range, Opts) ->
    Client = get_client(Opts),
    Schema = get_schema(Opts),
    Descriptor = {NS, Ref, Range},
    case machinery_mg_client:get_machine(marshal(descriptor, Descriptor), Client) of
        {ok, Machine} ->
            {ok, unmarshal({machine, Schema}, Machine)};
        {exception, #mg_stateproc_MachineNotFound{}} ->
            {error, notfound};
        {exception, #mg_stateproc_NamespaceNotFound{}} ->
            error({namespace_not_found, NS})
    end.

%% Woody handler

-spec handle_function
    ('ProcessSignal', woody:args(), woody_context:ctx(), backend_handler_opts()) ->
        {ok, mg_proto_state_processing_thrift:'SignalResult'()};
    ('ProcessCall', woody:args(), woody_context:ctx(), backend_handler_opts()) ->
        {ok, mg_proto_state_processing_thrift:'CallResult'()};
    ('ProcessRepair', woody:args(), woody_context:ctx(), backend_handler_opts()) ->
        {ok, mg_proto_state_processing_thrift:'RepairResult'()}.
handle_function('ProcessSignal', FunctionArgs, WoodyCtx, Opts) ->
    [#mg_stateproc_SignalArgs{signal = Signal, machine = Machine}] = FunctionArgs,
    #{handler := Handler, schema := Schema} = Opts,
    Machine1 = unmarshal({machine, Schema}, Machine),
    Result   = dispatch_signal(
        unmarshal({signal, Schema}, Signal),
        Machine1,
        machinery_utils:get_handler(Handler),
        get_handler_opts(WoodyCtx)
    ),
    {ok, marshal({signal_result, Schema}, handle_result(Result, Machine1))};
handle_function('ProcessCall', FunctionArgs, WoodyCtx, Opts) ->
    [#mg_stateproc_CallArgs{arg = Args, machine = Machine}] = FunctionArgs,
    #{handler := Handler, schema := Schema} = Opts,
    Machine1 = unmarshal({machine, Schema}, Machine),
    {Response, Result} = dispatch_call(
        unmarshal({schema, Schema, {args, call}}, Args),
        Machine1,
        machinery_utils:get_handler(Handler),
        get_handler_opts(WoodyCtx)
    ),
    {ok, marshal({call_result, Schema}, {Response, handle_result(Result, Machine1)})};
handle_function('ProcessRepair', FunctionArgs, WoodyCtx, Opts) ->
    [#mg_stateproc_RepairArgs{arg = Args, machine = Machine}] = FunctionArgs,
    #{handler := Handler, schema := Schema} = Opts,
    Machine1 = unmarshal({machine, Schema}, Machine),
    RepairResult = dispatch_repair(
        unmarshal({schema, Schema, {args, repair}}, Args),
        Machine1,
        machinery_utils:get_handler(Handler),
        get_handler_opts(WoodyCtx)
    ),
    case RepairResult of
        {ok, Response, Result} ->
            {ok, marshal({repair_result, Schema}, {Response, handle_result(Result, Machine1)})};
        {error, Reason} ->
            erlang:throw(marshal({repair_fail, Schema}, Reason))
    end.

%% Utils

-spec get_backend_handler_opts(logic_handler(_), backend_config()) ->
    backend_handler_opts().
get_backend_handler_opts(Handler, Config) ->
    Config#{handler => Handler}.

get_schema(#{schema := Schema}) ->
    Schema.

get_client(#{client := Client, woody_ctx := WoodyCtx}) ->
    machinery_mg_client:new(Client, WoodyCtx).

get_handler_opts(WoodyCtx) ->
    #{woody_ctx => WoodyCtx}.

dispatch_signal(Signal, Machine, Handler, Opts) ->
    machinery:dispatch_signal(Signal, Machine, Handler, Opts).

dispatch_call(Args, Machine, Handler, Opts) ->
    machinery:dispatch_call(Args, Machine, Handler, Opts).

dispatch_repair(Args, Machine, Handler, Opts) ->
    machinery:dispatch_repair(Args, Machine, Handler, Opts).

handle_result(Result, OrigMachine) ->
    Result#{aux_state => set_aux_state(
        maps:get(aux_state, Result, undefined),
        maps:get(aux_state, OrigMachine, machinery_msgpack:nil())
    )}.

set_aux_state(undefined, ReceivedState) ->
    ReceivedState;
set_aux_state(NewState, _) ->
    NewState.

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
    Version = machinery_mg_schema:get_version(Schema, event),
    #mg_stateproc_Event{
        'id'         = marshal(event_id, EventID),
        'created_at' = marshal(timestamp, CreatedAt),
        'data'       = marshal({schema, Schema, {event, Version}}, Body)
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
        response = marshal({schema, Schema, {response, call}}, Response),
        change   = marshal({state_change, Schema}, V),
        action   = marshal(action, maps:get(action, V, []))
    };

marshal({repair_result, Schema}, {Response, #{} = V}) ->
    #mg_stateproc_RepairResult{
        response = marshal({schema, Schema, {response, {repair, success}}}, Response),
        change   = marshal({state_change, Schema}, V),
        action   = marshal(action, maps:get(action, V, []))
    };

marshal({repair_fail, Schema}, Reason) ->
    #mg_stateproc_RepairFailed{
        reason = marshal({schema, Schema, {response, {repair, failure}}}, Reason)
    };

marshal({state_change, Schema}, #{} = V) ->
    Version = machinery_mg_schema:get_version(Schema, aux_state),
    #mg_stateproc_MachineStateChange{
        events = [
            #mg_stateproc_Content{data = Event}
            || Event <- marshal({list, {schema, Schema, {event, Version}}}, maps:get(events, V, []))
        ],
        % TODO
        % Provide this to logic handlers as well
        aux_state = #mg_stateproc_Content{
            data = marshal({schema, Schema, {aux_state, Version}}, maps:get(aux_state, V, undefined))
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
        'aux_state'     = #mg_stateproc_Content{format_version = Version, data = AuxState}
    }
) ->
    #{
        ns              => unmarshal(namespace, NS),
        id              => unmarshal(id, ID),
        history         => unmarshal({history, Schema}, History),
        range           => unmarshal(range, Range),
        aux_state       => unmarshal({maybe, {schema, Schema, {aux_state, Version}}}, AuxState)
    };

unmarshal({history, Schema}, V) ->
    unmarshal({list, {event, Schema}}, V);

unmarshal(
    {event, Schema},
    #mg_stateproc_Event{
        'id'             = EventID,
        'created_at'     = CreatedAt,
        'format_version' = Version,
        'data'           = Payload
    }
) ->
    {
        unmarshal(event_id, EventID),
        unmarshal(timestamp, CreatedAt),
        unmarshal({schema, Schema, {event, Version}}, Payload)
    };

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
