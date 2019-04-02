%%%
%%% Machinery machinegun backend

%% TODO
%%
%%  - There's marshalling scattered around which is common enough for _any_ thrift interface.

-module(machinery_mg_backend).
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").

-type namespace()       :: machinery:namespace().
-type id()              :: machinery:id().
-type range()           :: machinery:range().
-type args(T)           :: machinery:args(T).
-type response(T)       :: machinery:response(T).
-type machine(E, A)     :: machinery:machine(E, A).
-type logic_handler(T)  :: machinery:logic_handler(T).

-define(BACKEND_CORE_OPTS,
    schema          := machinery_mg_schema:schema()
).

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

-spec call(namespace(), id(), range(), args(_), backend_opts()) ->
    {ok, response(_)} | {error, notfound}.
call(NS, ID, Range, Args, Opts) ->
    Client = get_client(Opts),
    Schema = get_schema(Opts),
    Descriptor = {NS, ID, Range},
    CallArgs = marshal({schema, Schema, {args, call}}, Args),
    case machinery_mg_client:call(marshal(descriptor, Descriptor), CallArgs, Client) of
        {ok, Response} ->
            {ok, unmarshal({schema, Schema, response}, Response)};
        {exception, #mg_stateproc_MachineNotFound{}} ->
            {error, notfound};
        {exception, #mg_stateproc_NamespaceNotFound{}} ->
            error({namespace_not_found, NS});
        {exception, #mg_stateproc_MachineFailed{}} ->
            error({failed, NS, ID})
    end.

-spec repair(namespace(), id(), range(), args(_), backend_opts()) ->
    ok | {error, notfound | working}.
repair(NS, ID, Range, Args, Opts) ->
    Client = get_client(Opts),
    Schema = get_schema(Opts),
    Descriptor = {NS, ID, Range},
    CallArgs = marshal({schema, Schema, {args, repair}}, Args),
    case machinery_mg_client:repair(marshal(descriptor, Descriptor), CallArgs, Client) of
        {ok, ok} ->
            ok;
        {exception, #mg_stateproc_MachineNotFound{}} ->
            {error, notfound};
        {exception, #mg_stateproc_MachineAlreadyWorking{}} ->
            {error, working};
        {exception, #mg_stateproc_NamespaceNotFound{}} ->
            error({namespace_not_found, NS})
    end.

-spec get(namespace(), id(), range(), backend_opts()) ->
    {ok, machine(_, _)} | {error, notfound}.
get(NS, ID, Range, Opts) ->
    Client = get_client(Opts),
    Schema = get_schema(Opts),
    Descriptor = {NS, ID, Range},
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
        {ok, mg_proto_state_processing_thrift:'CallResult'()}.
handle_function(
    'ProcessSignal',
    [#mg_stateproc_SignalArgs{signal = Signal, machine = Machine}],
    WoodyCtx,
    #{handler := Handler, schema := Schema}
) ->
    Machine1 = unmarshal({machine, Schema}, Machine),
    Result   = dispatch_signal(
        unmarshal({signal, Schema}, Signal),
        Machine1,
        machinery_utils:get_handler(Handler),
        get_handler_opts(WoodyCtx)
    ),
    {ok, marshal({signal_result, Schema}, handle_result(Result, Machine1))};
handle_function(
    'ProcessCall',
    [#mg_stateproc_CallArgs{arg = Args, machine = Machine}],
    WoodyCtx,
    #{handler := Handler, schema := Schema}
) ->
    Machine1 = unmarshal({machine, Schema}, Machine),
    {Response, Result} = dispatch_call(
        unmarshal({schema, Schema, {args, call}}, Args),
        Machine1,
        machinery_utils:get_handler(Handler),
        get_handler_opts(WoodyCtx)
    ),
    {ok, marshal({call_result, Schema}, {Response, handle_result(Result, Machine1)})}.

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

marshal(descriptor, {NS, ID, Range}) ->
    #mg_stateproc_MachineDescriptor{
        'ns'        = marshal(namespace, NS),
        'ref'       = {'id', marshal(id, ID)},
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
        'id'            = marshal(event_id, EventID),
        'created_at'    = marshal(timestamp, CreatedAt),
        'event_payload' = marshal({schema, Schema, event}, Body)
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
        events = marshal({list, {schema, Schema, event}}, maps:get(events, V, [])),
        % TODO
        % Provide this to logic handlers as well
        aux_state = marshal({schema, Schema, aux_state}, maps:get(aux_state, V, undefined))
    };

marshal(action, V) when is_list(V) ->
    lists:foldl(fun apply_action/2, #mg_stateproc_ComplexAction{}, V);

marshal(action, V) ->
    marshal(action, [V]);

marshal(timer, {timeout, V}) when V > 0 ->
    {timeout, marshal(integer, V)};

marshal(timer, {deadline, V}) ->
    {deadline, marshal(timestamp, V)};

marshal(namespace, V) ->
    marshal(atom, V);

marshal(id, V) ->
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

marshal(timestamp, {{Date, Time}, USec} = V) ->
    {ok, Result} = rfc3339:format({Date, Time, USec, 0}),
    if is_binary(Result) ->
        Result;
    true ->
        error(badarg, {timestamp, V})
    end;

marshal({list, T}, V) when is_list(V) ->
    [marshal(T, E) || E <- V];

marshal({maybe, _}, undefined) ->
    undefined;

marshal({maybe, T}, V) ->
    marshal(T, V);

marshal({enum, Choices = [_ | _]} = T, V) when is_atom(V) ->
    _ = lists:member(V, Choices) orelse error(badarg, {T, V}),
    V;

marshal(atom, V) when is_atom(V) ->
    atom_to_binary(V, utf8);

marshal(string, V) when is_binary(V) ->
    V;

marshal(integer, V) when is_integer(V) ->
    V;

marshal(T, V) ->
    error(badarg, {T, V}).

apply_action({set_timer, V}, CA) ->
    CA#mg_stateproc_ComplexAction{
        timer = {set_timer, #mg_stateproc_SetTimerAction{timer = marshal(timer, V)}}
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
        'aux_state'     = AuxState
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
        'id'            = EventID,
        'created_at'    = CreatedAt,
        'event_payload' = Payload
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
    case rfc3339:parse(V) of
        {ok, {Date, Time, USec, TZOffset}} when TZOffset == undefined orelse TZOffset == 0 ->
            {{Date, Time}, USec};
        {ok, _} ->
            error(badarg, {timestamp, V, badoffset});
        {error, Reason} ->
            error(badarg, {timestamp, V, Reason})
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
            error(badarg, {T, V})
    end;

unmarshal(atom, V) when is_binary(V) ->
    binary_to_existing_atom(V, utf8);

unmarshal(string, V) when is_binary(V) ->
    V;

unmarshal(integer, V) when is_integer(V) ->
    V;

unmarshal(T, V) ->
    error(badarg, {T, V}).
