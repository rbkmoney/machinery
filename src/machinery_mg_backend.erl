%%%
%%% Machinery machinegun backend

%% TODO
%%
%%  - There's marshalling scattered around which is common enough for _any_ thrift interface.

-module(machinery_mg_backend).

-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").

-type namespace() :: machinery:namespace().
-type ref() :: machinery:ref().
-type id() :: machinery:id().
-type range() :: machinery:range().
-type args(T) :: machinery:args(T).
-type response(T) :: machinery:response(T).
-type error(T) :: machinery:error(T).
-type machine(E, A) :: machinery:machine(E, A).
-type logic_handler(T) :: machinery:logic_handler(T).

-define(BACKEND_CORE_OPTS,
    schema := machinery_mg_schema:schema()
).

-define(MICROS_PER_SEC, (1000 * 1000)).

%% Server types
-type backend_config() :: #{
    ?BACKEND_CORE_OPTS
}.

-type handler_config() :: #{
    path := woody:path(),
    backend_config := backend_config()
}.

%% handler server spec
-type handler(A) :: {logic_handler(A), handler_config()}.

-type handler_opts() :: machinery:handler_opts(#{
    woody_ctx := woody_context:ctx()
}).

-type backend_handler_opts() :: #{
    handler := logic_handler(_),
    ?BACKEND_CORE_OPTS
}.

%% Client types
-type backend_opts() :: machinery:backend_opts(#{
    woody_ctx := woody_context:ctx(),
    client := machinery_mg_client:woody_client(),
    ?BACKEND_CORE_OPTS
}).

-type backend() :: {?MODULE, backend_opts()}.

-type backend_opts_static() :: #{
    client := machinery_mg_client:woody_client(),
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

-spec get_routes([handler(_)], machinery_utils:route_opts()) -> machinery_utils:woody_routes().
get_routes(Handlers, Opts) ->
    machinery_utils:get_woody_routes(Handlers, fun get_handler/2, Opts).

-spec get_handler(handler(_), machinery_utils:route_opts()) -> machinery_utils:woody_handler().
get_handler({LogicHandler, #{path := Path, backend_config := Config}}, _) ->
    {Path, {
        {mg_proto_state_processing_thrift, 'Processor'},
        {?MODULE, get_backend_handler_opts(LogicHandler, Config)}
    }}.

-spec new(woody_context:ctx(), backend_opts_static()) -> backend().
new(WoodyCtx, Opts = #{client := _, schema := _}) ->
    {?MODULE, Opts#{woody_ctx => WoodyCtx}}.

%% Machinery backend

-spec start(namespace(), id(), args(_), backend_opts()) -> ok | {error, exists}.
start(NS, ID, Args, Opts) ->
    Client = get_client(Opts),
    Schema = get_schema(Opts),
    SContext0 = build_schema_context(NS, ID),
    {InitArgs, _SContext1} = marshal({schema, Schema, {args, init}, SContext0}, Args),
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

-spec call(namespace(), ref(), range(), args(_), backend_opts()) -> {ok, response(_)} | {error, notfound}.
call(NS, Ref, Range, Args, Opts) ->
    Client = get_client(Opts),
    Schema = get_schema(Opts),
    SContext0 = build_schema_context(NS, Ref),
    Descriptor = {NS, Ref, Range},
    {CallArgs, SContext1} = marshal({schema, Schema, {args, call}, SContext0}, Args),
    case machinery_mg_client:call(marshal(descriptor, Descriptor), CallArgs, Client) of
        {ok, Response0} ->
            {Response1, _SContext2} = unmarshal({schema, Schema, {response, call}, SContext1}, Response0),
            {ok, Response1};
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
    SContext0 = build_schema_context(NS, Ref),
    Descriptor = {NS, Ref, Range},
    {RepairArgs, SContext1} = marshal({schema, Schema, {args, repair}, SContext0}, Args),
    case machinery_mg_client:repair(marshal(descriptor, Descriptor), RepairArgs, Client) of
        {ok, Response0} ->
            {Response1, _SContext2} = unmarshal({schema, Schema, {response, {repair, success}}, SContext1}, Response0),
            {ok, Response1};
        {exception, #mg_stateproc_RepairFailed{reason = Reason}} ->
            {error, {failed, unmarshal({schema, Schema, {response, {repair, failure}}, SContext1}, Reason)}};
        {exception, #mg_stateproc_MachineNotFound{}} ->
            {error, notfound};
        {exception, #mg_stateproc_MachineAlreadyWorking{}} ->
            {error, working};
        {exception, #mg_stateproc_NamespaceNotFound{}} ->
            error({namespace_not_found, NS});
        {exception, #mg_stateproc_MachineFailed{}} ->
            error({failed, NS, Ref})
    end.

-spec get(namespace(), ref(), range(), backend_opts()) -> {ok, machine(_, _)} | {error, notfound}.
get(NS, Ref, Range, Opts) ->
    Client = get_client(Opts),
    Schema = get_schema(Opts),
    Descriptor = {NS, Ref, Range},
    case machinery_mg_client:get_machine(marshal(descriptor, Descriptor), Client) of
        {ok, Machine0} ->
            {Machine1, _Context} = unmarshal({machine, Schema}, Machine0),
            {ok, Machine1};
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
    {#mg_stateproc_SignalArgs{signal = MarshaledSignal, machine = MarshaledMachine}} = FunctionArgs,
    #{handler := Handler, schema := Schema} = Opts,
    {Machine, SContext0} = unmarshal({machine, Schema}, MarshaledMachine),
    {Signal, SContext1} = unmarshal({signal, Schema, SContext0}, MarshaledSignal),
    Result = dispatch_signal(
        Signal,
        Machine,
        machinery_utils:get_handler(Handler),
        get_handler_opts(WoodyCtx)
    ),
    {ok, marshal({signal_result, Schema, SContext1}, handle_result(Result, Machine))};
handle_function('ProcessCall', FunctionArgs, WoodyCtx, Opts) ->
    {#mg_stateproc_CallArgs{arg = MarshaledArgs, machine = MarshaledMachine}} = FunctionArgs,
    #{handler := Handler, schema := Schema} = Opts,
    {Machine, SContext0} = unmarshal({machine, Schema}, MarshaledMachine),
    {Args, SContext1} = unmarshal({schema, Schema, {args, call}, SContext0}, MarshaledArgs),
    {Response, Result} = dispatch_call(
        Args,
        Machine,
        machinery_utils:get_handler(Handler),
        get_handler_opts(WoodyCtx)
    ),
    {ok, marshal({call_result, Schema, SContext1}, {Response, handle_result(Result, Machine)})};
handle_function('ProcessRepair', FunctionArgs, WoodyCtx, Opts) ->
    {#mg_stateproc_RepairArgs{arg = MarshaledArgs, machine = MarshaledMachine}} = FunctionArgs,
    #{handler := Handler, schema := Schema} = Opts,
    {Machine, SContext0} = unmarshal({machine, Schema}, MarshaledMachine),
    {Args, SContext1} = unmarshal({schema, Schema, {args, repair}, SContext0}, MarshaledArgs),
    RepairResult = dispatch_repair(
        Args,
        Machine,
        machinery_utils:get_handler(Handler),
        get_handler_opts(WoodyCtx)
    ),
    case RepairResult of
        {ok, {Response, Result}} ->
            {ok, marshal({repair_result, Schema, SContext1}, {Response, handle_result(Result, Machine)})};
        {error, Reason} ->
            erlang:throw(marshal({repair_fail, Schema, SContext1}, Reason))
    end.

%% Utils

-spec get_backend_handler_opts(logic_handler(_), backend_config()) -> backend_handler_opts().
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
    Result#{
        aux_state => set_aux_state(
            maps:get(aux_state, Result, undefined),
            maps:get(aux_state, OrigMachine, machinery_msgpack:nil())
        )
    }.

set_aux_state(undefined, ReceivedState) ->
    ReceivedState;
set_aux_state(NewState, _) ->
    NewState.

-spec build_schema_context(namespace(), ref()) -> machinery_mg_schema:context().
build_schema_context(NS, Ref) ->
    #{
        machine_ns => NS,
        machine_ref => Ref
    }.

%% Marshalling

marshal({signal_result, Schema, Context}, #{} = V) ->
    #mg_stateproc_SignalResult{
        change = marshal({state_change, Schema, Context}, V),
        action = marshal(action, maps:get(action, V, []))
    };
marshal({call_result, Schema, Context}, {Response0, #{} = V}) ->
    % It is expected that schema doesn't want to save anything in the context here.
    % The main reason for this is the intention to simplify the code.
    % So, feel free to change the behavior it is needed for you.
    {Response1, Context} = marshal({schema, Schema, {response, call}, Context}, Response0),
    #mg_stateproc_CallResult{
        response = Response1,
        change = marshal({state_change, Schema, Context}, V),
        action = marshal(action, maps:get(action, V, []))
    };
marshal({repair_result, Schema, Context}, {Response0, #{} = V}) ->
    % It is expected that schema doesn't want to save anything in the context here.
    {Response1, Context} = marshal({schema, Schema, {response, {repair, success}}, Context}, Response0),
    #mg_stateproc_RepairResult{
        response = Response1,
        change = marshal({state_change, Schema, Context}, V),
        action = marshal(action, maps:get(action, V, []))
    };
marshal({repair_fail, Schema, Context}, Reason) ->
    % It is expected that schema doesn't want to save anything in the context here.
    {Reason1, Context} = marshal({schema, Schema, {response, {repair, failure}}, Context}, Reason),
    #mg_stateproc_RepairFailed{
        reason = Reason1
    };
marshal({state_change, Schema, Context}, #{} = V) ->
    AuxStateVersion = machinery_mg_schema:get_version(Schema, aux_state),
    EventVersion = machinery_mg_schema:get_version(Schema, event),
    #mg_stateproc_MachineStateChange{
        events = marshal({list, {new_event_change, EventVersion, Schema, Context}}, maps:get(events, V, [])),
        aux_state = marshal({aux_state_change, AuxStateVersion, Schema, Context}, maps:get(aux_state, V, undefined))
    };
marshal({new_event_change, EventVersion, Schema, Context}, V) ->
    % It is expected that schema doesn't want to save anything in the context here.
    {Event, Context} = marshal({schema, Schema, {event, EventVersion}, Context}, V),
    #mg_stateproc_Content{
        data = Event,
        format_version = EventVersion
    };
marshal({aux_state_change, AuxStateVersion, Schema, Context}, V) ->
    % It is expected that schema doesn't want to save anything in the context here.
    {AuxState, Context} = marshal({schema, Schema, {aux_state, AuxStateVersion}, Context}, V),
    #mg_stateproc_Content{
        data = AuxState,
        format_version = AuxStateVersion
    };
marshal(action, V) when is_list(V) ->
    lists:foldl(fun apply_action/2, #mg_stateproc_ComplexAction{}, V);
marshal(action, V) ->
    marshal(action, [V]);
marshal(timer, {timeout, V}) ->
    {timeout, marshal(integer, V)};
marshal(timer, {deadline, V}) ->
    {deadline, marshal(timestamp, V)};
marshal({list, T}, V) when is_list(V) ->
    [marshal(T, E) || E <- V];
marshal(T, V) ->
    machinery_mg_codec:marshal(T, V).

apply_action({set_timer, V}, CA) ->
    CA#mg_stateproc_ComplexAction{
        timer = {set_timer, #mg_stateproc_SetTimerAction{timer = marshal(timer, V)}}
    };
apply_action({set_timer, T, Range}, CA) ->
    CA#mg_stateproc_ComplexAction{
        timer =
            {set_timer, #mg_stateproc_SetTimerAction{
                timer = marshal(timer, T),
                range = marshal(range, Range)
            }}
    };
apply_action({set_timer, T, Range, HandlingTimeout}, CA) ->
    CA#mg_stateproc_ComplexAction{
        timer =
            {set_timer, #mg_stateproc_SetTimerAction{
                timer = marshal(timer, T),
                range = marshal(range, Range),
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

unmarshal(
    {machine, Schema},
    #mg_stateproc_Machine{
        'ns' = NS,
        'id' = ID,
        'history' = History,
        'history_range' = Range,
        'aux_state' = #mg_stateproc_Content{format_version = Version, data = AuxState}
    }
) ->
    ID1 = unmarshal(id, ID),
    NS1 = unmarshal(namespace, NS),
    Context0 = build_schema_context(NS1, ID1),
    {AuxState1, Context1} = unmarshal({schema, Schema, {aux_state, Version}, Context0}, AuxState),
    Machine = #{
        ns => ID1,
        id => NS1,
        history => unmarshal({history, Schema, Context1}, History),
        range => unmarshal(range, Range),
        aux_state => AuxState1
    },
    {Machine, Context1};
unmarshal({history, Schema, Context}, V) ->
    unmarshal({list, {event, Schema, Context}}, V);
unmarshal(
    {event, Schema, Context0},
    #mg_stateproc_Event{
        'id' = EventID,
        'created_at' = CreatedAt0,
        'format_version' = Version,
        'data' = Payload0
    }
) ->
    CreatedAt1 = unmarshal(timestamp, CreatedAt0),
    Context1 = Context0#{created_at => CreatedAt1},
    % It is expected that schema doesn't want to save anything in the context here.
    {Payload1, Context1} = unmarshal({schema, Schema, {event, Version}, Context1}, Payload0),
    {
        unmarshal(event_id, EventID),
        CreatedAt1,
        Payload1
    };
unmarshal({signal, Schema, Context0}, {init, #mg_stateproc_InitSignal{arg = Args0}}) ->
    {Args1, Context1} = unmarshal({schema, Schema, {args, init}, Context0}, Args0),
    {{init, Args1}, Context1};
unmarshal({signal, _Schema, Context}, {timeout, #mg_stateproc_TimeoutSignal{}}) ->
    {timeout, Context};
unmarshal({signal, Schema, Context0}, {repair, #mg_stateproc_RepairSignal{arg = Args0}}) ->
    {Args1, Context1} = unmarshal({schema, Schema, {args, repair}, Context0}, Args0),
    {{repair, Args1}, Context1};
unmarshal({list, T}, V) when is_list(V) ->
    [unmarshal(T, E) || E <- V];
unmarshal(T, V) ->
    machinery_mg_codec:unmarshal(T, V).
