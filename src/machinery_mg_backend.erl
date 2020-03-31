%%%
%%% Machinery machinegun backend

-module(machinery_mg_backend).
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").

-type namespace()       :: machinery:namespace().
-type ref()             :: machinery:ref().
-type id()              :: machinery:id().
-type range()           :: machinery:range().
-type args(T)           :: machinery:args(T).
-type response(T)       :: machinery:response(T).
-type machine(E, A)     :: machinery:machine(E, A).
-type logic_handler(T)  :: machinery:logic_handler(T).

-define(BACKEND_CORE_OPTS,
    schema          := machinery_mg_schema:schema(),
    marshaller      := machinery_backend_marshaller:backend()
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
new(WoodyCtx, Opts = #{client := _, schema := _, marshaller := _}) ->
    {?MODULE, Opts#{woody_ctx => WoodyCtx}}.

%% Machinery backend

-spec start(namespace(), id(), args(_), backend_opts()) ->
    ok | {error, exists}.
start(NS, ID, Args, Opts) ->
    Client = get_client(Opts),
    Schema = get_schema(Opts),
    Marshaller = get_marshaller(Opts),
    MarshaledArgs = machinery_backend_marshaller:marshal(Marshaller, {schema, Schema, {args, init}}, Args),
    MarshaledNS = machinery_backend_marshaller:marshal(Marshaller, namespace, NS),
    MarshaledID = machinery_backend_marshaller:marshal(Marshaller, id, ID),
    case machinery_mg_client:start(MarshaledNS, MarshaledID, MarshaledArgs, Client) of
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
    Marshaller = get_marshaller(Opts),
    Descriptor = {NS, Ref, Range},
    MarshaledArgs = machinery_backend_marshaller:marshal(Marshaller, {schema, Schema, {args, call}}, Args),
    MarshaledDesc = machinery_backend_marshaller:marshal(Marshaller, descriptor, Descriptor),
    case machinery_mg_client:call(MarshaledDesc, MarshaledArgs, Client) of
        {ok, Response} ->
            {ok, machinery_backend_marshaller:unmarshal(Marshaller, {schema, Schema, response}, Response)};
        {exception, #mg_stateproc_MachineNotFound{}} ->
            {error, notfound};
        {exception, #mg_stateproc_NamespaceNotFound{}} ->
            error({namespace_not_found, NS});
        {exception, #mg_stateproc_MachineFailed{}} ->
            error({failed, NS, Ref})
    end.

-spec repair(namespace(), ref(), range(), args(_), backend_opts()) ->
    ok | {error, notfound | working}.
repair(NS, Ref, Range, Args, Opts) ->
    Client = get_client(Opts),
    Schema = get_schema(Opts),
    Marshaller = get_marshaller(Opts),
    Descriptor = {NS, Ref, Range},
    MarshaledArgs = machinery_backend_marshaller:marshal(Marshaller, {schema, Schema, {args, repair}}, Args),
    MarshaledDesc = machinery_backend_marshaller:marshal(Marshaller, descriptor, Descriptor),
    case machinery_mg_client:repair(MarshaledDesc, MarshaledArgs, Client) of
        {ok, ok} ->
            ok;
        {exception, #mg_stateproc_MachineNotFound{}} ->
            {error, notfound};
        {exception, #mg_stateproc_MachineAlreadyWorking{}} ->
            {error, working};
        {exception, #mg_stateproc_NamespaceNotFound{}} ->
            error({namespace_not_found, NS})
    end.

-spec get(namespace(), ref(), range(), backend_opts()) ->
    {ok, machine(_, _)} | {error, notfound}.
get(NS, Ref, Range, Opts) ->
    Client = get_client(Opts),
    Schema = get_schema(Opts),
    Marshaller = get_marshaller(Opts),
    Descriptor = {NS, Ref, Range},
    MarshaledDesc = machinery_backend_marshaller:marshal(Marshaller, descriptor, Descriptor),
    case machinery_mg_client:get_machine(MarshaledDesc, Client) of
        {ok, Machine} ->
            {ok, machinery_backend_marshaller:unmarshal(Marshaller, {machine, Schema}, Machine)};
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
    #{handler := Handler, schema := Schema, marshaller := Marshaller}
) ->
    Machine1 = machinery_backend_marshaller:unmarshal(Marshaller, {machine, Schema}, Machine),
    Result   = dispatch_signal(
        machinery_backend_marshaller:unmarshal(Marshaller, {signal, Schema}, Signal),
        Machine1,
        machinery_utils:get_handler(Handler),
        get_handler_opts(WoodyCtx)
    ),
    {ok, machinery_backend_marshaller:marshal(Marshaller, {signal_result, Schema}, handle_result(Result, Machine1))};
handle_function(
    'ProcessCall',
    [#mg_stateproc_CallArgs{arg = Args, machine = Machine}],
    WoodyCtx,
    #{handler := Handler, schema := Schema, marshaller := Marshaller}
) ->
    Machine1 = machinery_backend_marshaller:unmarshal(Marshaller, {machine, Schema}, Machine),
    {Response, Result} = dispatch_call(
        machinery_backend_marshaller:unmarshal(Marshaller, {schema, Schema, {args, call}}, Args),
        Machine1,
        machinery_utils:get_handler(Handler),
        get_handler_opts(WoodyCtx)
    ),
    {ok, machinery_backend_marshaller:marshal(
        Marshaller,
        {call_result, Schema},
        {Response, handle_result(Result, Machine1
    )})}.

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

get_marshaller(#{marshaller := Marshaller}) ->
    Marshaller.
