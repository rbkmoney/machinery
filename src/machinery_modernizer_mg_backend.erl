%%%
%%% Modernizer machinegun backend
%%%

-module(machinery_modernizer_mg_backend).

-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").

-type namespace() :: machinery:namespace().
-type ref() :: machinery:ref().
-type range() :: machinery:range().
-type logic_handler(T) :: machinery:logic_handler(T).

-define(BACKEND_CORE_OPTS,
    schema := machinery_mg_schema:schema()
).

%% Server types
-type backend_config() :: #{
    ?BACKEND_CORE_OPTS
}.

-type handler_config() :: #{
    path := woody:path(),
    backend_config := backend_config()
}.

%% handler server spec
-type handler() :: handler_config().

-type handler_opts() ::
    machinery:handler_opts(#{
        woody_ctx := woody_context:ctx()
    }).

-type backend_handler_opts() :: #{
    ?BACKEND_CORE_OPTS
}.

%% Client types
-type backend_opts() ::
    machinery:backend_opts(#{
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
-export_type([handler/0]).
-export_type([handler_opts/0]).
-export_type([backend_opts/0]).
-export_type([backend/0]).

%% API
-export([new/2]).
-export([get_routes/2]).
-export([get_handler/2]).

%% Machinery backend
-behaviour(machinery_modernizer_backend).

-export([modernize/4]).

%% Woody handler
-behaviour(woody_server_thrift_handler).

-export([handle_function/4]).

%% API

-spec get_routes([handler()], machinery_utils:route_opts()) -> machinery_utils:woody_routes().
get_routes(Handlers, Opts) ->
    machinery_utils:get_woody_routes(Handlers, fun get_handler/2, Opts).

-spec get_handler(handler(), machinery_utils:route_opts()) -> machinery_utils:woody_handler().
get_handler(#{path := Path, backend_config := Config}, _) ->
    {Path, {
        {mg_proto_state_processing_thrift, 'Modernizer'},
        {?MODULE, Config}
    }}.

-spec new(woody_context:ctx(), backend_opts_static()) -> backend().
new(WoodyCtx, Opts = #{client := _, schema := _}) ->
    {?MODULE, Opts#{woody_ctx => WoodyCtx}}.

%% Machinery backend

-spec modernize(namespace(), ref(), range(), backend_opts()) -> ok | {error, notfound}.
modernize(NS, Ref, Range, Opts) ->
    Client = get_client(Opts),
    Descriptor = {NS, Ref, Range},
    case machinery_mg_client:modernize(marshal(descriptor, Descriptor), Client) of
        {ok, ok} ->
            ok;
        {exception, #mg_stateproc_MachineNotFound{}} ->
            {error, notfound};
        {exception, #mg_stateproc_NamespaceNotFound{}} ->
            error({namespace_not_found, NS})
    end.

%% Woody handler

-spec handle_function('ModernizeEvent', woody:args(), woody_context:ctx(), backend_handler_opts()) ->
    {ok, mg_proto_state_processing_thrift:'ModernizeEventResult'()}.
handle_function('ModernizeEvent', {MachineEvent0}, _WoodyCtx, #{schema := Schema}) ->
    {MachineEvent, Context} = unmarshal_machine_event(Schema, MachineEvent0),
    TargetVersion = machinery_mg_schema:get_version(Schema, event),
    EventPayload = marshal_event_content(Schema, TargetVersion, Context, MachineEvent),
    {ok, marshal(modernize_event_result, EventPayload)}.

%% Utils

unmarshal_machine_event(Schema, #mg_stateproc_MachineEvent{
    ns = MachineNS,
    id = MachineID,
    event = Event
}) ->
    ID = unmarshal(id, MachineID),
    NS = unmarshal(namespace, MachineNS),
    Context = build_schema_context(NS, ID),
    {unmarshal({event, Schema, Context}, Event), Context}.

marshal_event_content(Schema, Version, Context0, _Event = #{data := EventData0}) ->
    {EventData1, Context0} = marshal({schema, Schema, {event, Version}, Context0}, EventData0),
    #mg_stateproc_Content{
        format_version = maybe_marshal(format_version, Version),
        data = EventData1
    }.

get_client(#{client := Client, woody_ctx := WoodyCtx}) ->
    machinery_mg_client:new(Client, WoodyCtx).

build_schema_context(NS, Ref) ->
    #{
        machine_ns => NS,
        machine_ref => Ref
    }.

%% Marshalling

marshal(modernize_event_result, Content) ->
    #mg_stateproc_ModernizeEventResult{
        event_payload = Content
    };
marshal(format_version, V) ->
    marshal(integer, V);
marshal(T, V) ->
    machinery_mg_codec:marshal(T, V).

%% Unmarshalling

unmarshal({event, Schema, Context0}, #mg_stateproc_Event{
    id = EventID,
    created_at = CreatedAt0,
    format_version = Version,
    data = EventData0
}) ->
    CreatedAt1 = unmarshal(timestamp, CreatedAt0),
    Context1 = Context0#{created_at => CreatedAt1},
    {EventData1, Context1} = unmarshal({schema, Schema, {event, Version}, Context1}, EventData0),
    #{
        id => unmarshal(event_id, EventID),
        created_at => CreatedAt1,
        format_version => maybe_unmarshal(format_version, Version),
        data => EventData1
    };
unmarshal(format_version, V) ->
    unmarshal(integer, V);
unmarshal(T, V) ->
    machinery_mg_codec:unmarshal(T, V).

maybe_unmarshal(_Type, undefined) ->
    undefined;
maybe_unmarshal(Type, Value) ->
    unmarshal(Type, Value).

maybe_marshal(_Type, undefined) ->
    undefined;
maybe_marshal(Type, Value) ->
    marshal(Type, Value).
