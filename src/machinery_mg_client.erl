%%%
%%% Simplistic machinegun client.

-module(machinery_mg_client).

-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").

%% API

-export([new/2]).

-export([start/4]).
-export([call/3]).
-export([repair/3]).
-export([get_machine/2]).
-export([modernize/2]).

-type woody_client() :: #{
    url := woody:url(),
    event_handler := woody:ev_handler(),
    transport_opts => woody_client_thrift_http_transport:transport_options()
}.

-opaque client() :: {woody_client(), woody_context:ctx()}.

-export_type([woody_client/0]).
-export_type([client/0]).

%%

-spec new(woody_client(), woody_context:ctx()) -> client().
new(WoodyClient = #{url := _, event_handler := _}, WoodyCtx) ->
    {WoodyClient, WoodyCtx}.

%%

-type namespace() :: mg_proto_base_thrift:'Namespace'().
-type id() :: mg_proto_base_thrift:'ID'().
-type args() :: mg_proto_state_processing_thrift:'Args'().
-type descriptor() :: mg_proto_state_processing_thrift:'MachineDescriptor'().
-type call_response() :: mg_proto_state_processing_thrift:'CallResponse'().
-type repair_response() :: mg_proto_state_processing_thrift:'RepairResponse'().
-type machine() :: mg_proto_state_processing_thrift:'Machine'().
-type namespace_not_found() :: mg_proto_state_processing_thrift:'NamespaceNotFound'().
-type machine_not_found() :: mg_proto_state_processing_thrift:'MachineNotFound'().
-type machine_already_exists() :: mg_proto_state_processing_thrift:'MachineAlreadyExists'().
-type machine_already_working() :: mg_proto_state_processing_thrift:'MachineAlreadyWorking'().
-type machine_failed() :: mg_proto_state_processing_thrift:'MachineFailed'().
-type repair_failed() :: mg_proto_state_processing_thrift:'RepairFailed'().

-type start_errors() ::
    namespace_not_found()
    | machine_already_exists()
    | machine_failed().

-type call_errors() ::
    namespace_not_found()
    | machine_not_found()
    | machine_failed().

-type repair_errors() ::
    namespace_not_found()
    | machine_not_found()
    | machine_already_working()
    | machine_failed()
    | repair_failed().

-type get_errors() ::
    namespace_not_found()
    | machine_not_found().

-type modernize_errors() ::
    namespace_not_found()
    | machine_not_found().

-spec start(namespace(), id(), args(), client()) ->
    {ok, ok}
    | {exception, start_errors()}.
start(NS, ID, Args, Client) ->
    issue_call('Start', [NS, ID, Args], Client).

-spec call(descriptor(), args(), client()) ->
    {ok, call_response()}
    | {exception, call_errors()}.
call(Descriptor, Args, Client) ->
    issue_call('Call', [Descriptor, Args], Client).

-spec repair(descriptor(), args(), client()) ->
    {ok, repair_response()}
    | {exception, repair_errors()}.
repair(Descriptor, Args, Client) ->
    issue_call('Repair', [Descriptor, Args], Client).

-spec get_machine(descriptor(), client()) ->
    {ok, machine()}
    | {exception, get_errors()}.
get_machine(Descriptor, Client) ->
    issue_call('GetMachine', [Descriptor], Client).

-spec modernize(descriptor(), client()) ->
    {ok, ok}
    | {exception, modernize_errors()}.
modernize(Descriptor, Client) ->
    issue_call('Modernize', [Descriptor], Client).

%% Internal functions

issue_call(Function, Args, {WoodyClient, WoodyCtx}) when is_list(Args) ->
    ArgsTuple = list_to_tuple(Args),
    issue_call(Function, ArgsTuple, {WoodyClient, WoodyCtx});
issue_call(Function, Args, {WoodyClient, WoodyCtx}) ->
    Service = {mg_proto_state_processing_thrift, 'Automaton'},
    Request = {Service, Function, Args},
    woody_client:call(Request, WoodyClient, WoodyCtx).
