%%%
%%% Machine API abstraction.
%%% Behaviour and API.
%%%

%% TODO
%%
%%  - What if we make `start` idempotent and argsless in the `mg_api`.
%%  - Storage schema behaviour

-module(mg_api).

-type namespace()     :: atom().
-type id()            :: binary().
-type args(T)         :: T.
-type response(T)     :: T.

-type usec_part()     :: 0..999999.
-type timestamp()     :: {calendar:datetime(), usec_part()}.

-type event_id()      :: integer().
-type event_body(T)   :: T.
-type event(T)        :: {event_id(), timestamp(), event_body(T)}.
-type history(T)      :: [event(T)].

-type event_cursor()  :: undefined | event_id().
-type limit()         :: undefined | pos_integer().
-type direction()     :: forward | backward.
-type scope()         :: {event_cursor(), limit(), direction()}.

-type machine(T)      :: #{
    namespace         := namespace(),
    id                := id(),
    history           := history(T)
    %% TODO
    %% aux_state should be here as well
    %% history_range ?
    %% timer ?
}.

-export_type([namespace/0]).
-export_type([id/0]).
-export_type([scope/0]).
-export_type([args/1]).
-export_type([response/1]).
-export_type([machine/1]).

-type mod_opts(O) :: module() | {module(), O}.

%% handler
-type handler_args(T)   :: args(T).  %% provided to logic handler from handler server spec
-type handler_opts(T)   :: T.  %% provided to logic handler from mg api backend
-type handler_config(T) :: T.  %% configuration for handler server  spec
-type handler(A, C)     :: {mod_opts(handler_args(A)), handler_config(C)}. %% handler server spec

%% client
-type backend_opts(T)   :: T.  %% opts for backend client
-type backend(O)        :: mod_opts(backend_opts(O)). %% client backend

-export_type([mod_opts/1]).
-export_type([handler_opts/1]).
-export_type([handler_args/1]).
-export_type([handler_config/1]).
-export_type([handler/2]).
-export_type([backend_opts/1]).
-export_type([backend/1]).


%% API

-export([start/4]).
-export([call/4]).
-export([call/5]).
-export([get/3]).
-export([get/4]).

%% Behaviour
-type seconds() :: pos_integer().
-type timer()   ::
    {timeout, seconds()} |
    {deadline, timestamp()}.

-type result(T) :: #{
    events => [event_body(T)],
    action => action() | [action()]
}.

-type action() ::
    {set_timer, timer()} |
    unset_timer          |
    continue             |
    remove.

-export_type([result/1]).
-export_type([action/0]).

-callback init(args(_), machine(T), handler_args(_), handler_opts(_)) ->
    result(T).

-callback process_timeout(machine(T), handler_args(_), handler_opts(_)) ->
    result(T).

-callback process_call(args(_), machine(T), handler_args(_), handler_opts(_)) ->
    {response(_), result(T)}.

%% API

-spec start(namespace(), id(), args(_), backend(_)) ->
    ok | {error, exists}.

start(NS, ID, Args, Backend) ->
    {Module, Opts} = mg_api_utils:get_backend(Backend),
    mg_api_backend:start(Module, NS, ID, Args, Opts).

-spec call(namespace(), id(), args(_), backend(_)) ->
    {ok, response(_)} | {error, notfound}.

call(NS, ID, Args, Backend) ->
    call(NS, ID, {undefined, undefined, forward}, Args, Backend).

-spec call(namespace(), id(), scope(), args(_), backend(_)) ->
    {ok, response(_)} | {error, notfound}.

call(NS, ID, Scope, Args, Backend) ->
    {Module, Opts} = mg_api_utils:get_backend(Backend),
    mg_api_backend:call(Module, NS, ID, Scope, Args, Opts).

-spec get(namespace(), id(), backend(_)) ->
    {ok, machine(_)} | {error, notfound}.

get(NS, ID, Backend) ->
    get(NS, ID, {undefined, undefined, forward}, Backend).

-spec get(namespace(), id(), scope(), backend(_)) ->
    {ok, machine(_)} | {error, notfound}.

get(NS, ID, Scope, Backend) ->
    {Module, Opts} = mg_api_utils:get_backend(Backend),
    mg_api_backend:get(Module, NS, ID, Scope, Opts).
