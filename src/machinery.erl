%%%
%%% Machine API abstraction.
%%% Behaviour and API.
%%%

%% TODO
%%
%%  - What if we make `start` idempotent and argsless in the `machinery`.
%%  - Storage schema behaviour

-module(machinery).

-type namespace()     :: atom().
-type id()            :: binary().
-type tag()           :: {tag, binary()}.
-type args(T)         :: T.
-type response(T)     :: T.

-type usec_part()     :: 0..999999.
-type timestamp()     :: {calendar:datetime(), usec_part()}.

-type event_id()      :: integer().
-type event_body(T)   :: T.
-type event(T)        :: {event_id(), timestamp(), event_body(T)}.
-type history(T)      :: [event(T)].
-type aux_state(T)    :: T.

-type event_cursor()  :: undefined | event_id().
-type limit()         :: undefined | non_neg_integer().
-type direction()     :: forward | backward.
-type range()         :: {event_cursor(), limit(), direction()}.
-type signal(T)       :: {init, args(T)} | {repair, args(T)} | timeout.
-type machine(E, A)   :: #{
    namespace         := namespace(),
    id                := id(),
    history           := history(E),
    aux_state         := aux_state(A)
    %% TODO
    %% history_range ?
    %% timer ?
}.

-export_type([namespace/0]).
-export_type([id/0]).
-export_type([tag/0]).
-export_type([range/0]).
-export_type([args/1]).
-export_type([response/1]).
-export_type([machine/2]).
-export_type([event/1]).

-type modopts(O) :: module() | {module(), O}.

%% handler
-type handler_opts(T)   :: T.                         %% provided to logic handler from  machinery backend
-type handler_args(T)   :: args(T).                   %% provided to logic handler from handler server spec
-type logic_handler(A)  :: modopts(handler_args(A)).

%% client
-type backend_opts(T)   :: T.                        %% opts for client backend
-type backend(O)        :: modopts(backend_opts(O)). %% client backend

-export_type([modopts/1]).
-export_type([handler_opts/1]).
-export_type([handler_args/1]).
-export_type([logic_handler/1]).
-export_type([backend_opts/1]).
-export_type([backend/1]).

%% API
-export([start/4]).
-export([call/4]).
-export([call/5]).
-export([repair/4]).
-export([repair/5]).
-export([get/3]).
-export([get/4]).

%% Internal API
-export([dispatch_signal/4]).
-export([dispatch_call/4]).

%% Behaviour definition
-type seconds() :: non_neg_integer().
-type timer()   ::
    {timeout, seconds()} |
    {deadline, timestamp()}.

-type result(E, A) :: #{
    events    => [event_body(E)],
    action    => action() | [action()],
    aux_state => aux_state(A)
}.

-type action() ::
    {set_timer, timer()}                     |
    {set_timer, timer(), range()}            |
    {set_timer, timer(), range(), seconds()} |
    {tag, binary()}                          |
    unset_timer                              |
    continue                                 |
    remove.

-export_type([timer/0]).
-export_type([timestamp/0]).
-export_type([seconds/0]).
-export_type([result/2]).
-export_type([action/0]).

-callback init(args(_), machine(E, A), handler_args(_), handler_opts(_)) ->
    result(E, A).

-callback process_repair(args(_), machine(E, A), handler_args(_), handler_opts(_)) ->
    result(E, A).

-callback process_timeout(machine(E, A), handler_args(_), handler_opts(_)) ->
    result(E, A).

-callback process_call(args(_), machine(E, A), handler_args(_), handler_opts(_)) ->
    {response(_), result(E, A)}.

%% API

-spec start(namespace(), id(), args(_), backend(_)) ->
    ok | {error, exists}.
start(NS, ID, Args, Backend) ->
    {Module, Opts} = machinery_utils:get_backend(Backend),
    machinery_backend:start(Module, NS, ID, Args, Opts).

-spec call(namespace(), id() | tag(), args(_), backend(_)) ->
    {ok, response(_)} | {error, notfound}.
call(NS, IDorTag, Args, Backend) ->
    call(NS, IDorTag, {undefined, undefined, forward}, Args, Backend).

-spec call(namespace(), id() | tag(), range(), args(_), backend(_)) ->
    {ok, response(_)} | {error, notfound}.
call(NS, IDorTag, Range, Args, Backend) ->
    {Module, Opts} = machinery_utils:get_backend(Backend),
    machinery_backend:call(Module, NS, IDorTag, Range, Args, Opts).

-spec repair(namespace(), id() | tag(), args(_), backend(_)) ->
    ok | {error, notfound | working}.
repair(NS, IDorTag, Args, Backend) ->
    repair(NS, IDorTag, {undefined, undefined, forward}, Args, Backend).

-spec repair(namespace(), id() | tag(), range(), args(_), backend(_)) ->
    ok | {error, notfound | working}.
repair(NS, IDorTag, Range, Args, Backend) ->
    {Module, Opts} = machinery_utils:get_backend(Backend),
    machinery_backend:repair(Module, NS, IDorTag, Range, Args, Opts).

-spec get(namespace(), id() | tag(), backend(_)) ->
    {ok, machine(_, _)} | {error, notfound}.
get(NS, IDorTag, Backend) ->
    get(NS, IDorTag, {undefined, undefined, forward}, Backend).

-spec get(namespace(), id() | tag(), range(), backend(_)) ->
    {ok, machine(_, _)} | {error, notfound}.
get(NS, IDorTag, Range, Backend) ->
    {Module, Opts} = machinery_utils:get_backend(Backend),
    machinery_backend:get(Module, NS, IDorTag, Range, Opts).

%% Internal API

-spec dispatch_signal(signal(_), machine(E, A), logic_handler(_), handler_opts(_)) ->
    result(E, A).
dispatch_signal({init, Args}, Machine, {Handler, HandlerArgs}, Opts) ->
    Handler:init(Args, Machine, HandlerArgs, Opts);
dispatch_signal({repair, Args}, Machine, {Handler, HandlerArgs}, Opts) ->
    Handler:process_repair(Args, Machine, HandlerArgs, Opts);
dispatch_signal(timeout, Machine, {Handler, HandlerArgs}, Opts) ->
    Handler:process_timeout(Machine, HandlerArgs, Opts).

-spec dispatch_call(args(_), machine(E, A), logic_handler(_), handler_opts(_)) ->
    {response(_), result(E, A)}.
dispatch_call(Args, Machine, {Handler, HandlerArgs}, Opts) ->
    Handler:process_call(Args, Machine, HandlerArgs, Opts).
