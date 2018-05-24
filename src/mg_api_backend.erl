%%%
%%% Supervisor for machinegun state processor handlers.

-module(mg_api_backend).

%% API

-export([start/5]).
-export([call/6]).
-export([get/5]).

%% Behaviour

-callback start(mg_api:namespace(), mg_api:id(), mg_api:args(_), mg_api:backend_opts(_)) ->
    ok | {error, exists}.

-callback call(mg_api:namespace(), mg_api:id(), mg_api:scope(), mg_api:args(_), mg_api:backend_opts(_)) ->
    {ok, mg_api:response(_)} | {error, notfound}.

-callback get(mg_api:namespace(), mg_api:id(), mg_api:scope(), mg_api:backend_opts(_)) ->
    {ok, mg_api:machine(_)} | {error, notfound}.

%% API

-type backend() :: module().

-spec start(backend(), mg_api:namespace(), mg_api:id(), mg_api:args(_), mg_api:backend_opts(_)) ->
    ok | {error, exists}.

start(Backend, Namespace, Id, Args, Opts) ->
    Backend:start(Namespace, Id, Args, Opts).

-spec call(backend(), mg_api:namespace(), mg_api:id(), mg_api:scope(), mg_api:args(_), mg_api:backend_opts(_)) ->
    {ok, mg_api:response(_)} | {error, notfound}.

call(Backend, Namespace, Id, Scope, Args, Opts) ->
    Backend:call(Namespace, Id, Scope, Args, Opts).

-spec get(backend(), mg_api:namespace(), mg_api:id(), mg_api:scope(), mg_api:backend_opts(_)) ->
    {ok, mg_api:machine(_)} | {error, notfound}.

get(Backend, Namespace, Id, Scope, Opts) ->
    Backend:get(Namespace, Id, Scope, Opts).
