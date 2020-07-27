%%%
%%% Machinery backend behaviour.

-module(machinery_backend).

%% API
-export([start/5]).
-export([call/6]).
-export([repair/6]).
-export([get/5]).

%% Behaviour definition

-type namespace()    :: machinery:namespace().
-type id()           :: machinery:id().
-type ref()          :: machinery:ref().
-type range()        :: machinery:range().
-type args()         :: machinery:args(_).
-type backend_opts() :: machinery:backend_opts(_).

-callback start(namespace(), id(), args(), backend_opts()) ->
    ok | {error, exists}.

-callback call(namespace(), id(), range(), args(), backend_opts()) ->
    {ok, machinery:response(_)} | {error, notfound}.

-callback repair(namespace(), id(), range(), args(), backend_opts()) ->
    {ok, machinery:response(_)} | {error, {failed, machinery:error(_)} | notfound | working}.

-callback get(namespace(), id(), range(), backend_opts()) ->
    {ok, machinery:machine(_, _)} | {error, notfound}.

%% API

-type backend() :: module().

-spec start(backend(), namespace(), id(), args(), backend_opts()) ->
    ok | {error, exists}.
start(Backend, Namespace, Id, Args, Opts) ->
    Backend:start(Namespace, Id, Args, Opts).

-spec call(backend(), namespace(), ref(), range(), args(), backend_opts()) ->
    {ok, machinery:response(_)} | {error, notfound}.
call(Backend, Namespace, Ref, Range, Args, Opts) ->
    Backend:call(Namespace, Ref, Range, Args, Opts).

-spec repair(backend(), namespace(), ref(), range(), args(), backend_opts()) ->
    {ok, machinery:response(_)} | {error, {failed, machinery:error(_)} | notfound | working}.
repair(Backend, Namespace, Ref, Range, Args, Opts) ->
    Backend:repair(Namespace, Ref, Range, Args, Opts).

-spec get(backend(), namespace(), ref(), range(), backend_opts()) ->
    {ok, machinery:machine(_, _)} | {error, notfound}.
get(Backend, Namespace, Ref, Range, Opts) ->
    Backend:get(Namespace, Ref, Range, Opts).
