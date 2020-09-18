%%%
%%% Modernizer backend behaviour.
%%%

-module(machinery_modernizer_backend).

%% API
-export([modernize/5]).

%% Behaviour definition

-type namespace()    :: machinery:namespace().
-type id()           :: machinery:id().
-type ref()          :: machinery:ref().
-type range()        :: machinery:range().
-type backend_opts() :: machinery:backend_opts(_).

-callback modernize(namespace(), id(), range(), backend_opts()) ->
    ok | {error, notfound}.

%% API

-type backend() :: module().

-spec modernize(backend(), namespace(), ref(), range(), backend_opts()) ->
    ok | {error, notfound}.
modernize(Backend, Namespace, Ref, Range, Opts) ->
    Backend:modernize(Namespace, Ref, Range, Opts).
