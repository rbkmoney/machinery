%%%
%%% Modernizer API abstraction.
%%% Behaviour and API.
%%%

-module(machinery_modernizer).

% API
-type namespace() :: machinery:namespace().
-type ref()       :: machinery:ref().
-type range()     :: machinery:range().
-type backend()   :: machinery:backend(_).

-export([modernize/3]).
-export([modernize/4]).

%% API

-spec modernize(namespace(), ref(), backend()) ->
    ok| {error, notfound}.
modernize(NS, Ref, Backend) ->
    modernize(NS, Ref, {undefined, undefined, forward}, Backend).

-spec modernize(namespace(), ref(), range(), backend()) ->
    ok | {error, notfound}.
modernize(NS, Ref, Range, Backend) ->
    {Module, Opts} = machinery_utils:get_backend(Backend),
    machinery_modernizer_backend:modernize(Module, NS, Ref, Range, Opts).
