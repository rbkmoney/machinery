-module(machinery_machine_unique_tag_mg_example).

%% API

-export([child_spec/1]).
-export([tag/4]).
-export([tag_until/5]).
-export([untag/4]).
-export([get/3]).

-type id()         :: machinery:id().
-type namespace()  :: machinery:namespace().
-type tag()        :: machinery_machine_unique_tag:tag().
-type timer()      :: machinery:timer().

-type opts()       :: #{
    woody_ctx := woody_context:ctx()
}.
-export_type([opts/0]).

-define(LOGIC_HANDLER, machinery_machine_unique_tag).

%% API
-spec child_spec(_Id) ->
    supervisor:child_spec().
child_spec(Id) ->
    machinery_utils:woody_child_spec(Id, get_routes(), config(server_opts)).

-spec tag(namespace(), tag(), id(), opts()) ->
    ok | {error, {set, id()}}.
tag(NS, Tag, ID, Opts) ->
    machinery_machine_unique_tag:tag(NS, Tag, ID, get_backend(Opts)).

-spec tag_until(namespace(), tag(), id(), timer(), opts()) ->
    ok | {error, {set, id()}}.
tag_until(NS, Tag, ID, Timer, Opts) ->
    machinery_machine_unique_tag:tag_until(NS, Tag, ID, Timer, get_backend(Opts)).

-spec untag(namespace(), tag(), id(), opts()) ->
    ok | {error, {set, id()}}.
untag(NS, Tag, ID, Opts) ->
    machinery_machine_unique_tag:untag(NS, Tag, ID, get_backend(Opts)).

-spec get(namespace(), tag(), opts()) ->
    {ok, id()} | {error, unset}.
get(NS, Tag, Opts) ->
    machinery_machine_unique_tag:get(NS, Tag, get_backend(Opts)).

%%% Internal functions

%% handler
-spec get_routes() ->
    machinery_utils:woody_routes().
get_routes() ->
    machinery_mg_backend:get_routes(
        [get_handler(config(handler_path))],
        maps:with([event_handler, handler_limits], config())
    ).

get_handler(Path) ->
    {?LOGIC_HANDLER, get_handler_config(Path)}.

get_handler_config(Path) ->
    #{path => Path, backend_config => get_backend_config()}.

get_backend_config() ->
    maps:with([schema, marshaller], config()).

%% client
-spec get_backend(opts()) ->
    machinery_mg_backend:backend().
get_backend(#{woody_ctx := WoodyCtx}) ->
    machinery_mg_backend:new(WoodyCtx, #{
        client     => get_woody_client(),
        schema     => config(schema),
        marshaller => config(marshaller)
    }).

-spec get_woody_client() ->
    machinery_mg_client:woody_client().
get_woody_client() ->
    maps:with([url, event_handler], config()).

%% config
config(Key) ->
    maps:get(Key, config()).

config() ->
    #{
        schema        => machinery_mg_schema_generic,
        marshaller    => machinery_mg_backend_marshaller,
        url           => <<"http://machinegun:8022/v1/automaton">>,
        handler_path  => <<"/v1/stateproc">>,
        event_handler => woody_event_handler_default,
        server_opts   => #{
            ip    => {0, 0, 0, 0},
            port  => 8022
        }
    }.
