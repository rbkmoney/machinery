-module(mg_api_machine_unique_tag_mg_example).

%% API

-export([child_spec/1]).
-export([tag/4]).
-export([untag/4]).
-export([get/3]).

-type id()         :: mg_api:id().
-type namespace()  :: mg_api:namespace().
-type tag()        :: mg_api_machine_unique_tag:tag().

-type opts()       :: #{
    woody_ctx := woody_context:ctx()
}.
-export_type([opts/0]).

-define(LOGIC_HANDLER, mg_api_machine_unique_tag).

%% API
-spec child_spec(_Id) ->
    supervisor:child_spec().
child_spec(Id) ->
    mg_api_utils:woody_child_spec(Id, get_routes(), config(net_opts)).

-spec tag(namespace(), tag(), id(), opts()) ->
    ok | {error, {set, id()}}.
tag(NS, Tag, ID, Opts) ->
    mg_api_machine_unique_tag:tag(NS, Tag, ID, get_backend(Opts)).

-spec untag(namespace(), tag(), id(), opts()) ->
    ok | {error, {set, id()}}.
untag(NS, Tag, ID, Opts) ->
    mg_api_machine_unique_tag:untag(NS, Tag, ID, get_backend(Opts)).

-spec get(namespace(), tag(), opts()) ->
    {ok, id()} | {error, unset}.
get(NS, Tag, Opts) ->
    mg_api_machine_unique_tag:get(NS, Tag, get_backend(Opts)).

%%% Internal functions

%% handler
get_routes() ->
    mg_api_mg_backend:get_routes(
        [get_handler(config(handler_path))],
        maps:with([event_handler, handler_limits], config())
    ).

get_handler(Path) ->
    {?LOGIC_HANDLER, get_handler_config(Path)}.

get_handler_config(Path) ->
    #{path => Path, backend_config => get_backend_config()}.

get_backend_config() ->
    maps:with([schema], config()).

%% client
get_backend(Opts) ->
    {config(backend), get_backend_opts(Opts)}.

get_backend_opts(#{woody_ctx := WoodyCtx}) ->
    #{
        woody_ctx => WoodyCtx,
        client    => get_woody_client(),
        schema    => config(schema)
    }.

get_woody_client() ->
    maps:with([url, event_handler], config()).

%% config
config(Key) ->
    maps:get(Key, config()).

config() ->
    #{
        backend       => mg_api_mg_backend,
        schema        => mg_api_mg_schema_generic,
        url           => <<"http://machinegun:8022/v1/automaton">>,
        handler_path  => "/v1/stateproc",
        event_handler => woody_event_handler_default,
        net_opts      => #{
            ip    => {0, 0, 0, 0},
            port  => 8022
        }
    }.
