-module(machinery_utils).

%% Types

-type woody_routes()      :: [woody_server_thrift_http_handler:route(_)].
-type woody_handler()     :: woody:http_handler(woody:th_handler()).
-type handler(T)          :: T.
-type get_woody_handler() :: fun((handler(_), route_opts()) -> woody_handler()).

-type woody_server_config() :: #{
    ip             := inet:ip_address(),
    port           := inet:port_number(),
    net_opts       => cowboy_protocol:opts()
}.

-type route_opts() :: #{
    event_handler  := woody:ev_handler(),
    handler_limits => woody_server_thrift_http_handler:handler_limits()
}.

-export_type([woody_server_config/0]).
-export_type([woody_routes/0]).
-export_type([woody_handler/0]).
-export_type([route_opts/0]).
-export_type([handler/1]).
-export_type([get_woody_handler/0]).

%% API

-export([get_handler/1]).
-export([get_backend/1]).
-export([expand_modopts/2]).
-export([woody_child_spec/3]).
-export([get_woody_routes/3]).

%% API

-spec get_handler(machinery:modopts(Opts)) ->
    {module(), Opts}.

get_handler(Handler) ->
    expand_modopts(Handler, undefined).

-spec get_backend(machinery:backend(Opts)) ->
    {module(), Opts}.

get_backend(Backend) ->
    expand_modopts(Backend, #{}).

-spec expand_modopts(machinery:modopts(Opts), Opts) ->
    {module(), Opts}.

expand_modopts({Mod, Opts}, _) ->
    {Mod, Opts};
expand_modopts(Mod, Opts) ->
    {Mod, Opts}.

-spec woody_child_spec(_Id, woody_routes(), woody_server_config()) ->
    supervisor:child_spec().

woody_child_spec(Id, Routes, Config) ->
    woody_server:child_spec(Id, Config#{
        %% ev handler for `handlers`, which is `[]`, so this is just to satisfy the spec.
        event_handler     => woody_event_handler_default,
        handlers          => [],
        additional_routes => Routes
    }).

-spec get_woody_routes([handler(_)], get_woody_handler(), route_opts()) ->
    woody_routes().

get_woody_routes(Handlers, GetHandler, Opts = #{event_handler := _}) ->
    woody_server_thrift_http_handler:get_routes(Opts#{
        handlers => [GetHandler(H, Opts) || H <- Handlers]
    }).
