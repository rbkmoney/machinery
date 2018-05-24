-module(mg_api_utils).

-type woody_server_config() :: #{
    ip             := inet:ip_address(),
    port           := inet:port_number(),
    net_opts       => cowboy_protocol:opts()
}.
-export_type([woody_server_config/0]).

-type woody_routes() :: [woody_server_thrift_http_handler:route(_)].
-export_type([woody_routes/0]).

-type route_opts() :: #{
    event_handler  := woody:ev_handler(),
    handler_limits => woody_server_thrift_http_handler:handler_limits()
}.

-export_type([route_opts/0]).

-type woody_handler()     :: woody:http_handler(woody:th_handler()).
-type get_woody_handler() :: fun((mg_api:handler(_, _), route_opts()) -> woody_handler()).

-export_type([woody_handler/0]).
-export_type([get_woody_handler/0]).


-export([get_handler/1]).
-export([get_backend/1]).
-export([woody_child_spec/3]).
-export([get_woody_routes/3]).

-spec get_handler(mg_api:handler(_, _)) ->
    mg_api:handler(_, _).

get_handler(Handler) ->
    expand_modopts(Handler, undefined).

-spec get_backend(mg_api:backend(_)) ->
    mg_api:backend(_).

get_backend(Backend) ->
    expand_modopts(Backend, #{}).

-spec expand_modopts(mg_api:mod_opts(Opts), AltOpts) ->
    {module(), Opts | AltOpts}.

expand_modopts({Mod, Opts}, _) ->
    {Mod, Opts};
expand_modopts(Mod, Opts) ->
    {Mod, Opts}.

-spec woody_child_spec(_Id, woody_routes(), woody_server_config()) ->
    supervisor:child_spec().

woody_child_spec(Id, Routes, Config) ->
    woody_server:child_spec(Id, Config#{
        event_handler     => woody_event_handler_default,
        handlers          => [],
        additional_routes => Routes
    }).

-spec get_woody_routes([mg_api:handler(_, _)], get_woody_handler(), route_opts()) ->
    woody_routes().

get_woody_routes(Handlers, GetHandler, Opts = #{event_handler := _}) ->
    woody_server_thrift_http_handler:get_routes(Opts#{
        handlers => [GetHandler(H, Opts) || H <- Handlers]
    }).

