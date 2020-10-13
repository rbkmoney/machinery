-module(machinery_mg_modernizer_flow_SUITE).

-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").

%% Common Tests callbacks
-export([all/0]).
-export([groups/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).
-export([init_per_group/2]).
-export([end_per_group/2]).
-export([init_per_testcase/2]).

%% Tests

-export([modernizer_test/1]).
-export([skip_upgrading_test/1]).

%% Machinery callbacks

-behaviour(machinery).

-export([init/4]).
-export([process_timeout/3]).
-export([process_repair/4]).
-export([process_call/4]).

-behaviour(machinery_mg_schema).

-export([marshal/3]).
-export([unmarshal/3]).
-export([get_version/1]).

%% Internal types

-type config() :: ct_helper:config().
-type test_case_name() :: ct_helper:test_case_name().
-type group_name() :: ct_helper:group_name().
-type test_return() :: _ | no_return().

-spec all() -> [test_case_name() | {group, group_name()}].
all() ->
    [
        {group, machinery_mg_backend}
    ].

-spec groups() -> [{group_name(), list(), test_case_name()}].
groups() ->
    [
        {machinery_mg_backend, [], [{group, all}]},
        {all, [], [
            modernizer_test,
            skip_upgrading_test
        ]}
    ].

-spec init_per_suite(config()) -> config().
init_per_suite(C) ->
    {StartedApps, _StartupCtx} = ct_helper:start_apps([machinery]),
    [{started_apps, StartedApps} | C].

-spec end_per_suite(config()) -> _.
end_per_suite(C) ->
    ok = ct_helper:stop_apps(?config(started_apps, C)),
    ok.

-spec init_per_group(group_name(), config()) -> config().
init_per_group(machinery_mg_backend = Name, C0) ->
    C1 = [
        {backend, Name},
        {modernizer_backend, machinery_modernizer_mg_backend},
        {group_sup, ct_sup:start()}
        | C0
    ],
    {ok, _Pid} = start_backend(C1),
    C1;
init_per_group(_Name, C) ->
    C.

-spec end_per_group(group_name(), config()) -> config().
end_per_group(machinery_mg_backend, C) ->
    ok = ct_sup:stop(?config(group_sup, C)),
    C;
end_per_group(_Name, C) ->
    C.

-spec init_per_testcase(test_case_name(), config()) -> config().
init_per_testcase(TestCaseName, C) ->
    ct_helper:makeup_cfg([ct_helper:test_case_name(TestCaseName), ct_helper:woody_ctx()], C).

%% Tests

-spec modernizer_test(config()) -> test_return().
modernizer_test(C) ->
    _ = setup_test_ets(),
    ID = unique(),
    true = set_event_version(undefined),
    ?assertEqual(ok, start(ID, init_something, C)),
    [{EventID, Timestamp, Event}] = get_history(ID, C),
    true = set_event_version(1),
    ?assertEqual(ok, modernize(ID, C)),
    ?assertEqual([{EventID, Timestamp, {v1, Event}}], get_history(ID, C)),
    _ = delete_test_ets().

-spec skip_upgrading_test(config()) -> test_return().
skip_upgrading_test(C) ->
    _ = setup_test_ets(),
    ID = unique(),
    true = set_event_version(1),
    ?assertEqual(ok, start(ID, init_something, C)),
    [{EventID, Timestamp, {v1, Event}}] = get_history(ID, C),
    true = set_event_version(2),
    ?assertEqual(ok, modernize(ID, C)),
    ?assertEqual([{EventID, Timestamp, {v1, Event}}], get_history(ID, C)),
    _ = delete_test_ets().

%% Machinery handler

-type event() :: any().
-type aux_st() :: any().
-type machine() :: machinery:machine(event(), aux_st()).
-type handler_opts() :: machinery:handler_opts(_).
-type result() :: machinery:result(event(), aux_st()).

-spec init(_Args, machine(), undefined, handler_opts()) -> result().
init(init_something, _Machine, _, _Opts) ->
    #{
        events => [init_event],
        aux_state => #{}
    }.

-spec process_timeout(machine(), undefined, handler_opts()) -> result().
process_timeout(_Args, _, _Opts) ->
    erlang:error({not_implemented, process_timeout}).

-spec process_call(_Args, machine(), undefined, handler_opts()) -> no_return().
process_call(_Args, _Machine, _, _Opts) ->
    erlang:error({not_implemented, process_call}).

-spec process_repair(_Args, machine(), undefined, handler_opts()) -> no_return().
process_repair(_Args, _Machine, _, _Opts) ->
    erlang:error({not_implemented, process_repair}).

%% machinery_mg_schema callbacks

-spec marshal(machinery_mg_schema:t(), any(), machinery_mg_schema:context()) ->
    {machinery_msgpack:t(), machinery_mg_schema:context()}.
marshal({event, 2}, V, C) ->
    {{bin, erlang:term_to_binary({v2, V})}, C};
marshal({event, 1}, V, C) ->
    {{bin, erlang:term_to_binary({v1, V})}, C};
marshal(T, V, C) ->
    machinery_mg_schema_generic:marshal(T, V, C).

-spec unmarshal(machinery_mg_schema:t(), machinery_msgpack:t(), machinery_mg_schema:context()) ->
    {any(), machinery_mg_schema:context()}.
unmarshal({event, 2}, V, C) ->
    {bin, EncodedV} = V,
    {erlang:binary_to_term(EncodedV), C};
unmarshal({event, 1}, V, C) ->
    {bin, EncodedV} = V,
    {erlang:binary_to_term(EncodedV), C};
unmarshal(T, V, C) ->
    machinery_mg_schema_generic:unmarshal(T, V, C).

-spec get_version(machinery_mg_schema:vt()) -> machinery_mg_schema:version().
get_version(aux_state) ->
    undefined;
get_version(event) ->
    get_event_version().

%% Helpers

start(ID, Args, C) ->
    machinery:start(namespace(), ID, Args, get_backend(C)).

modernize(ID, C) ->
    machinery_modernizer:modernize(namespace(), ID, get_modernizer_backend(C)).

get(ID, C) ->
    machinery:get(namespace(), ID, get_backend(C)).

get_history(ID, C) ->
    {ok, #{history := History}} = get(ID, C),
    History.

namespace() ->
    general.

unique() ->
    genlib:unique().

start_backend(C) ->
    {ok, _PID} = supervisor:start_child(
        ?config(group_sup, C),
        child_spec(C)
    ).

-define(ETS, test_ets).

setup_test_ets() ->
    _ = ets:new(?ETS, [set, named_table]).

delete_test_ets() ->
    _ = ets:delete(?ETS).

set_event_version(Version) ->
    ets:insert(?ETS, {event_version, Version}).

get_event_version() ->
    [{event_version, Version}] = ets:lookup(?ETS, event_version),
    Version.

-spec child_spec(config()) -> supervisor:child_spec().
child_spec(C) ->
    child_spec(?config(backend, C), C).

-spec child_spec(atom(), config()) -> supervisor:child_spec().
child_spec(machinery_mg_backend, _C) ->
    Routes = backend_mg_routes() ++ modernizer_mg_routes(),
    ServerConfig = #{
        ip => {0, 0, 0, 0},
        port => 8022
    },
    machinery_utils:woody_child_spec(machinery_mg_backend, Routes, ServerConfig).

backend_mg_routes() ->
    BackendConfig = #{
        path => <<"/v1/stateproc">>,
        backend_config => #{
            schema => ?MODULE
        }
    },
    Handler = {?MODULE, BackendConfig},
    machinery_mg_backend:get_routes(
        [Handler],
        #{event_handler => woody_event_handler_default}
    ).

modernizer_mg_routes() ->
    ModernizerConfig = #{
        path => <<"/v1/modernizer">>,
        backend_config => #{
            schema => ?MODULE
        }
    },
    machinery_modernizer_mg_backend:get_routes(
        [ModernizerConfig],
        #{event_handler => woody_event_handler_default}
    ).

-spec get_backend(config()) -> machinery_mg_backend:backend().
get_backend(C) ->
    get_backend(?config(backend, C), C).

-spec get_modernizer_backend(config()) -> machinery_mg_backend:backend().
get_modernizer_backend(C) ->
    get_backend(?config(modernizer_backend, C), C).

-spec get_backend(atom(), config()) ->
    machinery_mg_backend:backend()
    | machinery_modernizer_mg_backend:backend().
get_backend(machinery_mg_backend, C) ->
    machinery_mg_backend:new(
        ct_helper:get_woody_ctx(C),
        #{
            client => #{
                url => <<"http://machinegun:8022/v1/automaton">>,
                event_handler => woody_event_handler_default
            },
            schema => ?MODULE
        }
    );
get_backend(machinery_modernizer_mg_backend, C) ->
    machinery_modernizer_mg_backend:new(
        ct_helper:get_woody_ctx(C),
        #{
            client => #{
                url => <<"http://machinegun:8022/v1/automaton">>,
                event_handler => woody_event_handler_default
            },
            schema => ?MODULE
        }
    ).
