-module(machinery_start_SUITE).

-behaviour(machinery).

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

-export([ordinary_start_test/1]).
-export([exists_start_test/1]).
-export([unknown_namespace_start_test/1]).
-export([failed_start_test/1]).

%% Machinery callbacks

-export([init/4]).
-export([process_timeout/3]).
-export([process_repair/4]).
-export([process_call/4]).

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

-spec groups() ->
    [{group_name(), list(), test_case_name()}].
groups() ->
    [
        {machinery_mg_backend, [], [{group, all}]},
        {all, [parallel], [
            ordinary_start_test,
            exists_start_test,
            unknown_namespace_start_test,
            failed_start_test
        ]}
    ].

-spec init_per_suite(config()) -> config().
init_per_suite(C) ->
    {StartedApps, _StartupCtx} = ct_helper:start_apps([machinery]),
    [{started_apps, StartedApps}| C].

-spec end_per_suite(config()) -> _.
end_per_suite(C) ->
    ok = ct_helper:stop_apps(?config(started_apps, C)),
    ok.

-spec init_per_group(group_name(), config()) -> config().
init_per_group(machinery_mg_backend = Name, C0) ->
    C1 = [{backend, Name}, {group_sup, ct_sup:start()} | C0],
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

-spec ordinary_start_test(config()) -> test_return().
ordinary_start_test(C) ->
    ID = unique(),
    ?assertEqual(ok, start(ID, init_something, C)).

-spec exists_start_test(config()) -> test_return().
exists_start_test(C) ->
    ID = unique(),
    ?assertEqual(ok, start(ID, init_something, C)),
    ?assertEqual({error, exists}, start(ID, init_something, C)).

-spec unknown_namespace_start_test(config()) -> test_return().
unknown_namespace_start_test(C) ->
    ID = unique(),
    ?assertError({namespace_not_found, mmm}, machinery:start(mmm, ID, init_something, get_backend(C))).

-spec failed_start_test(config()) -> test_return().
failed_start_test(C) ->
    ID = unique(),
    ?assertError({failed, general, ID}, start(ID, fail, C)),
    ?assertError({failed, general, ID}, start(ID, fail, C)),
    ?assertEqual(ok, start(ID, init_something, C)),
    ?assertEqual({error, exists}, start(ID, fail, C)).

%% Machinery handler

-type event() :: any().
-type aux_st() :: any().
-type machine() :: machinery:machine(event(), aux_st()).
-type handler_opts() :: machinery:handler_opts(_).
-type result() :: machinery:result(event(), aux_st()).
-type response() :: machinery:response(_).

-spec init(_Args, machine(), undefined, handler_opts()) ->
    result().
init(init_something, _Machine, _, _Opts) ->
    #{
        events => [init_event],
        aux_state => #{some => <<"complex">>, aux_state => 1}
    };
init(fail, _Machine, _, _Opts) ->
    erlang:error(fail).

-spec process_timeout(machine(), undefined, handler_opts()) ->
    result().
process_timeout(#{}, _, _Opts) ->
    #{}.

-spec process_call(_Args, machine(), undefined, handler_opts()) ->
    {response(), result()}.
process_call(_Args, _Machine, _, _Opts) ->
    erlang:error({not_implemented, process_call}).

-spec process_repair(_Args, machine(), undefined, handler_opts()) ->
    no_return().
process_repair(_Args, _Machine, _, _Opts) ->
    erlang:error({not_implemented, process_repair}).

%% Helpers

start(ID, Args, C) ->
    machinery:start(namespace(), ID, Args, get_backend(C)).

namespace() ->
    general.

unique() ->
    genlib:unique().

start_backend(C) ->
    {ok, _PID} = supervisor:start_child(
        ?config(group_sup, C),
        child_spec(C)
    ).

-spec child_spec(config()) ->
    supervisor:child_spec().
child_spec(C) ->
    child_spec(?config(backend, C), C).

-spec child_spec(atom(), config()) ->
    supervisor:child_spec().
child_spec(machinery_mg_backend, _C) ->
    BackendConfig = #{
        path => <<"/v1/stateproc">>,
        backend_config => #{
            schema => machinery_mg_schema_generic
        }
    },
    Handler = {?MODULE, BackendConfig},
    Routes = machinery_mg_backend:get_routes(
        [Handler],
        #{event_handler => woody_event_handler_default}
    ),
    ServerConfig = #{
        ip => {0, 0, 0, 0},
        port => 8022
    },
    machinery_utils:woody_child_spec(machinery_mg_backend, Routes, ServerConfig).

-spec get_backend(config()) ->
    machinery_mg_backend:backend().
get_backend(C) ->
    get_backend(?config(backend, C), C).

-spec get_backend(atom(), config()) ->
    machinery_mg_backend:backend().
get_backend(machinery_mg_backend, C) ->
    machinery_mg_backend:new(
        ct_helper:get_woody_ctx(C),
        #{
            client => #{
                url => <<"http://machinegun:8022/v1/automaton">>,
                event_handler => woody_event_handler_default
            },
            schema => machinery_mg_schema_generic
        }
    ).
