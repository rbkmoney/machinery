-module(machinery_timeout_SUITE).

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

-export([start_with_timer_test/1]).
-export([start_with_continue_test/1]).
-export([start_with_ranged_timer_test/1]).
-export([call_with_timer_test/1]).
-export([call_with_continue_test/1]).
-export([call_with_ranged_timer_test/1]).

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
            start_with_timer_test,
            start_with_continue_test,
            start_with_ranged_timer_test,
            call_with_timer_test,
            call_with_continue_test,
            call_with_ranged_timer_test
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

-spec start_with_timer_test(config()) -> test_return().
start_with_timer_test(C) ->
    ID = unique(),
    ?assertEqual(ok, start(ID, init_timer, C)),
    timer:sleep(timer:seconds(5)),
    Expected = lists:seq(1, 10),
    ?assertMatch({ok, #{aux_state := Expected}}, get(ID, C)).

-spec start_with_continue_test(config()) -> test_return().
start_with_continue_test(C) ->
    ID = unique(),
    ?assertEqual(ok, start(ID, init_continue, C)),
    timer:sleep(timer:seconds(5)),
    Expected = lists:seq(1, 10),
    ?assertMatch({ok, #{aux_state := Expected}}, get(ID, C)).

-spec start_with_ranged_timer_test(config()) -> test_return().
start_with_ranged_timer_test(C) ->
    ID = unique(),
    ?assertEqual(ok, start(ID, init_timer_with_range, C)),
    timer:sleep(timer:seconds(5)),
    Expected = lists:seq(2, 3),
    ?assertMatch({ok, #{aux_state := Expected}}, get(ID, C)).

-spec call_with_timer_test(config()) -> test_return().
call_with_timer_test(C) ->
    ID = unique(),
    ?assertEqual(ok, start(ID, nop, C)),
    ?assertEqual({ok, done}, call(ID, timer, C)),
    timer:sleep(timer:seconds(5)),
    Expected = lists:seq(1, 10),
    ?assertMatch({ok, #{aux_state := Expected}}, get(ID, C)).

-spec call_with_continue_test(config()) -> test_return().
call_with_continue_test(C) ->
    ID = unique(),
    ?assertEqual(ok, start(ID, nop, C)),
    ?assertEqual({ok, done}, call(ID, continue, C)),
    timer:sleep(timer:seconds(5)),
    Expected = lists:seq(1, 10),
    ?assertMatch({ok, #{aux_state := Expected}}, get(ID, C)).

-spec call_with_ranged_timer_test(config()) -> test_return().
call_with_ranged_timer_test(C) ->
    ID = unique(),
    ?assertEqual(ok, start(ID, nop, C)),
    ?assertEqual({ok, done}, call(ID, timer_with_range, C)),
    timer:sleep(timer:seconds(5)),
    Expected = lists:seq(2, 3),
    ?assertMatch({ok, #{aux_state := Expected}}, get(ID, C)).

%% Machinery handler

-type event() :: any().
-type aux_st() :: any().
-type machine() :: machinery:machine(event(), aux_st()).
-type handler_opts() :: machinery:handler_opts(_).
-type result() :: machinery:result(event(), aux_st()).
-type response() :: machinery:response(_).

-spec init(_Args, machine(), undefined, handler_opts()) ->
    result().
init(nop, _Machine, _, _Opts) ->
    #{};
init(init_timer, _Machine, _, _Opts) ->
    #{
        events => lists:seq(1, 10),
        action => {set_timer, {timeout, 0}}
    };
init(init_continue, _Machine, _, _Opts) ->
    #{
        events => lists:seq(1, 10),
        action => continue
    };
init(init_timer_with_range, _Machine, _, _Opts) ->
    #{
        events => lists:seq(1, 10),
        action => {set_timer, {deadline, {{{1990, 01, 01}, {0, 0, 0}}, 0}}, {1, 2, forward}, 15}
    }.

-spec process_timeout(machine(), undefined, handler_opts()) ->
    result().
process_timeout(#{history := History}, _, _Opts) ->
    Bodies = lists:map(fun({_ID, _CreatedAt, Body}) -> Body end, History),
    #{
        events => [timer_fired],
        action => unset_timer,  % why not
        aux_state => Bodies
    }.

-spec process_call(_Args, machine(), undefined, handler_opts()) ->
    {response(), result()}.
process_call(timer, _Machine, _, _Opts) ->
    {done, #{
        events => lists:seq(1, 10),
        action => {set_timer, {timeout, 0}}
    }};
process_call(continue, _Machine, _, _Opts) ->
    {done, #{
        events => lists:seq(1, 10),
        action => continue
    }};
process_call(timer_with_range, _Machine, _, _Opts) ->
    {done, #{
        events => lists:seq(1, 10),
        action => {set_timer, {deadline, {{{1990, 01, 01}, {0, 0, 0}}, 0}}, {1, 2, forward}, 15}
    }}.

-spec process_repair(_Args, machine(), undefined, handler_opts()) ->
    no_return().
process_repair(_Args, _Machine, _, _Opts) ->
    erlang:error({not_implemented, process_repair}).

%% Helpers

start(ID, Args, C) ->
    machinery:start(namespace(), ID, Args, get_backend(C)).

call(Ref, Args, C) ->
    machinery:call(namespace(), Ref, Args, get_backend(C)).

get(Ref, C) ->
    machinery:get(namespace(), Ref, get_backend(C)).

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
