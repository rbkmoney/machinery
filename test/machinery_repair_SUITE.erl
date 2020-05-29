-module(machinery_repair_SUITE).

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

-export([simple_repair_test/1]).
-export([complex_repair_test/1]).
-export([ranged_repair_test/1]).
-export([notfound_repair_test/1]).
-export([failed_repair_test/1]).
-export([unknown_namespace_repair_test/1]).
-export([working_repair_test/1]).

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
            simple_repair_test,
            complex_repair_test,
            ranged_repair_test,
            notfound_repair_test,
            failed_repair_test,
            unknown_namespace_repair_test,
            working_repair_test
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

-spec simple_repair_test(config()) -> test_return().
simple_repair_test(C) ->
    ID = unique(),
    ?assertEqual(ok, start(ID, init_numbers, C)),
    ?assertError({failed, general, ID}, call(ID, fail, C)),
    ?assertEqual({ok, <<"ok">>}, repair(ID, simple, C)),
    ?assertEqual({ok, lists:seq(1, 100)}, call(ID, get_events, C)).

-spec complex_repair_test(config()) -> test_return().
complex_repair_test(C) ->
    ID = unique(),
    ?assertEqual(ok, start(ID, init_numbers, C)),
    ?assertError({failed, general, ID}, call(ID, fail, C)),
    ?assertEqual({ok, <<"ok">>}, repair(ID, {add_events, [repair_event]}, C)),
    ?assertEqual({ok, lists:seq(1, 100) ++ [repair_event]}, call(ID, get_events, C)).

-spec ranged_repair_test(config()) -> test_return().
ranged_repair_test(C) ->
    ID = unique(),
    ?assertEqual(ok, start(ID, init_numbers, C)),
    ?assertError({failed, general, ID}, call(ID, fail, C)),
    ?assertEqual({ok, <<"ok">>}, repair(ID, count_events, {20, 10, forward}, C)),
    ?assertEqual({ok, lists:seq(1, 100) ++ [{count_events, 10}]}, call(ID, get_events, C)).

-spec notfound_repair_test(config()) -> test_return().
notfound_repair_test(C) ->
    ID = unique(),
    ?assertEqual({error, notfound}, repair(ID, simple, C)).

-spec failed_repair_test(config()) -> test_return().
failed_repair_test(C) ->
    ID = unique(),
    ?assertEqual(ok, start(ID, init_numbers, C)),
    ?assertError({failed, general, ID}, call(ID, fail, C)),
    ?assertError({failed, general, ID}, repair(ID, fail, C)),
    ?assertError({failed, general, ID}, call(ID, get_events, C)).

-spec unknown_namespace_repair_test(config()) -> test_return().
unknown_namespace_repair_test(C) ->
    ID = unique(),
    ?assertError({namespace_not_found, mmm}, machinery:repair(mmm, ID, simple, get_backend(C))).

-spec working_repair_test(config()) -> test_return().
working_repair_test(C) ->
    ID = unique(),
    ?assertEqual(ok, start(ID, init_numbers, C)),
    ?assertEqual({error, working}, repair(ID, simple, C)).

%% Machinery handler

-type event() :: any().
-type aux_st() :: any().
-type machine() :: machinery:machine(event(), aux_st()).
-type handler_opts() :: machinery:handler_opts(_).
-type result() :: machinery:result(event(), aux_st()).
-type response() :: machinery:response(_).

-spec init(_Args, machine(), undefined, handler_opts()) ->
    result().
init(init_numbers, _Machine, _, _Opts) ->
    #{
        events => lists:seq(1, 100)
    }.

-spec process_timeout(machine(), undefined, handler_opts()) ->
    result().
process_timeout(#{}, _, _Opts) ->
    erlang:error({not_implemented, process_timeout}).

-spec process_call(_Args, machine(), undefined, handler_opts()) ->
    {response(), result()}.
process_call(get_events, #{history := History}, _, _Opts) ->
    Bodies = lists:map(fun({_ID, _CreatedAt, Body}) -> Body end, History),
    {Bodies, #{}};
process_call(fail, _Machine, _, _Opts) ->
    erlang:error(fail).

-spec process_repair(_Args, machine(), undefined, handler_opts()) ->
    no_return().
process_repair(simple, _Machine, _, _Opts) ->
    {done, #{}};
process_repair({add_events, Events}, _Machine, _, _Opts) ->
    {done, #{events => Events}};
process_repair(count_events, #{history := History}, _, _Opts) ->
    {done, #{events => [{count_events, erlang:length(History)}]}};
process_repair(fail, _Machine, _, _Opts) ->
    erlang:error(fail).

%% Helpers

start(ID, Args, C) ->
    machinery:start(namespace(), ID, Args, get_backend(C)).

call(Ref, Args, C) ->
    machinery:call(namespace(), Ref, Args, get_backend(C)).

repair(Ref, Args, C) ->
    machinery:repair(namespace(), Ref, Args, get_backend(C)).

repair(Ref, Args, Range, C) ->
    machinery:repair(namespace(), Ref, Range, Args, get_backend(C)).

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
