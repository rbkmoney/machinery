-module(machinery_call_SUITE).

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

-export([ordinary_call_test/1]).
-export([notfound_call_test/1]).
-export([unknown_namespace_call_test/1]).
-export([ranged_call_test/1]).
-export([failed_call_test/1]).
-export([remove_call_test/1]).

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

-spec groups() -> [{group_name(), list(), [test_case_name() | {group, group_name()}]}].
groups() ->
    [
        {machinery_mg_backend, [], [{group, all}]},
        {all, [parallel], [
            ordinary_call_test,
            notfound_call_test,
            unknown_namespace_call_test,
            ranged_call_test,
            failed_call_test,
            remove_call_test
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

-spec ordinary_call_test(config()) -> test_return().
ordinary_call_test(C) ->
    ID = unique(),
    ?assertEqual(ok, start(ID, init_numbers, C)),
    ?assertEqual({ok, done}, call(ID, do_something, C)).

-spec notfound_call_test(config()) -> test_return().
notfound_call_test(C) ->
    ID = unique(),
    ?assertEqual({error, notfound}, call(ID, do_something, C)).

-spec unknown_namespace_call_test(config()) -> test_return().
unknown_namespace_call_test(C) ->
    ID = unique(),
    ?assertError({namespace_not_found, mmm}, machinery:call(mmm, ID, do_something, get_backend(C))).

-spec ranged_call_test(config()) -> test_return().
ranged_call_test(C) ->
    ID = unique(),
    ?assertEqual(ok, start(ID, init_numbers, C)),
    ?assertEqual({ok, lists:seq(9, 1, -1)}, call(ID, get_events, {10, 9, backward}, C)),
    ?assertEqual({ok, lists:seq(3, 11)}, call(ID, get_events, {2, 9, forward}, C)).

-spec failed_call_test(config()) -> test_return().
failed_call_test(C) ->
    ID = unique(),
    ?assertEqual(ok, start(ID, init_numbers, C)),
    ?assertError({failed, general, ID}, call(ID, fail, C)).

-spec remove_call_test(config()) -> test_return().
remove_call_test(C) ->
    ID = unique(),
    ?assertEqual(ok, start(ID, init_numbers, C)),
    ?assertEqual({ok, removed}, call(ID, remove, C)),
    ?assertEqual({error, notfound}, call(ID, do_something, C)).

%% Machinery handler

-type event() :: any().
-type aux_st() :: any().
-type machine() :: machinery:machine(event(), aux_st()).
-type handler_opts() :: machinery:handler_opts(_).
-type result() :: machinery:result(event(), aux_st()).
-type response() :: machinery:response(_).

-spec init(_Args, machine(), undefined, handler_opts()) -> result().
init(init_numbers, _Machine, _, _Opts) ->
    #{
        events => lists:seq(1, 100)
    }.

-spec process_timeout(machine(), undefined, handler_opts()) -> no_return().
process_timeout(#{}, _, _Opts) ->
    erlang:error({not_implemented, process_timeout}).

-spec process_call(_Args, machine(), undefined, handler_opts()) -> {response(), result()}.
process_call(do_something, _Machine, _, _Opts) ->
    {done, #{
        events => [1, yet_another_event],
        aux_state => <<>>
    }};
process_call(get_events, #{history := History}, _, _Opts) ->
    Bodies = lists:map(fun({_ID, _CreatedAt, Body}) -> Body end, History),
    {Bodies, #{}};
process_call(remove, _Machine, _, _Opts) ->
    {removed, #{action => [remove]}};
process_call(fail, _Machine, _, _Opts) ->
    erlang:error(fail).

-spec process_repair(_Args, machine(), undefined, handler_opts()) -> no_return().
process_repair(_Args, _Machine, _, _Opts) ->
    erlang:error({not_implemented, process_repair}).

%% Helpers

start(ID, Args, C) ->
    machinery:start(namespace(), ID, Args, get_backend(C)).

call(Ref, Args, C) ->
    machinery:call(namespace(), Ref, Args, get_backend(C)).

call(Ref, Args, Range, C) ->
    machinery:call(namespace(), Ref, Range, Args, get_backend(C)).

namespace() ->
    general.

unique() ->
    genlib:unique().

start_backend(C) ->
    {ok, _PID} = supervisor:start_child(
        ?config(group_sup, C),
        child_spec(C)
    ).

-spec child_spec(config()) -> supervisor:child_spec().
child_spec(C) ->
    child_spec(?config(backend, C), C).

-spec child_spec(atom(), config()) -> supervisor:child_spec().
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

-spec get_backend(config()) -> machinery_mg_backend:backend().
get_backend(C) ->
    get_backend(?config(backend, C), C).

-spec get_backend(atom(), config()) -> machinery_mg_backend:backend().
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
