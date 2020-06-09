-module(machinery_mg_schema_flow_SUITE).

-behaviour(machinery).

-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").

%% Common Tests callbacks
-export([all/0]).
-export([groups/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).
-export([init_per_testcase/2]).

%% Tests

-export([schema_versions_used_properly_test/1]).

%% Machinery callbacks

-export([init/4]).
-export([process_timeout/3]).
-export([process_repair/4]).
-export([process_call/4]).

%% machinery_mg_schema callbacks

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
        {group, all}
    ].

-spec groups() ->
    [{group_name(), list(), test_case_name()}].
groups() ->
    [
        {all, [parallel], [
            schema_versions_used_properly_test
        ]}
    ].

-spec init_per_suite(config()) -> config().
init_per_suite(C0) ->
    {StartedApps, _StartupCtx} = ct_helper:start_apps([machinery]),
    C1 = [{backend, machinery_mg_backend}, {group_sup, ct_sup:start()} | C0],
    {ok, _Pid} = start_backend(C1),
    [{started_apps, StartedApps}| C1].

-spec end_per_suite(config()) -> _.
end_per_suite(C) ->
    ok = ct_helper:stop_apps(?config(started_apps, C)),
    ok = ct_sup:stop(?config(group_sup, C)).

-spec init_per_testcase(test_case_name(), config()) -> config().
init_per_testcase(TestCaseName, C) ->
    ct_helper:makeup_cfg([ct_helper:test_case_name(TestCaseName), ct_helper:woody_ctx()], C).

%% Tests

-spec schema_versions_used_properly_test(config()) -> test_return().
schema_versions_used_properly_test(C) ->
    ID = unique(),
    ?assertEqual(ok, start(ID, init_something, C)),
    ?assertEqual({ok, done}, call(ID, do_something, C)),
    timer:sleep(timer:seconds(5)),
    ?assertEqual({ok, done}, call(ID, do_something, C)),
    ?assertError({failed, general, ID}, call(ID, fail, C)),
    ?assertEqual({ok, done}, repair(ID, repair_something, {20, 10, forward}, C)),
    ?assertEqual({ok, done}, call(ID, do_something, C)).

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
        action => continue,
        aux_state => #{some => <<"complex">>, aux_state => 1}
    }.

-spec process_timeout(machine(), undefined, handler_opts()) ->
    result().
process_timeout(_Machine, _, _Opts) ->
    #{
        events => [{timeout_event, 1}],
        action => unset_timer,  % why not
        aux_state => #{some => <<"other complex">>, aux_state => 1}
    }.

-spec process_call(_Args, machine(), undefined, handler_opts()) ->
    {response(), result()}.
process_call(do_something, _Machine, _, _Opts) ->
    {done, #{
        events => [call_event],
        aux_state => undefined
    }};
process_call(fail, _Machine, _, _Opts) ->
    erlang:error(fail).

-spec process_repair(_Args, machine(), undefined, handler_opts()) ->
    no_return().
process_repair(repair_something, #{history := History}, _, _Opts) ->
    {ok, {done, #{events => [{count_events, erlang:length(History)}]}}}.

%% machinery_mg_schema callbacks

-spec marshal(machinery_mg_schema:t(), any(), machinery_mg_schema:context()) ->
    {machinery_msgpack:t(), machinery_mg_schema:context()}.
marshal(T, V, C) when
    T =:= {aux_state, 1} orelse
    T =:= {event, 2} orelse
    T =:= {args, init} orelse
    T =:= {args, call} orelse
    T =:= {args, repair} orelse
    T =:= {response, call} orelse
    T =:= {response, {repair, success}} orelse
    T =:= {response, {repair, failure}}
->
    {{bin, erlang:term_to_binary(V)}, process_context(T, C)}.

-spec unmarshal(machinery_mg_schema:t(), machinery_msgpack:t(), machinery_mg_schema:context()) ->
    {any(), machinery_mg_schema:context()}.
unmarshal(T, V, C) when
    T =:= {aux_state, 1} orelse
    T =:= {event, 2} orelse
    T =:= {args, init} orelse
    T =:= {args, call} orelse
    T =:= {args, repair} orelse
    T =:= {response, call} orelse
    T =:= {response, {repair, failure}}
->
    {bin, EncodedV} = V,
    {erlang:binary_to_term(EncodedV), process_context(T, C)};
unmarshal({aux_state, undefined} = T, {bin, <<>>}, C) ->
    % initial aux_state
    {undefined, process_context(T, C)};
unmarshal({response, {repair, success}} = T, {bin, <<"ok">>}, C) ->
    % mg repair migration artefact
    {done, process_context(T, C)}.

-spec get_version(machinery_mg_schema:vt()) ->
    machinery_mg_schema:version().
get_version(aux_state) ->
    1;
get_version(event) ->
    2.

-spec process_context(machinery_mg_schema:t(), machinery_mg_schema:context()) ->
    machinery_mg_schema:context() | no_return().
process_context(T, C) ->
    ?assertMatch(#{machine_ref := _, machine_ns := general}, C),
    do_process_context(T, C).

-spec do_process_context(machinery_mg_schema:t(), machinery_mg_schema:context()) ->
    machinery_mg_schema:context() | no_return().
do_process_context({response, call}, C) ->
    ?assertMatch(#{{args, call} := ok}, C),
    C;
do_process_context({response, {repair, success}}, C) ->
    ?assertMatch(#{{args, repair} := ok}, C),
    C;
do_process_context({response, {repair, failure}}, C) ->
    ?assertMatch(#{{args, repair} := ok}, C),
    C;
do_process_context({args, _} = T, C) ->
    C#{T => ok};
do_process_context({event, _}, C) ->
    ?assertMatch(#{aux_state := _, my_key := test}, C),
    C;
do_process_context({aux_state, _}, C) ->
    C#{my_key => test}.

%% Helpers

start(ID, Args, C) ->
    machinery:start(namespace(), ID, Args, get_backend(C)).

call(Ref, Args, C) ->
    machinery:call(namespace(), Ref, Args, get_backend(C)).

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
            schema => ?MODULE
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
            schema => ?MODULE
        }
    ).
