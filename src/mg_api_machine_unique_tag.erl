%%%
%%% Machine behaving as a unique tag.

-module(mg_api_machine_unique_tag).

%% API

-type id()        :: mg_api:id().
-type namespace() :: mg_api:namespace().
-type tag()       :: binary().

-export_type([tag/0]).

-export([tag/4]).
-export([untag/4]).
-export([get/3]).

%% Machine behaviour

-behaviour(mg_api).

-export([init/4]).
-export([process_timeout/3]).
-export([process_call/4]).

%%

-spec tag(namespace(), tag(), id(), mg_api:backend(_)) ->
    ok | {error, {set, id()}}.

tag(NS, Tag, ID, Backend) ->
    case mg_api:start(construct_namespace(NS), Tag, {tag, ID}, Backend) of
        ok ->
            ok;
        {error, exists} ->
            case get(NS, Tag, Backend) of
                {ok, ID} ->
                    ok;
                {ok, IDWas} ->
                    {error, {set, IDWas}};
                {error, unset} ->
                    tag(NS, Tag, ID, Backend)
            end
    end.

-spec untag(namespace(), tag(), id(), mg_api:backend(_)) ->
    ok | {error, {set, id()}}.

untag(NS, Tag, ID, Backend) ->
    case mg_api:call(construct_namespace(NS), Tag, {untag, ID}, Backend) of
        {ok, ok} ->
            ok;
        {ok, {error, IDWas}} ->
            {error, {set, IDWas}};
        {error, notfound} ->
            ok
    end.

-spec get(namespace(), tag(), mg_api:backend(_)) ->
    {ok, id()} | {error, unset}.

get(NS, Tag, Backend) ->
    case mg_api:get(construct_namespace(NS), Tag, Backend) of
        {ok, Machine} ->
            ID = get_machine_st(Machine),
            {ok, ID};
        {error, notfound} ->
            {error, unset}
    end.

construct_namespace(NS) ->
    list_to_atom(atom_to_list(NS) ++ "/tags").

%%

-type machine()      :: mg_api:machine(ev()).
-type handler_opts() :: mg_api:handler_opts(_).
-type result()       :: mg_api:result(ev()).
-type response()     :: mg_api:response(
    ok | {error, id()}
).

-type ev() ::
    {tag_set, id()} |
    tag_unset.

-spec init({tag, id()}, machine(), undefined, handler_opts()) ->
    result().

-spec process_timeout(machine(), undefined, handler_opts()) ->
    result().

-spec process_call({untag, id()}, machine(), undefined, handler_opts()) ->
    {response(), result()}.

init({tag, ID}, _Machine, _, _Opts) ->
    #{
        events => [ID]
    }.

process_timeout(#{}, _, _Opts) ->
    #{}.

process_call({untag, ID}, Machine, _, _Opts) ->
    case get_machine_st(Machine) of
        ID ->
            {ok, #{
                action => [remove]
            }};
        IDWas ->
            {{error, IDWas}, #{}}
    end.

%%

get_machine_st(#{history := History}) ->
    collapse_history(History).

collapse_history(History) ->
    lists:foldl(fun apply_event/2, undefined, History).

apply_event({_ID, _CreatedAt, Body}, St) ->
    apply_event_body(Body, St).

apply_event_body(ID, undefined) ->
    ID.
