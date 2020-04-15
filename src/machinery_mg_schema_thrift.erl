-module(machinery_mg_schema_thrift).

%% Storage schema behaviour
-behaviour(machinery_mg_schema).

-export([marshal/2]).
-export([unmarshal/2]).
-export([get_version/1]).

-spec get_version(machinery_mg_schema:vt()) ->
    machinery_mg_schema:version().

%% 

-type t()  :: machinery_mg_schema:t().
-type v(T) :: machinery_mg_schema:v(T).
-type eterm() ::
    atom()   |
    number() |
    tuple()  |
    binary() |
    list()   |
    map().

get_version(_) ->
    undefined.

-spec marshal(t(), v(eterm())) ->
    machinery_msgpack:t().

marshal(Type, Data) ->
    case is_supported_type(Type) of
        true ->
            serialize(Type, Data);
        false ->
            machinery_mg_schema_generic:marshal(Type, Data)
    end.

serialize(Type, Data) -> 
    {ok, Trans} = thrift_membuffer_transport:new(),
    {ok, Proto} = thrift_binary_protocol:new(Trans, [{strict_read, true}, {strict_write, true}]),
    case thrift_protocol:write(Proto, {Type, Data}) of
        {NewProto, ok} ->
            {_, {ok, Result}} = thrift_protocol:close_transport(NewProto),
            wrap_result(Result);
        {_NewProto, {error, Reason}} ->
            erlang:error({thrift, {protocol, Reason}})
    end.

wrap_result(Result) when is_binary(Result) ->
    {bin, Result}.

-spec unmarshal(t(), machinery_msgpack:t()) ->
    v(eterm()).

unmarshal(Type, Data) ->
    case is_supported_type(Type) of
        true ->
            deserialize(Type, Data);
        false ->
            machinery_mg_schema_generic:unmarshal(Type, Data)
    end.

deserialize(Type, Data) ->
    {ok, Trans} = thrift_membuffer_transport:new(Data),
    {ok, Proto} = thrift_binary_protocol:new(Trans, [{strict_read, true}, {strict_write, true}]),
    case thrift_protocol:read(Proto, Type) of
        {_NewProto, {ok, {bin, Binary}}} ->
            Binary;
        {_NewProto, {error, Reason}} ->
            erlang:error({thrift, {protocol, Reason}})
    end.

is_supported_type(Type) ->
    lists:member(Type, supported_types()).


% which types can be marshalled by this module
supported_types() ->
    [].
