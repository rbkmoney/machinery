%%%
%%% Msgpack manipulation employed by machinegun interfaces.

-module(machinery_msgpack).

-include_lib("mg_proto/include/mg_proto_msgpack_thrift.hrl").

%% API

-export([wrap/1]).
-export([unwrap/1]).

-export([nil/0]).

-type t() :: mg_proto_msgpack_thrift:'Value'().

-export_type([t/0]).

%%

-spec wrap
    (nil) -> t();
    (boolean()) -> t();
    (integer()) -> t();
    (float()) -> t();
    %% string
    (binary()) -> t();
    %% binary
    ({binary, binary()}) -> t();
    ([t()]) -> t();
    (#{t() => t()}) -> t().
wrap(nil) ->
    {nl, #mg_msgpack_Nil{}};
wrap(V) when is_boolean(V) ->
    {b, V};
wrap(V) when is_integer(V) ->
    {i, V};
wrap(V) when is_float(V) ->
    V;
wrap(V) when is_binary(V) ->
    % Assuming well-formed UTF-8 bytestring.
    {str, V};
wrap({binary, V}) when is_binary(V) ->
    {bin, V};
wrap(V) when is_list(V) ->
    {arr, V};
wrap(V) when is_map(V) ->
    {obj, V}.

-spec unwrap(t()) ->
    nil
    | boolean()
    | integer()
    | float()
    %% string
    | binary()
    %% binary
    | {binary, binary()}
    | [t()]
    | #{t() => t()}.
unwrap({nl, #mg_msgpack_Nil{}}) ->
    nil;
unwrap({b, V}) when is_boolean(V) ->
    V;
unwrap({i, V}) when is_integer(V) ->
    V;
unwrap({flt, V}) when is_float(V) ->
    V;
unwrap({str, V}) when is_binary(V) ->
    % Assuming well-formed UTF-8 bytestring.
    V;
unwrap({bin, V}) when is_binary(V) ->
    {binary, V};
unwrap({arr, V}) when is_list(V) ->
    V;
unwrap({obj, V}) when is_map(V) ->
    V.

%%

-spec nil() -> t().
nil() ->
    wrap(nil).
