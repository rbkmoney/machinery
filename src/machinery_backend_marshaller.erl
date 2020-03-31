%%%
%%% Copyright 2020 RBKmoney
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%
%%%
%%% Machinery backend marshaller behaviour.

-module(machinery_backend_marshaller).

%% API
-export([marshal/3]).
-export([unmarshal/3]).

%% Behaviour definition

-type kind() :: term().
-type args() :: term().
-type result() :: term().

-export_type([kind/0]).
-export_type([args/0]).
-export_type([result/0]).

-callback marshal(kind(), args()) ->
    result().

-callback unmarshal(kind(), args()) ->
    result().

%% API

-type backend() :: module().
-export_type([backend/0]).

-spec marshal(backend(), kind(), args()) ->
    result().

marshal(Backend, Kind, UnmarshalledData) ->
    Backend:marshal(Kind, UnmarshalledData).

-spec unmarshal(backend(), kind(), args()) ->
    result().

unmarshal(Backend, Kind, MarshalledData) ->
    Backend:unmarshal(Kind, MarshalledData).
