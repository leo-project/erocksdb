%% -------------------------------------------------------------------
%%
%%  eleveldb: Erlang Wrapper for LevelDB (http://code.google.com/p/leveldb/)
%%
%% Copyright (c) 2010 Basho Technologies, Inc. All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(basho_bench_driver_eldb).

-record(state, { ref }).

-export([new/1,
         run/4]).

%% ====================================================================
%% API
%% ====================================================================

new(Id) ->
    %% Pull the eleveldb_config key which has all the key/value pairs for the
    %% engine -- stuff everything into the eleveldb application namespace
    %% so that starting the app will pull it in.
    application:load(eleveldb),
    Config = basho_bench_config:get(eleveldb_config, [{max_open_files, 50}]),
    [ok = application:set_env(eleveldb, K, V) || {K, V} <- Config],

    if Id == 1 ->
            io:format("\n"),
            io:format("NOTE: ELevelDB driver is using separate data\n"),
            io:format("      directories for each concurrent basho_bench\n"),
            io:format("      driver instance.\n\n");
       true ->
            ok
    end,

    WorkDir = basho_bench_config:get(eleveldb_work_dir, "/tmp/eleveldb.bb") ++
        "." ++ integer_to_list(Id),
    case basho_bench_config:get(eleveldb_clear_work_dir, false) of
        true ->
            io:format("Clearing work dir: " ++ WorkDir ++ "\n"),
            os:cmd("rm -rf " ++ WorkDir ++ "/*");
        false ->
            ok
    end,

    case eleveldb:open(WorkDir, [{create_if_missing, true}] ++ Config) of
        {ok, Ref} ->
            {ok, #state { ref = Ref }};
        {error, Reason} ->
            {error, Reason}
    end.

run(get, KeyGen, _ValueGen, State) ->
    Key = iolist_to_binary(KeyGen()),
    case eleveldb:get(State#state.ref, Key, []) of
        {ok, _Value} ->
            {ok, State};
        not_found ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(put, KeyGen, ValueGen, State) ->
    print_status(State#state.ref, 1000),
    Key = iolist_to_binary(KeyGen()),
    case eleveldb:put(State#state.ref, Key, ValueGen(), []) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, State, Reason}
    end.


print_status(Ref, Count) ->
    status_counter(Count, fun() ->
                                  {ok, S} = eleveldb:status(Ref, <<"leveldb.stats">>),
                                  io:format("~s\n", [S])
                          end).

status_counter(Max, Fun) ->
    Curr = case erlang:get(status_counter) of
               undefined ->
                   -1;
               Value ->
                   Value
           end,
    Next = (Curr + 1) rem Max,
    erlang:put(status_counter, Next),
    case Next of
        0 -> Fun(), ok;
        _ -> ok
    end.

