%% -------------------------------------------------------------------
%%
%% basho_bench: Benchmarking Suite
%%
%% Copyright (c) 2009-2010 Basho Techonologies
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
-module(basho_bench_driver_rocksdb).

-export([new/1,
         run/4]).


-record(state, { db_handle,
                 filename,
                 db_options,
                 cf_options}).

%% ====================================================================
%% API
%% ====================================================================

new(_Id) ->
    %% Make sure erocksdb is available
    case code:which(erocksdb) of
        non_existing ->
            io:format("~s requires erocksdb to be available on code path.\n",
                      [?MODULE]),
            exit(1);
        _ ->
            ok
    end,

    %% Get the target directory
    Dir = basho_bench_config:get(rocksdb_dir, "."),
    Filename = filename:join(Dir, "test.rocksdb"),
    ok = filelib:ensure_dir(Filename),

    %% Get any erocksdb options 
    DBOptions = basho_bench_config:get(rocksdb_db_options, [{create_if_missing, true}]),
    CFOptions = basho_bench_config:get(rocksdb_cf_options, []),
    case erocksdb:open(Filename, DBOptions, CFOptions) of
        {error, Reason} ->
            io:format("Failed to open rocksdb in ~s: ~p\n", [Filename, Reason]);
        {ok, DBHandle}->
            {ok, #state { db_handle = DBHandle,
                          filename = Filename,
                          db_options = DBOptions,
                          cf_options = CFOptions}}
    end.



run(get, KeyGen, _ValueGen, State) ->
    case erocksdb:get(State#state.db_handle, KeyGen(), []) of
        {ok, _Value} ->
            {ok, State};
        not_found ->
            {ok, State};
        {error, Reason} ->
            {error, Reason}
    end;
run(put, KeyGen, ValueGen, State) ->
    case erocksdb:put(State#state.db_handle, KeyGen(), ValueGen(), []) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason}
    end.

