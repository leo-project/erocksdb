%%======================================================================
%%
%% erocksdb: Erlang Wrapper for RocksDB (https://github.com/facebook/rocksdb)
%%
%% Copyright (c) 2012-2015 Rakuten, Inc.
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
%% @doc Erlang Wrapper for RocksDB
%% @reference https://github.com/leo-project/erocksdb/blob/master/src/erocksdb.erl
%% @end
%%======================================================================
-module(erocksdb).

-export([open/3, open/4, open_with_cf/3, close/1]).
-export([snapshot/1, release_snapshot/1]).
-export([list_column_families/2, create_column_family/3, drop_column_family/2]).
-export([put/4, put/5, delete/3, delete/4, write/3, get/3, get/4]).
-export([iterator/2, iterator/3, iterator_with_cf/3, iterator_move/2, iterator_close/1]).
-export([fold/4, fold/5, fold_keys/4, fold_keys/5]).
-export([destroy/2, repair/2, is_empty/1]).
-export([checkpoint/2]).
-export([count/1, count/2, status/1, status/2, status/3]).

-export_type([db_handle/0,
              cf_handle/0,
              itr_handle/0,
              snapshot_handle/0,
              compression_type/0,
              compaction_style/0,
              access_hint/0,
              wal_recovery_mode/0]).

-on_load(init/0).

-ifdef(TEST).
-compile(export_all).
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).
-endif.
-include_lib("eunit/include/eunit.hrl").
-endif.

%% This cannot be a separate function. Code must be inline to trigger
%% Erlang compiler's use of optimized selective receive.
-define(WAIT_FOR_REPLY(Ref),
        receive {Ref, Reply} ->
                Reply
        end).

-spec init() -> ok | {error, any()}.
init() ->
    SoName = case code:priv_dir(?MODULE) of
                 {error, bad_name} ->
                     case code:which(?MODULE) of
                         Filename when is_list(Filename) ->
                             filename:join([filename:dirname(Filename),"../priv", "erocksdb"]);
                         _ ->
                             filename:join("../priv", "erocksdb")
                     end;
                 Dir ->
                     filename:join(Dir, "erocksdb")
             end,
    erlang:load_nif(SoName, application:get_all_env(erocksdb)).

-record(db_path, {path        :: file:filename_all(),
                  target_size :: non_neg_integer()}).

-record(cf_descriptor, {name    :: string(),
                        options :: cf_options()}).

-type compression_type() :: snappy | zlib | bzip2 | lz4 | lz4h | none.
-type compaction_style() :: level | universal | fifo | none.
-type access_hint() :: normal | sequential | willneed | none.
-type wal_recovery_mode() :: tolerate_corrupted_tail_records |
                             absolute_consistency |
                             point_in_time_recovery |
                             skip_any_corrupted_records.
-type read_tier() :: read_all | block_cache | persisted | memtable.

-opaque db_handle() :: binary().
-opaque cf_handle() :: binary().
-opaque itr_handle() :: binary().
-opaque snapshot_handle() :: binary().

-type block_based_table_options() :: [{no_block_cache, boolean()} |
                                      {block_size, pos_integer()} |
                                      {block_cache_size, pos_integer()} |
                                      {bloom_filter_policy, BitsPerKey :: pos_integer()} |
                                      {format_version, 0 | 1 | 2} |
                                      {skip_table_builder_flush, boolean()} |
                                      {cache_index_and_filter_blocks, boolean()}].

-type cf_options() :: [{block_cache_size_mb_for_point_lookup, non_neg_integer()} |
                       {memtable_memory_budget, pos_integer()} |
                       {write_buffer_size,  pos_integer()} |
                       {db_write_buffer_size,  pos_integer()} |
                       {max_write_buffer_number,  pos_integer()} |
                       {min_write_buffer_number_to_merge,  pos_integer()} |
                       {compression,  compression_type()} |
                       {num_levels,  pos_integer()} |
                       {level0_file_num_compaction_trigger,  integer()} |
                       {level0_slowdown_writes_trigger,  integer()} |
                       {level0_stop_writes_trigger,  integer()} |
                       {max_mem_compaction_level,  pos_integer()} |
                       {target_file_size_base,  pos_integer()} |
                       {target_file_size_multiplier,  pos_integer()} |
                       {max_bytes_for_level_base,  pos_integer()} |
                       {max_bytes_for_level_multiplier,  pos_integer()} |
                       {max_compaction_bytes,  pos_integer()} |
                       {soft_rate_limit,  float()} |
                       {hard_rate_limit,  float()} |
                       {arena_block_size,  integer()} |
                       {disable_auto_compactions,  boolean()} |
                       {purge_redundant_kvs_while_flush,  boolean()} |
                       {compaction_style,  compaction_style()} |
                       {verify_checksums_in_compaction,  boolean()} |
                       {max_sequential_skip_in_iterations,  pos_integer()} |
                       {inplace_update_support,  boolean()} |
                       {inplace_update_num_locks,  pos_integer()} |
                       {table_factory_block_cache_size, pos_integer()} |
                       {in_memory_mode, boolean()} |
                       {block_based_table_options, block_based_table_options()}].

-type db_options() :: [{total_threads, pos_integer()} |
                       {create_if_missing, boolean()} |
                       {create_missing_column_families, boolean()} |
                       {error_if_exists, boolean()} |
                       {paranoid_checks, boolean()} |
                       {max_open_files, integer()} |
                       {max_total_wal_size, non_neg_integer()} |
                       {disable_data_sync, boolean()} |
                       {use_fsync, boolean()} |
                       {db_paths, list(#db_path{})} |
                       {db_log_dir, file:filename_all()} |
                       {wal_dir, file:filename_all()} |
                       {delete_obsolete_files_period_micros, pos_integer()} |
                       {max_background_jobs, pos_integer()} |
                       {max_log_file_size, non_neg_integer()} |
                       {log_file_time_to_roll, non_neg_integer()} |
                       {keep_log_file_num, pos_integer()} |
                       {max_manifest_file_size, pos_integer()} |
                       {table_cache_numshardbits, pos_integer()} |
                       {wal_ttl_seconds, non_neg_integer()} |
                       {wal_size_limit_mb, non_neg_integer()} |
                       {manifest_preallocation_size, pos_integer()} |
                       {allow_os_buffer, boolean()} |
                       {allow_mmap_reads, boolean()} |
                       {allow_mmap_writes, boolean()} |
                       {is_fd_close_on_exec, boolean()} |
                       {skip_log_error_on_recovery, boolean()} |
                       {stats_dump_period_sec, non_neg_integer()} |
                       {advise_random_on_open, boolean()} |
                       {access_hint, access_hint()} |
                       {compaction_readahead_size, non_neg_integer()} |
                       {use_adaptive_mutex, boolean()} |
                       {bytes_per_sync, non_neg_integer()} |
                       {wal_bytes_per_sync, non_neg_integer()} |
                       {skip_stats_update_on_db_open, boolean()} |
                       {enable_thread_tracking, boolean()} |
                       {avoid_flush_during_shutdown, boolean()} |
                       {avoid_flush_during_recovery, boolean()} |
                       {dump_malloc_stats, boolean()} |
                       {enable_write_thread_adaptive_yield, boolean()} |
                       {allow_concurrent_memtable_write, boolean()} |
                       {enable_pipelined_write, boolean()} |
                       {write_thread_slow_yield_usec, non_neg_integer()} |
                       {write_thread_max_yield_usec, non_neg_integer()} |
                       {wal_recovery_mode, wal_recovery_mode()}].

-type read_options() :: [{verify_checksums, boolean()} |
                         {fill_cache, boolean()} |
                         {iterate_upper_bound, binary()} |
                         {tailing, boolean()} |
                         {total_order_seek, boolean()} |
                         {managed, boolean()} |
                         {prefix_same_as_start, boolean()} |
                         {pin_data, boolean()} |
                         {background_purge_on_iterator_cleanup, boolean()} |
                         {ignore_range_deletions, boolean()} |
                         {readahead_size, non_neg_integer()} |
                         {max_skippable_internal_keys, non_neg_integer()} |
                         {read_tier, read_tier()} |
                         {snapshot, snapshot_handle()}].

-type write_options() :: [{sync, boolean()} |
                          {disable_wal, boolean()} |
                          {no_slowdown, boolean()} |
                          {low_pri, boolean()} |
                          {ignore_missing_column_families, boolean()}].

-type write_actions() :: [{put, Key::binary(), Value::binary()} |
                          {put, ColumnFamilyHandle::cf_handle(), Key::binary(), Value::binary()} |
                          {delete, Key::binary()} |
                          {delete, ColumnFamilyHandle::cf_handle(), Key::binary()} |
                          clear].

-type iterator_action() :: first | last | next | prev | binary().

async_open(_CallerRef, _Name, _DBOpts, _CFOpts, _Ttl) ->
    erlang:nif_error({error, not_loaded}).

%% @doc
%% Open RocksDB with the defalut column family
-spec(open(Name, DBOpts, CFOpts) ->
             {ok, db_handle()} | {error, any()} when Name::file:filename_all(),
                                                     DBOpts::db_options(),
                                                     CFOpts::cf_options()).
open(Name, DBOpts, CFOpts) ->
    open(Name, DBOpts, CFOpts, 0).

%% @doc
%% Open RocksDB with the defalut column family and TTL
-spec(open(Name, DBOpts, CFOpts, Ttl) ->
             {ok, db_handle()} | {error, any()} when Name::file:filename_all(),
                                                     DBOpts::db_options(),
                                                     CFOpts::cf_options(),
                                                     Ttl::number()).
open(Name, DBOpts, CFOpts, Ttl) ->
    CallerRef = make_ref(),
    async_open(CallerRef, Name, DBOpts, CFOpts, Ttl),
    ?WAIT_FOR_REPLY(CallerRef).

%% @doc
%% Open RocksDB with the specified column families
-spec(open_with_cf(Name, DBOpts, CFDescriptors) ->
             {ok, db_handle(), list(cf_handle())} | {error, any()}
               when Name::file:filename_all(),
                    DBOpts :: db_options(),
                    CFDescriptors :: list(#cf_descriptor{})).
open_with_cf(_Name, _DBOpts, _CFDescriptors) ->
    {error, not_implemeted}.

async_close(_Callerfef, _DBHandle) ->
    erlang:nif_error({error, not_loaded}).

%% @doc
%% Close RocksDB
-spec(close(DBHandle) ->
             ok | {error, any()} when DBHandle::db_handle()).
close(DBHandle) ->
    CallerRef = make_ref(),
    async_close(CallerRef, DBHandle),
    ?WAIT_FOR_REPLY(CallerRef).

async_snapshot(_CallerRef, _DbHandle) ->
    erlang:nif_error({error, not_loaded}).


%% @sdoc return a database snapshot
%% Snapshots provide consistent read-only views over the entire state of the key-value store
-spec(snapshot(DbHandle::db_handle()) ->
    {ok, snapshot_handle()} | {error, any()}).
snapshot(DbHandle) ->
    CallerRef = make_ref(),
    async_snapshot(CallerRef, DbHandle),
    ?WAIT_FOR_REPLY(CallerRef).

async_release_snapshot(_CallerRef, _SnapshotHandle) ->
    erlang:nif_error({error, not_loaded}).

%% @doc release a snapshot
-spec(release_snapshot(SnapshotHandle::snapshot_handle()) ->
    ok | {error, any()}).
release_snapshot(SnapshotHandle) ->
    CallerRef = make_ref(),
    async_release_snapshot(CallerRef, SnapshotHandle),
    ?WAIT_FOR_REPLY(CallerRef).

%% @doc
%% List column families
-spec(list_column_families(Name, DBOpts) ->
             {ok, list(string())} | {error, any()} when Name::file:filename_all(),
                                                        DBOpts::db_options()).
list_column_families(_Name, _DBOpts) ->
    {error, not_implemeted}.

%% @doc
%% Create a new column family
-spec(create_column_family(DBHandle, Name, CFOpts) ->
             {ok, cf_handle()} | {error, any()} when DBHandle::db_handle(),
                                                     Name::string(),
                                                     CFOpts::cf_options()).
create_column_family(_DBHandle, _Name, _CFOpts) ->
    {error, not_implemeted}.

%% @doc
%% Drop a column family
-spec(drop_column_family(DBHandle, CFHandle) ->
             ok | {error, any()} when DBHandle::db_handle(),
                                      CFHandle::cf_handle()).
drop_column_family(_DBHandle, _CFHandle) ->
    {error, not_implemeted}.

%% @doc
%% Put a key/value pair into the default column family
-spec(put(DBHandle, Key, Value, WriteOpts) ->
             ok | {error, any()} when DBHandle::db_handle(),
                                      Key::binary(),
                                      Value::binary(),
                                      WriteOpts::write_options()).
put(DBHandle, Key, Value, WriteOpts) ->
    write(DBHandle, [{put, Key, Value}], WriteOpts).

%% @doc
%% Put a key/value pair into the specified column family
-spec(put(DBHandle, CFHandle, Key, Value, WriteOpts) ->
             ok | {error, any()} when DBHandle::db_handle(),
                                      CFHandle::cf_handle(),
                                      Key::binary(),
                                      Value::binary(),
                                      WriteOpts::write_options()).
put(_DBHandle, _CFHandle, _Key, _Value, _WriteOpts) ->
    {error, not_implemeted}.

%% @doc
%% Delete a key/value pair in the default column family
-spec(delete(DBHandle, Key, WriteOpts) ->
             ok | {error, any()} when DBHandle::db_handle(),
                                      Key::binary(),
                                      WriteOpts::write_options()).
delete(DBHandle, Key, WriteOpts) ->
    write(DBHandle, [{delete, Key}], WriteOpts).

%% @doc
%% Delete a key/value pair in the specified column family
-spec(delete(DBHandle, CFHandle, Key, WriteOpts) ->
             ok | {error, any()} when DBHandle::db_handle(),
                                      CFHandle::cf_handle(),
                                      Key::binary(),
                                      WriteOpts::write_options()).
delete(_DBHandle, _CFHandle, _Key, _WriteOpts) ->
    {error, not_implemeted}.

async_write(_CallerRef, _DBHandle, _WriteActions, _WriteOpts) ->
    erlang:nif_error({error, not_loaded}).

%% @doc
%% Apply the specified updates to the database.
-spec(write(DBHandle, WriteActions, WriteOpts) ->
             ok | {error, any()} when DBHandle::db_handle(),
                                      WriteActions::write_actions(),
                                      WriteOpts::write_options()).
write(DBHandle, WriteActions, WriteOpts) ->
    CallerRef = make_ref(),
    async_write(CallerRef, DBHandle, WriteActions, WriteOpts),
    ?WAIT_FOR_REPLY(CallerRef).

async_get(_CallerRef, _DBHandle, _Key, _ReadOpts) ->
    erlang:nif_error({error, not_loaded}).

%% @doc
%% Retrieve a key/value pair in the default column family
-spec(get(DBHandle, Key, ReadOpts) ->
             {ok, binary()} | not_found | {error, any()} when DBHandle::db_handle(),
                                                              Key::binary(),
                                                              ReadOpts::read_options()).
get(DBHandle, Key, ReadOpts) ->
    CallerRef = make_ref(),
    async_get(CallerRef, DBHandle, Key, ReadOpts),
    ?WAIT_FOR_REPLY(CallerRef).

%% @doc
%% Retrieve a key/value pair in the specified column family
-spec(get(DBHandle, CFHandle, Key, ReadOpts) ->
             {ok, binary()} | not_found | {error, any()} when DBHandle::db_handle(),
                                                              CFHandle::cf_handle(),
                                                              Key::binary(),
                                                              ReadOpts::read_options()).
get(_DBHandle, _CFHandle, _Key, _ReadOpts) ->
    {error, not_implemeted}.

async_iterator(_CallerRef, _DBHandle, _ReadOpts) ->
    erlang:nif_error({error, not_loaded}).

async_iterator(_CallerRef, _DBHandle, _ReadOpts, keys_only) ->
    erlang:nif_error({error, not_loaded}).

%% @doc
%% Return a iterator over the contents of the database.
%% The result of iterator() is initially invalid (caller must
%% call iterator_move function on the iterator before using it).
-spec(iterator(DBHandle, ReadOpts) ->
             {ok, itr_handle()} | {error, any()} when DBHandle::db_handle(),
                                                      ReadOpts::read_options()).
iterator(DBHandle, ReadOpts) ->
    CallerRef = make_ref(),
    async_iterator(CallerRef, DBHandle, ReadOpts),
    ?WAIT_FOR_REPLY(CallerRef).

iterator(DBHandle, ReadOpts, keys_only) ->
    CallerRef = make_ref(),
    async_iterator(CallerRef, DBHandle, ReadOpts, keys_only),
    ?WAIT_FOR_REPLY(CallerRef).

%% @doc
%% Return a iterator over the contents of the specified column family.
-spec(iterator_with_cf(DBHandle, CFHandle, ReadOpts) ->
             {ok, itr_handle()} | {error, any()} when DBHandle::db_handle(),
                                                      CFHandle::cf_handle(),
                                                      ReadOpts::read_options()).
iterator_with_cf(_DBHandle, _CFHandle, _ReadOpts) ->
    {error, not_implemeted}.

async_iterator_move(_CallerRef, _ITRHandle, _ITRAction) ->
    erlang:nif_error({error, not_loaded}).

%% @doc
%% Move to the specified place
-spec(iterator_move(ITRHandle, ITRAction) ->
             {ok, Key::binary(), Value::binary()} |
             {ok, Key::binary()} |
             {error, invalid_iterator} |
             {error, iterator_closed} when ITRHandle::itr_handle(),
                                           ITRAction::iterator_action()).
iterator_move(ITRHandle, ITRAction) ->
    case async_iterator_move(undefined, ITRHandle, ITRAction) of
        Ref when is_reference(Ref) ->
            receive
                {Ref, X} -> X
            end;
        {ok, _} = Key -> Key;
        {ok, _, _} = KeyVal -> KeyVal;
        ER -> ER
    end.

async_iterator_close(_CallerRef, _ITRHandle) ->
    erlang:nif_error({error, not_loaded}).

%% @doc
%% Close a iterator
-spec(iterator_close(ITRHandle) ->
             ok when ITRHandle::itr_handle()).
iterator_close(ITRHandle) ->
    CallerRef = make_ref(),
    async_iterator_close(CallerRef, ITRHandle),
    ?WAIT_FOR_REPLY(CallerRef).

-type fold_fun() :: fun(({Key::binary(), Value::binary()}, any()) -> any()).

%% @doc
%% Calls Fun(Elem, AccIn) on successive elements in the default column family
%% starting with AccIn == Acc0.
%% Fun/2 must return a new accumulator which is passed to the next call.
%% The function returns the final value of the accumulator.
%% Acc0 is returned if the default column family is empty.
-spec(fold(DBHandle, Fun, Acc0, ReadOpts) ->
             any() when DBHandle::db_handle(),
                        Fun::fold_fun(),
                        Acc0::any(),
                        ReadOpts::read_options()).
fold(DBHandle, Fun, Acc0, ReadOpts) ->
    {ok, Itr} = iterator(DBHandle, ReadOpts),
    do_fold(Itr, Fun, Acc0).

%% @doc
%% Calls Fun(Elem, AccIn) on successive elements in the specified column family
%% Other specs are same with fold/4
-spec(fold(DBHandle, CFHandle, Fun, Acc0, ReadOpts) ->
             any() when DBHandle::db_handle(),
                        CFHandle::cf_handle(),
                        Fun::fold_fun(),
                        Acc0::any(),
                        ReadOpts::read_options()).
fold(_DBHandle, _CFHandle, _Fun, _Acc0, _ReadOpts) ->
    _Acc0.

-type fold_keys_fun() :: fun((Key::binary(), any()) -> any()).

%% @doc
%% Calls Fun(Elem, AccIn) on successive elements in the default column family
%% starting with AccIn == Acc0.
%% Fun/2 must return a new accumulator which is passed to the next call.
%% The function returns the final value of the accumulator.
%% Acc0 is returned if the default column family is empty.
-spec(fold_keys(DBHandle, Fun, Acc0, ReadOpts) ->
             any() when DBHandle::db_handle(),
                        Fun::fold_keys_fun(),
                        Acc0::any(),
                        ReadOpts::read_options()).
fold_keys(DBHandle, Fun, Acc0, ReadOpts) ->
    {ok, Itr} = iterator(DBHandle, ReadOpts, keys_only),
    do_fold(Itr, Fun, Acc0).

%% @doc
%% Calls Fun(Elem, AccIn) on successive elements in the specified column family
%% Other specs are same with fold_keys/4
-spec(fold_keys(DBHandle, CFHandle, Fun, Acc0, ReadOpts) ->
             any() when DBHandle::db_handle(),
                        CFHandle::cf_handle(),
                        Fun::fold_keys_fun(),
                        Acc0::any(),
                        ReadOpts::read_options()).
fold_keys(_DBHandle, _CFHandle, _Fun, _Acc0, _ReadOpts) ->
    _Acc0.

is_empty(_DBHandle) ->
    erlang:nif_error({error, not_loaded}).

%% @doc
%% Destroy the contents of the specified database.
%% Be very careful using this method.
-spec(destroy(Name, DBOpts) ->
             ok | {error, any()} when Name::file:filename_all(),
                                      DBOpts::db_options()).
destroy(_Name, _DBOpts) ->
    erlang:nif_error({error, not_loaded}).

%% @doc
%% Try to repair as much of the contents of the database as possible.
%% Some data may be lost, so be careful when calling this function
-spec(repair(Name, DBOpts) ->
             ok | {error, any()} when Name::file:filename_all(),
                                      DBOpts::db_options()).
repair(_Name, _DBOpts) ->
    erlang:nif_error({error, not_loaded}).


async_checkpoint(_Callerfef, _DbHandle, _Path) ->
    erlang:nif_error({error, not_loaded}).

%% @doc take a snapshot of a running RocksDB database in a separate directory
%% http://rocksdb.org/blog/2609/use-checkpoints-for-efficient-snapshots/
-spec(checkpoint(DbHandle::db_handle(), Path::file:filename_all()) ->
ok | {error, any()}).
checkpoint(DbHandle, Path) ->
    CallerRef = make_ref(),
    async_checkpoint(CallerRef, DbHandle, Path),
    ?WAIT_FOR_REPLY(CallerRef).


%% @doc
%% Return the approximate number of keys in the default column family.
%% Implemented by calling GetIntProperty with "rocksdb.estimate-num-keys"
%%
-spec(count(DBHandle) ->
             non_neg_integer() | {error, any()} when DBHandle::db_handle()).
count(DBHandle) ->
    case status(DBHandle, <<"rocksdb.estimate-num-keys">>) of
        {ok, BinCount} ->
            erlang:binary_to_integer(BinCount);
        Error ->
            Error
    end.

%% @doc
%% Return the approximate number of keys in the specified column family.
%%
-spec(count(DBHandle, CFHandle) ->
             non_neg_integer() | {error, any()} when DBHandle::db_handle(),
                                                     CFHandle::cf_handle()).
count(_DBHandle, _CFHandle) ->
    {error, not_implemeted}.

%% @doc
%% Return the current status of the default column family
%% Implemented by calling GetProperty with "rocksdb.stats"
%%
-spec(status(DBHandle) ->
             {ok, any()} | {error, any()} when DBHandle::db_handle()).
status(DBHandle) ->
    status(DBHandle, <<"rocksdb.stats">>).

%% @doc
%% Return the RocksDB internal status of the default column family specified at Property
%%
-spec(status(DBHandle, Property) ->
             {ok, any()} | {error, any()} when DBHandle::db_handle(),
                                               Property::binary()).
status(_DBHandle, _Property) ->
    erlang:nif_error({error, not_loaded}).

%% @doc
%% Return the RocksDB internal status of the specified column family specified at Property
%%
-spec(status(DBHandle, CFHandle, Property) ->
             string() | {error, any()} when DBHandle::db_handle(),
                                            CFHandle::cf_handle(),
                                            Property::binary()).
status(_DBHandle, _CFHandle, _Property) ->
    {error, not_implemeted}.

%% ===================================================================
%% Internal functions
%% ===================================================================
do_fold(Itr, Fun, Acc0) ->
    try
        fold_loop(iterator_move(Itr, first), Itr, Fun, Acc0)
    after
        iterator_close(Itr)
    end.

fold_loop({error, iterator_closed}, _Itr, _Fun, Acc0) ->
    throw({iterator_closed, Acc0});
fold_loop({error, invalid_iterator}, _Itr, _Fun, Acc0) ->
    Acc0;
fold_loop({ok, K}, Itr, Fun, Acc0) ->
    Acc = Fun(K, Acc0),
    fold_loop(iterator_move(Itr, next), Itr, Fun, Acc);
fold_loop({ok, K, V}, Itr, Fun, Acc0) ->
    Acc = Fun({K, V}, Acc0),
    fold_loop(iterator_move(Itr, next), Itr, Fun, Acc).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).
open_test() -> [{open_test_Z(), l} || l <- lists:seq(1, 20)].
open_test_Z() ->
    os:cmd("rm -rf /tmp/erocksdb.open.test"),
    {ok, Ref} = open("/tmp/erocksdb.open.test", [{create_if_missing, true}], []),
    true = ?MODULE:is_empty(Ref),
    ok = ?MODULE:put(Ref, <<"abc">>, <<"123">>, []),
    false = ?MODULE:is_empty(Ref),
    {ok, <<"123">>} = ?MODULE:get(Ref, <<"abc">>, []),
    {ok, 1} = ?MODULE:count(Ref),
    not_found = ?MODULE:get(Ref, <<"def">>, []),
    ok = ?MODULE:delete(Ref, <<"abc">>, []),
    not_found = ?MODULE:get(Ref, <<"abc">>, []),
    true = ?MODULE:is_empty(Ref).

fold_test() -> [{fold_test_Z(), l} || l <- lists:seq(1, 20)].
fold_test_Z() ->
    os:cmd("rm -rf /tmp/erocksdb.fold.test"),
    {ok, Ref} = open("/tmp/erocksdb.fold.test", [{create_if_missing, true}], []),
    ok = ?MODULE:put(Ref, <<"def">>, <<"456">>, []),
    ok = ?MODULE:put(Ref, <<"abc">>, <<"123">>, []),
    ok = ?MODULE:put(Ref, <<"hij">>, <<"789">>, []),
    [{<<"abc">>, <<"123">>},
     {<<"def">>, <<"456">>},
     {<<"hij">>, <<"789">>}] = lists:reverse(fold(Ref, fun({K, V}, Acc) -> [{K, V} | Acc] end,
                                                  [], [])).

fold_keys_test() -> [{fold_keys_test_Z(), l} || l <- lists:seq(1, 20)].
fold_keys_test_Z() ->
    os:cmd("rm -rf /tmp/erocksdb.fold.keys.test"),
    {ok, Ref} = open("/tmp/erocksdb.fold.keys.test", [{create_if_missing, true}], []),
    ok = ?MODULE:put(Ref, <<"def">>, <<"456">>, []),
    ok = ?MODULE:put(Ref, <<"abc">>, <<"123">>, []),
    ok = ?MODULE:put(Ref, <<"hij">>, <<"789">>, []),
    [<<"abc">>, <<"def">>, <<"hij">>] = lists:reverse(fold_keys(Ref,
                                                                fun(K, Acc) -> [K | Acc] end,
                                                                [], [])).

destroy_test() -> [{destroy_test_Z(), l} || l <- lists:seq(1, 20)].
destroy_test_Z() ->
    os:cmd("rm -rf /tmp/erocksdb.destroy.test"),
    {ok, Ref} = open("/tmp/erocksdb.destroy.test", [{create_if_missing, true}], []),
    ok = ?MODULE:put(Ref, <<"def">>, <<"456">>, []),
    {ok, <<"456">>} = ?MODULE:get(Ref, <<"def">>, []),
    close(Ref),
    ok = ?MODULE:destroy("/tmp/erocksdb.destroy.test", []),
    {error, {db_open, _}} = open("/tmp/erocksdb.destroy.test", [{error_if_exists, true}], []).

compression_test() -> [{compression_test_Z(), l} || l <- lists:seq(1, 20)].
compression_test_Z() ->
    CompressibleData = list_to_binary([0 || _X <- lists:seq(1,20)]),
    os:cmd("rm -rf /tmp/erocksdb.compress.0 /tmp/erocksdb.compress.1"),
    {ok, Ref0} = open("/tmp/erocksdb.compress.0", [{create_if_missing, true}],
                      [{compression, none}]),
    [ok = ?MODULE:put(Ref0, <<I:64/unsigned>>, CompressibleData, [{sync, true}]) ||
        I <- lists:seq(1,10)],
    {ok, Ref1} = open("/tmp/erocksdb.compress.1", [{create_if_missing, true}],
                      [{compression, snappy}]),
    [ok = ?MODULE:put(Ref1, <<I:64/unsigned>>, CompressibleData, [{sync, true}]) ||
        I <- lists:seq(1,10)],
    %% Check both of the LOG files created to see if the compression option was correctly
    %% passed down
    MatchCompressOption =
        fun(File, Expected) ->
                {ok, Contents} = file:read_file(File),
                case re:run(Contents, "Options.compression: " ++ Expected) of
                    {match, _} -> match;
                    nomatch -> nomatch
                end
        end,
    Log0Option = MatchCompressOption("/tmp/erocksdb.compress.0/LOG", "0"),
    Log1Option = MatchCompressOption("/tmp/erocksdb.compress.1/LOG", "1"),
    ?assert(Log0Option =:= match andalso Log1Option =:= match).

close_test() -> [{close_test_Z(), l} || l <- lists:seq(1, 20)].
close_test_Z() ->
    os:cmd("rm -rf /tmp/erocksdb.close.test"),
    {ok, Ref} = open("/tmp/erocksdb.close.test", [{create_if_missing, true}], []),
    ?assertEqual(ok, close(Ref)),
    ?assertEqual({error, einval}, close(Ref)).

close_fold_test() -> [{close_fold_test_Z(), l} || l <- lists:seq(1, 20)].
close_fold_test_Z() ->
    os:cmd("rm -rf /tmp/erocksdb.close_fold.test"),
    {ok, Ref} = open("/tmp/erocksdb.close_fold.test", [{create_if_missing, true}], []),
    ok = erocksdb:put(Ref, <<"k">>,<<"v">>,[]),
    ?assertException(throw, {iterator_closed, ok}, % ok is returned by close as the acc
                     erocksdb:fold(Ref, fun(_,_A) -> erocksdb:close(Ref) end, undefined, [])).

-ifdef(EQC).
qc(P) ->
    ?assert(eqc:quickcheck(?QC_OUT(P))).

keys() ->
    eqc_gen:non_empty(list(eqc_gen:non_empty(binary()))).

values() ->
    eqc_gen:non_empty(list(binary())).

ops(Keys, Values) ->
    {oneof([put, delete]), oneof(Keys), oneof(Values)}.

apply_kv_ops([], _Ref, Acc0) ->
    Acc0;
apply_kv_ops([{put, K, V} | Rest], Ref, Acc0) ->
    ok = erocksdb:put(Ref, K, V, []),
    apply_kv_ops(Rest, Ref, orddict:store(K, V, Acc0));
apply_kv_ops([{delete, K, _} | Rest], Ref, Acc0) ->
    ok = erocksdb:delete(Ref, K, []),
    apply_kv_ops(Rest, Ref, orddict:store(K, deleted, Acc0)).

prop_put_delete() ->
    ?LET({Keys, Values}, {keys(), values()},
         ?FORALL(Ops, eqc_gen:non_empty(list(ops(Keys, Values))),
                 begin
                     ?cmd("rm -rf /tmp/erocksdb.putdelete.qc"),
                     {ok, Ref} = erocksdb:open("/tmp/erocksdb.putdelete.qc",
                                               [{create_if_missing, true}], []),
                     Model = apply_kv_ops(Ops, Ref, []),

                     %% Valdiate that all deleted values return not_found
                     F = fun({K, deleted}) ->
                                 ?assertEqual(not_found, erocksdb:get(Ref, K, []));
                            ({K, V}) ->
                                 ?assertEqual({ok, V}, erocksdb:get(Ref, K, []))
                         end,
                     lists:map(F, Model),

                     %% Validate that a fold returns sorted values
                     Actual = lists:reverse(fold(Ref, fun({K, V}, Acc) -> [{K, V} | Acc] end,
                                                 [], [])),
                     ?assertEqual([{K, V} || {K, V} <- Model, V /= deleted],
                                  Actual),
                     ok = erocksdb:close(Ref),
                     true
                 end)).

prop_put_delete_test_() ->
    Timeout1 = 10,
    Timeout2 = 15,
    %% We use the ?ALWAYS(300, ...) wrapper around the second test as a
    %% regression test.
    [{timeout,  3 * Timeout1,
      {"No ?ALWAYS()", fun() -> qc(eqc:testing_time(Timeout1,prop_put_delete())) end}},
     {timeout, 10 * Timeout2,
      {"With ?ALWAYS()", fun() -> qc(eqc:testing_time(Timeout2,?ALWAYS(150,prop_put_delete()))) end}}].
-endif.
-endif.
