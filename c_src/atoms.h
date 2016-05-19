// -------------------------------------------------------------------
//
// eleveldb: Erlang Wrapper for LevelDB (http://code.google.com/p/leveldb/)
//
// Copyright (c) 2011-2013 Basho Technologies, Inc. All Rights Reserved.
//
// This file is provided to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain
// a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// -------------------------------------------------------------------

#ifndef ATOMS_H
#define ATOMS_H

namespace erocksdb {

// Atoms (initialized in on_load)
// Related to Erlang
extern ERL_NIF_TERM ATOM_TRUE;
extern ERL_NIF_TERM ATOM_FALSE;
extern ERL_NIF_TERM ATOM_OK;
extern ERL_NIF_TERM ATOM_ERROR;
extern ERL_NIF_TERM ATOM_EINVAL;
extern ERL_NIF_TERM ATOM_BADARG;
extern ERL_NIF_TERM ATOM_NOT_FOUND;

// Related to CFOptions
extern ERL_NIF_TERM ATOM_BLOCK_CACHE_SIZE_MB_FOR_POINT_LOOKUP;
extern ERL_NIF_TERM ATOM_MEMTABLE_MEMORY_BUDGET;
extern ERL_NIF_TERM ATOM_WRITE_BUFFER_SIZE;
extern ERL_NIF_TERM ATOM_MAX_WRITE_BUFFER_NUMBER;
extern ERL_NIF_TERM ATOM_MIN_WRITE_BUFFER_NUMBER_TO_MERGE;
extern ERL_NIF_TERM ATOM_COMPRESSION;
extern ERL_NIF_TERM ATOM_NUM_LEVELS;
extern ERL_NIF_TERM ATOM_LEVEL0_FILE_NUM_COMPACTION_TRIGGER;
extern ERL_NIF_TERM ATOM_LEVEL0_SLOWDOWN_WRITES_TRIGGER;
extern ERL_NIF_TERM ATOM_LEVEL0_STOP_WRITES_TRIGGER;
extern ERL_NIF_TERM ATOM_MAX_MEM_COMPACTION_LEVEL;
extern ERL_NIF_TERM ATOM_TARGET_FILE_SIZE_BASE;
extern ERL_NIF_TERM ATOM_TARGET_FILE_SIZE_MULTIPLIER;
extern ERL_NIF_TERM ATOM_MAX_BYTES_FOR_LEVEL_BASE;
extern ERL_NIF_TERM ATOM_MAX_BYTES_FOR_LEVEL_MULTIPLIER;
extern ERL_NIF_TERM ATOM_EXPANDED_COMPACTION_FACTOR;
extern ERL_NIF_TERM ATOM_SOURCE_COMPACTION_FACTOR;
extern ERL_NIF_TERM ATOM_MAX_GRANDPARENT_OVERLAP_FACTOR;
extern ERL_NIF_TERM ATOM_SOFT_RATE_LIMIT;
extern ERL_NIF_TERM ATOM_HARD_RATE_LIMIT;
extern ERL_NIF_TERM ATOM_ARENA_BLOCK_SIZE;
extern ERL_NIF_TERM ATOM_DISABLE_AUTO_COMPACTIONS;
extern ERL_NIF_TERM ATOM_PURGE_REDUNDANT_KVS_WHILE_FLUSH;
extern ERL_NIF_TERM ATOM_COMPACTION_STYLE;
extern ERL_NIF_TERM ATOM_VERIFY_CHECKSUMS_IN_COMPACTION;
extern ERL_NIF_TERM ATOM_FILTER_DELETES;
extern ERL_NIF_TERM ATOM_MAX_SEQUENTIAL_SKIP_IN_ITERATIONS;
extern ERL_NIF_TERM ATOM_INPLACE_UPDATE_SUPPORT;
extern ERL_NIF_TERM ATOM_INPLACE_UPDATE_NUM_LOCKS;
extern ERL_NIF_TERM ATOM_TABLE_FACTORY_BLOCK_CACHE_SIZE;
extern ERL_NIF_TERM ATOM_IN_MEMORY_MODE;

// Related to DBOptions
extern ERL_NIF_TERM ATOM_TOTAL_THREADS;
extern ERL_NIF_TERM ATOM_CREATE_IF_MISSING;
extern ERL_NIF_TERM ATOM_CREATE_MISSING_COLUMN_FAMILIES;
extern ERL_NIF_TERM ATOM_ERROR_IF_EXISTS;
extern ERL_NIF_TERM ATOM_PARANOID_CHECKS;
extern ERL_NIF_TERM ATOM_MAX_OPEN_FILES;
extern ERL_NIF_TERM ATOM_MAX_TOTAL_WAL_SIZE;
extern ERL_NIF_TERM ATOM_DISABLE_DATA_SYNC;
extern ERL_NIF_TERM ATOM_USE_FSYNC;
extern ERL_NIF_TERM ATOM_DB_PATHS;
extern ERL_NIF_TERM ATOM_DB_LOG_DIR;
extern ERL_NIF_TERM ATOM_WAL_DIR;
extern ERL_NIF_TERM ATOM_DELETE_OBSOLETE_FILES_PERIOD_MICROS;
extern ERL_NIF_TERM ATOM_MAX_BACKGROUND_COMPACTIONS;
extern ERL_NIF_TERM ATOM_MAX_BACKGROUND_FLUSHES;
extern ERL_NIF_TERM ATOM_MAX_LOG_FILE_SIZE;
extern ERL_NIF_TERM ATOM_LOG_FILE_TIME_TO_ROLL;
extern ERL_NIF_TERM ATOM_KEEP_LOG_FILE_NUM;
extern ERL_NIF_TERM ATOM_MAX_MANIFEST_FILE_SIZE;
extern ERL_NIF_TERM ATOM_TABLE_CACHE_NUMSHARDBITS;
extern ERL_NIF_TERM ATOM_WAL_TTL_SECONDS;
extern ERL_NIF_TERM ATOM_WAL_SIZE_LIMIT_MB;
extern ERL_NIF_TERM ATOM_MANIFEST_PREALLOCATION_SIZE;
extern ERL_NIF_TERM ATOM_ALLOW_OS_BUFFER;
extern ERL_NIF_TERM ATOM_ALLOW_MMAP_READS;
extern ERL_NIF_TERM ATOM_ALLOW_MMAP_WRITES;
extern ERL_NIF_TERM ATOM_IS_FD_CLOSE_ON_EXEC;
extern ERL_NIF_TERM ATOM_SKIP_LOG_ERROR_ON_RECOVERY;
extern ERL_NIF_TERM ATOM_STATS_DUMP_PERIOD_SEC;
extern ERL_NIF_TERM ATOM_ADVISE_RANDOM_ON_OPEN;
extern ERL_NIF_TERM ATOM_ACCESS_HINT;
extern ERL_NIF_TERM ATOM_USE_ADAPTIVE_MUTEX;
extern ERL_NIF_TERM ATOM_BYTES_PER_SYNC;

// Related to Read Options
extern ERL_NIF_TERM ATOM_VERIFY_CHECKSUMS;
extern ERL_NIF_TERM ATOM_FILL_CACHE;
extern ERL_NIF_TERM ATOM_ITERATE_UPPER_BOUND;
extern ERL_NIF_TERM ATOM_TAILING;
extern ERL_NIF_TERM ATOM_TOTAL_ORDER_SEEK;
extern ERL_NIF_TERM ATOM_SNAPSHOT;
extern ERL_NIF_TERM ATOM_BAD_SNAPSHOT;


// Related to Write Options
extern ERL_NIF_TERM ATOM_SYNC;
extern ERL_NIF_TERM ATOM_DISABLE_WAL;
extern ERL_NIF_TERM ATOM_TIMEOUT_HINT_US;
extern ERL_NIF_TERM ATOM_IGNORE_MISSING_COLUMN_FAMILIES;

// Related to Write Actions 
extern ERL_NIF_TERM ATOM_CLEAR;
extern ERL_NIF_TERM ATOM_PUT;
extern ERL_NIF_TERM ATOM_DELETE;


// Related to Iterator Actions
extern ERL_NIF_TERM ATOM_FIRST;
extern ERL_NIF_TERM ATOM_LAST;
extern ERL_NIF_TERM ATOM_NEXT;
extern ERL_NIF_TERM ATOM_PREV;

// Related to Iterator Value to be retrieved
extern ERL_NIF_TERM ATOM_KEYS_ONLY;

// Related to Access Hint
extern ERL_NIF_TERM ATOM_ACCESS_HINT_NORMAL;
extern ERL_NIF_TERM ATOM_ACCESS_HINT_SEQUENTIAL;
extern ERL_NIF_TERM ATOM_ACCESS_HINT_WILLNEED;
extern ERL_NIF_TERM ATOM_ACCESS_HINT_NONE;

// Related to Compression Type
extern ERL_NIF_TERM ATOM_COMPRESSION_TYPE_SNAPPY;
extern ERL_NIF_TERM ATOM_COMPRESSION_TYPE_ZLIB;
extern ERL_NIF_TERM ATOM_COMPRESSION_TYPE_BZIP2;
extern ERL_NIF_TERM ATOM_COMPRESSION_TYPE_LZ4;
extern ERL_NIF_TERM ATOM_COMPRESSION_TYPE_LZ4H;
extern ERL_NIF_TERM ATOM_COMPRESSION_TYPE_NONE;

// Related to Compaction Style
extern ERL_NIF_TERM ATOM_COMPACTION_STYLE_LEVEL;
extern ERL_NIF_TERM ATOM_COMPACTION_STYLE_UNIVERSAL;
extern ERL_NIF_TERM ATOM_COMPACTION_STYLE_FIFO;
extern ERL_NIF_TERM ATOM_COMPACTION_STYLE_NONE;

// Related to Error Codes
extern ERL_NIF_TERM ATOM_ERROR_DB_OPEN;
extern ERL_NIF_TERM ATOM_ERROR_DB_PUT;
extern ERL_NIF_TERM ATOM_ERROR_DB_DELETE;
extern ERL_NIF_TERM ATOM_ERROR_DB_WRITE;
extern ERL_NIF_TERM ATOM_ERROR_DB_DESTROY;
extern ERL_NIF_TERM ATOM_ERROR_DB_REPAIR;
extern ERL_NIF_TERM ATOM_BAD_WRITE_ACTION;
extern ERL_NIF_TERM ATOM_KEEP_RESOURCE_FAILED;
extern ERL_NIF_TERM ATOM_ITERATOR_CLOSED;
extern ERL_NIF_TERM ATOM_INVALID_ITERATOR;

// Related to NIF initialize parameters
extern ERL_NIF_TERM ATOM_WRITE_THREADS;

}   // namespace erocksdb


// Erlang helpers:

ERL_NIF_TERM error_einval(ErlNifEnv* env);

template <typename Acc> ERL_NIF_TERM fold(ErlNifEnv* env, ERL_NIF_TERM list,
                                          ERL_NIF_TERM(*fun)(ErlNifEnv*, ERL_NIF_TERM, Acc&),
                                          Acc& acc)
{
    ERL_NIF_TERM head, tail = list;
    while (enif_get_list_cell(env, tail, &head, &tail))
    {
        ERL_NIF_TERM result = fun(env, head, acc);
        if (result != erocksdb::ATOM_OK)
        {
            return result;
        }
    }

    return erocksdb::ATOM_OK;
}





#endif // ATOMS_H
