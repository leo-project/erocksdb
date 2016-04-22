// -------------------------------------------------------------------`
// Copyright (c) 2011-2013 Basho Technologies, Inc. All Rights Reserved.
// Copyright (c) 2016 Benoit Chesneau. All Rights Reserved.
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


#include <vector>

#include "erocksdb.h"

#include "rocksdb/db.h"
#include "rocksdb/env.h"

#ifndef INCL_REFOBJECTS_H
    #include "refobjects.h"
#endif

#ifndef ATOMS_H
    #include "atoms.h"
#endif

#ifndef INCL_UTIL_H
    #include "util.h"
#endif


#ifndef INCL_EROCKSB_KV_H
    #include "erocksdb_kv.h"
#endif


ERL_NIF_TERM parse_read_option(ErlNifEnv* env, ERL_NIF_TERM item, rocksdb::ReadOptions& opts)
{
    int arity;
    const ERL_NIF_TERM* option;
    if (enif_get_tuple(env, item, &arity, &option) && 2==arity)
    {
        if (option[0] == erocksdb::ATOM_VERIFY_CHECKSUMS)
            opts.verify_checksums = (option[1] == erocksdb::ATOM_TRUE);
        else if (option[0] == erocksdb::ATOM_FILL_CACHE)
            opts.fill_cache = (option[1] == erocksdb::ATOM_TRUE);
        else if (option[0] == erocksdb::ATOM_ITERATE_UPPER_BOUND)
            // @TODO Who should be the Slice owner?
            ;
        else if (option[0] == erocksdb::ATOM_TAILING)
            opts.tailing = (option[1] == erocksdb::ATOM_TRUE);
        else if (option[0] == erocksdb::ATOM_TOTAL_ORDER_SEEK)
            opts.total_order_seek = (option[1] == erocksdb::ATOM_TRUE);
        else if (option[0] == erocksdb::ATOM_SNAPSHOT)
        {
            erocksdb::ReferencePtr<erocksdb::SnapshotObject> snapshot_ptr;
            snapshot_ptr.assign(erocksdb::SnapshotObject::RetrieveSnapshotObject(env, option[1]));

            if(NULL==snapshot_ptr.get())
                return erocksdb::ATOM_BADARG;

            opts.snapshot = snapshot_ptr->m_Snapshot;
        }
    }

    return erocksdb::ATOM_OK;
}

ERL_NIF_TERM parse_write_option(ErlNifEnv* env, ERL_NIF_TERM item, rocksdb::WriteOptions& opts)
{
    int arity;
    const ERL_NIF_TERM* option;
    if (enif_get_tuple(env, item, &arity, &option) && 2==arity)
    {
        if (option[0] == erocksdb::ATOM_SYNC)
            opts.sync = (option[1] == erocksdb::ATOM_TRUE);
        else if (option[0] == erocksdb::ATOM_DISABLE_WAL)
            opts.disableWAL = (option[1] == erocksdb::ATOM_TRUE);
        else if (option[0] == erocksdb::ATOM_TIMEOUT_HINT_US)
        {
            ErlNifUInt64 timeout_hint_us;
            if (enif_get_uint64(env, option[1], &timeout_hint_us))
                opts.timeout_hint_us = timeout_hint_us;
        }
        else if (option[0] == erocksdb::ATOM_IGNORE_MISSING_COLUMN_FAMILIES)
            opts.ignore_missing_column_families = (option[1] == erocksdb::ATOM_TRUE);
    }

    return erocksdb::ATOM_OK;
}

ERL_NIF_TERM write_batch_item(ErlNifEnv* env, ERL_NIF_TERM item, rocksdb::WriteBatch& batch)
{
    int arity;
    const ERL_NIF_TERM* action;
    if (enif_get_tuple(env, item, &arity, &action) ||
        enif_is_atom(env, item))
    {
        if (item == erocksdb::ATOM_CLEAR)
        {
            batch.Clear();
            return erocksdb::ATOM_OK;
        }

        ErlNifBinary key, value;
        erocksdb::ReferencePtr<erocksdb::ColumnFamilyObject> cf_ptr;

        if (action[0] == erocksdb::ATOM_PUT)
        {
            if(arity == 3  &&
               enif_inspect_binary(env, action[1], &key) &&
               enif_inspect_binary(env, action[2], &value))
            {
                rocksdb::Slice key_slice((const char*)key.data, key.size);
                rocksdb::Slice value_slice((const char*)value.data, value.size);
                batch.Put(key_slice, value_slice);
                return erocksdb::ATOM_OK;
            }
            else
            {
                const ERL_NIF_TERM& cf_ref = action[1];
                cf_ptr.assign(erocksdb::ColumnFamilyObject::RetrieveColumnFamilyObject(env, cf_ref));
                erocksdb::ColumnFamilyObject* cf = cf_ptr.get();

                if(NULL!=cf_ptr.get() &&
                   enif_inspect_binary(env, action[2], &key) &&
                   enif_inspect_binary(env, action[3], &value))
                {
                    rocksdb::Slice key_slice((const char*)key.data, key.size);
                    rocksdb::Slice value_slice((const char*)value.data, value.size);
                    batch.Put(cf->m_ColumnFamily, key_slice, value_slice);
                    return erocksdb::ATOM_OK;
                }
            }
        }

        if (action[0] == erocksdb::ATOM_DELETE)
        {
            if(arity == 2 && enif_inspect_binary(env, action[1], &key))
            {
                rocksdb::Slice key_slice((const char*)key.data, key.size);
                batch.Delete(key_slice);
                return erocksdb::ATOM_OK;
            }
            else
            {
                const ERL_NIF_TERM& cf_ref = action[1];
                cf_ptr.assign(erocksdb::ColumnFamilyObject::RetrieveColumnFamilyObject(env, cf_ref));
                erocksdb::ColumnFamilyObject* cf = cf_ptr.get();

                if(NULL!=cf_ptr.get() && enif_inspect_binary(env, action[2], &key))
                {
                    rocksdb::Slice key_slice((const char*)key.data, key.size);
                    batch.Delete(cf->m_ColumnFamily, key_slice);
                    return erocksdb::ATOM_OK;
                }
            }
        }
    }

    // Failed to match clear/put/delete; return the failing item
    return item;
}


namespace erocksdb {

ERL_NIF_TERM
Write(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    const ERL_NIF_TERM& handle_ref = argv[0];
    const ERL_NIF_TERM& action_ref = argv[1];
    const ERL_NIF_TERM& opts_ref   = argv[2];

    ReferencePtr<DbObject> db_ptr;

    db_ptr.assign(DbObject::RetrieveDbObject(env, handle_ref));

    if(NULL==db_ptr.get()
       || !enif_is_list(env, action_ref)
       || !enif_is_list(env, opts_ref))
    {
        return enif_make_badarg(env);
    }

    // is this even possible?
    if(NULL == db_ptr->m_Db)
    {
        return error_einval(env);
    }

    // Construct a write batch:
    rocksdb::WriteBatch* batch = new rocksdb::WriteBatch;
    ERL_NIF_TERM result = fold(env, argv[1], write_batch_item, *batch);
    if(ATOM_OK != result)
    {
        return enif_make_tuple2(env, ATOM_ERROR,
                enif_make_tuple2(env, ATOM_BAD_WRITE_ACTION, result));
    }   // if

    rocksdb::WriteOptions* opts = new rocksdb::WriteOptions;
    fold(env, argv[2], parse_write_option, *opts);

    rocksdb::Status status = db_ptr->m_Db->Write(*opts, batch);

    delete batch;
    delete opts;

    if (status.ok())
    {
        return ATOM_OK;
    }

    return error_tuple(env, ATOM_ERROR, status);
}   // write


ERL_NIF_TERM
Get(
    ErlNifEnv* env,
    int argc,
    const ERL_NIF_TERM argv[])
{
    ReferencePtr<DbObject> db_ptr;
    if(!enif_get_db(env, argv[0], &db_ptr))
        return enif_make_badarg(env);

    int i = 1;
    if(argc == 4)
        i = 2;

    rocksdb::Slice key;
    if(!binary_to_slice(env, argv[i], &key))
    {
        return enif_make_badarg(env);
    }

    rocksdb::ReadOptions *opts = new rocksdb::ReadOptions();
    fold(env, argv[i+1], parse_read_option, *opts);

    rocksdb::Status status;
    std::string value;
    if(argc==4)
    {
        ReferencePtr<ColumnFamilyObject> cf_ptr;
        if(!enif_get_cf(env, argv[1], &cf_ptr))
            return enif_make_badarg(env);

        status = db_ptr->m_Db->Get(*opts, cf_ptr->m_ColumnFamily, key, &value);
    }
    else
    {
        status = db_ptr->m_Db->Get(*opts, key, &value);
    }

    if (!status.ok())
    {
        return ATOM_NOT_FOUND;
    }

    ERL_NIF_TERM value_bin;
    unsigned char* v = enif_make_new_binary(env, value.size(), &value_bin);
    memcpy(v, value.c_str(), value.size());

    return enif_make_tuple2(env, ATOM_OK, value_bin);
}   // get


ERL_NIF_TERM
Put(
    ErlNifEnv* env,
    int argc,
    const ERL_NIF_TERM argv[])
{
    ReferencePtr<DbObject> db_ptr;
    if(!enif_get_db(env, argv[0], &db_ptr))
        return enif_make_badarg(env);

    int i = 1;
    if(argc == 5)
        i = 2;

    rocksdb::Slice key;
    rocksdb::Slice value;
    if(!binary_to_slice(env, argv[i], &key) ||
            !binary_to_slice(env, argv[i+1], &value))
    {
        return enif_make_badarg(env);
    }

    rocksdb::WriteOptions* opts = new rocksdb::WriteOptions;
    fold(env, argv[i+2], parse_write_option, *opts);

    rocksdb::Status status;
    if(argc==5)
    {
        ReferencePtr<ColumnFamilyObject> cf_ptr;
        if(!enif_get_cf(env, argv[1], &cf_ptr))
            return enif_make_badarg(env);

        status = db_ptr->m_Db->Put(*opts, cf_ptr->m_ColumnFamily, key, value);
    }
    else
    {
        status = db_ptr->m_Db->Put(*opts, key, value);
    }

    if(status.ok())
    {
        return ATOM_OK;
    }

    return error_tuple(env, ATOM_ERROR, status);
}

ERL_NIF_TERM
Delete(
    ErlNifEnv* env,
    int argc,
    const ERL_NIF_TERM argv[])
{
    ReferencePtr<DbObject> db_ptr;
    if(!enif_get_db(env, argv[0], &db_ptr))
        return enif_make_badarg(env);

    int i = 1;
    if(argc == 4)
        i = 2;

    rocksdb::Slice key;
    if(!binary_to_slice(env, argv[i], &key))
    {
        return enif_make_badarg(env);
    }

    rocksdb::WriteOptions* opts = new rocksdb::WriteOptions;
    fold(env, argv[i+1], parse_write_option, *opts);

    rocksdb::Status status;
    if(argc==4)
    {
        ReferencePtr<ColumnFamilyObject> cf_ptr;
        if(!enif_get_cf(env, argv[1], &cf_ptr))
            return enif_make_badarg(env);

        status = db_ptr->m_Db->Delete(*opts, cf_ptr->m_ColumnFamily, key);
    }
    else
    {
        status = db_ptr->m_Db->Delete(*opts, key);
    }

    if(status.ok())
    {
        return ATOM_OK;
    }

    return error_tuple(env, ATOM_ERROR, status);
}



}
