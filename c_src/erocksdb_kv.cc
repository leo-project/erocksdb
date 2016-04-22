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
write(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
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
get(
    ErlNifEnv* env,
    int argc,
    const ERL_NIF_TERM argv[])
{
    const ERL_NIF_TERM& dbh_ref    = argv[0];
    ERL_NIF_TERM cf_ref;
    ERL_NIF_TERM key_ref;
    ERL_NIF_TERM opts_ref;

    ReferencePtr<DbObject> db_ptr;
    ReferencePtr<ColumnFamilyObject> cf_ptr;

    rocksdb::ReadOptions *opts = new rocksdb::ReadOptions();

    /// retrieve vars
    db_ptr.assign(DbObject::RetrieveDbObject(env, dbh_ref));
    if(NULL==db_ptr.get())
        return enif_make_badarg(env);


    if(argc  == 3)
    {
        key_ref = argv[1];
        opts_ref = argv[2];
    }
    else
    {
        cf_ref = argv[1];
        key_ref = argv[2];
        opts_ref = argv[3];
        // we use a column family assign the value
        cf_ptr.assign(ColumnFamilyObject::RetrieveColumnFamilyObject(env, cf_ref));
    }


    if(NULL==db_ptr.get()
       || !enif_is_list(env, opts_ref)
       || !enif_is_binary(env, key_ref))
    {
        return enif_make_badarg(env);
    }

    if(NULL == db_ptr->m_Db)
    {
        return error_einval(env);
    }

    if(argc > 3) {
        if(NULL==cf_ptr.get())
            return enif_make_badarg(env);
    }


    ERL_NIF_TERM fold_result;
    fold_result = fold(env, opts_ref, parse_read_option, *opts);
    if(fold_result!=ATOM_OK)
    {
        return enif_make_badarg(env);
    }


    // convert key to string
    rocksdb::Slice key_slice;

    if (!binary_to_slice(env, key_ref, &key_slice))
        return enif_make_badarg(env);


    // get value
    ERL_NIF_TERM value_bin;
    std::string value;
    rocksdb::Status status;
    if(argc==3)
    {
        status = db_ptr->m_Db->Get(*opts, key_slice, &value);
    }
    else
    {
        ColumnFamilyObject* cf = cf_ptr.get();
        status = db_ptr->m_Db->Get(*opts, cf->m_ColumnFamily, key_slice, &value);
    }

    if (!status.ok())
    {
        return ATOM_NOT_FOUND;
    }

    unsigned char* v = enif_make_new_binary(env, value.size(), &value_bin);
    memcpy(v, value.c_str(), value.size());

    return enif_make_tuple2(env, ATOM_OK, value_bin);
}   // get


}
