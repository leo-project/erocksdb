// -------------------------------------------------------------------
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
#include "rocksdb/comparator.h"
#include "rocksdb/env.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/slice_transform.h"

#ifndef INCL_REFOBJECTS_H
    #include "refobjects.h"
#endif

#ifndef ATOMS_H
    #include "atoms.h"
#endif

#ifndef INCL_UTIL_H
    #include "util.h"
#endif


namespace erocksdb {

ERL_NIF_TERM
iterator(
    ErlNifEnv* env,
    int argc,
    const ERL_NIF_TERM argv[])
{
    const ERL_NIF_TERM& dbh_ref     = argv[0];
    const ERL_NIF_TERM& options_ref = argv[1];
    const bool keys_only = ((argc == 3) && (argv[2] == ATOM_KEYS_ONLY));

    rocksdb::ReadOptions *opts = new rocksdb::ReadOptions;

    ReferencePtr<DbObject> db_ptr;
    db_ptr.assign(DbObject::RetrieveDbObject(env, dbh_ref));

    if(NULL==db_ptr.get()
       || !enif_is_list(env, options_ref))
     {
        return enif_make_badarg(env);
     }

    // likely useless
    if(NULL == db_ptr->m_Db)
        return error_einval(env);

    ERL_NIF_TERM fold_result;
    fold_result = fold(env, options_ref, parse_read_option, *opts);
    if(fold_result!=erocksdb::ATOM_OK)
        return enif_make_badarg(env);

    ItrObject * itr_ptr;
    rocksdb::Iterator * iterator;

    iterator = db_ptr->m_Db->NewIterator(*opts);
    itr_ptr = ItrObject::CreateItrObject(db_ptr.get(), iterator, keys_only);

    ERL_NIF_TERM result = enif_make_resource(env, itr_ptr);

    // release reference created during CreateItrObject()
    enif_release_resource(itr_ptr);
    opts=NULL;  // ptr ownership given to ItrObject

    return enif_make_tuple2(env, ATOM_OK, result);
}   // iterator



ERL_NIF_TERM
iterators(
    ErlNifEnv* env,
    int argc,
    const ERL_NIF_TERM argv[])
{

    const ERL_NIF_TERM& dbh_ref     = argv[0];
    const ERL_NIF_TERM& cfs_ref = argv[1];
    const ERL_NIF_TERM& options_ref = argv[2];
    const bool keys_only = ((argc == 4) && (argv[3] == ATOM_KEYS_ONLY));

    rocksdb::ReadOptions *opts = new rocksdb::ReadOptions();

    ReferencePtr<DbObject> db_ptr;
    db_ptr.assign(DbObject::RetrieveDbObject(env, dbh_ref));


    if(NULL==db_ptr.get() ||
       !enif_is_list(env, options_ref) ||
       !enif_is_list(env, cfs_ref))
    {
       return enif_make_badarg(env);
    }

    if(NULL == db_ptr->m_Db)
        return error_einval(env);

    // parse options
    fold(env, options_ref, parse_read_option, *opts);
    std::vector<rocksdb::ColumnFamilyHandle*> column_families;
    std::vector<ColumnFamilyObject*> cf_objects;

    ERL_NIF_TERM head, tail = cfs_ref;
    while(enif_get_list_cell(env, tail, &head, &tail))
    {
        ReferencePtr<ColumnFamilyObject> cf_ptr;
        cf_ptr.assign(ColumnFamilyObject::RetrieveColumnFamilyObject(env, head));
        ColumnFamilyObject* cf = cf_ptr.get();
        column_families.push_back(cf->m_ColumnFamily);
        cf_objects.push_back(cf);
    }

    std::vector<rocksdb::Iterator*> iterators;
    db_ptr->m_Db->NewIterators(*opts, column_families, &iterators);

    ERL_NIF_TERM result = enif_make_list(env, 0);
    try {
        for (size_t i = 0; i < iterators.size(); i++) {
            ItrObject * itr_ptr;
            ColumnFamilyObject* cf = cf_objects[i];
            itr_ptr = ItrObject::CreateItrObject(db_ptr.get(), iterators[i], keys_only, cf);
            ERL_NIF_TERM itr_res = enif_make_resource(env, itr_ptr);
            result = enif_make_list_cell(env, itr_res, result);
            enif_release_resource(itr_ptr);
        }
    } catch (const std::exception& e) {
        // pass through and return nullptr
    }
    opts=NULL;
    ERL_NIF_TERM result_out;
    enif_make_reverse_list(env, result, &result_out);

    return enif_make_tuple2(env, erocksdb::ATOM_OK, result_out);
}

ERL_NIF_TERM
iterator_close(
    ErlNifEnv* env,
    int argc,
    const ERL_NIF_TERM argv[])
{
    ItrObject * itr_ptr;
    ERL_NIF_TERM ret_term;

    ret_term=ATOM_OK;

    itr_ptr=ItrObject::RetrieveItrObject(env, argv[0], true);

    if (NULL!=itr_ptr)
    {
        // set closing flag ... atomic likely unnecessary (but safer)
        ErlRefObject::InitiateCloseRequest(itr_ptr);

        itr_ptr=NULL;

        ret_term=ATOM_OK;
    }   // if
    else
    {
        ret_term=enif_make_badarg(env);
    }   // else

    return(ret_term);

}   // iterator_close

ERL_NIF_TERM
iterator_move(
    ErlNifEnv* env,
    int argc,
    const ERL_NIF_TERM argv[])
{
    const ERL_NIF_TERM& itr_handle_ref   = argv[0];
    const ERL_NIF_TERM& action_or_target = argv[1];


    ReferencePtr<ItrObject> itr_ptr;
    itr_ptr.assign(ItrObject::RetrieveItrObject(env, itr_handle_ref));

    if(NULL==itr_ptr.get())
    {
        return enif_make_badarg(env);
    }

    rocksdb::Iterator* itr = itr_ptr->m_Iterator;



    if(enif_is_atom(env, action_or_target))
    {
        if(ATOM_FIRST == action_or_target) itr->SeekToFirst();
        if(ATOM_LAST == action_or_target) itr->SeekToLast();
        if(ATOM_NEXT == action_or_target) itr->Next();
        if(ATOM_PREV == action_or_target) { itr->Prev(); }
    }
    else
    {
        ErlNifBinary key;
        if(!enif_inspect_binary(env, action_or_target, &key))
        {
            return error_einval(env);
        }

        std::string m_Key;
        m_Key.assign((const char *)key.data, key.size);
        rocksdb::Slice key_slice(m_Key);
        itr->Seek(key_slice);
    }

    if(!itr->Valid())
    {
        return enif_make_tuple2(env, ATOM_ERROR, ATOM_INVALID_ITERATOR);
    }

    rocksdb::Status status = itr->status();

    if(!status.ok())
    {
        return error_tuple(env, ATOM_ERROR, status);
    }

    if(itr_ptr->keys_only)
    {
        return enif_make_tuple2(env, ATOM_OK, slice_to_binary(env, itr->key()));
    }
    else
    {
        return enif_make_tuple3(env, ATOM_OK,
                                slice_to_binary(env, itr->key()),
                                slice_to_binary(env, itr->value()));
    }

}   // iter_move

}