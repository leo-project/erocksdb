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
#ifndef __EROCKSDB_DETAIL_HPP
    #include "detail.hpp"
#endif

#ifndef INCL_WORKITEMS_H
    #include "workitems.h"
#endif

#include "rocksdb/cache.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/utilities/db_ttl.h"

// error_tuple duplicated in workitems.cc and erocksdb.cc ... how to fix?
static ERL_NIF_TERM error_tuple(ErlNifEnv* env, ERL_NIF_TERM error, rocksdb::Status& status)
{
    ERL_NIF_TERM reason = enif_make_string(env, status.ToString().c_str(),
                                           ERL_NIF_LATIN1);
    return enif_make_tuple2(env, erocksdb::ATOM_ERROR,
                            enif_make_tuple2(env, error, reason));
}


static ERL_NIF_TERM slice_to_binary(ErlNifEnv* env, rocksdb::Slice s)
{
    ERL_NIF_TERM result;
    unsigned char* value = enif_make_new_binary(env, s.size(), &result);
    memcpy(value, s.data(), s.size());
    return result;
}


namespace erocksdb {


/**
 * WorkTask functions
 */


WorkTask::WorkTask(ErlNifEnv *caller_env, ERL_NIF_TERM& caller_ref)
    : terms_set(false), resubmit_work(false)
{
    if (NULL!=caller_env)
    {
        local_env_ = enif_alloc_env();
        caller_ref_term = enif_make_copy(local_env_, caller_ref);
        caller_pid_term = enif_make_pid(local_env_, enif_self(caller_env, &local_pid));
        terms_set=true;
    }   // if
    else
    {
        local_env_=NULL;
        terms_set=false;
    }   // else

    return;

}   // WorkTask::WorkTask


WorkTask::WorkTask(ErlNifEnv *caller_env, ERL_NIF_TERM& caller_ref, DbObject * DbPtr)
    : m_DbPtr(DbPtr), terms_set(false), resubmit_work(false)
{
    if (NULL!=caller_env)
    {
        local_env_ = enif_alloc_env();
        caller_ref_term = enif_make_copy(local_env_, caller_ref);
        caller_pid_term = enif_make_pid(local_env_, enif_self(caller_env, &local_pid));
        terms_set=true;
    }   // if
    else
    {
        local_env_=NULL;
        terms_set=false;
    }   // else

    return;

}   // WorkTask::WorkTask


WorkTask::~WorkTask()
{
    ErlNifEnv * env_ptr;

    // this is likely overkill in the present code, but seemed
    //  important at one time and leaving for safety
    env_ptr=local_env_;
    if (compare_and_swap(&local_env_, env_ptr, (ErlNifEnv *)NULL)
        && NULL!=env_ptr)
    {
        enif_free_env(env_ptr);
    }   // if

    return;

}   // WorkTask::~WorkTask


void
WorkTask::prepare_recycle()
{
    // does not work by default
    resubmit_work=false;
}  // WorkTask::prepare_recycle


void
WorkTask::recycle()
{
    // does not work by default
}   // WorkTask::recycle




/**
 * OpenTask functions
 */

OpenTask::OpenTask(
    ErlNifEnv* caller_env,
    ERL_NIF_TERM& _caller_ref,
    const std::string& db_name_,
    rocksdb::Options *Options_,
    bool use_ttl_,
    int32_t ttl_)
    : WorkTask(caller_env, _caller_ref),
    db_name(db_name_), options(Options_), use_ttl(use_ttl_), ttl(ttl_)
{
}   // OpenTask::OpenTask


work_result
OpenTask::operator()()
{
    DbObject * db_ptr;
    rocksdb::DB *db(0);

    rocksdb::Status status;

    if(!use_ttl)
    {
        status = rocksdb::DB::Open(*options, db_name, &db);
    }
    else
    {
         rocksdb::DBWithTTL* db_with_ttl(0);
         status = rocksdb::DBWithTTL::Open(*options, db_name, &db_with_ttl, ttl);
         db = db_with_ttl;
    }

    if(!status.ok())
        return error_tuple(local_env(), ATOM_ERROR_DB_OPEN, status);

    db_ptr=DbObject::CreateDbObject(db, options);

    // create a resource reference to send erlang
    ERL_NIF_TERM result = enif_make_resource(local_env(), db_ptr);

    // clear the automatic reference from enif_alloc_resource in CreateDbObject
    enif_release_resource(db_ptr);

    return work_result(local_env(), ATOM_OK, result);

}   // OpenTask::operator()



/**
 * MoveTask functions
 */

work_result
MoveTask::operator()()
{
    rocksdb::Iterator* itr = m_ItrWrap->get();

    if(NULL == itr)
        return work_result(local_env(), ATOM_ERROR, ATOM_ITERATOR_CLOSED);

    switch(action)
    {
        case FIRST: itr->SeekToFirst(); break;

        case LAST:  itr->SeekToLast();  break;

        case PREFETCH:
        case NEXT:  if(itr->Valid()) itr->Next(); break;

        case PREV:  if(itr->Valid()) itr->Prev(); break;

        case SEEK:
        {
            rocksdb::Slice key_slice(seek_target);

            itr->Seek(key_slice);
            break;
        }   // case

        default:
            // JFW: note: *not* { ERROR, badarg() } here-- we want the exception:
            // JDB: note: We can't send an exception as a message. It crashes Erlang.
            //            Changing to be {error, badarg}.
            return work_result(local_env(), ATOM_ERROR, ATOM_BADARG);
            break;

    }   // switch


    // who got back first, us or the erlang loop
    if (compare_and_swap(&m_ItrWrap->m_HandoffAtomic, 0, 1))
    {
        // this is prefetch of next iteration.  It returned faster than actual
        //  request to retrieve it.  Stop and wait for erlang to catch up.
        //  (even if this result is an Invalid() )
    }   // if
    else
    {
        // setup next race for the response
        m_ItrWrap->m_HandoffAtomic=0;

        if(itr->Valid())
        {
            if (PREFETCH==action)
                prepare_recycle();

            // erlang is waiting, send message
            if(m_ItrWrap->m_KeysOnly)
                return work_result(local_env(), ATOM_OK, slice_to_binary(local_env(), itr->key()));

            return work_result(local_env(), ATOM_OK,
                               slice_to_binary(local_env(), itr->key()),
                               slice_to_binary(local_env(), itr->value()));
        }   // if
        else
        {
            return work_result(local_env(), ATOM_ERROR, ATOM_INVALID_ITERATOR);
        }   // else

    }   // else

    return(work_result());
}


ErlNifEnv *
MoveTask::local_env()
{
    if (NULL==local_env_)
        local_env_ = enif_alloc_env();

    if (!terms_set)
    {
        caller_ref_term = enif_make_copy(local_env_, m_ItrWrap->itr_ref);
        caller_pid_term = enif_make_pid(local_env_, &local_pid);
        terms_set=true;
    }   // if

    return(local_env_);

}   // MoveTask::local_env


void
MoveTask::prepare_recycle()
{

    resubmit_work=true;

}  // MoveTask::prepare_recycle


void
MoveTask::recycle()
{
    // test for race condition of simultaneous delete & recycle
    if (1<RefInc())
    {
        if (NULL!=local_env_)
            enif_clear_env(local_env_);

        terms_set=false;
        resubmit_work=false;

        // only do this in non-race condition
        RefDec();
    }   // if
    else
    {
        // touch NOTHING
    }   // else

}   // MoveTask::recycle



} // namespace erocksdb


