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

#ifndef INCL_REFOBJECTS_H
    #include "refobjects.h"
#endif


#include "rocksdb/transaction_log.h"
#include "rocksdb/cache.h"
#include "rocksdb/filter_policy.h"


namespace erocksdb {

/**
* RefObject Functions
*/

RefObject::RefObject()
        : m_RefCount(0) {
}   // RefObject::RefObject


RefObject::~RefObject() {
}   // RefObject::~RefObject


uint32_t
RefObject::RefInc() {

    return (erocksdb::inc_and_fetch(&m_RefCount));

}   // RefObject::RefInc


uint32_t
RefObject::RefDec() {
    uint32_t current_refs;

    current_refs = erocksdb::dec_and_fetch(&m_RefCount);
    if (0 == current_refs)
        delete this;

    return (current_refs);

}   // RefObject::RefDec


/**
* Erlang reference object
*/

ErlRefObject::ErlRefObject()
        : m_CloseRequested(0) {
    pthread_mutexattr_t attr;

    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&m_CloseMutex, &attr);
    pthread_cond_init(&m_CloseCond, NULL);
    pthread_mutexattr_destroy(&attr);

    return;

}   // ErlRefObject::ErlRefObject


ErlRefObject::~ErlRefObject() {

    pthread_mutex_lock(&m_CloseMutex);
    m_CloseRequested = 3;
    pthread_cond_broadcast(&m_CloseCond);
    pthread_mutex_unlock(&m_CloseMutex);

    // DO NOT DESTROY m_CloseMutex or m_CloseCond here

}   // ErlRefObject::~ErlRefObject


bool
ErlRefObject::InitiateCloseRequest(
        ErlRefObject *Object) {
    bool ret_flag;

    ret_flag = false;

    // special handling since destructor may have been called
    if (NULL != Object && 0 == Object->m_CloseRequested)
        ret_flag = compare_and_swap(&Object->m_CloseRequested, 0, 1);

    // vtable is still good, this thread is initiating close
    //   ask object to clean-up
    if (ret_flag) {
        Object->Shutdown();
    }   // if

    return (ret_flag);

}   // ErlRefObject::InitiateCloseRequest


void
ErlRefObject::AwaitCloseAndDestructor(
        ErlRefObject *Object) {
    // NOTE:  it is possible, actually likely, that this
    //        routine is called AFTER the destructor is called
    //        Don't panic.

    if (NULL != Object) {
        // quick test if any work pending
        if (3 != Object->m_CloseRequested) {
            pthread_mutex_lock(&Object->m_CloseMutex);

            // retest after mutex helc
            while (3 != Object->m_CloseRequested) {
                pthread_cond_wait(&Object->m_CloseCond, &Object->m_CloseMutex);
            }   // while
            pthread_mutex_unlock(&Object->m_CloseMutex);
        }   // if

        pthread_mutex_destroy(&Object->m_CloseMutex);
        pthread_cond_destroy(&Object->m_CloseCond);
    }   // if

    return;

}   // ErlRefObject::AwaitCloseAndDestructor


uint32_t
ErlRefObject::RefDec() {
    uint32_t cur_count;

    cur_count = erocksdb::dec_and_fetch(&m_RefCount);

    // this the last active after close requested?
    //  (atomic swap should be unnecessary ... but going for safety)
    if (0 == cur_count && compare_and_swap(&m_CloseRequested, 1, 2)) {
        // deconstruct, but let erlang deallocate memory later
        this->~ErlRefObject();
    }   // if

    return (cur_count);

}   // DbObject::RefDec



/**
* DbObject Functions
*/

ErlNifResourceType *DbObject::m_Db_RESOURCE(NULL);


void
DbObject::CreateDbObjectType(
        ErlNifEnv *Env) {
    ErlNifResourceFlags flags = (ErlNifResourceFlags)(ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER);

    m_Db_RESOURCE = enif_open_resource_type(Env, NULL, "erocksdb_DbObject",
                                            &DbObject::DbObjectResourceCleanup,
                                            flags, NULL);

    return;

}   // DbObject::CreateDbObjectType


DbObject *
DbObject::CreateDbObject(
        rocksdb::DB *Db,
        rocksdb::Options *Options) {
    DbObject *ret_ptr;
    void *alloc_ptr;

    // the alloc call initializes the reference count to "one"
    alloc_ptr = enif_alloc_resource(m_Db_RESOURCE, sizeof(DbObject));
    ret_ptr = new(alloc_ptr) DbObject(Db, Options);

    // manual reference increase to keep active until "close" called
    //  only inc local counter, leave erl ref count alone ... will force
    //  erlang to call us if process holding ref dies
    ret_ptr->RefInc();

    // see OpenTask::operator() for release of reference count

    return (ret_ptr);

}   // DbObject::CreateDbObject

DbObject *
DbObject::RetrieveDbObject(
        ErlNifEnv *Env,
        const ERL_NIF_TERM &DbTerm) {
    DbObject *ret_ptr;

    ret_ptr = NULL;

    if (enif_get_resource(Env, DbTerm, m_Db_RESOURCE, (void **) &ret_ptr)) {
        // has close been requested?
        if (ret_ptr->m_CloseRequested) {
            // object already closing
            ret_ptr = NULL;
        }   // else
    }   // if

    return (ret_ptr);

}   // DbObject::RetrieveDbObject


void
DbObject::DbObjectResourceCleanup(
        ErlNifEnv *Env,
        void *Arg) {
    DbObject *db_ptr;

    db_ptr = (DbObject *) Arg;

    // YES, the destructor may already have been called
    InitiateCloseRequest(db_ptr);

    // YES, the destructor may already have been called
    AwaitCloseAndDestructor(db_ptr);

    return;

}   // DbObject::DbObjectResourceCleanup


DbObject::DbObject(
        rocksdb::DB *DbPtr,
        rocksdb::Options *Options)
        : m_Db(DbPtr), m_DbOptions(Options) { }   // DbObject::DbObject

DbObject::~DbObject() {

    // close the db
    delete m_Db;
    m_Db = NULL;

    if (NULL != m_DbOptions) {
        delete m_DbOptions;
        m_DbOptions = NULL;
    }   // if



    // do not clean up m_CloseMutex and m_CloseCond

    return;

}   // DbObject::~DbObject


void
DbObject::Shutdown() {
#if 1
    bool again;
    ItrObject *itr_ptr;
    SnapshotObject *snapshot_ptr;
    ColumnFamilyObject *column_family_ptr;
    TLogItrObject *tlog_ptr;

    do {
        again = false;
        itr_ptr = NULL;

        // lock the ItrList
        {
            MutexLock lock(m_ItrMutex);

            if (!m_ItrList.empty()) {
                again = true;
                itr_ptr = m_ItrList.front();
                m_ItrList.pop_front();
            }   // if
        }

        // must be outside lock so ItrObject can attempt
        //  RemoveReference
        if (again)
            ItrObject::InitiateCloseRequest(itr_ptr);

    } while (again);

    // clean snapshots linked to the database object
    again = true;
    do {
        again = false;
        snapshot_ptr = NULL;

        // lock the SnapshotList
        {
            MutexLock lock(m_SnapshotMutex);

            if (!m_SnapshotList.empty()) {
                again = true;
                snapshot_ptr = m_SnapshotList.front();
                m_SnapshotList.pop_front();
            }   // if
        }

        // must be outside lock so SnapshotObject can attempt
        //  RemoveReference
        if (again)
            SnapshotObject::InitiateCloseRequest(snapshot_ptr);

    } while (again);

    // clean columns families
    again = true;
    do {
        again = false;
        column_family_ptr = NULL;

        // lock the SnapshotList
        {
            MutexLock lock(m_ColumnFamilyMutex);

            if (!m_ColumnFamilyList.empty()) {
                again = true;
                column_family_ptr = m_ColumnFamilyList.front();
                m_ColumnFamilyList.pop_front();
            }   // if
        }

        // must be outside lock so SnapshotObject can attempt
        //  RemoveReference
        if (again)
            ColumnFamilyObject::InitiateCloseRequest(column_family_ptr);

    } while (again);

    // clean transaction log iterators
    again = true;
    do {
        again = false;
        tlog_ptr = NULL;

        // lock the SnapshotList
        {
            MutexLock lock(m_TLogItrMutex);

            if (!m_TLogItrList.empty()) {
                again = true;
                tlog_ptr = m_TLogItrList.front();
                m_TLogItrList.pop_front();
            }   // if
        }

        // must be outside lock so SnapshotObject can attempt
        //  RemoveReference
        if (again)
            TLogItrObject::InitiateCloseRequest(tlog_ptr);

    } while (again);

#endif

    RefDec();

    return;

}   // DbObject::Shutdown


void
DbObject::AddColumnFamilyReference(
        ColumnFamilyObject *ColumnFamilyPtr) {
    MutexLock lock(m_ColumnFamilyMutex);

    m_ColumnFamilyList.push_back(ColumnFamilyPtr);

    return;

}   // DbObject::ColumnFamilyReference


void
DbObject::RemoveColumnFamilyReference(
        ColumnFamilyObject *ColumnFamilyPtr) {
    MutexLock lock(m_ColumnFamilyMutex);

    m_ColumnFamilyList.remove(ColumnFamilyPtr);

    return;

}   // DbObject::RemoveColumnFamilyReference

void
DbObject::AddReference(
        ItrObject *ItrPtr) {
    MutexLock lock(m_ItrMutex);
    m_ItrList.push_back(ItrPtr);
    return;
}   // DbObject::AddReference


void
DbObject::RemoveReference(
        ItrObject *ItrPtr) {
    MutexLock lock(m_ItrMutex);

    m_ItrList.remove(ItrPtr);

    return;

}   // DbObject::RemoveReference

void
DbObject::AddSnapshotReference(
        SnapshotObject *SnapshotPtr) {
    MutexLock lock(m_SnapshotMutex);

    m_SnapshotList.push_back(SnapshotPtr);

    return;

}   // DbObject::AddSnapshotReference


void
DbObject::RemoveSnapshotReference(
        SnapshotObject *SnapshotPtr) {
    MutexLock lock(m_SnapshotMutex);

    m_SnapshotList.remove(SnapshotPtr);

    return;

}   // DbObject::RemoveSnapshotReference



void
DbObject::AddTLogReference(TLogItrObject *TLogItrPtr) {
    MutexLock lock(m_TLogItrMutex);

    m_TLogItrList.push_back(TLogItrPtr);

    return;

}   // DbObject::AddTLogReference


void
DbObject::RemoveTLogReference(TLogItrObject *TLogItrPtr) {
    MutexLock lock(m_TLogItrMutex);

    m_TLogItrList.remove(TLogItrPtr);

    return;

}   // DbObject::RemoveTLogReference

/**
* ColumnFamily object
*/

ErlNifResourceType *ColumnFamilyObject::m_ColumnFamily_RESOURCE(NULL);


void
ColumnFamilyObject::CreateColumnFamilyObjectType(
        ErlNifEnv *Env) {
    ErlNifResourceFlags flags = (ErlNifResourceFlags)(ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER);

    m_ColumnFamily_RESOURCE = enif_open_resource_type(Env, NULL, "erocksdb_ColumnFamilyObject",
                                                      &ColumnFamilyObject::ColumnFamilyObjectResourceCleanup,
                                                      flags, NULL);

    return;

}   // ColumnFamilyObject::CreateSnapshotObjectType


ColumnFamilyObject *
ColumnFamilyObject::CreateColumnFamilyObject(
        DbObject *DbPtr,
        rocksdb::ColumnFamilyHandle *Handle) {
    ColumnFamilyObject *ret_ptr;
    void *alloc_ptr;

    // the alloc call initializes the reference count to "one"
    alloc_ptr = enif_alloc_resource(m_ColumnFamily_RESOURCE, sizeof(ColumnFamilyObject));
    ret_ptr = new(alloc_ptr) ColumnFamilyObject(DbPtr, Handle);

    // manual reference increase to keep active until "close" called
    //  only inc local counter
    ret_ptr->RefInc();

    // see IterTask::operator() for release of reference count

    return (ret_ptr);

}   // ColumnFamilyObject::ColumnFamilySnapshotObject


ColumnFamilyObject *
ColumnFamilyObject::RetrieveColumnFamilyObject(
        ErlNifEnv *Env,
        const ERL_NIF_TERM &ColumnFamilyTerm) {
    ColumnFamilyObject *ret_ptr;

    ret_ptr = NULL;

    if (enif_get_resource(Env, ColumnFamilyTerm, m_ColumnFamily_RESOURCE, (void **) &ret_ptr)) {
        // has close been requested?
        if (ret_ptr->m_CloseRequested) {
            // object already closing
            ret_ptr = NULL;
        }   // else
    }   // if

    return (ret_ptr);

}   // ColumnFamilyObject::RetrieveColumnFamilyObject


void
ColumnFamilyObject::ColumnFamilyObjectResourceCleanup(
        ErlNifEnv *Env,
        void *Arg) {
    ColumnFamilyObject *handle_ptr;

    handle_ptr = (ColumnFamilyObject *) Arg;


    // vtable for snapshot_ptr could be invalid if close already
    //  occurred
    InitiateCloseRequest(handle_ptr);

    // YES this can be called after snapshot_ptr destructor.  Don't panic.
    AwaitCloseAndDestructor(handle_ptr);

    return;

}   // ColumnFamilyObject::ColumnFamilyObjectResourceCleanup


ColumnFamilyObject::ColumnFamilyObject(
        DbObject *DbPtr,
        rocksdb::ColumnFamilyHandle *Handle)
        : m_ColumnFamily(Handle), m_DbPtr(DbPtr) {
    if (NULL != DbPtr)
        DbPtr->AddColumnFamilyReference(this);

}   // ColumnFamilyObject::ColumnFamilyObject


ColumnFamilyObject::~ColumnFamilyObject() {

    m_ColumnFamily = NULL;

    if (NULL != m_DbPtr.get())
        m_DbPtr->RemoveColumnFamilyReference(this);

    return;

}   // ColumnFamilyObject::~ColumnFamilyObject


void
ColumnFamilyObject::Shutdown() {
#if 1
    bool again;
    ItrObject *itr_ptr;

    do {
        again = false;
        itr_ptr = NULL;

        // lock the ItrList
        {
            MutexLock lock(m_ItrMutex);

            if (!m_ItrList.empty()) {
                again = true;
                itr_ptr = m_ItrList.front();
                m_ItrList.pop_front();
            }   // if
        }

        // must be outside lock so ItrObject can attempt
        //  RemoveReference
        if (again)
            ItrObject::InitiateCloseRequest(itr_ptr);

    } while (again);
#endif

    RefDec();
    return;
}   // ColumnFamilyObject::CloseRequest


void
ColumnFamilyObject::AddItrReference(
        ItrObject *ItrPtr) {
    MutexLock lock(m_ItrMutex);
    m_ItrList.push_back(ItrPtr);
    return;
}   // DbObject::AddReference


void
ColumnFamilyObject::RemoveItrReference(
        ItrObject *ItrPtr) {
    MutexLock lock(m_ItrMutex);

    m_ItrList.remove(ItrPtr);

    return;

}   // DbObject::RemoveReference

/**
* snapshot object
*/

ErlNifResourceType *SnapshotObject::m_DbSnapshot_RESOURCE(NULL);


void
SnapshotObject::CreateSnapshotObjectType(
        ErlNifEnv *Env) {
    ErlNifResourceFlags flags = (ErlNifResourceFlags)(ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER);

    m_DbSnapshot_RESOURCE = enif_open_resource_type(Env, NULL, "erocksdb_SnapshotObject",
                                                    &SnapshotObject::SnapshotObjectResourceCleanup,
                                                    flags, NULL);

    return;

}   // SnapshotObject::CreateSnapshotObjectType


SnapshotObject *
SnapshotObject::CreateSnapshotObject(
        DbObject *DbPtr,
        const rocksdb::Snapshot *Snapshot) {
    SnapshotObject *ret_ptr;
    void *alloc_ptr;

    // the alloc call initializes the reference count to "one"
    alloc_ptr = enif_alloc_resource(m_DbSnapshot_RESOURCE, sizeof(SnapshotObject));

    ret_ptr = new(alloc_ptr) SnapshotObject(DbPtr, Snapshot);

    // manual reference increase to keep active until "close" called
    //  only inc local counter
    ret_ptr->RefInc();

    // see IterTask::operator() for release of reference count

    return (ret_ptr);

}   // SnapshotObject::CreateSnapshotObject


SnapshotObject *
SnapshotObject::RetrieveSnapshotObject(
        ErlNifEnv *Env,
        const ERL_NIF_TERM &SnapshotTerm) {
    SnapshotObject *ret_ptr;

    ret_ptr = NULL;

    if (enif_get_resource(Env, SnapshotTerm, m_DbSnapshot_RESOURCE, (void **) &ret_ptr)) {
        // has close been requested?
        if (ret_ptr->m_CloseRequested) {
            // object already closing
            ret_ptr = NULL;
        }   // else
    }   // if

    return (ret_ptr);

}   // SnapshotObject::RetrieveSnapshotObject


void
SnapshotObject::SnapshotObjectResourceCleanup(
        ErlNifEnv *Env,
        void *Arg) {
    SnapshotObject *snapshot_ptr;

    snapshot_ptr = (SnapshotObject *) Arg;

    if (NULL != snapshot_ptr->m_Snapshot)
        snapshot_ptr->m_DbPtr->m_Db->ReleaseSnapshot(snapshot_ptr->m_Snapshot);

    // vtable for snapshot_ptr could be invalid if close already
    //  occurred
    InitiateCloseRequest(snapshot_ptr);

    // YES this can be called after snapshot_ptr destructor.  Don't panic.
    AwaitCloseAndDestructor(snapshot_ptr);

    return;

}   // SnapshotObject::SnapshotObjectResourceCleanup


SnapshotObject::SnapshotObject(
        DbObject *DbPtr,
        const rocksdb::Snapshot *Snapshot)
        : m_Snapshot(Snapshot), m_DbPtr(DbPtr) {
    if (NULL != DbPtr)
        DbPtr->AddSnapshotReference(this);

}   // SnapshotObject::SnapshotObject


SnapshotObject::~SnapshotObject() {

    if (NULL != m_DbPtr.get())
        m_DbPtr->RemoveSnapshotReference(this);

    m_Snapshot = NULL;

    // do not clean up m_CloseMutex and m_CloseCond

    return;

}   // SnapshotObject::~SnapshotObject


void
SnapshotObject::Shutdown() {
    RefDec();

    return;
}   // ItrObject::CloseRequest


/**
* Iterator management object
*/

ErlNifResourceType *ItrObject::m_Itr_RESOURCE(NULL);


void
ItrObject::CreateItrObjectType(
        ErlNifEnv *Env) {
    ErlNifResourceFlags flags = (ErlNifResourceFlags)(ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER);

    m_Itr_RESOURCE = enif_open_resource_type(Env, NULL, "erocksdb_ItrObject",
                                             &ItrObject::ItrObjectResourceCleanup,
                                             flags, NULL);

    return;

}   // ItrObject::CreateItrObjectType


ItrObject *
ItrObject::CreateItrObject(
        DbObject *DbPtr,
        rocksdb::Iterator *Iterator,
        bool KeysOnly) {
    ItrObject *ret_ptr;
    void *alloc_ptr;

    // the alloc call initializes the reference count to "one"
    alloc_ptr = enif_alloc_resource(m_Itr_RESOURCE, sizeof(ItrObject));

    ret_ptr = new(alloc_ptr) ItrObject(DbPtr, Iterator, KeysOnly);

    // manual reference increase to keep active until "close" called
    //  only inc local counter
    ret_ptr->RefInc();

    // see IterTask::operator() for release of reference count

    return (ret_ptr);

}   // ItrObject::CreateItrObject

ItrObject *
ItrObject::CreateItrObject(
        DbObject *DbPtr,
        rocksdb::Iterator *Iterator,
        bool KeysOnly,
        ColumnFamilyObject *ColumnFamilyPtr) {
    ItrObject *ret_ptr;
    void *alloc_ptr;

    // the alloc call initializes the reference count to "one"
    alloc_ptr = enif_alloc_resource(m_Itr_RESOURCE, sizeof(ItrObject));

    ret_ptr = new(alloc_ptr) ItrObject(DbPtr, Iterator, KeysOnly, ColumnFamilyPtr);

    // manual reference increase to keep active until "close" called
    //  only inc local counter
    ret_ptr->RefInc();

    // see IterTask::operator() for release of reference count

    return (ret_ptr);

}   // ItrObject::CreateItrObject


ItrObject *
ItrObject::RetrieveItrObject(
        ErlNifEnv *Env,
        const ERL_NIF_TERM &ItrTerm, bool ItrClosing) {
    ItrObject *ret_ptr;

    ret_ptr = NULL;

    if (enif_get_resource(Env, ItrTerm, m_Itr_RESOURCE, (void **) &ret_ptr)) {
        // has close been requested?
        if (ret_ptr->m_CloseRequested
            || (!ItrClosing && ret_ptr->m_DbPtr->m_CloseRequested)) {
            // object already closing
            ret_ptr = NULL;
        }   // else
    }   // if

    return (ret_ptr);

}   // ItrObject::RetrieveItrObject


void
ItrObject::ItrObjectResourceCleanup(
        ErlNifEnv *Env,
        void *Arg) {
    ItrObject *itr_ptr;

    itr_ptr = (ItrObject *) Arg;

    // vtable for itr_ptr could be invalid if close already
    //  occurred
    InitiateCloseRequest(itr_ptr);

    // YES this can be called after itr_ptr destructor.  Don't panic.
    AwaitCloseAndDestructor(itr_ptr);

    return;

}   // ItrObject::ItrObjectResourceCleanup


ItrObject::ItrObject(
        DbObject *DbPtr,
        rocksdb::Iterator *Iterator,
        bool KeysOnly)
        : keys_only(KeysOnly), m_Iterator(Iterator), m_DbPtr(DbPtr) {
    if (NULL != DbPtr)
        DbPtr->AddReference(this);

}   // ItrObject::ItrObject


ItrObject::ItrObject(
        DbObject *DbPtr,
        rocksdb::Iterator *Iterator,
        bool KeysOnly,
        ColumnFamilyObject *ColumnFamilyPtr)
        : m_ColumnFamilyPtr(ColumnFamilyPtr), keys_only(KeysOnly), m_Iterator(Iterator), m_DbPtr(DbPtr) {
    if (NULL != DbPtr)
        DbPtr->AddReference(this);

    if (NULL != ColumnFamilyPtr)
        m_ColumnFamilyPtr->AddItrReference(this);

}   // ItrObject::ItrObject



ItrObject::~ItrObject() {
    // not likely to have active reuse item since it would
    //  block destruction

    if (NULL != m_DbPtr.get())
        m_DbPtr->RemoveReference(this);

    if (NULL != m_ColumnFamilyPtr.get())
        m_ColumnFamilyPtr->RemoveItrReference(this);

    // do not clean up m_CloseMutex and m_CloseCond

    return;

}   // ItrObject::~ItrObject


void
ItrObject::Shutdown() {
    // if there is an active move object, set it up to delete
    //  (reuse_move holds a counter to this object, which will
    //   release when move object destructs)
    RefDec();

    return;

}   // ItrObject::CloseRequest



/**
* transaction log object
*/

ErlNifResourceType *TLogItrObject::m_TLogItr_RESOURCE(NULL);


void
TLogItrObject::CreateTLogItrObjectType(
        ErlNifEnv *Env) {
    ErlNifResourceFlags flags = (ErlNifResourceFlags)(ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER);

    m_TLogItr_RESOURCE = enif_open_resource_type(Env, NULL, "erocksdb_TLogItrObject",
                                                 &TLogItrObject::TLogItrObjectResourceCleanup,
                                                 flags, NULL);

    return;

}   // SnapshotObject::CreateSnapshotObjectType


TLogItrObject *
TLogItrObject::CreateTLogItrObject(
        DbObject *DbPtr,
        rocksdb::TransactionLogIterator * Itr) {
    TLogItrObject *ret_ptr;
    void *alloc_ptr;

    // the alloc call initializes the reference count to "one"
    alloc_ptr = enif_alloc_resource(m_TLogItr_RESOURCE, sizeof(TLogItrObject));

    ret_ptr = new(alloc_ptr) TLogItrObject(DbPtr, Itr);

    // manual reference increase to keep active until "close" called
    //  only inc local counter
    ret_ptr->RefInc();

    // see IterTask::operator() for release of reference count

    return (ret_ptr);

}   // TLogItrObject::CreateTLogItrObject


TLogItrObject *
TLogItrObject::RetrieveTLogItrObject(
        ErlNifEnv *Env,
        const ERL_NIF_TERM &TLogItrTerm) {
    TLogItrObject *ret_ptr;

    ret_ptr = NULL;

    if (enif_get_resource(Env, TLogItrTerm, m_TLogItr_RESOURCE, (void **) &ret_ptr)) {
        // has close been requested?
        if (ret_ptr->m_CloseRequested) {
            // object already closing
            ret_ptr = NULL;
        }   // else
    }   // if

    return (ret_ptr);

}   // TLogItrObject::RetrieveTLogItrObject


void
TLogItrObject::TLogItrObjectResourceCleanup(
        ErlNifEnv *Env,
        void *Arg) {
    TLogItrObject *tlog_ptr;

    tlog_ptr = (TLogItrObject *) Arg;

    // vtable for snapshot_ptr could be invalid if close already
    //  occurred
    InitiateCloseRequest(tlog_ptr);

    // YES this can be called after snapshot_ptr destructor.  Don't panic.
    AwaitCloseAndDestructor(tlog_ptr);

    return;

}   // SnapshotObject::SnapshotObjectResourceCleanup


TLogItrObject::TLogItrObject(
        DbObject *DbPtr,
        rocksdb::TransactionLogIterator * Itr)
        : m_Iter(Itr), m_DbPtr(DbPtr) {

    if (NULL != DbPtr)
        DbPtr->AddTLogReference(this);

}   // TLogItrObject::TLogItrObject


TLogItrObject::~TLogItrObject() {

    if (NULL != m_DbPtr.get())
        m_DbPtr->RemoveTLogReference(this);

    m_Iter = NULL;

    // do not clean up m_CloseMutex and m_CloseCond

    return;

}   // TLogItrObject::~TLogItrObject


void
TLogItrObject::Shutdown() {
    RefDec();

    return;
}   // TLogItrObject::CloseRequest



}