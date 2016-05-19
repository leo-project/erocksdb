%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 19. May 2016 22:42
%%%-------------------------------------------------------------------
-module(transaction_log).
-author("benoitc").

-compile([export_all/1]).
-include_lib("eunit/include/eunit.hrl").

destroy_reopen(DbName, Options) ->
  _ = erocksdb:destroy(DbName, []),
  {ok, Db} = erocksdb:open(DbName, Options, []),
  Db.

close_destroy(Db, DbName) ->
  erocksdb:close(Db),
  erocksdb:destroy(DbName, []).

basic_test() ->
  Db = destroy_reopen("test.db", [{create_if_missing, true}]),
  Db1 = destroy_reopen("test1.db", [{create_if_missing, true}]),

  ok = erocksdb:put(Db, <<"a">>, <<"v1">>, []),
  ?assertEqual({ok, <<"v1">>}, erocksdb:get(Db, <<"a">>, [])),
  ?assertEqual(1, erocksdb:get_latest_sequence_number(Db)),

  {ok, Itr} = erocksdb:get_updates_since(Db, 0),
  {ok, Last, TransactionBin} = erocksdb:next_update(Itr),
  ?assertEqual(1, Last),

  ?assertEqual(not_found, erocksdb:get(Db1, <<"a">>, [])),
  ok = erocksdb:write_update(Db1, TransactionBin, []),
  ?assertEqual({ok, <<"v1">>}, erocksdb:get(Db1, <<"a">>, [])),

  close_destroy(Db, "test.db"),
  close_destroy(Db1, "test1.db"),
  ok.
