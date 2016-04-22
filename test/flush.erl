%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 21. Apr 2016 21:49
%%%-------------------------------------------------------------------
-module(flush).
-author("benoitc").

-compile([export_all/1]).
-include_lib("eunit/include/eunit.hrl").

flush_test() ->
  os:cmd("rm -rf test.db"),
  {ok, Db} = erocksdb:open("test.db", [{create_if_missing, true}], []),
  ok = erocksdb:put(Db, <<"a">>, <<"1">>, []),
  ok = erocksdb:flush(Db),
  {ok, <<"1">>} = erocksdb:get(Db, <<"a">>, []),
  erocksdb:close(Db).