%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 21. Apr 2016 21:49
%%%-------------------------------------------------------------------
-module(in_mem).
-author("benoitc").

-compile([export_all/1]).
-include_lib("eunit/include/eunit.hrl").


basic_test() ->
  {ok, Db} = erocksdb:open("", []),
  ok = erocksdb:put(Db, <<"a">>, <<"1">>, []),
  ?assertEqual({ok, <<"1">>}, erocksdb:get(Db, <<"a">>, [])),
  {ok, Db1} = erocksdb:open("", []),
  ok = erocksdb:put(Db1, <<"a">>, <<"2">>, []),
  ?assertEqual({ok, <<"1">>}, erocksdb:get(Db, <<"a">>, [])),
  ?assertEqual({ok, <<"2">>}, erocksdb:get(Db1, <<"a">>, [])),
  ok = erocksdb:close(Db),
  ok = erocksdb:close(Db1),
  ok.


with_mem_name_test() ->
  {ok, Db} = erocksdb:open("mem", []),
  ?assert(filelib:is_dir("mem") =:= false),
  ok = erocksdb:put(Db, <<"a">>, <<"1">>, []),
  ?assertEqual({ok, <<"1">>}, erocksdb:get(Db, <<"a">>, [])),
  {ok, Db1} = erocksdb:open("mem", []),
  ok = erocksdb:put(Db1, <<"a">>, <<"2">>, []),
  ?assertEqual({ok, <<"1">>}, erocksdb:get(Db, <<"a">>, [])),
  ?assertEqual({ok, <<"2">>}, erocksdb:get(Db1, <<"a">>, [])),
  ok = erocksdb:close(Db),
  ok = erocksdb:close(Db1),
  ok.
