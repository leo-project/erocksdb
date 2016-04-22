-module(db).
-compile([export_all/1]).
-include_lib("eunit/include/eunit.hrl").

destroy_reopen(DbName, Options) ->
    _ = erocksdb:destroy(DbName, []),
    {ok, Db} = erocksdb:open(DbName, Options, []),
    Db.


flush_test() ->
  os:cmd("rm -rf test.db"),
  {ok, Db} = erocksdb:open("test.db", [{create_if_missing, true}], []),
  ok = erocksdb:put(Db, <<"a">>, <<"1">>, []),
  ok = erocksdb:flush(Db),
  {ok, <<"1">>} = erocksdb:get(Db, <<"a">>, []),
  erocksdb:close(Db).


randomstring(Len) ->
  list_to_binary([random:uniform(95) || _I <- lists:seq(1, Len)]).

key(I) when is_integer(I) ->
  <<I:128/unsigned>>.


approximate_sizes_test() ->
    Db = destroy_reopen("test.db", [{create_if_missing, true},
                                    {write_buffer_size, 100000000},
                                    {compression, none}]),

    try
      N = 128,
      random:seed(),
      lists:foreach(fun(I) ->
                        ok = erocksdb:put(Db, key(I), randomstring(1024),[])
                    end, lists:seq(0, N)),
      timer:sleep(500),
      Start = key(50),
      End = key(60),
      Size = erocksdb:get_approximate_sizes(Db, Start, End, true),

      ?assert(Size >= 6000),
      ?assert(Size =< 204800),
      Size2 = erocksdb:get_approximate_sizes(Db, Start, End, false),
      ?assertEqual(0, Size2),
      Start2 = key(500),
      End2 = key(600),
      Size3 = erocksdb:get_approximate_sizes(Db, Start2, End2, true),
      io:format("size is ~p~n", [Size3]),
      ?assertEqual(0, Size3),
      lists:foreach(fun(I) ->
                             ok = erocksdb:put(Db, key(1000 + I), randomstring(1024), [])
                    end, lists:seq(0, N)),
      timer:sleep(500),

      Size4 = erocksdb:get_approximate_sizes(Db, Start2, End2, true),
      ?assertEqual(0, Size4),
      Start3 = key(1000),
      End3 = key(1020),
      Size5 = erocksdb:get_approximate_sizes(Db, Start3, End3, true),


      ?assert(Size5 >= 6000)
  after
      erocksdb:close(Db)
  end.


