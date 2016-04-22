-module(column_family).


-compile([export_all/1]).
-include_lib("eunit/include/eunit.hrl").


basic_test() ->
    erocksdb:destroy("test.db", []),
    ColumnFamilies = [{"default", []}],
    {ok, Db, Handles1} = erocksdb:open_with_cf("test.db", [{create_if_missing, true}], ColumnFamilies),
    ?assertEqual(1, length(Handles1)),
    ?assertEqual({ok, ["default"]}, erocksdb:list_column_families("test.db", [])),
    {ok, Handle} = erocksdb:create_column_family(Db, "test", []),

    ?assertEqual({ok, ["default", "test"]}, erocksdb:list_column_families("test.db", [])),

    ok = erocksdb:drop_column_family(Handle),
    ?assertEqual({ok, ["default"]}, erocksdb:list_column_families("test.db", [])),
    erocksdb:close(Db),
    ok.


column_order_test() ->
    os:cmd("rm -rf test.db"),
    ColumnFamilies = [{"default", []}],
    {ok, Db, Handles1} = erocksdb:open_with_cf("test.db", [{create_if_missing, true}], ColumnFamilies),
    ?assertEqual(1, length(Handles1)),
    ?assertEqual({ok, ["default"]}, erocksdb:list_column_families("test.db", [])),
    {ok, Handle} = erocksdb:create_column_family(Db, "test", []),
    erocksdb:close(Db),

    ?assertEqual({ok, ["default", "test"]}, erocksdb:list_column_families("test.db", [])),

    ColumnFamilies2 = [{"default", []}, {"test", []}],
    {ok, Db2, Handles2} = erocksdb:open_with_cf("test.db", [{create_if_missing, true}], ColumnFamilies2),

    [_DefaultH, TestH] = Handles2,
    ok = erocksdb:drop_column_family(TestH),
    ?assertEqual({ok, ["default"]}, erocksdb:list_column_families("test.db", [])),
    erocksdb:close(Db2),
    ok.

try_remove_default_test() ->
    os:cmd("rm -rf test.db"),
    ColumnFamilies = [{"default", []}],
    {ok, Db, [DefaultH]} = erocksdb:open_with_cf("test.db", [{create_if_missing, true}], ColumnFamilies),
    {error, _} = erocksdb:drop_column_family(DefaultH),
    {ok, _Handle} = erocksdb:create_column_family(Db, "test", []),
    erocksdb:close(Db),
    ColumnFamilies2 = [{"default", []}, {"test", []}],
    {ok, Db2, [DefaultH2, _]} = erocksdb:open_with_cf("test.db", [{create_if_missing, true}], ColumnFamilies2),
    {error, _} = erocksdb:drop_column_family(DefaultH2),
    erocksdb:close(Db2),
    ok.


basic_kvs_test() ->
    os:cmd("rm -rf test.db"),
    ColumnFamilies = [{"default", []}],
    {ok, Db, [DefaultH]} = erocksdb:open_with_cf("test.db", [{create_if_missing, true}], ColumnFamilies),
    ok = erocksdb:put(Db, DefaultH, <<"a">>, <<"a1">>, []),
    {ok,  <<"a1">>} = erocksdb:get(Db, DefaultH, <<"a">>, []),
    ok = erocksdb:put(Db, DefaultH, <<"b">>, <<"b1">>, []),
    {ok, <<"b1">>} = erocksdb:get(Db, DefaultH, <<"b">>, []),
    ok = erocksdb:delete(Db, DefaultH, <<"b">>, []),
    not_found = erocksdb:get(Db, DefaultH, <<"b">>, []),

    {ok, TestH} = erocksdb:create_column_family(Db, "test", []),
    erocksdb:put(Db, TestH, <<"a">>, <<"a2">>, []),
    {ok,  <<"a1">>} = erocksdb:get(Db, DefaultH, <<"a">>, []),
    {ok,  <<"a2">>} = erocksdb:get(Db, TestH, <<"a">>, []),

    erocksdb:close(Db),
    ok.


iterators_test() ->
    os:cmd("rm -rf ltest"),  % NOTE
    {ok, Ref, [DefaultH]} = erocksdb:open_with_cf("ltest", [{create_if_missing, true}], [{"default", []}]),
    {ok, TestH} = erocksdb:create_column_family(Ref, "test", []),
    try
        erocksdb:put(Ref, DefaultH, <<"a">>, <<"x">>, []),
        erocksdb:put(Ref, DefaultH, <<"b">>, <<"y">>, []),
        erocksdb:put(Ref, TestH, <<"a">>, <<"x1">>, []),
        erocksdb:put(Ref, TestH, <<"b">>, <<"y1">>, []),

        {ok, [DefaultIt, TestIt]} = erocksdb:iterators(Ref, [DefaultH, TestH], []),
        ?assertEqual({ok, <<"a">>, <<"x">>},erocksdb:iterator_move(DefaultIt, <<>>)),
        ?assertEqual({ok, <<"a">>, <<"x1">>},erocksdb:iterator_move(TestIt, <<>>)),
        ?assertEqual({ok, <<"b">>, <<"y">>},erocksdb:iterator_move(DefaultIt, next)),
        ?assertEqual({ok, <<"b">>, <<"y1">>},erocksdb:iterator_move(TestIt, next)),
        ?assertEqual({ok, <<"a">>, <<"x">>},erocksdb:iterator_move(DefaultIt, prev)),
        ?assertEqual({ok, <<"a">>, <<"x1">>},erocksdb:iterator_move(TestIt, prev)),
        ok = erocksdb:iterator_close(TestIt)
    after
        erocksdb:close(Ref)
    end.

iterators_drop_column_test() ->
    os:cmd("rm -rf ltest"),  % NOTE
    {ok, Ref, [DefaultH]} = erocksdb:open_with_cf("ltest", [{create_if_missing, true}], [{"default", []}]),
    {ok, TestH} = erocksdb:create_column_family(Ref, "test", []),
    try
        erocksdb:put(Ref, DefaultH, <<"a">>, <<"x">>, []),
        erocksdb:put(Ref, DefaultH, <<"b">>, <<"y">>, []),
        erocksdb:put(Ref, TestH, <<"a">>, <<"x1">>, []),
        erocksdb:put(Ref, TestH, <<"b">>, <<"y1">>, []),

        {ok, [DefaultIt, TestIt]} = erocksdb:iterators(Ref, [DefaultH, TestH], []),
        ?assertEqual({ok, <<"a">>, <<"x">>},erocksdb:iterator_move(DefaultIt, <<>>)),
        ?assertEqual({ok, <<"a">>, <<"x1">>},erocksdb:iterator_move(TestIt, <<>>)),
        ?assertEqual({ok, <<"b">>, <<"y">>},erocksdb:iterator_move(DefaultIt, next)),
        ?assertEqual({ok, <<"b">>, <<"y1">>},erocksdb:iterator_move(TestIt, next)),
        ?assertEqual({ok, <<"a">>, <<"x">>},erocksdb:iterator_move(DefaultIt, prev)),
        ok = erocksdb:drop_column_family(TestH),
        ?assertError(badarg, erocksdb:iterator_move(TestIt, prev))
    after
        erocksdb:close(Ref)
    end.
