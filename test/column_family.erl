-module(column_family).


-compile([export_all/1]).
-include_lib("eunit/include/eunit.hrl").


basic_test() ->
    os:cmd("rm -rf test.db"),
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