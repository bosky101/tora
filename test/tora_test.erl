setup() ->
    {ok, _Pid} = tora:start(mypool),
    ok = tora:vanish(mypool).

cleanup(_Foo) ->
    ok = tora:stop(mypool).

test_pool_create() ->
    tora:pool_create(mypool1),
    ?assertEqual(1, tora:pool_size(mypool1)),
    tora:stop(mypool1).

test_start_link() ->
    tora:pool_create(mypool2),
    ?assertEqual(1, tora:pool_size(mypool2)),
    ConnPid = lists:nth(1, tora:pool_connections(mypool2)),
    exit(ConnPid, forced_death),
    timer:sleep(1000), % lets give gen_server some time to process the EXIT signal
    ?assertEqual(0, tora:pool_size(mypool2)),
    tora:stop(mypool2).

test_add_to_pool() ->
    ok = tora:add_to_pool(mypool), % add another connection handler
    ?assertEqual(2, tora:pool_size(mypool)).
    
test_put_get() ->
    ok = tora:put(mypool, "put_get1", <<"harish">>),
    ok = tora:put(mypool, "put_get2", <<2#1101:32>>),
    <<"harish">> = tora:get(mypool, "put_get1"),
    <<2#1101:32>> = tora:get(mypool, "put_get2").

test_putkeep() ->
    ok = tora:put(mypool, "putkeep1", <<"apple">>),
    {error, _} = tora:putkeep(mypool, "putkeep1", <<"orange">>),
    <<"apple">> = tora:get(mypool, "putkeep1"), % no effect if key already exists before putkeep

    ok = tora:putkeep(mypool, "putkeep2", <<"orange">>),
    <<"orange">> = tora:get(mypool, "putkeep2"). % puts the key if key does not exist already

test_putcat() ->
    ok = tora:put(mypool, "putcat1", <<"foo">>),
    % append "bar" to the end
    ok = tora:putcat(mypool, "putcat1", <<"bar">>),
    <<"foobar">> = tora:get(mypool, "putcat1").

test_putsh1() ->
    ok = tora:put(mypool, "putsh1", <<"foo">>),
    % append "bar" to the end and shift to the left to retain the width of "4"
    ok = tora:putsh1(mypool, "putsh1", <<"bar">>, 4),
    <<"obar">> = tora:get(mypool, "putsh1").

test_putnr() ->
    tora:putnr(mypool, "putnr1", <<"mango">>),
    <<"mango">> = tora:get(mypool, "putnr1").

test_out() ->
    ok = tora:put(mypool, "out1", <<"germany">>),
    <<"germany">> = tora:get(mypool, "out1"),
    ok = tora:out(mypool, "out1"),
    {error, _} = tora:get(mypool, "out1").

test_mget() ->
    ok = tora:put(mypool, "mget1", <<"usa">>),
    ok = tora:put(mypool, "mget2", <<"canada">>),
    ok = tora:put(mypool, "mget3", <<"singapore">>),
    ok = tora:put(mypool, "mget4", <<"india">>),
    [{"mget1", <<"usa">>}, {"mget2", <<"canada">>}, 
        {"mget3", <<"singapore">>}, {"mget4", <<"india">>} ] = tora:mget(mypool, ["mget1", "mget2", "mget3", "mget4"]).

test_vsiz() ->
    ok = tora:put(mypool, "vsiz1", <<"singapore">>),
    9 = tora:vsiz(mypool, "vsiz1").

test_iter() ->
    ok = tora:put(mypool, "a", <<"first">>),
    ok = tora:iterinit(mypool),
    "a" = tora:iternext(mypool). % "a" should be the first key

test_fwmkeys() ->
    ok = tora:put(mypool, "fwmkeys1", <<"1">>),
    ok = tora:put(mypool, "fwmkeys2", <<"2">>),
    ok = tora:put(mypool, "fwmkeys3", <<"3">>),
    ok = tora:put(mypool, "fwmkeys4", <<"4">>),

    Keys1 = tora:fwmkeys(mypool, "fwmkeys", 4),
    ?assertEqual(4, length(Keys1)),
    ?assert(lists:member("fwmkeys1", Keys1)),
    ?assert(lists:member("fwmkeys2", Keys1)),
    ?assert(lists:member("fwmkeys3", Keys1)),
    ?assert(lists:member("fwmkeys4", Keys1)),

    Keys2 = tora:fwmkeys(mypool, "fwmkeys", 2),
    ?assertEqual(2, length(Keys2)).

test_addint() ->
    ok = tora:put(mypool, "addint1", <<100:32/little>>),
    <<100:32/little>> = tora:get(mypool, "addint1"),
    120 = tora:addint(mypool, "addint1", 20).

test_vanish() ->
    ok = tora:put(mypool, "vanish1", <<"what a waste">>),
    ok = tora:vanish(mypool),
    {error, _} = tora:get(mypool, "vanish1").

test_rnum() ->
    ok = tora:vanish(mypool),
    ok = tora:put(mypool, "rnum1", <<"foo">>),
    ok = tora:put(mypool, "rnum2", <<"foo">>),
    ?assertEqual(2, tora:rnum(mypool)),
    ok = tora:vanish(mypool),
    ?assertEqual(0, tora:rnum(mypool)).

% test generators
put_tests() ->
    [fun test_put_get/0, fun test_putkeep/0, fun test_putcat/0, fun test_putsh1/0, fun test_putnr/0].
gen_test_() ->
    Tests = [
            fun test_pool_create/0, fun test_add_to_pool/0, fun test_start_link/0,
            put_tests(), fun test_out/0, fun test_mget/0, fun test_vsiz/0, fun test_iter/0, fun test_fwmkeys/0,
            fun test_addint/0, fun() -> ?assertEqual(ok, tora:sync(mypool)) end, 
            fun test_vanish/0, fun test_rnum/0, fun() -> tora:size(mypool) end, fun() -> tora:stat(mypool) end
        ],
    {inorder, {setup, fun setup/0, fun cleanup/1, Tests}}.
