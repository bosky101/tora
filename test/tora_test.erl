setup() ->
    {ok, _Pid} = tora:start_link(mypool),
    ok = tora:vanish(mypool).

cleanup(_Foo) ->
    ok = tora:stop(mypool).


test_add_to_pool() ->
    ok = tora:add_to_pool(mypool), % add another connection handler
    ?assertEqual(2, tora:connections_in_pool(mypool)).
    
test_put_get() ->
    ok = tora:put(mypool, "put_get1", <<"harish">>),
    ok = tora:put(mypool, "put_get2", <<2#1101:32>>),
    <<"harish">> = tora:get(mypool, "put_get1"),
    <<2#1101:32>> = tora:get(mypool, "put_get2").

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

gen_test_() ->
    Tests = [
            fun test_add_to_pool/0,
            fun test_put_get/0, 
            fun test_vanish/0, fun test_rnum/0
        ],
    {inorder, {setup, fun setup/0, fun cleanup/1, Tests}}.