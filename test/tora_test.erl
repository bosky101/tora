setup() ->
    tora:connect().

put_get() ->
    ok = tora:put("key1", <<"harish">>),
    ok = tora:put("key2", <<2#1101:32>>),
    <<"harish">> = tora:get("key1"),
    <<2#1101:32>> = tora:get("key2").

gen_test_() ->
    Tests = [fun put_get/0],
    {inorder, {setup, fun setup/0, Tests}}.