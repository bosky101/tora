setup() ->
    tora:connect().

test_put_get() ->
    ok = tora:put("put_get1", <<"harish">>),
    ok = tora:put("put_get2", <<2#1101:32>>),
    <<"harish">> = tora:get("put_get1"),
    <<2#1101:32>> = tora:get("put_get2").

test_putkeep() ->
    ok = tora:put("putkeep1", <<"apple">>),
    {error, _} = tora:putkeep("putkeep1", <<"orange">>),
    <<"apple">> = tora:get("putkeep1"), % no effect if key already exists before putkeep
    
    ok = tora:putkeep("putkeep2", <<"orange">>),
    <<"orange">> = tora:get("putkeep2"). % puts the key if key does not exist already

test_putcat() ->
    ok = tora:put("putcat1", <<"foo">>),
    % append "bar" to the end
    ok = tora:putcat("putcat1", <<"bar">>),
    <<"foobar">> = tora:get("putcat1").

test_putsh1() ->
    ok = tora:put("putsh1", <<"foo">>),
    % append "bar" to the end and shift to the left to retain the width of "4"
    ok = tora:putsh1("putsh1", <<"bar">>, 4),
    <<"obar">> = tora:get("putsh1").

test_putnr() ->
    tora:putnr("putnr1", <<"mango">>),
    <<"mango">> = tora:get("putnr1").

test_out() ->
    ok = tora:put("out1", <<"germany">>),
    <<"germany">> = tora:get("out1"),
    ok = tora:out("out1"),
    {error, _} = tora:get("out1").

test_mget() ->
    ok = tora:put("mget1", <<"usa">>),
    ok = tora:put("mget2", <<"canada">>),
    ok = tora:put("mget3", <<"singapore">>),
    ok = tora:put("mget4", <<"india">>),
    [{"mget1", <<"usa">>}, {"mget2", <<"canada">>}, 
        {"mget3", <<"singapore">>}, {"mget4", <<"india">>} ] = tora:mget(["mget1", "mget2", "mget3", "mget4"]).

test_vsiz() ->
    ok = tora:put("vsiz1", <<"singapore">>),
    9 = tora:vsiz("vsiz1").

test_iter() ->
    ok = tora:put("a", <<"first">>),
    ok = tora:iterinit(),
    "a" = tora:iternext(). % "a" should be the first key

test_fwmkeys() ->
    ok = tora:put("fwmkeys1", <<"1">>),
    ok = tora:put("fwmkeys2", <<"2">>),
    ok = tora:put("fwmkeys3", <<"3">>),
    ok = tora:put("fwmkeys4", <<"4">>),

    Keys1 = tora:fwmkeys("fwmkeys", 4),
    ?assertEqual(4, length(Keys1)),
    ?assert(lists:member("fwmkeys1", Keys1)),
    ?assert(lists:member("fwmkeys2", Keys1)),
    ?assert(lists:member("fwmkeys3", Keys1)),
    ?assert(lists:member("fwmkeys4", Keys1)),

    Keys2 = tora:fwmkeys("fwmkeys", 2),
    ?assertEqual(2, length(Keys2)).

test_addint() ->
    ok = tora:put("addint1", <<100:32/little>>),
    <<100:32/little>> = tora:get("addint1"),
    120 = tora:addint("addint1", 20).

test_vanish() ->
    ok = tora:put("vanish1", <<"what a waste">>),
    ok = tora:vanish(),
    {error, _} = tora:get("vanish1").

% test generators
put_tests() ->
    [fun test_put_get/0, fun test_putkeep/0, fun test_putcat/0, fun test_putsh1/0, fun test_putnr/0].
gen_test_() ->
    Tests = [
            put_tests(), fun test_out/0, fun test_mget/0, fun test_vsiz/0, fun test_iter/0, fun test_fwmkeys/0,
            fun test_addint/0, fun() -> ?assertEqual(ok, tora:sync()) end, fun test_vanish/0
        ],
    {inorder, {setup, fun setup/0, Tests}}.

