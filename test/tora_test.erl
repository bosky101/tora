setup() ->
    tora:connect().

put_get() ->
    ok = tora:put("put_get1", <<"harish">>),
    ok = tora:put("put_get2", <<2#1101:32>>),
    <<"harish">> = tora:get("put_get1"),
    <<2#1101:32>> = tora:get("put_get2").

putkeep() ->
    ok = tora:put("putkeep1", <<"apple">>),
    {error, _} = tora:putkeep("putkeep1", <<"orange">>),
    <<"apple">> = tora:get("putkeep1"), % no effect if key already exists before putkeep
    
    ok = tora:putkeep("putkeep2", <<"orange">>),
    <<"orange">> = tora:get("putkeep2"). % puts the key if key does not exist already

putcat() ->
    ok = tora:put("putcat1", <<"foo">>),
    % append "bar" to the end
    ok = tora:putcat("putcat1", <<"bar">>),
    <<"foobar">> = tora:get("putcat1").

putsh1() ->
    ok = tora:put("putsh1", <<"foo">>),
    % append "bar" to the end and shift to the lift to retain the width of "4"
    ok = tora:putsh1("putsh1", <<"bar">>, 4),
    <<"obar">> = tora:get("putsh1").

putnr() ->
    tora:putnr("putnr1", <<"mango">>),
    <<"mango">> = tora:get("putnr1").

out() ->
    ok = tora:put("out1", <<"germany">>),
    <<"germany">> = tora:get("out1"),
    ok = tora:out("out1"),
    {error, _} = tora:get("out1").

mget() ->
    ok = tora:put("mget1", <<"usa">>),
    ok = tora:put("mget2", <<"canada">>),
    ok = tora:put("mget3", <<"singapore">>),
    ok = tora:put("mget4", <<"india">>),
    [{"mget1", <<"usa">>}, {"mget2", <<"canada">>}, 
        {"mget3", <<"singapore">>}, {"mget4", <<"india">>} ] = tora:mget(["mget1", "mget2", "mget3", "mget4"]).

% test generators
put_tests() ->
    [fun put_get/0, fun putkeep/0, fun putcat/0, fun putsh1/0, fun putnr/0].
gen_test_() ->
    Tests = [put_tests(), fun out/0, fun mget/0],
    {inorder, {setup, fun setup/0, Tests}}.