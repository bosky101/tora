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

put_tests() ->
    [fun put_get/0, fun putkeep/0, fun putcat/0, fun putsh1/0, fun putnr/0].

gen_test_() ->
    Tests = [put_tests()],
    {inorder, {setup, fun setup/0, Tests}}.