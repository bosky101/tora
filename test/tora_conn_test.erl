start_stop_test() ->
    {ok, Pid} = tora_conn:start(),
    ok = tora_conn:stop(Pid).

start_link_test() ->
    process_flag(trap_exit, true),
    {ok, Pid} = tora_conn:start_link(),
    exit(Pid, forced_death),
    true = receive
        {'EXIT', Pid, forced_death} -> true;
        _ -> false
    after ?TIMEOUT -> false
    end,
    process_flag(trap_exit, false).

put_get_test() ->
    {ok, Pid} = tora_conn:start(),
    ok = tora_conn:put(Pid, "put_get1", <<"harish">>),
    ok = tora_conn:put(Pid, "put_get2", <<2#1101:32>>),
    <<"harish">> = tora_conn:get(Pid, "put_get1"),
    <<2#1101:32>> = tora_conn:get(Pid, "put_get2"),
    ok = tora_conn:stop(Pid).

putkeep_test() ->
    {ok, Pid} = tora_conn:start(),
    ok = tora_conn:put(Pid, "putkeep1", <<"apple">>),
    {error, _} = tora_conn:putkeep(Pid, "putkeep1", <<"orange">>),
    <<"apple">> = tora_conn:get(Pid, "putkeep1"), % no effect if key already exists before putkeep
    
    ok = tora_conn:putkeep(Pid, "putkeep2", <<"orange">>),
    <<"orange">> = tora_conn:get(Pid, "putkeep2"), % puts the key if key does not exist already
    ok = tora_conn:stop(Pid).

putcat_test() ->
    {ok, Pid} = tora_conn:start(),
    ok = tora_conn:put(Pid, "putcat1", <<"foo">>),
    % append "bar" to the end
    ok = tora_conn:putcat(Pid, "putcat1", <<"bar">>),
    <<"foobar">> = tora_conn:get(Pid, "putcat1"),
    ok = tora_conn:stop(Pid).

putsh1_test() ->
    {ok, Pid} = tora_conn:start(),
    ok = tora_conn:put(Pid, "putsh1", <<"foo">>),
    % append "bar" to the end and shift to the left to retain the width of "4"
    ok = tora_conn:putsh1(Pid, "putsh1", <<"bar">>, 4),
    <<"obar">> = tora_conn:get(Pid, "putsh1"),
    ok = tora_conn:stop(Pid).

putnr_test() ->
    {ok, Pid} = tora_conn:start(),
    tora_conn:putnr(Pid, "putnr1", <<"mango">>),
    <<"mango">> = tora_conn:get(Pid, "putnr1"),
    ok = tora_conn:stop(Pid).

out_test() ->
    {ok, Pid} = tora_conn:start(),
    ok = tora_conn:put(Pid, "out1", <<"germany">>),
    <<"germany">> = tora_conn:get(Pid, "out1"),
    ok = tora_conn:out(Pid, "out1"),
    {error, _} = tora_conn:get(Pid, "out1"),
    ok = tora_conn:stop(Pid).

mget_test() ->
    {ok, Pid} = tora_conn:start(),
    ok = tora_conn:put(Pid, "mget1", <<"usa">>),
    ok = tora_conn:put(Pid, "mget2", <<"canada">>),
    ok = tora_conn:put(Pid, "mget3", <<"singapore">>),
    ok = tora_conn:put(Pid, "mget4", <<"india">>),
    [{"mget1", <<"usa">>}, {"mget2", <<"canada">>}, 
        {"mget3", <<"singapore">>}, {"mget4", <<"india">>} ] = tora_conn:mget(Pid, ["mget1", "mget2", "mget3", "mget4"]),
    ok = tora_conn:stop(Pid).

vsiz_test() ->
    {ok, Pid} = tora_conn:start(),
    ok = tora_conn:put(Pid, "vsiz1", <<"singapore">>),
    9 = tora_conn:vsiz(Pid, "vsiz1"),
    ok = tora_conn:stop(Pid).

iter_test() ->
    {ok, Pid} = tora_conn:start(),
    ok = tora_conn:put(Pid, "a", <<"first">>),
    ok = tora_conn:iterinit(Pid),
    "a" = tora_conn:iternext(Pid), % "a" should be the first key
    ok = tora_conn:stop(Pid).

fwmkeys_test() ->
    {ok, Pid} = tora_conn:start(),
    ok = tora_conn:put(Pid, "fwmkeys1", <<"1">>),
    ok = tora_conn:put(Pid, "fwmkeys2", <<"2">>),
    ok = tora_conn:put(Pid, "fwmkeys3", <<"3">>),
    ok = tora_conn:put(Pid, "fwmkeys4", <<"4">>),

    Keys1 = tora_conn:fwmkeys(Pid, "fwmkeys", 4),
    ?assertEqual(4, length(Keys1)),
    ?assert(lists:member("fwmkeys1", Keys1)),
    ?assert(lists:member("fwmkeys2", Keys1)),
    ?assert(lists:member("fwmkeys3", Keys1)),
    ?assert(lists:member("fwmkeys4", Keys1)),

    Keys2 = tora_conn:fwmkeys(Pid, "fwmkeys", 2),
    ?assertEqual(2, length(Keys2)),
    ok = tora_conn:stop(Pid).

addint_test() ->
    {ok, Pid} = tora_conn:start(),
    ok = tora_conn:put(Pid, "addint1", <<100:32/little>>),
    <<100:32/little>> = tora_conn:get(Pid, "addint1"),
    120 = tora_conn:addint(Pid, "addint1", 20),
    ok = tora_conn:stop(Pid).

sync_test() ->
    {ok, Pid} = tora_conn:start(),
    ok = tora_conn:sync(Pid),
    ok = tora_conn:stop(Pid).

vanish_test() ->
    {ok, Pid} = tora_conn:start(),
    ok = tora_conn:put(Pid, "vanish1", <<"what a waste">>),
    ok = tora_conn:vanish(Pid),
    {error, _} = tora_conn:get(Pid, "vanish1"),
    ok = tora_conn:stop(Pid).

rnum_test() ->
    {ok, Pid} = tora_conn:start(),
    ok = tora_conn:vanish(Pid),
    ok = tora_conn:put(Pid, "rnum1", <<"foo">>),
    ok = tora_conn:put(Pid, "rnum2", <<"foo">>),
    ?assertEqual(2, tora_conn:rnum(Pid)),
    ok = tora_conn:vanish(Pid),
    ?assertEqual(0, tora_conn:rnum(Pid)),
    ok = tora_conn:stop(Pid).

size_test() ->
    {ok, Pid} = tora_conn:start(),
    tora_conn:size(Pid),
    ok = tora_conn:stop(Pid).

stat_test() ->
    {ok, Pid} = tora_conn:start(),
    tora_conn:stat(Pid),
    ok = tora_conn:stop(Pid).
