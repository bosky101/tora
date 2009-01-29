start_stop_test() ->
    {ok, Pid} = tora_conn:start(),
    ok = tora_conn:stop(Pid).

put_get_test() ->
    {ok, Pid} = tora_conn:start(),
    ok = tora_conn:put(Pid, "put_get1", <<"harish">>),
    ok = tora_conn:put(Pid, "put_get2", <<2#1101:32>>),
    <<"harish">> = tora_conn:get(Pid, "put_get1"),
    <<2#1101:32>> = tora_conn:get(Pid, "put_get2"),
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
