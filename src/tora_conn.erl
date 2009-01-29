%%%-------------------------------------------------------------------
%%% File:      tora_conn.erl
%%% @author    Harish Mallipeddi <harish.mallipeddi@gmail.com>
%%% @copyright 2009 Harish Mallipeddi
%%% @doc ttserver connection handler 
%%%
%%% @since Sun Jan 25 20:18:49 SGT 2009 by Harish Mallipeddi
%%%-------------------------------------------------------------------
-module(tora_conn).
-author('harish.mallipeddi@gmail.com').

-define(TIMEOUT, 5000).
-define(TCP_OPTS, [
    binary, {packet, raw}, {nodelay, true},{reuseaddr, true}, {active, true}
]).
-define(TT_DEFAULT_HOST, "localhost").
-define(TT_DEFAULT_PORT, 1978).

-define(KEYSIZE, apply(fun () -> KeySize = byte_size(Key), <<KeySize:32>> end, [])).
-define(VALSIZE, apply(fun () -> ValSize = byte_size(Value), <<ValSize:32>> end, [])).

-define(SUCCESS, <<0:8>>).

%% Command IDs
-define(CID_PUT, <<16#c810:16>>).
-define(CID_PUTKEEP, <<16#c811:16>>).
-define(CID_PUTCAT, <<16#c812:16>>).
-define(CID_PUTSH1, <<16#c813:16>>).
-define(CID_PUTNR, <<16#c818:16>>).
-define(CID_OUT, <<16#c820:16>>).
-define(CID_GET, <<16#c830:16>>).
-define(CID_MGET, <<16#c831:16>>).
-define(CID_VSIZ, <<16#c838:16>>).
-define(CID_ITERINIT, <<16#c850:16>>).
-define(CID_ITERNEXT, <<16#c851:16>>).
-define(CID_FWMKEYS, <<16#c858:16>>).
-define(CID_ADDINT, <<16#c860:16>>).
-define(CID_ADDDOUBLE, <<16#c861:16>>).
-define(CID_ADDEXT, <<16#c868:16>>).
-define(CID_SYNC, <<16#c870:16>>).
-define(CID_VANISH, <<16#c871:16>>).
-define(CID_COPY, <<16#c872:16>>).
-define(CID_RESTORE, <<16#c873:16>>).
-define(CID_SETMST, <<16#c878:16>>).
-define(CID_RNUM, <<16#c880:16>>).
-define(CID_SIZE, <<16#c881:16>>).
-define(CID_STAT, <<16#c888:16>>).
-define(CID_MISC, <<16#c890:16>>).

-export([
        start/0, start/2, start_link/0, start_link/2, stop/1, stop/2,
        put/3, put/4, putkeep/3, putkeep/4, putcat/3, putcat/4, putsh1/4, putsh1/5, putnr/3, out/2, out/3,
        get/2, get/3, mget/2, mget/3,
        vsiz/2, vsiz/3, iterinit/1, iterinit/2, iternext/1, iternext/2, fwmkeys/3, fwmkeys/4,
        addint/3, addint/4, adddouble/4, adddouble/5, 
        sync/1, sync/2, vanish/1, vanish/2, rnum/1, rnum/2, size/1, size/2, stat/1, stat/2
    ]).
%% -export([ext/4, misc/3]). % NOT IMPLEMENTED
-export([copy/2, copy/3, restore/3, restore/4, setmst/3, setmst/4]). % NOT TESTED
-record(state, {host, port, socket}).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include("test/tora_conn_test.erl").
-endif.

%%====================================================================
%% Public API
%%====================================================================

%% @doc start a connection handler (assumes default host & port)
start() ->
    start(?TT_DEFAULT_HOST, ?TT_DEFAULT_PORT).
%% @doc start a connection handler
start(Host, Port) when is_list(Host) andalso is_integer(Port) ->
    start(fun spawn/1, Host, Port).

%% @doc start a connection handler (assumes default host & port)
start_link() ->
    start_link(?TT_DEFAULT_HOST, ?TT_DEFAULT_PORT).
%% @doc start a connection handler
start_link(Host, Port) when is_list(Host) andalso is_integer(Port) ->
    start(fun spawn_link/1, Host, Port).

%% @private
start(SpawnMethod, Host, Port) ->
    Self = self(),
    Pid = SpawnMethod(fun() -> init([Host, Port, Self]) end),
    receive
        {init, Pid, ok} -> {ok, Pid};
        {init, Pid, {error, Reason}} -> {error, Reason};
        Unknown -> {error, Unknown}
    after ?TIMEOUT ->
        {error, timeout}
    end.

%% @doc stop the connection handler
stop(ConnHandler) -> stop(ConnHandler, self()).
stop(ConnHandler, From) -> call_(terminate, ConnHandler, From, []).

%% @doc store the given key, value pair
put(ConnHandler, Key, Value) ->
    put(ConnHandler, Key, Value, self()).
put(ConnHandler, Key, Value, From) when is_list(Key) andalso is_binary(Value) ->
    call_(put, ConnHandler, From, [list_to_binary(Key), Value]).

%% @doc 
%% store the given key, value pair only if the given key does not exist already.
%% if it already exists, will throw an error.
%% @end
putkeep(ConnHandler, Key, Value) ->
    putkeep(ConnHandler, Key, Value, self()).
%% @private
putkeep(ConnHandler, Key, Value, From) when is_list(Key) andalso is_binary(Value) ->
    call_(putkeep, ConnHandler, From, [list_to_binary(Key), Value]).

%% @doc append Value to the end
putcat(ConnHandler, Key, Value) ->
    putcat(ConnHandler, Key, Value, self()).
%% @private
putcat(ConnHandler, Key, Value, From) when is_list(Key) andalso is_binary(Value) ->
    call_(putcat, ConnHandler, From, [list_to_binary(Key), Value]).

%% @doc append Value to the end and shift to the left to retain the Width supplied
putsh1(ConnHandler, Key, Value, Width) ->
    putsh1(ConnHandler, Key, Value, Width, self()).
%% @private
putsh1(ConnHandler, Key, Value, Width, From) 
    when is_list(Key) andalso is_binary(Value) andalso is_integer(Width) ->
        call_(putsh1, ConnHandler, From, [list_to_binary(Key), Value, Width]).

%% @doc store the key, value pair but don't wait for response from the server
putnr(ConnHandler, Key, Value) when is_list(Key) andalso is_binary(Value) ->
    cast_(putnr, ConnHandler, [list_to_binary(Key), Value]).

%% @doc remove the record corresponding to the given key
out(ConnHandler, Key) ->
    out(ConnHandler, Key, self()).
%% @private
out(ConnHandler, Key, From) when is_list(Key) ->
    call_(out, ConnHandler, From, [list_to_binary(Key)]).

%% @doc get the value for the given key
get(ConnHandler, Key) ->
    get(ConnHandler, Key, self()).
%% @private
get(ConnHandler, Key, From) when is_list(Key) ->
    call_(get, ConnHandler, From, [list_to_binary(Key)]).

%% @doc multi-get
mget(ConnHandler, Keys) ->
    mget(ConnHandler, Keys, self()).
%% @private
mget(ConnHandler, Keys, From) when is_list(Keys) ->
    BKeys = [list_to_binary(Key) || Key <- Keys],
    call_(mget, ConnHandler, From, [BKeys]).

%% @doc return the size of the value for the given key
vsiz(ConnHandler, Key) ->
    vsiz(ConnHandler, Key, self()).
%% @private
vsiz(ConnHandler, Key, From) when is_list(Key) ->
    call_(vsiz, ConnHandler, From, [list_to_binary(Key)]).

%% @doc initialize the iterator to iterate over keys
iterinit(ConnHandler) -> iterinit(ConnHandler, self()).
iterinit(ConnHandler, From) -> call_(iterinit, ConnHandler, From, []).

%% @doc return the next key from the iterator
iternext(ConnHandler) -> iternext(ConnHandler, self()).
iternext(ConnHandler, From) -> call_(iternext, ConnHandler, From, []).

%% @doc return keys which start with the given Prefix (a maximum of MaxKeys are returned)
fwmkeys(ConnHandler, Prefix, MaxKeys) ->
    fwmkeys(ConnHandler, Prefix, MaxKeys, self()).
fwmkeys(ConnHandler, Prefix, MaxKeys, From) when is_list(Prefix) andalso is_integer(MaxKeys) -> 
    call_(fwmkeys, ConnHandler, From, [list_to_binary(Prefix), MaxKeys]).

%% @doc add integer to the value and return the summation value
addint(ConnHandler, Key, N) ->
    addint(ConnHandler, Key, N, self()).
%% @private
addint(ConnHandler, Key, N, From) when is_list(Key) andalso is_integer(N) ->
    call_(addint, ConnHandler, From, [list_to_binary(Key), N]).

%% @doc add a real number to the value and return the summation value (NOT TESTED)
adddouble(ConnHandler, Key, Integral, Fractional) ->
    adddouble(ConnHandler, Key, Integral, Fractional, self()).
%% @private
adddouble(ConnHandler, Key, Integral, Fractional, From) 
    when is_list(Key) andalso is_integer(Integral) andalso is_integer(Fractional) ->
        call_(adddouble, ConnHandler, From, [list_to_binary(Key), Integral, Fractional]).

%% @doc sync updates to file/device.
sync(ConnHandler) -> sync(ConnHandler, self()).
%% @private
sync(ConnHandler, From) -> call_(sync, ConnHandler, From, []).

%% @doc remove all records
vanish(ConnHandler) -> vanish(ConnHandler, self()).
%% @private
vanish(ConnHandler, From) -> call_(vanish, ConnHandler, From, []).

%% @doc total number of records
rnum(ConnHandler) -> rnum(ConnHandler, self()).
%% @private
rnum(ConnHandler, From) -> call_(rnum, ConnHandler, From, []).

%% @doc size of the database
size(ConnHandler) -> size(ConnHandler, self()).
%% @private
size(ConnHandler, From) -> call_(size, ConnHandler, From, []).

%% @doc status message of the database
stat(ConnHandler) -> stat(ConnHandler, self()).
%% @private
stat(ConnHandler, From) -> call_(stat, ConnHandler, From, []).

%% @doc
%% copy database to a different file (NOT TESTED)<br/>
%%  Path - specifies the path of the destination file. If it begins with `@', the trailing substring is executed as a command line.<br/>
%% @end
copy(ConnHandler, Path) ->
    copy(ConnHandler, Path, self()).
%% @private
copy(ConnHandler, Path, From) when is_list(Path) ->
    call_(copy, ConnHandler, From, [list_to_binary(Path)]).

%% @doc 
%% restore database file from update log (NOT TESTED)<br/>
%%  Path - specifies the path of the update log directory. If it begins with `+', the trailing substring is treated as the path and consistency checking is omitted.<br/>
%%  TS - specifies the beginning timestamp in microseconds.<br/>
%% @end
restore(ConnHandler, Path, TS) ->
    restore(ConnHandler, Path, TS, self()).
%% @private
restore(ConnHandler, Path, TS, From) when is_list(Path) andalso is_integer(TS) ->
    call_(restore, ConnHandler, From, [list_to_binary(Path), TS]).

%% @doc 
%% set the replication master (NOT TESTED)<br/>
%%  Host - specifies the name/address of the server.<br/>
%%  Port - specifies the port number.<br/>
%% @end
setmst(ConnHandler, Host, Port) ->
    setmst(ConnHandler, Host, Port, self()).
%% @private
setmst(ConnHandler, Host, Port, From) when is_list(Host) andalso is_integer(Port) ->
    call_(setmst, ConnHandler, From, [list_to_binary(Host), Port]).

%%====================================================================
%% Private stuff
%%====================================================================

call_(MethodName, ConnHandler, From, Args) when is_pid(From) -> % caller is just a regular process
    Self = self(),
    ConnHandler ! {handle_call, Self, {MethodName, Args}},
    receive
        {handle_call, ConnHandler, R} -> R;
        Unknown -> {error, Unknown}
    after ?TIMEOUT ->
        {error, timeout}
    end;    
call_(MethodName, ConnHandler, From, Args) -> % caller is a gen_server process
    Self = self(),
    ConnHandler ! {handle_call, Self, {MethodName, Args}},
    receive
        {handle_call, ConnHandler, R} -> gen_server:reply(From, R);
        Unknown -> gen_server:reply(From, {error, Unknown})
    after ?TIMEOUT ->
        gen_server:reply(From, {error, timeout})
    end.

cast_(MethodName, ConnHandler, Args) ->
    ConnHandler ! {handle_cast, {MethodName, Args}}.

init([Host, Port, Parent]) ->
    Self = self(),
    case gen_tcp:connect(Host, Port, ?TCP_OPTS) of
        {ok, Sock} ->
            State = #state{host=Host, port=Port, socket=Sock},
            Parent ! {init, Self, ok},
            loop(State);
        {error, Reason} ->
            Parent ! {init, Self, {error, Reason}}
    end.

loop(State) ->
    Self = self(),
    #state{socket=Sock} = State,
    NewState = receive
        {handle_call, From, {terminate, []}} ->
            reply_(State, From, {handle_call, Self, gen_tcp:close(Sock)});
        {handle_call, From, {put, [Key, Value]}} ->
            gen_tcp:send(Sock, iolist_to_binary([?CID_PUT, ?KEYSIZE, ?VALSIZE, Key, Value])),
            reply_(State, From, {handle_call, Self, recv_(Sock, fun recv_success/2)});
        {handle_call, From, {putkeep, [Key, Value]}} ->
            gen_tcp:send(Sock, iolist_to_binary([?CID_PUTKEEP, ?KEYSIZE, ?VALSIZE, Key, Value])),
            reply_(State, From, {handle_call, Self, recv_(Sock, fun recv_success/2)});
        {handle_call, From, {putcat, [Key, Value]}} ->
            gen_tcp:send(Sock, iolist_to_binary([?CID_PUTCAT, ?KEYSIZE, ?VALSIZE, Key, Value])),
            reply_(State, From, {handle_call, Self, recv_(Sock, fun recv_success/2)});
        {handle_call, From, {putsh1, [Key, Value, Width]}} ->
            gen_tcp:send(Sock, iolist_to_binary([?CID_PUTSH1, ?KEYSIZE, ?VALSIZE, <<Width:32>>, Key, Value])),
            reply_(State, From, {handle_call, Self, recv_(Sock, fun recv_success/2)});
        {handle_call, From, {out, [Key]}} ->
            gen_tcp:send(Sock, iolist_to_binary([?CID_OUT, ?KEYSIZE, Key])),
            reply_(State, From, {handle_call, Self, recv_(Sock, fun recv_success/2)});
        {handle_call, From, {get, [Key]}} ->
            gen_tcp:send(Sock, iolist_to_binary([?CID_GET, ?KEYSIZE, Key])),
            reply_(State, From, {handle_call, Self, recv_(Sock, fun recv_size_data/2)});
        {handle_call, From, {mget, [BKeys]}} ->
            KeysCount = length(BKeys),
            Bins = iolist_to_binary(
                        lists:map(
                            fun(Key) -> KeySize = byte_size(Key), iolist_to_binary([<<KeySize:32>>, Key]) end, 
                            BKeys
                        )
                    ),
            gen_tcp:send(Sock, iolist_to_binary([?CID_MGET, <<KeysCount:32>>, Bins])),
            reply_(State, From, {handle_call, Self, recv_(Sock, fun recv_mget/2)});
        {handle_call, From, {vsiz, [Key]}} ->
            gen_tcp:send(Sock, iolist_to_binary([?CID_VSIZ, ?KEYSIZE, Key])),
            reply_(State, From, {handle_call, Self, recv_(Sock, fun recv_size/2)});
        {handle_call, From, {iterinit, []}} ->
            gen_tcp:send(Sock, ?CID_ITERINIT),
            reply_(State, From, {handle_call, Self, recv_(Sock, fun recv_success/2)});
        {handle_call, From, {iternext, []}} ->
            gen_tcp:send(Sock, ?CID_ITERNEXT),
            reply_(State, From, {handle_call, Self, recv_(Sock, fun recv_iternext/2)});
        {handle_call, From, {fwmkeys, [Prefix, MaxKeys]}} ->
            PrefixSize = byte_size(Prefix),
            gen_tcp:send(Sock, iolist_to_binary([?CID_FWMKEYS, <<PrefixSize:32>>, <<MaxKeys:32>>, Prefix])),
            reply_(State, From, {handle_call, Self, recv_(Sock, fun recv_fwmkeys/2)});    
        {handle_call, From, {addint, [Key, N]}} ->
            gen_tcp:send(Sock, iolist_to_binary([?CID_ADDINT, ?KEYSIZE, <<N:32>>, Key])),
            reply_(State, From, {handle_call, Self, recv_(Sock, fun recv_addint/2)});
        {handle_call, From, {adddouble, [Key, Integral, Fractional]}} ->
            gen_tcp:send(Sock, iolist_to_binary([?CID_ADDDOUBLE, ?KEYSIZE, <<Integral:64>>, <<Fractional:64>>, Key])),
            reply_(State, From, {handle_call, Self, recv_(Sock, fun recv_adddouble/2)});
        {handle_call, From, {sync, []}} ->
            gen_tcp:send(Sock, ?CID_SYNC),
            reply_(State, From, {handle_call, Self, recv_(Sock, fun recv_success/2)});
        {handle_call, From, {vanish, []}} ->
            gen_tcp:send(Sock, ?CID_VANISH),
            reply_(State, From, {handle_call, Self, recv_(Sock, fun recv_success/2)});
        {handle_call, From, {rnum, []}} ->
            gen_tcp:send(Sock, ?CID_RNUM),
            reply_(State, From, {handle_call, Self, recv_(Sock, fun recv_size64/2)});
        {handle_call, From, {size, []}} ->
            gen_tcp:send(Sock, ?CID_SIZE),
            reply_(State, From, {handle_call, Self, recv_(Sock, fun recv_size64/2)});                
        {handle_call, From, {stat, []}} ->
            gen_tcp:send(Sock, ?CID_STAT),
            reply_(State, From, {handle_call, Self, recv_(Sock, fun recv_stat/2)});
        {handle_call, From, {copy, [Path]}} ->
            PathSize = byte_size(Path),
            gen_tcp:send(Sock, iolist_to_binary([?CID_COPY, <<PathSize:32>>, Path])),
            reply_(State, From, {handle_call, Self, recv_(Sock, fun recv_success/2)});
        {handle_call, From, {restore, [Path, TS]}} ->
            PathSize = byte_size(Path),
            gen_tcp:send(Sock, iolist_to_binary([?CID_RESTORE, <<PathSize:32>>, <<TS:64>>, Path])),
            reply_(State, From, {handle_call, Self, recv_(Sock, fun recv_success/2)});
        {handle_call, From, {setmst, [Host, Port]}} ->
            HostSize = byte_size(Host),
            gen_tcp:send(Sock, iolist_to_binary([?CID_SETMST, <<HostSize:32>>, <<Port:32>>, Host])),
            reply_(State, From, {handle_call, Self, recv_(Sock, fun recv_success/2)});
        {handle_call, From, _} ->
            reply_(State, From, {handle_call, Self, {error, invalid_call}});
        {handle_cast, {putnr, [Key, Value]}} ->
            gen_tcp:send(Sock, iolist_to_binary([?CID_PUTNR, ?KEYSIZE, ?VALSIZE, Key, Value])),
            State;
        _ ->
            void
    end,
    loop(NewState).

%% send reply back to method calling process
reply_(State, DestPid, Reply) ->
    NewState = case Reply of
        {_, _, {error, conn_closed}} -> reconnect(State);
        {_, _, {error, conn_error}} -> reconnect(State);
        _ -> State
    end,
    DestPid ! Reply,
    NewState.

reconnect(#state{host=Host, port=Port}) ->
    {ok, SockNew} = gen_tcp:connect(Host, Port, ?TCP_OPTS),
    #state{host=Host, port=Port, socket=SockNew}.

%% receive response from the server via the socket
recv_(Sock, CustomHandler) ->
    receive
        {tcp, Sock, <<Code:8>>} when Code =/= 0 -> {error, Code};
        {tcp_closed, Sock} -> {error, conn_closed};
        {tcp_error, Sock, _Reason} -> {error, conn_error};
        Reply -> CustomHandler(Sock, Reply)
    after ?TIMEOUT -> {error, timeout}
    end.

recv_mget(Sock, Reply) -> recv_count_4tuple(Sock, Reply).
recv_fwmkeys(Sock, Reply) -> recv_count_2tuple(Sock, Reply).
recv_addint(Sock, Reply) -> recv_size(Sock, Reply).
recv_adddouble(Sock, Reply) -> recv_size64_size64(Sock, Reply).
recv_stat(Sock, Reply) -> recv_size_data(Sock, Reply).
recv_iternext(Sock, Reply) -> binary_to_list(recv_size_data(Sock, Reply)).

%%====================================================================
%% Generic handlers for common reply formats
%%====================================================================

%% receive 8-bit success flag
recv_success(_Sock, {tcp, _, ?SUCCESS}) -> ok.

%% receive 8-bit success flag + 32-bit int
recv_size(_Sock, {tcp, _, <<0:8, ValSize:32>>}) -> ValSize.

%% receive 8-bit success flag + 64-bit int
recv_size64(_Sock, {tcp, _, <<0:8, ValSize:64>>}) -> ValSize.

%% receive 8-bit success flag + 64-bit int + 64-bit int
recv_size64_size64(_Sock, {tcp, _, <<0:8, V1:64, V2:64>>}) -> {V1, V2}.

%% receive 8-bit success flag + length1 + data1
recv_size_data(Sock, Reply) ->
    case Reply of
        {tcp, _, <<0:8, Length:32, Rest/binary>>} ->
            {Value, <<>>} = recv_until(Sock, Rest, Length),
            Value
    end.

%% receive 8-bit success flag + count + (length1, length2, data1, data2)*count
recv_count_4tuple(Sock, Reply) ->
    case Reply of
        {tcp, _, <<0:8, RecCnt:32, Rest/binary>>} ->
            {KVS, _} = lists:mapfoldl(
                            fun(_N, Acc) ->
                                <<KeySize:32, ValSize:32, Bin/binary>> = Acc,
                                {Key, Rest1} = recv_until(Sock, Bin, KeySize),
                                {Value, Rest2} = recv_until(Sock, Rest1, ValSize),
                                {{binary_to_list(Key), Value}, Rest2}
                            end, 
                            Rest, lists:seq(1, RecCnt)
                        ),
            KVS
    end.

%% receive 8-bit success flag + count + (length1, data1)*count
recv_count_2tuple(Sock, Reply) ->
    case Reply of
        {tcp, _, <<0:8, Cnt:32, Rest/binary>>} ->
            {Keys, _} = lists:mapfoldl(
                            fun(_N, Acc) ->
                                <<KeySize:32, Bin/binary>> = Acc,
                                {Key, Rest1} = recv_until(Sock, Bin, KeySize),
                                {binary_to_list(Key), Rest1}
                            end,
                            Rest, lists:seq(1, Cnt)
                        ),
            Keys
    end.

%%====================================================================
%% Utils
%%====================================================================

recv_until(Sock, Bin, ReqLength) when byte_size(Bin) < ReqLength ->
    receive
        {tcp, Sock, Data} ->
            Combined = <<Bin/binary, Data/binary>>,
            recv_until(Sock, Combined, ReqLength);
     	{error, closed} ->
  			connection_closed
    after ?TIMEOUT -> timeout
    end;    
recv_until(_Sock, Bin, ReqLength) when byte_size(Bin) =:= ReqLength ->
    {Bin, <<>>};
recv_until(_Sock, Bin, ReqLength) when byte_size(Bin) > ReqLength ->
    <<Required:ReqLength/binary, Rest/binary>> = Bin,
    {Required, Rest}.
