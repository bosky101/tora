%%%-------------------------------------------------------------------
%%% File:      tora.erl
%%% @author    Harish Mallipeddi <harish.mallipeddi@gmail.com>
%%% @copyright 2009 Harish Mallipeddi
%%% @doc       An Erlang client for Tokyo Tyrant (speaks Tokyo Tyrant's TCP/IP protocol).
%%% @version   0.1
%%% @reference See <a href="http://tokyocabinet.sourceforge.net/tyrantdoc/">Tokyo Tyrant Docs</a> for more info.
%%% @since     Sat Jan 24 16:41:41 SGT 2009 by Harish Mallipeddi
%%%-------------------------------------------------------------------
-module(tora).
-author('harish.mallipeddi@gmail.com').

-behaviour(gen_server).

-define(SERVER, ?MODULE).
-define(TIMEOUT, 5000).
-define(TCP_OPTS, [
    binary, {packet, raw}, {nodelay, true},{reuseaddr, true}, {active, true}
]).
-define(TT_DEFAULT_HOST, "localhost").
-define(TT_DEFAULT_PORT, 1978).

-define(KEYSIZE, apply(fun () -> KeySize = byte_size(Key), <<KeySize:32>> end, [])).
-define(VALSIZE, apply(fun () -> ValSize = byte_size(Value), <<ValSize:32>> end, [])).

-define(SUCCESS, <<0:8>>).

%% Tokyo Tyrant protocol - Command IDs
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
-define(CID_SYNC, <<16#c870:16>>).
-define(CID_VANISH, <<16#c871:16>>).

%% API
-export([
    connect/0, connect/2, 
    put/2, putkeep/2, putcat/2, putsh1/3, putnr/2, out/1,
    get/1, mget/1, vsiz/1, iterinit/0, iternext/0,
    fwmkeys/2, addint/2, sync/0, vanish/0
]).
%% -export([ext/?, adddouble/3]).

%% gen_server callbacks
-export([
    init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3
]).

-record(state, {socket}).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include("test/tora_test.erl").
-endif.

%%====================================================================
%% Public API
%%====================================================================

%% @doc connect to a tokyo tyrant server running on localhost:1978
connect() ->
    connect(?TT_DEFAULT_HOST, ?TT_DEFAULT_PORT).

%% @doc connect to a tokyo tyrant server running on the given hostname:port
connect(Host, Port) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Host, Port], []).

%% @doc store the given key, value pair
put(Key, Value) when is_list(Key) andalso is_binary(Value) ->
    gen_server:call(?SERVER, {put, {list_to_binary(Key), Value}}).

%% @doc 
%% store the given key, value pair only if the given key does not exist already.
%% if it already exists, will throw an error.
%% @end
putkeep(Key, Value) when is_list(Key) andalso is_binary(Value) ->
    gen_server:call(?SERVER, {putkeep, {list_to_binary(Key), Value}}).

%% @doc append Value to the end
putcat(Key, Value) when is_list(Key) andalso is_binary(Value) ->
    gen_server:call(?SERVER, {putcat, {list_to_binary(Key), Value}}).

%% @doc append Value to the end and shift to the left to retain the Width supplied
putsh1(Key, Value, Width) when is_list(Key) andalso is_binary(Value) andalso is_integer(Width) ->
    gen_server:call(?SERVER, {putsh1, {list_to_binary(Key), Value, Width}}).    

%% @doc store the key, value pair but don't wait for response from the server
putnr(Key, Value) when is_list(Key) andalso is_binary(Value) ->
    gen_server:cast(?SERVER, {putnr, {list_to_binary(Key), Value}}).

%% @doc remove the record corresponding to the given key
out(Key) when is_list(Key) ->
    gen_server:call(?SERVER, {out, {list_to_binary(Key)}}).

%% @doc get the value for the given key
get(Key) when is_list(Key) ->
    gen_server:call(?SERVER, {get, {list_to_binary(Key)}}).

%% @doc multi-get
mget(Keys) when is_list(Keys) ->
    BKeys = [list_to_binary(Key) || Key <- Keys],
    gen_server:call(?SERVER, {mget, {BKeys}}).

%% @doc return the size of the value for the given key
vsiz(Key) when is_list(Key) ->
    gen_server:call(?SERVER, {vsiz, {list_to_binary(Key)}}).

%% @doc initialize the iterator to iterate over keys
iterinit() -> gen_server:call(?SERVER, {iterinit}).

%% @doc return the next key from the iterator
iternext() -> gen_server:call(?SERVER, {iternext}).

%% @doc return keys which start with the given Prefix (a maximum of MaxKeys are returned)
fwmkeys(Prefix, MaxKeys) when is_list(Prefix) andalso is_integer(MaxKeys) -> 
    gen_server:call(?SERVER, {fwmkeys, {list_to_binary(Prefix), MaxKeys}}).

%% @doc add integer N to the value and return the sum
addint(Key, N) when is_list(Key) andalso is_integer(N) ->
    gen_server:call(?SERVER, {addint, {list_to_binary(Key), N}}).

%% @doc sync
sync() -> gen_server:call(?SERVER, {sync}).

%% @doc vanish
vanish() -> gen_server:call(?SERVER, {vanish}).

%%====================================================================
%% gen_server callbacks
%%====================================================================
init([Host, Port]) ->
    {ok, Sock} = gen_tcp:connect(Host, Port, ?TCP_OPTS),
    {ok, #state{socket = Sock}}.

handle_call({put, {Key, Value}}, _From, #state{socket=Sock}) ->
    gen_tcp:send(Sock, iolist_to_binary([?CID_PUT, ?KEYSIZE, ?VALSIZE, Key, Value])),
    Handler = fun(Reply) -> recv_success(Sock, Reply) end,
    {reply, rr(Handler), #state{socket=Sock}};

handle_call({putkeep, {Key, Value}}, _From, #state{socket=Sock}) ->
    gen_tcp:send(Sock, iolist_to_binary([?CID_PUTKEEP, ?KEYSIZE, ?VALSIZE, Key, Value])),
    Handler = fun(Reply) -> recv_success(Sock, Reply) end,
    {reply, rr(Handler), #state{socket=Sock}};

handle_call({putcat, {Key, Value}}, _From, #state{socket=Sock}) ->
    gen_tcp:send(Sock, iolist_to_binary([?CID_PUTCAT, ?KEYSIZE, ?VALSIZE, Key, Value])),
    Handler = fun(Reply) -> recv_success(Sock, Reply) end,
    {reply, rr(Handler), #state{socket=Sock}};

handle_call({putsh1, {Key, Value, Width}}, _From, #state{socket=Sock}) ->
    gen_tcp:send(Sock, iolist_to_binary([?CID_PUTSH1, ?KEYSIZE, ?VALSIZE, <<Width:32>>, Key, Value])),
    Handler = fun(Reply) -> recv_success(Sock, Reply) end,
    {reply, rr(Handler), #state{socket=Sock}};

handle_call({out, {Key}}, _From, #state{socket=Sock}) ->
    gen_tcp:send(Sock, iolist_to_binary([?CID_OUT, ?KEYSIZE, Key])),
    Handler = fun(Reply) -> recv_success(Sock, Reply) end,
    {reply, rr(Handler), #state{socket=Sock}};

handle_call({get, {Key}}, _From, #state{socket=Sock}) ->
    gen_tcp:send(Sock, iolist_to_binary([?CID_GET, ?KEYSIZE, Key])),
    Handler = fun(Reply) -> recv_size_data(Sock, Reply) end,
    {reply, rr(Handler), #state{socket=Sock}};

handle_call({mget, {BKeys}}, _From, #state{socket=Sock}) ->
    KeysCount = length(BKeys),
    Bins = iolist_to_binary(
                lists:map(
                    fun(Key) -> KeySize = byte_size(Key), iolist_to_binary([<<KeySize:32>>, Key]) end, 
                    BKeys
                )
            ),
    gen_tcp:send(Sock, iolist_to_binary([?CID_MGET, <<KeysCount:32>>, Bins])),
    Handler = fun(Reply) -> recv_mget(Sock, Reply) end,
    {reply, rr(Handler), #state{socket=Sock}};

handle_call({vsiz, {Key}}, _From, #state{socket=Sock}) ->
    gen_tcp:send(Sock, iolist_to_binary([?CID_VSIZ, ?KEYSIZE, Key])),
    Handler = fun(Reply) -> recv_size(Sock, Reply) end,
    {reply, rr(Handler), #state{socket=Sock}};

handle_call({iterinit}, _From, #state{socket=Sock}) ->
    gen_tcp:send(Sock, ?CID_ITERINIT),
    Handler = fun(Reply) -> recv_success(Sock, Reply) end,
    {reply, rr(Handler), #state{socket=Sock}};

handle_call({iternext}, _From, #state{socket=Sock}) ->
    gen_tcp:send(Sock, ?CID_ITERNEXT),
    Handler = fun(Reply) -> recv_size_data(Sock, Reply) end,
    {reply, binary_to_list(rr(Handler)), #state{socket=Sock}};

handle_call({fwmkeys, {Prefix, MaxKeys}}, _From, #state{socket=Sock}) ->
    PrefixSize = byte_size(Prefix),
    gen_tcp:send(Sock, iolist_to_binary([?CID_FWMKEYS, <<PrefixSize:32>>, <<MaxKeys:32>>, Prefix])),
    Handler = fun(Reply) -> recv_fwmkeys(Sock, Reply) end,
    {reply, rr(Handler), #state{socket=Sock}};

handle_call({addint, {Key, N}}, _From, #state{socket=Sock}) ->
    gen_tcp:send(Sock, iolist_to_binary([?CID_ADDINT, ?KEYSIZE, <<N:32>>, Key])),
    Handler = fun(Reply) -> recv_addint(Sock, Reply) end,
    {reply, rr(Handler), #state{socket=Sock}};

handle_call({sync}, _From, #state{socket=Sock}) ->
    gen_tcp:send(Sock, ?CID_SYNC),
    Handler = fun(Reply) -> recv_success(Sock, Reply) end,
    {reply, rr(Handler), #state{socket=Sock}};

handle_call({vanish}, _From, #state{socket=Sock}) ->
    gen_tcp:send(Sock, ?CID_VANISH),
    Handler = fun(Reply) -> recv_success(Sock, Reply) end,
    {reply, rr(Handler), #state{socket=Sock}}.

handle_cast({putnr, {Key, Value}}, #state{socket=Sock}) ->
    gen_tcp:send(Sock, iolist_to_binary([?CID_PUTNR, ?KEYSIZE, ?VALSIZE, Key, Value])),
    {noreply, #state{socket=Sock}};
handle_cast(_Msg, State) -> {noreply, State}.

handle_info(_Info, State) -> {noreply, State}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

terminate(_Reason, #state{socket=Sock}) ->
    gen_tcp:close(Sock),
    ok.

%%====================================================================
%% Private stuff
%%====================================================================

rr(Handler) ->
    receive
        {tcp, _, <<Code:8>>} when Code =/= 0 -> {error, Code};
        {error, closed} -> connection_closed;
        Reply -> Handler(Reply)
    after ?TIMEOUT -> timeout
    end.

recv_mget(Sock, Reply) -> recv_count_4tuple(Sock, Reply).
recv_fwmkeys(Sock, Reply) -> recv_count_2tuple(Sock, Reply).
recv_addint(Sock, Reply) -> recv_size(Sock, Reply).

%%====================================================================
%% Generic handlers for common reply formats
%%====================================================================

%% receive 8-bit success flag
recv_success(_Sock, {tcp, _, ?SUCCESS}) -> ok.

%% receive 8-bit success flag + 32-bit int
recv_size(_Sock, {tcp, _, <<0:8, ValSize:32>>}) -> ValSize.

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
 