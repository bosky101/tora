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

-define(CID_PUT, <<16#c810:16>>).
-define(CID_GET, <<16#c830:16>>).
-define(CID_VANISH, <<16#c871:16>>).
-define(CID_RNUM, <<16#c880:16>>).

-export([
        start/0, start/2, start_link/0, start_link/2, stop/1, stop/2,
        put/3, put/4, get/2, get/3, 
        vanish/1, vanish/2, rnum/1, rnum/2
    ]).

-record(state, {host, port, socket}).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include("test/tora_conn_test.erl").
-endif.

%%====================================================================
%% Public API
%%====================================================================

start() ->
    start(?TT_DEFAULT_HOST, ?TT_DEFAULT_PORT).
start(Host, Port) when is_list(Host) andalso is_integer(Port) ->
    start(fun spawn/1, Host, Port).

start_link() ->
    start_link(?TT_DEFAULT_HOST, ?TT_DEFAULT_PORT).
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

stop(ConnHandler) -> stop(ConnHandler, self()).
stop(ConnHandler, From) -> call_(terminate, ConnHandler, From, []).

put(ConnHandler, Key, Value) ->
    put(ConnHandler, Key, Value, self()).
put(ConnHandler, Key, Value, From) when is_list(Key) andalso is_binary(Value) ->
    call_(put, ConnHandler, From, [list_to_binary(Key), Value]).


get(ConnHandler, Key) ->
    get(ConnHandler, Key, self()).
get(ConnHandler, Key, From) when is_list(Key) ->
    call_(get, ConnHandler, From, [list_to_binary(Key)]).

vanish(ConnHandler) -> vanish(ConnHandler, self()).
vanish(ConnHandler, From) -> call_(vanish, ConnHandler, From, []).

rnum(ConnHandler) -> rnum(ConnHandler, self()).
rnum(ConnHandler, From) -> call_(rnum, ConnHandler, From, []).

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
        {handle_call, From, {get, [Key]}} ->
            gen_tcp:send(Sock, iolist_to_binary([?CID_GET, ?KEYSIZE, Key])),
            reply_(State, From, {handle_call, Self, recv_(Sock, fun recv_size_data/2)});
        {handle_call, From, {vanish, []}} ->
            gen_tcp:send(Sock, ?CID_VANISH),
            reply_(State, From, {handle_call, Self, recv_(Sock, fun recv_success/2)});
        {handle_call, From, {rnum, []}} ->
            gen_tcp:send(Sock, ?CID_RNUM),
            reply_(State, From, {handle_call, Self, recv_(Sock, fun recv_size64/2)});
        {handle_call, From, _} ->
            reply_(State, From, {handle_call, Self, {error, invalid_call}});
        {handle_cast, {_MethodName, _Args}} ->
            void;
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

%% receive response from the server via the socket
recv_(Sock, CustomHandler) ->
    receive
        {tcp, Sock, <<Code:8>>} when Code =/= 0 -> {error, Code};
        {tcp_closed, Sock} -> {error, conn_closed};
        {tcp_error, Sock, _Reason} -> {error, conn_error};
        Reply -> CustomHandler(Sock, Reply)
    after ?TIMEOUT -> {error, timeout}
    end.

reconnect(#state{host=Host, port=Port}) ->
    {ok, SockNew} = gen_tcp:connect(Host, Port, ?TCP_OPTS),
    #state{host=Host, port=Port, socket=SockNew}.
    
%% receive 8-bit success flag
recv_success(_Sock, {tcp, _, ?SUCCESS}) -> ok.

%% receive 8-bit success flag + length1 + data1
recv_size_data(Sock, Reply) ->
    case Reply of
        {tcp, _, <<0:8, Length:32, Rest/binary>>} ->
            {Value, <<>>} = recv_until(Sock, Rest, Length),
            Value
    end.

%% receive 8-bit success flag + 64-bit int
recv_size64(_Sock, {tcp, _, <<0:8, ValSize:64>>}) -> ValSize.

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
