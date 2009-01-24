%%%-------------------------------------------------------------------
%%% File:      tora.erl
%%% @author    Harish Mallipeddi <harish.mallipeddi@gmail.com> []
%%% @copyright 2009 Harish Mallipeddi
%%% @doc  
%%%
%%% @end  
%%%
%%% @since Sat Jan 24 16:41:41 SGT 2009 by Harish Mallipeddi
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

%% API
-export([
    connect/0, connect/2, put/2, get/1
]).

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
connect() ->
    connect(?TT_DEFAULT_HOST, ?TT_DEFAULT_PORT).
connect(Host, Port) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Host, Port], []).
put(Key, Value) ->
    gen_server:call(?SERVER, {put, {list_to_binary(Key), Value}}).
get(Key) ->
    gen_server:call(?SERVER, {get, {list_to_binary(Key)}}).

%%====================================================================
%% gen_server callbacks
%%====================================================================
init([Host, Port]) ->
    {ok, Sock} = gen_tcp:connect(Host, Port, ?TCP_OPTS),
    {ok, #state{socket = Sock}}.

handle_call({put, {Key, Value}}, _From, #state{socket=Sock}) ->
    KeySize = byte_size(Key),
    ValSize = byte_size(Value),
    gen_tcp:send(Sock, iolist_to_binary([<<16#c810:16, KeySize:32, ValSize:32>>, Key, Value])),
    Reply = receive
        {tcp, _, <<0:8>>} -> ok;
        {tcp, _, <<Code:8>>} -> {error, Code};
        {error, closed} -> connection_closed
    after ?TIMEOUT ->
        timeout
    end,
    {reply, Reply, #state{socket=Sock}};
handle_call({get, {Key}}, _From, #state{socket=Sock}) ->
    KeySize = byte_size(Key),
    gen_tcp:send(Sock, iolist_to_binary([<<16#c830:16, KeySize:32>>, Key])),
    Reply = receive
        {tcp, _, <<0:8, _ValSize:32, Value/binary>>} -> Value;
        {tcp, _, <<Code:8, _/binary>>} -> {error, Code};
        {error, closed} -> connection_closed
    after ?TIMEOUT ->
        timeout
    end,
    {reply, Reply, #state{socket=Sock}}.

handle_cast(_Msg, State) -> {noreply, State}.

handle_info(_Info, State) -> {noreply, State}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

terminate(_Reason, #state{socket=Sock}) ->
    gen_tcp:close(Sock),
    ok.

%%====================================================================
%% Private stuff
%%====================================================================



