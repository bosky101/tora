%% Copyright 2009, Harish Mallipeddi <harish.mallipeddi@gmail.com>
%%
%% Permission is hereby granted, free of charge, to any person
%% obtaining a copy of this software and associated documentation
%% files (the "Software"), to deal in the Software without
%% restriction, including without limitation the rights to use,
%% copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the
%% Software is furnished to do so, subject to the following
%% conditions:
%%
%% The above copyright notice and this permission notice shall be
%% included in all copies or substantial portions of the Software.
%%
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
%% EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
%% OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
%% NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
%% HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
%% WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
%% FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
%% OTHER DEALINGS IN THE SOFTWARE.

%%%-------------------------------------------------------------------
%%% File:      tora.erl
%%% @author    Harish Mallipeddi <harish.mallipeddi@gmail.com>
%%% @copyright 2009 Harish Mallipeddi
%%% @doc
%%% An Erlang client for Tokyo Tyrant (speaks Tokyo Tyrant's TCP/IP protocol).
%%% @end
%%% @version   0.1
%%% @reference See <a href="http://tokyocabinet.sourceforge.net/tyrantdoc/">Tokyo Tyrant Docs</a> for more info.
%%% @since     Sat Jan 24 16:41:41 SGT 2009 by Harish Mallipeddi
%%%-------------------------------------------------------------------

-module(tora).
-author('harish.mallipeddi@gmail.com').

-behaviour(gen_server).

-define(SERVER, ?MODULE).
-define(TT_DEFAULT_HOST, "localhost").
-define(TT_DEFAULT_PORT, 1978).

%% API
-export([
    start/1, start/3, start_link/1, start_link/3, stop/1, 
    add_to_pool/1, add_to_pool/3, connections_in_pool/1,
    put/3, get/2, 
    rnum/1, vanish/1
]).

%% gen_server callbacks
-export([
    init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3
]).

-record(connid, {
    lf, % count of how many outstanding calls are assigned to this connection
    pid % pid of the process handling the connection
}).
-record(connection, {
    pid, % pid of the process handling the connection
    host,
    port
}).
-record(pool, {
    id, % PoolId
    connections % all connections within this pool
}).
-record(state, {
    pools % mapping PoolId -> pool tuple
}).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include("test/tora_test.erl").
-endif.

%%====================================================================
%% Public API
%%====================================================================

start(PoolId) when is_atom(PoolId) ->
    start(PoolId, ?TT_DEFAULT_HOST, ?TT_DEFAULT_PORT).
start(PoolId, Host, Port) when is_atom(PoolId) andalso is_list(Host) andalso is_integer(Port) ->
    gen_server:start({local, ?SERVER}, ?MODULE, [PoolId, Host, Port], []).

start_link(PoolId) when is_atom(PoolId) ->
    start_link(PoolId, ?TT_DEFAULT_HOST, ?TT_DEFAULT_PORT).
start_link(PoolId, Host, Port) when is_atom(PoolId) andalso is_list(Host) andalso is_integer(Port) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [PoolId, Host, Port], []).

%% @doc spawn a new connection handler and add it to the pool
add_to_pool(PoolId) when is_atom(PoolId) ->
    gen_server:call(?SERVER, {PoolId, add_to_pool}).

%% @doc spawn a new connection handler with the given (host, port) and add it to the pool
add_to_pool(PoolId, Host, Port) when is_atom(PoolId) andalso is_list(Host) andalso is_integer(Port) ->
    gen_server:call(?SERVER, {PoolId, add_to_pool, {Host, Port}}).

%% @doc return the number of connections currently present in the pool
connections_in_pool(PoolId) when is_atom(PoolId) ->
    gen_server:call(?SERVER, {PoolId, connections_in_pool}).

stop(PoolId) when is_atom(PoolId) ->
    gen_server:call(?SERVER, {PoolId, terminate}).

%% @doc store the given key, value pair
put(PoolId, Key, Value) when is_list(Key) andalso is_binary(Value) ->
    gen_server:call(?SERVER, {PoolId, put, {Key, Value}}).

%% @doc get the value for the given key
get(PoolId, Key) when is_list(Key) ->
    gen_server:call(?SERVER, {PoolId, get, {Key}}).

%% @doc remove all records
vanish(PoolId) -> gen_server:call(?SERVER, {PoolId, vanish}).

%% @doc return total number of records
rnum(PoolId) -> gen_server:call(?SERVER, {PoolId, rnum}).

%%====================================================================
%% gen_server callbacks
%%====================================================================
init([PoolId, Host, Port]) ->
    {ok, ConnPid} = tora_conn:start(Host, Port),
    Connection = #connection{pid=ConnPid, host=Host, port=Port},
    Connections = gb_trees:enter(#connid{lf=0, pid=ConnPid}, Connection, gb_trees:empty()),
    Pool = #pool{id=PoolId, connections=Connections},
    Pools = gb_trees:enter(PoolId, Pool, gb_trees:empty()),
    State = #state{pools=Pools},
    {ok, State}.

handle_call({PoolId, terminate}, _From, #state{pools=Pools}) ->
    {value, #pool{id=PoolId, connections=Connections}} = gb_trees:lookup(PoolId, Pools),
    spawn(fun() ->
            lists:map(
                fun(Conn) -> #connection{pid=ConnPid} = Conn, tora_conn:stop(ConnPid) end,
                gb_trees:values(Connections))
        end),
    Pools1 = gb_trees:delete(PoolId, Pools),
    {reply, ok, #state{pools=Pools1}};

handle_call({PoolId, add_to_pool}, _From, #state{pools=Pools}) ->
    {value, Pool} = gb_trees:lookup(PoolId, Pools),
    #pool{id=PoolId, connections=Connections} = Pool,
    % use the same (host, port) from the first connection created
    {_, #connection{host=Host, port=Port}} = gb_trees:smallest(Connections),
    Pool1 = add_conn(Pool, Host, Port),
    Pools1 = gb_trees:enter(PoolId, Pool1, Pools),
    {reply, ok, #state{pools=Pools1}};
handle_call({PoolId, add_to_pool, {Host, Port}}, _From, #state{pools=Pools}) ->
    {value, Pool} = gb_trees:lookup(PoolId, Pools),
    Pool1 = add_conn(Pool, Host, Port),
    Pools1 = gb_trees:enter(PoolId, Pool1, Pools),
    {reply, ok, #state{pools=Pools1}};    

handle_call({PoolId, connections_in_pool}, _From, #state{pools=Pools}) ->
    {value, #pool{connections=Connections}} = gb_trees:lookup(PoolId, Pools),
    PoolSize = gb_trees:size(Connections),
    {reply, PoolSize, #state{pools=Pools}};

handle_call({PoolId, put, {Key, Value}}, From, #state{pools=Pools}) ->
    Pools1 = with_connection(
        PoolId, Pools,
        fun(ConnHandler) -> tora_conn:put(ConnHandler, Key, Value, From) end
    ),
    {noreply, #state{pools=Pools1}};

handle_call({PoolId, get, {Key}}, From, #state{pools=Pools}) ->
    Pools1 = with_connection(
        PoolId, Pools,
        fun(ConnHandler) -> tora_conn:get(ConnHandler, Key, From) end
    ),
    {noreply, #state{pools=Pools1}};

handle_call({PoolId, vanish}, From, #state{pools=Pools}) ->
    Pools1 = with_connection(
        PoolId, Pools,
        fun(ConnHandler) -> tora_conn:vanish(ConnHandler, From) end
    ),
    {noreply, #state{pools=Pools1}};

handle_call({PoolId, rnum}, From, #state{pools=Pools}) ->
    Pools1 = with_connection(
        PoolId, Pools,
        fun(ConnHandler) -> tora_conn:rnum(ConnHandler, From) end
    ),
    {noreply, #state{pools=Pools1}}.

handle_cast(_Msg, State) -> {noreply, State}.

handle_info(_Info, State) -> {noreply, State}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

terminate(_Reason, #state{pools=_Pools}) ->
    ok.

%%====================================================================
%% Private stuff
%%====================================================================
add_conn(Pool, Host, Port) ->
    #pool{id=PoolId, connections=Connections} = Pool,
    {ok, ConnPid} = tora_conn:start(Host, Port),
    Connection = #connection{pid=ConnPid, host=Host, port=Port},
    Connections1 = gb_trees:enter(#connid{lf=0, pid=ConnPid}, Connection, Connections),
    #pool{id=PoolId, connections=Connections1}.

with_connection(PoolId, Pools, F) ->
    % get a connnection handler
    {value, #pool{id=PoolId, connections=Connections}} = gb_trees:lookup(PoolId, Pools),
    {#connid{lf=Usage, pid=ConnPid}, Connection, Connections1} = gb_trees:take_smallest(Connections),
    
    % assign call to connection handler
    F(ConnPid),
    
    % update lf for the connection handler
    Usage1 = Usage + 1,
    Connections2 = gb_trees:enter(#connid{lf=Usage1, pid=ConnPid}, Connection, Connections1),
    Pool1 = #pool{id=PoolId, connections=Connections2},
    gb_trees:enter(PoolId, Pool1, Pools).
