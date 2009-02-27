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
    start/1, start/3, start_link/1, start_link/3, stop/0, stop/1, 
    add_to_pool/1, add_to_pool/3, pool_size/1, pool_connections/1, pool_create/1, pool_create/3,
    put/3, putkeep/3, putcat/3, putsh1/4, putnr/3, out/2,
    get/2, mget/2, vsiz/2, iterinit/1, iternext/1, 
    fwmkeys/3, addint/3, adddouble/4, sync/1, vanish/1,
    rnum/1, size/1, stat/1
]).
%% -export([ext/4, misc/3]). % NOT IMPLEMENTED
-export([copy/2, restore/3, setmst/3]). % NOT TESTED

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
    pools, % mapping PoolId -> pool tuple
    conns % mapping ConnPid -> pool tuple
}).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include("test/tora_test.erl").
-endif.

%%====================================================================
%% Public API
%%====================================================================

%% @doc
%% Start the tora gen_server and create a default pool with the given PoolId <br/>
%% and add one connection to this pool (connection assumes default hostname and port).
%% @end
start(PoolId) when is_atom(PoolId) ->
    start(PoolId, ?TT_DEFAULT_HOST, ?TT_DEFAULT_PORT).
%% @doc
%% Start the tora gen_server and create a default pool with the given PoolId <br/>
%% and add one connection to this pool (connection uses the supplied hostname and port).
%% @end
start(PoolId, Host, Port) when is_atom(PoolId) andalso is_list(Host) andalso is_integer(Port) ->
    gen_server:start({local, ?SERVER}, ?MODULE, [PoolId, Host, Port], []).

start_link(PoolId) when is_atom(PoolId) ->
    start_link(PoolId, ?TT_DEFAULT_HOST, ?TT_DEFAULT_PORT).
start_link(PoolId, Host, Port) when is_atom(PoolId) andalso is_list(Host) andalso is_integer(Port) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [PoolId, Host, Port], []).

%% @doc spawn a new connection handler and add it to an existing pool
add_to_pool(PoolId) when is_atom(PoolId) ->
    gen_server:call(?SERVER, {PoolId, add_to_pool}).

%% @doc spawn a new connection handler with the given (host, port) and add it to the pool
add_to_pool(PoolId, Host, Port) when is_atom(PoolId) andalso is_list(Host) andalso is_integer(Port) ->
    gen_server:call(?SERVER, {PoolId, add_to_pool, {Host, Port}}).

%% @doc return the number of connections currently present in an existing pool
pool_size(PoolId) when is_atom(PoolId) ->
    gen_server:call(?SERVER, {PoolId, pool_size}).

%% @doc return the list of Pids of the connection handlers in an existing pool
pool_connections(PoolId) when is_atom(PoolId) ->
    gen_server:call(?SERVER, {PoolId, pool_connections}).

%% @doc create a new pool (also automatically creates 1 connection to default host and port)
pool_create(PoolId) ->
    pool_create(PoolId, ?TT_DEFAULT_HOST, ?TT_DEFAULT_PORT).
%% @doc create a new pool (also automatically creates 1 connection to supplied host and port)
pool_create(PoolId, Host, Port) when is_atom(PoolId) ->
    gen_server:call(?SERVER, {PoolId, pool_create, {Host, Port}}).

%% @doc stop the tora gen_server
stop() ->
    gen_server:cast(?SERVER, terminate_all).

%% @doc close all connections in an existing pool
stop(PoolId) when is_atom(PoolId) ->
    gen_server:call(?SERVER, {PoolId, terminate}).

%% @doc store the given key, value pair
put(PoolId, Key, Value) when is_list(Key) andalso is_binary(Value) ->
    gen_server:call(?SERVER, {PoolId, put, {Key, Value}}).

%% @doc 
%% store the given key, value pair only if the given key does not exist already.
%% if it already exists, will throw an error.
%% @end
putkeep(PoolId, Key, Value) when is_list(Key) andalso is_binary(Value) ->
    gen_server:call(?SERVER, {PoolId, putkeep, {Key, Value}}).

%% @doc append Value to the end
putcat(PoolId, Key, Value) when is_list(Key) andalso is_binary(Value) ->
    gen_server:call(?SERVER, {PoolId, putcat, {Key, Value}}).

%% @doc append Value to the end and shift to the left to retain the Width supplied
putsh1(PoolId, Key, Value, Width) when is_list(Key) andalso is_binary(Value) andalso is_integer(Width) ->
    gen_server:call(?SERVER, {PoolId, putsh1, {Key, Value, Width}}).    

%% @doc store the key, value pair but don't wait for response from the server
putnr(PoolId, Key, Value) when is_list(Key) andalso is_binary(Value) ->
    gen_server:cast(?SERVER, {PoolId, putnr, {Key, Value}}).

%% @doc remove the record corresponding to the given key
out(PoolId, Key) when is_list(Key) ->
    gen_server:call(?SERVER, {PoolId, out, {Key}}).

%% @doc get the value for the given key
get(PoolId, Key) when is_list(Key) ->
    gen_server:call(?SERVER, {PoolId, get, {Key}}).

%% @doc multi-get
mget(PoolId, Keys) when is_list(Keys) ->
    gen_server:call(?SERVER, {PoolId, mget, {Keys}}).

%% @doc return the size of the value for the given key
vsiz(PoolId, Key) when is_list(Key) ->
    gen_server:call(?SERVER, {PoolId, vsiz, {Key}}).

%% @doc initialize the iterator to iterate over keys
iterinit(PoolId) -> gen_server:call(?SERVER, {PoolId, iterinit}).

%% @doc return the next key from the iterator
iternext(PoolId) -> gen_server:call(?SERVER, {PoolId, iternext}).

%% @doc return keys which start with the given Prefix (a maximum of MaxKeys are returned)
fwmkeys(PoolId, Prefix, MaxKeys) when is_list(Prefix) andalso is_integer(MaxKeys) -> 
    gen_server:call(?SERVER, {PoolId, fwmkeys, {Prefix, MaxKeys}}).

%% @doc add integer to the value and return the summation value
addint(PoolId, Key, N) when is_list(Key) andalso is_integer(N) ->
    gen_server:call(?SERVER, {PoolId, addint, {Key, N}}).

%% @doc add a real number to the value and return the summation value (NOT TESTED)
adddouble(PoolId, Key, Integral, Fractional) 
    when is_list(Key) andalso is_integer(Integral) andalso is_integer(Fractional) ->
        gen_server:call(?SERVER, {PoolId, adddouble, {Key, Integral, Fractional}}).

%% @doc sync updates to file/device.
sync(PoolId) -> gen_server:call(?SERVER, {PoolId, sync}).

%% @doc remove all records
vanish(PoolId) -> gen_server:call(?SERVER, {PoolId, vanish}).

%% @doc return total number of records
rnum(PoolId) -> gen_server:call(?SERVER, {PoolId, rnum}).

%% @doc size of the database
size(PoolId) -> gen_server:call(?SERVER, {PoolId, size}).

%% @doc status message of the database
stat(PoolId) -> gen_server:call(?SERVER, {PoolId, stat}).

%% @doc
%% copy database to a different file (NOT TESTED)<br/>
%%  Path - specifies the path of the destination file. If it begins with `@', the trailing substring is executed as a command line.<br/>
%% @end
copy(PoolId, Path) when is_list(Path) ->
    gen_server:call(?SERVER, {PoolId, copy, {Path}}).

%% @doc 
%% restore database file from update log (NOT TESTED)<br/>
%%  Path - specifies the path of the update log directory. If it begins with `+', the trailing substring is treated as the path and consistency checking is omitted.<br/>
%%  TS - specifies the beginning timestamp in microseconds.<br/>
%% @end
restore(PoolId, Path, TS) when is_list(Path) andalso is_integer(TS) ->
    gen_server:call(?SERVER, {PoolId, restore, {Path, TS}}).

%% @doc 
%% set the replication master (NOT TESTED)<br/>
%%  Host - specifies the name/address of the server.<br/>
%%  Port - specifies the port number.<br/>
%% @end
setmst(PoolId, Host, Port) when is_list(Host) andalso is_integer(Port) ->
    gen_server:call(?SERVER, {PoolId, setmst, {Host, Port}}).

%%====================================================================
%% gen_server callbacks
%%====================================================================
init([PoolId, Host, Port]) ->
    process_flag(trap_exit, true),
    Pools = gb_trees:empty(),
    Conns = gb_trees:empty(),
    {ok, create_pool(PoolId, Host, Port, #state{pools=Pools, conns=Conns})}.

handle_call({PoolId, terminate}, _From, #state{pools=Pools, conns=Conns}) ->
    {value, #pool{id=PoolId, connections=Connections}} = gb_trees:lookup(PoolId, Pools),
    spawn(fun() ->
            lists:foreach(
                fun(Conn) -> 
                    #connection{pid=ConnPid} = Conn,
                    tora_conn:stop(ConnPid)
                end,
                gb_trees:values(Connections))
        end),
    Pools1 = gb_trees:delete(PoolId, Pools),
    Conns1 = lists:foldl(
                fun(Conn, Acc) ->
                    #connection{pid=ConnPid} = Conn,
                    gb_trees:delete(ConnPid, Acc)
                end,
                Conns,
                gb_trees:values(Connections)),
    {reply, ok, #state{pools=Pools1, conns=Conns1}};

handle_call({PoolId, add_to_pool}, _From, #state{pools=Pools, conns=Conns}) ->
    {value, Pool} = gb_trees:lookup(PoolId, Pools),
    #pool{id=PoolId, connections=Connections} = Pool,
    % use the same (host, port) from the first connection created
    {_, #connection{host=Host, port=Port}} = gb_trees:smallest(Connections),
    {ConnPid, Pool1} = add_conn(Pool, Host, Port),
    Pools1 = gb_trees:enter(PoolId, Pool1, Pools),
    Conns1 = gb_trees:enter(ConnPid, Pool1, Conns),
    {reply, ok, #state{pools=Pools1, conns=Conns1}};
handle_call({PoolId, add_to_pool, {Host, Port}}, _From, #state{pools=Pools, conns=Conns}) ->
    {value, Pool} = gb_trees:lookup(PoolId, Pools),
    {ConnPid, Pool1} = add_conn(Pool, Host, Port),
    Pools1 = gb_trees:enter(PoolId, Pool1, Pools),
    Conns1 = gb_trees:enter(ConnPid, Pool1, Conns),
    {reply, ok, #state{pools=Pools1, conns=Conns1}};    

handle_call({PoolId, pool_size}, _From, #state{pools=Pools}=State) ->
    {value, #pool{connections=Connections}} = gb_trees:lookup(PoolId, Pools),
    PoolSize = gb_trees:size(Connections),
    {reply, PoolSize, State};

handle_call({PoolId, pool_connections}, _From, #state{pools=Pools}=State) ->
    {value, #pool{connections=Connections}} = gb_trees:lookup(PoolId, Pools),
    ConnPids = [ConnPid || #connid{pid=ConnPid} <- gb_trees:keys(Connections)],
    {reply, ConnPids, State};

handle_call({PoolId, pool_create, {Host, Port}}, _From, State) ->
    {reply, ok, create_pool(PoolId, Host, Port, State)};

%% match all remaining API calls
handle_call({PoolId, Method}, From, State) when is_atom(Method) ->
    handle_call({PoolId, Method, {}}, From, State);
handle_call({PoolId, Method, ArgsTuple}, From, #state{pools=Pools, conns=Conns}) 
    when is_atom(Method) andalso is_tuple(ArgsTuple) ->
    Pools1 = with_connection(
        PoolId, Pools,
        fun(ConnHandler) ->
            InvokeArgs = [ConnHandler] ++ tuple_to_list(ArgsTuple) ++ [From],
            erlang:apply(tora_conn, Method, InvokeArgs)
        end
    ),
    {noreply, #state{pools=Pools1, conns=Conns}}.

handle_cast(terminate_all, State) ->
    {stop, normal, State};
handle_cast({PoolId, Method, ArgsTuple}, #state{pools=Pools, conns=Conns}) 
    when is_atom(Method) andalso is_tuple(ArgsTuple) ->
    Pools1 = with_connection(
        PoolId, Pools,
        fun(ConnHandler) ->
            InvokeArgs = [ConnHandler] ++ tuple_to_list(ArgsTuple),
            erlang:apply(tora_conn, Method, InvokeArgs)
        end
    ),
    {noreply, #state{pools=Pools1, conns=Conns}}.

handle_info({'EXIT', FromPid, _Reason}, #state{pools=Pools, conns=Conns}) ->
    case gb_trees:lookup(FromPid, Conns) of
        {value, #pool{id=PoolId, connections=Connections}} ->
            %io:format(user, "removing connection whose pid = ~p from connections=~p~n", [FromPid, Connections]),
            Connections1 = remove_connection_by_pid(Connections, FromPid),
            Pool1 = #pool{id=PoolId, connections=Connections1},
            Pools1 = gb_trees:enter(PoolId, Pool1, Pools),
            Conns1 = gb_trees:delete(FromPid, Conns),
            {noreply, #state{pools=Pools1, conns=Conns1}};
        _ ->
            {noreply, #state{pools=Pools, conns=Conns}}
    end;
handle_info(_Info, State) -> {noreply, State}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

terminate(_Reason, #state{conns=Conns}) ->
    % close all connections
    lists:foreach(
        fun (ConnPid) -> ok = tora_conn:stop(ConnPid) end,
        gb_trees:keys(Conns)),
    ok.

%%====================================================================
%% Private stuff
%%====================================================================
create_pool(PoolId, Host, Port, #state{pools=Pools, conns=Conns}) ->
    Connections = gb_trees:empty(),
    Pool = #pool{id=PoolId, connections=Connections},
    {ConnPid, Pool1} = add_conn(Pool, Host, Port),
    Pools1 = gb_trees:enter(PoolId, Pool1, Pools),
    Conns1 = gb_trees:enter(ConnPid, Pool1, Conns),
    #state{pools=Pools1, conns=Conns1}.

add_conn(Pool, Host, Port) ->
    #pool{id=PoolId, connections=Connections} = Pool,
    {ok, ConnPid} = tora_conn:start_link(Host, Port),
    Connection = #connection{pid=ConnPid, host=Host, port=Port},
    Connections1 = gb_trees:enter(#connid{lf=0, pid=ConnPid}, Connection, Connections),
    {ConnPid, #pool{id=PoolId, connections=Connections1}}.

with_connection(PoolId, Pools, F) ->
    % get a connnection handler
    {value, #pool{id=PoolId, connections=Connections}} = gb_trees:lookup(PoolId, Pools),
    {#connid{lf=Usage, pid=ConnPid}, Connection, Connections1} = gb_trees:take_smallest(Connections),
    
    % assign call to connection handler
    spawn(fun() -> F(ConnPid) end),
    
    % update lf for the connection handler
    Usage1 = Usage + 1,
    Connections2 = gb_trees:enter(#connid{lf=Usage1, pid=ConnPid}, Connection, Connections1),
    Pool1 = #pool{id=PoolId, connections=Connections2},
    gb_trees:enter(PoolId, Pool1, Pools).

remove_connection_by_pid(Connections, ConnPid) ->
    remove_connection_by_pid(gb_trees:keys(Connections), Connections, ConnPid).
remove_connection_by_pid([#connid{lf=LF, pid=ConnPid}|_Rest], Connections, ConnPid) ->
    gb_trees:delete(#connid{lf=LF, pid=ConnPid}, Connections);
remove_connection_by_pid([_H|Rest], Connections, ConnPid) ->
    remove_connection_by_pid(Rest, Connections, ConnPid);
remove_connection_by_pid([], Connections, _ConnPid) ->
    Connections.