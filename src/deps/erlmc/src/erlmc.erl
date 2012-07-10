%% Copyright (c) 2009
%% Jacob Vorreuter <jacob.vorreuter@gmail.com>
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
%%
%% http://code.google.com/p/memcached/wiki/MemcacheBinaryProtocol
%% @doc a binary protocol memcached client
-module(erlmc).

-export([start/0, start/1, start_link/0, start_link/1, init/2,
         add_server/3, remove_server/2, refresh_server/3, has_server/2,
         add_connection/2, remove_connection/2]).

%% api callbacks
-export([get/1, get/2, get_many/1, add/2, add/3, set/2, set/3,
         replace/2, replace/3, delete/1, increment/4, decrement/4,
         append/2, prepend/2, stats/0, stats/2, flush/0, flush/1, quit/0,
         setq/2, setq/3, deleteq/1, replaceq/2, replaceq/3, incrementq/4,
     decrementq/4, version/0]).

-include("erlmc.hrl").

-define(TIMEOUT, 5000).

%%--------------------------------------------------------------------
%%% API
%%--------------------------------------------------------------------
start() -> start([{"localhost", 11211, 1}]).
start(CacheServers) when is_list(CacheServers) ->
    random:seed(now()),
    case proc_lib:start(?MODULE, init, [self(), CacheServers], 5000) of
        {ok, _Pid} -> ok;
        Error -> Error
    end.

start_link() -> start_link([{"localhost", 11211, 1}]).
start_link(CacheServers) when is_list(CacheServers) ->
    random:seed(now()),
    proc_lib:start_link(?MODULE, init, [self(), CacheServers], 5000).

add_server(Host, Port, PoolSize) ->
    erlang:send(?MODULE, {add_server, Host, Port, PoolSize}),
    ok.

refresh_server(Host, Port, PoolSize) ->
    erlang:send(?MODULE, {refresh_server, Host, Port, PoolSize}),
    ok.

remove_server(Host, Port) ->
    erlang:send(?MODULE, {remove_server, Host, Port}),
    ok.

has_server(Host, Port) ->
    erlang:send(?MODULE, {has_server, self(), Host, Port}),

    receive
        {has_server_result, B} when is_boolean(B) -> B
    after
        5000 -> unknown
    end.

add_connection(Host, Port) ->
    erlang:send(?MODULE, {add_connection, Host, Port}),
    ok.

remove_connection(Host, Port) ->
    erlang:send(?MODULE, {remove_connection, Host, Port}),
    ok.

get(Key0) ->
  get(Key0, ?TIMEOUT).

get(Key0, Timeout) ->
    Key = package_key(Key0),
  call(map_key(Key), {get, Key}, Timeout).

get_many(Keys) ->
    Self = self(),
  SplitKeys = split_keys(Keys),
    Pids = [spawn(fun() ->
        Res = (catch call(unique_connection(Host, Port, NC), {get_many, SubKeys}, ?TIMEOUT)),
        Self ! {self(), Res}
     end) || {{{Host, Port}, NC}, SubKeys} <- SplitKeys],
    lists:append(lists:foldl(
        fun(Pid, Acc) ->
            receive
                {Pid, Res} -> [Res | Acc]
            after ?TIMEOUT ->
                Acc
            end
        end, [], Pids)).

add(Key, Value) ->
    add(Key, Value, 0).

add(Key0, Value, Expiration) when is_binary(Value), is_integer(Expiration) ->
    Key = package_key(Key0),
    call(map_key(Key), {add, Key, Value, Expiration}, ?TIMEOUT).

set(Key, Value) ->
    set(Key, Value, 0).

set(Key0, Value, Expiration) when is_binary(Value), is_integer(Expiration) ->
    Key = package_key(Key0),
    call(map_key(Key), {set, Key, Value, Expiration}, ?TIMEOUT).

setq(Key, Value) ->
    setq(Key, Value, 0).

setq(Key0, Value, Expiration) when is_binary(Value), is_integer(Expiration) ->
    Key = package_key(Key0),
    gen_server:cast(map_key(Key), {setq, Key, Value, Expiration}).

replace(Key, Value) ->
    replace(Key, Value, 0).

replace(Key0, Value, Expiration) when is_binary(Value), is_integer(Expiration) ->
    Key = package_key(Key0),
    call(map_key(Key), {replace, Key, Value, Expiration}, ?TIMEOUT).

replaceq(Key, Value) ->
    replaceq(Key, Value, 0).

replaceq(Key0, Value, Expiration) when is_binary(Value), is_integer(Expiration) ->
    Key = package_key(Key0),
    gen_server:cast(map_key(Key), {replaceq, Key, Value, Expiration}).

delete(Key0) ->
    Key = package_key(Key0),
    call(map_key(Key), {delete, Key}, ?TIMEOUT).

deleteq(Key0) ->
    Key = package_key(Key0),
    gen_server:cast(map_key(Key), {deleteq, Key}).

increment(Key0, Value, Initial, Expiration) when is_integer(Value), is_integer(Initial), is_integer(Expiration) ->
    Key = package_key(Key0),
    call(map_key(Key), {increment, Key, Value, Initial, Expiration}, ?TIMEOUT).

incrementq(Key0, Value, Initial, Expiration) when is_integer(Value), is_integer(Initial), is_integer(Expiration) ->
    Key = package_key(Key0),
    gen_server:cast(map_key(Key), {incrementq, Key, Value, Initial, Expiration}).

decrement(Key0, Value, Initial, Expiration) when is_integer(Value), is_integer(Initial), is_integer(Expiration) ->
    Key = package_key(Key0),
    call(map_key(Key), {decrement, Key, Value, Initial, Expiration}, ?TIMEOUT).

decrementq(Key0, Value, Initial, Expiration) when is_integer(Value), is_integer(Initial), is_integer(Expiration) ->
    Key = package_key(Key0),
    gen_server:cast(map_key(Key), {decrementq, Key, Value, Initial, Expiration}).

append(Key0, Value) when is_binary(Value) ->
    Key = package_key(Key0),
    call(map_key(Key), {append, Key, Value}, ?TIMEOUT).

prepend(Key0, Value) when is_binary(Value) ->
    Key = package_key(Key0),
    call(map_key(Key), {prepend, Key, Value}, ?TIMEOUT).

stats() ->
    multi_call(stats).

stats(Host, Port) ->
    host_port_call(Host, Port, stats).

flush() ->
    multi_call(flush).

flush(Expiration) when is_integer(Expiration) ->
    multi_call({flush, Expiration}).

quit() ->
    [begin
        {Key, [
            {'EXIT',{shutdown,{gen_server,call,[Pid,quit,?TIMEOUT]}}} ==
                (catch gen_server:call(Pid, quit, ?TIMEOUT)) || Pid <- Pids]}
     end || {Key, Pids} <- unique_connections()].

version() ->
    multi_call(version).

multi_call(Msg) ->
    [begin
        Pid = lists:nth(random:uniform(length(Pids)), Pids),
        {{Host, Port}, gen_server2:call(Pid, Msg, ?TIMEOUT)}
    end || {{Host, Port}, Pids} <- unique_connections()].

host_port_call(Host, Port, Msg) ->
    Pid = unique_connection(Host, Port),
    gen_server2:call(Pid, Msg, ?TIMEOUT).

call(Pid, Msg, Timeout) ->
    case gen_server2:call(Pid, Msg, Timeout) of
        {error, Error} -> exit({erlmc, Error});
        Resp -> Resp
    end.

%%--------------------------------------------------------------------
%%% Stateful loop
%%--------------------------------------------------------------------
init(Parent, CacheServers) ->
    process_flag(trap_exit, true),
    register(erlmc, self()),
    ets:new(erlmc_continuum, [ordered_set, protected, named_table]),
    ets:new(erlmc_connections, [bag, protected, named_table]),

    %% Continuum = [{uint(), {Host, Port}}]
    [add_server_to_continuum(Host, Port) || {Host, Port, _} <- CacheServers],

    %% Connections = [{{Host,Port}, ConnPid}]
    [begin
        [start_connection(Host, Port) || _ <- lists:seq(1, ConnPoolSize)]
     end || {Host, Port, ConnPoolSize} <- CacheServers],

    proc_lib:init_ack(Parent, {ok, self()}),

    loop().

loop() ->
    receive
        {add_server, Host, Port, ConnPoolSize} ->
            add_server_to_continuum(Host, Port),
      revalidate_connections(Host, Port),
      [start_connection(Host, Port) || _ <- lists:seq(1, ConnPoolSize)];
    {refresh_server, Host, Port, ConnPoolSize} ->
      % adding to continuum is idempotent
      add_server_to_continuum(Host, Port),
      % add only necessary connections to reach pool size
      LiveConnections = revalidate_connections(Host, Port),
      if LiveConnections < ConnPoolSize ->
         [start_connection(Host, Port) || _ <- lists:seq(1, ConnPoolSize - LiveConnections)];
      true ->
         ok
      end;
        {remove_server, Host, Port} ->
            [(catch gen_server:call(Pid, quit, ?TIMEOUT)) || [Pid] <- ets:match(erlmc_connections, {{Host, Port}, '$1'})],
            remove_server_from_continuum(Host, Port);
        {has_server, CallerPid, Host, Port} ->
            CallerPid ! {has_server_result, is_server_in_continuum(Host, Port)};
        {add_connection, Host, Port} ->
            start_connection(Host, Port);
        {remove_connection, Host, Port} ->
            [[Pid]|_] = ets:match(erlmc_connections, {{Host, Port}, '$1'}),
            (catch gen_server:call(Pid, quit, ?TIMEOUT));
        {'EXIT', Pid, Err} ->
            case ets:match(erlmc_connections, {'$1', Pid}) of
                [[{Host, Port}]] ->
                    ets:delete_object(erlmc_connections, {{Host, Port}, Pid}),
          update_connections_for_server(Host, Port, {3, -1, 0, 0}),
                    case Err of
                        shutdown -> ok;
                        _ -> start_connection(Host, Port)
                    end;
                _ ->
                    ok
            end
    end,
    loop().

start_connection(Host, Port) ->
    case erlmc_conn:start_link([Host, Port]) of
        {ok, Pid} ->
      ets:insert(erlmc_connections, {{Host, Port}, Pid}),
      update_connections_for_server(Host, Port, {3, 1});
        _ -> ok
    end.

revalidate_connections(Host, Port) ->
    [(catch gen_server:call(Pid, version, ?TIMEOUT)) || [Pid] <- ets:match(erlmc_connections, {{Host, Port}, '$1'})],
    LiveConnections = length(ets:match(erlmc_connections, {{Host, Port}, '$1'})),
    set_connections_for_server(Host, Port, LiveConnections),
    LiveConnections.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
add_server_to_continuum(Host, Port) ->
    [ets:insert(erlmc_continuum, {hash_to_uint(Host ++ integer_to_list(Port) ++ integer_to_list(I)), {Host, Port}, 0}) ||
    I <- lists:seq(1, 100)].

update_connections_for_server(Host, Port, UpdateOpr) ->
    case ets:match(erlmc_continuum, {'$1', {Host, Port}, '_'}) of
        [] ->
            ok;
        List ->
            [ets:update_counter(erlmc_continuum, Key, UpdateOpr) || [Key] <- List]
    end.

set_connections_for_server(Host, Port, Val) ->
    case ets:match(erlmc_continuum, {'$1', {Host, Port}, '_'}) of
        [] ->
            ok;
        List ->
            [ets:update_element(erlmc_continuum, Key, {3, Val}) || [Key] <- List]
    end.

remove_server_from_continuum(Host, Port) ->
    case ets:match(erlmc_continuum, {'$1', {Host, Port}, '_'}) of
        [] ->
            ok;
        List ->
            [ets:delete(erlmc_continuum, Key) || [Key] <- List]
    end.

is_server_in_continuum(Host, Port) ->
    case ets:match(erlmc_continuum, {'$1', {Host, Port}, '_'}) of
        [] ->
            false;
        _ ->
            true
    end.

package_key(Key) when is_atom(Key) ->
    atom_to_list(Key);

package_key(Key) when is_list(Key) ->
    Key;

package_key(Key) when is_binary(Key) ->
    binary_to_list(Key);

package_key(Key) ->
    lists:flatten(io_lib:format("~p", [Key])).

unique_connections() ->
    dict:to_list(lists:foldl(
        fun({Key, Val}, Dict) ->
            dict:append_list(Key, [Val], Dict)
        end, dict:new(), ets:tab2list(erlmc_connections))).

unique_connection(Host, Port) ->
  case ets:lookup(erlmc_connections, {Host, Port}) of
    [] -> exit({erlmc, {connection_not_found, {Host, Port}}});
    Pids ->
      unique_connection(Host, Port, length(Pids))
  end.
unique_connection(Host, Port, RandBase) ->
  TRand = crypto:rand_uniform(1, RandBase + 1),
  case ets:select(erlmc_connections, [{{{Host, Port}, '$1'},[],['$$']}], TRand) of
    {[[Pid]|_],_} -> Pid;
    '$end_of_table' ->
      % could not find the host in the connections list
      error_logger:info_msg("Removing ~p from pool - no connection found for index ~p ~n", [{Host, Port}, TRand]),
      remove_server(Host, Port),
      exit({erlmc, {connection_not_found, {Host, Port}}});
    _ ->
      exit({erlmc, {connection_not_found, {Host, Port}}})
  end.

%% Consistent hashing functions
%%
%% First, hash memcached servers to unsigned integers on a continuum. To
%% map a key to a memcached server, hash the key to an unsigned integer
%% and locate the next largest integer on the continuum. That integer
%% represents the hashed server that the key maps to.
%% reference: http://www8.org/w8-papers/2a-webserver/caching/paper2.html
hash_to_uint(Key) when is_list(Key) ->
    <<Int:128/unsigned-integer>> = erlang:md5(Key), Int.

%% @spec map_key(Key) -> Conn
%%         Key = string()
%%         Conn = pid()
map_key(Key) when is_list(Key) ->
    {{Host, Port}, NumConnections} =
        case find_next_largest(hash_to_uint(Key)) of
        '$end_of_table' -> exit(erlmc_continuum_empty);
            KeyIndex ->
              [{_, Value, NC}] = ets:lookup(erlmc_continuum, KeyIndex),
                {Value, NC}
        end,
    unique_connection(Host, Port, NumConnections).

map_key_host(Key) when is_list(Key) ->
  case find_next_largest(hash_to_uint(Key)) of
    '$end_of_table' -> exit(erlmc_continuum_empty);
    KeyIndex ->
      [{_, Value, NC}] = ets:lookup(erlmc_continuum, KeyIndex),
      {Value, NC}
  end.

find_next_largest(Hash) ->
    case ets:select(erlmc_continuum, [{{'$1','_','_'},[{'>', '$1', Hash}],['$1']}], 1) of
    '$end_of_table' -> ets:first(erlmc_continuum);
    {[Key], _} -> Key
  end.

split_keys(KeyList) -> split_keys(KeyList, []).
split_keys([], SplitKeys) ->
  HKeys = proplists:get_keys(SplitKeys),
  [{HKey, proplists:get_all_values(HKey, SplitKeys)} || HKey <- HKeys];
split_keys([Key0|Rest], SplitKeys) ->
  Key = package_key(Key0),
  split_keys(Rest, [{map_key_host(Key), Key} | SplitKeys]).

randomize(Lo, Hi) when Lo =:= Hi ->
    Lo;
randomize(Lo, Hi) ->
    crypto:rand_uniform(Lo, Hi).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

client_test_() ->
    {foreach,
        fun() ->
            ok
        end,
        fun(_) ->
            ok
        end,
        [
            {"connection test",
                fun() ->
                    {ok, Socket} = gen_tcp:connect("localhost", 11211, [binary, {packet, 0}, {active, false}]),
                    ok = gen_tcp:send(Socket, <<128,10,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0>>),
                    ?assertEqual({ok, <<129,10,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0>>}, gen_tcp:recv(Socket, 0, 2000)),
                    ok = gen_tcp:send(Socket, <<128,7,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0>>),
                    ok = gen_tcp:close(Socket)
                end
            },
            {"client test",
                fun() ->
                    ?assertEqual(ok, erlmc:start()),
                    ?assertEqual(<<>>, erlmc:set("Hello", <<"World">>)),
                    ?assertEqual(<<"Data exists for key.">>, erlmc:add("Hello", <<"Fail">>)),
                    ?assertEqual(<<"World">>, erlmc:get("Hello")),
                    ?assertEqual(<<>>, erlmc:delete("Hello")),
                    ?assertEqual(<<>>, erlmc:add("Hello", <<"World2">>)),
                    ?assertEqual(<<"World2">>, erlmc:get("Hello")),
                    ?assertEqual(<<>>, erlmc:append("Hello", <<"!!!">>)),
                    ?assertEqual(<<"World2!!!">>, erlmc:get("Hello")),
                    ?assertEqual(<<>>, erlmc:prepend("Hello", <<"$$$">>)),
                    ?assertEqual(<<"$$$World2!!!">>, erlmc:get("Hello")),
                    ?assertEqual(<<>>, erlmc:delete("Hello")),
                    ?assertEqual(<<>>, erlmc:get("Hello")),
                    ?assertEqual(<<>>, erlmc:set("One", <<"A">>)),
                    ?assertEqual(<<>>, erlmc:set("Two", <<"B">>)),
                    ?assertEqual(<<>>, erlmc:set("Three", <<"C">>)),
                    ?assertEqual([{"One",<<"A">>},{"Two",<<"B">>},{"Three",<<"C">>}],
                                    erlmc:get_many(["One", "Two", "Two-and-a-half", "Three"])),
                    ?assertEqual(<<1:64>>, erlmc:increment("inc1", 1, 1, 0)),
                    ?assertEqual(ok, erlmc:incrementq("inc1", 1, 1, 0)),
                    ?assertEqual(ok, erlmc:decrementq("inc1", 1, 1, 0)),
                    ?assertEqual(<<0:64>>, erlmc:decrement("inc1", 1, 1, 0)),
                    ?assertEqual([{{"localhost",11211},<<>>}], erlmc:flush(0)),
                    ?assertEqual([{{"localhost",11211},<<>>}], erlmc:flush(0)),
                    ?assertMatch([{{"localhost",11211}, [{_,_}|_]}], erlmc:stats()),
                    ?assertMatch([{_,_}|_], erlmc:stats("localhost",11211)),
                    ?assertEqual([{{"localhost",11211},[true]}], erlmc:quit()),
                    ?assertEqual(true, erlmc:has_server("localhost",11211)),
                    ?assertEqual(ok, erlmc:remove_server("localhost",11211)),
                    ?assertEqual(false, erlmc:has_server("localhost",11211))
                end
            }
        ]
    }.

-endif.

