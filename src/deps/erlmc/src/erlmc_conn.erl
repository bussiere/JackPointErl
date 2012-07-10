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
-module(erlmc_conn).
-behaviour(gen_server2).

-include("erlmc.hrl").

%% gen_server callbacks
-export([start_link/1, init/1, handle_call/3, handle_cast/2,
	     handle_info/2, terminate/2, code_change/3]).

%% API functions
start_link([Host, Port]) ->
	gen_server2:start_link(?MODULE, [Host, Port], []).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%% @hidden
%%--------------------------------------------------------------------
init([Host, Port]) ->
	case gen_tcp:connect(Host, Port, [binary, {packet, 0}, {active, once}]) of
    {ok, Socket} -> 
			{ok, {Socket, []}};
    Error -> 
			exit(Error)
  end.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%% @hidden
%%--------------------------------------------------------------------    
handle_call({get, Key}, From, {Socket, Queue}) ->
  BKey = list_to_binary(Key),
  send(Socket, #request{op_code=?OP_GetK, key=BKey}),
  {noreply, {Socket, Queue ++ [{get, BKey, From}]}};
% 
handle_call({get_many, []}, _From, Socket) -> 
  {reply, [], Socket};
%
handle_call({get_many, Keys}, From, {Socket, Queue}) ->
  [send(Socket, #request{op_code=?OP_GetK, key=list_to_binary(Key)}) || Key <- Keys], 
  send(Socket, #request{op_code=?OP_Noop}),
  {noreply, {Socket, Queue ++ [{get_many, From}]}};
%
handle_call({add, Key, Value, Expiration}, From, {Socket, Queue}) ->
  send(Socket, 
    #request{op_code=?OP_Add, extras = <<16#deadbeef:32, Expiration:32>>, key=list_to_binary(Key), value=Value}),
  {noreply, {Socket, Queue ++[{add, From}]}};
%    
handle_call({set, Key, Value, Expiration}, From, {Socket, Queue}) ->
	send(Socket, 
    #request{op_code=?OP_Set, extras = <<16#deadbeef:32, Expiration:32>>, key=list_to_binary(Key), value=Value}),
  {noreply, {Socket, Queue ++[{set, From}]}};
%
handle_call({replace, Key, Value, Expiration}, From, {Socket, Queue}) ->
	send(Socket, 
    #request{op_code=?OP_Replace, extras = <<16#deadbeef:32, Expiration:32>>, key=list_to_binary(Key), value=Value}),
  {noreply, {Socket, Queue ++[{replace, From}]}};
%
handle_call({delete, Key}, From, {Socket, Queue}) ->
	send(Socket, #request{op_code=?OP_Delete, key=list_to_binary(Key)}),
  {noreply, {Socket, Queue ++[{delete, From}]}};
%
handle_call({increment, Key, Value, Initial, Expiration}, From, {Socket, Queue}) ->
	send(Socket, 
    #request{op_code=?OP_Increment, extras = <<Value:64, Initial:64, Expiration:32>>, key=list_to_binary(Key)}),
  {noreply, {Socket, Queue ++[{increment, From}]}};
%
handle_call({decrement, Key, Value, Initial, Expiration}, From, {Socket, Queue}) ->
	send(Socket, 
    #request{op_code=?OP_Decrement, extras = <<Value:64, Initial:64, Expiration:32>>, key=list_to_binary(Key)}),
  {noreply, {Socket, Queue ++[{decrement, From}]}};
%
handle_call({append, Key, Value}, From, {Socket, Queue}) ->
	send(Socket, #request{op_code=?OP_Append, key=list_to_binary(Key), value=Value}),
  {noreply, {Socket, Queue ++[{append, From}]}};
%
handle_call({prepend, Key, Value}, From, {Socket, Queue}) ->
	send(Socket, #request{op_code=?OP_Prepend, key=list_to_binary(Key), value=Value}),
  {noreply, {Socket, Queue ++[{prepend, From}]}};
%
handle_call(stats, From, {Socket, Queue}) ->
	send(Socket, #request{op_code=?OP_Stat}),
  {noreply, {Socket, Queue ++[{stats, From}]}};
%
handle_call(flush, From, {Socket, Queue}) ->
	send(Socket, #request{op_code=?OP_Flush}),
  {noreply, {Socket, Queue ++[{flush, From}]}};
%
handle_call({flush, Expiration}, From, {Socket, Queue}) ->
	send(Socket, #request{op_code=?OP_Flush, extras = <<Expiration:32>>}),
  {noreply, {Socket, Queue ++[{flush, From}]}};
%   
handle_call(quit, _From, {Socket, Queue}) ->
	send(Socket, #request{op_code=?OP_Quit}),
  {noreply, {Socket, Queue ++[close]}};
%    
handle_call(version, From, {Socket, Queue}) ->
	send(Socket, #request{op_code=?OP_Version}),
  {noreply, {Socket, Queue ++[{version, From}]}};
%	
handle_call(_, _From, State) -> {reply, {error, invalid_call}, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%% @hidden
%%--------------------------------------------------------------------
handle_cast({setq, Key, Value, Expiration}, {Socket, Queue}) ->
  % set will always succeed
	send(Socket, 
    #request{op_code=?OP_SetQ, extras = <<16#deadbeef:32, Expiration:32>>, key=list_to_binary(Key), value=Value}),
  {noreply, {Socket, Queue}};
%
handle_cast({deleteq, Key}, {Socket, Queue}) ->
  %a not found key could cause and error so we dont do a quiet variant
	send(Socket, #request{op_code=?OP_Delete, key=list_to_binary(Key)}),
  {noreply, {Socket, Queue ++ [undefined]}};
%
handle_cast({replaceq, Key, Value, Expiration}, {Socket, Queue}) ->
	send(Socket, 
    #request{op_code=?OP_Replace, extras = <<16#deadbeef:32, Expiration:32>>, key=list_to_binary(Key), value=Value}),
  {noreply, {Socket, Queue ++ [undefined]}};
%
handle_cast({incrementq, Key, Value, Initial, Expiration}, {Socket, Queue}) ->
	send(Socket, 
    #request{op_code=?OP_Increment, extras = <<Value:64, Initial:64, Expiration:32>>, key=list_to_binary(Key)}),
  {noreply, {Socket, Queue ++[undefined]}};
%
handle_cast({decrementq, Key, Value, Initial, Expiration}, {Socket, Queue}) ->
	send(Socket, 
    #request{op_code=?OP_Decrement, extras = <<Value:64, Initial:64, Expiration:32>>, key=list_to_binary(Key)}),
  {noreply, {Socket, Queue ++[undefined]}};
%
handle_cast(_Message, State) -> {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%% @hidden
%%--------------------------------------------------------------------
handle_info({tcp_closed, Socket}, {Socket, _}) -> 
	gen_tcp:close(Socket),
  {stop, shutdown, undefined};
%
handle_info({tcp, Socket, Data}, {Socket, Queue}) -> 
  gen_tcp:unrecv(Socket, Data),
  check_receive(Socket, Queue);
%
handle_info(_Info, State) -> {noreply, State}.
%
%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%% @hidden
%%--------------------------------------------------------------------
terminate(_Reason, Socket) ->
	case is_port(Socket) of
		true -> gen_tcp:close(Socket);
		false -> ok
	end, ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%% @hidden
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------     
collect_stats_from_socket(Socket, Acc) -> collect_stats_from_socket(Socket, Acc, infinity).
collect_stats_from_socket(Socket, Acc, Timeout) ->
  case recv(Socket, Timeout) of
		{error, Err} -> {error, Err};
    #response{body_size=0} -> Acc;
    #response{key=Key, value=Value} ->
      collect_stats_from_socket(Socket, [{binary_to_atom(Key, utf8), binary_to_list(Value)}|Acc])
  end.
%
send(Socket, Request) ->
    Bin = encode_request(Request),
    gen_tcp:send(Socket, Bin).

recv(Socket, Timeout) ->
    case recv_header(Socket, Timeout) of
		{error, Err} ->
			{error, Err};
		HdrResp ->
    		recv_body(Socket, HdrResp)
    end.

encode_request(Request) when is_record(Request, request) ->
    Magic = 16#80,
    Opcode = Request#request.op_code,
    KeySize = size(Request#request.key),
    Extras = Request#request.extras,
    ExtrasSize = size(Extras),
    DataType = Request#request.data_type,
    Reserved = Request#request.reserved,
    Body = <<Extras:ExtrasSize/binary, (Request#request.key)/binary, (Request#request.value)/binary>>,
    BodySize = size(Body),
    Opaque = Request#request.opaque,
    CAS = Request#request.cas,
    <<Magic:8, Opcode:8, KeySize:16, ExtrasSize:8, DataType:8, Reserved:16, BodySize:32, Opaque:32, CAS:64, Body:BodySize/binary>>.

recv_header(Socket, Timeout) ->
    decode_response_header(recv_bytes(Socket, 24, Timeout)).
recv_body(Socket, #response{key_size = KeySize, extras_size = ExtrasSize, body_size = BodySize}=Resp) ->
    decode_response_body(recv_bytes(Socket, BodySize), ExtrasSize, KeySize, Resp).

decode_response_header({error, Err}) -> {error, Err};
decode_response_header(<<16#81:8, Opcode:8, KeySize:16, ExtrasSize:8, DataType:8, Status:16, BodySize:32, Opaque:32, CAS:64>>) ->
    #response{
        op_code = Opcode,
        data_type = DataType,
        status = Status,
        opaque = Opaque,
        cas = CAS,
        key_size = KeySize,
        extras_size = ExtrasSize,
        body_size = BodySize
    }.

decode_response_body({error, Err}, _, _, _) -> {error, Err};
decode_response_body(Bin, ExtrasSize, KeySize, Resp) ->
    <<Extras:ExtrasSize/binary, Key:KeySize/binary, Value/binary>> = Bin,
    Resp#response{
        extras = Extras,
        key = Key,
        value = Value
    }.

recv_bytes(_, 0) -> <<>>;
recv_bytes(Socket, NumBytes) -> recv_bytes(Socket, NumBytes, infinity).
recv_bytes(Socket, NumBytes, Timeout) ->
    case gen_tcp:recv(Socket, NumBytes, Timeout) of
        {ok, Bin} -> Bin;
        Err -> Err
    end.

read_pipelined(Socket, StopOp, Acc) -> read_pipelined(Socket, StopOp, Acc, infinity).
read_pipelined(Socket, StopOp, Acc, Timeout) ->
  case recv(Socket, Timeout) of
    {error, Err} -> {error, Err};
    #response{op_code = StopOp} -> Acc;
    #response{key=_, value= <<>>} -> read_pipelined(Socket, StopOp, Acc);
    #response{key=Key, value=Value} -> read_pipelined(Socket, StopOp, [{binary_to_list(Key), Value} | Acc])
	end.
%
recv_queued_response({get, Key, From}, Socket, Timeout) ->
  case recv(Socket,Timeout) of
    {error, timeout} -> {error, timeout};
    {error, Err} -> {stop, Err, {error, Err}, Socket};
    #response{key=Key, value=Value} -> 
      gen_server:reply(From, Value),
      ok;
    _ -> 
      gen_server:reply(From, <<>>),
      ok
	end;
recv_queued_response({get_many, From}, Socket, Timeout) ->
  case read_pipelined(Socket, ?OP_Noop, [], Timeout) of
    {error, timeout} -> {error, timeout};
    {error, Err} -> {stop, Err, {error, Err}, Socket};
    Resp -> 
      gen_server:reply(From, Resp),
      ok
	end;
recv_queued_response({stats, From}, Socket, Timeout) ->
  case collect_stats_from_socket(Socket, [], Timeout) of
    {error, timeout} -> {error, timeout};
		{error, Err} -> {stop, Err, {error, Err}, Socket};
		Reply -> 
      gen_server:reply(From, Reply),
      ok
	end;
recv_queued_response(close, Socket, Timeout) ->
  case recv(Socket, Timeout) of
    {error, timeout} -> {error, timeout};
		{error, Err} -> {stop, Err, {error, Err}, Socket};
    _ ->
	    gen_tcp:close(Socket),
      {stop, shutdown, undefined}
  end;
recv_queued_response(undefined, Socket, Timeout) ->
  case recv(Socket, Timeout) of
    {error, timeout} -> {error, timeout};
		{error, Err} -> {stop, Err, {error, Err}, Socket};
		_Resp -> ok
	end;
recv_queued_response({_, From}, Socket, Timeout) ->
  case recv(Socket, Timeout) of
    {error, timeout} -> {error, timeout};
		{error, Err} -> {stop, Err, {error, Err}, Socket};
		Resp -> 
      gen_server:reply(From, Resp#response.value),
      ok
	end.
%  
check_receive(Socket, []) ->
  inet:setopts(Socket, [{active, once}]),
  {noreply, {Socket, []}};
check_receive(Socket, Queue = [QueuedOp | Rest]) ->
  case recv_queued_response(QueuedOp, Socket, 0) of
    {error, timeout} ->
      inet:setopts(Socket, [{active, once}]),
      {noreply, {Socket, Queue}};
    ok -> check_receive(Socket, Rest);
    Resp -> Resp
  end.

