%%%
%%% Copyright 2011, Boundary
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%


%%%-------------------------------------------------------------------
%%% File:      gen_lb.erl
%%% @author    cliff moon <cliff@boundary.com> []
%%% @copyright 2011 Boundary
%%% @doc  
%%%
%%% @end  
%%%
%%% @since 2011-02-25 by cliff moon
%%%-------------------------------------------------------------------

-module(gen_lb).
-author('cliff@boundary.com').

-behaviour(gen_server).

%% API
-export([start_link/3, start_link/4, start_link/5, call/3, call_future/3, cast/2, stop/1]).

%% Selection Functions
-export([round_robin/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {context, select_node, remote_service, seeds, nodes, tref, handlers=dict:new(), pending=[], beats_up=0}).
-record(pending, {type,request,ref,from,time,timeout}).

-define(HEARTBEAT_INTERVAL, 10000).
%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% @spec start_link() -> {ok,Pid} | ignore | {error,Error}
%% @doc Starts the server
%% @end 
%%--------------------------------------------------------------------
% behaviour_info(callbacks) ->
%   [{init,1},
%    {select_node,4}].
  
start_link(Seed, RemoteService, SelectNode) when is_atom(Seed), is_function(SelectNode) ->
  start_link([Seed], RemoteService, SelectNode);
start_link(Seeds, RemoteService, SelectNode) when is_list(Seeds), is_function(SelectNode) ->
  gen_server:start_link(?MODULE, {Seeds, RemoteService, SelectNode, undefined}, []).
  
start_link(Name, Seed, RemoteService, SelectNode) when is_atom(Seed), is_function(SelectNode) ->
  start_link(Name, [Seed], RemoteService, SelectNode);
start_link(Name, Seeds, RemoteService, SelectNode) when is_list(Seeds), is_function(SelectNode) ->
  gen_server:start_link(Name, ?MODULE, {Seeds, RemoteService, SelectNode, undefined}, []);
  
start_link(Seed, RemoteService, SelectNode, Context) when is_atom(Seed), is_function(SelectNode) ->
  start_link([Seed], RemoteService, SelectNode, Context);
start_link(Seeds, RemoteService, SelectNode, Context) when is_list(Seeds), is_function(SelectNode) ->
  gen_server:start_link(?MODULE, {Seeds, RemoteService, SelectNode, Context}, []).
  
start_link(Name, Seed, RemoteService, SelectNode, Context) when is_atom(Seed), is_function(SelectNode) ->
  start_link(Name, [Seed], RemoteService, SelectNode, Context);
start_link(Name, Seeds, RemoteService, SelectNode, Context) when is_list(Seeds), is_function(SelectNode) ->
  gen_server:start_link(Name, ?MODULE, {Seeds, RemoteService, SelectNode, Context}, []).
  
stop(Name) ->
  gen_server:call(Name, stop).
  
call(Server, Request, Timeout) ->
  gen_server:call(Server, {call, Request, Timeout}, Timeout).
  
call_future(Server, Request, Timeout) ->
  Ref = make_ref(),
  Parent = self(),
  spawn_link(fun() ->
      Reply = call(Server, Request, Timeout),
      Parent ! {Ref, Reply}
    end),
  fun() ->
    receive
      {Ref, Reply} -> Reply
    after Timeout ->
      exit(timeout)
    end
  end.
  
cast(Server, Request) ->
  gen_server:cast(Server, {cast, Request}).
  
round_robin(Nodes, Request, Context) when not is_list(Context) ->
  round_robin(Nodes, Request, sets:to_list(Nodes));
round_robin(Nodes, Request, []) ->
  round_robin(Nodes, Request, sets:to_list(Nodes));
round_robin(Nodes, Request, [Node|Context]=C) ->
  case {sets:size(Nodes), length(C)} of
    {N,N} -> {Node, Context ++ [Node]};
    {_,_} -> round_robin(Nodes, Request, sets:to_list(Nodes))
  end.

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% @spec init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% @doc Initiates the server
%% @end 
%%--------------------------------------------------------------------
init({Seeds, RemoteService, SelectNode, Context}) ->
  process_flag(trap_exit, true),
  net_kernel:monitor_nodes(true, [{node_type,all}]),
  KnownNodes = query_cluster(Seeds),
  spawn_connect_nodes(sets:to_list(KnownNodes)),
  {ok, TRef} = timer:send_interval(?HEARTBEAT_INTERVAL, heartbeat),
  {ok, #state{seeds=Seeds,nodes=sets:new(),tref=TRef,context=Context,select_node=SelectNode,remote_service=RemoteService}}.

%%--------------------------------------------------------------------
%% @spec 
%% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% @doc Handling call messages
%% @end 
%%--------------------------------------------------------------------
handle_call(state, _From, State) ->
  {reply, State, State};
handle_call(nodes, _From, State) ->
  {reply, State#state.nodes, State};
handle_call({call, Request, Timeout}, From = {_, Ref}, State = #state{context=Context,remote_service=RemoteService,nodes=Nodes,pending=Pending,handlers=Handlers,select_node=SelectNode}) ->
  case sets:size(Nodes) of
    0 ->
      error_logger:error_msg("Cluster is down. Queueing request.~n"),
      Pend = #pending{type=call,request=Request,ref=Ref,from=From,time=now(),timeout=Timeout},
      {noreply, State#state{pending=[Pend|Pending]}};
    _ ->
      {Pid, Context2} = call_handler(From, RemoteService, Nodes, Ref, Request, Context, SelectNode, Timeout),
      {noreply, State#state{handlers=dict:store(Pid,Ref,Handlers),context=Context2}}
  end;
handle_call(stop, _From, State) ->
  {stop, normal, ok, State}.

%%--------------------------------------------------------------------
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% @doc Handling cast messages
%% @end 
%%--------------------------------------------------------------------
handle_cast(stop, State) ->
  {stop, normal, State};
handle_cast({cast, Request}, State = #state{context=Context,remote_service=RemoteService,nodes=Nodes,pending=Pending,select_node=SelectNode}) ->
  case sets:size(Nodes) of
    0 ->
      error_logger:error_msg("Cluster is down. Queueing request.~n"),
      Pend = #pending{type=cast,request=Request,time=now()},
      {noreply, State#state{pending=[Pend|Pending]}};
    _ ->
      Context2 = cast_handler(RemoteService, Nodes, Request, Context, SelectNode),
      {noreply, State#state{context=Context2}}
  end.

%%--------------------------------------------------------------------
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% @doc Handling all non call/cast messages
%% @end 
%%--------------------------------------------------------------------
handle_info(heartbeat, State=#state{seeds=Seeds,nodes=Nodes,beats_up=Beats}) ->
  KnownNodes = query_cluster([random_set_element(Nodes, Seeds)]),
  spawn_connect_nodes(sets:to_list(KnownNodes)),
  BeatsUp = case sets:size(Nodes) of
    0 -> 0;
    _ -> Beats+1
  end,
  {noreply, State#state{beats_up=BeatsUp}};
handle_info({'EXIT', Handler, Reason}, State=#state{handlers=Handlers}) ->
  case Reason of
    normal -> ok;
    _ -> error_logger:error_msg("Query handler ~p failed with ~p~n", [Handler,Reason])
  end,
  Handlers2 = dict:erase(Handler,Handlers),
  {noreply, State#state{handlers=Handlers2}};
handle_info({nodeup,Node,_}, State) ->
  {noreply, nodeup(Node,State)};
handle_info({nodedown,Node,_}, State) ->
  {noreply, nodedown(Node,State)};
handle_info({cluster,_,Nodes}, State = #state{nodes=NodeSet}) ->
  {noreply, State#state{nodes=sets:union(NodeSet, sets:from_list(Nodes))}};
handle_info(_Info, State) ->
  error_logger:info_msg("Load balancer did not understand: ~p~n", [_Info]),
  {noreply, State}.

%%--------------------------------------------------------------------
%% @spec terminate(Reason, State) -> void()
%% @doc This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%% @end 
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
  ok.

%%--------------------------------------------------------------------
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @doc Convert process state when code is changed
%% @end 
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
nodeup(Node, State=#state{nodes=Nodes,pending=Pending,remote_service=RemoteService}) ->
  error_logger:info_msg("~p: nodeup ~p~n", [?MODULE,Node]),
  case verify_membership(Node, RemoteService) of
    ok -> case length(Pending) of
        0 ->
          error_logger:info_msg("Nothing in queue.~n"),
          State#state{nodes=sets:add_element(Node,Nodes)};
        _ ->
          error_logger:info_msg("Flushing pending requests.~n"),
          flush_pending(State#state{nodes=sets:add_element(Node,Nodes)})
      end;
    _ -> State
  end.
  
verify_membership(Node, RemoteService) ->
  Ref = make_ref(),
  {RemoteService, Node} ! {ping, self(), Ref},
  receive
    {pong, Ref} -> ok
  after 1000 ->
    %% this could be a node with slow startup. schedule verification in a few seconds.
    timer:send_after(5000, self(), {nodeup, Node, []})
  end.
  
nodedown(Node, State=#state{nodes=Nodes}) ->
  error_logger:info_msg("~p: nodedown ~p~n", [?MODULE,Node]),
  State#state{nodes=sets:del_element(Node,Nodes)}.

random_set_element(Set, Default) ->
  case random_set_element(Set) of
    undefined -> case Default of
        [D|_] -> D;
        _ -> throw(no_seed_nodes)
      end;
    I -> I
  end.
  
random_set_element(Set) ->
  case sets:size(Set) of
    0 -> undefined;
    N -> lists:nth(random:uniform(N), sets:to_list(Set))
  end.

spawn_connect_nodes(Nodes) ->
  spawn(fun() ->
      lists:foreach(fun(Node) -> net_kernel:connect_node(Node) end, Nodes)
    end).

query_cluster(Seeds) ->
  Refs = lists:map(fun(Seed) ->
      Ref = make_ref(),
      {admin, Seed} ! {cluster, self(), Ref},
      Ref
    end, Seeds),
  lists:foldl(fun(Ref,Set) ->
      receive
        {cluster, Ref, Nodes} -> sets:union(Set, sets:from_list(Nodes))
      after 1000 ->
        Set
      end
    end, sets:new(), Refs).

cast_handler(RemoteService, Nodes, Request, Context, SelectNode) ->
  {Node,Context2} = SelectNode(Nodes,Request,Context),
  % error_logger:info_msg("sending cast to ~p~n", [{RemoteService, Node}]),
  {RemoteService,Node} ! Request,
  Context2.

call_handler(From, RemoteService, Nodes, Ref, Request, Context, SelectNode, Timeout) ->
  {Node,Context2} = SelectNode(Nodes,Request,Context),
  Pid = spawn_link(fun() ->
      % error_logger:info_msg("sending call to ~p~n", [{RemoteService, Node}]),
      {RemoteService, Node} ! {self(), Ref, Request},
      receive
        {RemoteEnd, Ref, Results} -> gen_server:reply(From, {RemoteEnd, Ref, Results})
      after Timeout ->
        gen_server:reply(From, {error, timeout})
      end
    end),
  {Pid, Context2}.
  
timeout_expired(Time, Timeout) ->
  Diff = (calendar:datetime_to_gregorian_seconds(calendar:now_to_local_time(now())) - calendar:datetime_to_gregorian_seconds(calendar:now_to_local_time(Time))) * 1000,
  Diff >= Timeout.
  
flush_pending(State = #state{pending=[]}) ->
  State;
flush_pending(State = #state{pending=[#pending{type=call,request=Request,ref=Ref,from=From,time=Time,timeout=Timeout}|Pending],nodes=Nodes,remote_service=RemoteService,context=Context,select_node=SelectNode,handlers=Handlers}) ->
  case timeout_expired(Time, Timeout) of
    true -> 
      error_logger:info_msg("Request ~p thrown out due to age.", [Ref]),
      flush_pending(State#state{pending=Pending});
    _ ->
      {Pid, Context2} = call_handler(From, RemoteService, Nodes, Ref, Request, Context, SelectNode, Timeout),
      flush_pending(State#state{pending=Pending,handlers=dict:store(Pid,Ref,Handlers),context=Context2})
  end;
flush_pending(State = #state{pending=[#pending{type=cast,request=Request}|Pending],context=Context,remote_service=RemoteService,nodes=Nodes,select_node=SelectNode}) ->
  Context2 = cast_handler(RemoteService, Nodes, Request, Context, SelectNode),
  flush_pending(State#state{pending=Pending,context=Context2}).

