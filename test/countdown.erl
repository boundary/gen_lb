%%%-------------------------------------------------------------------
%%% File:      countdown.erl
%%% @author    cliff moon <cliff@fastip.com> []
%%% @copyright 2011 fast_ip
%%% @doc  
%%%
%%% @end  
%%%
%%% @since 2011-03-03 by cliff moon
%%%-------------------------------------------------------------------
-module(countdown).
-author('cliff@fastip.com').

-behaviour(gen_server).

-include_lib("eunit/include/eunit.hrl").

%% API
-export([new/1, new/2, await/1, await/2, decrement/1, decrement/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {count,waiters}).

new(N) ->
  gen_server:start_link(?MODULE, N, []).
  
new(Name, N) ->
  gen_server:start_link(Name, ?MODULE, N, []).
  
await(Server) ->
  gen_server:call(Server, await).
  
await(Server, Timeout) ->
  gen_server:call(Server, await, Timeout).
  
decrement(Server) ->
  gen_server:cast(Server, {decrement,1}).

decrement(Server, N) ->
  gen_server:cast(Server, {decrement,N}).

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
init(N) ->
  {ok, #state{count=N,waiters=[]}}.

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
handle_call(await, _, State = #state{count=C}) when C =< 0 ->
  {reply, ok, State};
handle_call(await, From, State = #state{waiters=W}) ->
  {noreply, State#state{waiters=[From|W]}};
handle_call(count, _, State = #state{count=C}) ->
  {reply, C, State}.

%%--------------------------------------------------------------------
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% @doc Handling cast messages
%% @end 
%%--------------------------------------------------------------------
handle_cast({decrement,N}, State = #state{count=C,waiters=W}) ->
  C2 = C - N,
  ?debugFmt("new count ~p~n", [C2]),
  if
    C2 =< 0 -> 
      [ gen_server:reply(From, ok) || From <- W ],
      {noreply, State#state{count=C2,waiters=[]}};
    true ->
      {noreply, State#state{count=C2}}
  end;
handle_cast(_Msg, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% @doc Handling all non call/cast messages
%% @end 
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
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
