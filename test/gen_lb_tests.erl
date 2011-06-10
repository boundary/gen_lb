-module(gen_lb_tests).

-include_lib("eunit/include/eunit.hrl").

all_test_() ->
  {foreach,
    fun setup/0,
    fun teardown/1,
    [
      fun multiple_nodes_go_up/0,
      fun multiple_nodes_one_down/0,
      fun round_robin_dispatch/0,
      fun queue_when_down/0,
      fun flush_when_node_returns/0
    ]}.

setup() -> ok.
  
teardown(_) ->
  catch gen_lb:stop(lb),
  catch meck_send:unload(gen_lb).
  % meck:unload(),
  % meck_send:unload().
  
multiple_nodes_go_up() ->
  {ok, C} = countdown:new(5),
  meck_send:new(gen_lb),
  meck_send:add_remote(gen_lb, {net_kernel,connect_node}, connect_up(lb, C, [scylla@data1,scylla@data2])),
  meck_send:add_listener(gen_lb, {admin,scylla@data1}, spawn(fun() -> admin_loop(C, [scylla@data1,scylla@data2]) end)),
  meck_send:add_listener(gen_lb, {writer,scylla@data1}, spawn(fun() -> writer_loop(C, scylla@data1, true) end)),
  meck_send:add_listener(gen_lb, {writer,scylla@data2}, spawn(fun() -> writer_loop(C, scylla@data2, true) end)),
  gen_lb:start_link({local, lb}, [scylla@data1], writer, fun gen_lb:round_robin/3),
  countdown:await(C),
  Nodes = gen_server:call(lb, nodes),
  ?assertEqual(sets:from_list([scylla@data1,scylla@data2]), Nodes).
  
multiple_nodes_one_down() ->
  {ok, C} = countdown:new(5),
  meck_send:new(gen_lb),
  meck_send:add_remote(gen_lb, {net_kernel,connect_node}, connect_up(lb, C, [scylla@data1,scylla@data2])),
  meck_send:add_listener(gen_lb, {admin,scylla@data1}, spawn(fun() -> admin_loop(C, [scylla@data1,scylla@data2]) end)),
  meck_send:add_listener(gen_lb, {writer,scylla@data1}, spawn(fun() -> writer_loop(C, scylla@data1, true) end)),
  meck_send:add_listener(gen_lb, {writer,scylla@data2}, spawn(fun() -> writer_loop(C, scylla@data2, false) end)),
  gen_lb:start_link({local, lb}, [scylla@data1], writer, fun gen_lb:round_robin/3),
  countdown:await(C),
  Nodes = gen_server:call(lb, nodes),
  ?assertEqual(sets:from_list([scylla@data1]), Nodes).

round_robin_dispatch() ->
  {ok, C} = countdown:new(5),
  meck_send:new(gen_lb),
  meck_send:add_remote(gen_lb, {net_kernel,connect_node}, connect_up(lb, C, [scylla@data1,scylla@data2])),
  meck_send:add_listener(gen_lb, {admin,scylla@data1}, spawn(fun() -> admin_loop(C, [scylla@data1,scylla@data2]) end)),
  meck_send:add_listener(gen_lb, {writer,scylla@data1}, spawn(fun() -> writer_loop(C, scylla@data1, true) end)),
  meck_send:add_listener(gen_lb, {writer,scylla@data2}, spawn(fun() -> writer_loop(C, scylla@data2, true) end)),
  gen_lb:start_link({local, lb}, [scylla@data1], writer, fun gen_lb:round_robin/3),
  countdown:await(C),
  Reply1 = gen_lb:call(lb, "Herp", 1000),
  ?assertMatch({_, _, {scylla@data1,"Derp"}}, Reply1),
  Reply2 = gen_lb:call(lb, "Herp", 1000),
  ?assertMatch({_, _, {scylla@data2,"Derp"}}, Reply2),
  Reply3 = gen_lb:call(lb, "Herp", 1000),
  ?assertMatch({_, _, {scylla@data1,"Derp"}}, Reply3).
  
queue_when_down() ->
  meck_send:new(gen_lb),
  gen_lb:start_link({local, lb}, [scylla@data1], writer, fun gen_lb:round_robin/3),
  gen_lb:cast(lb, "Herp"),
  Fut = gen_lb:call_future(lb, "Herp", 30000),
  Nodes = gen_server:call(lb, nodes),
  State = gen_server:call(lb, state),
  Pending = element(9, State),
  ?assertEqual(sets:new(), Nodes),
  ?assertMatch([_], Pending).
  
flush_when_node_returns() ->
  {ok, C} = countdown:new(5),
  meck_send:new(gen_lb),
  meck_send:add_listener(gen_lb, {writer,scylla@data1}, spawn(fun() -> writer_loop(C, scylla@data1, true) end)),
  gen_lb:start_link({local, lb}, [scylla@data1], writer, fun gen_lb:round_robin/3),
  Fut = gen_lb:call_future(lb, "Herp", 1000),
  lb ! {nodeup,scylla@data1,make_ref()},
  Reply = Fut(),
  ?assertMatch({_, _, {scylla@data1, "Derp"}}, Reply).

admin_loop(Counter,Cluster) ->
  receive
    {cluster,Pid,Ref} ->
      io:format(user, "admin ~p~n", [Pid]),
      countdown:decrement(Counter),
      Pid ! {cluster, Ref, Cluster},
      io:format(user, "sent ~p to ~p~n",[{cluster,Ref,Cluster},Pid]),
      admin_loop(Counter,Cluster)
  end.
  
writer_loop(Counter, Name, Ping) ->
  receive
    {ping, Pid, Ref} ->
      if
        Ping -> 
          Pid ! {pong, Ref},
          countdown:decrement(Counter);
        true ->
          countdown:decrement(Counter)
      end;
    {Pid,Ref,Request} ->
      if
        Ping ->
          Pid ! {self(), Ref, {Name,"Derp"}},
          countdown:decrement(Counter);
        true ->
          countdown:decrement(Counter)
      end
  end,
  writer_loop(Counter, Name, Ping).
  
connect_up(Pid, Counter, NodesUp) ->
  fun(Node) ->
      io:format(user, "connect ~p with nodesup ~p hey ~p~n", [Node,NodesUp,lists:member(Node, NodesUp)]),
      case lists:member(Node, NodesUp) of
        true -> 
          io:format(user, "sending nodeup for ~p~n", [Node]),
          Pid ! {nodeup, Node, make_ref()},
          countdown:decrement(Counter);
        _ -> 
          io:format(user, "DERP DERP~n", []),
          countdown:decrement(Counter),
          ok
      end
    end.
