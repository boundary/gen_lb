-module(gen_lb_tests).

-include_lib("eunit/include/eunit.hrl").

all_test_() ->
  {foreach,
    fun setup/0,
    fun teardown/1,
    [
      fun query_seeds/0,
      fun multiple_nodes_go_up/0
    ]}.

setup() ->
  meck:new(gen_lb, [passthrough]).
  
teardown(_) ->
  gen_lb:stop(lb),
  meck:unload().

query_seeds() ->
  {ok, C} = countdown:new(1),
  meck:expect(gen_lb, snd, fun({admin,scylla@localhost}, {cluster,_,_}) ->
      countdown:decrement(C)
    end),
  gen_lb:start_link({local, lb}, scylla@localhost, writer, fun gen_lb:round_robin/3),
  countdown:await(C),
  ?assert(meck:validate(gen_lb)).
  
multiple_nodes_go_up() ->
  {ok, C} = countdown:new(5),
  meck:expect(gen_lb, snd, fun
    ({admin,scylla@data1}, {cluster,Pid,Ref}) ->
      countdown:decrement(C),
      Pid ! {cluster, Ref, [scylla@data1,scylla@data2]};
    ({writer,_}, {ping, Pid, Ref}) ->
      countdown:decrement(C),
      Pid ! {pong, Ref}
    end),
  meck:expect(gen_lb, connect_node, fun(scylla@data1) ->
      countdown:decrement(C),
      lb ! {nodeup, scylla@data1, make_ref()};
    (scylla@data2) ->
      countdown:decrement(C),
      lb ! {nodeup, scylla@data2, make_ref()}
    end),
  gen_lb:start_link({local, lb}, [scylla@data1], writer, fun gen_lb:round_robin/3),
  countdown:await(C),
  Nodes = gen_server:call(lb, nodes),
  ?assertEqual(sets:from_list([scylla@data1,scylla@data2]), Nodes),
  ?assert(meck:validate(gen_lb)).
  