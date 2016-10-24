%% Copyright 2016, Travelping GmbH <info@travelping.com>

%% Permission is hereby granted, free of charge, to any person obtaining a
%% copy of this software and associated documentation files (the "Software"),
%% to deal in the Software without restriction, including without limitation
%% the rights to use, copy, modify, merge, publish, distribute, sublicense,
%% and/or sell copies of the Software, and to permit persons to whom the
%% Software is furnished to do so, subject to the following conditions:

%% The above copyright notice and this permission notice shall be included in
%% all copies or substantial portions of the Software.

%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
%% FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
%% DEALINGS IN THE SOFTWARE.

-module(netdata).

-behaviour(gen_statem).

-compile([{parse_transform, cut}]).

%% API
-export([start_link/0, register_chart/1]).

%% gen_statem callbacks
-export([callback_mode/0, init/1, terminate/3, code_change/4]).
-export([connect/3, reporting/3]).

-define(SERVER, ?MODULE).
-define(DEFAULT_TIMEOUT, 2).
-define(MAX_TIMEOUT, 30).

-record(state, {charts	= #{}			:: map(),
		updated = false			:: 'true' | 'false',
		timer				:: 'undefined' | reference(),
		timeout = ?DEFAULT_TIMEOUT	:: integer(),
		interval			:: 'undefined' | integer(),
		last				:: 'undefined' | integer(),
		socket				:: 'undefined' | port()
	       }).

-define(STATISTICS, [active_tasks, context_switches, exact_reductions, garbage_collection,
		     io, reductions, run_queue, run_queue_lengths, runtime, scheduler_wall_time,
		     total_active_tasks, total_run_queue_lengths, wall_clock]).

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link() -> {ok, Pid :: pid()} |
		      ignore |
		      {error, Error :: term()}.
start_link() ->
    gen_statem:start_link({local, ?SERVER}, ?MODULE, [], []).

register_chart(Chart) ->
    case erlang:whereis(?SERVER) of
	Pid when is_pid(Pid) ->
	    gen_statem:call(Pid, {register, Chart});
	_ ->
	    {error, not_running}
    end.

%%%===================================================================
%%% gen_statem callbacks
%%%===================================================================

-spec callback_mode() -> gen_statem:callback_mode().
callback_mode() -> state_functions.

-spec init(Args :: term()) ->
		  {ok, State :: term(), Data :: term()} |
		  {ok, State :: term(), Data :: term(),
		   [gen_statem:action()] | gen_statem:action()} |
		  ignore |
		  {stop, Reason :: term()}.
init([]) ->
    State0 = #state{charts = #{}, timeout = ?DEFAULT_TIMEOUT},
    State = lists:foldl(fun init/2, State0, ?STATISTICS),
    {ok, connect, State, [{next_event, internal, reconnect}]}.

-spec connect(
	gen_statem:event_type(), Msg :: term(),
	Data :: term()) ->
			gen_statem:state_function_result().
connect(Type, reconnect, #state{timeout = TimeOut} = Data)
  when Type == internal; Type == info ->
    case gen_tcp:connect({local, <<0, "/tmp/netdata">>}, 0,
			 [local, {active, true}, {mode, list}, {packet, line}]) of
	{ok, Socket} ->
	    cancel_timer(Data),
	    {next_state, reporting, Data#state{socket = Socket, last = undefined}};

	_ ->
	    TRef = erlang:send_after(TimeOut * 1000, self(), reconnect),
	    NextTimeOut = min(?MAX_TIMEOUT, TimeOut * 2),
	    {keep_state, Data#state{timeout = NextTimeOut, timer = TRef}}
    end;

connect({call, From}, {register, Chart}, Data0) ->
    Data = register_chart(Chart, Data0),
    {keep_state, Data, [{reply, From, ok}]}.

-spec reporting(
	gen_statem:event_type(), Msg :: term(),
	Data :: term()) ->
			gen_statem:state_function_result().
reporting(Type, report, #state{charts = ChartsIn, updated = Updated, interval = Interval,
			       socket = Socket, last = Last} = Data0)
  when Type == internal; Type == info ->

    Data = if Updated -> send_chart_definition(Data0);
	      true    -> Data0
    end,

    Now = erlang:monotonic_time(microsecond),
    TDiff = if is_integer(Last) -> Now - Last;
	       true             -> undefined
	    end,
    {Report, ChartsOut} = report(TDiff, ChartsIn),
    gen_tcp:send(Socket, Report),

    TRef = erlang:send_after(Interval, self(), report),
    {keep_state, Data#state{charts = ChartsOut, timer = TRef, last = Now}};

reporting(info, {tcp, Socket, Msg}, #state{socket = Socket} = Data0) ->
    case io_lib:fread("START ~d", Msg) of
	{ok, [Interval], _} ->
	    Data = send_chart_definition(Data0#state{interval = Interval * 1000}),
	    {keep_state, Data, [{next_event, internal, report}]};
	_Other ->
	    keep_state_and_data
    end;
reporting(info, {tcp_closed, _Socket}, Data) ->
    cancel_timer(Data),
    {next_state, connect, Data#state{timeout = ?DEFAULT_TIMEOUT, socket = undefined}, [{next_event, internal, reconnect}]};
reporting(info, {tcp_error, _Socket, _Reason}, Data) ->
    cancel_timer(Data),
    {next_state, connect, Data#state{timeout = ?DEFAULT_TIMEOUT, socket = undefined}, [{next_event, internal, reconnect}]};

reporting({call, From}, {register, Chart}, Data0) ->
    Data = register_chart(Chart, Data0),
    {keep_state, Data, [{reply, From, ok}]};

reporting(_Type, _Msg, _Data) ->
    keep_state_and_data.

-spec terminate(Reason :: term(), State :: term(), Data :: term()) ->
		       any().
terminate(_Reason, _State, _Data) ->
    ok.

-spec code_change(
	OldVsn :: term() | {down,term()},
	State :: term(), Data :: term(), Extra :: term()) ->
			 {ok, NewState :: term(), NewData :: term()}.
code_change(_OldVsn, State, Data, _Extra) ->
    {ok, State, Data}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

cancel_timer(#state{timer = TRef}) when is_reference(TRef) ->
    case erlang:cancel_timer(TRef) of
        false ->
            receive {timeout, TRef, _} -> 0
            after 0 -> false
            end;
        RemainingTime ->
            RemainingTime
    end;
cancel_timer(_) ->
    false.

%% Fun = fun(K, V1, AccIn) -> {V2, AccOut})

maps_mapfold(Fun, Init, Map) when is_function(Fun,3), is_map(Map) ->
    {L,A} = lists:mapfoldl(fun({K,V1},AccIn) ->
				   {V2, AccOut} = Fun(K,V1,AccIn),
				   {{K,V2}, AccOut}
			   end, Init, maps:to_list(Map)),
    {maps:from_list(L), A}.

send_chart_definition(#state{charts = Charts, interval = Interval, socket = Socket} = Data) ->
    Init = init_report(Interval div 1000, Charts),
    gen_tcp:send(Socket, Init),
    Data#state{updated = false}.

-define(DEFAULT_CHART, #{type         => undefined,
			 id           => undefined,
			 name         => undefined,
			 title        => undefined,
			 units        => undefined,
			 charttype    => line,
			 priority     => 1000,
			 values       => [],
			 init         => fun(_Id, X) -> X end}).

-define(DEFAULT_VALUE, #{id         => undefined,
			 name       => undefined,
			 algorithm  => undefined,
			 multiplier => 1,
			 divisor    => 1,
			 hidden     => undefined,
			 get        => fun(_Id, _X) -> 0 end}).

register_chart(Chart0, #state{charts = Charts} = State) ->
    Chart1 = maps:merge(?DEFAULT_CHART, Chart0),
    Chart2 = Chart1#{
	       identifier => maps:get(identifier, Chart1, maps:get(id, Chart1)),
	       values =>
		   lists:map(fun(V) ->
				     V1 = maps:merge(?DEFAULT_VALUE, V),
				     V1#{identifier => maps:get(identifier, V1, maps:get(id, V1))}
			     end, maps:get(values, Chart1, []))},

    %% work arround for cut parse transform bug
    %% #{type := Type, id := Id} = Chart2,
    Type = maps:get(type, Chart2),
    Id = maps:get(id, Chart2),

    State#state{charts = Charts#{ {Type, Id} => Chart2 }, updated = true}.

init_report(Interval, Charts) ->
    Charts1 = maps:fold(init_chart(_, _, Interval, _), [], Charts),
    Charts2 = lists:reverse(Charts1),
    [string:join(Charts2, "\n"), "\n"].

report(Interval, ChartsIn) ->
    {ChartsOut, Reports1} = maps_mapfold(report_chart(_, _, Interval, _), [], ChartsIn),
    Reports2 = lists:reverse(Reports1),
    {[string:join(Reports2, "\n"), "\n"], ChartsOut}.

fmt({Id, Cnt}) ->
    [fmt(Id), "_", fmt(Cnt)];
fmt(undefined) ->
    "''";
fmt(V) when is_atom(V) ->
    atom_to_list(V);
fmt(V) when is_integer(V) ->
    integer_to_list(V);
fmt(V) when is_float(V) ->
    float_to_list(V);
fmt(V) when is_list(V); is_binary(V) ->
    ["\"", V, "\""].

fmt_id({Id, Cnt}) ->
    [fmt_id(Id), "_", fmt_id(Cnt)];
fmt_id(V) when is_integer(V) ->
    integer_to_list(V);
fmt_id(V) when is_atom(V) ->
    atom_to_list(V);
fmt_id(V) when is_list(V); is_binary(V) ->
    V.

init_chart(_K, #{type := Type, id := Id, values := Values} = Def, Interval, Acc) ->
    C0 = lists:map(fun({K, Default}) ->
			   fmt(maps:get(K, Def, Default));
		      (K) ->
			   fmt(maps:get(K, Def, undefined))
		   end,
		   [name, title, units,
		    {family, Id}, {context, "Erlang"},
		    charttype, priority,
		    {update_every, Interval}]),
    Chart = string:join(["CHART", [fmt_id(Type), $., fmt_id(Id)] | C0], " "),
    lists:foldl(fun(V, A) -> init_chart_values(V, A) end, [Chart | Acc], Values).

init_chart_values(#{id := Id} = Value, Acc) ->
    V0 = lists:map(fun(K) -> fmt(maps:get(K, Value, undefined)) end,
		   [name, algorithm, multiplier, divisor, hidden]),
    [string:join(["DIMENSION", fmt_id(Id) | V0], " ") | Acc].


report_chart_values(#{id := Id, identifier := Ident, get := Get}, Value, Acc) ->
    Set = ["SET ", fmt_id(Id), " = ", fmt(Get(Ident, Value))],
    [Set | Acc].

report_chart(_K, #{type := Type, id := Id, identifier := Ident,
		   values := Values, init := Init} = Report,
	     Interval, Acc0) ->
    Begin = if is_integer(Interval) ->
		    ["BEGIN ", fmt_id(Type) , $., fmt_id(Id), " ", fmt(Interval)];
	       true ->
		    ["BEGIN ", fmt_id(Type) , $., fmt_id(Id)]
	    end,
    Value = Init(Ident, maps:get(value, Report, undefined)),
    Acc = lists:foldl(report_chart_values(_, Value, _), [Begin | Acc0], Values),
    {Report#{value => Value}, ["END" | Acc]}.

%%%===================================================================
%%% default system charts
%%%===================================================================

erlang_simple_stats(Id, _Last) ->
    erlang:statistics(Id).

init(active_tasks, State) ->
    Value = #{algorithm => absolute, get => fun({_, Id}, Tasks) -> lists:nth(Id, Tasks) end},
    Values = [Value#{id => {active_tasks, Id}} ||
		 Id <- lists:seq(1, erlang:system_info(schedulers))],
    Chart = #{type   => erlang, id => active_tasks,
	      title  => "Active Tasks",
	      units  => "number",
	      values => Values,
	      init   => fun erlang_simple_stats/2
	     },
    register_chart(Chart, State);

init(context_switches, State) ->
    Chart = #{type   => erlang, id => context_switches,
	      title  => "Context Switches",
	      units  => "number",
	      values => [#{id => context_switches, algorithm => incremental,
			   get => fun(_, {ContextSwitches, _}) -> ContextSwitches end}],
	      init   => fun erlang_simple_stats/2
	     },
    register_chart(Chart, State);

init(exact_reductions, State) ->
    Chart = #{type => erlang, id => exact_reductions,
	      title => "Exact Reductions",
	      units => "number",
	      values => [#{id => exact_reductions, algorithm => incremental,
			   get => fun(_, {TotalExactReductions, _ExactReductionsSinceLastCall}) ->
					  TotalExactReductions end}],
	      init   => fun erlang_simple_stats/2
	     },
    register_chart(Chart, State);

init(garbage_collection, State0) ->
    Chart1 = #{type   => erlang, id => number_of_gcs, identifier => garbage_collection,
	       title  => "Number of Garbage Collection",
	       units  => "number",
	       values => [#{id => number_of_gcs, algorithm => incremental,
			    get => fun(_, {NumberofGCs, _, _}) -> NumberofGCs end}],
	       init   => fun erlang_simple_stats/2
	      },
    Chart2 = #{type   => erlang, id => words_reclaimed, identifier => garbage_collection,
	       title  => "Words Reclaimed",
	       units  => "number",
	       values => [#{id => words_reclaimed, algorithm => incremental,
			    get => fun(_, {_, WordsReclaimed, _}) -> WordsReclaimed end}],
	       init   => fun erlang_simple_stats/2
	      },
    State = register_chart(Chart1, State0),
    register_chart(Chart2, State);

init(io, State) ->
    Chart = #{type   => erlang, id => io,
	      title  => "I/O",
	      units  => "number",
	      values => [#{id => input, algorithm => incremental,
			   get => fun(_, {{input, Input}, _}) -> Input end},
			 #{id => output, algorithm => incremental,
			   get => fun(_, {_, {output, Output}}) -> Output end}],
	      init   => fun erlang_simple_stats/2
	     },
    register_chart(Chart, State);

init(reductions, State) ->
    Chart = #{type   => erlang, id => reductions,
	      title  => "Reductions",
	      units  => "number",
	      values => [#{id  => reductions, algorithm => incremental,
			   get => fun(_, {TotalReductions, _ReductionsSinceLastCall}) ->
					  TotalReductions end}],
	      init   => fun erlang_simple_stats/2
	     },
    register_chart(Chart, State);

init(run_queue, State) ->
    Chart = #{type   => erlang, id => run_queue,
	      title  => "Run Queue Length",
	      units  => "number",
	      values => [#{id => run_queue, algorithm => absolute,
			   get => fun(_, RunQueue) -> RunQueue end}],
	      init   => fun erlang_simple_stats/2
	     },
    register_chart(Chart, State);

init(run_queue_lengths, State) ->
    Value = #{algorithm => absolute, get => fun({_, Id}, Lengths) -> lists:nth(Id, Lengths) end},
    Values = [Value#{id => {run_queue_lengths, Id}} ||
		 Id <- lists:seq(1, erlang:system_info(schedulers))],
    Chart = #{type   => erlang, id => run_queue_lengths,
	      title  => "Run Queue Length",
	      units  => "number",
	      values => Values,
	      init   => fun erlang_simple_stats/2
	     },
    register_chart(Chart, State);

init(runtime, State) ->
    Chart = #{type   => erlang, id => runtime,
	      title  => "Runtime",
	      units  => "number",
	      values => [#{id => runtime, algorithm => incremental,
			   get => fun(_, {TotalRunTime, _TimeSinceLastCall}) ->
					  TotalRunTime end}],
	      init   => fun erlang_simple_stats/2
	     },
    register_chart(Chart, State);

init(scheduler_wall_time, State) ->
    erlang:system_flag(scheduler_wall_time, true),

    Get = fun({_, Id}, WallTime) ->
		  {_SchedulerId, ActiveTime, _TotalTime} =
		      lists:keyfind(Id, 1, WallTime),
		  ActiveTime
	  end,
    Value = #{algorithm => incremental, get => Get},
    Values = [Value#{id => {scheduler_wall_time, Id}} ||
		 Id <- lists:seq(1, erlang:system_info(schedulers))],
    Chart = #{type   => erlang, id => scheduler_wall_time,
	      title  => "Wall Time",
	      units  => "number",
	      values => Values,
	      init   => fun erlang_simple_stats/2
	     },
    register_chart(Chart, State);

init(total_active_tasks, State) ->
    Chart = #{type   => erlang, id => total_active_tasks,
	      title  => "Active Tasks",
	      units  => "number",
	      values => [#{id => total_active_tasks, algorithm => absolute,
			   get => fun(_, ActiveTasks) -> ActiveTasks end}],
	      init   => fun erlang_simple_stats/2
	     },
    register_chart(Chart, State);

init(total_run_queue_lengths, State) ->
    Chart = #{type   => erlang, id => total_run_queue_lengths,
	      title  => "Run Queue Lengths",
	      units  => "number",
	      values => [#{id => total_run_queue_lengths, algorithm => absolute,
			   get => fun(_, TotalRunQueueLenghts) -> TotalRunQueueLenghts end}],
	      init   => fun erlang_simple_stats/2
	     },
    register_chart(Chart, State);

init(wall_clock, State) ->
    Chart = #{type   => erlang, id => wall_clock,
	      title  => "Wall Clock",
	      units  => "number",
	      values => [#{id => wall_clock, algorithm => incremental,
			   get => fun(_, {TotalWallclockTime, _WallclockTimeSinceLastCall}) ->
					  TotalWallclockTime end}],
	      init   => fun erlang_simple_stats/2
	     },
    register_chart(Chart, State);

init(_, State) ->
    State.
