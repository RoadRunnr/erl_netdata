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
-export([start_link/0]).

%% gen_statem callbacks
-export([callback_mode/0, init/1, terminate/3, code_change/4]).
-export([connect/3, reporting/3]).

-define(SERVER, ?MODULE).
-define(DEFAULT_TIMEOUT, 2).
-define(MAX_TIMEOUT, 30).

-record(state, {charts, timer, timeout, interval, last, socket}).

-define(STATISTICS, [active_tasks, context_switches, exact_reductions, garbage_collection,
		     io, reductions, run_queue, run_queue_lengths, runtime, scheduler_wall_time,
		     total_active_tasks, total_run_queue_lengths, wall_clock]).

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link() ->
			{ok, Pid :: pid()} |
			ignore |
			{error, Error :: term()}.
start_link() ->
    gen_statem:start_link({local, ?SERVER}, ?MODULE, [], []).

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
    State0 = #state{charts = [], timeout = ?DEFAULT_TIMEOUT},
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
    end.

-spec reporting(
	gen_statem:event_type(), Msg :: term(),
	Data :: term()) ->
			gen_statem:state_function_result().
reporting(Type, report, #state{charts = ChartsIn, interval = Interval, socket = Socket, last = Last} = Data)
  when Type == internal; Type == info ->
    Now = erlang:monotonic_time(millisecond),
    TDiff = if is_integer(Last) -> Now - Last;
	       true             -> undefined
	    end,
    {Report, ChartsOut} = report(TDiff, ChartsIn),
    gen_tcp:send(Socket, Report),

    TRef = erlang:send_after(Interval, self(), report),
    {keep_state, Data#state{charts = ChartsOut, timer = TRef, last = Now}};

reporting(info, {tcp, Socket, Msg}, #state{charts = Charts, socket = Socket} = Data) ->
    case io_lib:fread("START ~d", Msg) of
	{ok, [Interval], _} ->
	    Init = init_report(Interval, Charts),
	    gen_tcp:send(Socket, Init),

	    {keep_state, Data#state{interval = Interval * 1000}, [{next_event, internal, report}]};
	_Other ->
	    keep_state_and_data
    end;
reporting(info, {tcp_closed, _Socket}, Data) ->
    cancel_timer(Data),
    {next_state, connect, Data#state{timeout = ?DEFAULT_TIMEOUT, socket = undefined}, [{next_event, internal, reconnect}]};
reporting(info, {tcp_error, _Socket, _Reason}, Data) ->
    cancel_timer(Data),
    {next_state, connect, Data#state{timeout = ?DEFAULT_TIMEOUT, socket = undefined}, [{next_event, internal, reconnect}]};
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

-define(DEFAULT_CHART, #{type         => undefined,
			 id           => undefined,
			 name         => undefined,
			 title        => undefined,
			 units        => undefined,
			 charttype    => line,
			 priority     => 1000,
			 values       => [],
			 init         => fun(X) -> X end}).

-define(DEFAULT_VALUE, #{id         => undefined,
			 name       => undefined,
			 algorithm  => undefined,
			 multiplier => 1,
			 divisor    => 1,
			 hidden     => undefined,
			 get        => fun(_Id, _X) -> 0 end}).

register_chart(Chart0, #state{charts = Charts} = State) ->
    Chart1 = maps:merge(?DEFAULT_CHART, Chart0),
    Chart2 = Chart1#{values =>
			 [maps:merge(?DEFAULT_VALUE, V) ||
			     V <- maps:get(values, Chart1, [])]},
    State#state{charts = [Chart2 | Charts]}.

init_report(Interval, Charts) ->
    Charts1 = lists:foldl(init_chart(_, Interval, _), [], Charts),
    Charts2 = lists:reverse(Charts1),
    [string:join(Charts2, "\n"), "\n"].

report(Interval, ChartsIn) ->
    {ChartsOut, Reports1} = lists:mapfoldl(report_chart(_, Interval, _), [], ChartsIn),
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

init_chart(#{type := Type, id := Id, values := Values} = Def, Interval, Acc) ->
    C0 = lists:map(fun({K, Default}) ->
			   fmt(maps:get(K, Def, Default));
		      (K) ->
			   fmt(maps:get(K, Def, undefined))
		   end,
		   [name, title, units,
		    {family, Id}, {context, "Erlang"},
		    charttype, priority,
		    {update_every, Interval}]),
    Chart = string:join(["CHART", [fmt(Type), $., fmt(Id)] | C0], " "),
    lists:foldl(fun(V, A) -> init_chart_values(V, A) end, [Chart | Acc], Values).

init_chart_values(Value, Acc) ->
    V0 = lists:map(fun(K) -> fmt(maps:get(K, Value, undefined)) end,
		   [id, name, algorithm, multiplier, divisor, hidden]),
    [string:join(["DIMENSION" | V0], " ") | Acc].


report_chart_values(#{id := Id, get := Get}, Value, Acc) ->
    Set = ["SET ", fmt(Id), " = ", fmt(Get(Id, Value))],
    [Set | Acc].

report_chart(#{type := Type, id := Id, values := Values, init := Init} = Report,
	     Interval, Acc0) ->
    Begin = if is_integer(Interval) ->
		    ["BEGIN ", fmt(Type) , $., fmt(Id), " ", fmt(Interval)];
	       true ->
		    ["BEGIN ", fmt(Type) , $., fmt(Id)]
	    end,
    Value = Init(maps:get(value, Report, undefined)),
    Acc = lists:foldl(report_chart_values(_, Value, _), [Begin | Acc0], Values),
    {Report#{value => Value}, ["END" | Acc]}.

%%%===================================================================
%%% default system charts
%%%===================================================================

init(active_tasks, State) ->
    Value = #{algorithm => absolute, get => fun({_, Id}, Tasks) -> lists:nth(Id, Tasks) end},
    Values = [Value#{id => {active_tasks, Id}} ||
		 Id <- lists:seq(1, erlang:system_info(schedulers))], 
    Chart = #{type   => erlang, id => active_tasks,
	      title  => "Active Tasks",
	      units  => "number",
	      values => Values,
	      init   => fun(_) -> erlang:statistics(active_tasks) end
	     },
    register_chart(Chart, State);

init(context_switches, State) ->
    Chart = #{type   => erlang, id => context_switches,
	      title  => "Context Switches",
	      units  => "number",
	      values => [#{id => context_switches, algorithm => incremental,
			   get => fun(_, {ContextSwitches, _}) -> ContextSwitches end}],
	      init   => fun(_) -> erlang:statistics(context_switches) end
	     },
    register_chart(Chart, State);

init(exact_reductions, State) ->
    Chart = #{type => erlang, id => exact_reductions,
	      title => "Exact Reductions",
	      units => "number",
	      values => [#{id => exact_reductions, algorithm => incremental,
			   get => fun(_, {TotalExactReductions, _ExactReductionsSinceLastCall}) ->
					  TotalExactReductions end}],
	      init => fun(_) -> erlang:statistics(exact_reductions) end
	     },
    register_chart(Chart, State);

init(garbage_collection, State0) ->
    Chart1 = #{type   => erlang, id => number_of_gcs,
	       title  => "Number of Garbage Collection",
	       units  => "number",
	       values => [#{id => number_of_gcs, algorithm => incremental,
			    get => fun(_, {NumberofGCs, _, _}) -> NumberofGCs end}],
	       init   => fun(_) -> erlang:statistics(garbage_collection) end
	      },
    Chart2 = #{type   => erlang, id => words_reclaimed,
	       title  => "Words Reclaimed",
	       units  => "number",
	       values => [#{id => words_reclaimed, algorithm => incremental,
			    get => fun(_, {_, WordsReclaimed, _}) -> WordsReclaimed end}],
	       init   => fun(_) -> erlang:statistics(garbage_collection) end
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
	      init   => fun(_) -> erlang:statistics(io) end
	     },
    register_chart(Chart, State);

init(reductions, State) ->
    Chart = #{type   => erlang, id => reductions,
	      title  => "Reductions",
	      units  => "number",
	      values => [#{id  => reductions, algorithm => incremental,
			   get => fun(_, {TotalReductions, _ReductionsSinceLastCall}) ->
					  TotalReductions end}],
	      init   => fun(_) -> erlang:statistics(reductions) end
	     },
    register_chart(Chart, State);

init(run_queue, State) ->
    Chart = #{type   => erlang, id => run_queue,
	      title  => "Run Queue Length",
	      units  => "number",
	      values => [#{id => run_queue, algorithm => absolute,
			   get => fun(_, RunQueue) -> RunQueue end}],
	      init   => fun(_) -> erlang:statistics(run_queue) end
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
	      init   => fun(_) -> erlang:statistics(run_queue_lengths) end
	     },
    register_chart(Chart, State);

init(runtime, State) ->
    Chart = #{type   => erlang, id => runtime,
	      title  => "Runtime",
	      units  => "number",
	      values => [#{id => runtime, algorithm => incremental,
			   get => fun(_, {TotalRunTime, _TimeSinceLastCall}) ->
					  TotalRunTime end}],
	      init   => fun(_) -> erlang:statistics(runtime) end
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
	      init   => fun(_) -> erlang:statistics(scheduler_wall_time) end
	     },
    register_chart(Chart, State);

init(total_active_tasks, State) ->
    Chart = #{type   => erlang, id => total_active_tasks,
	      title  => "Active Tasks",
	      units  => "number",
	      values => [#{id => total_active_tasks, algorithm => absolute,
			   get => fun(_, ActiveTasks) -> ActiveTasks end}],
	      init   => fun(_) -> erlang:statistics(total_active_tasks) end
	     },
    register_chart(Chart, State);

init(total_run_queue_lengths, State) ->
    Chart = #{type   => erlang, id => total_run_queue_lengths,
	      title  => "Run Queue Lengths",
	      units  => "number",
	      values => [#{id => total_run_queue_lengths, algorithm => absolute,
			   get => fun(_, TotalRunQueueLenghts) -> TotalRunQueueLenghts end}],
	      init   => fun(_) -> erlang:statistics(total_run_queue_lengths) end
	     },
    register_chart(Chart, State);

init(wall_clock, State) ->
    Chart = #{type   => erlang, id => wall_clock,
	      title  => "Wall Clock",
	      units  => "number",
	      values => [#{id => wall_clock, algorithm => incremental,
			   get => fun(_, {TotalWallclockTime, _WallclockTimeSinceLastCall}) ->
					  TotalWallclockTime end}],
	      init   => fun(_) -> erlang:statistics(wall_clock) end
	     },
    register_chart(Chart, State);

init(_, State) ->
    State.
