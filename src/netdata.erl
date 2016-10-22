-module(netdata).

-behaviour(gen_statem).

-compile([{parse_transform, cut},
	  {parse_transform, lager_transform}]).

%% API
-export([start_link/0]).

%% gen_statem callbacks
-export([init/1, terminate/3, code_change/4]).
-export([connect/3, reporting/3]).

-define(SERVER, ?MODULE).
-define(DEFAULT_TIMEOUT, 2).
-define(MAX_TIMEOUT, 30).

-record(state, {timeout, interval, socket}).

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

-spec init(Args :: term()) ->
		  {gen_statem:callback_mode(),
		   State :: term(), Data :: term()} |
		  {gen_statem:callback_mode(),
		   State :: term(), Data :: term(),
		   [gen_statem:action()] | gen_statem:action()} |
		  ignore |
		  {stop, Reason :: term()}.
init([]) ->
    io:format("netdata started~n"),
    {state_functions, connect, #state{timeout = ?DEFAULT_TIMEOUT}, [{timeout, 0, reconnect}]}.

-spec connect(
	gen_statem:event_type(), Msg :: term(),
	Data :: term()) ->
			gen_statem:state_function_result().
connect(timeout, reconnect, #state{timeout = TimeOut} = Data) ->
    case gen_tcp:connect({local, <<0, "/tmp/netdata">>}, 0,
			 [local, {active, true}, {mode, list}, {packet, line}]) of
	{ok, Socket} ->
	    {next_state, reporting, Data#state{socket = Socket}};
	_ when TimeOut < ?MAX_TIMEOUT ->
	    {keep_state, Data#state{timeout = TimeOut * 2}, [{timeout, TimeOut * 1000, reconnect}]};
	_ ->
	    {keep_state_and_data, [{timeout, TimeOut * 1000, reconnect}]}
    end.

-spec reporting(
	gen_statem:event_type(), Msg :: term(),
	Data :: term()) ->
			gen_statem:state_function_result().
reporting(timeout, report, #state{interval = Interval, socket = Socket}) ->
    Report= report(1),
    gen_tcp:send(Socket, Report),
    {keep_state_and_data, [{timeout, Interval, report}]};

reporting(info, {tcp, Socket, Msg}, #state{socket = Socket} = Data) ->
    case io_lib:fread("START ~d", Msg) of
	{ok, [Interval], _} ->
	    Init = init_report(Interval),
	    gen_tcp:send(Socket, Init),
	    {keep_state, Data#state{interval = Interval * 1000}, [{timeout, 0, report}]};
	_Other ->
	    keep_state_and_data
    end;
reporting(info, {tcp_closed, _Socket}, Data) ->
    {next_state, connect, Data#state{timeout = ?DEFAULT_TIMEOUT, socket = undefined}, [{timeout, 0, reconnect}]};
reporting(info, {tcp_error, _Socket, _Reason}, Data) ->
    {next_state, connect, Data#state{timeout = ?DEFAULT_TIMEOUT, socket = undefined}, [{timeout, 0, reconnect}]};
reporting(Type, Msg, Data) ->
    lager:error("In Reporting got (~p, ~p, ~p)", [Type, Msg, Data]),
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

-define(STATISTICS, [active_tasks, context_switches, exact_reductions, garbage_collection,
		     io, reductions, run_queue, run_queue_lengths, runtime, scheduler_wall_time,
		     total_active_tasks, total_run_queue_lengths, wall_clock]).

init_report(_Interval) ->
    Init = lists:foldl(fun init/2, [], ?STATISTICS),
    lists:flatten(string:join(Init, "\n")).

report(Interval) ->
    Data = lists:reverse(lists:foldl(report(_, Interval, _), [], ?STATISTICS)),
    [string:join(Data, "\n"), "\n"].

init_active_tasks(Cnt, Cnt, Acc) ->
    Acc;
init_active_tasks(Cnt, Max, Acc) ->
    Chart = io_lib:format("CHART erlang.active_task_~w '' \\\"Active Tasks #~w\\\" \\\"number\\\"", [Cnt + 1, Cnt + 1]),
    Dim = io_lib:format("DIMENSION active_tasks_~w '' absolute 1 1", [Cnt + 1]),
    init_active_tasks(Cnt + 1, Max, [Chart, Dim | Acc]).

init_run_queue_lengths(Cnt, Cnt, Acc) ->
    Acc;
init_run_queue_lengths(Cnt, Max, Acc) ->
    Chart = io_lib:format("CHART erlang.run_queue_lengths_~w '' \\\"Run Queue Length #~w\\\" \\\"number\\\"", [Cnt + 1, Cnt + 1]),
    Dim = io_lib:format("DIMENSION run_queue_lengths_~w '' absolute 1 1", [Cnt + 1]),
    init_run_queue_lengths(Cnt + 1, Max, [Chart, Dim | Acc]).

init_scheduler_wall_time(Cnt, Cnt, Acc) ->
    Acc;
init_scheduler_wall_time(Cnt, Max, Acc) ->
    Chart = io_lib:format("CHART erlang.wall_time_~w '' \\\"Wall Time #~w\\\" \\\"number\\\"", [Cnt + 1, Cnt + 1]),
    Dim = io_lib:format("DIMENSION scheduler_wall_time_~w '' absolute 1 1", [Cnt + 1]),
    init_scheduler_wall_time(Cnt + 1, Max, [Chart, Dim | Acc]).

init(active_tasks, Acc) ->
    init_active_tasks(0, erlang:system_info(schedulers), Acc);

init(context_switches, Acc) ->
    Chart = "CHART erlang.context_switches '' \\\"Context Switches\\\" \\\"number\\\"",
    Dim = "DIMENSION context_switches  '' absolute 1 1",
    [Chart, Dim | Acc];

init(exact_reductions, Acc) ->
    Chart = "CHART erlang.exact_reductions '' \\\"Exact Reductions\\\" \\\"number\\\"",
    Dim = "DIMENSION exact_reductions '' absolute 1 1",
    [Chart, Dim | Acc];

init(garbage_collection, Acc) ->
    ChartGCs = "CHART erlang.number_of_gcs '' \\\"Number of Garbage Collection\\\" \\\"number\\\"",
    DimGCs = "DIMENSION number_of_gcs '' absolute 1 1",
    ChartReclaimed = "CHART erlang.words_reclaimed '' \\\"Words Reclaimed\\\" \\\"number\\\"",
    DimReclaimed = "DIMENSION words_reclaimed '' absolute 1 1",
    [ChartGCs, DimGCs, ChartReclaimed, DimReclaimed | Acc];

init(io, Acc) ->
    InputChart = "CHART erlang.input '' \\\"Input\\\" \\\"number\\\"",
    OutputChart = "CHART erlang.output '' \\\"Output\\\" \\\"number\\\"",
    InputDim = "DIMENSION input '' absolute 1 1",
    OutputDim = "DIMENSION output '' absolute 1 1",
    [InputChart, InputDim, OutputChart, OutputDim | Acc];

init(reductions, Acc) ->
    Chart = "CHART erlang.reductions '' \\\"Reductions\\\" \\\"number\\\"",
    Dim = "DIMENSION reductions '' absolute 1 1",
    [Chart, Dim | Acc];

init(run_queue, Acc) ->
    Chart = "CHART erlang.run_queue '' \\\"Run Queue Length\\\" \\\"number\\\"",
    Dim = "DIMENSION run_queue '' absolute 1 1",
    [Chart, Dim | Acc];

init(run_queue_lengths, Acc) ->
    init_run_queue_lengths(0, erlang:system_info(schedulers), Acc);

init(runtime, Acc) ->
    Chart = "CHART erlang.runtime '' \\\"Runtime\\\" \\\"number\\\"",
    Dim = "DIMENSION runtime '' absolute 1 1",
    [Chart, Dim | Acc];

init(scheduler_wall_time, Acc) ->
    erlang:system_flag(scheduler_wall_time, true),
    init_scheduler_wall_time(0, erlang:system_info(schedulers), Acc);

init(total_active_tasks, Acc) ->
    Chart = "CHART erlang.total_active_tasks '' \\\"Active Tasks\\\" \\\"number\\\"",
    Dim = "DIMENSION total_active_tasks '' absolute 1 1",
    [Chart, Dim | Acc];

init(total_run_queue_lengths, Acc) ->
    Chart = "CHART erlang.total_run_queue_lengths '' \\\"Run Queue Lengths\\\" \\\"number\\\"",
    Dim = "DIMENSION total_run_queue_lengths '' absolute 1 1",
    [Chart, Dim | Acc];

init(wall_clock, Acc) ->
    Chart = "CHART erlang.wall_clock '' \\\"Wall Clock\\\" \\\"number\\\"",
    Dim = "DIMENSION wall_clock '' absolute 1 1",
    [Chart, Dim | Acc];

init(_, Acc) ->
    Acc.

report_chart_values([], Acc) ->
    Acc;
report_chart_values([{{Id, Cnt}, Value} | T], Acc) ->
    Set = io_lib:format("SET ~s_~w = ~w", [Id, Cnt, Value]),
    report_chart_values(T, [Set | Acc]);
report_chart_values([{Id, Value} | T], Acc) ->
    Set = io_lib:format("SET ~s = ~w", [Id, Value]),
    report_chart_values(T, [Set | Acc]).

report_chart(Type, {Id, Cnt}, Interval, Values, Acc0) ->
    Begin = io_lib:format("BEGIN ~s.~s_~w ~w", [Type, Id, Cnt, Interval]),
    Acc = report_chart_values(Values, [Begin | Acc0]),
    ["END" | Acc];
report_chart(Type, Id, Interval, Values, Acc0) ->
    Begin = io_lib:format("BEGIN ~s.~s ~w", [Type, Id, Interval]),
    Acc = report_chart_values(Values, [Begin | Acc0]),
    ["END" | Acc].

report_active_tasks(_Interval, [], _Cnt, Acc) ->
    Acc;
report_active_tasks(Interval, [ActiveTasks | T], Cnt, Acc0) ->
    Acc = report_chart(erlang, {active_tasks, Cnt + 1}, Interval, [{{active_tasks, Cnt + 1}, ActiveTasks}], Acc0),
    report_active_tasks(Interval, T, Cnt + 1, Acc).

report_run_queue_lengths(_Interval, [], _Cnt, Acc) ->
    Acc;
report_run_queue_lengths(Interval, [RunQueueLength | T], Cnt, Acc0) ->
    Acc = report_chart(erlang, {run_queue_lengths, Cnt + 1}, Interval, [{{run_queue_lenths, Cnt + 1}, RunQueueLength}], Acc0),
    report_run_queue_lengths(Interval, T, Cnt + 1, Acc).

report_scheduler_wall_time(_Interval, undefined, Acc) ->
    Acc;
report_scheduler_wall_time(_Interval, [], Acc) ->
    Acc;
report_scheduler_wall_time(Interval, [{SchedulerId, ActiveTime, _TotalTime} | T], Acc0) ->
    Acc = report_chart(erlang, {scheduler_wall_time, SchedulerId}, Interval, [{{scheduler_wall_time, SchedulerId}, ActiveTime}], Acc0),
    report_scheduler_wall_time(Interval, T, Acc).

report(active_tasks, Interval, Acc) ->
    ActiveTasks = erlang:statistics(active_tasks),
    report_active_tasks(Interval, ActiveTasks, 0, Acc);

report(context_switches, Interval, Acc) ->
    {ContextSwitches, _} = erlang:statistics(context_switches),
    report_chart(erlang, context_switches, Interval, [{context_switches, ContextSwitches}], Acc);

report(exact_reductions, Interval, Acc) ->
    {TotalExactReductions, _ExactReductionsSinceLastCall}
	= erlang:statistics(exact_reductions),
    report_chart(erlang, exact_reductions, Interval, [{exact_reductions, TotalExactReductions}], Acc);

report(garbage_collection, Interval, Acc0) ->
    {NumberofGCs, WordsReclaimed, _} = erlang:statistics(garbage_collection),
    Acc = report_chart(erlang, number_of_gcs, Interval, [{number_of_gcs, NumberofGCs}], Acc0),
    report_chart(erlang, words_reclaimed, Interval, [{words_reclaimed, WordsReclaimed}], Acc);

report(io, Interval, Acc0) ->
    {{input, Input}, {output, Output}} = erlang:statistics(io),
    Acc = report_chart(erlang, input, Interval, [{input, Input}], Acc0),
    report_chart(erlang, output, Interval, [{output, Output}], Acc);

report(reductions, Interval, Acc) ->
    {TotalReductions, _ReductionsSinceLastCall}
	= erlang:statistics(reductions),
    report_chart(erlang, reductions, Interval, [{reductions, TotalReductions}], Acc);

report(run_queue, Interval, Acc) ->
    RunQueue = erlang:statistics(run_queue),
    report_chart(erlang, run_queue, Interval, [{run_queue, RunQueue}], Acc);

report(run_queue_lengths, Interval, Acc) ->
    RunQueueLenghts = erlang:statistics(run_queue_lengths),
    report_run_queue_lengths(Interval, RunQueueLenghts, 0, Acc);

report(runtime, Interval, Acc) ->
    {TotalRunTime, _TimeSinceLastCall}
	= erlang:statistics(runtime),
    report_chart(erlang, runtime, Interval, [{runtime, TotalRunTime}], Acc);

report(scheduler_wall_time, Interval, Acc) ->
    SchedulerWallTimes = erlang:statistics(scheduler_wall_time),
    report_scheduler_wall_time(Interval, SchedulerWallTimes, Acc);

report(total_active_tasks, Interval, Acc) ->
    ActiveTasks = erlang:statistics(total_active_tasks),
    report_chart(erlang, total_active_tasks, Interval, [{total_active_tasks, ActiveTasks}], Acc);

report(total_run_queue_lengths, Interval, Acc) ->
    TotalRunQueueLenghts = erlang:statistics(total_run_queue_lengths),
    report_chart(erlang, total_run_queue_lengths, Interval, [{total_run_queue_lengths, TotalRunQueueLenghts}], Acc);

report(wall_clock, Interval, Acc) ->
    {TotalWallclockTime, _WallclockTimeSinceLastCall}
	= erlang:statistics(wall_clock),
    report_chart(erlang, wall_clock, Interval, [{wall_clock, TotalWallclockTime}], Acc);

report(_, _Interval, Acc) ->
    Acc.

