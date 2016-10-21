-module(netdata).

-export([init/0, data/0]).

%%%===================================================================
%%% API
%%%===================================================================

-define(STATISTICS, [active_tasks, context_switches, exact_reductions, garbage_collection,
		     io, reductions, run_queue, run_queue_lengths, runtime, scheduler_wall_time,
		     total_active_tasks, total_run_queue_lengths, wall_clock]).

init() ->
    Init = lists:foldl(fun init/2, [], ?STATISTICS),
    lists:flatten(string:join(Init, "\n")).

data() ->
    Data = lists:foldl(fun data/2, [], ?STATISTICS),
    lists:flatten(string:join(Data, "\n")).

%%%===================================================================
%%% Internal functions
%%%===================================================================

init_active_tasks(Cnt, Cnt, Acc) ->
    Acc;
init_active_tasks(Cnt, Max, Acc) ->
    Chart = io_lib:format("CHART erlang.active_task_~w '' \"Active Tasks #~w\" \"number\"", [Cnt + 1, Cnt + 1]),
    Dim = io_lib:format("DIMENSION active_tasks_~w '' absolute 1 1", [Cnt + 1]),
    init_active_tasks(Cnt + 1, Max, [Chart, Dim | Acc]).

init_run_queue_lengths(Cnt, Cnt, Acc) ->
    Acc;
init_run_queue_lengths(Cnt, Max, Acc) ->
    Chart = io_lib:format("CHART erlang.run_queue_lengths_~w '' \"Run Queue Length #~w\" \"number\"", [Cnt + 1, Cnt + 1]),
    Dim = io_lib:format("DIMENSION run_queue_lengths_~w '' absolute 1 1", [Cnt + 1]),
    init_run_queue_lengths(Cnt + 1, Max, [Chart, Dim | Acc]).

init_scheduler_wall_time(Cnt, Cnt, Acc) ->
    Acc;
init_scheduler_wall_time(Cnt, Max, Acc) ->
    Chart = io_lib:format("CHART erlang.wall_time_~w '' \"Wall Time #~w\" \"number\"", [Cnt + 1, Cnt + 1]),
    Dim = io_lib:format("DIMENSION scheduler_wall_time_~w '' absolute 1 1", [Cnt + 1]),
    init_scheduler_wall_time(Cnt + 1, Max, [Chart, Dim | Acc]).

init(active_tasks, Acc) ->
    init_active_tasks(0, erlang:system_info(schedulers), Acc);

init(context_switches, Acc) ->
    Chart = "CHART erlang.context_switches '' \"Context Switches\" \"number\"",
    Dim = "DIMENSION context_switches  '' absolute 1 1",
    [Chart, Dim | Acc];

init(exact_reductions, Acc) ->
    Chart = "CHART erlang.exact_reductions '' \"Exact Reductions\" \"number\"",
    Dim = "DIMENSION exact_reductions '' absolute 1 1",
    [Chart, Dim | Acc];

init(garbage_collection, Acc) ->
    ChartGCs = "CHART erlang.number_of_gcs '' \"Number of Garbage Collection\" \"number\"",
    DimGCs = "DIMENSION number_of_gcs '' absolute 1 1",
    ChartReclaimed = "CHART erlang.words_reclaimed '' \"Words Reclaimed\" \"number\"",
    DimReclaimed = "DIMENSION words_reclaimed '' absolute 1 1",
    [ChartGCs, DimGCs, ChartReclaimed, DimReclaimed | Acc];

init(io, Acc) ->
    InputChart = "CHART erlang.input '' \"Input\" \"number\"",
    OutputChart = "CHART erlang.output '' \"Output\" \"number\"",
    InputDim = "DIMENSION input '' absolute 1 1",
    OutputDim = "DIMENSION output '' absolute 1 1",
    [InputChart, InputDim, OutputChart, OutputDim | Acc];

init(reductions, Acc) ->
    Chart = "CHART erlang.reductions '' \"Reductions\" \"number\"",
    Dim = "DIMENSION reductions '' absolute 1 1",
    [Chart, Dim | Acc];

init(run_queue, Acc) ->
    Chart = "CHART erlang.run_queue '' \"Run Queue Length\" \"number\"",
    Dim = "DIMENSION run_queue '' absolute 1 1",
    [Chart, Dim | Acc];

init(run_queue_lengths, Acc) ->
    init_run_queue_lengths(0, erlang:system_info(schedulers), Acc);

init(runtime, Acc) ->
    Chart = "CHART erlang.runtime '' \"Runtime\" \"number\"",
    Dim = "DIMENSION runtime '' absolute 1 1",
    [Chart, Dim | Acc];

init(scheduler_wall_time, Acc) ->
    erlang:system_flag(scheduler_wall_time, true),
    init_scheduler_wall_time(0, erlang:system_info(schedulers), Acc);

init(total_active_tasks, Acc) ->
    Chart = "CHART erlang.total_active_tasks '' \"Active Tasks\" \"number\"",
    Dim = "DIMENSION total_active_tasks '' absolute 1 1",
    [Chart, Dim | Acc];

init(total_run_queue_lengths, Acc) ->
    Chart = "CHART erlang.total_run_queue_lengths '' \"Run Queue Lengths\" \"number\"",
    Dim = "DIMENSION total_run_queue_lengths '' absolute 1 1",
    [Chart, Dim | Acc];

init(wall_clock, Acc) ->
    Chart = "CHART erlang.wall_clock '' \"Wall Clock\" \"number\"",
    Dim = "DIMENSION wall_clock '' absolute 1 1",
    [Chart, Dim | Acc];

init(_, Acc) ->
    Acc.

data_active_tasks([], _Cnt, Acc) ->
    Acc;
data_active_tasks([ActiveTasks | T], Cnt, Acc) ->
    Data = io_lib:format("BEGIN erlang.active_tasks_~w\nSET active_tasks_~w = ~w\nEND", [Cnt + 1, Cnt + 1, ActiveTasks]),
    data_active_tasks(T, Cnt + 1, [Data | Acc]).

data_run_queue_lengths([], _Cnt, Acc) ->
    Acc;
data_run_queue_lengths([RunQueueLenght | T], Cnt, Acc) ->
    Data = io_lib:format("BEGIN erlang.run_queue_lengths_~w\nSET run_queue_lengths_~w = ~w\nEND", [Cnt + 1, Cnt + 1, RunQueueLenght]),
    data_run_queue_lengths(T, Cnt + 1, [Data | Acc]).

data_scheduler_wall_time(undefined, Acc) ->
    Acc;
data_scheduler_wall_time([], Acc) ->
    Acc;
data_scheduler_wall_time([{SchedulerId, ActiveTime, _TotalTime} | T], Acc) ->
    Data = io_lib:format("BEGIN erlang.scheduler_wall_time_~w\nSET scheduler_wall_time_~w = ~w\nEND", [SchedulerId, SchedulerId, ActiveTime]),
    data_scheduler_wall_time(T, [Data | Acc]).

data(active_tasks, Acc) ->
    ActiveTasks = erlang:statistics(active_tasks),
    data_active_tasks(ActiveTasks, 0, Acc);

data(context_switches, Acc) ->
    {ContextSwitches, _} = erlang:statistics(context_switches),
    Data = io_lib:format("BEGIN erlang.context_switches\nSET context_switches = ~w\nEND", [ContextSwitches]),
    [Data | Acc];

data(exact_reductions, Acc) ->
    {TotalExactReductions, _ExactReductionsSinceLastCall}
	= erlang:statistics(exact_reductions),
    Data = io_lib:format("BEGIN erlang.exact_reductions\nSET exact_reductions = ~w\nEND", [TotalExactReductions]),
    [Data | Acc];

data(garbage_collection, Acc) ->
    {NumberofGCs, WordsReclaimed, _} = erlang:statistics(garbage_collection),
    DataGCs = io_lib:format("BEGIN erlang.number_of_gcs\nSET number_of_gcs = ~w\nEND", [NumberofGCs]),
    DataReclaimed = io_lib:format("BEGIN erlang.words_reclaimed\nSET words_reclaimed = ~w\nEND", [WordsReclaimed]),
    [DataGCs, DataReclaimed | Acc];

data(io, Acc) ->
    {{input, Input}, {output, Output}} = erlang:statistics(io),
    InputData = io_lib:format("BEGIN erlang.input\nSET input = ~w\nEND", [Input]),
    OutputData = io_lib:format("BEGIN erlang.output\nSET output = ~w\nEND", [Output]),
    [InputData, OutputData | Acc];

data(reductions, Acc) ->
    {TotalReductions, _ReductionsSinceLastCall}
	= erlang:statistics(reductions),
    Data = io_lib:format("BEGIN erlang.reductions\nSET reductions = ~w\nEND", [TotalReductions]),
    [Data | Acc];

data(run_queue, Acc) ->
    RunQueue = erlang:statistics(run_queue),
    Data = io_lib:format("BEGIN erlang.run_queue\nSET run_queue = ~w\nEND", [RunQueue]),
    [Data | Acc];

data(run_queue_lengths, Acc) ->
    RunQueueLenghts = erlang:statistics(run_queue_lengths),
    data_run_queue_lengths(RunQueueLenghts, 0, Acc);

data(runtime, Acc) ->
    {TotalRunTime, _TimeSinceLastCall}
	= erlang:statistics(runtime),
    Data = io_lib:format("BEGIN erlang.runtime\nSET runtime = ~w\nEND", [TotalRunTime]),
    [Data | Acc];

data(scheduler_wall_time, Acc) ->
    SchedulerWallTimes = erlang:statistics(scheduler_wall_time),
    data_scheduler_wall_time(SchedulerWallTimes, Acc);

data(total_active_tasks, Acc) ->
    ActiveTasks = erlang:statistics(total_active_tasks),
    Data = io_lib:format("BEGIN erlang.total_active_tasks\nSET total_active_tasks = ~w\nEND", [ActiveTasks]),
    [Data | Acc];

data(total_run_queue_lengths, Acc) ->
    TotalRunQueueLenghts = erlang:statistics(total_run_queue_lengths),
    Data = io_lib:format("BEGIN erlang.total_run_queue_lengths\nSET total_run_queue_lengths = ~w\nEND", [TotalRunQueueLenghts]),
    [Data | Acc];

data(wall_clock, Acc) ->
    {TotalWallclockTime, _WallclockTimeSinceLastCall}
	= erlang:statistics(wall_clock),
    Data = io_lib:format("BEGIN erlang.wall_clock\nSET wall_clock = ~w\nEND", [TotalWallclockTime]),
    [Data | Acc];

data(X, Acc) ->
    [X | Acc].

