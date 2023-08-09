% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

% Scanner plugin server.
%
% This is gen_server starts, stops and reschedules plugin processes.
%

-module(couch_scanner_server).

-export([
    start_link/0,
    status/0
]).

-export([
    init/1,
    terminate/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-define(SCHED_INTERVAL_SEC, 5).
-define(PENALTY_BASE_SEC, 60).
-define(MAX_PENALTY_SEC, 8 * 3600).
-define(HEAL_THRESHOLD_SEC, 5 * 60).

-record(sched, {
    start_time = 0,
    error_count = 0,
    reschedule = 0
}).

-record(st, {
    pids = #{},
    scheduling = #{},
    tref
}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

status() ->
    gen_server:call(?MODULE, status, infinity).

% Private

init(_Args) ->
    process_flag(trap_exit, true),
    {ok, #st{}, interval_msec()}.

terminate(_Reason, #st{pids = Pids} = St) ->
    ToStop = maps:keys(Pids),
    lists:foldl(fun stop_plugin/2, St, ToStop),
    ok.

handle_call(status, _From, #st{} = St) ->
    Fun = fun(_, Sched) ->
        #{
            start_time => Sched#sched.start_time,
            error_count => Sched#sched.error_count,
            reschedule => Sched#sched.reschedule
        }
    end,
    SchedMaps = maps:map(Fun, St#st.scheduling),
    {reply, #{pids => St#st.pids, scheduling => SchedMaps}, St};
handle_call(Msg, _From, #st{} = St) ->
    couch_log:error("~p : unknown call ~p", [?MODULE, Msg]),
    {reply, {error, {invalid_call, Msg}}, St}.

handle_cast(Msg, #st{} = St) ->
    couch_log:error("~p : unknown cast ~p", [?MODULE, Msg]),
    {noreply, St}.

handle_info(timeout, #st{} = St) ->
    St1 =
        case in_maintenance() of
            true -> stop_in_maintenance(St);
            false -> start_stop_cfg(St)
        end,
    {noreply, schedule_timeout(St1)};
handle_info({'EXIT', Pid, Reason}, #st{pids = Pids} = St) ->
    case maps:filter(fun(_, P) -> P =:= Pid end, Pids) of
        Map when map_size(Map) == 1 ->
            [{Id, Pid}] = maps:to_list(Map),
            St1 = St#st{pids = maps:remove(Id, Pids)},
            {noreply, handle_exit(Id, Reason, St1)};
        Map when map_size(Map) == 0 ->
            {noreply, St}
    end;
handle_info(Msg, St) ->
    couch_log:error("~p : unknown info message ~p", [?MODULE, Msg]),
    {noreply, St}.

stop_in_maintenance(#st{pids = Pids} = St) ->
    case map_size(Pids) > 0 of
        true ->
            couch_log:info("~p stopping in maintenance mode", [?MODULE]),
            lists:foldl(fun stop_plugin/2, St, maps:keys(Pids));
        false ->
            St
    end.

start_stop_cfg(#st{pids = Pids, scheduling = Scheduling} = St) ->
    PluginIds = plugins(),
    RunningIds = maps:keys(Pids),
    ToStart = PluginIds -- RunningIds,
    ToStop = RunningIds -- PluginIds,
    St1 = lists:foldl(fun stop_plugin/2, St, ToStop),
    lists:foreach(fun couch_scanner_checkpoint:reset/1, ToStop),
    ToRemove = maps:keys(Scheduling) -- PluginIds,
    St2 = St1#st{scheduling = maps:without(ToRemove, Scheduling)},
    lists:foldl(fun start_plugin/2, St2, ToStart).

stop_plugin(Id, #st{} = St) ->
    #st{pids = Pids, scheduling = Scheduling} = St,
    {Pid, Pids1} = maps:take(Id, Pids),
    #{Id := #sched{} = Sched} = Scheduling,
    couch_log:info("~p : stopping ~s", [?MODULE, Id]),
    ok = couch_scanner_plugin:stop(Pid),
    Sched1 = Sched#sched{start_time = 0, reschedule = 0},
    Scheduling1 = Scheduling#{Id := Sched1},
    St#st{pids = Pids1, scheduling = Scheduling1}.

start_plugin(Id, #st{pids = Pids, scheduling = Scheduling} = St) ->
    Sched = maps:get(Id, Scheduling, #sched{}),
    NowSec = tsec(),
    case NowSec >= Sched#sched.reschedule of
        true ->
            couch_log:info("~p : starting ~s", [?MODULE, Id]),
            Pids1 = Pids#{Id => couch_scanner_plugin:spawn_link(Id)},
            Sched1 = Sched#sched{
                start_time = NowSec,
                reschedule = 0
            },
            Scheduling1 = Scheduling#{Id => Sched1},
            St#st{pids = Pids1, scheduling = Scheduling1};
        false ->
            St
    end.

plugins() ->
    Fun = fun
        ({K, "true"}, Acc) ->
            FullName = "couch_scanner_plugin_" ++ K,
            [list_to_binary(FullName) | Acc];
        ({_, _}, Acc) ->
            Acc
    end,
    lists:foldl(Fun, [], config:get("couch_scanner_plugins")).

handle_exit(Id, Reason, #st{} = St) ->
    #st{scheduling = Scheduling} = St,
    #{Id := Sched} = Scheduling,
    Sched1 = sched_exit_update(Id, Sched, Reason),
    St#st{scheduling = Scheduling#{Id := Sched1}}.

sched_exit_update(Id, #sched{} = Sched, {shutdown, {reschedule, TSec}}) ->
    couch_log:notice("~p : ~p rescheduled after ~p", [?MODULE, Id, TSec]),
    Sched#sched{start_time = 0, error_count = 0, reschedule = TSec};
sched_exit_update(Id, #sched{} = Sched, Norm) when
    Norm == shutdown; Norm == normal
->
    couch_log:notice("~p : ~p finished", [?MODULE, Id]),
    Sched#sched{start_time = 0, error_count = 0, reschedule = infinity};
sched_exit_update(Id, #sched{} = Sched, Error) ->
    couch_log:error("~p : ~p exited with error ~p", [?MODULE, Id, Error]),
    #sched{start_time = StartSec, error_count = ErrorCount} = Sched,
    Sched1 = Sched#sched{start_time = 0},
    NowSec = tsec(),
    % If process has been running successfully for a while without crashing
    % reset (forgive) its previous errors.
    case NowSec - StartSec =< heal_threshold_sec() of
        true -> penalize(NowSec, Sched1#sched{error_count = 1});
        false -> penalize(NowSec, Sched1#sched{error_count = ErrorCount + 1})
    end.

penalize(NowSec, #sched{error_count = ErrorCount} = Sched) ->
    PenaltySec = ?PENALTY_BASE_SEC * (1 bsl (ErrorCount - 1)),
    PenaltySec1 = min(PenaltySec, max_penalty_sec()),
    Sched#sched{reschedule = NowSec + PenaltySec1}.

in_maintenance() ->
    "false" /= config:get("couchdb", "maintenance_mode", "false").

tsec() ->
    erlang:system_time(second).

schedule_timeout(#st{tref = TRef} = St) ->
    case TRef of
        undefined -> ok;
        _ when is_reference(TRef) -> erlang:cancel_timer(TRef)
    end,
    TRef1 = erlang:send_after(interval_msec(), self(), timeout),
    St#st{tref = TRef1}.

interval_msec() ->
    1000 * cfg_int_param("sched_interval_sec", ?SCHED_INTERVAL_SEC).

heal_threshold_sec() ->
    cfg_int_param("heal_threshold_sec", ?HEAL_THRESHOLD_SEC).

max_penalty_sec() ->
    cfg_int_param("max_penalty_sec", ?MAX_PENALTY_SEC).

cfg_int_param(Key, Default) when is_list(Key), is_integer(Default) ->
    config:get_integer("couch_scanner", Key, Default).
