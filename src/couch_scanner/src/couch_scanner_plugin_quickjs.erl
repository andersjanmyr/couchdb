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

-module(couch_scanner_plugin_quickjs).

-export([
    start/1,
    resume/1,
    stop/1,
    checkpoint/1,
    db/2,
    ddoc/3,
    shard/2,
    doc_id/5,
    doc/4
]).

-include_lib("couch/include/couch_db.hrl").
-include_lib("mem3/include/mem3.hrl").

-define(QJS, <<"javascript_quickjs">>).
-define(SM, <<"javascript_spidermonkey">>).

-record(st, {
    scan_id
}).

-define(INFO(S, MSG), log(info, S, ?FUNCTION_NAME, "", MSG, [])).
-define(INFO(S, MSG, ARGS), log(info, S, ?FUNCTION_NAME, "", MSG, ARGS)).
-define(INFO(S, DB, MSG, ARGS), log(info, S, ?FUNCTION_NAME, DB, MSG, ARGS)).
-define(WARN(S, DB, MSG, ARGS), log(warning, S, ?FUNCTION_NAME, DB, MSG, ARGS)).

start(#{} = JsonSt) ->
    St = #st{scan_id = scan_id()},
    case JsonSt of
        #{<<"scan_id">> := OldScanId} -> ?INFO(St, "start, old id:~p", [OldScanId]);
        #{} -> ?INFO(St, "start new")
    end,
    % {reschedule, TSec}
    {ok, St}.

resume(#{} = JsonSt) ->
    #{<<"scan_id">> := ScanId} = JsonSt,
    St = #st{scan_id = ScanId},
    ?INFO(St, "resume id:~p", [ScanId]),
    % {reschedule, TSec}
    {ok, St}.

stop(#st{} = St) ->
    ?INFO(St, "stop"),
    % {{reschedule, TSec}, #{}}
    {ok, #{<<"scan_id">> => St#st.scan_id}}.

checkpoint(#st{} = St) ->
    ?INFO(St, "checkpoint"),
    {ok, #{<<"scan_id">> => St#st.scan_id}}.

db(#st{} = St, DbName) ->
    ?INFO(St, DbName, "", []),
    {ok, St}.

ddoc(#st{} = St, DbName, #doc{id = Id} = DDoc) ->
    ?INFO(St, DbName, "ddocid:~s", [Id]),
    #doc{id = Id, body = {Props = [_ | _]}} = DDoc,
    case couch_util:get_value(<<"language">>, Props, <<"javascript">>) of
        <<"javascript">> ->
            validate_ddoc_compare(St, DbName, DDoc);
        _ ->
            {skip, St}
    end.

shard(#st{} = St, #shard{} = Shard) ->
    ?INFO(St, Shard, "", []),
    % {skip, St}
    {ok, St}.

doc_id(#st{} = St, DocId, DocIndex, DocTotal, Db) ->
    ?INFO(St, Db, "did:~s ~p/~p", [DocId, DocIndex, DocTotal]),
    % {skip, St} | {stop, St}
    {ok, St}.

doc(#st{} = St, Db, DDocs, #doc{id = Id} = Doc) ->
    ?INFO(St, Db, "did:~s ddoc_count:~p", [Id, length(DDocs)]),
    % {stop, St}
    {ok, St}.

% Private

validate_ddoc_compare(#st{} = St, DbName, #doc{id = Id} = DDoc) ->
    {ok, Db} = couch_db:clustered_db(DbName, []),
    Qjs = validate_ddoc(Db, set_lang(?QJS, DDoc)),
    Sm = validate_ddoc(Db, set_lang(?SM, DDoc)),
    case Qjs == Sm of
        true ->
            {ok, St};
        false ->
            ?WARN(St, DbName, "ddid:~s validate failed ~p ~p", [Id, Qjs, Sm]),
            % No point in continuing with this ddoc
            {skip, St}
    end.

validate_ddoc(Db, #doc{} = DDoc) ->
    try
        ok = couch_index_server:validate(Db, couch_doc:with_ejson_body(DDoc))
    catch
        Tag:Error ->
            {Tag, Error}
    end.

set_lang(Lang, #doc{body = {Props}} = Doc) ->
    Props1 = lists:keystore(<<"language">>, 1, Props, {<<"language">>, Lang}),
    Doc#doc{body = {Props1}}.

log(Level, #st{scan_id = SId}, FName, Db, Msg, Args) ->
    DbName =
        case Db of
            Db when is_list(Db) -> <<>>;
            <<_/binary>> -> Db;
            #shard{name = ShardDbName} -> ShardDbName;
            _ -> couch_db:name(Db)
        end,
    Msg1 = "scanner_quickjs s:~s f:~s db:~s " ++ Msg,
    Args1 = [SId, FName, DbName] ++ Args,
    couch_log:Level(Msg1, Args1).

scan_id() ->
    TSec = integer_to_binary(erlang:system_time(second)),
    Rand = string:lowercase(binary:encode_hex(crypto:strong_rand_bytes(6))),
    <<TSec/binary, "-", Rand/binary>>.
