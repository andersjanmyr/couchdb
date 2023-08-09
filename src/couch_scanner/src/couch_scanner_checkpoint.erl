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

-module(couch_scanner_checkpoint).

-export([
    write/2,
    read/1,
    reset/1
]).

-include_lib("couch/include/couch_db.hrl").

write(<<Plugin/binary>>, #{} = State) ->
    with_db(fun(Db) -> update_doc(Db, doc_id(Plugin), State) end).

read(<<Plugin/binary>>) ->
    with_db(fun(Db) -> load_doc(Db, doc_id(Plugin)) end).

reset(<<Plugin/binary>>) ->
    with_db(fun(Db) -> delete_doc(Db, doc_id(Plugin)) end).

% Private functions

doc_id(Plugin) ->
    % Dashes are more conventional for doc ids
    Plugin1 = binary:replace(Plugin, <<"_">>, <<"-">>, [global]),
    <<?LOCAL_DOC_PREFIX, Plugin1/binary>>.

delete_doc(Db, DocId) ->
    case couch_db:open_doc(Db, DocId, []) of
        {ok, #doc{revs = {_, RevList}}} ->
            {ok, _} = couch_db:delete_doc(Db, DocId, RevList),
            ok;
        {not_found, _} ->
            ok
    end.

update_doc(Db, DocId, #{} = Body) ->
    EJsonBody = ?JSON_DECODE(?JSON_ENCODE(Body#{<<"_id">> => DocId})),
    Doc = couch_doc:from_json_obj(EJsonBody),
    case couch_db:open_doc(Db, DocId, []) of
        {ok, #doc{revs = Revs}} ->
            {ok, _} = couch_db:update_doc(Db, Doc#doc{revs = Revs}, []);
        {not_found, _} ->
            {ok, _} = couch_db:update_doc(Db, Doc, [])
    end,
    ok.

load_doc(Db, DocId) ->
    case couch_db:open_doc(Db, DocId, [ejson_body]) of
        {ok, #doc{body = EJsonBody}} ->
            ?JSON_DECODE(?JSON_ENCODE(EJsonBody), [return_maps]);
        {not_found, _} ->
            not_found
    end.

with_db(Fun) when is_function(Fun, 1) ->
    DbName = config:get("mem3", "shards_db", "_dbs"),
    case mem3_util:ensure_exists(DbName) of
        {ok, Db} ->
            try
                Fun(Db)
            after
                catch couch_db:close(Db)
            end;
        Else ->
            throw(Else)
    end.

-ifdef(TEST).

-include_lib("couch/include/couch_eunit.hrl").

couch_scanner_checkpoint_test_() ->
    {
        foreach,
        fun test_util:start_couch/0,
        fun test_util:stop_couch/1,
        [
            ?TDEF_FE(t_read_write_reset)
        ]
    }.

t_read_write_reset(_) ->
    Plugin = <<"scanner_plugin_abc">>,
    ok = reset(Plugin),
    ?assertEqual(ok, write(Plugin, #{<<"foo">> => 1})),
    ?assertEqual(#{<<"foo">> => 1}, read(Plugin)),
    ?assertEqual(ok, write(Plugin, #{<<"bar">> => 2})),
    ?assertEqual(#{<<"bar">> => 2}, read(Plugin)),
    ?assertEqual(not_found, read(<<"scanner_plugin_other">>)),
    ?assertEqual(ok, reset(Plugin)),
    ?assertEqual(not_found, read(Plugin)).

-endif.
