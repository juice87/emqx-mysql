-module(emqx_mysql).

-import(string,[equal/2]). 
-include("emqx_mysql.hrl").
-include_lib("emqx/include/emqx.hrl").

-define(SAVE_MESSAGE_PUBLISH, <<"INSERT INTO mqtt_msg(`mid`, `client_id`, `topic`, `payload`, `time`) VALUE(?, ?, ?, ?, ?);">>).

-export([load_hook/1, unload_hook/0, on_message_publish/2]).


load_hook(Env) ->
	emqx:hook('message.publish', fun ?MODULE:on_message_publish/2, [Env]).

unload_hook() ->
	emqx:unhook('message.publish', fun ?MODULE:on_message_publish/2).

on_message_publish(#message{from = emqx_sys} = Message, _State) ->
	{ok, Message};
on_message_publish(#message{flags = #{retain := true}} = Message, _State) ->
	#message{id = Id, topic = Topic, payload = Payload, from = From} = Message,
	ROOTMAP = jsx:decode(Payload, [return_maps]),
	Typestr = maps:get(<<"$type">>, ROOTMAP),
	if Typestr == <<"CycleData">> ->
		EventTime = maps:get(<<"timestamp">>, ROOTMAP),
		ControllerId = maps:get(<<"controllerId">>, ROOTMAP),
		OpMode = maps:get(<<"opMode">>, ROOTMAP),
		MapData = maps:get(<<"data">>, ROOTMAP),
		Z_QDVPPOS =  maps:get(<<"Z_QDVPPOS">>, MapData),
		Z_QDPRDCNT =  maps:get(<<"Z_QDPRDCNT">>, MapData),
		Z_QDCOLTIM =  maps:get(<<"Z_QDCOLTIM">>, MapData),
		Z_QDMAXPLSRPM =  maps:get(<<"Z_QDMAXPLSRPM">>, MapData),
		Z_QDCYCTIM =  maps:get(<<"Z_QDCYCTIM">>, MapData),
		Z_QDINJTIM =  maps:get(<<"Z_QDINJTIM">>, MapData),
		Z_QDMLDCLSTIM =  maps:get(<<"Z_QDMLDCLSTIM">>, MapData),
		Z_QDTEMPZ06 =  maps:get(<<"Z_QDTEMPZ06">>, MapData),
		Z_QDTEMPZ01 =  maps:get(<<"Z_QDTEMPZ01">>, MapData),
		Z_QDTEMPZ02 =  maps:get(<<"Z_QDTEMPZ02">>, MapData),
		Z_QDGODCNT =  maps:get(<<"Z_QDGODCNT">>, MapData),
		Z_QDHLDTIM =  maps:get(<<"Z_QDHLDTIM">>, MapData),
		Z_QDBCKPRS =  maps:get(<<"Z_QDBCKPRS">>, MapData),
		Z_QDMLDOPNENDPOS =  maps:get(<<"Z_QDMLDOPNENDPOS">>, MapData),
		Z_QDMAXINJSPD =  maps:get(<<"Z_QDMAXINJSPD">>, MapData),
		Z_QDINJENDPOS =  maps:get(<<"Z_QDINJENDPOS">>, MapData),
		Z_QDTEMPZ03 =  maps:get(<<"Z_QDTEMPZ03">>, MapData),
		Z_QDPLSENDPOS =  maps:get(<<"Z_QDPLSENDPOS">>, MapData),
		Z_QDMLDOPNTIM =  maps:get(<<"Z_QDMLDOPNTIM">>, MapData),
		Z_QDNOZTEMP =  maps:get(<<"Z_QDNOZTEMP">>, MapData),
		Z_QDFLAG =  maps:get(<<"Z_QDFLAG">>, MapData),
		Z_QDPLSTIM =  maps:get(<<"Z_QDPLSTIM">>, MapData),
		Z_QDTEMPZ05 =  maps:get(<<"Z_QDTEMPZ05">>, MapData),
emqx_mysql_cli:query(?SAVE_MESSAGE_PUBLISH, [emqx_guid:to_hexstr(Id), binary_to_list(From), binary_to_list(Topic), binary_to_list(Z_QDMLDCLSTIM), timestamp()]);
	false ->
		true
		end,
			
	%%emqx_mysql_cli:query(?SAVE_MESSAGE_PUBLISH, [emqx_guid:to_hexstr(Id), binary_to_list(From), binary_to_list(Topic), binary_to_list(Z_QDMLDCLSTIM), timestamp()]),
	%%emqx_mysql_cli:query(?SAVE_MESSAGE_PUBLISH, [emqx_guid:to_hexstr(Id), binary_to_list(From), binary_to_list(Topic), binary_to_list(Payload), timestamp()]),
	{ok, Message};
on_message_publish(Message, _State) ->
	{ok, Message}.

timestamp() ->
	{A,B,_C} = os:timestamp(),
	A*1000000+B.
