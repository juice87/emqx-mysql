-module(emqx_mysql).

-import(string,[equal/2]). 
-include("emqx_mysql.hrl").
-include_lib("emqx/include/emqx.hrl").

%%-define(SAVE_MESSAGE_PUBLISH, <<"INSERT INTO mqtt_msg(`mid`, `client_id`, `topic`, `payload`, `time`) VALUE(?, ?, ?, ?, ?);">>).

-define(SAVE_CYCLEDATA,<<"INSERT INTO cycledata(`client_id`, `receive_timestamp`, `controllerId`, `Z_QDVPPOS`, `Z_QDPRDCNT`, `Z_QDCOLTIM`, `Z_QDMAXPLSRPM`, `Z_QDCYCTIM`, `Z_QDINJTIM`, `Z_QDMLDCLSTIM`, `Z_QDTEMPZ01`, `Z_QDTEMPZ02`, `Z_QDTEMPZ03`, `Z_QDTEMPZ04`, `Z_QDTEMPZ05`, `Z_QDTEMPZ06`, `Z_QDNOZTEMP`, `Z_QDGODCNT`, `Z_QDHLDTIM`, `Z_QDBCKPRS`, `Z_QDMLDOPNENDPOS`, `Z_QDMAXINJSPD`, `Z_QDINJENDPOS`, `Z_QDPLSENDPOS`, `Z_QDMLDOPNTIM`, `Z_QDPLSTIM`, `Z_QDFLAG`, `EventTime`) VALUE (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);">>).

-define(SAVE_ALARM,<<"INSERT INTO alarm(`client_id`, `receive_timestamp`, `controllerId`,`alarmNum`,`value`,`EventTime`) value(?,?,?,?,?,?);">>).

-export([load_hook/1, unload_hook/0, on_message_publish/2]).

load_hook(Env) ->
	emqx:hook('message.publish', fun ?MODULE:on_message_publish/2, [Env]).

unload_hook() ->
	emqx:unhook('message.publish', fun ?MODULE:on_message_publish/2).

on_message_publish(#message{from = emqx_sys} = Message, _State) ->
	{ok, Message};
%%on_message_publish(#message{flags = #{retain := true}} = Message, _State) ->
on_message_publish(#message{topic = <<"/cycledata">>} = Message, _State) ->
	#message{id = Id, topic = Topic, payload = Payload, from = From} = Message,
	ROOTMAP = jsx:decode(Payload, [return_maps]),
	Typestr = maps:get(<<"$type">>, ROOTMAP),
	EventTime = maps:get(<<"timestamp">>, ROOTMAP),
	ControllerId = maps:get(<<"controllerId">>, ROOTMAP),
	OpMode = maps:get(<<"opMode">>, ROOTMAP),
	MapData = maps:get(<<"data">>, ROOTMAP),
	Z_QDVPPOS =  maps:get(<<"Z_QDVPPOS">>, MapData, 0),
	Z_QDPRDCNT =  maps:get(<<"Z_QDPRDCNT">>, MapData, 0),
	Z_QDCOLTIM =  maps:get(<<"Z_QDCOLTIM">>, MapData, 0),
	Z_QDMAXPLSRPM =  maps:get(<<"Z_QDMAXPLSRPM">>, MapData, 0),
	Z_QDCYCTIM =  maps:get(<<"Z_QDCYCTIM">>, MapData, 0),
	Z_QDINJTIM =  maps:get(<<"Z_QDINJTIM">>, MapData, 0),
	Z_QDMLDCLSTIM =  maps:get(<<"Z_QDMLDCLSTIM">>, MapData, 0),
	Z_QDTEMPZ06 =  maps:get(<<"Z_QDTEMPZ06">>, MapData, 0),
	Z_QDTEMPZ01 =  maps:get(<<"Z_QDTEMPZ01">>, MapData, 0),
	Z_QDTEMPZ02 =  maps:get(<<"Z_QDTEMPZ02">>, MapData, 0),
	Z_QDTEMPZ04 =  maps:get(<<"Z_QDTEMPZ04">>, MapData, 0),
	Z_QDGODCNT =  maps:get(<<"Z_QDGODCNT">>, MapData, 0),
	Z_QDHLDTIM =  maps:get(<<"Z_QDHLDTIM">>, MapData, 0),
	Z_QDBCKPRS =  maps:get(<<"Z_QDBCKPRS">>, MapData, 0),
	Z_QDMLDOPNENDPOS =  maps:get(<<"Z_QDMLDOPNENDPOS">>, MapData, 0),
	Z_QDMAXINJSPD =  maps:get(<<"Z_QDMAXINJSPD">>, MapData, 0),
	Z_QDINJENDPOS =  maps:get(<<"Z_QDINJENDPOS">>, MapData, 0),
	Z_QDTEMPZ03 =  maps:get(<<"Z_QDTEMPZ03">>, MapData, 0),
	Z_QDPLSENDPOS =  maps:get(<<"Z_QDPLSENDPOS">>, MapData, 0),
	Z_QDMLDOPNTIM =  maps:get(<<"Z_QDMLDOPNTIM">>, MapData, 0),
	Z_QDNOZTEMP =  maps:get(<<"Z_QDNOZTEMP">>, MapData, 0),
	Z_QDFLAG =  maps:get(<<"Z_QDFLAG">>, MapData, 0),
	Z_QDPLSTIM =  maps:get(<<"Z_QDPLSTIM">>, MapData, 0),
	Z_QDTEMPZ05 =  maps:get(<<"Z_QDTEMPZ05">>, MapData, 0),	
	
	emqx_mysql_cli:query(?SAVE_CYCLEDATA,[binary_to_list(From), timestamp(), ControllerId, Z_QDVPPOS, Z_QDPRDCNT, Z_QDCOLTIM, Z_QDMAXPLSRPM, Z_QDCYCTIM, Z_QDINJTIM, Z_QDMLDCLSTIM, Z_QDTEMPZ01, Z_QDTEMPZ02, Z_QDTEMPZ03, Z_QDTEMPZ04, Z_QDTEMPZ05, Z_QDTEMPZ06, Z_QDNOZTEMP, Z_QDGODCNT, Z_QDHLDTIM, Z_QDBCKPRS, Z_QDMLDOPNENDPOS, Z_QDMAXINJSPD, Z_QDINJENDPOS, Z_QDPLSENDPOS, Z_QDMLDOPNTIM, Z_QDPLSTIM, Z_QDFLAG, EventTime]),
		
	%%emqx_mysql_cli:query(?SAVE_MESSAGE_PUBLISH, [emqx_guid:to_hexstr(Id), binary_to_list(From), binary_to_list(Topic), Z_QDMLDCLSTIM, timestamp()]),
	%%emqx_mysql_cli:query(?SAVE_MESSAGE_PUBLISH, [emqx_guid:to_hexstr(Id), binary_to_list(From), binary_to_list(Topic), binary_to_list(Payload), timestamp()]),
	{ok, Message};

on_message_publish(#message{topic = <<"/alarm">>} = Message, _State) ->
	#message{id = Id, topic = Topic, payload = Payload, from = From} = Message,
	ROOTMAP = jsx:decode(Payload, [return_maps]),
	EventTime = maps:get(<<"timestamp">>, ROOTMAP),
	ControllerId = maps:get(<<"controllerId">>, ROOTMAP),
	MapAlarm = maps:get(<<"alarm">>, ROOTMAP),
	AlarmNum =  maps:get(<<"key">>, MapAlarm, 0),
	AlarmValue = maps:get(<<"value">>, MapAlarm, 0),
	emqx_mysql_cli:query(?SAVE_ALARM,[binary_to_list(From), timestamp(), ControllerId, AlarmNum, atom_to_list(AlarmValue), EventTime]),
	{ok, Message};

on_message_publish(Message, _State) ->
	{ok, Message}.

timestamp() ->
	{A,B,_C} = os:timestamp(),
	A*1000000+B.
