%%% Copyright (C) 2009 Enrique Marcote, Miguel Rodriguez
%%% All rights reserved.
%%%
%%% Redistribution and use in source and binary forms, with or without
%%% modification, are permitted provided that the following conditions are met:
%%%
%%% o Redistributions of source code must retain the above copyright notice,
%%%   this list of conditions and the following disclaimer.
%%%
%%% o Redistributions in binary form must reproduce the above copyright notice,
%%%   this list of conditions and the following disclaimer in the documentation
%%%   and/or other materials provided with the distribution.
%%%
%%% o Neither the name of ERLANG TRAINING AND CONSULTING nor the names of its
%%%   contributors may be used to endorse or promote products derived from this
%%%   software without specific prior written permission.
%%%
%%% THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
%%% AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
%%% IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
%%% ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
%%% LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
%%% CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
%%% SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
%%% INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
%%% CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
%%% ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
%%% POSSIBILITY OF SUCH DAMAGE.
-module(gen_mc_session).
-behaviour(gen_statem).

%%% INCLUDE FILES
-include_lib("oserl/include/oserl.hrl").

%%% BEHAVIOUR EXPORTS
-export([behaviour_info/1]).

%%% START/STOP EXPORTS
-export([start_link/2, stop/1, stop/2]).

%%% SMPP EXPORTS
-export([reply/2,
         alert_notification/2,
         outbind/2,
         deliver_sm/2,
         data_sm/2,
         unbind/1]).

%%% INIT/TERMINATE EXPORTS
-export([init/1, terminate/3]).

%%% GEN_STATEM STATE EXPORTS
-export([bound_rx/3,
         bound_tx/3,
         bound_trx/3,
         listen/3,
         open/3,
         outbound/3,
         unbound/3]).

%%% CALLBACK MODE EXPORTS
-export([callback_mode/0]).

%%% CODE UPDATE EXPORTS
-export([code_change/4]).

%%% MACROS
-define(BOUND(B),
        if
            B == ?COMMAND_ID_BIND_RECEIVER ->
                bound_rx;
            B == ?COMMAND_ID_BIND_TRANSMITTER ->
                bound_tx;
            B == ?COMMAND_ID_BIND_TRANSCEIVER ->
                bound_trx
        end).

%%% RECORDS
-record(st,
        {mc,
         mod,
         log,
         sequence_number = 0,
         sock,
         sock_ctrl,
         req_tab,
         op_tab,
         congestion_state = 0,
         timers,
         session_init_timer,
         enquire_link_timer,
         inactivity_timer,
         enquire_link_resp_timer,
         proxy_ip_list = []}).

%%%-----------------------------------------------------------------------------
%%% BEHAVIOUR EXPORTS
%%%-----------------------------------------------------------------------------
behaviour_info(callbacks) ->
    [{handle_accept, 2},
     {handle_bind, 2},
     {handle_closed, 2},
     {handle_enquire_link, 2},
     {handle_operation, 2},
     {handle_unbind, 2}];
behaviour_info(_Other) ->
    undefined.

%%%-----------------------------------------------------------------------------
%%% START/STOP EXPORTS
%%%-----------------------------------------------------------------------------
start_link(Mod, Opts) ->
    Mc = proplists:get_value(mc, Opts, self()),
    case proplists:get_value(lsock, Opts) of
        undefined -> start_connect(Mod, Mc, Opts);
        _LSock    -> start_listen(Mod, Mc, Opts)
    end.

stop(ServerRef) ->
    stop(ServerRef, normal).

stop(ServerRef, Reason) ->
    gen_statem:stop(ServerRef, Reason, ?ASSERT_TIME).


%%%-----------------------------------------------------------------------------
%%% SMPP EXPORTS
%%%-----------------------------------------------------------------------------
reply(ServerRef, {SeqNum, Reply}) ->
    Event = {reply, {SeqNum, Reply}},
    gen_statem:call(ServerRef, Event, ?ASSERT_TIME).

alert_notification(ServerRef, Params) ->
    Event = {?COMMAND_ID_ALERT_NOTIFICATION, Params},
    gen_statem:call(ServerRef, Event, ?ASSERT_TIME).

send_event(ServerRef, CmdId, Params) ->
    Ref = make_ref(),
    Event = {outpdu, CmdId, Params, Ref},
    gen_statem:cast(ServerRef, Event),
    Ref.

data_sm(ServerRef, Params) ->
    send_event(ServerRef, ?COMMAND_ID_DATA_SM, Params).


deliver_sm(ServerRef, Params) ->
    send_event(ServerRef, ?COMMAND_ID_DELIVER_SM, Params).


unbind(ServerRef) ->
    send_event(ServerRef, ?COMMAND_ID_UNBIND, []).


outbind(ServerRef, Params) ->
    Event = {?COMMAND_ID_OUTBIND, Params},
    gen_statem:call(ServerRef, Event).

%%%-----------------------------------------------------------------------------
%%% INIT/TERMINATE EXPORTS
%%%-----------------------------------------------------------------------------
init([Mod, Mc, Opts]) ->
    _Ref = erlang:monitor(process, Mc),
    Timers = proplists:get_value(timers, Opts, ?DEFAULT_TIMERS_SMPP),
    ProxyIpList= proplists:get_value(proxy_ip_list, Opts, false),
    Log = proplists:get_value(log, Opts),
    case proplists:get_value(lsock, Opts) of
        undefined ->
            init_open(Mod, Mc, proplists:get_value(sock, Opts), Timers, Log);
        LSock ->
            init_listen(Mod, Mc, LSock, Timers, Log, ProxyIpList)
    end.


init_open(Mod, Mc, Sock, Tmr, Log) ->
    Self = self(),
    Pid = spawn_link(smpp_session, wait_recv, [Self, Sock, Log]),
    {ok, open, #st{mc = Mc,
                   mod = Mod,
                   log = Log,
                   sock = Sock,
                   sock_ctrl = Pid,
                   req_tab = smpp_req_tab:new(),
                   op_tab = smpp_req_tab:new(),
                   timers = Tmr,
                   session_init_timer =
                       start_timer(Tmr, session_init_timer),
                   enquire_link_timer =
                       start_timer(Tmr, enquire_link_timer)}}.


init_listen(Mod, Mc, LSock, Tmr, Log, ProxyIpList) ->
    Self = self(),
    Pid = spawn_link(smpp_session, wait_accept, [Self, LSock, Log, ProxyIpList]),
    {ok, listen, #st{mc = Mc,
                     mod = Mod,
                     log = Log,
                     sock_ctrl = Pid,
                     req_tab = smpp_req_tab:new(),
                     op_tab = smpp_req_tab:new(),
                     timers = Tmr,
                     proxy_ip_list = ProxyIpList}}.


terminate(_Reason, _Stn, Std) ->
    unlink(Std#st.sock_ctrl),
    exit(Std#st.sock_ctrl, kill),
    if Std#st.sock == undefined -> ok; true -> gen_tcp:close(Std#st.sock) end.

%%%-----------------------------------------------------------------------------
%%% ASYNC REQUEST EXPORTS
%%%-----------------------------------------------------------------------------
bound_rx(cast, {?COMMAND_ID_UNBIND, _Pdu} = R, St) ->
    case handle_peer_unbind(R, St) of
        true ->
            cancel_timer(St#st.inactivity_timer),
            {next_state, unbound, St};
        false ->
            {next_state, bound_rx, St}
    end;
bound_rx(cast, {CmdId, _Pdu} = R, St)
  when CmdId == ?COMMAND_ID_BIND_RECEIVER;
       CmdId == ?COMMAND_ID_BIND_TRANSMITTER;
       CmdId == ?COMMAND_ID_BIND_TRANSCEIVER ->
    esme_ralybnd_resp(R, St#st.sock, St#st.log),
    {next_state, bound_rx, St};
bound_rx(EventType, EventContent, Data) ->
  handle_event(EventType, EventContent, bound_rx, Data).


bound_tx(cast, {CmdId, _Pdu} = R, St)
  when CmdId == ?COMMAND_ID_DATA_SM;
       CmdId == ?COMMAND_ID_SUBMIT_SM;
       CmdId == ?COMMAND_ID_SUBMIT_MULTI;
       CmdId == ?COMMAND_ID_REPLACE_SM;
       CmdId == ?COMMAND_ID_BROADCAST_SM;
       CmdId == ?COMMAND_ID_QUERY_SM;
       CmdId == ?COMMAND_ID_QUERY_BROADCAST_SM;
       CmdId == ?COMMAND_ID_CANCEL_BROADCAST_SM;
       CmdId == ?COMMAND_ID_CANCEL_SM ->
    handle_peer_operation(R, St),
    {next_state, bound_tx, St};
bound_tx(cast, {?COMMAND_ID_UNBIND, _Pdu} = R, St) ->
    case handle_peer_unbind(R, St) of
        true ->
            cancel_timer(St#st.inactivity_timer),
            {next_state, unbound, St};
        false ->
            {next_state, bound_tx, St}
    end;
bound_tx(cast, {CmdId, _Pdu} = R, St)
  when CmdId == ?COMMAND_ID_BIND_RECEIVER;
       CmdId == ?COMMAND_ID_BIND_TRANSMITTER;
       CmdId == ?COMMAND_ID_BIND_TRANSCEIVER ->
    esme_ralybnd_resp(R, St#st.sock, St#st.log),
    {next_state, bound_tx, St};
bound_tx(EventType, EventContent, Data) ->
  handle_event(EventType, EventContent, bound_tx, Data).


bound_trx(cast, {CmdId, _Pdu} = R, St)
  when CmdId == ?COMMAND_ID_DATA_SM;
       CmdId == ?COMMAND_ID_SUBMIT_SM;
       CmdId == ?COMMAND_ID_SUBMIT_MULTI;
       CmdId == ?COMMAND_ID_REPLACE_SM;
       CmdId == ?COMMAND_ID_BROADCAST_SM;
       CmdId == ?COMMAND_ID_QUERY_SM;
       CmdId == ?COMMAND_ID_QUERY_BROADCAST_SM;
       CmdId == ?COMMAND_ID_CANCEL_BROADCAST_SM;
       CmdId == ?COMMAND_ID_CANCEL_SM ->
    handle_peer_operation(R, St),
    {next_state, bound_trx, St};
bound_trx(cast, {?COMMAND_ID_UNBIND, _Pdu} = R, St) ->
    case handle_peer_unbind(R, St) of
        true ->
            cancel_timer(St#st.inactivity_timer),
            {next_state, unbound, St};
        false ->
            {next_state, bound_trx, St}
    end;
bound_trx(cast, {CmdId, _Pdu} = R, St)
  when CmdId == ?COMMAND_ID_BIND_RECEIVER;
       CmdId == ?COMMAND_ID_BIND_TRANSMITTER;
       CmdId == ?COMMAND_ID_BIND_TRANSCEIVER ->
    esme_ralybnd_resp(R, St#st.sock, St#st.log),
    {next_state, bound_trx, St};
bound_trx(EventType, EventContent, Data) ->
  handle_event(EventType, EventContent, bound_trx, Data).


listen({call, From}, {accept, Sock, Addr}, St) ->
    logger:debug("accepting connection, calling handle_accept of gen_mc ~p", [St#st.mc]),
    case (St#st.mod):handle_accept(St#st.mc, Addr) of
        ok ->
            cancel_timer(St#st.session_init_timer),
            TI = start_timer(St#st.timers, session_init_timer),

            cancel_timer(St#st.enquire_link_timer),
            TE = start_timer(St#st.timers, enquire_link_timer),

            UpdSt = St#st{sock = Sock, session_init_timer = TI, enquire_link_timer = TE},
            {next_state, open, UpdSt, [{reply, From, true}]};
        {error, _Reason} ->
            {next_state, listen, St, [{reply, From, false}]}
    end;
listen(EventType, EventContent, Data) ->
  handle_event(EventType, EventContent, listen, Data).


open(cast, activate, St) ->
    ok = gen_tcp:controlling_process(St#st.sock, St#st.sock_ctrl),
    St#st.sock_ctrl ! activate,
    {next_state, open, St};
open(cast, {CmdId, _Pdu} = R, St)
  when CmdId == ?COMMAND_ID_BIND_RECEIVER;
       CmdId == ?COMMAND_ID_BIND_TRANSMITTER;
       CmdId == ?COMMAND_ID_BIND_TRANSCEIVER ->
    case handle_peer_bind(R, St) of
        true ->
            cancel_timer(St#st.session_init_timer),
            cancel_timer(St#st.inactivity_timer),
            Timer = start_timer(St#st.timers, inactivity_timer),
            {next_state, ?BOUND(CmdId), St#st{inactivity_timer = Timer}};
        false ->
            {next_state, open, St}
    end;
open(EventType, EventContent, Data) ->
  handle_event(EventType, EventContent, open, Data).


outbound(cast, {CmdId, _Pdu} = R, St)
  when CmdId == ?COMMAND_ID_BIND_RECEIVER;
       CmdId == ?COMMAND_ID_BIND_TRANSMITTER;
       CmdId == ?COMMAND_ID_BIND_TRANSCEIVER ->
    case handle_peer_bind(R, St) of
        true ->
            cancel_timer(St#st.session_init_timer),
            cancel_timer(St#st.inactivity_timer),
            Timer = start_timer(St#st.timers, inactivity_timer),
            {next_state, ?BOUND(CmdId), St#st{inactivity_timer = Timer}};
        false ->
            {next_state, open, St}
    end;
outbound(EventType, EventContent, Data) ->
  handle_event(EventType, EventContent, outbound, Data).


unbound(EventType, EventContent, Data) ->
  handle_event(EventType, EventContent, unbound, Data).

%% Auxiliary function for Event/2 functions.
%%
%% Sends the corresponding response with a ``?ESME_RALYBND`` status.
esme_ralybnd_resp({CmdId, Pdu}, Sock, Log) ->
    SeqNum = smpp_operation:get_value(sequence_number, Pdu),
    send_response(?RESPONSE(CmdId), ?ESME_RALYBND, SeqNum, [], Sock, Log).

%% Auxiliary function for Event/2 functions.
%%
%% Sends the corresponding response with a ``?ESME_RINVBNDSTS`` status.
esme_rinvbndsts_resp({CmdId, Pdu}, Sock, Log) ->
    SeqNum = smpp_operation:get_value(sequence_number, Pdu),
    case ?VALID_COMMAND_ID(CmdId) of
        true ->
            RespId = ?RESPONSE(CmdId),
            send_response(RespId, ?ESME_RINVBNDSTS, SeqNum, [], Sock, Log);
        false ->
            RespId = ?COMMAND_ID_GENERIC_NACK,
            send_response(RespId, ?ESME_RINVCMDID, SeqNum, [], Sock, Log)
    end.

%%%-----------------------------------------------------------------------------
%%% HANDLE EXPORTS
%%%-----------------------------------------------------------------------------
handle_event(cast, {input, CmdId, _Pdu, _Lapse, _Timestamp}, Stn, Std)
  when CmdId == ?COMMAND_ID_ENQUIRE_LINK_RESP ->
    cancel_timer(Std#st.enquire_link_resp_timer),
    {next_state, Stn, Std};
handle_event(cast, {input, CmdId, Pdu, _Lapse, _Timestamp}, Stn, Std)
  when CmdId == ?COMMAND_ID_GENERIC_NACK ->
    cancel_timer(Std#st.enquire_link_resp_timer),  % In case it was set
    SeqNum = smpp_operation:get_value(sequence_number, Pdu),
    case smpp_req_tab:read(Std#st.req_tab, SeqNum) of
        {ok, {SeqNum, _ReqId, RTimer, Ref}} ->
            cancel_timer(RTimer),
            case smpp_operation:get_value(command_status, Pdu) of
                ?ESME_ROK ->    % Some MCs return ESME_ROK in generic_nack
                    handle_peer_resp({error, ?ESME_RINVCMDID}, Ref, Std);
                Status ->
                    handle_peer_resp({error, Status}, Ref, Std)
            end;
        {error, not_found} ->
            % Do not send anything, might enter a request/response loop
            true
    end,
    {next_state, Stn, Std};
handle_event(cast, {input, CmdId, Pdu, _Lapse, _Timestamp}, Stn, Std)
  when ?IS_RESPONSE(CmdId) ->
    cancel_timer(Std#st.enquire_link_resp_timer),  % In case it was set
    SeqNum = smpp_operation:get_value(sequence_number, Pdu),
    logger:debug("handle response in gen_mc_session ~p, sequence_number: ~p", [self(), SeqNum]),
    ReqId = ?REQUEST(CmdId),
    case smpp_req_tab:read(Std#st.req_tab, SeqNum) of
        {ok, {SeqNum, ReqId, RTimer, Ref}} ->
            cancel_timer(RTimer),
            case smpp_operation:get_value(command_status, Pdu) of
                ?ESME_ROK when CmdId == ?COMMAND_ID_UNBIND_RESP ->
                    cancel_timer(Std#st.inactivity_timer),
                    handle_peer_resp({ok, Pdu}, Ref, Std),
                    {next_state, unbound, Std};
                ?ESME_ROK ->
                    handle_peer_resp({ok, Pdu}, Ref, Std),
                    {next_state, Stn, Std};
                Status ->
                    handle_peer_resp({error, Status}, Ref, Std),
                    {next_state, Stn, Std}
            end;
        {error, not_found} ->
            Sock = Std#st.sock,
            Log = Std#st.log,
            Nack = ?COMMAND_ID_GENERIC_NACK,
            send_response(Nack, ?ESME_RINVCMDID, SeqNum, [], Sock, Log),
            {next_state, Stn, Std}
    end;
handle_event(cast, {input, CmdId, Pdu, _Lapse, _Timestamp}, Stn, Std)
  when CmdId == ?COMMAND_ID_ENQUIRE_LINK ->
    cancel_timer(Std#st.enquire_link_resp_timer),  % In case it was set

    ok = (Std#st.mod):handle_enquire_link(Std#st.mc, Pdu),
    SeqNum = smpp_operation:get_value(sequence_number, Pdu),
    RespId = ?COMMAND_ID_ENQUIRE_LINK_RESP,
    send_response(RespId, ?ESME_ROK, SeqNum, [], Std#st.sock, Std#st.log),

    cancel_timer(Std#st.enquire_link_timer),
    T = start_timer(Std#st.timers, enquire_link_timer),

    {next_state, Stn, Std#st{enquire_link_timer = T}};
handle_event(cast, {input, CmdId, Pdu, Lapse, Timestamp}, Stn, Std) ->
    cancel_timer(Std#st.enquire_link_resp_timer),  % In case it was set
    gen_statem:cast(self(), {CmdId, Pdu}),

    cancel_timer(Std#st.enquire_link_timer),
    TE = start_timer(Std#st.timers, enquire_link_timer),

    cancel_timer(Std#st.inactivity_timer),
    TI = start_timer(Std#st.timers, inactivity_timer),

    C = smpp_session:congestion(Std#st.congestion_state, Lapse, Timestamp),
    {next_state, Stn, Std#st{congestion_state = C,
                             enquire_link_timer = TE,
                             inactivity_timer = TI}};
handle_event(cast, {error, CmdId, Status, _SeqNum}, _Stn, Std)
  when ?IS_RESPONSE(CmdId) ->
    {stop, {error, Status}, Std};
handle_event(cast, {error, CmdId, Status, SeqNum}, Stn, Std) ->
    RespId = case ?VALID_COMMAND_ID(CmdId) of
                 true when CmdId /= ?COMMAND_ID_GENERIC_NACK ->
                     ?RESPONSE(CmdId);
                 _Otherwise ->
                     ?COMMAND_ID_GENERIC_NACK
             end,
    send_response(RespId, Status, SeqNum,[], Std#st.sock, Std#st.log),
    {next_state, Stn, Std};
handle_event(cast, ?COMMAND_ID_ENQUIRE_LINK, Stn, Std) ->
    NewStd = send_enquire_link(Std),
    {next_state, Stn, NewStd};
handle_event(cast, {sock_error, _Reason}, unbound, Std) ->
    gen_tcp:close(Std#st.sock),
    {stop, normal, Std#st{sock = undefined}};
handle_event(cast, {sock_error, Reason}, _Stn, Std) ->
    gen_tcp:close(Std#st.sock),
    (Std#st.mod):handle_closed(Std#st.mc, Reason),
    {stop, normal, Std#st{sock = undefined}};
handle_event(cast, {listen_error, Reason}, _Stn, Std) ->
    {stop, Reason, Std};
handle_event(cast, {outpdu, CmdId, Params, Ref}, Stn, Std) ->
    NewStd = send_request(CmdId, Params, Ref, Std),
    {next_state, Stn, NewStd};
handle_event(cast, R, Stn, Std) ->
  esme_rinvbndsts_resp(R, Std#st.sock, Std#st.log),
  {next_state, Stn, Std};
handle_event({call, From}, {reply, {SeqNum, Reply}}, Stn, Std) ->
  {ok, {SeqNum, CmdId}} = smpp_req_tab:read(Std#st.op_tab, SeqNum),
  RespId = ?RESPONSE(CmdId),
  Sock = Std#st.sock,
  Log = Std#st.log,
  case Reply of
    {ok, Params} ->
      send_response(RespId, ?ESME_ROK, SeqNum, Params, Sock, Log);
    {error, Error} ->
      send_response(RespId, Error, SeqNum, [], Sock, Log)
  end,
  {next_state, Stn, Std, [{reply, From, ok}]};
handle_event({call, From}, {?COMMAND_ID_OUTBIND, Params}, open, Std) ->
  NewStd = send_request(?COMMAND_ID_OUTBIND, Params, From, Std),
  {next_state, outbound, NewStd, [{reply, From, ok}]};
handle_event({call, From}, {CmdId, Params}, Stn, Std) ->
  NewStd = send_request(CmdId, Params, From, Std),
  {next_state, Stn, NewStd, [{reply, From, ok}]};
handle_event(info, {timeout, _Ref, Timer}, Stn, Std) ->
  case handle_timeout(Timer, Std) of
    ok ->
      {next_state, Stn, Std};
    {error, Reason} ->
      {stop, Reason, Std}
  end;
handle_event(info, {'DOWN', _Ref, _Type, _Mc, Reason}, _Stn, Std) ->
    {stop, Reason, Std};
handle_event(info, {inet_reply, _, ok}, Stn, Std) ->
    {next_state, Stn, Std};
handle_event(info, {inet_reply, _, Reason}, Stn, Std) ->
    gen_statem:cast(self(), {sock_error, Reason}),
    {next_state, Stn, Std};
handle_event(info, _Info, Stn, Std) ->
    {next_state, Stn, Std}.

%%%-----------------------------------------------------------------------------
%%% CALLBACK MODE EXPORTS
%%%-----------------------------------------------------------------------------
callback_mode() ->
  state_functions.

%%%-----------------------------------------------------------------------------
%%% CODE UPDATE EXPORTS
%%%-----------------------------------------------------------------------------
code_change(_OldVsn, Stn, Std, _Extra) ->
    {ok, Stn, Std}.

%%%-----------------------------------------------------------------------------
%%% START FUNCTIONS
%%%-----------------------------------------------------------------------------
start_connect(Mod, Mc, Opts) ->
    case smpp_session:connect(Opts) of
        {ok, Sock} ->
            Args = [Mod, Mc, [{sock, Sock} | Opts]],
            case gen_statem:start_link(?MODULE, Args, []) of
                {ok, Pid} ->
                    case gen_tcp:controlling_process(Sock, Pid) of
                        ok ->
                            gen_statem:cast(Pid, activate),
                            {ok, Pid};
                        CtrlError ->
                            gen_tcp:close(Sock),
                            CtrlError
                        end;
                SessionError ->
                    gen_tcp:close(Sock),
                    SessionError
                end;
            ConnError ->
                ConnError
    end.

start_listen(Mod, Mc, Opts) ->
  gen_statem:start_link(?MODULE, [Mod, Mc, Opts], []).

%%%-----------------------------------------------------------------------------
%%% HANDLE PEER FUNCTIONS
%%%-----------------------------------------------------------------------------
handle_peer_bind({CmdId, Pdu}, St) ->
    CmdName = ?COMMAND_NAME(CmdId),
    SeqNum = smpp_operation:get_value(sequence_number, Pdu),
    RespId = ?RESPONSE(CmdId),
    Sock = St#st.sock,
    Log = St#st.log,
    case (St#st.mod):handle_bind(St#st.mc, {CmdName, Pdu}) of
        {ok, Params} ->
            send_response(RespId, ?ESME_ROK, SeqNum, Params, Sock, Log),
            true;
        {error, Error} ->
            send_response(RespId, Error, SeqNum, [], Sock, Log),
            false
    end.


handle_peer_operation({CmdId, Pdu}, St) ->
    logger:debug("handle_peer_operation by gen_mc_session ~p, sequence_number: ~p", [self(), smpp_operation:get_value(sequence_number,Pdu)]),
    CmdName = ?COMMAND_NAME(CmdId),
    SeqNum = smpp_operation:get_value(sequence_number, Pdu),
    RespId = ?RESPONSE(CmdId),
    Sock = St#st.sock,
    Log = St#st.log,
    case (St#st.mod):handle_operation(St#st.mc, {CmdName, Pdu}) of
        noreply ->
            ok = smpp_req_tab:write(St#st.op_tab, {SeqNum, CmdId}),
            true;
        {ok, Params} ->
            send_response(RespId, ?ESME_ROK, SeqNum, Params, Sock, Log),
            true;
        {error, Error} ->
            send_response(RespId, Error, SeqNum, [], Sock, Log),
            false
    end.


handle_peer_resp(Reply, Ref, St) ->
    (St#st.mod):handle_resp(St#st.mc, Reply, Ref).


handle_peer_unbind({?COMMAND_ID_UNBIND, Pdu}, St) ->
    SeqNum = smpp_operation:get_value(sequence_number, Pdu),
    RespId = ?COMMAND_ID_UNBIND_RESP,
    case (St#st.mod):handle_unbind(St#st.mc, Pdu) of
        ok ->
            send_response(RespId, ?ESME_ROK, SeqNum, [], St#st.sock, St#st.log),
            true;
        {error, Error} ->
            send_response(RespId, Error, SeqNum, [],  St#st.sock, St#st.log),
            false
    end.

%%%-----------------------------------------------------------------------------
%%% HANDLE TIMEOUT
%%%-----------------------------------------------------------------------------
handle_timeout({mc_response_timer, SeqNum}, St) ->
    case smpp_req_tab:read(St#st.req_tab, SeqNum) of
        {ok, {SeqNum, CmdId, _, Ref}} ->
            Status = smpp_operation:request_failure_code(CmdId),
            handle_peer_resp({error, Status}, Ref, St),
            ok;
        {error, not_found} ->
            ok
    end;
handle_timeout(enquire_link_timer, _St) ->
    ok = gen_statem:cast(self(), ?COMMAND_ID_ENQUIRE_LINK);
handle_timeout(enquire_link_failure, _St) ->
    {error, {timeout, enquire_link}};
handle_timeout(session_init_timer, _St) ->
    {error, {timeout, session_init_timer}};
handle_timeout(inactivity_timer, _St) ->
    {error, {timeout, inactivity_timer}}.

%%%-----------------------------------------------------------------------------
%%% SEND PDU FUNCTIONS
%%%-----------------------------------------------------------------------------
send_enquire_link(St) ->
    SeqNum = ?INCR_SEQUENCE_NUMBER(St#st.sequence_number),
    Pdu = smpp_operation:new(?COMMAND_ID_ENQUIRE_LINK, SeqNum, []),
    ok = smpp_session:send_pdu(St#st.sock, Pdu, St#st.log),

    cancel_timer(St#st.enquire_link_timer),
    ETimer = start_timer(St#st.timers, enquire_link_timer),

    cancel_timer(St#st.enquire_link_resp_timer),
    RTimer = start_timer(St#st.timers, enquire_link_failure),

    St#st{sequence_number = SeqNum,
          enquire_link_timer = ETimer,
          enquire_link_resp_timer = RTimer,
          congestion_state = 0}.


send_request(CmdId, Params, _From, St)
  when CmdId == ?COMMAND_ID_ALERT_NOTIFICATION;
       CmdId == ?COMMAND_ID_OUTBIND ->
    SeqNum = ?INCR_SEQUENCE_NUMBER(St#st.sequence_number),
    Pdu = smpp_operation:new(CmdId, SeqNum, Params),
    ok = smpp_session:send_pdu(St#st.sock, Pdu, St#st.log),
    cancel_timer(St#st.enquire_link_timer),
    cancel_timer(St#st.inactivity_timer),
    St#st{sequence_number = SeqNum,
          enquire_link_timer = start_timer(St#st.timers, enquire_link_timer),
          inactivity_timer = start_timer(St#st.timers, inactivity_timer)};


send_request(CmdId, Params, Ref, St) ->
    SeqNum = ?INCR_SEQUENCE_NUMBER(St#st.sequence_number),
    Pdu = smpp_operation:new(CmdId, SeqNum, Params),
    case smpp_operation:pack(Pdu) of
        {ok, BinPdu} ->
            ok = smpp_session:send_pdu(St#st.sock, BinPdu, St#st.log),
            RTimer = start_timer(St#st.timers, {mc_response_timer, SeqNum}),
            ok = smpp_req_tab:write(St#st.req_tab, {SeqNum, CmdId, RTimer, Ref}),
            cancel_timer(St#st.enquire_link_timer),
            cancel_timer(St#st.inactivity_timer),
            St#st{sequence_number = SeqNum,
                  enquire_link_timer = start_timer(St#st.timers, enquire_link_timer),
                  inactivity_timer = start_timer(St#st.timers, inactivity_timer)};
        {error, _CmdId, Status, _SeqNum} ->
            handle_peer_resp({error, {command_status, Status}}, Ref, St),
            St
    end.


send_response(CmdId, Status, SeqNum, Params, Sock, Log) ->
    Pdu = smpp_operation:new(CmdId, Status, SeqNum, Params),
    smpp_session:send_pdu(Sock, Pdu, Log).

start_timer(Timers, Ref) ->
    smpp_session:start_timer(Timers, Ref).

cancel_timer(Ref) ->
    smpp_session:cancel_timer(Ref).
