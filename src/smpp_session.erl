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
-module(smpp_session).

%%% INCLUDE FILES
-include_lib("oserl/include/oserl.hrl").

%%% EXTERNAL EXPORTS
-export([congestion/3, connect/1, listen/1, tcp_send/2, send_pdu/3]).

%%% SOCKET LISTENER FUNCTIONS EXPORTS
-export([wait_accept/4, wait_accept/3, wait_recv/3, recv_loop/4]).

%% TIMER EXPORTS
-export([cancel_timer/1, start_timer/2]).

%%% MACROS
-define(CONNECT_OPTS(Ip),
        case Ip of
            undefined -> [binary, {packet, 0}, {active, false}];
            _         -> [binary, {packet, 0}, {active, false}, {ip, Ip}]
        end).
-define(CONNECT_TIME, 30000).
-define(LISTEN_OPTS(Ip),
        case Ip of
            undefined ->
              [binary, {packet, 0}, {active, false}, {reuseaddr, true}];
            _ ->
              [binary, {packet, 0}, {active, false}, {reuseaddr, true}, {ip, Ip}]
        end).

-define(MAX_COMMAND_LENGTH, 1000000). % ~1 megabyte

%%%-----------------------------------------------------------------------------
%%% EXTERNAL EXPORTS
%%%-----------------------------------------------------------------------------
%% Computes the congestion state.
%%
%% - CongestionSt: Current ``congestion_state`` value.
%% - WaitTime: Are the microseconds waiting for the PDU.
%% - Timestamp: Represents the moment when the PDU was received.
%%
%% The time since ``Timestamp`` is the PDU dispatching time.  If
%% this value equals the ``WaitTime`` (i.e. ``DispatchTime/WaitTime = 1``),
%% then we shall assume optimum load (value 85).  Having this in mind the
%% instant congestion state value is calculated.  Notice this value cannot be
%% greater than 99.
congestion(CongestionSt, WaitTime, Timestamp) ->
    case (timer:now_diff(time:timestamp(), Timestamp) div (WaitTime + 1)) * 85 of
        Val when Val < 1 ->
            0;
        Val when Val > 99 ->  % Out of bounds
            ((19 * CongestionSt) + 99) div 20;
        Val ->
            ((19 * CongestionSt) + Val) div 20
    end.


connect(Opts) ->
    Ip = proplists:get_value(ip, Opts),
    logger:debug("smpp_session connect opts: ~p", [Opts]),
    case proplists:get_value(sock, Opts, undefined) of
        undefined ->
            Addr = proplists:get_value(addr, Opts),
            Port = proplists:get_value(port, Opts, ?DEFAULT_SMPP_PORT),
            gen_tcp:connect(Addr, Port, ?CONNECT_OPTS(Ip), ?CONNECT_TIME);
        Sock ->
            case inet:setopts(Sock, ?CONNECT_OPTS(Ip)) of
                ok    -> {ok, Sock};
                Error -> Error
            end
    end.


listen(Opts) ->
    logger:debug("smpp_session listen opts: ~p", [Opts]),
    case proplists:get_value(lsock, Opts, undefined) of
        undefined ->
            Addr = proplists:get_value(addr, Opts, default_addr()),
            Port = proplists:get_value(port, Opts, ?DEFAULT_SMPP_PORT),
            gen_tcp:listen(Port, ?LISTEN_OPTS(Addr));
        LSock ->
            Addr = proplists:get_value(addr, Opts, default_addr()),
            case inet:setopts(LSock, ?LISTEN_OPTS(Addr)) of
                ok ->
                    {ok, LSock};
                Error ->
                    Error
            end
    end.

-if(?OTP_RELEASE >= 26). % https://github.com/erlang/otp/issues/7130
tcp_send(Sock, Data) ->
    gen_tcp:send(Sock, Data).
-else.
tcp_send(Sock, Data) when is_port(Sock) ->
    try erlang:port_command(Sock, Data) of
        true -> ok
    catch
        error:_Error -> {error, einval}
    end.
-endif.


send_pdu(Sock, BinPdu, Log) when is_list(BinPdu) ->
    case tcp_send(Sock, BinPdu) of
        ok ->
            ok = smpp_log_mgr:pdu(Log, BinPdu);
        {error, Reason} ->
            gen_statem:cast(self(), {sock_error, Reason})
    end;


send_pdu(Sock, Pdu, Log) ->
    case smpp_operation:pack(Pdu) of
        {ok, BinPdu} ->
            send_pdu(Sock, BinPdu, Log);
        {error, _CmdId, Status, _SeqNum} ->
            gen_tcp:close(Sock),
            exit({command_status, Status})
    end.

%%%-----------------------------------------------------------------------------
%%% SOCKET LISTENER FUNCTIONS
%%%-----------------------------------------------------------------------------
wait_accept(Pid, LSock, Log) ->
    wait_accept(Pid, LSock, Log, false).

wait_accept(Pid, LSock, Log, ProxyProtocol) ->
    case gen_tcp:accept(LSock) of
        {ok, Sock} ->
            case handle_accept(Pid, Sock, ProxyProtocol) of
                true ->
                    ?MODULE:recv_loop(Pid, Sock, <<>>, Log);
                false ->
                    gen_tcp:close(Sock),
                    ?MODULE:wait_accept(Pid, LSock, Log, ProxyProtocol)
            end;
        {error, Reason} ->
            gen_statem:cast(Pid, {listen_error, Reason})
    end.


wait_recv(Pid, Sock, Log) ->
    receive activate -> ?MODULE:recv_loop(Pid, Sock, <<>>, Log) end.


recv_loop(Pid, Sock, Buffer, Log) ->
    Timestamp = time:timestamp(),
    inet:setopts(Sock, [{active, once}]),
    receive
        {tcp, Sock, Input} ->
            L = timer:now_diff(time:timestamp(), Timestamp),
            B = handle_input(Pid, list_to_binary([Buffer, Input]), L, 1, Log),
            ?MODULE:recv_loop(Pid, Sock, B, Log);
        {tcp_closed, Sock} ->
            gen_statem:cast(Pid, {sock_error, closed});
        {tcp_error, Sock, Reason} ->
            gen_statem:cast(Pid, {sock_error, Reason})
    end.

%%%-----------------------------------------------------------------------------
%%% TIMER FUNCTIONS
%%%-----------------------------------------------------------------------------
cancel_timer(undefined) ->
    false;
cancel_timer(Ref) ->
    erlang:cancel_timer(Ref).


start_timer(#timers_smpp{mc_response_time = infinity}, {mc_response_timer, _}) ->
    undefined;
start_timer(#timers_smpp{esme_response_time = infinity}, {esme_response_timer, _}) ->
    undefined;
start_timer(#timers_smpp{enquire_link_failure_time = infinity}, enquire_link_failure) ->
    undefined;
start_timer(#timers_smpp{enquire_link_time = infinity}, enquire_link_timer) ->
    undefined;
start_timer(#timers_smpp{session_init_time = infinity}, session_init_timer) ->
    undefined;
start_timer(#timers_smpp{inactivity_time = infinity}, inactivity_timer) ->
    undefined;
start_timer(#timers_smpp{mc_response_time = Time}, {mc_response_timer, _} = Msg) ->
    erlang:start_timer(Time, self(), Msg);
start_timer(#timers_smpp{esme_response_time = Time}, {esme_response_timer, _} = Msg) ->
    erlang:start_timer(Time, self(), Msg);
start_timer(#timers_smpp{enquire_link_failure_time = Time}, enquire_link_failure) ->
    erlang:start_timer(Time, self(), enquire_link_failure);
start_timer(#timers_smpp{enquire_link_time = Time}, enquire_link_timer) ->
    erlang:start_timer(Time, self(), enquire_link_timer);
start_timer(#timers_smpp{session_init_time = Time}, session_init_timer) ->
    erlang:start_timer(Time, self(), session_init_timer);
start_timer(#timers_smpp{inactivity_time = Time}, inactivity_timer) ->
    erlang:start_timer(Time, self(), inactivity_timer).

%%%-----------------------------------------------------------------------------
%%% INTERNAL FUNCTIONS
%%%-----------------------------------------------------------------------------
default_addr() ->
    {ok, Host} = inet:gethostname(),
    {ok, Addr} = inet:getaddr(Host, inet),
    Addr.

handle_accept(Pid, Sock, ProxyIpList) ->
    case inet:peername(Sock) of
        {ok, {Addr, _Port}} ->
            logger:debug("sync_send_event accept to pid:~p Addr:~p, proxy_ip_list: ~p", [Pid, Addr, ProxyIpList]),
            case lists:member(Addr, ProxyIpList) of
                true -> case proxy_protocol:accept(Sock) of
                            {ok, {proxy_opts, _IpVersion, SourceAddress, DestAddress, SourcePort, DestPort, ConnectionInfo}} ->
                                gen_statem:call(Pid,
                                  {accept, Sock, {Addr, [
                                    SourceAddress, DestAddress, SourcePort, DestPort, ConnectionInfo
                                ]}});
                            {error, _Reason} -> false
                        end;
                false -> gen_statem:call(Pid, {accept, Sock, Addr})
            end;
        {error, _Reason} ->  % Most probably the socket is closed
            false
    end.

handle_input(Pid, <<CmdLen:32, _Rest/binary>> = Buffer, Lapse, N, Log) ->
    case CmdLen > ?MAX_COMMAND_LENGTH of
        true ->
            Reason = smpp_error:format(?ESME_RINVCMDLEN),
            logger:info("error during pdu handling: ~s", [Reason]),
            gen_statem:cast(Pid, {sock_error, ?ESME_RINVCMDLEN}),
            <<>>;
        false -> do_handle_input(Pid, Buffer, Lapse, N, Log)
    end;
handle_input(_Pid, Buffer, _Lapse, _N, _Log) ->
    Buffer.

do_handle_input(Pid, <<CmdLen:32, Rest/binary>> = Buffer, Lapse, N, Log) ->
    Now = time:timestamp(), % PDU received.  PDU handling starts now!
    Len = CmdLen - 4,
    case Rest of
        <<PduRest:Len/binary-unit:8, NextPdus/binary>> ->
            BinPdu = <<CmdLen:32, PduRest/binary>>,
            case catch smpp_operation:unpack(BinPdu) of
                {ok, Pdu} ->
                    smpp_log_mgr:pdu(Log, BinPdu),
                    CmdId = smpp_operation:get_value(command_id, Pdu),
                    Event = {input, CmdId, Pdu, (Lapse div N), Now},
                    logger:debug("sending pdu to session ~p, sequence_number ~p", [Pid, smpp_operation:get_value(sequence_number,Pdu)]),
                    gen_statem:cast(Pid, Event);
                {error, _CmdId, Status, _SeqNum} = Event ->
                    Reason = smpp_error:format(Status),
                    logger:info("error during pdu handling: ~s", [Reason]),
                    gen_statem:cast(Pid, Event);
                {'EXIT', _What} ->
                    Event = {error, 0, ?ESME_RUNKNOWNERR, 0},
                    gen_statem:cast(Pid, Event)
            end,
            % The buffer may carry more than one SMPP PDU.
            handle_input(Pid, NextPdus, Lapse, N + 1, Log);
        <<CmdId:32, _IncompletePduRest/binary>> ->
            case ?VALID_COMMAND_ID(CmdId) of
                true -> Buffer;
                false ->
                    Reason = smpp_error:format(?ESME_RINVCMDID),
                    logger:info("error during pdu handling: ~s", [Reason]),
                    gen_statem:cast(Pid, {sock_error, ?ESME_RINVCMDID}),
                    <<>>
            end;
        _IncompletePdu ->
            Buffer
    end;
do_handle_input(_Pid, Buffer, _Lapse, _N, _Log) ->
    Buffer.
