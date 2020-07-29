-module(proxy_protocol).

-include("proxy_protocol.hrl").

-export([accept/1]).

-record(proxy_opts, { inet_version :: ipv4|ipv6,
                      source_address :: inet:ip_address(),
                      dest_address :: inet:ip_address(),
                      source_port :: inet:port_number(),
                      dest_port :: inet:port_number(),
                      connection_info = []}).
-opaque proxy_opts() :: #proxy_opts{}.

-define(WAITING_TIMEOUT, 1000).

-export_type([proxy_opts/0]).
%%%-----------------------------------------------------------------------------
%%% EXPORTS
%%%-----------------------------------------------------------------------------
accept(Sock) ->
    inet:setopts(Sock, [{active, once}, {packet, line}]),
    receive
      {_, CSocket, <<"\r\n">>} ->
          ok = inet:setopts(Sock, [{packet, raw}]),
          {ok, ProxyHeader} = gen_tcp:recv(CSocket, 14, 1000),
          case parse_proxy_protocol_v2(<<"\r\n", ProxyHeader/binary>>) of
              {proxy, ipv4, _Protocol, Length} ->
                  {ok, ProxyAddr} = gen_tcp:recv(CSocket, Length, 1000),
                  case ProxyAddr of
                      <<SA1:8, SA2:8, SA3:8, SA4:8,
                        DA1:8, DA2:8, DA3:8, DA4:8,
                        SourcePort:16, DestPort:16, Rest/binary>> ->
                          SourceAddress = {SA1, SA2, SA3, SA4},
                          DestAddress = {DA1, DA2, DA3, DA4},
                          ConnectionInfo = parse_tlv(Rest),
                          {ok, #proxy_opts{inet_version = ipv4,
                                           source_address = SourceAddress,
                                           dest_address = DestAddress,
                                           source_port = SourcePort,
                                           dest_port = DestPort,
                                           connection_info = ConnectionInfo}};
                      _ ->
                          gen_tcp:close(Sock),
                          lager:error("Not proxy protocol"),
                          {error, not_proxy_protocol}
                  end;
              _Unsupported ->
                  gen_tcp:close(Sock),
                  {error, not_supported_v2}
            end;
        {_, _CSocket, Data} ->
            lager:notice("data: ~p", [Data])
    after
        ?WAITING_TIMEOUT ->
            gen_tcp:close(Sock),
            lager:error("Proxy protocol header expected but not received"),
            {error, timeout}
    end.

%%%-----------------------------------------------------------------------------
%%% INTERNAL FUNCTIONS
%%%-----------------------------------------------------------------------------
parse_proxy_protocol_v2(<<?HEADER, (?VSN):4, 0:4, X:4, Y:4, Len:16>>) ->
    {local, family(X), protocol(Y), Len};
parse_proxy_protocol_v2(<<?HEADER, (?VSN):4, 1:4, X:4, Y:4, Len:16>>) ->
    {proxy, family(X), protocol(Y), Len};
parse_proxy_protocol_v2(_) ->
    not_proxy_protocol.

parse_tlv(Rest) ->
    parse_tlv(Rest, []).

parse_tlv(<<>>, Result) ->
    Result;
parse_tlv(<<Type:8, Len:16, Value:Len/binary, Rest/binary>>, Result) ->
    case pp2_type(Type) of
        ssl ->
            parse_tlv(Rest, pp2_value(Type, Value) ++ Result);
        TypeName ->
            parse_tlv(Rest, [{TypeName, Value} | Result])
    end;
parse_tlv(_, _) ->
    {error, parse_tlv}.

pp2_type(?PP2_TYPE_ALPN) ->
    negotiated_protocol;
pp2_type(?PP2_TYPE_AUTHORITY) ->
    authority;
pp2_type(?PP2_TYPE_SSL) ->
    ssl;
pp2_type(?PP2_SUBTYPE_SSL_VERSION) ->
    protocol;
pp2_type(?PP2_SUBTYPE_SSL_CN) ->
    sni_hostname;
pp2_type(?PP2_TYPE_NETNS) ->
    netns;
pp2_type(_) ->
    invalid_pp2_type.

pp2_value(?PP2_TYPE_SSL, <<Client:1/binary, _:32, Rest/binary>>) ->
    case pp2_client(Client) of % validates bitfield format, but ignores data
        invalid_client ->
            invalid;
        _ ->
            %% Fetches TLV values attached, regardless of if the client
            %% specified SSL. If this is a problem, then we should fix,
            %% but in any case the blame appears to be on the sender
            %% who is giving us broken headers.
            parse_tlv(Rest)
    end;
pp2_value(_, Value) ->
    Value.

pp2_client(<<0:5,             % UNASSIGNED
             _ClientCert:1,   % PP2_CLIENT_CERT_SESS
             _ClientCert:1,   % PP2_CLIENT_CERT_CONN
             _ClientSSL:1>>) ->
    client_ssl;
pp2_client(_) ->
    invalid_client.

family(?AF_UNSPEC) ->
    af_unspec;
family(?AF_INET) ->
    ipv4;
family(?AF_INET6) ->
    ipv6;
family(?AF_UNIX) ->
    af_unix;
family(_) ->
    {error, invalid_address_family}.

protocol(?UNSPEC) ->
    unspec;
protocol(?STREAM) ->
    stream;
protocol(?DGRAM) ->
    dgram;
protocol(_) ->
    {error, invalid_protocol}.
