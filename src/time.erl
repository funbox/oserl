-module(time).

-export([timestamp/0]).

timestamp() ->
   OtpRelease = list_to_integer(erlang:system_info(otp_release)),
   case OtpRelease of
      17 -> erlang:now();
      N when N >= 18 -> erlang:timestamp();
      _ -> {error, "OTP verstion is not supported"}
   end.
