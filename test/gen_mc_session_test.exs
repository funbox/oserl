defmodule GenMcSessionTest do
  use ExUnit.Case

  test "gen_mc_session handle_timeout when req is not in smpp_req_tab" do
    {:ok, _mc} = MC.start_link(false)
    {:ok, _esme} = ESME.start_link(:rx, false)

    wait_bound()

    [session | _] = :gen_mc.call(MC, :rxs)

    MC.deliver_sm(deliver_sm_params())
    wait_recv()
    Process.sleep(100)

    send(session, {:timeout, make_ref(), {:mc_response_timer, 1}})
    Process.sleep(500)

    assert Process.alive?(session)
  after
    MC.stop()
    ESME.stop()
  end

  defp deliver_sm_params do
    [
      {:short_message, ~c"test"},
      {:source_addr, ~c"9999"},
      {:source_addr_ton, 2},
      {:source_addr_npi, 1},
      {:dest_addr_ton, 2},
      {:dest_addr_npi, 1},
      {:destination_addr, ~c"79123456789"},
      {:esm_class, 0},
      {:protocol_id, 0},
      {:priority_flag, 0}
    ]
  end

  defp wait_bound do
    if ESME.bound() do
      :ok
    else
      Process.sleep(100)
      wait_bound()
    end
  end

  defp wait_recv do
    if ESME.recv() > 0 do
      :ok
    else
      Process.sleep(100)
      wait_recv()
    end
  end
end
