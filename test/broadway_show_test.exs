defmodule BroadwayShowTest do
  use ExUnit.Case

  setup do
    {:ok, pid} = BroadwayShow.Pipeline.start_link([])

    [pid: pid]
  end

  test "runs broadway pipeline" do
    :timer.sleep(500)
  end
end
