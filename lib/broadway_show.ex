defmodule BroadwayShow do
  def start_link(opts), do: BroadwayShow.Pipeline.start_link(opts)
end
