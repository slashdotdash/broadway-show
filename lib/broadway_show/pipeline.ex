defmodule BroadwayShow.Pipeline do
  use Broadway

  import Integer

  alias Broadway.BatchInfo
  alias Broadway.Message
  alias BroadwayShow.Pipeline

  def start_link(_opts) do
    Broadway.start_link(Pipeline,
      name: BroadwayShow.Pipeline,
      producers: [
        default: [
          module: {BroadwayShow.Producer, []},
          stages: 1
        ]
      ],
      processors: [
        default: [stages: 1]
      ],
      batchers: [
        default: [stages: 3, batch_size: 1]
      ]
    )
  end

  @impl Broadway
  def handle_message(_processor, %Message{} = message, _context) do
    message
  end

  @impl Broadway
  def handle_batch(_batcher, messages, %BatchInfo{} = batch_info, _context) do
    [%Message{data: data} | _messages] = messages
    %BatchInfo{batch_key: batch_key} = batch_info

    log = inspect(self()) <> " [#{batch_key}] " <> String.pad_leading(inspect(data), 2)

    if data == 10 do
      IO.puts(log <> " x")
      Enum.map(messages, &Message.failed(&1, :error))
    else
      IO.puts(log <> " ✓")
      messages
    end
  end
end
