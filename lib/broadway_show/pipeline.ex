defmodule BroadwayShow.Pipeline do
  use Broadway

  alias Broadway.BatchInfo
  alias Broadway.Message
  alias BroadwayShow.Pipeline

  def start_link(opts) do
    Broadway.start_link(Pipeline,
      name: BroadwayShow.Pipeline,
      context: opts,
      producers: [
        default: [
          module: {BroadwayShow.Producer, opts},
          stages: 1
        ]
      ],
      processors: [
        default: [stages: 1]
      ],
      batchers: [
        default: [stages: 5, batch_size: 1]
      ]
    )
  end

  @impl Broadway
  def handle_message(_processor, %Message{} = message, _context) do
    message
  end

  @impl Broadway
  def handle_batch(_batcher, messages, _batch_info, context) do
    handle_data = Keyword.get(context, :handle_data)

    for %Message{data: data} = message <- messages do
      case handle_data.(data) do
        :ok -> message
        :error -> Message.failed(message, :error)
      end
    end
  end
end
