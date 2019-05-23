defmodule BroadwayShow.Producer do
  @moduledoc """
  A custom Broadway producer.

  See https://hexdocs.pm/broadway/custom-producers.html
  """

  use GenStage

  require Logger

  alias Broadway.Message

  @behaviour Broadway.Acknowledger
  @behaviour Broadway.Producer

  defmodule State do
    defstruct [:counter, :demand, :acks, :nacks, :opts, :status]

    def new(opts) do
      %State{
        counter: 0,
        demand: 0,
        acks: MapSet.new(),
        nacks: MapSet.new(),
        opts: opts,
        status: :running
      }
    end
  end

  alias BroadwayShow.Producer.State

  @impl GenStage
  def init(opts) do
    {:producer, State.new(opts)}
  end

  @impl GenStage
  def handle_demand(demand, %State{} = state) when demand > 0 do
    %State{demand: existing_demand} = state

    {events, state} = build_events(%State{state | demand: existing_demand + demand})

    {:noreply, events, state}
  end

  @impl Broadway.Acknowledger
  def ack(pid, successful, failed) do
    send(pid, {:ack, Enum.map(successful, & &1.data)})
    send(pid, {:nack, Enum.map(failed, &{&1.data, &1.status})})

    :ok
  end

  @impl GenStage
  def handle_info({:ack, ack}, %State{} = state) do
    %State{acks: acks} = state

    state = %State{state | acks: Enum.reduce(ack, acks, &MapSet.put(&2, &1))}

    {events, state} = build_events(state)

    {:noreply, events, state}
  end

  @impl GenStage
  def handle_info({:nack, nack}, %State{} = state) do
    %State{nacks: nacks} = state

    state =
      case nack do
        [] ->
          state

        nack ->
          %State{
            state
            | nacks: Enum.reduce(nack, nacks, &MapSet.put(&2, &1)),
              status: status_on_error(state)
          }
      end

    {events, state} = build_events(state)

    {:noreply, events, state}
  end

  defp status_on_error(%State{} = state) do
    %State{opts: opts, status: status} = state

    if Keyword.get(opts, :stop_on_error, false) do
      Logger.warn(fn -> "Producer requested to stop on error" end)

      :stopped
    else
      status
    end
  end

  # No demand for events.
  defp build_events(%State{demand: 0} = state), do: {[], state}

  # Don't produce any more events when status is `:stopped`.
  defp build_events(%State{status: :stopped} = state), do: {[], state}

  defp build_events(%State{} = state) do
    %State{acks: acks, nacks: nacks, counter: counter, demand: demand} = state

    events =
      counter..100
      |> Stream.filter(fn i ->
        previous = i - 5

        previous < 0 || MapSet.member?(acks, previous) || MapSet.member?(nacks, previous)
      end)
      |> Stream.map(fn i ->
        %Message{
          data: i,
          metadata: %{},
          acknowledger: {__MODULE__, self(), %{}},
          batch_key: rem(i, 5)
        }
      end)
      |> Enum.take(demand)

    counter = Enum.reduce(events, counter, fn %Message{data: counter}, _acc -> counter + 1 end)

    state = %State{state | counter: counter, demand: demand - length(events)}

    {events, state}
  end
end
