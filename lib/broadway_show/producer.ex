defmodule BroadwayShow.Producer do
  @moduledoc """
  A custom Broadway producer.

  See https://hexdocs.pm/broadway/custom-producers.html
  """

  use GenStage

  alias Broadway.Message

  @behaviour Broadway.Acknowledger
  @behaviour Broadway.Producer

  defmodule State do
    defstruct [:counter, :demand, :acks, :nacks, :status]

    def new do
      %State{counter: 0, demand: 0, acks: MapSet.new(), nacks: MapSet.new(), status: :running}
    end
  end

  alias BroadwayShow.Producer.State

  @impl GenStage
  def init(_opts) do
    {:producer, State.new()}
  end

  @impl GenStage
  def handle_demand(demand, %State{status: :running} = state) when demand > 0 do
    %State{demand: existing_demand} = state

    {events, state} = build_events(%State{state | demand: existing_demand + demand})

    {:noreply, events, state}
  end

  @impl GenStage
  def handle_demand(_demand, %State{status: :stopped} = state) do
    # Don't produce any more events when status is `:stopped`
    {:noreply, [], state}
  end

  @impl Broadway.Acknowledger
  def ack(pid, successful, failed) do
    send(pid, {:ack, Enum.map(successful, & &1.data)})
    send(pid, {:nack, Enum.map(failed, & &1.data)})

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
  def handle_info({:nack, []}, %State{} = state) do
    {events, state} = build_events(state)

    {:noreply, events, state}
  end

  def handle_info({:nack, nack}, %State{} = state) do
    %State{nacks: nacks} = state

    state = %State{state | nacks: Enum.reduce(nack, nacks, &MapSet.put(&2, &1)), status: :stopped}

    {events, state} = build_events(state)

    {:noreply, events, state}
  end

  defp build_events(%State{} = state) do
    %State{acks: acks, counter: counter, demand: demand} = state

    events =
      counter..1000
      |> Stream.filter(fn i ->
        expected_ack = i - 5

        expected_ack < 0 || MapSet.member?(acks, expected_ack)
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
