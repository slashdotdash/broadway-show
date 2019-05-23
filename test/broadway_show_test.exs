defmodule BroadwayShowTest do
  use ExUnit.Case

  describe "producer ignore error" do
    setup do
      reply_to = self()

      {:ok, _pid} =
        start_supervised(
          {BroadwayShow.Pipeline,
           [
             handle_data: fn data ->
               send(reply_to, data)

               if data == 10, do: :error, else: :ok
             end,
             stop_on_error: false
           ]}
        )

      :ok
    end

    test "should receive all numbers" do
      expected_numbers = MapSet.new(0..100)
      unexpected_numbers = MapSet.new()

      assert_receive_numbers(expected_numbers, unexpected_numbers)
    end
  end

  describe "producer stop on error" do
    setup do
      reply_to = self()

      {:ok, _pid} =
        start_supervised(
          {BroadwayShow.Pipeline,
           [
             handle_data: fn data ->
               send(reply_to, data)

               if data == 10, do: :error, else: :ok
             end,
             stop_on_error: true
           ]}
        )

      :ok
    end

    test "should not receive numbers from stopped partition after error" do
      expected_numbers = 0..100 |> Enum.filter(&(&1 <= 10 || rem(&1, 5) != 0)) |> MapSet.new()
      unexpected_numbers = 11..100 |> Enum.filter(&(rem(&1, 5) == 0)) |> MapSet.new()

      assert_receive_numbers(expected_numbers, unexpected_numbers)
    end
  end

  defp assert_receive_numbers(expected_numbers, unexpected_numbers) do
    receive do
      n ->
        assert MapSet.member?(expected_numbers, n)
        refute MapSet.member?(unexpected_numbers, n)

        assert_receive_numbers(expected_numbers, unexpected_numbers)
    after
      100 ->
        refute_receive 15
    end
  end
end
