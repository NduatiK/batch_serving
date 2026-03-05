defmodule BatchServingTest do
  use ExUnit.Case

  def square_values(values), do: Enum.map(values, &(&1 * &1))
  def square_serving(), do: BatchServing.new(&square_values/1)

  describe "inline" do
    test "simple inline one" do
      assert 4 == BatchServing.inline(square_serving(), 2)
    end

    test "simple inline many" do
      assert [1, 4, 9, 16] == BatchServing.inline_many(square_serving(), [1, 2, 3, 4])
    end

    test "simple inline many with stream" do
      input_stream = Stream.map([1, 2, 3, 4], & &1)

      assert [[1], [4], [9], [16]] ==
               BatchServing.inline_many(square_serving(), input_stream)
    end

    test "input/result mapping inline single" do
      serving =
        square_serving()
        |> BatchServing.map_inputs(fn input -> input end)
        |> BatchServing.map_results(&Enum.map(&1, fn x -> "#{x}" end))

      assert "4" == BatchServing.inline(serving, 2)
    end

    test "input/result mapping inline" do
      serving =
        square_serving()
        |> BatchServing.map_inputs(fn input -> input end)
        |> BatchServing.map_results(&Enum.map(&1, fn x -> "#{x}" end))

      assert ["1", "4", "9", "16"] == BatchServing.inline_many(serving, [1, 2, 3, 4])
    end
  end

  describe "serving process" do
    test "dispatch single" do
      {:ok, _pid} =
        start_supervised(%{id: BatchServing.PG, start: {:pg, :start_link, [BatchServing.PG]}})

      {:ok, _pid} =
        start_supervised(
          {BatchServing,
           [
             serving: square_serving(),
             name: MyServing,
             batch_size: 10,
             batch_timeout: 100
           ]}
        )

      assert 4 == BatchServing.dispatch(MyServing, 2)
    end

    test "dispatch many" do
      {:ok, _pid} =
        start_supervised(%{id: BatchServing.PG, start: {:pg, :start_link, [BatchServing.PG]}})

      {:ok, _pid} =
        start_supervised(
          {BatchServing,
           [
             serving: square_serving(),
             name: MyServing,
             batch_size: 10,
             batch_timeout: 100
           ]}
        )

      assert [1, 4, 9, 16, 25] == BatchServing.dispatch_many(MyServing, [1, 2, 3, 4, 5])
    end

    test "dispatch many mini example with stream" do
      {:ok, _pid} =
        start_supervised(%{id: BatchServing.PG, start: {:pg, :start_link, [BatchServing.PG]}})

      {:ok, _pid} =
        start_supervised(
          {BatchServing,
           [
             serving: square_serving(),
             name: StreamServing,
             batch_size: 10,
             batch_timeout: 100
           ]}
        )

      input_stream = Stream.map([1, 2, 3], & &1)

      assert [1, 4, 9] == BatchServing.dispatch_many(StreamServing, input_stream)
    end

    test "dispatch many stream treats each streamed list as one item" do
      {:ok, _pid} =
        start_supervised(%{id: BatchServing.PG, start: {:pg, :start_link, [BatchServing.PG]}})

      {:ok, _pid} =
        start_supervised(
          {BatchServing,
           [
             serving: BatchServing.new(fn values -> Enum.map(values, &Enum.sum/1) end),
             name: StreamItemServing,
             batch_size: 2,
             batch_timeout: 100
           ]}
        )

      input_stream = Stream.map([[1, 2], [3]], & &1)

      assert [3, 3] == BatchServing.dispatch_many(StreamItemServing, input_stream)
    end

    test "dispatch many with stream" do
      {:ok, _pid} =
        start_supervised(%{id: BatchServing.PG, start: {:pg, :start_link, [BatchServing.PG]}})

      {:ok, _pid} =
        start_supervised(
          {BatchServing,
           serving:
             BatchServing.new(fn values ->
               Enum.map(values, &(&1 * &1))
             end)
             |> BatchServing.map_inputs(fn input ->
               input
               |> List.wrap()
               |> Stream.map(& &1)
             end),
           name: MyServing,
           batch_size: 2,
           batch_timeout: 100}
        )

      assert [[1, 4, 9], [1, 4, 9], [1, 4, 9], [1, 4, 9]] =
               Task.async_stream(
                 1..4,
                 fn _ ->
                   data = [1, 2, 3]
                   BatchServing.dispatch_many(MyServing, data)
                 end,
                 max_concurrency: 2
               )
               |> Enum.map(fn {:ok, results} -> results end)
               |> Enum.to_list()
    end

    test "dispatch single with coalescing" do
      {:ok, _pid} =
        start_supervised(%{id: BatchServing.PG, start: {:pg, :start_link, [BatchServing.PG]}})

      {:ok, _pid} =
        start_supervised(
          {BatchServing,
           serving:
             BatchServing.new(fn values ->
               Enum.map(values, &(&1 * &1))
             end),
           name: MyServing,
           batch_size: 2,
           batch_timeout: 100}
        )

      run_work = fn num, duration_range ->
        started_at = System.monotonic_time(:millisecond)

        assert square_values(1..num) ==
                 1..num
                 |> Task.async_stream(
                   fn num ->
                     BatchServing.dispatch(MyServing, num)
                   end,
                   max_concurrency: 4
                 )
                 |> Enum.map(fn {:ok, results} -> results end)
                 |> Enum.to_list()

        total_latency = System.monotonic_time(:millisecond) - started_at
        assert total_latency in duration_range
      end

      # 5 numbers were processed in 3 batches, 1 batch had to wait before executing
      assert run_work.(5, 101..120)

      # 6 numbers were processed in 3 batches that were immediately filled
      assert run_work.(6, 0..10)
    end

    test "streaming returns first batch before full completion" do
      {:ok, _pid} =
        start_supervised(%{id: BatchServing.PG, start: {:pg, :start_link, [BatchServing.PG]}})

      {:ok, _pid} =
        start_supervised(
          {BatchServing,
           serving:
             BatchServing.new(fn values ->
               :timer.sleep(200)
               square_values(values)
             end)
             |> BatchServing.streaming(),
           name: EarlyStreamServing,
           batch_size: 2,
           batch_timeout: 50}
        )

      started_at = System.monotonic_time(:millisecond)

      {events, first_event_latency} =
        BatchServing.dispatch_many(EarlyStreamServing, [1, 2, 3, 4])
        |> Enum.reduce({[], nil}, fn event, {acc, first_latency} ->
          current_latency = System.monotonic_time(:millisecond) - started_at
          {[event | acc], first_latency || current_latency}
        end)

      total_latency = System.monotonic_time(:millisecond) - started_at
      events = Enum.reverse(events)

      assert events == [{:batch, [1, 4]}, {:batch, [9, 16]}]
      assert first_event_latency >= 150
      assert first_event_latency < total_latency
      assert total_latency >= 350
    end

    test "map_inputs can transform structured input" do
      serving =
        square_serving()
        |> BatchServing.map_inputs(fn values -> Enum.map(values, & &1.number) end)

      assert 4 == BatchServing.inline(serving, %{number: 2})
      assert [4, 9] == BatchServing.inline_many(serving, [%{number: 2}, %{number: 3}])
    end

    test "inline_many must be called with a list or stream" do
      serving =
        square_serving()
        |> BatchServing.map_inputs(fn values -> values end)

      assert_raise FunctionClauseError, fn ->
        BatchServing.inline_many(serving, %{numbers: [1, 2, 3]})
      end
    end

    defmodule ServingModule do
      @behaviour BatchServing

      @impl true
      def init(_inline_or_process, :unused_arg, [_options]) do
        {:ok, fn batch -> Enum.map(batch.values, &(&1 * &1)) end}
      end

      @impl true
      def handle_batch(batch, 0, function) do
        {:execute, fn -> function.(batch) end, function}
      end
    end

    test "module" do
      {:ok, _pid} =
        start_supervised(%{id: BatchServing.PG, start: {:pg, :start_link, [BatchServing.PG]}})

      {:ok, _pid} =
        start_supervised(
          {BatchServing,
           serving: BatchServing.new(ServingModule, :unused_arg),
           name: MyServing,
           batch_size: 3,
           batch_timeout: 100}
        )

      assert [1, 4, 9, 16, 25] == BatchServing.dispatch_many(MyServing, [1, 2, 3, 4, 5])
    end

    defmodule HookStreamingModule do
      @behaviour BatchServing

      @impl true
      def init(_inline_or_process, :ok, [options]) do
        {:ok, %{hooks: Keyword.get(options, :hooks, %{})}}
      end

      @impl true
      def handle_batch(%BatchServing.Batch{values: values}, 0, %{hooks: hooks} = state) do
        {:execute,
         fn ->
           if hook_fun = hooks[:progress], do: hook_fun.(values)
           Enum.map(values, &(&1 * 10))
         end, state}
      end
    end

    test "streaming with hooks emits hook events" do
      {:ok, _pid} =
        start_supervised(%{id: BatchServing.PG, start: {:pg, :start_link, [BatchServing.PG]}})

      {:ok, _pid} =
        start_supervised(
          {BatchServing,
           serving:
             BatchServing.new(HookStreamingModule, :ok)
             |> BatchServing.streaming(hooks: [:progress]),
           name: HookStreamServing,
           batch_size: 3,
           batch_timeout: 50}
        )

      events =
        BatchServing.dispatch_many(HookStreamServing, [1, 2, 3, 4, 5])
        |> Enum.to_list()

      assert events == [
               {:progress, [1, 2, 3]},
               {:batch, [10, 20, 30]},
               {:progress, [4, 5]},
               {:batch, [40, 50]}
             ]
    end

    test "partitions" do
      {:ok, _pid} =
        start_supervised(%{id: BatchServing.PG, start: {:pg, :start_link, [BatchServing.PG]}})

      {:ok, _pid} =
        start_supervised(
          {BatchServing,
           serving:
             BatchServing.new(fn values ->
               :timer.sleep(2_000)
               Enum.map(values, &(&1 * &1))
             end),
           name: PartitionedServing,
           batch_size: 2,
           partitions: 4,
           batch_timeout: 100}
        )

      {time_in_microseconds, _} =
        :timer.tc(fn ->
          Task.async_stream(
            1..4,
            fn i ->
              batch = [i, i + 1]

              assert [i ** 2, (i + 1) ** 2] ==
                       BatchServing.dispatch_many(PartitionedServing, batch)
            end,
            max_concurrency: 4
          )
          |> Enum.map(fn {:ok, results} -> results end)
          |> Enum.to_list()
        end)

      time_in_seconds = time_in_microseconds / 1_000_000
      assert time_in_seconds < 2.1
    end

    test "dispatch_safe returns ok for successful requests" do
      {:ok, _pid} =
        start_supervised(%{id: BatchServing.PG, start: {:pg, :start_link, [BatchServing.PG]}})

      {:ok, _pid} =
        start_supervised(
          {BatchServing,
           serving: square_serving(), name: SafeServing, batch_size: 10, batch_timeout: 100}
        )

      assert {:ok, [4, 9]} = BatchServing.dispatch_many_safe(SafeServing, [2, 3])
    end

    test "dispatch_safe returns error for missing serving" do
      assert {:error, _reason} = BatchServing.dispatch_safe({:local, MissingServing}, 1)
    end
  end
end
