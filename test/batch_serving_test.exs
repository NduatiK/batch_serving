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

      assert 4 == BatchServing.dispatch!(MyServing, 2)
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

      assert [1, 4, 9, 16, 25] == BatchServing.dispatch_many!(MyServing, [1, 2, 3, 4, 5])
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

      assert [1, 4, 9] == BatchServing.dispatch_many!(StreamServing, input_stream)
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

      assert [3, 3] == BatchServing.dispatch_many!(StreamItemServing, input_stream)
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
                   BatchServing.dispatch_many!(MyServing, data)
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
                     BatchServing.dispatch!(MyServing, num)
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
        BatchServing.dispatch_many!(EarlyStreamServing, [1, 2, 3, 4])
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

      assert [1, 4, 9, 16, 25] == BatchServing.dispatch_many!(MyServing, [1, 2, 3, 4, 5])
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

    defmodule RecordingHookStreamingModule do
      @behaviour BatchServing

      @impl true
      def init(_inline_or_process, test_pid, options) do
        {:ok, %{test_pid: test_pid, options: options}}
      end

      @impl true
      def handle_batch(
            %BatchServing.Batch{values: values},
            partition,
            %{test_pid: test_pid, options: options} = state
          ) do
        hooks = Enum.at(options, partition)[:hooks]

        {:execute,
         fn ->
           send(test_pid, {:executed_batch, values})
           if hook_fun = hooks[:progress], do: hook_fun.(values)
           values
         end, state}
      end
    end

    defmodule DelayedPartitionStreamingModule do
      @behaviour BatchServing

      @impl true
      def init(_inline_or_process, test_pid, options) do
        {:ok, %{test_pid: test_pid, options: options}}
      end

      @impl true
      def handle_batch(
            %BatchServing.Batch{values: values},
            partition,
            %{test_pid: test_pid, options: options} = state
          ) do
        hooks = Enum.at(options, partition)[:hooks]

        {:execute,
         fn ->
           if partition == 0, do: Process.sleep(100)
           send(test_pid, {:executed_batch, partition, values})
           if hook_fun = hooks[:progress], do: hook_fun.(values)
           values
         end, state}
      end
    end

    defmodule RecordingBatchModule do
      @behaviour BatchServing

      @impl true
      def init(_inline_or_process, test_pid, [_options]) do
        {:ok, test_pid}
      end

      @impl true
      def handle_batch(%BatchServing.Batch{values: values} = batch, 0, test_pid) do
        {:execute,
         fn ->
           send(test_pid, {:executed_batch, values, batch.size})
           values
         end, test_pid}
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
        BatchServing.dispatch_many!(HookStreamServing, [1, 2, 3, 4, 5])
        |> Enum.to_list()

      assert events == [
               {:progress, [1, 2, 3]},
               {:batch, [10, 20, 30]},
               {:progress, [4, 5]},
               {:batch, [40, 50]}
             ]
    end

    test "streaming hooks preserve explicit batch boundaries across callers" do
      {:ok, _pid} =
        start_supervised(%{id: BatchServing.PG, start: {:pg, :start_link, [BatchServing.PG]}})

      {:ok, _pid} =
        start_supervised(
          {BatchServing,
           serving:
             BatchServing.new(RecordingHookStreamingModule, self())
             |> BatchServing.streaming(hooks: [:progress]),
           name: HookBoundaryServing,
           batch_size: 3,
           batch_timeout: 50}
        )

      results =
        [[1, 2], [3, 4], [5, 6]]
        |> Task.async_stream(
          fn batch ->
            BatchServing.dispatch_many!(HookBoundaryServing, batch)
            |> Enum.to_list()
          end,
          max_concurrency: 3,
          ordered: true
        )
        |> Enum.map(fn {:ok, events} -> events end)

      executed_batches =
        for _ <- 1..3 do
          assert_receive {:executed_batch, values}, 1_000
          values
        end

      assert results == [
               [{:progress, [1, 2]}, {:batch, [1, 2]}],
               [{:progress, [3, 4]}, {:batch, [3, 4]}],
               [{:progress, [5, 6]}, {:batch, [5, 6]}]
             ]

      assert executed_batches == [[1, 2], [3, 4], [5, 6]]
    end

    test "streaming hooks preserve explicit batch boundaries with partitions" do
      {:ok, _pid} =
        start_supervised(%{id: BatchServing.PG, start: {:pg, :start_link, [BatchServing.PG]}})

      {:ok, _pid} =
        start_supervised({
          BatchServing,
          serving:
            BatchServing.new(RecordingHookStreamingModule, self())
            |> BatchServing.streaming(hooks: [:progress]),
          name: HookBoundaryServing,
          batch_size: 4,
          partitions: 2,
          batch_timeout: 50
        })

      results =
        BatchServing.dispatch_many!(HookBoundaryServing, Enum.to_list(1..9))
        |> Enum.to_list()

      assert results == [
               {:progress, [1, 2, 3, 4]},
               {:batch, [1, 2, 3, 4]},
               {:progress, [5, 6, 7, 8]},
               {:batch, [5, 6, 7, 8]},
               {:progress, [9]},
               {:batch, [9]}
             ]
    end

    test "partitioning does not reorder results" do
      {:ok, _pid} =
        start_supervised(%{id: BatchServing.PG, start: {:pg, :start_link, [BatchServing.PG]}})

      {:ok, _pid} =
        start_supervised({
          BatchServing,
          serving: BatchServing.new(RecordingHookStreamingModule, self()),
          name: HookBoundaryServing,
          batch_size: 4,
          partitions: 2,
          batch_timeout: 50
        })

      results =
        BatchServing.dispatch_many!(HookBoundaryServing, Enum.to_list(1..9))
        |> Enum.to_list()

      assert results == Enum.to_list(1..9)
    end

    test "streaming hooks stay ordered when a later partition finishes first" do
      {:ok, _pid} =
        start_supervised(%{id: BatchServing.PG, start: {:pg, :start_link, [BatchServing.PG]}})

      {:ok, _pid} =
        start_supervised({
          BatchServing,
          serving:
            BatchServing.new(DelayedPartitionStreamingModule, self())
            |> BatchServing.streaming(hooks: [:progress]),
          name: InterleavedHookServing,
          batch_size: 4,
          partitions: 2,
          batch_timeout: 50
        })

      results =
        BatchServing.dispatch_many!(InterleavedHookServing, Enum.to_list(1..8))
        |> Enum.to_list()

      assert_receive {:executed_batch, 1, [5, 6, 7, 8]}, 1_000
      assert_receive {:executed_batch, 0, [1, 2, 3, 4]}, 1_000

      assert results == [
               {:progress, [1, 2, 3, 4]},
               {:batch, [1, 2, 3, 4]},
               {:progress, [5, 6, 7, 8]},
               {:batch, [5, 6, 7, 8]}
             ]
    end

    test "distributed streaming stays ordered when a later partition finishes first" do
      {:ok, _pid} =
        start_supervised(%{id: BatchServing.PG, start: {:pg, :start_link, [BatchServing.PG]}})

      {:ok, _pid} =
        start_supervised({
          BatchServing,
          serving:
            BatchServing.new(DelayedPartitionStreamingModule, self())
            |> BatchServing.streaming(hooks: [:progress]),
          name: DistributedInterleavedHookServing,
          batch_size: 4,
          partitions: 2,
          batch_timeout: 50
        })

      results =
        BatchServing.dispatch_many!(
          {:distributed, DistributedInterleavedHookServing},
          Enum.to_list(1..8)
        )
        |> Enum.to_list()

      assert_receive {:executed_batch, 1, [5, 6, 7, 8]}, 1_000
      assert_receive {:executed_batch, 0, [1, 2, 3, 4]}, 1_000

      assert results == [
               {:progress, [1, 2, 3, 4]},
               {:batch, [1, 2, 3, 4]},
               {:progress, [5, 6, 7, 8]},
               {:batch, [5, 6, 7, 8]}
             ]
    end

    test "without hooks, concurrent explicit batches are handled 3 at a time" do
      {:ok, _pid} =
        start_supervised(%{id: BatchServing.PG, start: {:pg, :start_link, [BatchServing.PG]}})

      {:ok, _pid} =
        start_supervised(
          {BatchServing,
           serving: BatchServing.new(RecordingBatchModule, self()),
           name: CoalescingServing,
           batch_size: 3,
           batch_timeout: 50}
        )

      results =
        [[1, 2], [3, 4], [5, 6]]
        |> Task.async_stream(
          fn batch -> BatchServing.dispatch_many!(CoalescingServing, batch) end,
          max_concurrency: 3,
          ordered: true
        )
        |> Enum.map(fn {:ok, values} -> values end)

      executed_batches =
        for _ <- 1..2 do
          assert_receive {:executed_batch, values, size}, 1_000
          {values, size}
        end

      assert results == [[1, 2], [3, 4], [5, 6]]
      assert executed_batches == [{[1, 2, 3], 3}, {[4, 5, 6], 3}]
    end

    test "with hooks, concurrent dispatch callers are still handled 3 at a time" do
      {:ok, _pid} =
        start_supervised(%{id: BatchServing.PG, start: {:pg, :start_link, [BatchServing.PG]}})

      {:ok, _pid} =
        start_supervised(
          {BatchServing,
           serving:
             BatchServing.new(RecordingHookStreamingModule, self())
             |> BatchServing.streaming(hooks: [:progress]),
           name: HookDispatchServing,
           batch_size: 3,
           batch_timeout: 50}
        )

      results =
        1..6
        |> Task.async_stream(
          fn value ->
            BatchServing.dispatch!(HookDispatchServing, value)
            |> Enum.to_list()
          end,
          max_concurrency: 6,
          ordered: true
        )
        |> Enum.map(fn {:ok, events} -> events end)

      executed_batches =
        for _ <- 1..2 do
          assert_receive {:executed_batch, values}, 1_000
          values
        end

      assert results == [
               [{:progress, [1]}, {:item, 1}],
               [{:progress, [2]}, {:item, 2}],
               [{:progress, [3]}, {:item, 3}],
               [{:progress, [4]}, {:item, 4}],
               [{:progress, [5]}, {:item, 5}],
               [{:progress, [6]}, {:item, 6}]
             ]

      assert executed_batches == [[1, 2, 3], [4, 5, 6]]
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
                       BatchServing.dispatch_many!(PartitionedServing, batch)
            end,
            max_concurrency: 4
          )
          |> Enum.map(fn {:ok, results} -> results end)
          |> Enum.to_list()
        end)

      time_in_seconds = time_in_microseconds / 1_000_000
      assert time_in_seconds < 2.1
    end

    test "dispatch_many returns ok for successful requests" do
      {:ok, _pid} =
        start_supervised(%{id: BatchServing.PG, start: {:pg, :start_link, [BatchServing.PG]}})

      {:ok, _pid} =
        start_supervised(
          {BatchServing,
           serving: square_serving(), name: SafeServing, batch_size: 10, batch_timeout: 100}
        )

      assert {:ok, [4, 9]} = BatchServing.dispatch_many(SafeServing, [2, 3])
    end

    test "dispatch returns error for missing serving" do
      assert {:error, _reason} = BatchServing.dispatch({:local, MissingServing}, 1)
    end

    test "dispatch_many returns error tuple for missing serving instead of exiting" do
      assert {:error, _reason} = BatchServing.dispatch_many({:local, MissingServing}, [1, 2])
    end

    test "dispatch_many!/dispatch! exits for missing serving" do
      reason = catch_exit(BatchServing.dispatch!({:local, MissingServing}, 2))
      assert {:noproc, _details} = reason

      reason = catch_exit(BatchServing.dispatch!(MissingServing, 2))
      assert {:noproc, _details} = reason

      reason = catch_exit(BatchServing.dispatch_many!({:local, MissingServing}, [1, 2]))
      assert {:noproc, _details} = reason

      reason = catch_exit(BatchServing.dispatch_many!(MissingServing, [1, 2]))
      assert {:noproc, _details} = reason
    end
  end
end
