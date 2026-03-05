defmodule BatchServingTest do
  use ExUnit.Case

  def square_values(values), do: Enum.map(values, &(&1 * &1))

  describe "inline" do
    test "simple inline" do
      serving = BatchServing.new(&square_values/1)
      assert [1, 4, 9, 16] == BatchServing.run(serving, [1, 2, 3, 4])
    end

    test "input/result mapping inline" do
      serving =
        BatchServing.new(&square_values/1)
        |> BatchServing.map_input(fn input -> input end)
        |> BatchServing.map_result(fn output -> {:ok, output} end)

      assert {:ok, [1, 4, 9, 16]} == BatchServing.run(serving, [1, 2, 3, 4])
    end
  end

  describe "serving process" do
    test "batched run" do
      {:ok, _pid} =
        start_supervised(%{id: BatchServing.PG, start: {:pg, :start_link, [BatchServing.PG]}})

      {:ok, _pid} =
        start_supervised(
          {BatchServing,
           serving: BatchServing.new(fn values -> Enum.map(values, &(&1 * &1)) end),
           name: MyServing,
           batch_size: 10,
           batch_timeout: 100}
        )

      assert [1, 4, 9, 16, 25] == BatchServing.batched_run(MyServing, [[1, 2, 3], [4, 5]])
    end

    test "batched run with stream" do
      {:ok, _pid} =
        start_supervised(%{id: BatchServing.PG, start: {:pg, :start_link, [BatchServing.PG]}})

      {:ok, _pid} =
        start_supervised(
          {BatchServing,
           serving:
             BatchServing.new(fn values ->
               Enum.map(values, &(&1 * &1))
             end)
             |> BatchServing.map_input(fn input ->
               input
               |> List.wrap()
               |> Enum.chunk_every(2)
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
                   BatchServing.batched_run(MyServing, data)
                 end,
                 max_concurrency: 2
               )
               |> Enum.map(fn {:ok, results} -> results end)
               |> Enum.to_list()
    end

    test "batched run with stream(2)" do
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

      submit_work = fn num ->
        hd(BatchServing.batched_run(MyServing, [num]))
      end

      assert [_, _, _, _, _, _] =
               Task.async_stream(
                 1..6,
                 fn num ->
                   submit_work.(num)
                 end,
                 max_concurrency: 4
               )
               |> Enum.map(fn {:ok, results} -> results end)
               |> Enum.to_list()
    end

    test "keys" do
      serving =
        BatchServing.new(fn
          :double, values -> Enum.map(values, fn v -> v * 2 end)
          :half, values -> Enum.map(values, fn v -> v / 2 end)
        end)
        |> BatchServing.map_input(fn {key, values} -> {key, values} end)

      assert [0, 2, 4, 6, 8, 10, 12, 14, 16, 18] ==
               BatchServing.run(serving, {:double, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]})

      assert [0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5] ==
               BatchServing.run(serving, {:half, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]})
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

      assert [1, 4, 9, 16, 25] == BatchServing.batched_run(MyServing, [[1, 2, 3], [4, 5]])
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
              assert [i ** 2, (i + 1) ** 2] == BatchServing.batched_run(PartitionedServing, batch)
            end,
            max_concurrency: 4
          )
          |> Enum.map(fn {:ok, results} -> results end)
          |> Enum.to_list()
        end)

      time_in_seconds = time_in_microseconds / 1_000_000
      assert time_in_seconds < 2.1
    end

    test "batched_run_safe returns ok for successful requests" do
      {:ok, _pid} =
        start_supervised(%{id: BatchServing.PG, start: {:pg, :start_link, [BatchServing.PG]}})

      {:ok, _pid} =
        start_supervised(
          {BatchServing,
           serving: BatchServing.new(fn values -> Enum.map(values, &(&1 * &1)) end),
           name: SafeServing,
           batch_size: 10,
           batch_timeout: 100}
        )

      assert {:ok, [4, 9]} = BatchServing.batched_run_safe(SafeServing, [2, 3])
    end

    test "batched_run_safe returns error for missing serving" do
      assert {:error, _reason} = BatchServing.batched_run_safe({:local, MissingServing}, [1])
    end
  end
end
