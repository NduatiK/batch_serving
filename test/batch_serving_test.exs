defmodule BatchServingTest do
  use ExUnit.Case

  test "simple inline" do
    serving = BatchServing.new(fn _opts -> fn a -> Enum.map(a.stack, &(&1 * &1)) end end)
    batch = BatchServing.Batch.stack([1, 2, 3, 4])
    assert [1, 4, 9, 16] == BatchServing.run(serving, batch)
  end

  test "pre/post-processing inline" do
    serving =
      BatchServing.new(fn _opts -> fn a -> Enum.map(a.stack, &(&1 * &1)) end end)
      |> BatchServing.client_preprocessing(fn input -> {input, :client_info} end)
      |> BatchServing.client_postprocessing(&{&1, &2})

    batch = BatchServing.Batch.stack([1, 2, 3, 4])

    assert {{[1, 4, 9, 16], :server_info}, :client_info} ==
             BatchServing.run(serving, batch)
  end

  test "batched run" do
    {:ok, _pid} =
      start_supervised(%{id: Serving.PG, start: {:pg, :start_link, [Serving.PG]}})

    {:ok, _pid} =
      start_supervised(
        {BatchServing,
         serving: BatchServing.new(fn _opts -> fn a -> Enum.map(a.stack, &(&1 * &1)) end end),
         name: MyServing,
         batch_size: 10,
         batch_timeout: 100}
      )

    batch1 = BatchServing.Batch.stack([1, 2, 3])
    batch2 = BatchServing.Batch.stack([4, 5])

    assert [1, 4, 9, 16, 25] ==
             BatchServing.batched_run(MyServing, [batch1, batch2])
  end

  test "batched run with stream" do
    {:ok, _pid} =
      start_supervised(%{id: Serving.PG, start: {:pg, :start_link, [Serving.PG]}})

    {:ok, _pid} =
      start_supervised(
        {BatchServing,
         serving:
           BatchServing.new(fn _opts ->
             fn a ->
               Enum.map(a.stack, &(&1 * &1))
             end
           end)
           |> BatchServing.client_preprocessing(fn input ->
             input
             |> Stream.flat_map(fn a ->
               if match?(%Stream{}, a) do
                 a
               else
                 List.wrap(a)
               end
             end)
             |> Stream.flat_map(& &1.stack)
             |> Stream.chunk_every(2)
             |> Stream.map(&BatchServing.Batch.stack(&1))
             |> Enum.to_list()
             |> Stream.map(& &1)
             |> then(&{&1, :client_info})
           end),
         name: MyServing,
         batch_size: 2,
         batch_timeout: 100}
      )

    assert [[1, 4, 9], [1, 4, 9], [1, 4, 9], [1, 4, 9]] =
             Task.async_stream(
               1..4,
               fn _ ->
                 #  data = Stream.map([BatchServing.Batch.stack([1, 2, 3])], & &1)
                 data = BatchServing.Batch.stack([1, 2, 3])

                 BatchServing.batched_run(MyServing, [data])
               end,
               max_concurrency: 2
             )
             |> Enum.map(fn {:ok, results} -> results end)
             |> Enum.to_list()
  end

  test "batched run with stream(2)" do
    {:ok, _pid} =
      start_supervised(%{id: Serving.PG, start: {:pg, :start_link, [Serving.PG]}})

    {:ok, _pid} =
      start_supervised(
        {BatchServing,
         serving:
           BatchServing.new(fn _opts ->
             fn a ->
               Enum.map(a.stack, &(&1 * &1))
             end
           end),
         name: MyServing,
         batch_size: 2,
         batch_timeout: 100}
      )

    submit_work = fn num ->
      data = BatchServing.Batch.stack([num])
      hd(BatchServing.batched_run(MyServing, data))
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
        :double, _opts -> &Enum.map(&1.stack, fn v -> v * 2 end)
        :half, _opts -> &Enum.map(&1.stack, fn v -> v / 2 end)
      end)

    double_batch =
      BatchServing.Batch.concatenate([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
      |> BatchServing.Batch.key(:double)

    assert [0, 2, 4, 6, 8, 10, 12, 14, 16, 18] == BatchServing.run(serving, double_batch)

    half_batch =
      BatchServing.Batch.concatenate([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
      |> BatchServing.Batch.key(:half)

    assert [0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5] ==
             BatchServing.run(serving, half_batch)
  end

  defmodule ServingModule do
    @behaviour BatchServing

    @impl true
    def init(_inline_or_process, :unused_arg, [_options]) do
      {:ok, fn a -> Enum.map(IO.inspect(a.stack), &(&1 * &1)) end}
    end

    @impl true
    def handle_batch(batch, 0, function) do
      {:execute, fn -> {function.(batch), :server_info} end, function}
    end
  end

  test "module" do
    {:ok, _pid} =
      start_supervised(%{id: Serving.PG, start: {:pg, :start_link, [Serving.PG]}})

    {:ok, _pid} =
      start_supervised(
        {BatchServing,
         serving: BatchServing.new(ServingModule, :unused_arg),
         name: MyServing,
         batch_size: 3,
         batch_timeout: 100}
      )

    batch1 = BatchServing.Batch.stack([1, 2, 3])
    batch2 = BatchServing.Batch.stack([4, 5])

    assert [1, 4, 9, 16, 25] ==
             BatchServing.batched_run(MyServing, [batch1, batch2])
  end
end
