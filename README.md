# BatchServing

`BatchServing` is a fork of `Nx.Serving` focused on generic concurrent batch work.

It lets many callers submit work independently, then transparently groups calls
arriving in the same time window into one batch, executes that batch once, and
returns the right slice of results to each caller.

This is useful when you have high fan-in workloads (for example from web/API
requests, jobs, or pipelines) where per-call execution is expensive but batched
execution is efficient.

## Installation

Add `batch_serving` to your dependencies:

```elixir
def deps do
  [
    {:batch_serving, "~> 0.1.5"}
  ]
end
```

## Core idea

1. Define a serving function that receives a list of values.
2. Start a serving process with `batch_size` and `batch_timeout`.
3. Call `BatchServing.batched_run/2` from many concurrent callers.
4. Calls close together in time are merged and executed once.

## Quick start

### 1) Define and start a serving

```elixir
children = [
  BatchServing.create_serving_process_group_spec(),
  {BatchServing,
   serving: BatchServing.new(fn values ->
     Enum.map(values, &(&1 * &1))
   end),
   name: MyServing,
   batch_size: 10,
   batch_timeout: 100}
]

Supervisor.start_link(children, strategy: :one_for_one)
```

`batch_size` is the max combined size per execution.
`batch_timeout` (ms) is how long to wait for more calls before dispatching.

### 2) Submit work

```elixir
BatchServing.batched_run(MyServing, [2, 3])
#=> [4, 9]
```

From multiple concurrent callers:

```elixir
Task.async_stream(1..20, fn n ->
  hd(BatchServing.batched_run(MyServing, [n]))
end, max_concurrency: 20)
|> Enum.to_list()
```

Each caller gets its own result, but execution is internally batched.

## API overview

- `BatchServing.new/1` - Build a serving from a function.
- `BatchServing.run/2` - Run inline in the current process (no cross-caller batching).
- `BatchServing.batched_run/2` - Send work to a serving process for transparent batching.
- `BatchServing.map_input/2` - Map complex input into value lists (or keyed value lists).
- `BatchServing.map_result/2` - Map execution results into caller-facing shapes.

## Advanced options

- `partitions: n` - Run multiple partitions for parallel batch execution.
- `batch_keys: [...]` - Support distinct queues/functions under one serving name.
- keyed batches through `map_input/2` by returning `{batch_key, values}`.
- `streaming/2` - Stream batch results/events.

## Notes

- Start `BatchServing` early in your supervision tree so dependent processes can use it.
- run/2: executes immediately in the caller process, using only the input you pass right now. It does not wait for or merge work from other processes.
- batched_run/2: sends the request to the serving process, which briefly queues incoming requests and combines multiple callers’ inputs into one batch (up to batch_size or until batch_timeout), then returns each caller’s portion of the result.
