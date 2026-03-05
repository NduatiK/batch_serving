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
    {:batch_serving, "~> 1.0.0"}
  ]
end
```

## Core idea

1. Define a serving function that receives a list of values.
2. Start a serving process with `batch_size` and `batch_timeout`.
3. Use `BatchServing.dispatch/2` for single items and `BatchServing.dispatch_many/2` for explicit batches.
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
BatchServing.dispatch_many!(MyServing, [2, 3])
#=> [4, 9]
BatchServing.dispatch!(MyServing, 2)
#=> 4

BatchServing.dispatch_many!(MyServing, ["2"])
#=> !!!! exit !!!!!
BatchServing.dispatch!(MyServing, "2")
#=> !!!! exit !!!!!

BatchServing.dispatch_many(MyServing, [2, 3])
#=> {:ok, [4, 9]}
BatchServing.dispatch(MyServing, 2)
#=> {:ok, 4}

BatchServing.dispatch_many(MyServing, ["2"])
#=> {:error, _}
BatchServing.dispatch(MyServing, "2")
#=> {:error, _}
```

From multiple concurrent callers:

```elixir
Task.async_stream(1..20, fn n ->
  BatchServing.dispatch(MyServing, n)
end, max_concurrency: 20)
|> Enum.to_list()
```

Each caller gets its own result, but execution is internally batched.

## API overview

- `BatchServing.new/1` - Build a serving from a function.

- `BatchServing.map_inputs/2` - Map complex input into value lists.
- `BatchServing.map_results/2` - Map execution results into caller-facing shapes.

- `BatchServing.inline/2` - Run a single item inline.
- `BatchServing.inline_many/2` - Run an explicit batch inline.
- `BatchServing.dispatch/2` - Send a single item to a serving process for coalesced execution.
- `BatchServing.dispatch_many/2` - Send an explicit batch to a serving process.

## Advanced options

- `partitions: n` - Run multiple partitions for parallel batch execution.
- `streaming/2` - Stream batch results/events.

## Hooks (Streaming Runtime Events)

Hooks are useful when you need live runtime signals in addition to final batch output.

When streaming is enabled:

- `{:batch, output}` events carry result data.
- hook events carry custom telemetry/progress data in the shape `{hook_name, payload}`.

Example:

```elixir
serving =
  BatchServing.new(MyServingModule, :ok)
  |> BatchServing.streaming(hooks: [:progress])

BatchServing.dispatch_many!(MyServing, inputs)
|> Enum.each(fn
  {:progress, meta} -> IO.inspect(meta, label: "progress")
  {:batch, output} -> IO.inspect(output, label: "batch")
end)
```

Practical use case:

- embeddings/indexing pipeline where users need live progress, latency, and cost updates while batches are running.

Multi-user note:

- if a single batch contains items from multiple users, batch-level metrics are not directly user-attributable.
- use one of the three approaches below to keep per-user reporting meaningful.

See detailed guide and examples:

- [Hooks guide with multi-user patterns](docs/hooks.livemd)
- [Livebook demo](docs/liveview_batch_progress_demo.livemd)

## Notes

- Start `BatchServing` early in your supervision tree so dependent processes can use it.
- `inline/2` executes one item immediately in the caller process.
- `inline_many/2` executes one explicit batch immediately in the caller process.
- `dispatch/2` coalesces single-item calls across callers.
- `dispatch_many/2` coalesces explicit batch calls across callers.
