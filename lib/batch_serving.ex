defmodule BatchServing do
  @moduledoc """
  BatchServing batches work submitted by concurrent callers and executes it
  as a single request based on batch size and timeout limits.

  You can execute a serving inline with `inline/2`, or start a serving process and
  submit requests with `dispatch/2` for transparent cross-caller batching.

  Callbacks:

    * `map_inputs/2` - map caller input into a list of values (or stream of values)
    * `map_results/2` - map serving output into caller-facing result
  """

  alias __MODULE__

  @doc false
  @enforce_keys [:module, :arg]
  defstruct [
    :module,
    :arg,
    :map_inputs,
    :map_results,
    :streaming,
    :batch_size,
    distributed_postprocessing: &Function.identity/1,
    process_options: [],
    runtime_options: []
  ]

  @type mapped_input() :: list() | Enumerable.t(term())
  @type map_inputs() :: (term() -> mapped_input())
  @type map_results() :: (term() -> term())
  @type distributed_preprocessing() :: (term() -> term())
  @type distributed_postprocessing() :: (term() -> term())

  @type t :: %__MODULE__{
          module: atom(),
          arg: term(),
          map_inputs: map_inputs(),
          map_results: map_results(),
          distributed_postprocessing: distributed_postprocessing(),
          process_options: keyword(),
          runtime_options: keyword(),
          streaming: nil | %{hooks: [atom()]},
          batch_size: nil | pos_integer()
        }

  @process_keys [
    :batch_size,
    :batch_timeout,
    :partitions,
    :shutdown,
    :hibernate_after,
    :spawn_opt
  ]

  @doc """
  The callback used to initialize the serving.

  The first argument reveals if the serving is executed inline,
  such as by calling `inline/2`, or started in a serving process.
  The second argument is the serving argument given to `new/2`.
  The third argument is a list of runtime options for each partition.

  It must return `{:ok, state}`, where the `state` can be any term.
  """
  @callback init(type :: :inline | :process, arg :: term(), [runtime_options :: keyword]) ::
              {:ok, state :: term()}

  @doc """
  Receives a batch, a partition, and returns a function to execute the batch.

  In case of serving processes, the function is executed is an
  separate process.
  """
  @callback handle_batch(BatchServing.Batch.t(), partition :: non_neg_integer(), state) ::
              {:execute, (-> term()), state}
            when state: term()

  def create_serving_process_group_spec() do
    %{id: BatchServing.PG, start: {:pg, :start_link, [BatchServing.PG]}}
  end

  @doc """
  Creates a new function serving.

  It expects a one-arity function that receives a list of values.
  """
  def new(function, runtime_options \\ [])

  def new(function, runtime_options)
      when is_function(function, 1) and is_list(runtime_options) do
    new(BatchServing.Default, function, runtime_options)
  end

  def new(module, arg) when is_atom(module) do
    new(module, arg, [])
  end

  @doc """
  Sets the batch size for this serving.

  This batch size is used to split batches given to both `inline/2` and
  `dispatch/2`, enforcing that the batch size never goes over a limit.
  If you only want to batch within the serving process, you must set
  `:batch_size` via `process_options/2` (or on `start_link/1`).

  > #### Why batch on `inline/2`? {: .info}
  >
  > By default, `inline/2` does not place a limit on its input size. It always
  > processes inputs directly within the current process. On the other hand,
  > `dispatch/2` always sends your input to a separate process, which
  > will batch and execute the serving only once the batch is full or a
  > timeout has elapsed.
  >
  > However, in some situations, an input given to `inline/2` needs to be
  > broken into several batches. If we were to very large batches to our
  > computation, the computation could require too much memory. In such
  > cases, setting a batch size even on `inline/2` is beneficial, because
  > BatchServing takes care of splitting a large batch into smaller ones
  > that do not exceed the `batch_size` value.
  """
  def batch_size(%BatchServing{} = serving, batch_size) when batch_size > 0 do
    %{serving | batch_size: batch_size}
  end

  @doc """
  Creates a new module-based serving.

  It expects a module and an argument that is given to its `init`
  callback.

  A third optional argument called `runtime_options` are additional
  runtime options passed to the module.
  """
  def new(module, arg, runtime_options) when is_atom(module) and is_list(runtime_options) do
    runtime_options = Keyword.merge(BatchServing.default_options(), runtime_options)
    %BatchServing{module: module, arg: arg, runtime_options: runtime_options}
  end

  @doc """
  Sets the input mapping function.

  The default implementation:

    * treats list input as one batch of values
    * treats non-list enumerables (such as streams) as a stream of values
    * wraps any other term into a single-item list
  """
  def map_inputs(%BatchServing{} = serving, function)
      when is_function(function, 1) or is_nil(function) do
    %{serving | map_inputs: function}
  end

  @doc """
  Sets the result mapping function.
  """
  def map_results(%BatchServing{} = serving, function)
      when is_function(function, 1) or is_nil(function) do
    %{serving | map_results: function}
  end

  @doc """
  Sets the distributed postprocessing function.

  The default implementation is `Function.identity/1`.
  """
  def distributed_postprocessing(%BatchServing{} = serving, function)
      when is_function(function, 1) do
    %{serving | distributed_postprocessing: function}
  end

  @doc """
  Configure the serving to stream its results.

  Once `inline/2` or `dispatch/2` are invoked, it will then
  return a stream. The stream must be consumed in the same
  process that calls `inline/2` or `dispatch/2`.

  Batches will be streamed as they arrive. You may also opt-in
  to stream `runtime` hooks.

  ## Options

    * `:hooks` - a list of hook names that will become streaming events

  ## Implementation details

  ### Result mapping

  Once streaming is enabled, the result mapping callback
  will receive a stream which will emit events for each hook
  in the shape of:

      {hook_name, term()}

  The stream will also receive events in the shape of
  `{:batch, output}` as batches are processed by the
  serving. The result mapping function is often expected to call
  `Stream.transform/3` to process those events into something
  usable by callers.

  If the `:hooks` option is given, only a single `:batch` event
  is emitted, at the end, as detailed next.

  ### Batch limits

  If you are streaming hooks, the serving server can no longer break
  batch and you are unable to push a payload bigger than `:batch_size`.
  For example, imagine you have a `batch_size` of 3 and you push three
  batches of two elements (AA, BB, and CC). Without hooks, the batches
  will be consumed as:

      AAB -> BCC

  With streaming, we can't break the batch `BB`, as above, so we will
  consistently pad with zeroes:

      AA0 -> BB0 -> CC0

  In practice, this should not be a major problem, as you should
  generally avoid having a batch size that is not a multiple of the
  most common batches.
  """
  def streaming(%BatchServing{} = serving, opts \\ []) do
    hooks = Keyword.get(opts, :hooks, [])

    if serving.streaming do
      raise ArgumentError, "serving is already marked as streaming"
    end

    %{serving | streaming: %{hooks: hooks}}
  end

  @doc """
  Sets the process options of this serving.

  These are the same options as supported on `start_link/1`,
  except `:name` and `:serving` itself.
  """
  def process_options(%BatchServing{} = serving, opts) when is_list(opts) do
    %{serving | process_options: Keyword.validate!(opts, @process_keys)}
  end

  @doc """
  Sets runtime options for this serving.
  """
  def runtime_options(%BatchServing{} = serving, runtime_options) when is_list(runtime_options) do
    %{serving | runtime_options: runtime_options}
  end

  def default_options() do
    []
  end

  @doc """
  Runs `serving` for a single item inline with the current process.
  """
  def inline(%BatchServing{} = serving, item) do
    [result] = do_run(serving, item, :single)
    result
  end

  @doc """
  Runs `serving` for explicit batch input inline with the current process.
  """
  def inline_many(%BatchServing{} = serving, batch_input) when is_list(batch_input) do
    do_run(serving, batch_input, :batch)
  end

  def inline_many(%BatchServing{} = serving, %Stream{} = batch_input) do
    do_run(serving, batch_input, :batch)
  end

  defp do_run(%BatchServing{} = serving, input, mode) do
    %{
      module: module,
      arg: arg,
      map_inputs: preprocessing,
      map_results: postprocessing,
      runtime_options: runtime_options,
      streaming: streaming,
      batch_size: limit
    } = serving

    batch_or_stream = handle_preprocessing(preprocessing, input, mode)
    {pid_ref, runtime_options} = run_streaming(streaming, runtime_options, batch_or_stream, limit)
    stream = run_batch_or_stream(batch_or_stream, limit)

    execution_result =
      case pid_ref do
        {pid, ref} ->
          send(pid, {ref, module, arg, runtime_options, stream})
          receive_stream("inline/2", ref, :unknown)

        nil ->
          stream
          |> Enum.map_reduce(nil, fn %BatchServing.Batch{size: size} = batch, cache ->
            {:ok, state} = cache || handle_init(module, :inline, arg, [runtime_options])

            {{run_execute(batch, module, state), size}, {:ok, state}}
          end)
          |> elem(0)
          |> Enum.map(&elem(&1, 0))
          |> case do
            [single] -> single
            all -> all
          end
      end

    handle_postprocessing(postprocessing, execution_result)
  end

  defp run_streaming(nil, runtime_options, _batch_or_stream, _limit),
    do: {nil, runtime_options}

  defp run_streaming(%{hooks: []}, runtime_options, _batch_or_stream, _limit),
    do: {run_streaming(), runtime_options}

  defp run_streaming(%{hooks: hooks}, runtime_options, batch_or_stream, limit) do
    size =
      case batch_or_stream do
        %BatchServing.Batch{size: size} ->
          if limit == nil or size <= limit do
            size
          else
            raise ArgumentError,
                  "batch size (#{size}) cannot exceed BatchServing server batch size of #{limit} when streaming hooks"
          end

        _ ->
          raise ArgumentError,
                "streaming hooks do not support input streaming; map_inputs must produce a single value list"
      end

    {pid, ref} = run_streaming()

    runtime_options =
      update_in(runtime_options[:hooks], fn acc ->
        Enum.reduce(hooks, acc || %{}, fn hook, acc ->
          Map.put(acc, hook, &run_hook(ref, size, &1, hook))
        end)
      end)

    {{pid, ref}, runtime_options}
  end

  defp run_streaming do
    pid =
      spawn_link(fn ->
        receive do
          {ref, module, arg, runtime_options, stream} ->
            Enum.reduce(stream, {0, nil}, fn
              %BatchServing.Batch{size: size} = batch, {start, cache} ->
                {:ok, state} = cache || handle_init(module, :inline, arg, [runtime_options])

                output = run_execute(batch, module, state)
                send(ref, {ref, {:batch, {0, size, output}}})
                {start + size, {:ok, state}}
            end)
        end
      end)

    # {pid, Process.monitor(pid, alias: :demonitor)}
    {pid, :erlang.monitor(:process, pid, alias: :demonitor)}
  end

  defp run_hook(ref, size, result, hook) do
    send(ref, {ref, {:hook, {0, size, result, hook}}})
  end

  defp run_batch_or_stream(%BatchServing.Batch{size: size} = batch, limit)
       when is_nil(limit) or size < limit do
    [batch]
  end

  defp run_batch_or_stream(%BatchServing.Batch{} = batch, limit) do
    Stream.unfold(batch, fn
      %BatchServing.Batch{size: size} = batch when size > limit ->
        BatchServing.Batch.split(batch, limit)

      %BatchServing.Batch{} = batch ->
        {batch, :done}

      :done ->
        nil
    end)
  end

  defp run_batch_or_stream(stream, limit) do
    Stream.each(stream, fn
      %BatchServing.Batch{size: size} when is_nil(limit) or size <= limit ->
        :ok

      other ->
        raise "mapped input produced an invalid batch" <>
                if(limit, do: " of maximum size #{limit}", else: "") <> ", got: #{inspect(other)}"
    end)
  end

  defp run_execute(batch, module, state) do
    {:execute, function, _} = handle_batch(module, batch, 0, state)

    :telemetry.span([:batch_serving, :serving, :execute], %{module: module}, fn ->
      output = handle_executed(module, function.())
      {output, %{module: module}}
    end)
  end

  ## Process API

  @spec child_spec(maybe_improper_list()) :: %{
          id: atom(),
          start: {BatchServing, :start_link, [maybe_improper_list(), ...]},
          type: :supervisor
        }
  @doc false
  def child_spec(opts) when is_list(opts) do
    name = opts[:name]

    if name == nil or not is_atom(name) do
      raise ArgumentError,
            ":name option is expected when starting BatchServing and must be an atom"
    end

    opts[:serving] ||
      raise ArgumentError, ":serving option is expected when starting Serving"

    %{
      id: name,
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor
    }
  end

  @doc """
  Starts a `BatchServing` process to batch requests for a given serving.

  ## Options

  All options, except `:name` and `:serving`, can also be set via
  `process_options/2`.

    * `:name` - an atom with the name of the process

    * `:serving` - a `BatchServing` struct with the serving configuration

    * `:batch_size` - the maximum batch size. A default value can be set with
      `batch_size/2`, which applies to both `inline/2` and `dispatch/2`.
      Setting this option only affects `dispatch/2` and it defaults to `1`
      if none is set.

    * `:batch_timeout` - the maximum time to wait, in milliseconds,
      before executing the batch (defaults to `100`ms)

    * `:partitions` - The number of partitions (defaults to `1`)

    * `:shutdown` - the maximum time for the serving to shutdown. This will
      block until the existing computation finishes (defaults to `30_000`ms)

    * `:hibernate_after` and `:spawn_opt` - configure the underlying serving
      workers (see `GenServer.start_link/3`)
  """
  def start_link(opts) do
    opts = Keyword.validate!(opts, [:name, :serving] ++ @process_keys)
    name = Keyword.fetch!(opts, :name)
    serving = Keyword.fetch!(opts, :serving)
    opts = Keyword.merge(serving.process_options, opts)

    serving_batch_size = serving.batch_size
    opts_batch_size = opts[:batch_size]

    batch_size =
      if serving_batch_size && opts_batch_size && serving_batch_size != opts_batch_size do
        raise ArgumentError,
              "the batch size set via BatchServing.batch_size/2 (#{serving_batch_size}) " <>
                "does not match the batch size given to the serving process (#{opts_batch_size})"
      else
        serving_batch_size || opts_batch_size || 1
      end

    shutdown = Keyword.get(opts, :shutdown, 30_000)
    partitions = Keyword.get(opts, :partitions, 1)
    batch_timeout = Keyword.get(opts, :batch_timeout, 100)
    process_options = Keyword.take(opts, [:name, :hibernate_after, :spawn_opt])

    supervisor = Module.concat(name, "Supervisor")
    task_supervisor = Module.concat(name, "TaskSupervisor")
    arg = {name, serving, partitions, batch_size, batch_timeout, task_supervisor}

    children = [
      {Task.Supervisor, name: task_supervisor},
      %{
        id: __MODULE__,
        start: {GenServer, :start_link, [__MODULE__, arg, process_options]},
        shutdown: shutdown
      }
    ]

    Supervisor.start_link(children, strategy: :one_for_all, max_restarts: 0, name: supervisor)
  end

  @doc """
  Runs a single item on the serving process given by `name`.
  """
  def dispatch(name, input, distributed_preprocessing \\ &Function.identity/1)

  def dispatch(name, input, distributed_preprocessing)
      when is_atom(name) and not is_list(input) do
    [v] =
      if pid = Process.whereis(name) do
        local_batched_run!(pid, name, input, :single)
      else
        distributed_batched_run!(name, input, distributed_preprocessing, :single)
      end

    v
  end

  def dispatch(name, _input, _distributed_preprocessing) when is_atom(name) do
    raise ArgumentError,
          "dispatch/3 accepts a single item; use dispatch_many/3 for explicit batches"
  end

  def dispatch({:local, name}, input, _distributed_preprocessing)
      when is_atom(name) and not is_list(input) do
    pid =
      Process.whereis(name) || exit({:noproc, {__MODULE__, :local_batched_run, [name, input]}})

    [v] = local_batched_run!(pid, name, input, :single)
    v
  end

  def dispatch({:local, _name}, _input, _distributed_preprocessing) do
    raise ArgumentError,
          "dispatch/3 accepts a single item; use dispatch_many/3 for explicit batches"
  end

  def dispatch({:distributed, name}, input, distributed_preprocessing)
      when is_atom(name) and not is_list(input) do
    [v] = distributed_batched_run!(name, input, distributed_preprocessing, :single)
    v
  end

  def dispatch({:distributed, _name}, _input, _distributed_preprocessing) do
    raise ArgumentError,
          "dispatch/3 accepts a single item; use dispatch_many/3 for explicit batches"
  end

  @doc """
  Runs explicit batch input on the serving process given by `name`.
  """
  def dispatch_many!(name, batch_input, distributed_preprocessing \\ &Function.identity/1)

  def dispatch_many!(name, batch_input, distributed_preprocessing) when is_atom(name) do
    if pid = Process.whereis(name) do
      local_batched_run!(pid, name, batch_input, :batch)
    else
      distributed_batched_run!(name, batch_input, distributed_preprocessing, :batch)
    end
  end

  def dispatch_many!({:local, name}, batch_input, _distributed_preprocessing)
      when is_atom(name) do
    pid =
      Process.whereis(name) ||
        exit({:noproc, {__MODULE__, :local_batched_run_batch, [name, batch_input]}})

    local_batched_run!(pid, name, batch_input, :batch)
  end

  def dispatch_many!({:distributed, name}, batch_input, distributed_preprocessing)
      when is_atom(name) do
    distributed_batched_run!(name, batch_input, distributed_preprocessing, :batch)
  end

  @doc """
  Safe variant of `dispatch/3` that does not exit on runtime failures.

  Returns `{:ok, result}` or `{:error, reason}`.
  """
  def dispatch_safe(name, input, distributed_preprocessing \\ &Function.identity/1) do
    {:ok, dispatch(name, input, distributed_preprocessing)}
  catch
    :exit, reason -> {:error, reason}
  end

  @doc """
  Safe variant of `dispatch_many/3` that does not exit on runtime failures.

  Returns `{:ok, result}` or `{:error, reason}`.
  """
  def dispatch_many(name, batch_input, distributed_preprocessing \\ &Function.identity/1) do
    {:ok, dispatch_many!(name, batch_input, distributed_preprocessing)}
  catch
    :exit, reason -> {:error, reason}
  end

  defp local_batched_run!(pid, name, input, mode) do
    case local_batched_run(pid, name, input, mode) do
      {:ok, result} -> result
      {:DOWN, reason} -> exit({reason, {__MODULE__, :local_batched_run, [name, input]}})
    end
  end

  defp local_batched_run(pid, name, input, input_mode) do
    %{
      preprocessing: preprocessing,
      postprocessing: postprocessing,
      limit: limit,
      mode: serving_mode
    } =
      :persistent_term.get(persistent_key(name), nil) ||
        raise(
          ArgumentError,
          "could not find BatchServing with name #{inspect(name)}. " <>
            "Make sure your BatchServing is running and/or started as part of your supervision tree"
        )

    preprocessed = handle_preprocessing(preprocessing, input, input_mode)

    ref = :erlang.monitor(:process, pid, alias: :demonitor)
    # ref = Process.monitor(pid, alias: :demonitor)

    size_or_unknown =
      case preprocessed do
        %BatchServing.Batch{size: size} = batch ->
          if serving_mode == :hooks and batch.size > limit do
            batch
            |> run_batch_or_stream(limit)
            |> Enum.each(fn split_batch ->
              Process.send(pid, {__MODULE__, :dispatch, [ref], split_batch}, [:noconnect])
            end)
          else
            Process.send(pid, {__MODULE__, :dispatch, [ref], batch}, [:noconnect])
          end

          size

        stream ->
          if serving_mode == :hooks do
            raise ArgumentError,
                  "streaming hooks do not support input streaming; map_inputs must produce a single value list"
          end

          spawn_link(fn ->
            # We also need to monitor the streaming process. To avoid leaking
            # messages in the parent inbox, we ask the serving to do it.
            Process.send(pid, {__MODULE__, :proxy_monitor, self(), ref}, [:noconnect])
            monitor_ref = Process.monitor(pid)

            pending =
              Enum.reduce(stream, 0, fn
                %BatchServing.Batch{size: size} = batch, acc when size <= limit ->
                  refs = [ref, self()]
                  Process.send(pid, {__MODULE__, :dispatch, refs, batch}, [:noconnect])
                  acc + size

                other, _acc ->
                  raise "mapped input produced an invalid batch of maximum size #{limit}, " <>
                          "got: #{inspect(other)}"
              end)

            receive_size(monitor_ref, ref, pending)
          end)

          :unknown
      end

    case serving_mode do
      :execute ->
        case receive_execute(ref, size_or_unknown) do
          {:ok, value} ->
            {:ok, handle_postprocessing(postprocessing, value)}

          {:DOWN, reason} ->
            {:DOWN, reason}
        end

      _ ->
        stream = receive_stream("dispatch/2", ref, size_or_unknown)
        {:ok, handle_postprocessing(postprocessing, stream)}
    end
  end

  defp distributed_batched_run!(name, input, distributed_callback, mode) do
    distributed_batched_run_with_retries!(name, distributed_callback.(input), 3, mode)
  end

  defp distributed_batched_run_with_retries!(name, input, 0, _mode) do
    exit({:noproc, {__MODULE__, :distributed_batched_run, [name, input, [retries: 0]]}})
  end

  defp distributed_batched_run_with_retries!(name, input, retries, mode) do
    case :pg.get_members(BatchServing.PG, __MODULE__) do
      [] ->
        exit({:noproc, {__MODULE__, :distributed_batched_run, [name, input, [retries: retries]]}})

      entries ->
        pid = Enum.random(entries)
        ref = make_ref()
        args = [self(), ref, name, input, mode]

        {_, monitor_ref} =
          Node.spawn_monitor(node(pid), __MODULE__, :__distributed_batched_run__, args)

        receive do
          {^ref, :streaming} ->
            owner = self()

            Stream.resource(
              fn ->
                if self() != owner do
                  raise "the stream returned from BatchServing.dispatch/2 must be consumed in the same process"
                end

                :ok
              end,
              fn :ok ->
                receive do
                  {^ref, event} ->
                    {[event], :ok}

                  {:DOWN, ^monitor_ref, _, _, {^ref, :streaming}} ->
                    {:halt, :ok}

                  {:DOWN, ^monitor_ref, _, _, reason} ->
                    exit({reason, {BatchServing, :streaming, []}})
                end
              end,
              fn _ -> :ok end
            )

          {:DOWN, ^monitor_ref, _, _, {^ref, result}} ->
            result

          {:DOWN, ^monitor_ref, _, _, :noproc} ->
            distributed_batched_run_with_retries!(name, input, retries - 1, mode)

          {:DOWN, ^monitor_ref, _, _, reason} ->
            exit_args = [name, input, [retries: retries]]
            exit({reason, {__MODULE__, :distributed_batched_run, exit_args}})
        end
    end
  end

  @doc false
  def __distributed_batched_run__(client_pid, ref, name, input, mode) do
    pid = Process.whereis(name) || exit(:noproc)

    case local_batched_run(pid, name, input, mode) do
      {:ok, result} ->
        %{mode: mode, distributed_postprocessing: dist_post} =
          :persistent_term.get(persistent_key(name))

        if mode == :execute do
          exit({ref, dist_post.(result)})
        else
          send(client_pid, {ref, :streaming})
          Enum.each(dist_post.(result), &send(client_pid, {ref, &1}))
          exit({ref, :streaming})
        end

      {:DOWN, reason} ->
        exit(reason)
    end
  end

  ## Client message receiving

  defp receive_size(_monitor, _ref, 0), do: :ok

  defp receive_size(monitor_ref, ref, pending) do
    receive do
      {^ref, size} ->
        receive_size(monitor_ref, ref, pending - size)

      {:DOWN, ^monitor_ref, _, _, reason} ->
        exit(reason)
    end
  end

  defp receive_stream(fun, ref, size) when is_integer(size) or size == :unknown do
    owner = self()

    Stream.resource(
      fn ->
        if self() != owner do
          raise "the stream returned from BatchServing.#{fun} must be consumed in the same process"
        end

        0
      end,
      fn
        ^size ->
          {:halt, :done}

        index ->
          case receive_each(ref, size, index) do
            :done ->
              {:halt, :done}

            {:hook, {hook_start, hook_size, output, hook}} ->
              value = Enum.slice(output, hook_start, hook_size)
              {[{hook, value}], index}

            {:batch, {output_start, output_size, output}} ->
              value = Enum.slice(output, output_start, output_size)
              {[{:batch, value}], index + output_size}

            {:DOWN, reason} ->
              exit({reason, {BatchServing, :streaming, []}})
          end
      end,
      fn _ -> :ok end
    )
  end

  defp receive_execute(ref, size) when is_integer(size) or size == :unknown do
    receive_execute(ref, size, 0, [])
  end

  defp receive_execute(ref, size, index, acc) do
    case receive_each(ref, size, index) do
      :done ->
        {:ok, acc}

      {:batch, {output_start, output_size, output}} ->
        # If we have a single response, slice and return immediately.
        # Otherwise we collect their contents and build the concatenated result later.
        if acc == [] and output_size + index == size do
          {:ok, Enum.slice(output, output_start, output_size)}
        else
          receive_execute(
            ref,
            size,
            index + output_size,
            acc ++ Enum.slice(output, output_start, output_size)
          )
        end

      {:DOWN, reason} ->
        {:DOWN, reason}
    end
  end

  defp receive_each(_ref, size, size) do
    :done
  end

  defp receive_each(ref, size, index) do
    receive do
      {^ref, {:hook, _} = reply} ->
        reply

      {^ref, {:batch, {_output_start, output_size, _output}} = reply} ->
        if output_size + index == size do
          Process.demonitor(ref, [:flush])
        end

        reply

      # The serving itself never finishes with normal reason,
      # but the streaming process does to signal it is concluded
      # and its messages are proxied here.
      {:DOWN, ^ref, _, _, :normal} ->
        Process.demonitor(ref, [:flush])
        :done

      {:DOWN, ^ref, _, _, reason} ->
        # We fake monitor messages, so still demonitor and flush.
        Process.demonitor(ref, [:flush])
        {:DOWN, reason}
    end
  end

  ## Process callbacks

  require Logger
  @behaviour GenServer

  @single_stack_key {__MODULE__, :stack}
  @empty_stack {[], 0, :none}
  @empty_queue :queue.new()
  @timeout_message __MODULE__

  @impl true
  def init({name, serving, partitions, batch_size, batch_timeout, task_supervisor}) do
    Process.flag(:trap_exit, true)
    partitions_opts = serving_partitions(serving, partitions)

    partitions_count = length(partitions_opts)

    {mode, partitions_opts, hooks_table} = serving_streaming(serving, partitions_opts)
    {:ok, module_state} = handle_init(serving.module, :process, serving.arg, partitions_opts)

    :persistent_term.put(
      persistent_key(name),
      %{
        limit: batch_size,
        preprocessing: serving.map_inputs,
        postprocessing: serving.map_results,
        distributed_postprocessing: serving.distributed_postprocessing,
        mode: mode
      }
    )

    :pg.join(BatchServing.PG, __MODULE__, List.duplicate(self(), partitions_count))
    stack_init()

    # We keep batches in a stack. Once the stack is full
    # or it times out, we either execute or enqueue it.
    state = %{
      module: serving.module,
      module_state: module_state,
      limit: batch_size,
      timeout: batch_timeout,
      in_queue: @empty_queue,
      out_queue: Enum.reduce(0..(partitions_count - 1), :queue.new(), &:queue.in/2),
      tasks: [],
      pending_batches: @empty_queue,
      task_supervisor: task_supervisor,
      hooks_table: hooks_table
    }

    {:ok, state}
  end

  defp serving_partitions(%BatchServing{runtime_options: runtime_options}, partitions) do
    List.duplicate(runtime_options, partitions)
  end

  defp serving_streaming(%BatchServing{streaming: nil}, partitions) do
    {:execute, partitions, nil}
  end

  defp serving_streaming(%BatchServing{streaming: %{hooks: []}}, partitions) do
    {:batches, partitions, nil}
  end

  defp serving_streaming(%BatchServing{streaming: %{hooks: hooks}}, partitions) do
    ets = :ets.new(__MODULE__, [:public, :set, read_concurrency: true])

    partitions =
      Enum.with_index(partitions, fn runtime_options, index ->
        update_in(runtime_options[:hooks], fn acc ->
          Enum.reduce(hooks, acc || %{}, fn hook, acc ->
            Map.put(acc, hook, &server_hook(ets, index, hook, &1))
          end)
        end)
      end)

    {:hooks, partitions, ets}
  end

  defp server_hook(ets, index, hook, result) do
    for {[ref | _pids], start, size} <- :ets.lookup_element(ets, index, 2) do
      send(ref, {ref, {:hook, {start, size, result, hook}}})
    end
  end

  @impl true
  def handle_info({__MODULE__, :proxy_monitor, pid, ref}, state) do
    # Process.monitor(pid, tag: {:proxy, ref})
    :erlang.monitor(:process, pid, tag: {:proxy, ref})
    {:noreply, state}
  end

  def handle_info({__MODULE__, :dispatch, refs, %BatchServing.Batch{} = batch}, state) do
    %{limit: limit} = state
    count = stack_count()

    state =
      cond do
        # Single entry takes the whole batch.
        # Execute what we have (if any) and execute a new one.
        batch.size == limit ->
          state
          |> server_execute()
          |> server_stack(refs, batch, :skip_timer)
          |> server_execute()

        # We go over the limit, but if using hooks, we can't split.
        batch.size + count > limit and state.hooks_table != nil ->
          state
          |> server_execute()
          |> server_stack(refs, batch, :set_timer)

        # Split as necessary.
        true ->
          server_stack_and_execute_loop(state, batch, count, refs)
      end

    {:noreply, state}
  end

  def handle_info({@timeout_message, :timeout, ref}, %{out_queue: out_queue} = state) do
    case stack_timer() do
      # We have processing power, so execute it immediately.
      {^ref, _timer_ref} when out_queue != @empty_queue ->
        {:noreply, server_execute(state)}

      # Otherwise we will queue it but keep on increasing the batch.
      {^ref, _timer_ref} ->
        stack_update(fn {[_ | _] = stack, count, _timer} ->
          {stack, count, :done}
        end)

        {:noreply, update_in(state.in_queue, &:queue.in(:pending, &1))}

      # Otherwise this is an old timer message, just ignore it.
      _ ->
        {:noreply, state}
    end
  end

  def handle_info({ref, :done}, %{tasks: tasks} = state) do
    case Enum.split_with(tasks, &(elem(&1, 0).ref == ref)) do
      {[{_task, partition, _ref_sizes}], tasks} ->
        Process.demonitor(ref, [:flush])
        noreply_task_done_and_continue(state, tasks, partition)

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({{:proxy, ref}, _ref, type, info, reason}, state) do
    send(ref, {:DOWN, ref, type, info, reason})
    {:noreply, state}
  end

  def handle_info({:DOWN, ref, :process, _process, reason}, %{tasks: tasks} = state) do
    case Enum.split_with(tasks, &(elem(&1, 0).ref == ref)) do
      {[{_task, partition, ref_sizes}], tasks} ->
        server_reply_down(reason, ref_sizes)
        noreply_task_done_and_continue(state, tasks, partition)

      _ ->
        {:noreply, state}
    end
  end

  def handle_info(msg, state) do
    Logger.warning("Unknown message in Serving: #{inspect(msg)}")
    {:noreply, state}
  end

  @impl true
  def handle_continue(:maybe_task, state) do
    {:noreply, server_maybe_task(state)}
  end

  @impl true
  def terminate(_reason, %{tasks: tasks, pending_batches: pending_batches}) do
    # Emulate the process is gone for entries in the queue.
    for {_batch, ref_sizes} <- :queue.to_list(pending_batches) do
      server_reply_down(:noproc, ref_sizes)
    end

    # As well as for entries in the stack.
    for {[ref | _pids], _batch} <- stack_entries() do
      send(ref, {:DOWN, ref, :process, self(), :noproc})
    end

    # And wait until all current tasks are processed
    for {%Task{ref: ref}, _partition, ref_sizes} <- tasks do
      receive do
        {^ref, :done} -> Process.demonitor(ref, [:flush])
        {:DOWN, ^ref, :process, _, reason} -> server_reply_down(reason, ref_sizes)
      end
    end

    :ok
  end

  # We don't spawn the task here because, if it crashes,
  # we want a checked-in version of the state that knows
  # the current task has finished.
  defp noreply_task_done_and_continue(%{out_queue: out_queue} = state, tasks, partition) do
    out_queue = :queue.in(partition, out_queue)
    {:noreply, %{state | tasks: tasks, out_queue: out_queue}, {:continue, :maybe_task}}
  end

  defp server_reply_down(reason, ref_sizes) do
    for {[ref | _refs], _start, _size} <- ref_sizes do
      send(ref, {:DOWN, ref, :process, self(), reason})
    end
  end

  defp server_stack_and_execute_loop(state, batch, count, refs) do
    %{limit: limit} = state
    %{size: size} = batch

    cond do
      size + count < limit ->
        server_stack(state, refs, batch, :set_timer)

      size + count > limit ->
        {current, batch} = BatchServing.Batch.split(batch, limit - count)

        state
        |> server_stack(refs, current, :skip_timer)
        |> server_execute()
        |> server_stack_and_execute_loop(batch, 0, refs)

      true ->
        state
        |> server_stack(refs, batch, :skip_timer)
        |> server_execute()
    end
  end

  defp server_stack(%{limit: limit} = state, refs, batch, timer_mode) do
    stack_update(fn {stack, count, timer} when batch.size + count <= limit ->
      timer =
        if timer == :none and timer_mode == :set_timer do
          ref = make_ref()
          {ref, Process.send_after(self(), {@timeout_message, :timeout, ref}, state.timeout)}
        else
          timer
        end

      {[{refs, batch} | stack], count + batch.size, timer}
    end)

    state
  end

  defp server_execute(state) do
    if stack_count() == 0 do
      state
    else
      {batch_refs, timer} = stack_to_batch_refs()
      state = update_in(state.pending_batches, &:queue.in(batch_refs, &1))

      state =
        if timer == :done do
          state
        else
          update_in(state.in_queue, &:queue.in(:pending, &1))
        end

      server_maybe_task(state)
    end
  end

  defp server_maybe_task(state) do
    %{out_queue: out_queue, in_queue: in_queue, pending_batches: pending_batches} = state

    with {{:value, partition}, out_queue} <- :queue.out(out_queue),
         {{:value, :pending}, in_queue} <- :queue.out(in_queue) do
      {{batch, ref_sizes}, pending_batches} =
        case :queue.out(pending_batches) do
          {:empty, _pending_batches} ->
            # If there is no entry pending, then we have a timed-out in-construction batch.
            {batch_refs, :done} = stack_to_batch_refs()
            {batch_refs, pending_batches}

          {{:value, batch_refs}, queue} ->
            {batch_refs, queue}
        end

      %{module: module, module_state: module_state, hooks_table: hooks_table} = state
      {:execute, function, module_state} = handle_batch(module, batch, partition, module_state)

      wrapped_function = fn ->
        :telemetry.span([:batch_serving, :serving, :execute], %{module: module}, fn ->
          if hooks_table do
            :ets.insert(hooks_table, {partition, ref_sizes})
          end

          output = function.()

          for {[ref | pids], start, size} <- ref_sizes do
            send(ref, {ref, {:batch, {start, size, output}}})

            for pid <- pids do
              send(pid, {ref, size})
            end
          end

          {:done, %{module: module}}
        end)
      end

      task = Task.Supervisor.async_nolink(state.task_supervisor, wrapped_function)
      tasks = [{task, partition, ref_sizes} | state.tasks]

      %{
        state
        | module_state: module_state,
          tasks: tasks,
          out_queue: out_queue,
          in_queue: in_queue,
          pending_batches: pending_batches
      }
    else
      _ -> state
    end
  end

  ## Stack management
  #
  # The stack is stored in the process dictionary for performance.
  defp stack_init do
    Process.put(@single_stack_key, @empty_stack)
    :ok
  end

  defp stack_count do
    {_stack, count, _timer} = Process.get(@single_stack_key)
    count
  end

  defp stack_timer do
    {_stack, _count, timer} = Process.get(@single_stack_key)
    timer
  end

  defp stack_entries do
    {stack, _count, _timer} = Process.get(@single_stack_key)
    stack
  end

  defp stack_update(fun) do
    Process.put(@single_stack_key, fun.(Process.get(@single_stack_key)))
    :ok
  end

  defp stack_to_batch_refs do
    {[_ | _] = stack, count, timer} = Process.get(@single_stack_key)
    :ok = stack_init()

    with {ref, timer_ref} <- timer do
      Process.cancel_timer(timer_ref)

      receive do
        {@timeout_message, :timeout, ^ref} -> :ok
      after
        0 -> :ok
      end
    end

    {ref_sizes, batches, _} =
      Enum.reduce(stack, {[], [], count}, fn {refs, batch}, {ref_sizes, batches, ending} ->
        size = batch.size
        {[{refs, ending - size, size} | ref_sizes], [batch | batches], ending - size}
      end)

    {{BatchServing.Batch.merge(batches), ref_sizes}, timer}
  end

  ## Shared helpers

  defp persistent_key(name) when is_atom(name) do
    {__MODULE__, name}
  end

  defp handle_init(module, type, arg, [_ | _] = partitions) do
    case module.init(type, arg, partitions) do
      {:ok, _} = pair ->
        pair

      other ->
        raise "#{inspect(module)}.init/3 must return {:ok, state}. Got: #{inspect(other)}"
    end
  end

  defp handle_batch(module, batch, partition, state) do
    case module.handle_batch(batch, partition, state) do
      {:execute, function, _} = pair when is_function(function, 0) ->
        pair

      other ->
        raise "#{inspect(module)}.handle_batch/3 must return {:execute, function, state}, " <>
                "where function is a function that receives no arguments and returns output. " <>
                "Got: #{inspect(other)}"
    end
  end

  defp handle_executed(_module, result), do: result

  defp handle_preprocessing(preprocessing, input, :single) do
    handle_preprocessing(preprocessing, [input], :batch)
  end

  defp handle_preprocessing(nil, batch_input, :batch) do
    mapped_to_batch_or_stream(batch_input)
  end

  defp handle_preprocessing(preprocessing, input, _mode) do
    meta = %{input: input}

    :telemetry.span([:batch_serving, :serving, :preprocessing], meta, fn ->
      mapped = preprocessing.(input)

      batch_or_stream =
        mapped_to_batch_or_stream(mapped) || raise_bad_map_inputs!(preprocessing, mapped)

      {batch_or_stream, meta}
    end)
  end

  defp raise_bad_map_inputs!(preprocessing, result) do
    raise "map_inputs function #{inspect(preprocessing)} must return a list of values, " <>
            "or a stream of values. Got: #{inspect(result)}"
  end

  defp mapped_to_batch_or_stream(values) when is_list(values) do
    mapped_list_to_batch!(values)
  end

  defp mapped_to_batch_or_stream(stream) do
    if Enumerable.impl_for(stream) do
      Stream.map(stream, &mapped_stream_entry_to_batch/1)
    end
  end

  defp mapped_list_to_batch!(values) when is_list(values) do
    if values == [], do: raise(ArgumentError, "cannot inline with empty value list")
    BatchServing.Batch.values(values)
  end

  defp mapped_stream_entry_to_batch(value) do
    BatchServing.Batch.values([value])
  end

  defp handle_postprocessing(nil, result), do: result

  defp handle_postprocessing(postprocessing, result) do
    :telemetry.span([:batch_serving, :serving, :postprocessing], %{}, fn ->
      {postprocessing.(result), %{}}
    end)
  end
end

defmodule BatchServing.Default do
  @moduledoc false
  @behaviour BatchServing

  @impl true
  def init(_type, fun, partitions) do
    batch_funs =
      Enum.with_index(partitions, fn runtime_options, index ->
        _ = runtime_options
        value = fn batch -> fun.(batch.values) end

        {index, value}
      end)

    {:ok, Map.new(batch_funs)}
  end

  @impl true
  def handle_batch(batch, partition, batch_funs) do
    batch_fun = Map.fetch!(batch_funs, partition)

    {:execute, fn -> batch_fun.(batch) end, batch_funs}
  end
end
