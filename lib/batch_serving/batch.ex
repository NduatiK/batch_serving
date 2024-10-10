defmodule BatchServing.Batch do
  @moduledoc """
  Creates a batch of tensors (and containers).

  A batch is lazily traversed, concatenated, and padded upon `defn` invocation.
  """

  @doc """
  A BatchServing.Batch struct.

  The `:size` field is public.
  """
  @enforce_keys [:key]
  @derive {Inspect, only: [:size]}
  defstruct [:key, stack: [], size: 0]

  @type t :: %BatchServing.Batch{
          stack: list(),
          size: non_neg_integer(),
          key: term()
        }

  @doc """
  Returns a new empty batch.
  """
  def new, do: %BatchServing.Batch{key: :default}

  @doc """
  Sets the batch key for the given batch.
  """
  def key(%BatchServing.Batch{} = batch, key) do
    %{batch | key: key}
  end

  @doc """
  Merges two batches.

  The tensors on the left will appear before the tensors on the right.

  The size and padding of both batches are summed. The padding still
  applies only at the end of batch.

  It will raise if the batch templates are incompatible.

  ## Examples

      iex> batch1 = BatchServing.Batch.stack([1, 2, 3])
      iex> batch2 = BatchServing.Batch.concatenate(BatchServing.Batch.stack([4, 5]), [6, 7, 8])
      iex> batch = BatchServing.Batch.merge(batch1, batch2)
      iex> batch.size
      8
      iex> batch.stack
      [1, 2, 3, 4, 5, 6, 7, 8]

  """
  def merge(left, right), do: merge([left, right])

  @doc """
  Merges a list of batches.

  See `merge/2`.
  """
  def merge([]), do: new()

  def merge([%BatchServing.Batch{} = head | tail]) do
    %{stack: stack, size: size, key: head_key} = head

    {stack, size} =
      Enum.reduce(tail, {stack, size}, fn batch, acc ->
        %BatchServing.Batch{stack: stack, size: size, key: tail_key} = batch
        {acc_stack, acc_size} = acc

        if head_key != tail_key do
          raise ArgumentError,
                "cannot merge batches with different batch keys: #{inspect(head_key)} and #{inspect(tail_key)}"
        end

        {acc_stack ++ stack, size + acc_size}
      end)

    %BatchServing.Batch{stack: stack, size: size, key: head_key}
  end

  @doc """
  Splits a batch in two, where the first one has at most `n` elements.

  If there is any padding and the batch is not full, the amount of padding
  necessary will be moved to the first batch and the remaining stays in the
  second batch.

  ## Examples

  iex> batch = BatchServing.Batch.concatenate(BatchServing.Batch.stack([1, 2]), [3, 4, 5])
  iex> {left, right} = BatchServing.Batch.split(batch, 3)
  iex> left.stack
  [1, 2, 3]
  iex> right.stack
  [4, 5]
  """
  def split(%BatchServing.Batch{} = batch, n) when is_integer(n) and n > 0 do
    %{stack: stack, size: size, key: key} = batch

    if n < size do
      {high_priority, low_priority} = Enum.split(stack, n)

      {%{batch | stack: high_priority, size: n},
       %BatchServing.Batch{size: size - n, stack: low_priority, key: key}}
    else
      {batch, %BatchServing.Batch{key: key}}
    end
  end

  @doc """
  Concatenates the given entries to the batch.

  Entries are concatenated based on their first axis.
  If the first axis has multiple entries, each entry
  is added to the size of the batch.

  You can either concatenate to an existing batch
  or skip the batch argument to create a new batch.

  See `stack/2` if you want to stack entries instead
  of concatenating them.

  ## Examples

  If no batch is given, one is automatically created:

      iex> batch = BatchServing.Batch.concatenate([1, 2, 3])
      iex> batch.stack
      [1, 2, 3]

  But you can also concatenate to existing batches:

      iex> batch = BatchServing.Batch.concatenate(BatchServing.Batch.stack([1]), ([2]))
      iex> batch = BatchServing.Batch.concatenate(batch, [3, 4])
      iex> batch.stack
      [1, 2, 3, 4]

  If the first axis has multiple entries, each entry counts
  towards the size of the batch:

      iex> batch = BatchServing.Batch.concatenate(BatchServing.Batch.stack([1, 2]), [3, 4, 5])
      iex> batch.size
      5
      iex> batch.stack
      [1, 2, 3, 4, 5]


  """
  def concatenate(%BatchServing.Batch{} = batch \\ new(), entries) when is_list(entries),
    do: add(batch, entries)

  @doc """
  Stacks the given entries to the batch.

  Each entry counts exactly as a single entry.
  You can either stack to an existing batch
  or skip the batch argument to create a new batch.

  See `concatenate/2` if you want to concatenate entries
  instead of stacking them.

  ## Examples

  If no batch is given, one is automatically created:

      iex> batch = BatchServing.Batch.stack([1, 2, 3])
      iex> batch.size
      3
      iex> batch.stack
      [1, 2, 3]

  But you can also stack an existing batch:

      iex> batch = BatchServing.Batch.stack([1, 2])
      iex> batch = BatchServing.Batch.stack(batch, [3, 4])
      iex> batch.size
      4
      iex> batch.stack
      [1, 2, 3, 4]

  """
  def stack(%BatchServing.Batch{} = batch \\ new(), entries) when is_list(entries),
    do: add(batch, entries)

  defp add(batch, []), do: batch

  defp add(batch, list) do
    %{stack: stack, size: size} = batch
    %{batch | stack: stack ++ list, size: size + Enum.count(list)}
  end
end
