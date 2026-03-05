defmodule BatchServing.Batch do
  @moduledoc """
  Creates a batch of values.
  """

  @doc """
  A BatchServing.Batch struct.

  The `:size` field is public.
  """
  @derive {Inspect, only: [:size]}
  defstruct values: [], size: 0

  @type t :: %BatchServing.Batch{
          values: list(),
          size: non_neg_integer()
        }

  @doc """
  Returns a new empty batch.
  """
  def new, do: %BatchServing.Batch{}

  @doc """
  Merges two batches.

  The values on the left will appear before the values on the right.

  The size and padding of both batches are summed. The padding still
  applies only at the end of batch.

  It will raise if the batch templates are incompatible.

  ## Examples

      iex> batch1 = BatchServing.Batch.values([1, 2, 3])
      iex> batch2 = BatchServing.Batch.values(BatchServing.Batch.values([4, 5]), [6, 7, 8])
      iex> batch = BatchServing.Batch.merge(batch1, batch2)
      iex> batch.size
      8
      iex> batch.values
      [1, 2, 3, 4, 5, 6, 7, 8]

  """
  def merge(left, right), do: merge([left, right])

  @doc """
  Merges a list of batches.

  See `merge/2`.
  """
  def merge([]), do: new()

  def merge([%BatchServing.Batch{} = head | tail]) do
    %{values: values, size: size} = head

    {values, size} =
      Enum.reduce(tail, {values, size}, fn batch, acc ->
        %BatchServing.Batch{values: values, size: size} = batch
        {acc_values, acc_size} = acc

        {acc_values ++ values, size + acc_size}
      end)

    %BatchServing.Batch{values: values, size: size}
  end

  @doc """
  Splits a batch in two, where the first one has at most `n` elements.

  If there is any padding and the batch is not full, the amount of padding
  necessary will be moved to the first batch and the remaining stays in the
  second batch.

  ## Examples

    iex> batch = BatchServing.Batch.values(BatchServing.Batch.values([1, 2]), [3, 4, 5])
    iex> {left, right} = BatchServing.Batch.split(batch, 3)
    iex> left.values
    [1, 2, 3]
    iex> right.values
    [4, 5]
  """
  def split(%BatchServing.Batch{} = batch, n) when is_integer(n) and n > 0 do
    %{values: values, size: size} = batch

    if n < size do
      {high_priority, low_priority} = Enum.split(values, n)

      {%{batch | values: high_priority, size: n},
       %BatchServing.Batch{size: size - n, values: low_priority}}
    else
      {batch, %BatchServing.Batch{}}
    end
  end

  @doc """
  Adds the given entries to the batch.

  You can either add values to an existing batch
  or skip the batch argument to create a new batch.

  ## Examples

  If no batch is given, one is automatically created:

      iex> batch = BatchServing.Batch.values([1, 2, 3])
      iex> batch.values
      [1, 2, 3]

  But you can also add values to existing batches:

      iex> batch = BatchServing.Batch.values(BatchServing.Batch.values([1]), [2])
      iex> batch = BatchServing.Batch.values(batch, [3, 4])
      iex> batch.values
      [1, 2, 3, 4]
  """
  def values(%BatchServing.Batch{} = batch \\ new(), entries) when is_list(entries),
    do: add(batch, entries)

  defp add(batch, []), do: batch

  defp add(batch, list) do
    %{values: values, size: size} = batch
    %{batch | values: values ++ list, size: size + Enum.count(list)}
  end
end
