defmodule BrodEx.GroupWorker do
  @moduledoc """
  This GenServer calls into its handler module, an evaluates the return, and
  reacts accordingly to its group coordinator.
  """

  use GenServer

  require Logger
  require Record
  import Record, only: [defrecord: 2, extract: 2]

  defrecord :kafka_message_set, extract(:kafka_message_set, from_lib: "brod/include/brod.hrl")
  defrecord :kafka_message, extract(:kafka_message, from_lib: "brod/include/brod.hrl")

  def start_link(%{
    group_id: _group_id,
    group_coordinator_pid: _group_coordinator_pid,
    current_gen_id: _current_gen_id,
    topic: _topic,
    partition: _partition,
    handler_mod: _handler_mod,
    consumer_options: _consumer_options
  } = args) do
    id = worker_id(args)
    GenServer.start_link(__MODULE__, args, name: id, id: id)
  end

  def worker_id(%{group_id: group_id, topic: topic, partition: partition}) do
    :"kafka_coordinated_worker__#{group_id}__#{topic}__#{partition}"
  end

  def init(%{
    group_coordinator_pid: group_coordinator_pid,
    current_gen_id: current_gen_id,
    topic: topic,
    partition: partition,
    handler_mod: handler_mod,
    consumer_options: consumer_options
  }) do
    send(self(), :subscribe)
    state = %{
      group_coordinator_pid: group_coordinator_pid,
      gen_id: current_gen_id,
      topic: topic,
      partition: partition,
      handler_mod: handler_mod,
      consumer_pid: nil,
      consumer_options: consumer_options,
      retries_remaining: 3
    }
    {:ok, state}
  end

  def handle_info(
    {_pid, {:kafka_message_set, topic, partition, _high_wm_offset, message_set}},
    state
  ) do
    ^topic = state.topic
    ^partition = state.partition

    Enum.map(message_set, &(message_map(&1, state)))
    |> consume_messages(state)
    {:noreply, state}
  end

  def handle_info(
    {:DOWN, _ref, _process, pid, reason},
    %{consumer_pid: consumer_pid} = state)
  when pid == consumer_pid do
    {:stop, {:shutdown, {:consumer_down, reason}}, state}
  end

  def handle_info(:subscribe, %{topic: topic, partition: partition, consumer_options: consumer_options} = state) do
    case :brod.subscribe(client_id(), self(), topic, partition, consumer_options) do
      {:ok, pid} ->
        Process.monitor(pid)
        {:noreply, %{state | consumer_pid: pid}}
      {:error, reason} ->
        subscribe_error(reason, state)
    end
  end

  def handle_info({:retry_messages, [%{offset: offset} | _] = messages}, state) do
    Logger.info("event#retry starting from #{state.topic}/#{state.partition}@{#{offset}} messages_count=#{Enum.count(messages)}")
    consume_messages(messages, state)
    {:noreply, state}
  end

  def handle_info(unknown, state) do
    Logger.warn "event#unknown_message=#{inspect self()} reason=#{inspect unknown}"
    {:noreply, state}
  end

  def subscribe_error(reason, %{retries_remaining: retries_remaining} = state) when retries_remaining > 0 do
    Logger.warn "event#subscribe_error=#{inspect self()} reason=#{inspect reason}"
    Process.send_after(self(), :subscribe, subscription_retry_delay())
    {:noreply, %{state | retries_remaining: retries_remaining - 1}}
  end

  def subscribe_error(reason, state) do
    Logger.error "event#subscribe_error=#{inspect self()} reason=#{inspect reason}"
    {:stop, {:subscribe_failed, :retries_exceeded, reason}, state}
  end

  def consume_messages([%{offset: offset} = message | tl], state) do
    case state.handler_mod.handle_message(message) do
      {:ok, :ack} -> ack_offset(offset, state)
      :ok -> nil
    end
    consume_messages(tl, state)
  rescue
    error ->
      handle_consumer_error(error, message, tl, state)
  end

  def consume_messages([], _), do: nil

  def message_map({:kafka_message, offset, _, _, _, value, _, _, _}, state) do
    %{
      value: value,
      topic: state.topic,
      partition: state.partition,
      offset: offset,
      worker_pid: self(),
      kafka_message: kafka_message,
      retry_backoff: nil
    }
  end

  def handle_consumer_error(error, %{offset: offset} = message, remaining_messages, state) do
    log = "event#error consuming from #{state.topic}/#{state.partition}@{#{offset}}"
    Logger.error(log <> "\n" <> Exception.format(:error, error, System.stacktrace()))

    if function_exported?(state.handler_mod, :handle_error, 2) do
      case state.handler_mod.handle_error(error, message) do
        :retry -> retry(message, remaining_messages, state)
        _ -> nil
      end
    end
  end

  def retry(%{retry_backoff: retry_backoff} = message, remaining, state) do
    backoff = case retry_backoff do
      nil -> message_retry_initial_backoff()
      backoff -> backoff
    end
    next_backoff = min(backoff * backoff, message_retry_max_backoff())
    message = Map.put(message, :retry_backoff, next_backoff)
    messages = [message | remaining]

    Process.send_after(self(), {:retry_messages, messages}, backoff)
  end

  def ack_offset(
    offset,
    %{
      group_coordinator_pid: group_coordinator_pid,
      gen_id: gen_id,
      topic: topic,
      partition: partition,
      consumer_pid: consumer_pid
    }
  ) do
    :ok = :brod_group_coordinator.ack(group_coordinator_pid, gen_id, topic, partition, offset)
    # Request more messages from the consumer
    :ok = :brod.consume_ack(consumer_pid, offset)
  end

  defp subscription_retry_delay(), do: 10_000

  defp message_retry_max_backoff(), do: 60_000

  defp message_retry_initial_backoff(), do: 250

  defp client_id(), do: :brod_client_1
end
