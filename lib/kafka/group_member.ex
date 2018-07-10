defmodule Kafka.GroupMember do
  use GenServer

  require Logger

  @behaviour :brod_group_member

  def start_link(%{group_id: _group_id, topic: _topic, handler_mod: _handler_mod} = args) do
    GenServer.start_link(__MODULE__, args, name: name(args))
  end

  def name(%{group_id: group_id}) do
    :"kafka_group_member__#{group_id}"
  end

  def init(%{group_id: group_id, topic: topic, handler_mod: handler_mod}) do
    :ok = :brod.start_consumer(
      :brod_client_1,
      topic,
      prefetch_count: 1,
      offset_reset_policy: :reset_to_earliest
    )
    {:ok, pid} = :brod_group_coordinator.start_link(
      :brod_client_1,
      group_id,
      [topic],
      [],
      __MODULE__,
      self()
    )
    Process.monitor(pid)
    state = %{
      group_id: group_id,
      topic: topic,
      handler_mod: handler_mod,
      group_coordinator_pid: pid,
      current_gen_id: nil,
      workers: []
    }
    {:ok, state}
  end

  def assign_partitions(_pid, _members, _topic_partitions) do
    # Should not receive this
    Logger.warn "event#assign_partitions"
  end

  def get_committed_offsets(_group_member_pid, _topic_partitions) do
    # Should not receive this
    Logger.warn "event#get_committed_offsets"
  end

  def assignments_received(pid, _member_id, generation_id, assignments) do
    GenServer.cast(pid, {:assignments_received, generation_id, assignments})
  end

  def assignments_revoked(pid) do
    GenServer.cast(pid, :assignments_revoked)
  end

  def handle_cast({:assignments_received, gen_id, assignments}, state) do
    Logger.info "event#assignments_received generation_id=#{gen_id}"
    Process.send_after(self(), {:allocate_workers, gen_id, assignments}, 5_000)
    {:noreply, %{state | current_gen_id: gen_id}}
  end

  def handle_cast(:assignments_revoked, state) do
    Logger.info "event#assignments_revoked"
    {:noreply, stop_workers(state)}
  end

  def handle_info(
    {:DOWN, _ref, _process, pid, reason},
    %{group_coordinator_pid: group_coordinator_pid} = state)
  when pid == group_coordinator_pid do
    {:stop, {:shutdown, {:group_coordinator_down, reason}}, state}
  end

  def handle_info(
    {:allocate_workers, gen_id, _assignments},
    %{current_gen_id: current_gen_id} = state) when gen_id < current_gen_id
  do
    # discard old generation
    {:noreply, state}
  end

  def handle_info({:allocate_workers, gen_id, assignments}, state) do
    Logger.info "event#allocate_workers=#{inspect self()} generation_id=#{gen_id}"
    if state.workers != [] do
      stop_workers(state)
    end

    {:noreply, allocate_workers(assignments, state)}
  end

  def allocate_workers(
    [{:brod_received_assignment, topic, partition, offset} | tl],
    %{ workers: workers } = state
  ) do
    {:ok, pid} = Kafka.CoordinatedWorker.start_link(%{
      group_id: state.group_id,
      topic: topic,
      partition: partition,
      group_coordinator_pid: state.group_coordinator_pid,
      current_gen_id: state.current_gen_id,
      handler_mod: state.handler_mod,
      consumer_options: [
        begin_offset: begin_offset(offset)
      ]
    })
    allocate_workers(tl, %{state | workers: [pid | workers]})
  end
  def allocate_workers([], state), do: state

  def stop_workers(%{workers: [pid | tl]} = state) do
    GenServer.stop(pid)
    stop_workers(%{state | workers: tl})
  end
  def stop_workers(%{workers: []} = state), do: state

  def begin_offset(:undefined), do: :earliest
  def begin_offset(offset) when is_integer(offset), do: offset
end
