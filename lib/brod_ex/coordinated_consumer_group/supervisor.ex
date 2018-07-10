defmodule BrodEx.CoordinatedConsumerGroup.Supervisor do

  use Supervisor

  alias BrodEx.CoordinatedConsumerGroup

  def child_spec(%{group_id: group_id, topic: _topic, handler_mod: _handler_mod} = args) do
    name = process_name(group_id)
    %{
      id: name,
      start: {__MODULE__, :start_link, [args, name: name]}
    }
  end

  def start_link(%{
    group_id: _group_id,
    topic: _topic,
    handler_mod: _handler_mod
  } = args, opts) do
    Supervisor.start_link(__MODULE__, args, opts)
  end

  def init(args) do
    children = [
      {Member, [args]}
    ]
    Supervisor.init(children, strategy: :one_for_all)
  end

  def name(group_id) do
    Module.concat(BrodEx.CoordinatedGroupSupervisor, :"#{group_id}")
  end

end

