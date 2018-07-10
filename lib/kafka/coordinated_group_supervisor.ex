defmodule BrodEx.CoordinatedConsumerGroup.Supervisor do

  use Supervisor

  alias BrodEx.CoordinatedConsumerGroup

  def child_spec() do
    %{
      id: name,
      start: {__MODULE__, :start_link, [client, opts]}
    }
  end

  def start_link() do
    Supervisor.start_link(__MODULE__, [client, args], opts)
  end

  def init(args) do
    children = [
      {Coordinator, [args]}
    ]
    Supervisor.init(children, strategy: :one_for_all)
  end

  def name(group_id) do
    Module.concat(BrodEx.CoordinatedGroupSupervisor, :"#{group_id}")
  end

end

