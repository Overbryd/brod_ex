defmodule BrodEx.GroupCoordinator do
  @moduledoc """
  Implements the brod_group_member behaviour callbacks.

  A Kafka group member manages the consumer offsets for a coordinated consumer group.

  You can either manage the offsets locally, in your own database for example, or you
  choose to let Kafka manage these offsets for you.
  """

  @behaviour :brod_group_member

  def child_spec() do
    %{}
  end

  def start_link(client, %{group_id: group_id, topics: topics}) do
    :brod_group_coordinator.start_link(
      :brod_client_1,
      group_id,
      [topic],
      [],
      __MODULE__,
      self()
    )
  end
end
