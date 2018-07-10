defmodule Example do
  use Application

  def start(_type, _args) do
    children = [
      Example.BrodClient,
      {BrodEx.CoordinatedConsumerGroup,
        client: Example.BrodClient,
        id: "test-8.consumer",
        topics: ["test-8"],
        handler: Example.TestHandler
      }
    ]

    opts = [strategy: :one_for_one, name: Example.Supervisor]
    Supervisor.start_link(children, opts)
  end

end

