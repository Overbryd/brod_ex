defmodule BrodEx.Client do
  @moduledoc """
  Defines a client to interact with a Kafka cluster.

  Similar to an Ecto Repo, you define your own module and configure connection
  and client behaviour in your `config/config.exs.`

  When used the client expects the `:otp_app` as option. The `:otp_app` should point
  to an OTP application that has the Kafka client configuration. For example, the client:

      defmodule MyApp.Kafka do
        use BrodEx.Client, otp_app: :my_app
      end

  Can be configured with:

      config :your_app, YourApp.Kafka,
        endpoints: "localhost:9092"

  The client will transparently handle re-connection to your Kafka cluster.
  """

  @type t :: module

  @doc false
  defmacro __using__(use_opts \\ []) do
    quote bind_quoted: [use_opts: use_opts] do
      @behaviour BrodEx.Client
      @otp_app Keyword.fetch!(use_opts, :otp_app)
      @config Application.get_env(@otp_app, __MODULE__, [])

      def start_link(opts \\ []) do
        client_config = @config
                        |> Keyword.merge(opts)
                        |> BrodEx.Config.brod_client_config()
        clients = Application.get_env(:brod, :clients, [])
                  |> Keyword.merge([{__MODULE__, client_config}])

        Application.put_env(:brod, :clients, clients)

        :brod.start_link_client(client_config[:endpoints], __MODULE__, client_config)
      end

      def child_spec(opts) do
        %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, [opts]},
          type: :worker
        }
      end

      def endpoints do
        @config[:endpoints]
      end

      def produce(value) do
        BrodEx.produce(__MODULE__, value)
      end

      def produce(key, value) do
        BrodEx.produce(__MODULE__, key, value)
      end

      def produce(topic, partition, key, value) do
        BrodEx.produce(__MODULE__, topic, partition, key, value)
      end

      def produce_sync(key, value) do
        BrodEx.produce_sync(__MODULE__, key, value)
      end

      def produce_sync(topic, partition, key, value) do
        BrodEx.produce_sync(__MODULE__, topic, partition, key, value)
      end

      def consume_ack(topic, partition, offset) do
        BrodEx.consume_ack(__MODULE__, topic, partition, offset)
      end

      def metadata(topic) do
        :brod_client.get_metadata(__MODULE__, topic)
      end

      def partitions_count(topic) do
        :brod_client.get_partitions_count(__MODULE__, topic)
      end

      def resolve_offset(topic, partition) do
        BrodEx.resolve_offset(endpoints(), topic, partition)
      end

      def resolve_offset(topic, partition, time) do
        BrodEx.resolve_offset(endpoints(), topic, partition, time)
      end

      def fetch(topic, partition, offset) do
        BrodEx.fetch(endpoints(), topic, partition, offset)
      end

      def fetch(topic, partition, offset, max_wait_time, min_bytes, max_bytes) do
        BrodEx.fetch(endpoints(), topic, partition, offset, max_wait_time, min_bytes, max_bytes)
      end

      defoverridable child_spec: 1
    end
  end

  @doc """
  Starts a `:brod_client` and returns `{:ok, pid}`.

  ## Options

    * `:endpoints` – The `host:port` string(s) of the Kafka broker to connect to.
      Can be given as a list of strings, or just a string that separates multiple `host:port`
      entries with `;` or a ` ` (space).
    * `:reconnect_cool_down_seconds` – the number of seconds to wait between re-connection attempts (default: 10)
    * `:auto_start_producers` – set to false if you want to manually start your `:brod_producer` (default: true)
    * `:default_producer_config` – set default options to be passed to the start of producers (default: [])
  """
  @callback start_link(opts :: Keyword.t) :: {:ok, pid} |
                            {:error, {:already_started, pid}} |
                            {:error, term}
end

