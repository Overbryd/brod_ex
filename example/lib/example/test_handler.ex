defmodule TestHandler do
  @behaviour BrodEx.MessageHandler

  def call(%BrodEx.Message{value: "commit"}), do: :commit
    :commit
    :more
    :ok
  end

  def handle_error() do
  end
end
