defmodule Flex.Application do
  @moduledoc false

  use Application

  def start(_type, _args) do
    import Supervisor.Spec, warn: false

    children = [
      supervisor(Flex.Stream.Supervisor, [])
    ]

    opts = [strategy: :one_for_one, name: Flex.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
