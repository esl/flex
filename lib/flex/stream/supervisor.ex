defmodule Flex.Stream.Supervisor do
  @moduledoc false
  # Supervisor for Flex HTTP Stream backend GenServers

  use Supervisor

  alias Flex.Stream.Worker

  # API functions

  def new_worker(url) do
    Supervisor.start_child(__MODULE__, [url])
  end

  def start_link do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init(_) do
    children = [
      worker(Worker, [], restart: :temporary)
    ]
    supervise(children, strategy: :simple_one_for_one)
  end

end
