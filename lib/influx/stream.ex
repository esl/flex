defmodule Influx.Stream do
  @moduledoc """
  This module exposes API for wrapping HTTP requests into Stream
  """

  alias Influx.Stream.{Worker, Supervisor}

  @doc """
    Returns HTTP response wrapped into Stream.

    ## Example
      iex(1)> Influx.Stream.new_stream("tide-erlang-solutions.com")
      #Function<51.122079345/2 in Stream.resource/3>
  """
  @spec new_stream(String.t) :: Stream.t
  def new_stream(url) do
    Stream.resource(init_fun(url), next_fun(), after_fun())
  end

  # Function called to initialize Stream
  defp init_fun(url) do
    fn ->
      {:ok, pid} = Supervisor.new_worker(url)
      pid
    end
  end

  # Function called where there is demand for new element
  defp next_fun() do
    fn (pid) ->
      case Worker.get_chunk(pid) do
        {:chunk, chunk} -> {[chunk], pid}
        :halt -> {:halt, pid}
      end
    end
  end

  # Function called when stream is finished, used for clean_up
  defp after_fun() do
    fn (pid) ->
      Worker.stop(pid)
    end
  end
end
