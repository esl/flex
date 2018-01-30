defmodule Influx.Stream.Worker do
  @moduledoc false
  # Worker module for handling chunked HTTP request, used as a backend for
  # Influx.Stream

  use GenServer

  @influx_query_timeout 60_000

  # API

  @spec get_chunk(pid) :: {:chunk, String.t} | :halt
  def get_chunk(name) do
    GenServer.call(name, :get_chunk, @influx_query_timeout)
  end

  @spec start_link(String.t) :: GenServer.on_start()
  def start_link(url) do
    GenServer.start_link(__MODULE__, url)
  end

  @spec stop(pid) :: :ok
  def stop(name) do
    GenServer.stop(name)
  end

  # Callbacks

  def init(url) do
    GenServer.cast(self(), {:initalize, url})
    {:ok, %{id: nil, chunks: nil, reply_to: nil}}
  end

  def handle_cast({:initalize, url}, _state) do
    resp = HTTPoison.get!(url, [], [stream_to: self(),
                                    timeout: @influx_query_timeout,
                                    recv_timeout: @influx_query_timeout])
    {:noreply, %{id: resp.id, chunks: [], more_chunks: true, reply_to: nil}}
  end

  # Server is replaying with chunk, if there is any in accumulator
  def handle_call(:get_chunk, _from, %{chunks: [c | cs]} = state) do
    state = %{state | chunks: cs}
    response = {:chunk, c}
    {:reply, response, state}
  end

  # Server is not replaying, as there is no chunks in accumulator. However
  # we know that there will be more chunks, as we did not received
  # %HTTPostion.AsyncEnd yet
  def handle_call(:get_chunk, from, %{chunks: [], more_chunks: true} = state) do
    state = %{state | reply_to: from}
    {:noreply, state}
  end

  # Server is replaying with `:halt` atom to indicate there will be no more
  # chunks. We know that, because we received %HTTPostion.AsyncEnd
  def handle_call(:get_chunk, _from,
                  %{chunks: [], more_chunks: false} = state) do
    {:reply, :halt, state}
  end

  # Server acumulates new chunk, when there is no client demending chunk.
  def handle_info(%HTTPoison.AsyncChunk{id: id, chunk: chunk},
                  %{id: id, reply_to: nil} = state) do
    {:noreply, %{state | chunks: [chunk | state.chunks]}}
  end

  # Server do not accumulate new chunk, where there is client waiting for
  # chunk. Instead of storing it, chunk is send directly to client.
  def handle_info(%HTTPoison.AsyncChunk{id: id, chunk: chunk},
                  %{id: id, reply_to: pid} = state) do
    GenServer.reply(pid, {:chunk, chunk})
    {:noreply, %{state | reply_to: nil}}
  end

  # If there is end of Stream and there is no client waiting for chunk, server
  # just marks in its state, that it is the end.
  def handle_info(%HTTPoison.AsyncEnd{id: id},
                  %{id: id, reply_to: nil} = state) do
    {:noreply, %{state | more_chunks: false}}
  end

  # If there is end of stream AND thre is nothing in accumulator AND there is
  # client waiting for response we replay with `:halt` to indicate there will be
  # no more chunks.
  def handle_info(%HTTPoison.AsyncEnd{id: id},
                  %{id: id, chunks: [], reply_to: pid} = state) do
    GenServer.reply(pid, :halt)
    state = %{state | more_chunks: false, reply_to: nil}
    {:noreply, state}
  end

  # Handle headers, do nothing
  def handle_info(%HTTPoison.AsyncHeaders{id: id}, %{id: id} = state) do
    {:noreply, state}
  end

  # Handle status, do nothing if successful
  def handle_info(%HTTPoison.AsyncStatus{code: 200, id: id},
                  %{id: id} = state) do
    {:noreply, state}
  end

  # Handle status, stop if response is not 200
  def handle_info(%HTTPoison.AsyncStatus{code: code, id: id},
                  %{id: id} = state) do
    {:stop, {:bad_code, code}, state}
  end
end
