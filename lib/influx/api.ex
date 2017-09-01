defmodule Influx.API do
  @moduledoc """
  Responsible for interacting with InfluxDB HTTP API.

  The current implementation uses HTTPoison as an HTTP client.

  """

  @type epoch :: :ms
               | :s

  @typedoc """
  Set of parameters for this module

  - `:db`
     which database should be used for the query
  - `:epoch`
     sets the unit of timestamp either given in a query or returned in a response
  - `:host`
    address for InfluxDB HTTP API endpoint e.g. `http://localhost:8086`
  - `:pretty`
    if true, returned JSON will be pretty formatted.
    Default: `false`
  - `:chunked`
    if true, a response will be splitted into multiple responses.
    A response will be splitted when there is the end of data seria
    or when `chunked_size` is reached, whatever occurs first.
    Default: `false`
  - `:chunked_size`
    sets the maximum size of returned chunk. By default InfluxDB uses `10 000`
    if it is not set explicitly.
    Default: `nil`
  """
  @type param :: {:db, String.t}
               | {:epoch, epoch}
               | {:host, String.t}
               | {:pretty, boolean}
               | {:chunked, boolean}
               | {:chunk_size, integer}

  @type params :: [param]

  ###
  # API functions
  ###

  @doc """
  Performs a query through InfluxDB HTTP API. The response is given as a Stream.

  The query is given as a string in the first parameter. It makes sense to use this
  function only for queries expected to return big amount of data.

  Also this function makes sense only with `chunked: true, chunked_size: size`
  parmeters. If it is not given, result will be returned as a single element, so
  there are no advantages of Streams.

  Required parameters: `:db`, `:epoch`, `:host`
  Optional parameters: `:pretty`, `:chunked`, :`chunk_size`
  """
  @spec stream(String.t, params) :: Stream.t | {:error, any}
  def stream(query, params) do
    params = Enum.into(params, %{})
    required = [:db, :epoch, :host]
    optional = [:pretty, :chunked, :chunk_size]
    case check_params(params, required, optional) do
      {:ok, params} ->
        url = form_read_url(query, params)
        Influx.Stream.new_stream(url)
      error ->
        error
    end
  end

  @doc """
  Performs query through InfluxDB HTTP API.

  Query is given as a string in the first parameter.

  Required parameters: `:db`, `:epoch`, `:host`
  Optional parameters: `:pretty`, `:chunked`, :`chunk_size`
  """
  @spec query(String.t, params) :: {:ok, HTTPoison.Response.t} | {:error, any}
  def query(query, params) do
    params = Enum.into(params, %{})
    required = [:db, :epoch, :host]
    optional = [:pretty, :chunked, :chunk_size]
    do_get_to_read_endpoint(query, params, required, optional)
  end

  @doc """
  Performs POST request including the first parameter as data. The first
  parameter is expected to be an Influx Line Protocol formatted binary.

  For more info regarding it see:
  https://docs.influxdata.com/influxdb/latest/write_protocols/line_protocol_tutorial/

  It requires Influx API endpoint, database name and epoch parameters.

  Actually epoch parameter is not required by InfluxDB - it implicitly assumes
  epoch is "ns" - nanosecond. However during working with InfluxDB we
  encountered a lot of issues related to it, thus it is safer to require it.

  Required parameters: `:db`, `:epoch`, `:host`
  Optional parameters: `:pretty`

  ```
    iex(1)> Influx.API.write("measurement", [])
    {:error, [missing: :db, missing: :epoch, missing: :host]}
  ```

  """
  @spec write(String.t, params) :: {:ok, HTTPoison.Response.t} | {:error, any}
  def write(data, params) do
    params = Enum.into(params, %{})
    required = [:db, :epoch, :host]
    optional = [:pretty]
    do_post_to_write_endpoint(data, params, required, optional)
  end

  @doc """
  Perform database creation with given name, for given InfluxDB. It expects
  only one parameter - address of InfluxDB API endpoint.

      iex(1)> Influx.API.create_db("my_db", [])
      {:error, [missing: :host]}

      iex(2)> Influx.API.create_db("my_db", host: "http://localhost:8086")
      {:ok, %HTTPoison.Response{ ... }}

  Required parameters: `:host`
  Optional parameters: none
  """
  @spec create_db(String.t, params) :: {:ok, HTTPoison.Response.t}
                                     | {:error, any}
  def create_db(db_name, params) do
    params = Enum.into(params, %{})
    required = [:host]
    optional = []
    query = "CREATE DATABASE \"#{db_name}\""
    do_post_to_read_endpoint(query, params, required, optional)
  end

  @doc """
  Performs "DELETE DATABASE" database query.

  Required parameters: `:host`
  """
  @spec delete_database(String.t, params) :: {:ok, HTTPoison.Response.t}
                                           | {:error, any}
  def delete_database(db, params) do
    params = Enum.into(params, %{})
    required = [:host]
    optional = [:pretty, :chunked, :chunk_size]
    query = "DROP DATABASE \"#{db}\""
    do_post_to_read_endpoint(query, params, required, optional)
  end

  ###
  # Schema exploration releted queries
  ###

  @doc """
  Performs "SHOW MEASUREMENTS" database query.

  Result will include all available measurements in given database.

  Required parameters: `:host`, `:db`
  """
  @spec get_measurements(params) :: {:ok, HTTPoison.Response.t} | {:error, any}
  def get_measurements(params) do
    params = Enum.into(params, %{})
    required = [:db, :host]
    optional = [:pretty, :chunked, :chunk_size]
    query = "SHOW MEASUREMENTS"
    do_get_to_read_endpoint(query, params, required, optional)
  end

  @doc """
  Performs "SHOW TAG KEYS" database query.

  Result will return all measurements available in database with tags included.

  Required parameters: `:host`, `:db`
  """
  @spec get_tag_keys(params) :: {:ok, HTTPoison.Response.t} | {:error, any}
  def get_tag_keys(params) do
    required = [:db, :host]
    optional = [:pretty, :chunked, :chunk_size]
    query = "SHOW TAG KEYS"
    do_get_to_read_endpoint(query, params, required, optional)
  end

  @doc """
  Performs "SHOW FIELD KEYS" database query.

  Result will return all measurmenet available in database with field types.

  Required parameters: `:host`, `:db`
  """
  @spec get_field_keys(params) :: {:ok, HTTPoison.Response.t} | {:error, any}
  def get_field_keys(params) do
    required = [:db, :host]
    optional = [:pretty, :chunked, :chunk_size]
    query = "SHOW FIELD KEYS"
    do_get_to_read_endpoint(query, params, required, optional)
  end

  @doc """
  Performs "SHOW DATABASES" database query.

  Result will return all available databases.

  Required parameters: `:host`
  """
  @spec get_databases(params) :: {:ok, HTTPoison.Response.t} | {:error, any}
  def get_databases(params) do
    params = Enum.into(params, %{})
    required = [:host]
    optional = [:pretty, :chunked, :chunk_size]
    query = "SHOW DATABASES"
    do_get_to_read_endpoint(query, params, required, optional)
  end

  ###
  # Private functions
  ###

  @spec do_get_to_read_endpoint(String.t, params, [atom], [atom])
  :: {:ok, HTTPoison.Response.t} | {:error, any}
  defp do_get_to_read_endpoint(query, params, required, optional) do
    params = Enum.into(params, %{})
    case check_params(params, required, optional) do
      {:ok, params} ->
        url = form_read_url(query, params)
        HTTPoison.get(url)
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp do_post_to_read_endpoint(query, params, required, optional) do
    case check_params(params, required, optional) do
      {:ok, params} ->
        url = form_read_url(query, params)
        HTTPoison.post(url, "")
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp do_post_to_write_endpoint(data, params, required, optional) do
    case check_params(params, required, optional) do
      {:ok, params} ->
        url = form_write_url(params)
        HTTPoison.post(url, data)
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp form_write_url(params) do
    {host, params} = Map.pop(params, :host)
    uri = URI.parse(host)
    {precision, params} = Map.pop(params, :epoch) # on read we have epoch param
                                                  # on writing it is :precision
    query = Map.put(params, :precision, precision)
            |> URI.encode_query()
    %URI{uri | path: "/write", query: query}
    |> URI.to_string()
  end

  defp form_read_url(query, params) do
    {host, params} = Map.pop(params, :host)
    uri = URI.parse(host)
    query = maybe_put_query_param(query, params)
            |> URI.encode_query()
    %URI{uri | path: "/query", query: query}
    |> URI.to_string()
  end

  defp maybe_put_query_param("", params), do: params
  defp maybe_put_query_param(q, params),  do: Map.put(params, "q", q)

  ###
  # Parameters checking functions
  ###

  @spec check_params(map, params, params) :: {:ok, map} | {:error, Keyword.t}
  defp check_params(params, required, optional) do
    given_params = Map.keys(params)
    missing = get_missing_params(given_params, required)
    bad = get_bad_params(given_params, required ++ optional)
    case missing do
      [] -> {:ok, Map.drop(params, bad)}
      errors -> {:error, errors}
    end
  end

  defp get_missing_params(params, required) do
    Enum.map(required -- params, &({:missing, &1}))
  end

  defp get_bad_params(params, allowed) do
    Enum.map(params -- allowed, &(&1))
  end

end
