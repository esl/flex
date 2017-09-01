defmodule Influx.TestCase do
  @moduledoc """
  This module defines the test case to be used by tests that require interaction
  with InfluxDB

  In order to use this case template, you need to configure address of running
  InfluxDB instance API:

      config :influx,
        test_host: "http://localhost:8086"
  """

  use ExUnit.CaseTemplate

  using do
    influx_host = Application.get_env(:influx, :test_host)

    if is_nil(influx_host) do
      raise ":test_host application env must be set in order to use " <>
            "Influx.TestCase"
    end

    ping_url = influx_host <> "/ping"
    case HTTPoison.head(ping_url) do
      {:ok, %HTTPoison.Response{status_code: 204}} ->
        :ok
      error ->
        raise "It seems InfluxDB at #{influx_host} is down. Error message: " <>
          "#{inspect(error)}"
    end

    quote do
    end
  end

  # InfluxDB will be cleared before each test case
  setup do
    host = Application.get_env(:influx, :test_host)
    db = "test_db"
    measurement_name = "measurement"
    tag = "tag"
    tag_value = "my_tag"
    measurement =
      "#{measurement_name},#{tag}=#{tag_value} value=2000,value2=2000"

    clear_db(host)
    create_db(db, host)
    insert_measurement(measurement, db, host)

    context = %{influx_host: host,
                present_measurement: measurement_name,
                present_db: db,
                present_tag: tag,
                tag_value: tag_value}
    {:ok, context}
  end

  def clear_db(host) do
    query = host <> "/query?q=SHOW+DATABASES"
    %HTTPoison.Response{body: body, status_code: 200} = HTTPoison.get!(query)
    result = Poison.decode!(body)
    dbs = get_in(result, ["results", Access.at(0), "series", Access.at(0),
                          "values"]) |> List.flatten # get out values from map
    dbs = List.delete(dbs, "_internal")
    Enum.each(dbs, &delete_db(&1, host))
  end

  def create_db(db, host) do
    query = host <> "/query?q=CREATE+DATABASE+#{db}"
    %HTTPoison.Response{status_code: 200} = HTTPoison.post!(query, "")
  end

  def delete_db(db, host) do
    query = host <> "/query?q=DROP+DATABASE+#{db}"
    %HTTPoison.Response{status_code: 200} = HTTPoison.post!(query, "")
  end

  def insert_measurement(measurement, db, host) do
    query = host <> "/write?db=#{db}"
    %HTTPoison.Response{status_code: 204} = HTTPoison.post!(query, measurement)
  end

end
