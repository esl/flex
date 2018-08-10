defmodule Flex.APITest do
  use Flex.TestCase

  alias Flex.{API, Query, Datapoints}
  alias HTTPoison.Response

  require Logger

  @moduletag :external

  test "List of databases can be retrieved", %{influx_host: host} do
    params = %{host: host}
    assert {:ok, %Response{status_code: 200}} = API.get_databases(params)
  end

  test "Database can be created", %{influx_host: host} do
    # given
    params = %{host: host}
    db = "my_db"
    # when
    {:ok, %Response{status_code: 200}} = API.create_db(db, params)
    # then
    {:ok, %Response{status_code: 200, body: body}} = API.get_databases(params)
    databases = Poison.decode!(body)
                |> Datapoints.format_results()
                |> extract_field_from_datapoints("name")
    assert db in databases
  end

  test "Database can be deleted", %{influx_host: host} do
    # given
    params = %{host: host}
    db = "my_db"
    {:ok, %Response{status_code: 200}} = API.create_db(db, params)
    # when
    {:ok, %Response{status_code: 200}} = API.delete_database(db, params)
    # then
    {:ok, %Response{status_code: 200, body: body}} = API.get_databases(params)
    databases = Poison.decode!(body)
                |> Datapoints.format_results()
                |> extract_field_from_datapoints("name")
    refute db in databases
  end

  test "Tag names can be retrieved",
  %{influx_host: host, present_db: db, present_tag: tag} do
    # given
    params = %{host: host, db: db}
    # when
    {:ok, %Response{status_code: 200, body: body}} = API.get_tag_keys(params)
    # then
    tags = Poison.decode!(body)
                   |> Datapoints.format_results()
                   |> extract_field_from_datapoints("tagKey")
    assert tag in tags
  end

  test "Measurements can be listed",
  %{influx_host: host, present_db: db, present_measurement: m} do
    # given
    params = %{host: host, db: db}
    # when
    {:ok, %Response{status_code: 200, body: body}} =
      API.get_measurements(params)
    # then
    measurements = Poison.decode!(body)
                   |> Datapoints.format_results()
                   |> extract_field_from_datapoints("name")
    assert m in measurements
  end

  test "Measurement can be inserted", %{influx_host: host, present_db: db} do
    # given
    params = %{host: host, db: db, epoch: :ms}
    m_name = "my_shiny_measurement"
    m = "#{m_name} value=1234"
    # when
    {:ok, %Response{status_code: 204}} = API.write(m, params)
    # then
    {:ok, %Response{status_code: 200, body: body}} =
      API.get_measurements(params)
    measurements = Poison.decode!(body)
                   |> Datapoints.format_results()
                   |> extract_field_from_datapoints("name")
    assert m_name in measurements
  end

  test "Groupped measurement can be inserted",
  %{influx_host: host, present_db: db} do
    # given
    params = %{host: host, db: db, epoch: :ms}
    m_name1 = "my_shiny_measurement1"
    m_name2 = "my_shiny_measurement2"
    m = "#{m_name1} value=1234\n #{m_name2} value=1234"
    # when
    {:ok, %Response{status_code: 204}} = API.write(m, params)
    # then
    {:ok, %Response{status_code: 200, body: body}} =
      API.get_measurements(params)
    measurements = Poison.decode!(body)
                   |> Datapoints.format_results()
                   |> extract_field_from_datapoints("name")
    assert m_name1 in measurements
    assert m_name2 in measurements
  end

  test "Simple queries can be performed",
  %{influx_host: host, present_db: db} do
    # given
    params = %{host: host, db: db, epoch: :ms}

    m_name = "measurement1"
    data = [%{"value" => 1000, "time" => 1111},
            %{"value" => 2000, "time" => 2222},
            %{"value" => 3000, "time" => 3333}]
    datapoint = %{"name" => m_name, "data" => data}
    _ = insert_datapoint(params, datapoint)
    {:ok, query} = %Query{measurements: [m_name]} |> Query.build_query
    # when
    {:ok, %Response{status_code: 200, body: body}} = API.query(query, params)
    # then
    datapoints = Poison.decode!(body) |> Datapoints.format_results()
    values = extract_field_from_datapoints(datapoints, "value")
    times = extract_field_from_datapoints(datapoints, "time")

    Enum.each(data,
              fn(%{"value" => v, "time" => t}) ->
                 assert v in values
                 assert t in times
              end)
    assert length(data) == length(values)
    assert length(data) == length(times)
  end

  test "Can specify fields to query for",
  %{influx_host: host, present_db: db} do
    # given
    params = %{host: host, db: db, epoch: :ms}

    m_name = "measurement1"
    data = [%{"value" => 1000, "value2" => 1000, "time" => 1111},
            %{"value" => 2000, "value2" => 2000, "time" => 2222},
            %{"value" => 3000, "value2" => 3000, "time" => 3333}]
    datapoint = %{"name" => m_name, "data" => data}
    _ = insert_datapoint(params, datapoint)
    {:ok, query} = %Query{fields: ["value2"],
                          measurements: [m_name]} |> Query.build_query
    # when
    {:ok, %Response{status_code: 200, body: body}} = API.query(query, params)
    # then
    datapoints = Poison.decode!(body) |> Datapoints.format_results()
    values = extract_field_from_datapoints(datapoints, "value")
    values2 = extract_field_from_datapoints(datapoints, "value2")
    times = extract_field_from_datapoints(datapoints, "time")

    assert Enum.all?(values, &is_nil/1)
    assert length(data) == length(values2)
    assert length(data) == length(times)
  end

  test "Queries with `from` and `to` can be performed",
  %{influx_host: host, present_db: db} do
    # given
    params = %{host: host, db: db, epoch: :ms}

    m_name = "measurement1"
    data = [%{"value" => 1000, "time" => 1111},
            %{"value" => 2000, "time" => 2222},
            %{"value" => 3000, "time" => 3333}]
    datapoint = %{"name" => m_name, "data" => data}
    _ = insert_datapoint(params, datapoint)
    {:ok, query} = %Query{measurements: [m_name],
                          from: "2000ms",
                          to: "3000ms"} |> Query.build_query
    # when
    {:ok, %Response{status_code: 200, body: body}} = API.query(query, params)
    # then
    datapoints = Poison.decode!(body) |> Datapoints.format_results()
    values = extract_field_from_datapoints(datapoints, "value")
    times = extract_field_from_datapoints(datapoints, "time")

    assert 1 == length(values)
    assert 1 == length(times)
    assert 2000 in values
    assert 2222 in times
  end

  test "Queries can be groupped by time",
  %{influx_host: host, present_db: db} do
    # given
    params = %{host: host, db: db, epoch: :ms}

    m_name = "measurement1"
    data = [%{"value" => 1000, "time" => 1200},
            %{"value" => 4000, "time" => 1600},
            %{"value" => 2000, "time" => 2200},
            %{"value" => 8000, "time" => 2600}]
    datapoint = %{"name" => m_name, "data" => data}
    _ = insert_datapoint(params, datapoint)
    {:ok, query} = %Query{fields: ["max(value)"],
                          measurements: [m_name],
                          from: "1000ms",
                          to: "3000ms",
                          group_by: ["time(1000ms)"]} |> Query.build_query
    # when
    {:ok, %Response{status_code: 200, body: body}} = API.query(query, params)
    # then
    datapoints = Poison.decode!(body) |> Datapoints.format_results()
    values = extract_field_from_datapoints(datapoints, "max")
    times = extract_field_from_datapoints(datapoints, "time")

    assert 2 == length(values)
    assert 2 == length(times)
    assert 4000 in values and 8000 in values
    assert 1000 in times  and 2000 in times
  end

  test "Queries can be groupped by tag",
  %{influx_host: host, present_db: db} do
    # given
    params = %{host: host, db: db, epoch: :ms}

    m_name = "measurement1"
    data = [%{"value" => 1000, "node" => "first",  "time" => 1200},
            %{"value" => 4000, "node" => "second", "time" => 1600},
            %{"value" => 2000, "node" => "first",  "time" => 2200},
            %{"value" => 8000, "node" => "second", "time" => 2600}]
    datapoint = %{"name" => m_name, "data" => data, "tags" => ["node"]}
    _ = insert_datapoint(params, datapoint)
    {:ok, query} = %Query{fields: ["max(value)"],
                          measurements: [m_name],
                          from: "1000ms",
                          to: "3000ms",
                          group_by: ["node"]} |> Query.build_query
    # when
    {:ok, %Response{status_code: 200, body: body}} = API.query(query, params)
    # then
    datapoints = Poison.decode!(body) |> Datapoints.format_results()
    values = extract_field_from_datapoints(datapoints, "max")
    times = extract_field_from_datapoints(datapoints, "time")

    assert 2 == length(values)
    assert 2 == length(times)
    assert 2000 in values and 8000 in values
    assert 2200 in times  and 2600 in times
  end

  test "Queries can be retrieved as a stream",
  %{influx_host: host, present_db: db} do
    # given
    params = %{host: host, db: db, epoch: :ms, chunked: true, chunk_size: 1}
    m_name = "measurement1"
    data = [%{"value" => 1000, "node" => "first",  "time" => 1200},
            %{"value" => 4000, "node" => "second", "time" => 1600},
            %{"value" => 2000, "node" => "first",  "time" => 2200},
            %{"value" => 8000, "node" => "second", "time" => 2600}]
    datapoint = %{"name" => m_name, "data" => data, "tags" => ["node"]}
    _ = insert_datapoint(params, datapoint)
    {:ok, query} = %Query{measurements: [m_name]} |> Query.build_query
    # when
    stream = API.stream(query, params)
    # then
    datapoints = Stream.map(stream, &Poison.decode!/1)
                 |> Enum.map(&Datapoints.format_results/1)
    values = Enum.flat_map(datapoints,
                           &(extract_field_from_datapoints(&1, "value")))
    times = Enum.flat_map(datapoints,
                          &(extract_field_from_datapoints(&1, "time")))

    expected_values = Enum.map(data, &(Map.get(&1, "value")))
    expected_times = Enum.map(data, &(Map.get(&1, "time")))

    Enum.each(expected_values,
              fn (value) ->
                assert value in values
              end)
    Enum.each(expected_times,
              fn (time) ->
                assert time in times
              end)
  end

  @tag capture_log: true # Silence GenServer crash
  test "Queries to invalid InfluxDB returns error while retreving stream" do
    # given
    params = %{host: "http://nonexisting", db: "some_db", epoch: :ms,
               chunked: true, chunk_size: 1}
    {:ok, query} = %Query{measurements: ["m"]} |> Query.build_query
    # when
    stream = API.stream(query, params)
    # then
    {error, _gen_server_info} = catch_exit(Stream.run(stream))
    assert  error == :noproc
  end

  defp extract_field_from_datapoints(datapoints, field) do
  get_in(datapoints, [Access.all(), "data", Access.all(), field])
  |> List.flatten()
  end

  defp insert_datapoint(params, datapoint) do
    m = Datapoints.to_line_protocol(datapoint) |> Enum.join("\n")
    {:ok, %Response{status_code: 204}} = API.write(m, params)
  end

end
