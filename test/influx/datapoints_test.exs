defmodule Influx.DatapointsTest do
  use Influx.TestCase

  alias Influx.Datapoints

  for query_fun <- [&__MODULE__.from_query/3,
                    &__MODULE__.get_tags/3,
                    &__MODULE__.get_measurements/3] do
    @tag :external
    test "InfluxDB response from #{inspect(query_fun)} map can be formatted",
    %{influx_host: host, present_db: db, present_measurement: m} do
      response = unquote(query_fun).(host, db, m)
      measurements = Datapoints.format_results(response)
      Enum.each(measurements,
                fn (measurement) ->
                  assert "name" in Map.keys(measurement)
                  assert "data" in Map.keys(measurement)
                end
      )
    end
  end

  @tag :external
  test "Datapoint can have added tags",
  %{influx_host: host, present_db: db, present_measurement: m, present_tag: t}
  do
    [measurement] = from_query(host, db, m) |> Datapoints.format_results()
    tags = get_tags(host, db) |> Datapoints.format_results()

    measurement_with_tags = Datapoints.add_tags_to_datapoint(measurement, tags)

    assert "name" in Map.keys(measurement_with_tags)
    assert "data" in Map.keys(measurement_with_tags)
    assert "tags" in Map.keys(measurement_with_tags)
    assert t in measurement_with_tags["tags"]
  end

  test "InfluxDB line protocol can be formed from measurement map" do
    m = %{
      "name" => "m",
      "data" => [
        %{"key" => "value", "time" => 12_345},
        %{"key" => "value", "some_tag" => "some_tag_value", "time" => 12_346},
      ],
      "tags" => ["some_tag"]
    }

    lines = Datapoints.to_line_protocol(m)
    assert 2 = length(lines)
    assert "m key=value 12345" in lines
    assert "m,some_tag=some_tag_value key=value 12346" in lines

  end

  def from_query(host, db, measurement) do
    query =
      URI.encode_query(%{"q" => "SELECT * FROM \"#{measurement}\"", "db" => db})
    url = host <> "/query?#{query}"
    http_get(url)
  end

  def get_tags(host, db, _ \\ "") do
    query = URI.encode_query(%{"q" => "SHOW TAG KEYS", "db" => db})
    url = host <> "/query?#{query}"
    http_get(url)
  end

  def get_measurements(host, db, _ \\ "") do
    query = URI.encode_query(%{"q" => "SHOW MEASUREMENTS", "db" => db})
    url = host <> "/query?#{query}"
    http_get(url)
  end

  defp http_get(url) do
    %HTTPoison.Response{status_code: 200, body: body} = HTTPoison.get!(url)
    Poison.decode!(body)
  end

end
