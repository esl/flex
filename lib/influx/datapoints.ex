defmodule Influx.Datapoints do

  @moduledoc """
  Module contains functions to format reponses for InfluxDB queries.
  """

  @type data :: %{required(String.t) => String.t}

  @typedoc """
  Datapoint is formatted response from InfluxDB.

  List of datapoints is returned from `format_results/1` function. It simplifies
  transformations on InfluxDB responses.

  ## Example datapoint:
  ```
    %{"name" => "amoc.times.message_ttd.max",
      "data" => [
                 %{"value" => 134633.0, "time" => "2017-02-22T13:36:51Z"}
                 %{"value" => 134634.0, "time" => "2017-02-22T13:37:01Z"}
                ]
    }
  ```
  """
  @type datapoint :: %{String.t => String.t,
                       String.t => [data]}

  @type datapoints :: [datapoint]

  @typedoc """
  `t:datapoint/0` with list of tags.

  This should be used for datapoints that describes metrics. It can be created
  with `add_tags_to_datapoint/2`. See its docs for examples.
  """
  @type datapoint_with_tags :: %{String.t => String.t, #name
                                 String.t => [data], #data
                                 String.t => [String.t]} #tags

  @type datapoints_with_tags :: [datapoint_with_tags]

  # TODO: This should be removed, as same extraction can be done
  # with format_results/1 function.
  @doc false
  @spec convert(map) :: map | []
  def convert(res) do
    if res["results"] == [%{}] do
      []
    else
      Enum.flat_map(res["results"],
                    fn(r) -> Enum.map(r["series"],
                                      fn(s) -> %{"target" => get_name(s),
                                        "datapoints" => make_datapoints_values(
                                          s["columns"], s["values"])}
                                      end)
                    end)
    end
  end

  @doc """
  Converts values from valid datapoint to InfluxDB Line Protocol.

  Structure of InfluxDB Line Protocol:
  ```
  <measurements_name>[,<tag_name>=<tag_val>] <val_name>=<value> [<timestamp>]
  ```

  Map should have form of:

  ```
  %{"name" => "amoc.times.message_ttd.max",
    "data" => [
               %{
                 "max" => 1346342154.0,
                 "time" => "2017-02-22T13:36:51Z",
                 "node" => "first_one"
                },
              ]
    "tags" => ["node"]
   }
  ```
  """
  @spec to_line_protocol(datapoint, String.t) :: Enumerable.t
  def to_line_protocol(measurement, extra_tags \\ "") do
    name = measurement["name"]
    tags = Map.get(measurement, "tags", [])
    measurement["data"]
    |> Enum.map(&(make_line_protocol(&1, name, tags, extra_tags)))
  end

  @doc """
  This functions tranforms InfluxDB bare JSON response into internal
  representation of datapoint.

  Internal representation of datapoint is map with two fields: `"name"` and
  `"data"`.

  For example:

  ```
  iex> results = %{"results" =>
                      [%{"series" =>
                        [%{"columns" => ["time", "max"],
                           "name" => "amoc.times.message_ttd.max",
                            "values" => [["2017-02-22T13:36:51Z", 1346342154.0]]
                          }],
                         "statement_id" => 0}]}
  iex> format_results(results)
  [
    %{"name" => "amoc.times.message_ttd.max",
      "data" => [%{"max" => 1346342154.0, "time" => "2017-02-22T13:36:51Z"}]
    }
  ]
  ```
  """
  @spec format_results(map) :: datapoints
  def format_results(results) do
    get_in(results, ["results", Access.all()])
    |> Enum.flat_map(&parse_statement/1)
  end

  @doc """
  This function adds to datapoint map another field `tags`.

  Tags are retrievied from second argument, which is expected to be a result
  of `SHOW TAG KEYS` query formatted with `format_results/1`.

  ## Example

      iex> measurement =
        %{"name" => "amoc.times.message_ttd.max",
          "data" => [%{"max" => 1346342154.0, "time" => "2017-02-22T13:36:51Z"}]
        }
      iex> tags =
        [
          %{"name" =>"amoc.times.message_ttd.max",
            "data" => [%{"tagKey" => "tag1"}, %{"tagKey" => "tag2"}]
          },
          %{"name" =>"another.metric",
            "data" -> [%{"tagKey" => "any_tag"}]
          }
        ]
      iex> add_tags_to_datapoint(measurement, tags)
      %{"name" => "amoc.times.message_ttd.max",
        "data" => [%{"max" => 1346342154.0, "time" => "2017-02-22T13:36:51Z"}],
        "tags" => ["tag1", "tag2"]
      }

  """
  @spec add_tags_to_datapoint(datapoint, datapoints) :: datapoint_with_tags
  def add_tags_to_datapoint(measurement, tags_for_measurements) do
    name = measurement["name"]
    tags =
      Enum.find(tags_for_measurements, %{}, &(&1["name"] == name))
      |> get_in([Access.key("data", []), Access.all(), "tagKey"])
    Map.put(measurement, "tags", tags)
  end

  @doc """
  Function escapes characters disallowed in InfluxDB Line Protocol.
  """
  @spec escape_chars(String.t) :: String.t
  def escape_chars(value) when is_binary(value) do
    value
    |> String.replace(",", "\\,")
    |> String.replace("=", "\\=")
    |> String.replace(" ", "\\ ")
    |> String.replace("\"", "\\\"")
  end

  def escape_chars(value), do: value

  @doc """
  Functions returns string representation of given value in InfluxDB Line
  Protocol format.

  Data types:
    - Float - 82
    - Integer - 82i
    - String - "some string"
    - Boolean - t, T, true, True, TRUE, f, F, false, False, FALSE

  """
  def escape_field(value) when is_integer(value), do: "#{value}i"
  def escape_field(value) when is_float(value) or is_boolean(value),
    do: "#{value}"
  def escape_field(value) when is_binary(value), do: "\"#{value}\""

  ###
  # Private functions
  ###

  @spec get_name(map) :: String.t
  defp get_name(series) do
    tags = case series["tags"] do
             nil -> ""
             some -> some |> Poison.encode! |> String.replace("\"", " ")
           end
    series["name"] <> " " <> tags
  end

  @spec parse_statement(map) :: [map]
  defp parse_statement(%{"error" => msg}),
    do: [%{"name" => "error", "data" => msg}]

  defp parse_statement(%{"series" => series}),
    do: Enum.map(series, &parse_seria/1)

  defp parse_statement(_),
    do: [%{"name" => "error", "data" => "empty response"}]

  @spec parse_seria(map) :: map
  defp parse_seria(seria) do
    name = seria["name"]
    keys = seria["columns"]
    key_values_map = Enum.map(seria["values"],
                              &(Enum.zip(keys, &1) |> Enum.into(%{})))
    %{"name" => name,
      "data" => key_values_map}
  end

  @spec make_datapoints_values([String.t], [[integer]]) :: [[integer]]
  defp make_datapoints_values([_, "time"], values), do: values

  defp make_datapoints_values(["time", _], values) do
    Enum.map(values, fn(v) -> Enum.reverse(v) end)
  end

  defp make_datapoints_values(columns, values) do
    # We want a metrics in format of [value, timestamp]
    # but sometimes 'timestamp' column is on first place
    index = Enum.find_index(columns, fn(x) -> x == "time" end)
    Enum.map(values, fn(v) ->
      val = Enum.fetch!(v, index)
      List.delete_at(v, index) ++ [val]
    end)
  end

  # This is how Line Protocol look like:
  #  weather,location=us-midwest temperature=82 1465839830100400200
  #    |    -------------------- --------------  |
  #    |             |             |             |
  #    |             |             |             |
  #  +-----------+--------+-+---------+-+---------+
  #  |measurement|,tag_set| |field_set| |timestamp|
  #  +-----------+--------+-+---------+-+---------+
  #
  #  Provided data should be in form of map:
  #  data = %{"value" => 122,
  #           "some tag" => tag_value}
  #
  # measurement_name is just string
  #
  # tags is a list of strings. It says which keys in data should be
  # formed as tag. All other keys will be formed as fields.
  # Only "time" key is an exception. It is treated as a timestamp.
  #
  # Extra tags is already formatted as part of Line Protocol

  @spec make_line_protocol(map, String.t, [String.t], String.t) :: String.t
  defp make_line_protocol(data, measurement_name, tags, extra_tags) do
    {timestamp, data} = Map.pop(data, "time")
    {tags, fields} = Enum.split_with(data, &(is_tag?(&1, tags)))
    tags = tuples_to_line(tags, :tag)
    fields = tuples_to_line(fields, :field)
    tags = [tags, extra_tags] |> Enum.filter(&(&1 != "")) |> Enum.join(",")
    case tags do
      ""   -> "#{measurement_name} #{fields} #{timestamp}"
      tags -> "#{measurement_name},#{tags} #{fields} #{timestamp}"
    end
  end

  defp is_tag?({key, _}, tags), do: key in tags

  # part of making line protocol:
  # [{"a", "b"}, {"c", "d"}] -> "a=b,c=d"
  defp tuples_to_line(tuples, :tag) do
    tuples
    |> Enum.map(fn ({k, v}) -> "#{k}=#{escape_chars(v)}" end)
    |> Enum.join(",")
  end

  defp tuples_to_line(tuples, :field) do
    tuples
    |> Enum.map(fn ({k, v}) -> "#{k}=#{escape_field(v)}" end)
    |> Enum.join(",")
  end
end
