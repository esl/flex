defmodule Influx.Query do
  @moduledoc """
  This module is used for constructing InfluxDB Queries.

  Queries are built using Query struct:
  ```
    %Query{fields: nil, from: nil, group_by: nil, measurements: nil,
     to: nil, where: []}
  ```

  ## measurements
  This is mandatory option to build query expression. Is is expected to be
  a list of string.

  ## fields
  It is expected to be list of strings. When nothing is provided,
  it will get all fields. This is optional.

  Examples of transformations:

      iex(1)> %Query{fields: nil, measurements: ["ttd"]}
              |> Query.build_query
      {:ok, "SELECT * FROM \\"ttd\\""}

      iex(2)> %Query{fields: ["max(value)", "test_id"], measurements: ["ttd"]}
              |> Query.build_query
      {:ok, "SELECT max(value),test_id FROM \\"ttd\\""}

  ## where
  This is optional parameter for filtering data we are quering for. It expects
  a list of `where()` tuples. They are in form:
  ```
  {field :: String.t, value :: expression(), comparator :: comparator()}
  ```
  First one indicated field name, second is expression or literal, which Influx
  is going to compare with field and last one is comparator. Values will be
  espaced if need (e.g. string literals should be escaped with `'`).

  If we want to put expression into value, we should point directly that
  it is expression and it should not be escaped:
  ```
  {:expr, "now() - 20w"}
  ```

  Examples:

       iex(1)> %Query{measurements: ["tdd"],
                       where: [{"start_type", "manual", :=}]}
               |> Query.build_query
       {:ok, "SELECT * FROM \\"tdd\\" WHERE start_type = 'manual'"}

       iex(2)> %Query{measurements: ["tdd"],
                       where: [{"time", {:expr, "now() - 2w"}, :>}]}
               |> Query.build_query
       {:ok, "SELECT * FROM \\"tdd\\" WHERE time > now() - 2w"}

  ## from and to
  This is simplifier for specifying `where` parameter. It is common to query
  with specific time barriers. So instead of writing:
  ```
  %Query{measurements: ["tdd"],
          where: [{"time", {:expr, "now() - 2w"}, :>},
                  {"time", {:expr, "now() - 1w"}, :<}]}
  ```
  we can write:
  ```
  iex(1)> %Query{measurements: ["tdd"],
                  from: "now() - 2w",
                  to: "now() - 1w"}
          |> Query.build_query
  {:ok, "SELECT * FROM \\"tdd\\" WHERE time > now() - 2w and time < now() - 1w"}
 ```
  ## group by
  It expects a list of fields, which response should be groupped by.

  Response could be also groupped by time, however InfluxDB API requires to
  put time limits in WHERE clouse

  ```
  iex(1)> %Query{measurements: ["tdd"],
                  from: "now() - 2w",
                  group_by: ["time(2d)"]}
          |> Query.build_query
  {:ok, "SELECT * FROM \\"tdd\\" WHERE time > now() - 2w GROUP BY time(2d)"}
  ```

  """

  require Logger

  defstruct from: nil,
    to: nil,
    measurements: nil,
    fields: nil,
    where: [],
    group_by: nil

  @type field :: String.t
  @type expr :: String.t | {:expr, String.t}
  @type comparator :: := | :< | :<= | :> | :>= | :"=~" | :"!~"

  @type where() :: {field, expr, comparator}

  @type t() :: %__MODULE__{from: String.t,
                           to: String.t,
                           measurements: [String.t],
                           fields: [String.t],
                           where: [where()],
                           group_by: [String.t]
  }

  @doc """
  This function builds InfluxDB Query from a `Query` struct.

  It will validate correctness of the given parameters and build a string that
  represents and actual query to be sent to Influx..

  NOTE: Module is in early development and may build incorrect queries!
  """
  @spec build_query(__MODULE__.t) :: String.t | {:error, any}
  def build_query(%__MODULE__{} = query) do
    with {:ok, query}       <- add_timestamps_to_where(query),
         {:ok, measurement} <- build_measurement(query),
         {:ok, fields}      <- build_fields(query),
         {:ok, where}       <- build_where(query),
         {:ok, group_by}    <- build_group_by(query) do
           {:ok, "SELECT #{fields} "
                 <> "FROM #{measurement}"
                 <> "#{where}"
                 <> "#{group_by}"}
    else
      {:error, _} = error -> error
    end
  end

  @doc """
  Stack string queries so that they're sent to InfluxDB as a single query.
  """
  @spec stack_queries([String.t]) :: String.t | {:error, any}
  def stack_queries(string_queries) do
    Enum.reduce(string_queries, &stack_queries/2)
  end

  # Private functions

  # InfluxDB HTTP API allows to send multiple queries at once by delimiting them
  # with a semicolon - and this is exactly what this function does.
  #
  # Look at the following link to see the details:
  # https://docs.influxdata.com/influxdb/v0.9/guides/querying_data/#multiple-queries
  @spec stack_queries(String.t, String.t) :: String.t
  defp stack_queries(query, accumulated_queries),
    do: "#{query};#{accumulated_queries}"

  # This function converts fields `from` and `to` into WHERE expression.
  #
  # Theese fields are just shortends - e.g. we know that value `from` will be
  # compared to a time field in query, with greater comparator: `:>`
  defp add_timestamps_to_where(%__MODULE__{where: wheres} = query) do
    time_clause = [maybe_get_from(query), maybe_get_to(query)]
    query = Map.put(query, :from, nil)
            |> Map.put(:to, nil)
    case List.flatten(time_clause) do
      [] ->
        {:ok, query}
      time_clause ->
        query = Map.put(query, :where, [time_clause | wheres])
        {:ok, query}
    end
  end

  defp maybe_get_from(%__MODULE__{from: nil}), do: []
  defp maybe_get_from(%__MODULE__{from: from}),
    do: [{"time", {:expr, from}, :>}]

  defp maybe_get_to(%__MODULE__{to: nil}), do: []
  defp maybe_get_to(%__MODULE__{to: to}),
    do: [{"time", {:expr, to}, :<}]

  defp build_measurement(%__MODULE__{measurements: m})
  when is_list(m) and length(m) > 0 do
    m = m
        |> Enum.map(&escape_val(&1, "\""))
        |> Enum.join(",")
    {:ok, "#{m}"}
  end
 defp build_measurement(%__MODULE__{}), do: {:error, :measurements}

  defp build_fields(%__MODULE__{fields: nil}), do: {:ok, "*"}
  defp build_fields(%__MODULE__{fields: f}),   do: {:ok, Enum.join(f, ",")}

  defp build_where(%__MODULE__{where: nil}), do: {:ok, ""}
  defp build_where(%__MODULE__{where: where}) do
    {time_clauses, where} = pop_time_clauses(where)
    results = Enum.map(where, &parse_and_group/1)
              |> validate()
    with {:ok, conds} <- results,
      conds = Enum.join(conds, " OR "),
      {:ok, time_clauses} <- form_time_clause(time_clauses),
      {:ok, where_clause} <- join_time_with_clauses(conds, time_clauses),
      {:empty, false} <- {:empty, where_clause == ""}
    do
      {:ok, "WHERE #{where_clause}"}
    else
      {:empty, true} -> {:ok, ""}
      error -> error
    end
  end

  defp pop_time_clauses(where) do
    time_clauses = List.flatten(where) |> Enum.filter(&is_time_clause?/1)
    where = Enum.map(where,
                     fn(x) ->
                       Enum.reject(x, &is_time_clause?/1)
                     end)
    {time_clauses, where}
  end

  defp is_time_clause?({"time", _, _}), do: true
  defp is_time_clause?(_), do: false

  defp form_time_clause( time_clauses) do
    times = Enum.map(time_clauses, &parse_single_condition/1)
            |> validate()
    case times do
      {:ok, []} -> {:ok, ""}
      {:ok, results} -> {:ok, "#{Enum.join(results," AND ")}"}
      error -> error
    end
  end

  defp join_time_with_clauses(conds, ""),
    do: {:ok, conds}

  defp join_time_with_clauses("", time_clauses),
    do: {:ok, time_clauses}

  defp join_time_with_clauses(conds, time_clauses),
    do: {:ok, "(#{time_clauses}) AND (#{conds})"}

  defp parse_and_group(and_joined_conditions) do
    results = Enum.map(and_joined_conditions, &parse_single_condition/1)
              |> validate()
    case results do
      {:ok, []} -> ""
      {:ok, results} -> Enum.join(results, " AND ")
      error -> error
    end
  end

  @valid_comparators [:=, :<, :<=, :>, :>=, :"=~", :"!~"]

  defp parse_single_condition({field, {:expr, expression}, comparator}) do
    case comparator in @valid_comparators do
      true -> "#{field} #{comparator} #{expression}"
      false -> {:error, {:invalid_op, comparator}}
    end
  end
  defp parse_single_condition({field, value, comparator}) do
    case comparator in @valid_comparators do
      true -> "#{field} #{comparator} #{escape_val(value, "'")}"
      false -> {:error, {:invalid_op, comparator}}
    end
  end

  defp build_group_by(%__MODULE__{group_by: nil}), do: {:ok, ""}
  defp build_group_by(%__MODULE__{group_by: "*"}), do: {:ok, " GROUP BY *"}
  defp build_group_by(%__MODULE__{group_by: group_by, where: wheres}) do
    {valid, invalid} = group_by
                       |> Enum.map(&parse_group_by(&1, wheres))
                       |> Enum.split_with(&valid?/1)
    cond do
      valid != [] and invalid == [] ->
        {:ok, " GROUP BY " <> Enum.join(valid, ",")}
      valid == [] and invalid == [] ->
        {:ok, ""}
      true ->
        {:error, invalid}
    end
  end

  # GROUP BY time(<time_interval>),[tag_key]
  defp parse_group_by("time(" <> _ = time, wheres) do
    # we need to check for time condition where clause, becaouse groupping
    # by time in disallowed without giving timerange.
    case Enum.any?(wheres, fn ({"time", _, _}) -> true
                              (_)              -> false end) do
      true -> time
      false -> {:error, "missing time condition in where statement"}
    end
  end
  defp parse_group_by(tag, _) when is_binary(tag), do: escape_val(tag, "\"")
  defp parse_group_by(tag, _), do: {:error, tag}

  defp escape_val(val, escape_char) do
    cond do
      is_duration?(val) -> val
      is_regex?(val) -> val
      is_expression?(val) -> val
      true -> "#{escape_char}#{val}#{escape_char}"
    end
  end

  @duration_units ["u", "µ", "ms", "s", "m", "h", "d", "w"]
  defp is_duration?(val) do
    case Integer.parse(val) do
      {int, suffix} when is_integer(int) -> suffix in @duration_units
      :error -> false
    end
  end

  defp is_regex?(val) do
    Regex.match?(~r/\/.*\//, val)
  end

  defp is_expression?({:expr, _}), do: true
  defp is_expression?(_), do: false

  defp valid?({:error, _}), do: false
  defp valid?(_), do: true

end
