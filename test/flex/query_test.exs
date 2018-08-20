defmodule Flex.QueryTest do
  use ExUnit.Case

  alias Flex.Query

  test "Query without measurements is invalid" do
    query1 = %Query{measurements: []}
    query2 = %Query{measurements: nil}

    assert {:error, _} = Query.build_query(query1)
    assert {:error, _} = Query.build_query(query2)
  end

  test "Query requires at least one measurement to be valid" do
    query = %Query{measurements: ["m"]}

    assert {:ok, _} = Query.build_query(query)
  end

  test "Query without fields specification, SELECTS all fields" do
    query = %Query{fields: nil, measurements: ["m"]}

    assert {:ok, query} = Query.build_query(query)
    assert "SELECT *" <> _ = query
  end

  test "Query can specify multiple fields to select" do
    query = %Query{fields: ["f1", "f2"], measurements: ["m"]}

    assert {:ok, query} = Query.build_query(query)
    assert "SELECT f1,f2" <> _ = query
  end

  test "Field can hold expressions" do
    query = %Query{fields: ["max(value) - 20"], measurements: ["m"]}

    assert {:ok, query} = Query.build_query(query)
    assert "SELECT max(value) - 20" <> _ = query
  end

  test "Where can hold simple conditions" do
    query = %Query{measurements: ["m"], where: [{"node", "node-1", :=}]}

    assert {:ok, query} = Query.build_query(query)
    assert [_, "node = 'node-1'"] = String.split(query, "WHERE ")
  end

  test "Where can hold expressions" do
    query = %Query{measurements: ["m"],
                   where: [{"time", {:expr, "now() - 2h"}, :<}]}

    assert {:ok, query} = Query.build_query(query)
    assert [_, "time < now() - 2h"] = String.split(query, "WHERE ")
  end

  for unit <- ["u", "µ", "ms", "s", "m", "h", "d", "w"] do
    test "Duration unit '#{unit}' is not escaped" do
      query = %Query{measurements: ["m"],
                     where: [{"time", "20#{unquote(unit)}", :<}]}

      assert {:ok, query} = Query.build_query(query)
      assert [_, "time < 20#{unquote(unit)}"] = String.split(query, "WHERE ")
    end
  end

  test "Multiple conditions are joined with AND" do
    query = %Query{measurements: ["m"],
                   where: [
                     {"time", {:expr, "now() - 2h"}, :<},
                     {"node", "node-1", :=}
                     ]}

    assert {:ok, query} = Query.build_query(query)
    assert [_, where_clause] = String.split(query, "WHERE ")
    assert "time < now() - 2h and node = 'node-1'" = where_clause
  end

  test "'from' and 'to' fields are converted into WHERE conditions" do
    query = %Query{measurements: ["m"],
                   from: "now() - 2d",
                   to: "now() - 1d"
                  }
    assert {:ok, query} = Query.build_query(query)
    assert [_, where_clause] = String.split(query, "WHERE ")
    conditions = String.split(where_clause, " and ")
    assert "time > now() - 2d" in conditions
    assert "time < now() - 1d" in conditions
  end

  test "Query can hold GROUP BY" do
    query = %Query{measurements: ["m"],
                   group_by: ["node"]}

    assert {:ok, query} = Query.build_query(query)
    assert [_, group_by_clause] = String.split(query, "GROUP BY ")
    assert "\"node\"" = group_by_clause
  end

  test "GROUP BY time is incorrect, when there is no WHERE time condition" do
    query = %Query{measurements: ["m"],
                   group_by: ["time(2d)"]}

    assert {:error, _} = Query.build_query(query)
  end

  test "GROUP BY time is correct, when there is WHERE time condition" do
    query = %Query{measurements: ["m"],
                   from: "now() - 2d",
                   group_by: ["time(2d)"]}

    assert {:ok, query} = Query.build_query(query)
    assert [_, group_by_clause] = String.split(query, "GROUP BY ")
    assert "time(2d)" = group_by_clause
  end

  test "GROUP BY fill is correct even if is not the last element of group_by list" do
    query = %Query{fields: ["f1"],
                   measurements: ["m"],
                   group_by: ["time(2d)", "fill(null)", "sample_tag"],
                   from: "now() - 2d",
                   to: "now() - 1d"}
    assert {:ok, query} = Query.build_query(query)
    assert [_, group_by_clause] = String.split(query, "GROUP BY ")
    assert "time(2d),\"sample_tag\" fill(null)" == group_by_clause
  end

  for opt <- ["10", "10.1", "null", "none", "previous", "linear"] do
    test "GROUP BY query is properly built, when fill option is '#{opt}'." do
      fill = "fill(" <> unquote(opt) <> ")"
      query = %Query{fields: ["f1"],
                     measurements: ["m"],
                     group_by: ["time(5m)", fill],
                     from: "now() - 2d",
                     to: "now() - 1d"}
      assert {:ok, query} = Query.build_query(query)
      assert [_, group_by_clause] = String.split(query, "GROUP BY ")
      assert "time(5m) " <> fill == group_by_clause
    end
  end

  test "GROUP BY fill is incorrect when fill option is not valid" do
    query = %Query{fields: ["f1"],
                   measurements: ["m"],
                   group_by: ["time(2d)", "fill(invalid_opt)"],
                   from: "now() - 2d",
                   to: "now() - 1d"}
    assert {:error, _} = Query.build_query(query)
  end
end
