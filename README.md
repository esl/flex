# Influx

A simple InfluxDB client.

## Installation

Add to your deps
```elixir
deps do
  [
    ...
    {:influx, "~> 0.1"}
    ...
  ]
end
````

And to your applications (pre Elixir 1.4)
```elixir
def application do
  [
    applications: [
      ...
      :influx,
      ...
    ],
    ...
  ]
end
```
