# Flex

A simple InfluxDB client.

## Installation

Add to your deps
```elixir
deps do
  [
    ...
    {:flex, "~> 0.1"}
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
      :flex,
      ...
    ],
    ...
  ]
end
```

## Test

You'll need Influx serving locally on port 8086 (see `config/config.exs`):

```Shell
docker pull influxdb && \
docker run -d -p 8086:8086 -v influxdb:/var/lib/influxdb influxdb
```

Then go for a `mix test`.
