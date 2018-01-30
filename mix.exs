defmodule Influx.Mixfile do
  use Mix.Project

  def project do
    [
      app: :influx,
      version: "0.1.0",
      elixir: "~> 1.3",
      build_embedded: Mix.env == :prod,
      start_permanent: Mix.env == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      applications: [:logger, :httpoison, :poison],
      mod: {Influx.Application, []}
    ]
  end

  defp deps do
    [
      {:httpoison, "~> 0.13.0"},
      {:poison,    "~> 2.2"},
      {:ex_doc,    "~> 0.18.1", only: :dev},
      {:earmark,   "~> 1.2.2",  only: :dev},
      {:dialyxir,  "~> 0.5.1",  only: :dev},
      {:credo,     "~> 0.8.8",  only: :dev},
    ]
  end
end
