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
      extra_applications: [:logger],
      mod: {Influx.Application, []}
    ]
  end

  defp deps do
    [
      {:httpoison, "~> 0.9.0"},
      {:poison, "~> 2.0"},
      {:ex_doc, "~> 0.16.2", only: :dev},
      {:earmark, "~> 1.0", only: :dev},
      {:dialyxir, "~> 0.4", only: :dev},
      {:credo, "~> 0.5", only: :dev},
    ]
  end
end
