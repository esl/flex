defmodule Flex.Mixfile do
  use Mix.Project

  def project do
    [
      app: :flex,
      version: "0.2.0",
      elixir: "~> 1.6",
      build_embedded: Mix.env == :prod,
      start_permanent: Mix.env == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      applications: [:logger, :httpoison, :poison],
      mod: {Flex.Application, []}
    ]
  end

  defp deps do
    [
      {:httpoison, "~> 0.13"},
      {:poison,    "~> 2.2"},
      {:ex_doc,    "~> 0.18", only: :dev},
      {:earmark,   "~> 1.2",  only: :dev},
      {:dialyxir,  "~> 0.5",  only: :dev},
      {:credo,     "~> 0.8",  only: :dev},
    ]
  end
end
