defmodule Oserl.MixProject do
  use Mix.Project

  def project do
    [
      description: "Oserl Smpp",
      app: :oserl,
      version: "1.1.2",
      language: :erlang,
      elixirc_paths: elixirc_paths(Mix.env()),
      preferred_cli_env: [eunit: :test],
      package: package(),
      deps: deps()
    ]
  end

  defp package do
    [
      files: ["src", "include", "mix.exs", "README*"],
      maintainers: ["Funbox"],
      licenses: ["Copyright 2025 Fun-box, all rights reserved"],
      links: %{"GitHub" => "https://github.com/funbox/oserl.git"}
    ]
  end

  def application do
    [
      extra_applications: [:kernel, :stdlib, :mix]
    ]
  end

  # Specifies which paths to compile per environment.
  defp elixirc_paths(:test), do: ["src", "test/support"]
  defp elixirc_paths(_), do: ["src"]

  defp deps do
    [
      {:common_lib, ">= 4.0.0", repo: "funbox"}
    ]
  end
end
