defmodule BatchServing.MixProject do
  use Mix.Project

  @source_url "https://github.com/NduatiK/batch_serving"

  def project do
    [
      app: :batch_serving,
      description: "A serving for batch processing",
      version: "0.1.2",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:telemetry, "~> 0.4.0 or ~> 1.0"},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false}
    ]
  end

  defp package do
    %{
      maintainers: ["Nduati Kuria"],
      licenses: ["MIT"],
      links: %{
        "GitHub" => @source_url
      }
    }
  end
end
