defmodule BatchServing.MixProject do
  use Mix.Project

  @source_url "https://github.com/NduatiK/batch_serving"

  def project do
    [
      app: :batch_serving,
      description: "A serving for batch processing",
      version: "1.0.2",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package(),
      docs: &docs/0
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

  defp docs do
    [
      extras: [
        "README.md",
        "docs/hooks.livemd",
        "docs/liveview_batch_progress_demo.livemd"
      ]
    ]
  end
end
