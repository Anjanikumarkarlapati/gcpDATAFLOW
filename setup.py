"""Packaging so Dataflow workers receive the `clickstream` package (--setup_file)."""
import setuptools

setuptools.setup(
    name="clickstream-pipeline",
    version="1.0.0",
    description="Dataflow streaming pipeline for clickstream aggregation",
    packages=setuptools.find_packages(include=["clickstream", "clickstream.*"]),
    install_requires=[],  # apache-beam is already on the Dataflow worker image
)
