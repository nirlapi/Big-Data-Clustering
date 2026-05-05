# Big Data Clustering

> Streaming clustering experiments using Kafka + Spark (K-Means & GMM)

Project created as a demonstration for a CV / portfolio. The repository contains streaming and batch experiments that show how to run distributed incremental K-Means clustering on streaming data (Kafka) with Spark for distributed map‑reduce centroid updates. It also includes a Gaussian Mixture Model notebook for comparison.

## Features

- Streaming ingestion via Kafka
- Distributed incremental K-Means updates using Spark (map–reduce style)
- Notebooks demonstrating the K-Means and GMM experiments
- Utilities for producing example messages to Kafka

## Repo structure

- `K-Means.ipynb` — Notebook implementing the incremental, distributed K-Means streaming experiment
- `GMM.ipynb` — Notebook with Gaussian Mixture Model experiments
- `producer.py` — Simple data producer used to publish datasets to a Kafka topic

## Quick setup

Prerequisites:

- Python 3.8+ (3.10 recommended)
- Apache Kafka running locally (broker on `localhost:9092`) or accessible cluster
- Apache Spark for running the notebooks (or use `pyspark` package)

Recommended steps (macOS / Linux):

```bash
# create & activate a virtualenv
python3 -m venv .venv
source .venv/bin/activate

# install required packages (adjust versions as needed)
pip install confluent-kafka pyspark numpy pandas
```

Notes:

- Ensure a Kafka broker is running before starting the streaming experiments.
- Spark can be started locally (standalone) for notebook runs; the notebooks initialize a SparkSession.

## Usage

1. Start Kafka (and Zookeeper if your Kafka distribution requires it).
2. Produce data to the topic used by the notebooks. You can use `producer.py` to publish sample records.

Example run (after activating `.venv` and starting Kafka):

```bash
python producer.py
# then open and run the notebooks: K-Means.ipynb or GMM.ipynb
```

## Notes for reviewers

- The K-Means notebook includes a warm-start, distributed centroid updates using Spark's `mapPartitions`/`treeAggregate` pattern, and a final silhouette evaluation computed on the collected dataset.
- The notebooks are designed for local benchmarking and educational demonstration; configuration values (partitions, replication factor, window size) are kept small for single-machine runs.

## Contributors

- Nir Lapidot
- Shiri Guniman
- Anaelle Meimoun

---

If you want, I can also add a `requirements.txt`, example Kafka startup commands, or a short CI step to validate notebooks. Would you like any of those added?