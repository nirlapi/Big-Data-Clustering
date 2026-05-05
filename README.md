# Big Data Streaming Clustering: Spotify Audio Features Analysis

> **A distributed, incremental Machine Learning pipeline using Apache Spark and Kafka for real-time clustering of streaming data.**

This project demonstrates the architecture and implementation of a streaming Big Data pipeline designed to cluster complex, high-dimensional data (such as Spotify tracks based on musical characteristics). It implements distributed, incremental updates for **K-Means** and **Gaussian Mixture Models (GMM)**, processing data as it arrives via Kafka without requiring a full model retrain. 

## 🧠 Technical Highlights & Architecture

This repository showcases advanced data science and data engineering methodologies:

* **Streaming Data Ingestion (Kafka):** A custom Python producer simulates real-time data streaming by parsing massive CSV files (up to 114k rows) and publishing batched, JSON-serialized messages to a local Kafka broker with controlled pacing.
* **Distributed Map-Reduce ML (PySpark):** Instead of using standard single-node libraries, the clustering algorithms are implemented using Spark's `mapPartitions` and `treeAggregate` functions. This enables distributed Expectation-Maximization (EM) for GMMs and incremental centroid updates for K-Means across a cluster.
* **Dynamic Feature Alignment:** The pipeline is resilient to schema evolution. If new features appear in later streaming windows, the models dynamically extend their parameters (e.g., zero-padding means and identity covariance matrices for GMMs) while keeping the data distributed.
* **Performance Optimization:** Includes JVM and Catalyst Optimizer warm-start procedures to eliminate one-time initialization overhead during streaming. It also features buffer size monitoring to fast-forward processing when data spikes occur.
* **Distributed Model Evaluation:** Implements scalable, Map-Reduce-based computation of global evaluation metrics, including Akaike Information Criterion (AIC) and Bayesian Information Criterion (BIC) for GMMs, and a centroid-based approximate Silhouette Score for K-Means.

## 📂 Repository Structure

* `K-Means.ipynb` — Implements the incremental, distributed K-Means streaming pipeline. Features include initial Spark ML K-Means++ bootstrapping, distributed centroid updates, and final global silhouette evaluation.
* `GMM.ipynb` — Implements a distributed streaming Gaussian Mixture Model. Features include complex incremental EM updates handling full covariance matrices, and distributed AIC/BIC metric tracking per window.
* `producer.py` — A robust Kafka producer script that streams structured datasets (ranging from 30k to 114k rows) sequentially into the pipeline, utilizing batching and pacing controls to simulate real-world streaming throughput.

## 🛠️ Tech Stack

* **Languages:** Python, SQL
* **Big Data & Streaming:** Apache Spark (PySpark), Apache Kafka (`confluent_kafka`)
* **Machine Learning:** PySpark MLlib, NumPy, SciPy
* **Data Manipulation:** Pandas

## 🚀 Quick Setup & Usage

**Prerequisites:**
* Python 3.8+ (3.10 recommended)
* Apache Kafka running locally (broker on `localhost:9092`)
* Apache Spark (or the `pyspark` Python package)

**1. Environment Setup (macOS / Linux):**
```bash
# create & activate a virtualenv
python3 -m venv .venv
source .venv/bin/activate

# install required packages
pip install confluent-kafka pyspark numpy pandas scipy
```

**2. Running the Pipeline:**
First, ensure your Kafka broker and Zookeeper (if required) are running. Then, start the data producer to stream the Spotify datasets into the topic:
```bash
python producer.py
```
While the producer is running, execute either `K-Means.ipynb` or `GMM.ipynb` to consume the stream, dynamically update the clustering models, and view the real-time distributed evaluation metrics.

## 👥 Contributors

* Nir Lapidot
* Shiri Guniman
* Anaelle Meimoun
