# Kafka producer that streams rows from one or more CSV files into a Kafka topic.
# Each Kafka message contains a *batch* (list) of parsed rows to reduce per-message overhead.
# Timing is controlled with INTERVAL to simulate/pace streaming.

import time
import json
import csv
from kafka import KafkaProducer

# Kafka/topic configuration (adjust to match your local broker + consumer expectations)
TOPIC_NAME = "final_runn"
BOOTSTRAP_SERVERS = ['localhost:9092']

# Batching + pacing controls:
# - BATCH_SIZE: number of parsed CSV rows bundled into one Kafka message
# - INTERVAL: sleep after each batch send (helps throttle throughput)
BATCH_SIZE = 1000
INTERVAL = 0.2

# Ordered list of CSV "experiments" to stream sequentially (one after the other)
experiments = [
    "exported_dataframes/df_30k_part_1.csv",
    "exported_dataframes/df_30k_part_2.csv",
    "exported_dataframes/df_30k_part_3.csv",
    "exported_dataframes/df_60k_part_1.csv",
    "exported_dataframes/df_60k_part_2.csv",
    "exported_dataframes/df_60k_part_3.csv",
    "exported_dataframes/df_90k_part_1.csv",
    "exported_dataframes/df_90k_part_2.csv",
    "exported_dataframes/df_90k_part_3.csv",
    "exported_dataframes/spotify_data_prepped.csv"
]

def try_convert(value):
    # Convert CSV string fields into floats when possible.
    # Non-numeric fields (or missing values) return None and are skipped.
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def create_producer():
    # Build a KafkaProducer with JSON serialization for message values.
    # Retries + timeouts help when the broker is slow to respond or temporarily unavailable.
    try:
        return KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            request_timeout_ms=60000,
            retries=5
        )
    except Exception as e:
        # Fail early if Kafka is not reachable; caller will abort streaming for this file.
        print(f"Error connecting to Kafka: {e}")
        return None

def process_and_send(file_path):
    # Stream a single CSV file into Kafka as a sequence of batched messages.
    producer = create_producer()
    if not producer:
        return

    print(f"Starting stream for '{file_path}'...")
    batch = []  # accumulates parsed rows until BATCH_SIZE, then sent as one message

    try:
        with open(file_path, mode='r', encoding='utf-8') as f:
            # DictReader gives {column_name: value} per row based on the CSV header.
            csv_reader = csv.DictReader(f)
            for row in csv_reader:
                # Parse only numeric fields into a compact dict (drops non-numeric columns).
                parsed_row = {}
                for key, value in row.items():
                    converted = try_convert(value)
                    if converted is not None:
                        parsed_row[key] = converted

                # Only enqueue rows that have at least one numeric field after parsing.
                if parsed_row:
                    batch.append(parsed_row)

                # When batch reaches target size, send to Kafka and pace the stream.
                if len(batch) >= BATCH_SIZE:
                    future = producer.send(TOPIC_NAME, batch)
                    # Block until broker acks the send (surface errors early).
                    future.get(timeout=60)

                    batch = []
                    time.sleep(INTERVAL)

            # Send any remaining rows that didn't fill a complete batch.
            if batch:
                future = producer.send(TOPIC_NAME, batch)
                future.get(timeout=60)

        # Ensure buffered messages are actually transmitted before exiting.
        producer.flush()
        print(f"Finished streaming {file_path}")

    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
    except Exception as e:
        # Catch-all for parsing, Kafka, or IO issues during streaming.
        print(f"Unexpected error while streaming: {e}")
    finally:
        # Always close the producer to release network resources cleanly.
        if producer:
            producer.close()

if __name__ == "__main__":
    # Run the full sequence: stream each experiment file in order, with a short pause between files.
    print("Starting Master Producer Sequence...")
    for file in experiments:
        process_and_send(file)
        time.sleep(2)
