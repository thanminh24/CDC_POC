#!/usr/bin/env python3
# test_cdc.py
# This script ensures Kafka connectors are created, queries Postgres tables before and after insert for baseline/verification,
# inserts random test data into PostgreSQL tables, waits for propagation, and queries
# the corresponding Iceberg tables via the recommended spark-submit command.
# Logs are suppressed for Iceberg output, showing only the tables using positive keyword matching.
# Assumptions:
# - Docker Compose services (kafka-postgres, kafka-standalone, kafka-spark) are running.
# - The CDC pipeline (Debezium source, Kafka, Iceberg sink) configurations are in kafka/config/.
# - Iceberg tables are accessible via the script /scripts/print_iceberg_tables.py with 'spark' arg.
# - Run this script from the repository root where docker-compose.yaml exists.
# - Python 3.x with standard libraries (no external dependencies required).
# - Place this script in the repo root for integration.

import subprocess
import time
import sys
import json
import random

def run_command(command, capture_output=True):
    """Helper function to run shell commands with error handling."""
    try:
        ret = subprocess.run(command, shell=True, capture_output=capture_output, text=True, check=True)
        if capture_output:
            return (ret.stdout + ret.stderr).strip()
        else:
            return None
    except subprocess.CalledProcessError as e:
        if capture_output:
            error_output = (e.stdout + e.stderr).strip()
        else:
            error_output = str(e)
        print(f"Error executing command '{command}': {error_output}", file=sys.stderr)
        raise

def ensure_connector(name, config_file):
    """Ensure the Kafka connector is created if it doesn't exist."""
    try:
        output = run_command("curl -s http://localhost:8083/connectors")
        connectors = json.loads(output)
        if name in connectors:
            print(f"Connector '{name}' already exists.")
            return
    except Exception as e:
        print(f"Error checking connectors: {e}")
        return

    create_cmd = (
        f'curl -X POST -H "Content-Type: application/json" '
        f'--data "@{config_file}" http://localhost:8083/connectors'
    )
    print(f"Creating connector '{name}'...")
    try:
        create_output = run_command(create_cmd)
        print(create_output)
    except Exception as e:
        print(f"Failed to create connector '{name}'.")

# Ensure connectors are set up
ensure_connector("dbz-pg-source", "kafka/config/connect-postgres-source.json")
ensure_connector("iceberg-sink", "kafka/config/connect-iceberg-sink.json")

# Wait for connectors to initialize
print("Waiting 10 seconds for connectors to initialize...")
time.sleep(10)

# Query Postgres tables for baseline (before insert)
print("Querying Postgres table 'commerce.account' (before insert)...")
pg_account_cmd = "docker compose exec -T kafka-postgres psql -U postgres -d postgres -c \"SELECT * FROM commerce.account ORDER BY user_id;\""
try:
    pg_account_output = run_command(pg_account_cmd)
    print(pg_account_output)
except Exception:
    print("Failed to query Postgres account table (before).")

print("Querying Postgres table 'commerce.product' (before insert)...")
pg_product_cmd = "docker compose exec -T kafka-postgres psql -U postgres -d postgres -c \"SELECT * FROM commerce.product ORDER BY product_id;\""
try:
    pg_product_output = run_command(pg_product_cmd)
    print(pg_product_output)
except Exception:
    print("Failed to query Postgres product table (before).")

# Generate random test data
random_email = f"test_{random.randint(0, 999999)}@example.com"
random_product = f"Item_{random.choice(['A', 'B', 'C'])}{random.randint(0, 999999)}"

# Insert new data into PostgreSQL using psql via docker compose exec
print("Inserting test data into PostgreSQL...")
insert_sql = f"""
INSERT INTO commerce.account (email) VALUES ('{random_email}');
INSERT INTO commerce.product (product_name) VALUES ('{random_product}');
"""
# Use echo to pipe SQL into psql
command = f"echo \"{insert_sql}\" | docker compose exec -T kafka-postgres psql -U postgres -d postgres"
try:
    output = run_command(command)
    print(output)
    print("Data inserted successfully.")
except Exception:
    print("Insert may have partially failed, but continuing...")

# Query Postgres tables after insert for verification
print("Querying Postgres table 'commerce.account' (after insert)...")
try:
    pg_account_output_after = run_command(pg_account_cmd)
    print(pg_account_output_after)
except Exception:
    print("Failed to query Postgres account table (after).")

print("Querying Postgres table 'commerce.product' (after insert)...")
try:
    pg_product_output_after = run_command(pg_product_cmd)
    print(pg_product_output_after)
except Exception:
    print("Failed to query Postgres product table (after).")

# Wait for CDC to propagate (adjust sleep time as needed based on pipeline latency)
wait_seconds = 30
print(f"Waiting {wait_seconds} seconds for CDC propagation...")
time.sleep(wait_seconds)

# Query Iceberg tables using the recommended spark-submit command, suppressing non-table logs with positive keyword filter
print("Querying Iceberg tables via print_iceberg_tables.py 'spark'...")
iceberg_cmd = (
    "docker container exec kafka-spark "
    "/opt/spark/bin/spark-submit "
    "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0 "
    "--conf spark.driver.extraJavaOptions=\"-Divy.cache.dir=/.ivy -Divy.home=/.ivy\" "
    "/scripts/print_iceberg_tables.py spark"
)
try:
    full_output = run_command(iceberg_cmd)
    # Filter to show only table lines: borders (+), headers (before/after/op), data (NULL, user_id/product_id in JSON)
    lines = full_output.split('\n')
    for line in lines:
        stripped = line.strip()
        if stripped.startswith('+') or ('|' in stripped and ('before' in stripped or 'after' in stripped or 'op' in stripped or 'NULL' in stripped or 'user_id' in stripped or 'product_id' in stripped)):
            print(line)
except Exception:
    print("Query failed; tables may not exist or pipeline not propagating.")

print(f"Test complete. Check the output above to verify if the new data ('{random_email}' and '{random_product}') appears in the Iceberg tables.")